"""Behavioral pattern tests: runtime delegation, isolation and forbidden transitions."""

import asyncio
import logging
import runpy
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.common.gateway import ServiceGateway
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.logging import LoggingConfigurator
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import EventType, Failure, Task, TaskResult, TaskState
from sentinel.core.analysis_factory import (
    AnalysisComponentFactory,
    SyntheticSecurityFactory,
)
from sentinel.core.pipeline import produce, work
from sentinel.core.service import (
    BaseService,
    ProducerService,
    ServerService,
    WatchdogService,
    WorkerService,
)
from sentinel.producer.builder import SecurityEventBuilder
from sentinel.worker.executor import WorkerExecutor
from tests.test_tasks import event, success


class PatternTests(unittest.TestCase):
    def test_singleton_concurrency_and_host_logging_preserved(self):
        with ThreadPoolExecutor(max_workers=16) as pool:
            instances = list(pool.map(lambda _: LoggingConfigurator(), range(100)))
        self.assertTrue(all(item is instances[0] for item in instances))
        root = logging.getLogger()
        before = list(root.handlers)
        instances[0].configure()
        instances[-1].configure()
        self.assertEqual(root.handlers, before)
        self.assertFalse(hasattr(instances[0], "tasks"))

    def test_builder_required_fields_validation_replay_and_copy_isolation(self):
        builder = SecurityEventBuilder()
        with self.assertRaises(ValidationError):
            builder.build()
        payload = {"nested": {"value": 1}}
        builder.from_source("p").of_type(EventType.LOGIN_SUCCESS).with_payload(payload)
        payload["nested"]["value"] = 2
        first, second = builder.build(), builder.build()
        self.assertNotEqual(first.event_id, second.event_id)
        first.payload["nested"]["value"] = 3
        self.assertEqual(second.payload["nested"]["value"], 1)
        replay = builder.identified_by(second.event_id).at(second.timestamp).build()
        self.assertEqual(replay, second)
        with self.assertRaises(ValidationError):
            builder.from_source(" ").build()

    def test_abstract_factory_family_and_injected_command(self):
        with self.assertRaises(TypeError):
            AnalysisComponentFactory()  # type: ignore[abstract]  # Verify the abstract contract.
        with self.assertRaises(TypeError):
            ServiceGateway()  # type: ignore[abstract]  # Verify the abstract contract.
        family = SyntheticSecurityFactory()
        generator = family.create_generator("p")
        command = family.create_analysis_command()
        for _ in range(3):
            generated = generator.generate()
            result = command.execute(generated.model_dump(mode="json"))
            self.assertEqual(result["event_id"], str(generated.event_id))
            self.assertEqual(result["triggered_rule"], generated.event_type.value)
        alternate = Mock(spec=AnalysisComponentFactory)
        alternate.create_analysis_command.return_value.execute.return_value = {
            "alternate": True
        }
        self.assertEqual(
            WorkerExecutor(alternate).execute("analyze_security_event", {}),
            {"alternate": True},
        )
        alternate.create_analysis_command.assert_called_once_with()

    def test_template_method_hooks_preserve_common_setup_and_no_construction_io(self):
        class HookService(BaseService):
            def _configure_routes(self, app: FastAPI):
                self.asserted = app.state.service is self and any(
                    getattr(r, "path", None) == "/health" for r in app.routes
                )
                app.get("/hook")(lambda: {"ok": True})

        config = ServiceConfig.from_env(ServiceRole.PRODUCER, {})
        service = HookService(config)
        app = service.create_app()
        self.assertTrue(service.asserted)
        self.assertEqual(TestClient(app).get("/hook").json(), {"ok": True})
        for cls in (ProducerService, WorkerService, WatchdogService, ServerService):
            self.assertIs(cls.create_app, BaseService.create_app)
        self.assertEqual(service.status.value, "starting")

    def test_state_matrix_and_lease_guards(self):
        initial = Task(event=event())
        ready = initial.ready()
        running = ready.claim("w")
        assert running.lease_token is not None
        complete = running.finish(success(running))
        failed = running.model_copy(update={"max_attempts": 1}).finish(
            TaskResult(
                worker_id="w",
                lease_token=running.lease_token,
                outcome=Failure(status="failure", error="execution failed"),
            )
        )
        for context in (initial, ready, complete, failed):
            with self.assertRaises(ValueError):
                context.finish(success(running))
            with self.assertRaises(ValueError):
                context.recover_expired()
        for context in (ready, running, complete, failed):
            with self.assertRaises(ValueError):
                context.ready()
        for context in (initial, running, complete, failed):
            with self.assertRaises(ValueError):
                context.claim("w")
        for worker in ("", " ", None):
            with self.assertRaises(ValueError):
                ready.claim(worker)  # type: ignore[arg-type]  # Invalid-input contract.
        for lease in (True, 0, -1, "1", float("nan"), float("inf"), 1e100):
            with self.assertRaises(ValueError):
                ready.claim("w", lease)  # type: ignore[arg-type]  # Invalid-input contract.
        with self.assertRaises(ValueError):
            ready.model_copy(update={"attempts": ready.max_attempts}).claim("w")
        for context in (running, running.model_copy(update={"lease_expires_at": None})):
            with self.assertRaises(ValueError):
                context.recover_expired(now=running.updated_at)
        expired = running.recover_expired(now=running.lease_expires_at)
        self.assertEqual(expired.state, TaskState.READY)
        self.assertEqual(expired.recovery_count, 1)
        with self.assertRaises(ValueError):
            initial._transition(TaskState.READY, TaskState.RUNNING)
        with self.assertRaises(ValidationError):
            event(timestamp=datetime(1, 1, 1, tzinfo=timezone(timedelta(hours=14))))

    def test_cli_entrypoints_use_factories_without_starting_servers(self):
        for role in ServiceRole:
            module = f"sentinel.{role.value}.main"
            with patch("sentinel.core.entrypoint.serve") as serve:
                namespace = runpy.run_module(module, run_name="__main__")
                serve.assert_called_once()
                self.assertEqual(serve.call_args.args[1], role)
                self.assertIn("main", namespace)
            with patch("sentinel.core.entrypoint.serve") as serve:
                runpy.run_module(module)
                serve.assert_not_called()


class FactoryPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_runtime_loops_use_injected_family_and_gateway(self):
        from tests.test_pipeline import cancel, config, task

        for loop, method in ((produce, "submit_event"), (work, "submit_result")):
            client = AsyncMock(spec=ServiceGateway)
            family = Mock(spec=AnalysisComponentFactory)
            family.create_generator.return_value.generate.return_value = event()
            family.create_analysis_command.return_value.execute.return_value = {
                "event_id": str(uuid4())
            }
            client.claim_task.return_value = task()
            done = asyncio.Event()
            getattr(client, method).side_effect = lambda *args: done.set()
            runner = asyncio.create_task(loop(config(), client, family))
            try:
                await asyncio.wait_for(done.wait(), 1)
            finally:
                await cancel(runner)
            if loop is produce:
                family.create_generator.assert_called_once_with("w")
            else:
                family.create_analysis_command.assert_called_once_with()
                family.create_analysis_command.return_value.execute.assert_called_once()
        self.assertTrue(issubclass(ServiceHTTPClient, ServiceGateway))
