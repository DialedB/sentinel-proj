"""Shared identity limits, fatal task supervision and gateway failure contracts."""

import asyncio
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from pydantic import ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.common.errors import DomainError
from sentinel.common.gateway import ServiceGateway
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.limits import MAX_SOURCE_LENGTH
from sentinel.common.models import ServiceRole, ServiceStatus
from sentinel.common.tasks import SecurityEvent
from sentinel.common.transport import Heartbeat, Registration
from sentinel.core.pipeline import produce, work
from sentinel.core.service import ProducerService, ServerService, WorkerService
from sentinel.producer.generator import SecurityEventGenerator
from sentinel.server.gateway import LocalServiceGateway
from tests.test_pipeline import cancel, config, task
from tests.test_tasks import event, success


def registration(identity: str, role: ServiceRole = ServiceRole.WORKER) -> Registration:
    return Registration(service_id=identity, role=role, host="localhost", port=8001)


def error_fields(error: DomainError) -> tuple[int, str, str, str]:
    return error.status, error.code, str(error), error.next_action


class IdentityLimitTests(unittest.TestCase):
    def test_config_and_models_share_inclusive_source_limit(self):
        accepted, rejected = "p" * MAX_SOURCE_LENGTH, "p" * (MAX_SOURCE_LENGTH + 1)
        for role in ServiceRole:
            with self.subTest(role=role):
                configured = ServiceConfig.from_env(role, {"SERVICE_ID": accepted})
                self.assertEqual(configured.service_id, accepted)
                with self.assertRaises(ValueError):
                    ServiceConfig.from_env(role, {"SERVICE_ID": rejected})
                with self.assertRaises(ValueError):
                    replace(configured, service_id=rejected)
                with self.assertRaises(ValueError):
                    replace(configured, main_server_id=rejected)
        self.assertEqual(SecurityEventGenerator(accepted).generate().source, accepted)
        self.assertEqual(registration(accepted).service_id, accepted)
        self.assertEqual(Heartbeat(service_id=accepted).service_id, accepted)
        for make in (
            registration,
            lambda identity: Heartbeat(service_id=identity),
            lambda identity: SecurityEvent(
                source=identity, event_type="login_success", payload={}
            ),
        ):
            with self.assertRaises(ValidationError):
                make(rejected)


class SupervisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_fatal_pipeline_is_logged_and_health_stays_unhealthy_after_heartbeat(
        self,
    ):
        for service_type, role in (
            (ProducerService, ServiceRole.PRODUCER),
            (WorkerService, ServiceRole.WORKER),
        ):
            with self.subTest(role=role):
                client = AsyncMock(spec=ServiceGateway)
                fail, heartbeat = asyncio.Event(), asyncio.Event()

                async def broken(*args):
                    await fail.wait()
                    raise RuntimeError("unexpected generator or pipeline failure")

                async def beat(*args):
                    if service.status is ServiceStatus.UNHEALTHY:
                        heartbeat.set()

                client.heartbeat.side_effect = beat
                service = service_type(config(role))
                app = service.create_app()
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://service"
                ) as health_client:
                    with (
                        patch("sentinel.core.service.ServiceHTTPClient") as constructor,
                        patch(
                            "sentinel.core.pipeline.run_pipeline", side_effect=broken
                        ),
                        self.assertLogs("sentinel.core.service", level="ERROR") as logs,
                    ):
                        constructor.return_value.__aenter__ = AsyncMock(
                            return_value=client
                        )
                        constructor.return_value.__aexit__ = AsyncMock(
                            return_value=False
                        )
                        async with app.router.lifespan_context(app):
                            self.assertEqual(
                                (await health_client.get("/health")).json()["status"],
                                "healthy",
                            )
                            fail.set()
                            await asyncio.wait_for(heartbeat.wait(), 1)
                            self.assertEqual(
                                (await health_client.get("/health")).json()["status"],
                                "unhealthy",
                            )
                        self.assertEqual(service.status, ServiceStatus.STOPPED)
                    self.assertEqual(len(logs.records), 1)
                    self.assertIn("pipeline-w", logs.output[0])
                    self.assertIsInstance(logs.records[0].exc_info[1], RuntimeError)

    async def test_unexpected_pipeline_return_marks_health_unhealthy(self):
        service = ProducerService(config(ServiceRole.PRODUCER))
        app = service.create_app()
        with (
            patch("sentinel.core.service.ServiceHTTPClient") as constructor,
            patch("sentinel.core.pipeline.run_pipeline", new_callable=AsyncMock),
            self.assertLogs("sentinel.core.service", level="ERROR") as logs,
        ):
            constructor.return_value.__aenter__ = AsyncMock(
                return_value=AsyncMock(spec=ServiceGateway)
            )
            constructor.return_value.__aexit__ = AsyncMock(return_value=False)
            async with app.router.lifespan_context(app):
                # One turn runs the coroutine, the next runs its done callback.
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                self.assertEqual(service.status, ServiceStatus.UNHEALTHY)
        self.assertEqual(len(logs.records), 1)
        self.assertIn("exited unexpectedly", logs.output[0])
        self.assertEqual(service.status, ServiceStatus.STOPPED)

    async def test_clean_pipeline_cancellation_does_not_report_failure(self):
        service = ProducerService(config(ServiceRole.PRODUCER))
        app = service.create_app()
        entered = asyncio.Event()

        async def blocked(*args):
            entered.set()
            await asyncio.Event().wait()

        with (
            patch("sentinel.core.service.ServiceHTTPClient") as constructor,
            patch("sentinel.core.pipeline.run_pipeline", side_effect=blocked),
            self.assertNoLogs("sentinel.core.service", level="ERROR"),
        ):
            constructor.return_value.__aenter__ = AsyncMock(
                return_value=AsyncMock(spec=ServiceGateway)
            )
            constructor.return_value.__aexit__ = AsyncMock(return_value=False)
            async with app.router.lifespan_context(app):
                await asyncio.wait_for(entered.wait(), 1)
                self.assertEqual(service.status, ServiceStatus.HEALTHY)
            self.assertEqual(service.status, ServiceStatus.STOPPED)


class GatewayContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.service = ServerService(
            ServiceConfig.from_env(
                ServiceRole.SERVER,
                {
                    "SERVICE_ID": "central-custom",
                    "MAIN_SERVER_ID": "central-alias",
                    "DATABASE_PATH": str(Path(directory.name) / "tasks.db"),
                },
            )
        )
        self.service.tasks.initialize()
        self.local = LocalServiceGateway(self.service.tasks, self.service.registry)
        self.transport = httpx.AsyncClient(
            transport=httpx.ASGITransport(self.service.create_app()),
            base_url="http://central/",
        )
        with patch(
            "sentinel.common.http_client.httpx.AsyncClient", return_value=self.transport
        ):
            self.remote = ServiceHTTPClient("http://central", 1)
        await self.remote.__aenter__()
        self.addAsyncCleanup(self.remote.__aexit__, None, None, None)
        await self.local.register(registration("w"))
        await self.local.register(registration("p", ServiceRole.PRODUCER))

    async def assert_same_error(self, operation, status, code):
        failures = []
        for gateway in (self.local, self.remote):
            with self.assertRaises(DomainError) as raised:
                await operation(gateway)
            failures.append(error_fields(raised.exception))
        self.assertEqual(failures[0], failures[1])
        self.assertEqual(failures[0][:2], (status, code))
        self.assertTrue(failures[0][3])

    async def test_identity_not_found_role_and_reserved_ids_have_same_errors(self):
        await self.assert_same_error(lambda g: g.heartbeat("absent"), 404, "not_found")
        await self.assert_same_error(
            lambda g: g.claim_task("absent"), 404, "unknown_worker"
        )
        await self.assert_same_error(lambda g: g.claim_task("p"), 409, "wrong_role")
        for identity in (self.service.service_id, self.service.config.main_server_id):
            await self.assert_same_error(
                lambda g: g.register(registration(identity)), 409, "conflict"
            )
        self.assertEqual(
            {r.service_id for r in self.service.registry.snapshot()}, {"w", "p"}
        )
        # Reserved IDs are explicit configuration, not a hard-coded default.
        await self.local.register(registration("server-1"))
        await self.remote.register(registration("server-1"))

    async def test_task_not_found_duplicate_and_lease_conflicts_have_same_errors(self):
        submitted = await self.local.submit_event(event())
        changed = submitted.event.model_copy(update={"payload": {"different": True}})
        await self.assert_same_error(lambda g: g.submit_event(changed), 409, "conflict")
        running = await self.local.claim_task("w")
        result = success(running)
        await self.assert_same_error(
            lambda g: g.submit_result(uuid4(), result), 404, "not_found"
        )
        stale = result.model_copy(update={"lease_token": uuid4()})
        await self.assert_same_error(
            lambda g: g.submit_result(running.task_id, stale), 409, "conflict"
        )
        await self.local.submit_result(running.task_id, result)
        # A lost acknowledgement is terminal, not a reason to execute again.
        await self.assert_same_error(
            lambda g: g.submit_result(running.task_id, result), 409, "conflict"
        )

    async def test_sqlite_and_corruption_failures_have_same_safe_errors(self):
        running = task()
        for method, operation in (
            ("submit", lambda g: g.submit_event(event())),
            ("claim", lambda g: g.claim_task("w")),
            ("finish", lambda g: g.submit_result(running.task_id, success(running))),
        ):
            with (
                self.subTest(method=method),
                patch.object(
                    self.service.tasks,
                    method,
                    side_effect=sqlite3.OperationalError("private db path"),
                ),
                self.assertLogs("sentinel.server.errors", level="ERROR"),
            ):
                await self.assert_same_error(operation, 503, "storage_unavailable")
        stored = await self.local.submit_event(event())
        with self.service.tasks.database.connection(write=True) as connection:
            connection.execute(
                "UPDATE tasks SET task_json='invalid' WHERE task_id=?",
                (str(stored.task_id),),
            )
        with self.assertLogs("sentinel.server.errors", level="ERROR"):
            await self.assert_same_error(
                lambda g: g.claim_task("w"), 503, "storage_corruption"
            )

    async def test_unexpected_local_errors_are_not_misclassified(self):
        with patch.object(
            self.service.tasks, "submit", side_effect=RuntimeError("bug")
        ):
            with self.assertRaisesRegex(RuntimeError, "bug"):
                await self.local.submit_event(event())


class ApplicationRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_producer_retries_domain_failure_with_same_event(self):
        gateway = AsyncMock(spec=ServiceGateway)
        done = asyncio.Event()
        sent = []

        async def submit(generated):
            sent.append(generated)
            if len(sent) == 1:
                raise DomainError("storage_unavailable", "Offline", "Retry.", 503)
            done.set()

        gateway.submit_event.side_effect = submit
        runner = asyncio.create_task(produce(config(ServiceRole.PRODUCER), gateway))
        try:
            await asyncio.wait_for(done.wait(), 1)
        finally:
            await cancel(runner)
        self.assertIs(sent[0], sent[1])

    async def test_worker_retries_application_503_without_reexecution(self):
        gateway = AsyncMock(spec=ServiceGateway)
        running = task()
        gateway.claim_task.return_value = running
        done = asyncio.Event()
        submitted = []

        async def submit(task_id, result):
            submitted.append(result)
            if len(submitted) == 1:
                raise DomainError("storage_unavailable", "Offline", "Retry.", 503)
            done.set()

        gateway.submit_result.side_effect = submit
        with patch(
            "sentinel.core.pipeline.WorkerExecutor.execute", return_value={}
        ) as execute:
            runner = asyncio.create_task(work(config(), gateway))
            try:
                await asyncio.wait_for(done.wait(), 1)
            finally:
                await cancel(runner)
            execute.assert_called_once()
        self.assertEqual(gateway.claim_task.await_count, 1)
        self.assertIs(submitted[0], submitted[1])
