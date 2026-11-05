"""Phase 5 domain, atomic queue, command and HTTP contracts."""

import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient
from pydantic import ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import (
    EventType,
    Failure,
    SecurityEvent,
    Success,
    Task,
    TaskResult,
    TaskState,
)
from sentinel.core.commands import AnalyzeSecurityEventCommand, TaskCommand
from sentinel.core.service import ServerService
from sentinel.producer.generator import SecurityEventGenerator
from sentinel.security.evaluator import SecurityEvaluator
from sentinel.server.tasks import TaskQueue
from sentinel.worker.executor import WorkerExecutor


def event(**changes: object) -> SecurityEvent:
    data: dict[str, Any] = dict(
        source="producer-1",
        event_type="login_success",
        payload={"nested": {"value": 1}},
    )
    data.update(changes)
    return SecurityEvent.model_validate(data)


def success(task: Task, worker: str = "w") -> TaskResult:
    return TaskResult(
        worker_id=worker,
        outcome=Success(
            status="success", result=dict(SecurityEvaluator().evaluate(task.event))
        ),
    )


class DomainTests(unittest.TestCase):
    def test_event_validation_and_utc(self) -> None:
        first = event()
        self.assertEqual(first.timestamp.utcoffset(), timedelta(0))
        self.assertNotEqual(first.event_id, event().event_id)
        for field, value in [
            ("timestamp", True),
            ("timestamp", 123),
            ("timestamp", datetime(2026, 1, 1)),
            ("event_id", "bad"),
            ("event_id", True),
            ("event_type", "invalid"),
            ("payload", []),
            ("source", " "),
        ]:
            data = first.model_dump()
            data[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(
                ValidationError
            ):
                SecurityEvent.model_validate(data)
        data = first.model_dump()
        data["timestamp"] = "2026-01-01T03:00:00+03:00"
        self.assertEqual(SecurityEvent(**data).timestamp.hour, 0)

    def test_explicit_lifecycle_and_no_terminal_transitions(self) -> None:
        initial = Task(event=event())
        with self.assertRaises(ValueError):
            initial.claim("w")
        ready = initial.ready()
        running = ready.claim("w")
        with self.assertRaises(ValueError):
            running.finish(success(running, "other"))
        complete = running.finish(success(running))
        failed = running.finish(
            TaskResult(worker_id="w", outcome=Failure(status="failure", error="bad"))
        )
        self.assertEqual(complete.state, TaskState.COMPLETED)
        self.assertEqual(failed.state, TaskState.FAILED)
        for terminal in (complete, failed):
            for call in (
                terminal.ready,
                lambda: terminal.claim("w"),
                lambda: terminal.finish(success(terminal)),
            ):
                with self.assertRaises(ValueError):
                    call()
        self.assertEqual(initial.state, TaskState.PENDING)
        with self.assertRaises(ValidationError):
            initial.state = TaskState.READY

    def test_result_union_requires_exactly_one(self) -> None:
        for outcome in (
            {"status": "success"},
            {"status": "failure"},
            {"status": "success", "result": {}, "error": "bad"},
            {"status": "failure", "error": "bad", "result": {}},
            {"status": "failure", "error": " "},
        ):
            with self.assertRaises(ValidationError):
                TaskResult(worker_id="w", outcome=outcome)

    def test_exact_event_contract_and_generator(self) -> None:
        self.assertEqual(
            {kind.name: kind.value for kind in EventType},
            {
                "LOGIN_FAILED": "login_failed",
                "LOGIN_SUCCESS": "login_success",
                "ACCESS_DENIED": "access_denied",
            },
        )
        generator = SecurityEventGenerator("producer-2")
        events = [generator.generate() for _ in range(6)]
        self.assertEqual(len({e.event_id for e in events}), 6)
        self.assertEqual([e.event_type for e in events], list(EventType) * 2)
        for generated in events:
            self.assertEqual(
                set(generated.model_dump()),
                {"event_id", "source", "event_type", "timestamp", "payload"},
            )
            self.assertEqual(generated.source, "producer-2")
            self.assertEqual(generated.timestamp.utcoffset(), timedelta(0))
            self.assertEqual(
                generated.payload, {"user": "fictional-alex", "source_ip": "192.0.2.10"}
            )
        for invalid in ("benign", "suspicious", "high_risk"):
            with self.assertRaises(ValidationError):
                event(event_type=invalid)
        legacy = event().model_dump()
        legacy["producer_id"] = legacy.pop("source")
        with self.assertRaises(ValidationError):
            SecurityEvent.model_validate(legacy)

    def test_exact_evaluator_and_command_results(self) -> None:
        executor = WorkerExecutor()
        evaluator = SecurityEvaluator()
        for kind, classification, score in [
            (EventType.LOGIN_SUCCESS, "BENIGN", 0),
            (EventType.LOGIN_FAILED, "SUSPICIOUS", 40),
            (EventType.ACCESS_DENIED, "HIGH_RISK", 80),
        ]:
            e = event(event_type=kind)
            before = datetime.now().astimezone()
            for result in (
                evaluator.evaluate(e),
                executor.execute(
                    "analyze_security_event",
                    e.model_dump(mode="json"),
                ),
            ):
                self.assertEqual(
                    set(result),
                    {
                        "event_id",
                        "classification",
                        "risk_score",
                        "triggered_rule",
                        "processing_timestamp",
                    },
                )
                self.assertEqual(result["event_id"], str(e.event_id))
                self.assertEqual(result["classification"], classification)
                self.assertEqual(result["risk_score"], score)
                self.assertIs(type(result["risk_score"]), int)
                self.assertEqual(result["triggered_rule"], kind.value)
                processed = datetime.fromisoformat(result["processing_timestamp"])
                self.assertEqual(processed.utcoffset(), timedelta(0))
                self.assertGreaterEqual(processed, before)
                self.assertLessEqual(processed, datetime.now().astimezone())
        self.assertTrue(issubclass(AnalyzeSecurityEventCommand, TaskCommand))
        with self.assertRaises(TypeError):
            TaskCommand()
        with self.assertRaisesRegex(ValueError, "Unknown command"):
            executor.execute("missing", {})

    def test_analyze_command_delegates_to_injected_evaluator(self) -> None:
        from unittest.mock import create_autospec

        evaluator = create_autospec(SecurityEvaluator, instance=True)
        e = event()
        expected = SecurityEvaluator().evaluate(e)
        evaluator.evaluate.return_value = expected
        command = AnalyzeSecurityEventCommand(evaluator)
        self.assertEqual(command.execute(e.model_dump(mode="json")), expected)
        evaluator.evaluate.assert_called_once_with(e)


class QueueTests(unittest.TestCase):
    def test_fifo_dedup_conflicts_and_copy_isolation(self) -> None:
        q = TaskQueue()
        e = event()
        first = q.submit(e)
        self.assertEqual(q.submit(e).task_id, first.task_id)
        changed = e.model_copy(deep=True)
        changed.payload["nested"]["value"] = 2
        with self.assertRaises(ValueError):
            q.submit(changed)
        changed.payload["nested"]["value"] = True
        with self.assertRaises(ValueError):
            q.submit(changed)
        e.payload["nested"]["value"] = 3
        first.event.payload.clear()
        self.assertEqual(q.get(first.task_id).event.payload["nested"]["value"], 1)
        second = q.submit(event())
        self.assertEqual(q.claim("w").task_id, first.task_id)
        self.assertEqual(q.claim("w2").task_id, second.task_id)
        self.assertIsNone(q.claim("w"))
        with self.assertRaises(ValueError):
            q.finish(first.task_id, success(q.get(first.task_id), "w2"))
        result = success(q.get(first.task_id))
        q.finish(first.task_id, result)
        result.outcome.result.clear()
        self.assertIn("event_id", q.get(first.task_id).outcome.result)
        with self.assertRaises(ValueError):
            q.finish(first.task_id, success(q.get(first.task_id)))
        self.assertEqual(
            q.stats(),
            dict(
                total=2,
                pending=0,
                ready=0,
                running=1,
                completed=1,
                failed=0,
                queue_depth=0,
            ),
        )
        snapshot = q.snapshot()
        snapshot[0].event.payload.clear()
        snapshot.clear()
        self.assertTrue(q.get(first.task_id).event.payload)
        with self.assertRaises(KeyError):
            q.get(uuid4())

    def test_concurrent_atomic_claims_and_submissions(self) -> None:
        q = TaskQueue()
        e = event()
        with ThreadPoolExecutor(max_workers=16) as pool:
            ids = list(pool.map(lambda _: q.submit(e).task_id, range(100)))
        self.assertEqual(len(set(ids)), 1)
        for _ in range(49):
            q.submit(event())
        with ThreadPoolExecutor(max_workers=16) as pool:
            claimed = list(pool.map(lambda i: q.claim(f"w{i}"), range(100)))
        tasks = [t for t in claimed if t]
        self.assertEqual(len(tasks), 50)
        self.assertEqual(len({t.task_id for t in tasks}), 50)
        self.assertEqual(q.stats()["running"], 50)


class TaskAPITests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ServerService(
            ServiceConfig("s", ServiceRole.SERVER, "localhost", 8000, "http://central")
        )
        self.client = TestClient(self.service.create_app())
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)
        for name, role in [("w", "worker"), ("w2", "worker"), ("p", "producer")]:
            self.client.post(
                "/register",
                json=dict(service_id=name, role=role, host="localhost", port=8001),
            )

    def test_pipeline_identity_dedup_conflicts_and_stats(self) -> None:
        c = self.client
        self.assertEqual(c.get("/tasks/next").status_code, 422)
        self.assertEqual(c.get("/tasks/next?worker_id=unknown").status_code, 404)
        self.assertEqual(c.get("/tasks/next?worker_id=p").status_code, 409)
        empty = c.get("/tasks/next?worker_id=w")
        self.assertEqual((empty.status_code, empty.content), (204, b""))
        e = event().model_dump(mode="json")
        task = c.post("/events", json=e).json()
        self.assertEqual(task["state"], "READY")
        self.assertEqual(c.post("/events", json=e).json()["task_id"], task["task_id"])
        self.assertEqual(c.post("/events", json={**e, "payload": {}}).status_code, 409)
        task = c.get("/tasks/next?worker_id=w").json()
        path = "/tasks/" + task["task_id"] + "/result"
        body = dict(
            worker_id="w",
            outcome=dict(
                status="success",
                result=dict(
                    SecurityEvaluator().evaluate(SecurityEvent.model_validate(e))
                ),
            ),
        )
        self.assertEqual(
            c.post(path, json={**body, "worker_id": "w2"}).status_code, 409
        )
        self.assertEqual(c.post(path, json={**body, "worker_id": "p"}).status_code, 409)
        self.assertEqual(
            c.post(path, json={**body, "worker_id": "missing"}).status_code, 404
        )
        self.assertEqual(
            c.post(
                path, json={**body, "outcome": dict(status="success", result={})}
            ).status_code,
            409,
        )
        self.assertEqual(
            c.post("/tasks/" + str(uuid4()) + "/result", json=body).status_code, 404
        )
        completed = c.post(path, json=body).json()
        self.assertEqual(completed["state"], "COMPLETED")
        self.assertEqual(completed["outcome"], body["outcome"])
        self.assertEqual(completed["event"], e)
        self.assertEqual(c.post(path, json=body).status_code, 409)
        self.assertEqual(c.get("/stats").json()["completed"], 1)
        self.assertEqual(len(c.get("/tasks").json()), 1)
        self.assertEqual(c.get("/tasks/" + task["task_id"]).json()["worker_id"], "w")
        self.assertEqual(c.get("/tasks/" + str(uuid4())).status_code, 404)
        self.assertEqual(
            c.post("/events", json={**e, "timestamp": True}).status_code, 422
        )

    def test_failure_route(self) -> None:
        self.client.post("/events", json=event().model_dump(mode="json"))
        task = self.client.get("/tasks/next?worker_id=w").json()
        response = self.client.post(
            "/tasks/" + task["task_id"] + "/result",
            json={
                "worker_id": "w",
                "outcome": {"status": "failure", "error": "unknown command"},
            },
        )
        self.assertEqual(response.json()["state"], "FAILED")
        self.assertEqual(self.client.get("/stats").json()["failed"], 1)


class PipelineConfigTests(unittest.TestCase):
    def test_defaults_overrides_and_validation(self) -> None:
        config = ServiceConfig.from_env(ServiceRole.WORKER, {})
        self.assertEqual(
            (
                config.event_generation_interval,
                config.worker_poll_interval,
                config.processing_delay,
            ),
            (2, 1, 0),
        )
        loaded = ServiceConfig.from_env(
            ServiceRole.WORKER,
            {
                "EVENT_GENERATION_INTERVAL": ".5",
                "WORKER_POLL_INTERVAL": ".2",
                "PROCESSING_DELAY": ".1",
            },
        )
        self.assertEqual(
            (
                loaded.event_generation_interval,
                loaded.worker_poll_interval,
                loaded.processing_delay,
            ),
            (0.5, 0.2, 0.1),
        )
        for name in (
            "event_generation_interval",
            "worker_poll_interval",
            "processing_delay",
        ):
            for value in (True, -1, float("nan"), float("inf"), "1"):
                with self.assertRaises(ValueError):
                    replace(config, **{name: value})
        for name in ("EVENT_GENERATION_INTERVAL", "WORKER_POLL_INTERVAL"):
            with self.assertRaises(ValueError):
                ServiceConfig.from_env(ServiceRole.WORKER, {name: "0"})
