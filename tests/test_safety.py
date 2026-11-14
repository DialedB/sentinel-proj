"""Poison-message, public-read, pagination, structured-error and Proxy regressions."""

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import httpx
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.common.errors import StorageCorruptionError
from sentinel.common.gateway import ServiceGateway
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.json_payload import validate_payload
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import Success, Task
from sentinel.common.transport import Registration
from sentinel.core.service import ServerService
from sentinel.server.gateway import LocalServiceGateway

from tests.test_tasks import event, success


class PayloadTests(unittest.TestCase):
    def test_json_types_finite_depth_nodes_and_size(self):
        valid = {
            "null": None,
            "bool": True,
            "int": 4,
            "float": 2.5,
            "list": ["text", {}],
        }
        self.assertEqual(validate_payload(valid), valid)
        deep = {}
        for _ in range(200):
            deep = {"nested": deep}
        for invalid in (
            [],
            {1: "not string"},
            {"value": object()},
            {"value": (1, 2)},
            {"value": float("nan")},
            {"value": float("inf")},
            deep,
            {"value": "x" * 65536},
            {"value": list(range(10001))},
            {str(i): i for i in range(10001)},
            {"value": [None] * 10000},
            {"value": 10**5000},
        ):
            with (
                self.subTest(kind=type(invalid).__name__),
                self.assertRaises(ValueError),
            ):
                validate_payload(invalid)
        cyclic = {}
        cyclic["cycle"] = cyclic
        with self.assertRaises(ValueError):
            validate_payload(cyclic)
        with self.assertRaises(ValidationError):
            Success(status="success", result={"value": float("nan")})

    def test_repository_revalidates_nested_mutation_before_atomic_save(self):
        with tempfile.TemporaryDirectory() as directory:
            service = ServerService(
                ServiceConfig.from_env(
                    ServiceRole.SERVER, {"DATABASE_PATH": str(Path(directory) / "db")}
                )
            )
            queue = service.tasks
            queue.initialize()
            e = event()
            e.payload["bad"] = float("nan")
            with self.assertRaises(ValidationError):
                queue.submit(e)
            self.assertEqual(queue.stats()["total"], 0)
            task = queue.submit(event())
            running = queue.claim("w")
            assert running is not None
            result = success(running)
            assert isinstance(result.outcome, Success)
            result.outcome.result["bad"] = float("inf")
            with self.assertRaises(ValidationError):
                queue.finish(task.task_id, result)
            self.assertEqual(queue.get(task.task_id).state.value, "RUNNING")
            self.assertEqual(len(service.events.list_events()), 3)
            with self.assertRaises(ValueError):
                queue.claim(" ")
            for limit in (0, 201, True, "1"):
                with self.assertRaises(ValueError):
                    queue.page(limit=limit)  # type: ignore[arg-type]  # Invalid-input contract.


class SafetyAPITests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.service = ServerService(
            ServiceConfig.from_env(
                ServiceRole.SERVER,
                {
                    "DATABASE_PATH": str(Path(directory.name) / "tasks.db"),
                },
            )
        )
        self.client = TestClient(self.service.create_app())
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)
        for identity, role in (("w", "worker"), ("w2", "worker"), ("p", "producer")):
            self.client.post(
                "/register",
                json=dict(service_id=identity, role=role, host="localhost", port=8001),
            )

    def assert_error(self, response, status, code):
        self.assertEqual(response.status_code, status, response.text)
        body = response.json()
        self.assertEqual(body["error"]["code"], code)
        self.assertTrue(body["error"]["next_action"])
        self.assertEqual(body["detail"], body["error"]["message"])
        return body

    def test_poison_payload_rejected_without_blocking_next_claim(self):
        deep = {}
        for _ in range(200):
            deep = {"child": deep}
        body = event().model_dump(mode="json")
        for changes in (
            {"payload": deep},
            {"payload": {"large": "x" * 65536}},
            {"timestamp": "0001-01-01T00:00:00+14:00"},
            {"source": float("nan")},
            {"payload": {"bad": float("nan")}},
        ):
            response = self.client.post(
                "/events",
                content=json.dumps({**body, **changes}),
                headers={"content-type": "application/json"},
            )
            data = self.assert_error(response, 422, "validation_error")
            self.assertTrue(data["fields"])
            self.assertNotIn("NaN", response.text)
        self.assertEqual(self.client.get("/stats").json()["total"], 0)
        accepted = self.client.post("/events", json=body).json()
        claimed = self.client.get("/tasks/next?worker_id=w").json()
        self.assertEqual(claimed["task_id"], accepted["task_id"])
        self.assertTrue(claimed["lease_token"])
        self.assertEqual(
            self.service.tasks.get(UUID(claimed["task_id"])),
            Task.model_validate(claimed),
        )

    def test_bounded_pagination_and_lease_token_only_in_claim(self):
        submitted = [
            self.client.post("/events", json=event().model_dump(mode="json")).json()
            for _ in range(55)
        ]
        oldest = self.client.get("/tasks").json()
        self.assertEqual(
            [t["task_id"] for t in oldest], [t["task_id"] for t in submitted[:50]]
        )
        latest = self.client.get("/tasks?latest=true").json()
        self.assertEqual(
            [t["task_id"] for t in latest], [t["task_id"] for t in submitted[-50:]]
        )
        page = self.client.get("/tasks?limit=2&latest=true").json()
        self.assertEqual(
            [t["task_id"] for t in page], [t["task_id"] for t in submitted[-2:]]
        )
        after = submitted[1]["task_id"]
        page = self.client.get(f"/tasks?limit=2&after_task_id={after}").json()
        self.assertEqual(
            [t["task_id"] for t in page], [t["task_id"] for t in submitted[2:4]]
        )
        self.assertEqual(
            self.client.get(f"/tasks?after_task_id={submitted[-1]['task_id']}").json(),
            [],
        )
        self.assert_error(
            self.client.get(f"/tasks?after_task_id={uuid4()}"), 404, "not_found"
        )
        for query in ("limit=0", "limit=201", "after_task_id=no"):
            self.assert_error(
                self.client.get("/tasks?" + query), 422, "validation_error"
            )
        claimed = self.client.get("/tasks/next?worker_id=w").json()
        public = self.client.get("/tasks/" + claimed["task_id"]).json()
        duplicate = self.client.post("/events", json=claimed["event"]).json()
        for task in (public, duplicate, *self.client.get("/tasks?limit=200").json()):
            self.assertNotIn("lease_token", task)
        self.assertTrue(claimed["lease_token"])
        self.assertEqual(len(self.service.tasks.snapshot()), 55)

    def test_registration_address_reserved_identity_and_sequence_overflow(self):
        for host in (
            "http://example.com",
            "host/path",
            "u@host",
            "host?x",
            "host#x",
            "host:80",
            " ",
            "bad_label",
        ):
            self.assert_error(
                self.client.post(
                    "/register",
                    json=dict(service_id="new", role="worker", host=host, port=8001),
                ),
                422,
                "validation_error",
            )
        for host in ("localhost", "worker-2", "127.0.0.1", "::1", "example.com."):
            self.assertEqual(
                self.client.post(
                    "/register",
                    json=dict(service_id="new", role="worker", host=host, port=8001),
                ).status_code,
                200,
            )
        for identity in (self.service.service_id, self.service.config.main_server_id):
            self.assert_error(
                self.client.post(
                    "/register",
                    json=dict(
                        service_id=identity, role="worker", host="localhost", port=8001
                    ),
                ),
                409,
                "conflict",
            )
        task = self.client.post("/events", json=event().model_dump(mode="json")).json()
        for path in ("/events/lifecycle", f"/tasks/{task['task_id']}/events"):
            self.assert_error(
                self.client.get(path + "?after_sequence=" + str(2**63)),
                422,
                "validation_error",
            )
        for value in (-1, 2**63, True):
            with self.assertRaises(ValueError):
                self.service.events.list_events(after_sequence=value)
        self.assert_error(
            self.client.get("/tasks/next?worker_id=absent"), 404, "unknown_worker"
        )
        self.assert_error(self.client.get("/tasks/next?worker_id=p"), 409, "wrong_role")

    def test_corrupt_storage_reports_503_not_misleading_conflict(self):
        task = self.service.tasks.submit(event())
        with self.service.tasks.database.connection(write=True) as connection:
            connection.execute(
                "UPDATE tasks SET task_json='not-json' WHERE task_id=?",
                (str(task.task_id),),
            )
        with self.assertLogs("sentinel.server.errors", level="ERROR"):
            for path in ("/tasks/next?worker_id=w", f"/tasks/{task.task_id}", "/tasks"):
                data = self.assert_error(
                    self.client.get(path), 503, "storage_corruption"
                )
                self.assertIn("back up", data["error"]["next_action"])
        with self.assertRaises(StorageCorruptionError):
            self.service.tasks.get(task.task_id)

    def test_structured_http_storage_and_unexpected_errors(self):
        self.assert_error(self.client.get("/missing"), 404, "not_found")
        self.assert_error(self.client.post("/health"), 405, "method_not_allowed")
        with patch.object(
            self.service.tasks,
            "stats",
            side_effect=sqlite3.OperationalError("private disk details"),
        ):
            with self.assertLogs("sentinel.server.errors", level="ERROR"):
                response = self.client.get("/stats")
            self.assert_error(response, 503, "storage_unavailable")
            self.assertNotIn("private disk", response.text)
        with patch.object(
            self.service.tasks,
            "stats",
            side_effect=RuntimeError("private stack details"),
        ):
            with self.assertLogs("sentinel.server.errors", level="ERROR"):
                response = TestClient(
                    self.client.app, raise_server_exceptions=False
                ).get("/stats")
            self.assert_error(response, 500, "internal_error")
            self.assertNotIn("private stack", response.text)
        with patch.object(
            self.service.tasks,
            "stats",
            side_effect=HTTPException(418, "teapot", headers={"X-Test": "kept"}),
        ):
            response = self.client.get("/stats")
            self.assert_error(response, 418, "http_error")
            self.assertEqual(response.headers["X-Test"], "kept")


class ProxyIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_subject_and_http_proxy_share_application_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            service = ServerService(
                ServiceConfig.from_env(
                    ServiceRole.SERVER, {"DATABASE_PATH": str(Path(directory) / "db")}
                )
            )
            service.tasks.initialize()
            local = LocalServiceGateway(service.tasks, service.registry)
            self.assertIsInstance(local, ServiceGateway)
            transport = httpx.AsyncClient(
                base_url="http://central/",
                transport=httpx.ASGITransport(app=service.create_app()),
            )
            with patch(
                "sentinel.common.http_client.httpx.AsyncClient", return_value=transport
            ):
                async with ServiceHTTPClient("http://central", 1) as remote:
                    for gateway in (local, remote):
                        await gateway.register(
                            Registration(
                                service_id="w",
                                role=ServiceRole.WORKER,
                                host="localhost",
                                port=8001,
                            )
                        )
                        await gateway.heartbeat("w")
                        submitted = await gateway.submit_event(event())
                        running = await gateway.claim_task("w")
                        assert running is not None
                        self.assertEqual(running.task_id, submitted.task_id)
                        self.assertIsNotNone(running.lease_token)
                        finished = await gateway.submit_result(
                            running.task_id, success(running)
                        )
                        self.assertEqual(finished.state.value, "COMPLETED")
                        self.assertIsNone(await gateway.claim_task("w"))
            self.assertTrue(transport.is_closed)
            self.assertEqual(service.tasks.stats()["completed"], 2)
