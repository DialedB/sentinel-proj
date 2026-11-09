"""Real application recreation and recovery lifespan contracts, isolated from dev data."""

import asyncio
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

from fastapi.testclient import TestClient

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import SecurityEvent
from sentinel.core.service import ServerService
from sentinel.reliability.recovery import run_recovery, storage_call
from sentinel.storage.task_repository import TaskRepository


class DurableAPITests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name) / "tasks.db")
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.config = ServiceConfig("s", ServiceRole.SERVER, "localhost", 8000,
                                    "http://central", database_path=self.path,
                                    recovery_interval_seconds=.01)

    def service(self):
        return ServerService(self.config, TaskRepository(
            self.path, clock=lambda: self.now, lease_seconds=10,
        ))

    def register(self, client):
        self.assertEqual(client.post("/register", json=dict(
            service_id="w", role="worker", host="localhost", port=8002,
        )).status_code, 200)

    def submit(self, client):
        event = SecurityEvent(source="p", event_type="login_success", payload={})
        return client.post("/events", json=event.model_dump(mode="json")).json()

    def result(self, task):
        return dict(worker_id="w", lease_token=task["lease_token"],
                    outcome=dict(status="success", result={"event_id": task["event"]["event_id"]}))

    def test_recreation_preserves_ids_outcomes_dedup_and_recovers_expired_running(self):
        service = self.service()
        self.assertFalse(Path(self.path).exists())
        with TestClient(service.create_app()) as client:
            self.register(client)
            complete = self.submit(client)
            running = client.get("/tasks/next?worker_id=w").json()
            completed = client.post(f'/tasks/{running["task_id"]}/result', json=self.result(running)).json()
            self.submit(client)
            running = client.get("/tasks/next?worker_id=w").json()
            ready = self.submit(client)
        self.now += timedelta(seconds=11)
        with TestClient(self.service().create_app()) as client:
            self.assertEqual(client.get("/services").json(), [])
            self.register(client)
            self.assertEqual(client.get(f'/tasks/{complete["task_id"]}').json(), completed)
            self.assertEqual(client.post("/events", json=completed["event"]).json(), completed)
            recovered = client.get(f'/tasks/{running["task_id"]}').json()
            self.assertEqual(recovered["state"], "READY")
            self.assertEqual(recovered["recovery_count"], 1)
            self.assertEqual(client.get(f'/tasks/{ready["task_id"]}').json()["state"], "READY")
            newer = client.get("/tasks/next?worker_id=w").json()
            self.assertEqual(newer["task_id"], running["task_id"])
            self.assertEqual(newer["attempts"], 2)
            self.assertNotEqual(newer["lease_token"], running["lease_token"])
            path = f'/tasks/{newer["task_id"]}/result'
            baseline = client.get("/stats").json()
            for token in (running["lease_token"], str(uuid4())):
                body = {**self.result(newer), "lease_token": token}
                self.assertEqual(client.post(path, json=body).status_code, 409)
                self.assertEqual(client.get("/stats").json(), baseline)
            missing = self.result(newer)
            del missing["lease_token"]
            self.assertEqual(client.post(path, json=missing).status_code, 422)
            self.assertEqual(client.post(path, json=self.result(newer)).json()["state"], "COMPLETED")
        with TestClient(self.service().create_app()) as client:
            self.assertEqual(client.get("/stats").json()["completed"], 2)
            self.assertEqual(client.get("/stats").json()["recovery_count"], 1)

    def test_expired_result_rejected_before_periodic_recovery(self):
        service = self.service()
        service.config = replace(self.config, recovery_interval_seconds=60)
        with TestClient(service.create_app()) as client:
            self.register(client)
            self.submit(client)
            running = client.get("/tasks/next?worker_id=w").json()
            self.now += timedelta(seconds=10)
            before = client.get("/stats").json()
            self.assertEqual(before["expired_active"], 1)
            response = client.post(f'/tasks/{running["task_id"]}/result', json=self.result(running))
            self.assertEqual(response.status_code, 409)
            self.assertEqual(client.get("/stats").json(), before)
            self.assertEqual(client.get(f'/tasks/{running["task_id"]}').json(), running)

    def test_config_defaults_and_strict_validation(self):
        config = ServiceConfig.from_env(ServiceRole.SERVER, {})
        self.assertEqual((config.database_path, config.task_lease_seconds,
                          config.task_max_attempts, config.recovery_interval_seconds),
                         ("data/sentinel.db", 30, 3, 5))
        loaded = ServiceConfig.from_env(ServiceRole.SERVER, dict(
            DATABASE_PATH=self.path, TASK_LEASE_SECONDS="6", TASK_MAX_ATTEMPTS="2",
            RECOVERY_INTERVAL_SECONDS="1"))
        self.assertEqual((loaded.database_path, loaded.task_lease_seconds,
                          loaded.task_max_attempts, loaded.recovery_interval_seconds),
                         (self.path, 6, 2, 1))
        for name in ("task_lease_seconds", "recovery_interval_seconds"):
            for value in (0, -1, True, "1", float("nan"), float("inf")):
                with self.assertRaises(ValueError):
                    replace(config, **{name: value})
        for value in (0, -1, True, "3", 3.0):
            with self.assertRaises(ValueError):
                replace(config, task_max_attempts=value)
        for value in ("", " ", None, ":memory:"):
            with self.assertRaises(ValueError):
                replace(config, database_path=value)
        for value in ("1.5", "true", "0", "-1"):
            with self.assertRaises(ValueError):
                ServiceConfig.from_env(ServiceRole.SERVER, {"TASK_MAX_ATTEMPTS": value})


class RecoveryLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_periodic_recovery_and_cancellation_no_leaked_loop(self):
        with tempfile.TemporaryDirectory() as directory:
            now = datetime(2026, 1, 1, tzinfo=timezone.utc)
            repo = TaskRepository(str(Path(directory) / "tasks.db"), clock=lambda: now)
            service = ServerService(ServiceConfig(
                "s", ServiceRole.SERVER, "localhost", 8000, "http://central",
                recovery_interval_seconds=.005,
            ), repo)
            app = service.create_app()
            async with app.router.lifespan_context(app):
                repo.submit(SecurityEvent(source="p", event_type="login_success", payload={}))
                running = repo.claim("w")
                now += timedelta(seconds=31)
                for _ in range(100):
                    if repo.get(running.task_id).recovery_count:
                        break
                    await asyncio.sleep(.005)
                self.assertEqual(repo.get(running.task_id).recovery_count, 1)
            self.assertFalse(any(t.get_name() == "recovery-s" for t in asyncio.all_tasks()))

    async def test_storage_cancellation_joins_inflight_thread(self):
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        def operation():
            entered.set()
            release.wait(2)
            finished.set()
        runner = asyncio.create_task(storage_call(operation))
        await asyncio.to_thread(entered.wait, 1)
        runner.cancel()
        await asyncio.sleep(.01)
        self.assertFalse(runner.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await runner
        self.assertTrue(finished.is_set())

    async def test_transient_database_error_retries(self):
        repo = Mock()
        called = asyncio.Event()
        count = 0
        async def recover(repository):
            nonlocal count
            count += 1
            if count == 1:
                raise sqlite3.OperationalError("busy")
            called.set()
        with patch("sentinel.reliability.recovery.recover_once", recover):
            runner = asyncio.create_task(run_recovery(repo, .001))
            try:
                with self.assertLogs("sentinel.reliability.recovery", level="ERROR"):
                    await asyncio.wait_for(called.wait(), 1)
            finally:
                runner.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await runner
        self.assertGreaterEqual(count, 2)
