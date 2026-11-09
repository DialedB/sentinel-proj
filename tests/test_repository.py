"""Durability, transactional concurrency, leases, retries and deterministic recovery."""

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from sentinel.common.tasks import Failure, SecurityEvent, Success, TaskResult, TaskState
from sentinel.storage.task_repository import TaskRepository


class RepositoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = str(Path(self.directory.name) / "nested" / "tasks.db")
        self.now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.repo = self.reopen()

    def reopen(self):
        repo = TaskRepository(self.path, clock=lambda: self.now, lease_seconds=10)
        repo.initialize()
        repo.initialize()
        return repo

    def event(self):
        return SecurityEvent(source="p", event_type="login_failed", payload={"n": 1})

    def result(self, task, *, failure=False, **changes):
        values = dict(worker_id=task.worker_id, lease_token=task.lease_token,
                      outcome=Failure(status="failure", error="execution failed") if failure
                      else Success(status="success", result={"event_id": str(task.event.event_id)}))
        values.update(changes)
        return TaskResult(**values)

    def test_restart_ready_running_terminal_and_dedup_type_sensitive(self):
        e = self.event()
        first = self.repo.submit(e)
        second = self.repo.submit(self.event())
        self.assertEqual(self.repo.claim("w").task_id, first.task_id)
        running = self.repo.get(first.task_id)
        self.repo.finish(first.task_id, self.result(running))
        third = self.repo.submit(self.event())
        self.assertEqual(self.repo.claim("w2").task_id, second.task_id)
        repo = self.reopen()
        self.assertEqual(repo.submit(e).state, TaskState.COMPLETED)
        self.assertEqual(repo.get(first.task_id), self.repo.get(first.task_id))
        self.assertEqual(repo.get(second.task_id).state, TaskState.RUNNING)
        self.assertEqual(repo.get(third.task_id).state, TaskState.READY)
        for value in (True, 1.0, "1"):
            changed = e.model_copy(update={"payload": {"n": value}})
            with self.assertRaises(ValueError):
                repo.submit(changed)
        self.assertEqual(repo.claim("w3").task_id, third.task_id)
        self.assertEqual(repo.stats()["attempts"], 3)

    def test_separate_instances_concurrent_submit_and_claim(self):
        other = self.reopen()
        e = self.event()
        with ThreadPoolExecutor(max_workers=12) as pool:
            ids = list(pool.map(lambda i: (self.repo if i % 2 else other).submit(e).task_id,
                                range(40)))
        self.assertEqual(len(set(ids)), 1)
        for _ in range(29):
            self.repo.submit(self.event())
        with ThreadPoolExecutor(max_workers=12) as pool:
            claims = list(pool.map(lambda i: (self.repo if i % 2 else other).claim(str(i)),
                                   range(60)))
        claims = [t for t in claims if t]
        self.assertEqual(len(claims), 30)
        self.assertEqual(len({t.task_id for t in claims}), 30)
        self.assertEqual(len({t.lease_token for t in claims}), 30)
        self.assertTrue(all(t.attempts == 1 for t in claims))

    def test_stale_wrong_worker_token_expired_and_superseded_are_noops(self):
        self.repo.submit(self.event())
        task = self.repo.claim("w")
        for changes in ({"worker_id": "other"}, {"lease_token": uuid4()}):
            with self.assertRaises(ValueError):
                self.repo.finish(task.task_id, self.result(task, **changes))
            self.assertEqual(self.repo.get(task.task_id), task)
        with self.assertRaises(KeyError):
            self.repo.finish(uuid4(), self.result(task))
        self.now += timedelta(seconds=10)
        self.assertEqual(self.repo.stats()["expired_active"], 1)
        with self.assertRaises(ValueError):
            self.repo.finish(task.task_id, self.result(task))
        self.assertEqual(self.repo.get(task.task_id), task)
        self.assertEqual(self.reopen().recover_expired(), 1)
        self.assertEqual(self.repo.recover_expired(), 0)
        newer = self.repo.claim("w")
        self.assertNotEqual(newer.lease_token, task.lease_token)
        self.assertEqual(newer.attempts, 2)
        with self.assertRaises(ValueError):
            self.repo.finish(task.task_id, self.result(task))
        self.assertEqual(self.repo.get(task.task_id), newer)
        completed = self.repo.finish(newer.task_id, self.result(newer))
        self.assertIsNone(completed.error)
        self.assertIsNone(completed.lease_token)
        with self.assertRaises(ValueError):
            self.repo.finish(newer.task_id, self.result(newer))
        self.assertEqual(self.repo.stats()["recovery_count"], 1)
        self.assertEqual(self.repo.stats()["retry_attempts"], 1)

    def test_execution_failures_requeue_until_persisted_limit_and_terminal_immutable(self):
        self.repo.submit(self.event())
        for attempt in range(1, 4):
            task = self.repo.claim("w")
            self.assertEqual(task.attempts, attempt)
            finished = self.repo.finish(task.task_id, self.result(task, failure=True))
            self.assertEqual(finished.state, TaskState.READY if attempt < 3 else TaskState.FAILED)
            self.assertEqual(finished.error, "execution failed")
            self.repo = self.reopen()
        self.assertIsNone(self.repo.claim("w"))
        self.assertEqual(self.repo.recover_expired(), 0)
        with self.assertRaises(ValueError):
            finished.recover_expired(self.now)
        self.assertEqual(self.repo.stats()["failed"], 1)
        self.assertEqual(self.repo.stats()["retry_attempts"], 2)

    def test_repeated_expiry_across_restart_exhausts_attempts(self):
        self.repo.submit(self.event())
        for attempt in range(1, 4):
            task = self.repo.claim("w")
            self.assertEqual(task.attempts, attempt)
            self.assertEqual(self.repo.recover_expired(), 0)
            self.now += timedelta(seconds=11)
            self.repo = self.reopen()
            self.assertEqual(self.repo.recover_expired(), 1)
            self.assertEqual(self.repo.recover_expired(), 0)
        terminal = self.repo.get(task.task_id)
        self.assertEqual(terminal.state, TaskState.FAILED)
        self.assertIsNone(terminal.worker_id)
        self.assertIsNone(terminal.lease_token)
        self.assertEqual(terminal.recovery_count, 3)
        self.assertEqual(self.repo.stats()["expired_active"], 0)
        self.assertIsNone(self.repo.claim("w"))

    def test_construction_no_io_clock_and_configuration(self):
        absent = str(Path(self.directory.name) / "absent.db")
        TaskRepository(absent)
        self.assertFalse(Path(absent).exists())
        for value in (True, 0, -1, float("inf"), float("nan"), "1"):
            with self.assertRaises(ValueError):
                TaskRepository(absent, lease_seconds=value)
        for value in (True, 0, -1, 1.5, "3"):
            with self.assertRaises(ValueError):
                TaskRepository(absent, max_attempts=value)
        self.now = datetime(2026, 1, 1)
        with self.assertRaises(ValueError):
            self.repo.submit(self.event())
        self.assertEqual(self.repo.snapshot(), [])
