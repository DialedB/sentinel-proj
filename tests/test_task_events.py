"""Every accepted task transition atomically appends its lifecycle history."""

import sqlite3
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

from sentinel.storage.event_repository import EventRepository, insert_event
from tests import test_repository


class TaskEventTests(unittest.TestCase):
    setUp = test_repository.RepositoryTests.setUp
    reopen = test_repository.RepositoryTests.reopen
    event = test_repository.RepositoryTests.event
    result = test_repository.RepositoryTests.result

    def history(self):
        return EventRepository(self.repo.database).list_events(
            after_sequence=0, limit=200
        )

    def test_complete_history_and_duplicate_rejection(self):
        security = self.event()
        task = self.repo.submit(security)
        self.assertEqual(self.repo.submit(security), task)
        running = self.repo.claim("worker")
        self.repo.finish(task.task_id, self.result(running))
        with self.assertRaises(ValueError):
            self.repo.finish(task.task_id, self.result(running))
        events = self.history()
        self.assertEqual(
            [e.event_type.value for e in events],
            ["task.created", "task.ready", "task.started", "task.completed"],
        )
        self.assertEqual(events[2].payload.worker_id, "worker")
        self.assertEqual(events[2].payload.attempts, 1)
        self.assertIsNotNone(events[2].payload.lease_expires_at)
        self.assertTrue(all("lease_token" not in e.model_dump_json() for e in events))
        self.assertEqual(len({e.event_id for e in events}), 4)
        self.assertTrue(all(e.task_id == task.task_id for e in events))

    def test_failure_recovery_and_permanent_failure_history(self):
        self.repo.submit(self.event())
        first = self.repo.claim("w1")
        self.repo.finish(first.task_id, self.result(first, failure=True))
        second = self.repo.claim("w2")
        self.now += timedelta(seconds=11)
        self.repo.recover_expired()
        third = self.repo.claim("w3")
        self.now += timedelta(seconds=11)
        self.repo.recover_expired()
        events = self.history()
        self.assertEqual(
            [e.event_type.value for e in events],
            [
                "task.created",
                "task.ready",
                "task.started",
                "task.requeued",
                "task.started",
                "task.requeued",
                "task.started",
                "task.failed",
            ],
        )
        self.assertEqual(events[3].payload.reason, "execution_failed")
        self.assertEqual(events[5].payload.reason, "lease_expired")
        self.assertEqual(events[5].payload.worker_id, second.worker_id)
        self.assertEqual(events[-1].payload.worker_id, third.worker_id)
        self.assertEqual(events[-1].payload.recovery_count, 2)

    def test_insert_failure_rolls_back_task_and_all_events(self):
        calls = 0

        def fail_second(connection, lifecycle):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise sqlite3.OperationalError("injected append failure")
            return insert_event(connection, lifecycle)

        with patch("sentinel.storage.task_repository.insert_event", fail_second):
            with self.assertRaises(sqlite3.OperationalError):
                self.repo.submit(self.event())
        self.assertEqual(self.repo.snapshot(), [])
        self.assertEqual(self.history(), [])
        self.repo.submit(self.event())
        before = self.repo.snapshot(), self.history()
        with patch(
            "sentinel.storage.task_repository.insert_event",
            side_effect=RuntimeError("append"),
        ):
            with self.assertRaises(RuntimeError):
                self.repo.claim("w")
        self.assertEqual((self.repo.snapshot(), self.history()), before)

    def test_concurrent_dedup_appends_only_two_events(self):
        security = self.event()
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda _: self.reopen().submit(security), range(24)))
        self.assertEqual(len(self.history()), 2)
