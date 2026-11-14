"""Cross-component crash boundaries, upgrade compatibility and atomic recovery."""

import asyncio
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

from sentinel.core.event_bus import EventBus
from sentinel.observers.audit import AuditSubscriber
from sentinel.observers.metrics import MetricsSubscriber
from sentinel.reliability.event_dispatcher import EventDispatcher
from sentinel.storage.event_repository import EventRepository
from sentinel.storage.event_projections import EventProjections, METRICS_SUBSCRIBER_ID
from sentinel.storage.task_repository import TaskRepository
from tests import test_repository
from tests.test_events import RecordingSubscriber


class TransactionIntegrationTests(unittest.TestCase):
    setUp = test_repository.RepositoryTests.setUp
    reopen = test_repository.RepositoryTests.reopen
    event = test_repository.RepositoryTests.event
    result = test_repository.RepositoryTests.result

    def test_finish_and_multi_task_recovery_rollback_after_task_write(self):
        events = EventRepository(self.repo.database)
        tasks = [self.repo.submit(self.event()) for _ in range(2)]
        running = [self.repo.claim("w") for _ in tasks]
        before = self.repo.snapshot(), events.list_events(after_sequence=0)
        with patch(
            "sentinel.storage.task_repository.insert_event",
            side_effect=sqlite3.OperationalError("disk"),
        ):
            with self.assertRaises(sqlite3.OperationalError):
                self.repo.finish(running[0].task_id, self.result(running[0]))
        self.assertEqual(
            (self.repo.snapshot(), events.list_events(after_sequence=0)), before
        )
        self.now += timedelta(seconds=11)
        from sentinel.storage.event_repository import insert_event

        calls = 0

        def fail_second(connection, event):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise sqlite3.OperationalError("disk")
            return insert_event(connection, event)

        with patch("sentinel.storage.task_repository.insert_event", fail_second):
            with self.assertRaises(sqlite3.OperationalError):
                self.repo.recover_expired()
        self.assertEqual(
            (self.repo.snapshot(), events.list_events(after_sequence=0)), before
        )

    def test_upgrade_existing_task_table_without_synthetic_history(self):
        original = self.repo.submit(self.event())
        with self.repo.database.connection() as connection:
            schema = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='tasks'"
            ).fetchone()[0]
            row = connection.execute("SELECT * FROM tasks").fetchone()
        legacy_path = str(Path(self.directory.name) / "legacy.db")
        with closing(sqlite3.connect(legacy_path)) as connection, connection:
            connection.execute(schema)
            connection.execute(
                "INSERT INTO tasks VALUES (" + ",".join("?" for _ in row) + ")",
                tuple(row),
            )
        upgraded = TaskRepository(legacy_path, clock=lambda: self.now)
        upgraded.initialize()
        upgraded.initialize()
        self.assertEqual(upgraded.get(original.task_id), original)
        history = EventRepository(upgraded.database)
        self.assertEqual(history.list_events(), [])
        upgraded.claim("w")
        self.assertEqual(
            [e.event_type.value for e in history.list_events()], ["task.started"]
        )

    def test_rebuild_readers_see_committed_snapshot_and_handler_reads_fresh_receipts(
        self,
    ):
        first = self.repo.submit(self.event())
        projections = EventProjections(self.repo.database)
        baseline = projections.rebuild_metrics()
        second = self.repo.submit(self.event())
        self.assertNotEqual(first.task_id, second.task_id)
        fresh = EventRepository(self.repo.database).list_events(task_id=second.task_id)[
            0
        ]
        entered, release, handler_entered = (
            threading.Event(),
            threading.Event(),
            threading.Event(),
        )
        original = projections._apply_metrics

        def pause_rebuild(connection, event):
            if not entered.is_set():
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("test did not release rebuild")
            original(connection, event)

        def handler():
            handler_entered.set()
            projections.apply_metrics(fresh)

        with patch.object(projections, "_apply_metrics", pause_rebuild):
            with ThreadPoolExecutor(max_workers=2) as pool:
                rebuild = pool.submit(projections.rebuild_metrics)
                try:
                    self.assertTrue(entered.wait(1))
                    self.assertEqual(projections.metrics_snapshot(), baseline)
                    handled = pool.submit(handler)
                    self.assertTrue(handler_entered.wait(1))
                    self.assertFalse(handled.done())
                finally:
                    release.set()
                self.assertEqual(rebuild.result()["created"], 2)
                handled.result()
        self.assertEqual(projections.metrics_snapshot()["created"], 2)

    def test_rejected_operations_append_nothing(self):
        self.repo.submit(self.event())
        task = self.repo.claim("w")
        events = EventRepository(self.repo.database)
        baseline = events.list_events()
        for changes in ({"worker_id": "wrong"},):
            with self.assertRaises(ValueError):
                self.repo.finish(task.task_id, self.result(task, **changes))
        self.now += timedelta(seconds=10)
        with self.assertRaises(ValueError):
            self.repo.finish(task.task_id, self.result(task))
        self.assertEqual(events.list_events(), baseline)


class DeliveryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_metrics_does_not_block_audit_restart_and_ack_replay(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = TaskRepository(str(Path(directory) / "events.db"))
            repo.initialize()
            from sentinel.common.tasks import SecurityEvent

            repo.submit(
                SecurityEvent(source="p", event_type="login_success", payload={})
            )
            events = EventRepository(repo.database)
            projections = EventProjections(repo.database)
            bus = EventBus()
            bus.register(RecordingSubscriber(METRICS_SUBSCRIBER_ID, True))
            bus.register(AuditSubscriber(projections))
            with self.assertLogs("sentinel.core.event_bus", level="ERROR"):
                await EventDispatcher(events, bus).dispatch_once()
            self.assertEqual(len(events.pending(METRICS_SUBSCRIBER_ID)), 2)
            self.assertEqual(events.pending("audit.v1"), [])
            self.assertEqual(projections.metrics_snapshot()["created"], 0)
            restarted = EventBus()
            restarted.register(MetricsSubscriber(projections))
            restarted.register(AuditSubscriber(projections))
            dispatcher = EventDispatcher(events, restarted)
            with patch.object(
                events,
                "record_delivery",
                side_effect=sqlite3.OperationalError("ack lost"),
            ):
                with self.assertRaises(sqlite3.OperationalError):
                    await dispatcher.dispatch_once()
            self.assertEqual(projections.metrics_snapshot()["created"], 1)
            await dispatcher.dispatch_once()
            self.assertEqual(projections.metrics_snapshot()["created"], 1)
            self.assertEqual(events.pending(METRICS_SUBSCRIBER_ID), [])

    async def test_cancellation_joins_inflight_ack_and_leaves_later_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = TaskRepository(str(Path(directory) / "events.db"))
            repo.initialize()
            from sentinel.common.tasks import SecurityEvent

            repo.submit(
                SecurityEvent(source="p", event_type="login_success", payload={})
            )
            events = EventRepository(repo.database)
            bus = EventBus()
            bus.register(RecordingSubscriber())
            entered, release = threading.Event(), threading.Event()
            original = events.record_delivery

            def delayed(*args, **kwargs):
                entered.set()
                release.wait(2)
                original(*args, **kwargs)

            with patch.object(events, "record_delivery", delayed):
                task = asyncio.create_task(EventDispatcher(events, bus).dispatch_once())
                self.assertTrue(await asyncio.to_thread(entered.wait, 1))
                task.cancel()
                await asyncio.sleep(0.005)
                self.assertFalse(task.done())
                release.set()
                with self.assertRaises(asyncio.CancelledError):
                    await task
            self.assertEqual(
                [e.event_type.value for e in events.pending("recording")],
                ["task.ready"],
            )
