"""Projection idempotency, rebuild, writer fencing and observer persistence."""

import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from sentinel.storage.database import Database
from sentinel.storage.event_repository import EventRepository, insert_event
from sentinel.storage.event_projections import EventProjections, METRICS_SUBSCRIBER_ID
from sentinel.observers.metrics import MetricsSubscriber
from tests.test_events import event


class ProjectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = Database(str(Path(directory.name) / "events.db"))
        self.db.initialize()
        self.projections = EventProjections(self.db)
        initial = event()
        self.events = []
        for kind, attempt in (("created", 0), ("ready", 0), ("started", 1),
                              ("requeued", 1), ("started", 2), ("completed", 2),
                              ("failed", 3)):
            metadata = initial.payload.model_copy(update={"attempts": attempt})
            with self.db.connection(write=True) as connection:
                self.events.append(insert_event(connection, event(
                    task_id=initial.task_id, event_type=f"task.{kind}", payload=metadata)))
        self.expected = dict(created=1, started=2, completed=1, permanently_failed=1,
                             requeued=1, retry_attempts=1)

    async def test_subscriber_replay_restart_and_rebuild(self):
        subscriber = MetricsSubscriber(self.projections)
        for item in self.events * 2:
            await subscriber.handle(item)
        self.assertEqual(self.projections.metrics_snapshot(), self.expected)
        reopened = EventProjections(Database(self.db.path))
        self.assertEqual(reopened.metrics_snapshot(), self.expected)
        self.assertEqual(reopened.rebuild_metrics(), self.expected)
        for item in self.events:
            reopened.apply_metrics(item)
        self.assertEqual(reopened.rebuild_metrics(), self.expected)
        self.assertEqual(reopened.metrics_snapshot(), self.expected)

    def test_concurrent_rebuild_handlers_and_atomic_rollback(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(self.projections.apply_metrics, item)
                       for item in self.events * 3]
            futures += [pool.submit(self.projections.rebuild_metrics) for _ in range(4)]
            for future in futures:
                future.result()
        self.assertEqual(self.projections.metrics_snapshot(), self.expected)
        # Failure after receipt insertion must roll it back with the projection.
        fresh = event()
        with self.db.connection(write=True) as connection:
            fresh = insert_event(connection, fresh)
        original = self.projections._receipt
        def fail_after_receipt(connection, event_id, identity):
            original(connection, event_id, identity)
            raise sqlite3.OperationalError("projection failed")
        with patch.object(self.projections, "_receipt", fail_after_receipt):
            with self.assertRaises(sqlite3.OperationalError):
                self.projections.apply_metrics(fresh)
        self.assertEqual(self.projections.metrics_snapshot(), self.expected)
        self.projections.apply_metrics(fresh)
        self.assertEqual(self.projections.metrics_snapshot()["created"], 2)

    def test_rebuild_leaves_delivery_acknowledgements_unchanged(self):
        repo = EventRepository(self.db)
        repo.seed_deliveries({METRICS_SUBSCRIBER_ID: frozenset({None})})
        repo.record_delivery(self.events[0].event_id, METRICS_SUBSCRIBER_ID, success=True)
        before = repo.pending(METRICS_SUBSCRIBER_ID)
        self.projections.rebuild_metrics()
        self.assertEqual(repo.pending(METRICS_SUBSCRIBER_ID), before)
