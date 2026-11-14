"""SQLite log and outbox durability tests use only temporary databases."""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from sentinel.storage.database import Database
from sentinel.storage.event_repository import EventRepository, insert_event
from tests.test_events import event


class EventRepositoryTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = Database(str(Path(directory.name) / "events.db"))
        self.db.initialize()
        self.repo = EventRepository(self.db)

    def test_append_sequence_restart_pagination_and_rollback(self):
        first = event()
        with self.db.connection(write=True) as connection:
            one = insert_event(connection, first)
            two = insert_event(connection, event(task_id=first.task_id))
        self.db.initialize()
        self.assertEqual(self.repo.list_events(), [one, two])
        self.assertEqual(self.repo.list_events(limit=1), [two])
        self.assertEqual(self.repo.list_events(after_sequence=0, limit=1), [one])
        self.assertEqual(self.repo.list_events(after_sequence=one.sequence), [two])
        self.assertEqual(self.repo.list_events(task_id=first.task_id), [one, two])
        with self.assertRaises(sqlite3.IntegrityError):
            with self.db.connection(write=True) as connection:
                insert_event(connection, event())
                insert_event(connection, first)
        self.assertEqual(self.repo.list_events(), [one, two])
        for statement in (
            "DELETE FROM task_events",
            "UPDATE task_events SET task_id='x'",
        ):
            with self.assertRaises(sqlite3.IntegrityError):
                with self.db.connection(write=True) as connection:
                    connection.execute(statement)
        for arguments in ({"limit": 0}, {"limit": 201}, {"after_sequence": -1}):
            with self.assertRaises(ValueError):
                self.repo.list_events(**arguments)

    def test_independent_ack_failure_restart_and_late_target_backlog(self):
        from sentinel.core.events import TaskEventType

        with self.db.connection(write=True) as connection:
            first = insert_event(connection, event())
            second = insert_event(connection, event(event_type="task.ready"))
        targets = {
            "metrics": frozenset({None}),
            "audit": frozenset({TaskEventType.CREATED}),
        }
        self.repo.seed_deliveries(targets)
        self.repo.seed_deliveries(targets)
        self.assertEqual(self.repo.pending("metrics"), [first, second])
        self.assertEqual(self.repo.pending("audit"), [first])
        self.repo.record_delivery(first.event_id, "audit", success=True)
        self.repo.record_delivery(
            first.event_id, "metrics", success=False, error="offline"
        )
        reopened = EventRepository(Database(self.db.path))
        reopened.seed_deliveries({})
        self.assertEqual(reopened.pending("audit"), [])
        self.assertEqual(reopened.pending("metrics"), [first, second])
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM event_deliveries WHERE subscriber_id='metrics' "
                "AND event_id=?",
                (str(first.event_id),),
            ).fetchone()
        self.assertEqual((row["attempts"], row["last_error"]), (1, "offline"))
        reopened.record_delivery(first.event_id, "metrics", success=True)
        self.assertEqual(reopened.pending("metrics"), [second])
        with self.db.connection(write=True) as connection:
            third = insert_event(connection, event())
        reopened.seed_deliveries({"late": frozenset({None})})
        self.assertEqual(reopened.pending("late"), [first, second, third])
        reopened.seed_deliveries(targets)
        self.assertEqual(reopened.pending("metrics"), [second, third])
