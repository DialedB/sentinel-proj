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
        for statement in ("DELETE FROM task_events", "UPDATE task_events SET task_id='x'"):
            with self.assertRaises(sqlite3.IntegrityError):
                with self.db.connection(write=True) as connection:
                    connection.execute(statement)
        for arguments in ({"limit": 0}, {"limit": 201}, {"after_sequence": -1}):
            with self.assertRaises(ValueError):
                self.repo.list_events(**arguments)
