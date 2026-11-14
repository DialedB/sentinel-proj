"""Transactional, replay-safe event projections; no persistent connections."""

import sqlite3
from uuid import UUID

from sentinel.core.events import TaskEventType, TaskLifecycleEvent
from sentinel.storage.database import Database
from sentinel.storage.event_repository import load_event

METRICS_SUBSCRIBER_ID = "metrics.v1"
AUDIT_SUBSCRIBER_ID = "audit.v1"
METRIC_COLUMNS = (
    "created",
    "started",
    "completed",
    "permanently_failed",
    "requeued",
    "retry_attempts",
)


class EventProjections:
    def __init__(self, database: Database) -> None:
        self.database = database

    @staticmethod
    def _receipt(
        connection: sqlite3.Connection, event_id: UUID, subscriber_id: str
    ) -> bool:
        return (
            connection.execute(
                "INSERT OR IGNORE INTO subscriber_receipts(subscriber_id, event_id) VALUES (?, ?)",
                (subscriber_id, str(event_id)),
            ).rowcount
            == 1
        )

    def receipt(self, event_id: UUID, subscriber_id: str) -> bool:
        with self.database.connection(write=True) as connection:
            return self._receipt(connection, event_id, subscriber_id)

    def _apply_metrics(
        self, connection: sqlite3.Connection, event: TaskLifecycleEvent
    ) -> None:
        if not self._receipt(connection, event.event_id, METRICS_SUBSCRIBER_ID):
            return
        column = {
            TaskEventType.CREATED: "created",
            TaskEventType.STARTED: "started",
            TaskEventType.COMPLETED: "completed",
            TaskEventType.FAILED: "permanently_failed",
            TaskEventType.REQUEUED: "requeued",
        }.get(event.event_type)
        if column is not None:
            connection.execute(
                f"UPDATE event_metrics SET {column}={column}+1 WHERE singleton=1"
            )
        if event.event_type == TaskEventType.STARTED and event.payload.attempts > 1:
            connection.execute(
                "UPDATE event_metrics SET retry_attempts=retry_attempts+1 WHERE singleton=1"
            )

    def apply_metrics(self, event: TaskLifecycleEvent) -> None:
        with self.database.connection(write=True) as connection:
            self._apply_metrics(connection, event)

    def metrics_snapshot(self) -> dict[str, int]:
        with self.database.connection() as connection:
            row = connection.execute(
                "SELECT * FROM event_metrics WHERE singleton=1"
            ).fetchone()
            return {name: row[name] for name in METRIC_COLUMNS}

    def rebuild_metrics(self) -> dict[str, int]:
        # One writer transaction fences concurrent handlers. A handler entering
        # after this commit reads fresh receipts and cannot double-count replay.
        with self.database.connection(write=True) as connection:
            connection.execute(
                "DELETE FROM subscriber_receipts WHERE subscriber_id=?",
                (METRICS_SUBSCRIBER_ID,),
            )
            assignments = ", ".join(f"{name}=0" for name in METRIC_COLUMNS)
            connection.execute(
                f"UPDATE event_metrics SET {assignments} WHERE singleton=1"
            )
            for row in connection.execute(
                "SELECT * FROM task_events ORDER BY sequence"
            ):
                self._apply_metrics(connection, load_event(row))
            row = connection.execute(
                "SELECT * FROM event_metrics WHERE singleton=1"
            ).fetchone()
            return {name: row[name] for name in METRIC_COLUMNS}
