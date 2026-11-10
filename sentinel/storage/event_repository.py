"""Append-only lifecycle log. Task writers supply their existing transaction."""

import sqlite3
from uuid import UUID

from sentinel.core.events import LifecycleMetadata, TaskLifecycleEvent
from sentinel.storage.database import Database


def insert_event(connection: sqlite3.Connection, event: TaskLifecycleEvent) -> TaskLifecycleEvent:
    cursor = connection.execute(
        "INSERT INTO task_events(event_id, task_id, event_type, timestamp, payload) "
        "VALUES (?, ?, ?, ?, ?)",
        (str(event.event_id), str(event.task_id), event.event_type.value,
         event.timestamp.isoformat(), event.payload.model_dump_json()),
    )
    return event.model_copy(update={"sequence": cursor.lastrowid})


def load_event(row: sqlite3.Row) -> TaskLifecycleEvent:
    return TaskLifecycleEvent(
        sequence=row["sequence"], event_id=row["event_id"], task_id=row["task_id"],
        event_type=row["event_type"], timestamp=row["timestamp"],
        payload=LifecycleMetadata.model_validate_json(row["payload"]),
    )


class EventRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def list_events(
        self, *, limit: int = 50, after_sequence: int | None = None,
        task_id: UUID | None = None,
    ) -> list[TaskLifecycleEvent]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        if after_sequence is not None and (
            type(after_sequence) is not int or after_sequence < 0
        ):
            raise ValueError("after_sequence must be nonnegative")
        clauses, params = [], []
        if task_id is not None:
            clauses.append("task_id=?")
            params.append(str(task_id))
        if after_sequence is not None:
            clauses.append("sequence>?")
            params.append(after_sequence)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        order = "ASC" if after_sequence is not None else "DESC"
        with self.database.connection() as connection:
            events = [load_event(row) for row in connection.execute(
                f"SELECT * FROM task_events{where} ORDER BY sequence {order} LIMIT ?",
                (*params, limit),
            )]
        return events if after_sequence is not None else list(reversed(events))
