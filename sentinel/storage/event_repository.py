"""Append-only lifecycle log. Task writers supply their existing transaction."""

import sqlite3
from uuid import UUID

from sentinel.core.events import LifecycleMetadata, TaskLifecycleEvent, TaskEventType
from sentinel.common.tasks import utcnow
from sentinel.storage.database import Database


def insert_event(
    connection: sqlite3.Connection, event: TaskLifecycleEvent
) -> TaskLifecycleEvent:
    cursor = connection.execute(
        "INSERT INTO task_events(event_id, task_id, event_type, timestamp, payload) "
        "VALUES (?, ?, ?, ?, ?)",
        (
            str(event.event_id),
            str(event.task_id),
            event.event_type.value,
            event.timestamp.isoformat(),
            event.payload.model_dump_json(),
        ),
    )
    return event.model_copy(update={"sequence": cursor.lastrowid})


def load_event(row: sqlite3.Row) -> TaskLifecycleEvent:
    return TaskLifecycleEvent(
        sequence=row["sequence"],
        event_id=row["event_id"],
        task_id=row["task_id"],
        event_type=row["event_type"],
        timestamp=row["timestamp"],
        payload=LifecycleMetadata.model_validate_json(row["payload"]),
    )


class EventRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def list_events(
        self,
        *,
        limit: int = 50,
        after_sequence: int | None = None,
        task_id: UUID | None = None,
    ) -> list[TaskLifecycleEvent]:
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        if after_sequence is not None and (
            type(after_sequence) is not int or not 0 <= after_sequence <= 2**63 - 1
        ):
            raise ValueError(
                "after_sequence must be a nonnegative signed 64-bit integer"
            )
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
            events = [
                load_event(row)
                for row in connection.execute(
                    f"SELECT * FROM task_events{where} ORDER BY sequence {order} LIMIT ?",
                    (*params, limit),
                )
            ]
        return events if after_sequence is not None else list(reversed(events))

    def seed_deliveries(
        self,
        targets: dict[str, frozenset[TaskEventType | None]],
    ) -> None:
        # The scan and insert share one writer snapshot. Later appends are picked up
        # next pass, including events committed while the server was stopped.
        with self.database.connection(write=True) as connection:
            for identity, types in targets.items():
                if not types:
                    continue
                params = tuple(kind.value for kind in types if kind is not None)
                where = (
                    ""
                    if None in types
                    else (
                        " WHERE event_type IN (" + ",".join("?" for _ in params) + ")"
                    )
                )
                connection.execute(
                    "INSERT OR IGNORE INTO event_deliveries(event_id, subscriber_id) "
                    "SELECT event_id, ? FROM task_events" + where,
                    (identity,) if None in types else (identity, *params),
                )

    def pending(
        self, subscriber_id: str, *, limit: int = 100
    ) -> list[TaskLifecycleEvent]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("batch limit must be between 1 and 100")
        with self.database.connection() as connection:
            return [
                load_event(row)
                for row in connection.execute(
                    "SELECT e.* FROM task_events e JOIN event_deliveries d "
                    "ON e.event_id=d.event_id WHERE d.subscriber_id=? "
                    "AND d.delivered_at IS NULL ORDER BY e.sequence LIMIT ?",
                    (subscriber_id, limit),
                )
            ]

    def record_delivery(
        self,
        event_id: UUID,
        subscriber_id: str,
        *,
        success: bool,
        error: str | None = None,
    ) -> None:
        with self.database.connection(write=True) as connection:
            connection.execute(
                "UPDATE event_deliveries SET attempts=attempts+1, last_error=?, delivered_at=? "
                "WHERE event_id=? AND subscriber_id=? AND delivered_at IS NULL",
                (
                    None if success else error,
                    utcnow().isoformat() if success else None,
                    str(event_id),
                    subscriber_id,
                ),
            )
