"""Durable atomic queue operations. SQL is confined to the storage package."""

import json
import math
import sqlite3
from collections.abc import Callable
from datetime import datetime, UTC
from uuid import UUID

from pydantic import ValidationError

from sentinel.common.errors import StorageCorruptionError
from sentinel.common.tasks import (
    SecurityEvent,
    Success,
    Task,
    TaskResult,
    TaskState,
    utcnow,
)
from sentinel.storage.database import Database
from sentinel.core.events import LifecycleMetadata, TaskEventType, TaskLifecycleEvent
from sentinel.storage.event_repository import insert_event


def canonical(value: object) -> str:
    # JSON preserves bool/int/float distinctions, unlike Python dictionary equality.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class TaskRepository:
    def __init__(
        self,
        path: str,
        *,
        lease_seconds: float = 30,
        max_attempts: int = 3,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be finite and positive")
        if type(max_attempts) is not int or max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        self.database = Database(path)
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.clock = clock

    def _now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return now.astimezone(UTC)

    def initialize(self) -> None:
        self.database.initialize()

    @staticmethod
    def _load(row: sqlite3.Row | None) -> Task:
        if row is None:
            raise KeyError("Unknown task ID")
        try:
            return Task.model_validate_json(row["task_json"])
        except ValidationError as error:
            raise StorageCorruptionError(row["task_id"]) from error

    @staticmethod
    def _values(task: Task) -> dict[str, object]:
        # Frozen models can still contain mutated nested dictionaries. Validate the
        # exact persisted representation before any task or outbox row is written.
        task = Task.model_validate(task.model_dump())
        task = Task.model_validate_json(task.model_dump_json())
        return dict(
            task_id=str(task.task_id),
            event_id=str(task.event.event_id),
            task_type=task.command,
            payload=canonical(task.event.payload),
            event_json=canonical(task.event.model_dump(mode="json")),
            task_json=task.model_dump_json(),
            status=task.state.value,
            worker_id=task.worker_id,
            attempts=task.attempts,
            max_attempts=task.max_attempts,
            lease_token=str(task.lease_token) if task.lease_token else None,
            lease_expires_at=task.lease_expires_at.isoformat()
            if task.lease_expires_at
            else None,
            created_at=task.created_at.isoformat(),
            updated_at=task.updated_at.isoformat(),
            result=canonical(task.outcome.result)
            if isinstance(task.outcome, Success)
            else None,
            error=task.error,
            recovery_count=task.recovery_count,
        )

    def _save(
        self,
        connection: sqlite3.Connection,
        task: Task,
        *,
        insert: bool = False,
        event_type: TaskEventType,
        reason: str,
        previous: Task | None = None,
    ) -> Task:
        values = self._values(task)
        if insert:
            columns = ", ".join(values)
            params = ", ".join(":" + key for key in values)
            connection.execute(
                f"INSERT INTO tasks ({columns}) VALUES ({params})", values
            )
        else:
            assignments = ", ".join(
                f"{key}=:{key}" for key in values if key != "task_id"
            )
            connection.execute(
                f"UPDATE tasks SET {assignments} WHERE task_id=:task_id", values
            )
        metadata = LifecycleMetadata(
            security_event_id=task.event.event_id,
            source=task.event.source,
            command=task.command,
            worker_id=(previous.worker_id if previous else task.worker_id),
            attempts=task.attempts,
            max_attempts=task.max_attempts,
            recovery_count=task.recovery_count,
            reason=reason,
            error=task.error,
            lease_expires_at=task.lease_expires_at,
        )
        types = (
            (TaskEventType.CREATED, TaskEventType.READY) if insert else (event_type,)
        )
        for kind in types:
            insert_event(
                connection,
                TaskLifecycleEvent(
                    task_id=task.task_id,
                    event_type=kind,
                    timestamp=task.updated_at,
                    payload=metadata,
                ),
            )
        return task

    def submit(self, event: SecurityEvent) -> Task:
        event = SecurityEvent.model_validate(event.model_dump())
        event_json = canonical(event.model_dump(mode="json"))
        with self.database.connection(write=True) as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE event_id=?", (str(event.event_id),)
            ).fetchone()
            if row is not None:
                if row["event_json"] != event_json:
                    raise ValueError("Event ID already exists with different content")
                return self._load(row)
            now = self._now()
            task = Task(
                event=event.model_copy(deep=True),
                max_attempts=self.max_attempts,
                created_at=now,
                updated_at=now,
            ).ready(now)
            return self._save(
                connection,
                task,
                insert=True,
                event_type=TaskEventType.READY,
                reason="submitted",
            )

    def claim(self, worker_id: str) -> Task | None:
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        with self.database.connection(write=True) as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE status='READY' ORDER BY created_at, sequence LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            task = self._load(row).claim(worker_id, self.lease_seconds, self._now())
            return self._save(
                connection, task, event_type=TaskEventType.STARTED, reason="claimed"
            )

    def finish(self, task_id: UUID, result: TaskResult) -> Task:
        with self.database.connection(write=True) as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?", (str(task_id),)
            ).fetchone()
            previous = self._load(row)
            task = previous.finish(result, self._now())
            return self._save(
                connection,
                task,
                previous=previous,
                event_type=self._finished_type(task),
                reason="completed"
                if task.state == TaskState.COMPLETED
                else "execution_failed",
            )

    @staticmethod
    def _finished_type(task: Task) -> TaskEventType:
        return {
            TaskState.COMPLETED: TaskEventType.COMPLETED,
            TaskState.READY: TaskEventType.REQUEUED,
            TaskState.FAILED: TaskEventType.FAILED,
        }[task.state]

    def recover_expired(self) -> int:
        with self.database.connection(write=True) as connection:
            now = self._now()
            rows = connection.execute(
                "SELECT * FROM tasks WHERE status='RUNNING' AND lease_expires_at<=?",
                (now.isoformat(),),
            ).fetchall()
            for row in rows:
                previous = self._load(row)
                task = previous.recover_expired(now)
                self._save(
                    connection,
                    task,
                    previous=previous,
                    event_type=self._finished_type(task),
                    reason="lease_expired",
                )
            return len(rows)

    def get(self, task_id: UUID) -> Task:
        with self.database.connection() as connection:
            return self._load(
                connection.execute(
                    "SELECT * FROM tasks WHERE task_id=?", (str(task_id),)
                ).fetchone()
            )

    def snapshot(self) -> list[Task]:
        with self.database.connection() as connection:
            return [
                self._load(row)
                for row in connection.execute(
                    "SELECT * FROM tasks ORDER BY created_at, sequence"
                )
            ]

    def page(
        self,
        *,
        limit: int = 50,
        after_task_id: UUID | None = None,
        latest: bool = False,
    ) -> list[Task]:
        """Forward insertion-order pages, optionally starting from the latest window."""
        if type(limit) is not int or not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        with self.database.connection() as connection:
            if after_task_id is None:
                order = "DESC" if latest else "ASC"
                rows = connection.execute(
                    f"SELECT * FROM tasks ORDER BY sequence {order} LIMIT ?",
                    (limit,),
                ).fetchall()
                return [self._load(row) for row in (reversed(rows) if latest else rows)]
            cursor = connection.execute(
                "SELECT sequence FROM tasks WHERE task_id=?",
                (str(after_task_id),),
            ).fetchone()
            if cursor is None:
                raise KeyError("Unknown task cursor")
            return [
                self._load(row)
                for row in connection.execute(
                    "SELECT * FROM tasks WHERE sequence>? ORDER BY sequence LIMIT ?",
                    (cursor["sequence"], limit),
                )
            ]

    def stats(self) -> dict[str, int]:
        with self.database.connection() as connection:
            # A single statement gives a consistent snapshot even with concurrent writers.
            rows = connection.execute(
                """
                SELECT status, COUNT(*) AS count, SUM(attempts) AS attempts,
                       SUM(MAX(attempts-1, 0)) AS retries,
                       SUM(recovery_count) AS recoveries,
                       SUM(CASE WHEN status='RUNNING' AND lease_expires_at<=?
                           THEN 1 ELSE 0 END) AS expired
                FROM tasks GROUP BY status
            """,
                (self._now().isoformat(),),
            ).fetchall()
        stats = {state.value.lower(): 0 for state in TaskState}
        stats.update(
            total=0, attempts=0, retry_attempts=0, recovery_count=0, expired_active=0
        )
        for row in rows:
            stats[row["status"].lower()] = row["count"]
            stats["total"] += row["count"]
            stats["attempts"] += row["attempts"]
            stats["retry_attempts"] += row["retries"]
            stats["recovery_count"] += row["recoveries"]
            stats["expired_active"] += row["expired"]
        stats["queue_depth"] = stats["ready"]
        return stats
