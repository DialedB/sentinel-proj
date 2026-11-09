"""Durable atomic queue operations. SQL is confined to the storage package."""

import json
import math
import sqlite3
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import UUID

from sentinel.common.tasks import SecurityEvent, Success, Task, TaskResult, TaskState, utcnow
from sentinel.storage.database import Database


def canonical(value: object) -> str:
    # JSON preserves bool/int/float distinctions, unlike Python dictionary equality.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class TaskRepository:
    def __init__(
        self, path: str, *, lease_seconds: float = 30, max_attempts: int = 3,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        if (isinstance(lease_seconds, bool) or not isinstance(lease_seconds, (int, float))
                or not math.isfinite(lease_seconds) or lease_seconds <= 0):
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
        return now.astimezone(timezone.utc)

    def initialize(self) -> None:
        self.database.initialize()

    @staticmethod
    def _load(row: sqlite3.Row | None) -> Task:
        if row is None:
            raise KeyError("Unknown task ID")
        return Task.model_validate_json(row["task_json"])

    @staticmethod
    def _values(task: Task) -> dict[str, object]:
        return dict(
            task_id=str(task.task_id), event_id=str(task.event.event_id),
            task_type=task.command, payload=canonical(task.event.payload),
            event_json=canonical(task.event.model_dump(mode="json")),
            task_json=task.model_dump_json(), status=task.state.value,
            worker_id=task.worker_id, attempts=task.attempts, max_attempts=task.max_attempts,
            lease_token=str(task.lease_token) if task.lease_token else None,
            lease_expires_at=task.lease_expires_at.isoformat() if task.lease_expires_at else None,
            created_at=task.created_at.isoformat(), updated_at=task.updated_at.isoformat(),
            result=canonical(task.outcome.result) if isinstance(task.outcome, Success) else None,
            error=task.error, recovery_count=task.recovery_count,
        )

    def _save(
        self, connection: sqlite3.Connection, task: Task, *, insert: bool = False,
    ) -> Task:
        values = self._values(task)
        if insert:
            columns = ", ".join(values)
            params = ", ".join(":" + key for key in values)
            connection.execute(f"INSERT INTO tasks ({columns}) VALUES ({params})", values)
        else:
            assignments = ", ".join(f"{key}=:{key}" for key in values if key != "task_id")
            connection.execute(f"UPDATE tasks SET {assignments} WHERE task_id=:task_id", values)
        return task

    def submit(self, event: SecurityEvent) -> Task:
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
            task = Task(event=event.model_copy(deep=True), max_attempts=self.max_attempts,
                        created_at=now, updated_at=now).ready(now)
            return self._save(connection, task, insert=True)

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
            return self._save(connection, task)

    def finish(self, task_id: UUID, result: TaskResult) -> Task:
        with self.database.connection(write=True) as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE task_id=?", (str(task_id),)
            ).fetchone()
            task = self._load(row).finish(result, self._now())
            return self._save(connection, task)

    def recover_expired(self) -> int:
        with self.database.connection(write=True) as connection:
            now = self._now()
            rows = connection.execute(
                "SELECT * FROM tasks WHERE status='RUNNING' AND lease_expires_at<=?",
                (now.isoformat(),),
            ).fetchall()
            for row in rows:
                self._save(connection, self._load(row).recover_expired(now))
            return len(rows)

    def get(self, task_id: UUID) -> Task:
        with self.database.connection() as connection:
            return self._load(connection.execute(
                "SELECT * FROM tasks WHERE task_id=?", (str(task_id),)
            ).fetchone())

    def snapshot(self) -> list[Task]:
        with self.database.connection() as connection:
            return [self._load(row) for row in connection.execute(
                "SELECT * FROM tasks ORDER BY created_at, sequence"
            )]

    def stats(self) -> dict[str, int]:
        with self.database.connection() as connection:
            # A single statement gives a consistent snapshot even with concurrent writers.
            rows = connection.execute("""
                SELECT status, COUNT(*) AS count, SUM(attempts) AS attempts,
                       SUM(MAX(attempts-1, 0)) AS retries,
                       SUM(recovery_count) AS recoveries,
                       SUM(CASE WHEN status='RUNNING' AND lease_expires_at<=?
                           THEN 1 ELSE 0 END) AS expired
                FROM tasks GROUP BY status
            """, (self._now().isoformat(),)).fetchall()
        stats = {state.value.lower(): 0 for state in TaskState}
        stats.update(total=0, attempts=0, retry_attempts=0, recovery_count=0, expired_active=0)
        for row in rows:
            stats[row["status"].lower()] = row["count"]
            stats["total"] += row["count"]
            stats["attempts"] += row["attempts"]
            stats["retry_attempts"] += row["retries"]
            stats["recovery_count"] += row["recoveries"]
            stats["expired_active"] += row["expired"]
        stats["queue_depth"] = stats["ready"]
        return stats
