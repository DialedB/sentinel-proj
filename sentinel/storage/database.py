"""Short-lived SQLite connections; construction performs no filesystem I/O."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator


class Database:
    def __init__(self, path: str) -> None:
        if not isinstance(path, str) or not path.strip() or path == ":memory:":
            raise ValueError("database path must name a file")
        self.path = path

    def initialize(self) -> None:
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL UNIQUE,
                    event_id TEXT NOT NULL UNIQUE,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    event_json TEXT NOT NULL,
                    task_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    worker_id TEXT,
                    attempts INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    recovery_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS task_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    task_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    payload TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS task_events_task
                    ON task_events(task_id, sequence);
                CREATE TRIGGER IF NOT EXISTS task_events_no_update
                    BEFORE UPDATE ON task_events BEGIN
                    SELECT RAISE(ABORT, 'task_events is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS task_events_no_delete
                    BEFORE DELETE ON task_events BEGIN
                    SELECT RAISE(ABORT, 'task_events is append-only'); END;
                CREATE INDEX IF NOT EXISTS tasks_ready
                    ON tasks(status, created_at, sequence);
                CREATE INDEX IF NOT EXISTS tasks_expiry
                    ON tasks(status, lease_expires_at);
            """)

    @contextmanager
    def connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=5000")
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if write:
                connection.rollback()
            raise
        finally:
            connection.close()
