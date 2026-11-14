"""Actionable domain/storage failures, independent of HTTP."""

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager


class DomainError(Exception):
    def __init__(self, code: str, message: str, next_action: str, status: int = 409):
        super().__init__(message)
        self.code = code
        self.next_action = next_action
        self.status = status


def storage_unavailable() -> DomainError:
    return DomainError(
        "storage_unavailable",
        "Task storage is unavailable",
        "Check database permissions, disk space and locks; retry with backoff.",
        503,
    )


@contextmanager
def application_errors(resource: str) -> Iterator[None]:
    """Normalize expected implementation failures at the application boundary."""
    try:
        yield
    except KeyError as error:
        raise DomainError(
            "not_found",
            f"Unknown {resource} ID",
            "Check the resource ID or register the service first.",
            404,
        ) from error
    except ValueError as error:
        raise DomainError(
            "conflict",
            str(error),
            "Refresh task state; use the current lease or a new event ID.",
        ) from error
    except sqlite3.Error as error:
        raise storage_unavailable() from error


class StorageCorruptionError(DomainError):
    def __init__(self, task_id: str):
        super().__init__(
            "storage_corruption",
            f"Stored task {task_id} cannot be decoded",
            "Stop central, back up the database, and restore or repair this task before retrying.",
            status=503,
        )
