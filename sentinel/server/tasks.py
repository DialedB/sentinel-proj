"""Compatibility import: there is only one queue implementation and source of truth.

Callers supply a database path and initialize it before use. Server lifespan owns
initialization in production; direct callers/tests initialize explicitly.
"""

from sentinel.storage.task_repository import TaskRepository

TaskQueue = TaskRepository
