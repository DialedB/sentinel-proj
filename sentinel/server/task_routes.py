"""Task HTTP API; registration is identity validation, not authentication."""

from collections.abc import Callable
from typing import TypeVar
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Response

from sentinel.common.models import ServiceRole
from sentinel.common.tasks import SecurityEvent, Task, TaskResult
from sentinel.server.registry import ServiceRegistry
from sentinel.storage.task_repository import TaskRepository

T = TypeVar("T")


def install_task_routes(
    app: FastAPI,
    queue: TaskRepository,
    registry: ServiceRegistry,
) -> None:
    def require_worker(worker_id: str) -> None:
        records = {record.service_id: record for record in registry.snapshot()}
        if worker_id not in records:
            raise HTTPException(404, "Unknown worker ID")
        if records[worker_id].role != ServiceRole.WORKER:
            raise HTTPException(409, "Registered service is not a worker")

    def operation(call: Callable[[], T]) -> T:
        try:
            return call()
        except KeyError as error:
            raise HTTPException(404, "Unknown task ID") from error
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    @app.post("/events", response_model=Task)
    def submit(event: SecurityEvent) -> Task:
        return operation(lambda: queue.submit(event))

    @app.get("/tasks/next", response_model=Task)
    def claim(
        worker_id: str = Query(min_length=1, pattern=r"\S"),
    ) -> Task | Response:
        require_worker(worker_id)
        task = operation(lambda: queue.claim(worker_id))
        return task if task is not None else Response(status_code=204)

    @app.get("/tasks", response_model=list[Task])
    def tasks() -> list[Task]:
        return queue.snapshot()

    @app.get("/stats")
    def stats() -> dict[str, int]:
        return queue.stats()

    @app.get("/tasks/{task_id}", response_model=Task)
    def task(task_id: UUID) -> Task:
        return operation(lambda: queue.get(task_id))

    @app.post("/tasks/{task_id}/result", response_model=Task)
    def result(task_id: UUID, result: TaskResult) -> Task:
        require_worker(result.worker_id)
        return operation(lambda: queue.finish(task_id, result))
