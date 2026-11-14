"""Task HTTP adapter; registration is identity validation, not authentication."""

from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Response

from sentinel.common.tasks import SecurityEvent, Task, TaskResult
from sentinel.server.gateway import LocalServiceGateway
from sentinel.server.registry import ServiceRegistry
from sentinel.storage.task_repository import TaskRepository


def install_task_routes(
    app: FastAPI, queue: TaskRepository, registry: ServiceRegistry
) -> None:
    gateway = LocalServiceGateway(queue, registry)
    app.state.gateway = gateway

    @app.post("/events", response_model=Task, response_model_exclude={"lease_token"})
    async def submit(event: SecurityEvent) -> Task:
        return await gateway.submit_event(event)

    @app.get("/tasks/next", response_model=Task)
    async def claim(
        worker_id: str = Query(min_length=1, pattern=r"\S"),
    ) -> Task | Response:
        task = await gateway.claim_task(worker_id)
        return task if task is not None else Response(status_code=204)

    @app.get(
        "/tasks",
        response_model=list[Task],
        response_model_exclude={"__all__": {"lease_token"}},
    )
    def tasks(
        limit: int = Query(default=50, ge=1, le=200),
        after_task_id: UUID | None = None,
        latest: bool = False,
    ) -> list[Task]:
        try:
            return queue.page(limit=limit, after_task_id=after_task_id, latest=latest)
        except KeyError as error:
            raise HTTPException(404, "Unknown task cursor") from error

    @app.get("/stats")
    def stats() -> dict[str, int]:
        return queue.stats()

    @app.get(
        "/tasks/{task_id}", response_model=Task, response_model_exclude={"lease_token"}
    )
    def task(task_id: UUID) -> Task:
        try:
            return queue.get(task_id)
        except KeyError as error:
            raise HTTPException(404, "Unknown task ID") from error

    @app.post(
        "/tasks/{task_id}/result",
        response_model=Task,
        response_model_exclude={"lease_token"},
    )
    async def result(task_id: UUID, result: TaskResult) -> Task:
        return await gateway.submit_result(task_id, result)
