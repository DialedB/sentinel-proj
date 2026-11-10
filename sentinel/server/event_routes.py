"""Read-only lifecycle history and eventually consistent projection APIs."""

from uuid import UUID

from fastapi import FastAPI, HTTPException, Query

from sentinel.core.events import TaskLifecycleEvent
from sentinel.storage.event_projections import EventProjections
from sentinel.storage.event_repository import EventRepository
from sentinel.storage.task_repository import TaskRepository


def install_event_routes(
    app: FastAPI, events: EventRepository, tasks: TaskRepository,
    projections: EventProjections,
) -> None:
    @app.get("/events/lifecycle", response_model=list[TaskLifecycleEvent])
    def lifecycle(
        limit: int = Query(default=50, ge=1, le=200),
        after_sequence: int | None = Query(default=None, ge=0),
    ) -> list[TaskLifecycleEvent]:
        return events.list_events(limit=limit, after_sequence=after_sequence)

    @app.get("/tasks/{task_id}/events", response_model=list[TaskLifecycleEvent])
    def task_events(
        task_id: UUID, limit: int = Query(default=50, ge=1, le=200),
        after_sequence: int | None = Query(default=None, ge=0),
    ) -> list[TaskLifecycleEvent]:
        try:
            tasks.get(task_id)
        except KeyError as error:
            raise HTTPException(404, "Unknown task ID") from error
        return events.list_events(task_id=task_id, limit=limit, after_sequence=after_sequence)

    @app.get("/metrics/events", response_model=dict[str, int])
    def metrics() -> dict[str, int]:
        return projections.metrics_snapshot()
