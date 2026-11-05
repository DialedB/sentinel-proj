"""Reusable async central-server transport; recovery belongs to the service."""

from types import TracebackType
from uuid import UUID

import httpx

from sentinel.common.tasks import SecurityEvent, Task, TaskResult
from sentinel.common.transport import Registration


class ServiceHTTPClient:
    def __init__(self, server_url: str, timeout: float) -> None:
        self._client = httpx.AsyncClient(
            base_url=server_url.rstrip("/") + "/",
            timeout=timeout,
        )

    async def __aenter__(self) -> "ServiceHTTPClient":
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def submit_event(self, event: SecurityEvent) -> Task:
        response = await self._client.post("events", json=event.model_dump(mode="json"))
        response.raise_for_status()
        return Task.model_validate(response.json())

    async def claim_task(self, worker_id: str) -> Task | None:
        response = await self._client.get("tasks/next", params={"worker_id": worker_id})
        response.raise_for_status()
        return (
            None
            if response.status_code == 204
            else Task.model_validate(response.json())
        )

    async def submit_result(self, task_id: UUID, result: TaskResult) -> Task:
        response = await self._client.post(
            f"tasks/{task_id}/result", json=result.model_dump(mode="json")
        )
        response.raise_for_status()
        return Task.model_validate(response.json())

    async def register(self, registration: Registration) -> None:
        response = await self._client.post(
            "register",
            json=registration.model_dump(mode="json"),
        )
        response.raise_for_status()

    async def heartbeat(self, service_id: str) -> None:
        response = await self._client.post(
            "heartbeat",
            json={"service_id": service_id},
        )
        response.raise_for_status()
