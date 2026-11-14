"""Remote Proxy for ServiceGateway; owns serialization, deadlines and connections."""

from types import TracebackType
from uuid import UUID

import httpx

from sentinel.common.errors import DomainError
from sentinel.common.gateway import ServiceGateway
from sentinel.common.tasks import SecurityEvent, Task, TaskResult
from sentinel.common.transport import Registration


def check_response(response: httpx.Response) -> None:
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as error:
        # Only the structured public error is part of the application contract.
        # Proxies and upstream servers may instead return HTML or arbitrary JSON.
        try:
            body = response.json()
        except ValueError:
            body = None
        detail = body.get("error") if isinstance(body, dict) else None
        if isinstance(detail, dict) and all(
            isinstance(detail.get(key), str) and detail[key].strip()
            for key in ("code", "message", "next_action")
        ):
            raise DomainError(
                detail["code"],
                detail["message"],
                detail["next_action"],
                response.status_code,
            ) from error
        raise DomainError(
            "gateway_error",
            "Central rejected the request",
            "Check service health and the request; retry transient failures with backoff.",
            response.status_code,
        ) from error


class ServiceHTTPClient(ServiceGateway):
    def __init__(self, server_url: str, timeout: float) -> None:
        self._client = httpx.AsyncClient(
            base_url=server_url.rstrip("/") + "/",
            timeout=timeout,
        )

    async def __aenter__(self) -> ServiceHTTPClient:
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
        check_response(response)
        return Task.model_validate(response.json())

    async def claim_task(self, worker_id: str) -> Task | None:
        response = await self._client.get("tasks/next", params={"worker_id": worker_id})
        check_response(response)
        return (
            None
            if response.status_code == 204
            else Task.model_validate(response.json())
        )

    async def submit_result(self, task_id: UUID, result: TaskResult) -> Task:
        response = await self._client.post(
            f"tasks/{task_id}/result", json=result.model_dump(mode="json")
        )
        check_response(response)
        return Task.model_validate(response.json())

    async def register(self, registration: Registration) -> None:
        response = await self._client.post(
            "register",
            json=registration.model_dump(mode="json"),
        )
        check_response(response)

    async def heartbeat(self, service_id: str) -> None:
        response = await self._client.post(
            "heartbeat",
            json={"service_id": service_id},
        )
        check_response(response)
