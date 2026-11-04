"""Reusable async central-server transport; recovery belongs to the service."""

from types import TracebackType

import httpx

from sentinel.common.transport import Registration


class ServiceHTTPClient:
    def __init__(self, server_url: str, timeout: float) -> None:
        self._client = httpx.AsyncClient(
            base_url=server_url.rstrip("/") + "/", timeout=timeout,
        )

    async def __aenter__(self) -> "ServiceHTTPClient":
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None,
        exc_value: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        await self._client.__aexit__(exc_type, exc_value, traceback)

    async def register(self, registration: Registration) -> None:
        response = await self._client.post(
            "register", json=registration.model_dump(mode="json"),
        )
        response.raise_for_status()

    async def heartbeat(self, service_id: str) -> None:
        response = await self._client.post(
            "heartbeat", json={"service_id": service_id},
        )
        response.raise_for_status()
