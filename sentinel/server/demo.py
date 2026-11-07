"""Read-only demo routes; watchdog requests use a fixed configured destination."""

import asyncio
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import TypeAdapter, ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.watchdog.models import CheckResult

_STATUS = TypeAdapter(list[CheckResult])
_PAGE = Path(__file__).with_name("demo.html")


def install_demo_routes(app: FastAPI, config: ServiceConfig) -> None:
    @app.get("/", include_in_schema=False)
    async def demo() -> FileResponse:
        return FileResponse(_PAGE, media_type="text/html")

    @app.get("/demo/status", response_model=list[CheckResult])
    async def status() -> list[CheckResult]:
        try:
            async with asyncio.timeout(config.http_timeout):
                response = await app.state.demo_client.get(
                    config.watchdog_url.rstrip("/") + "/status",
                )
                response.raise_for_status()
                return _STATUS.validate_json(response.content)
        except (httpx.HTTPError, TimeoutError, ValidationError) as error:
            raise HTTPException(
                503, "Watchdog unavailable: status could not be retrieved",
            ) from error
