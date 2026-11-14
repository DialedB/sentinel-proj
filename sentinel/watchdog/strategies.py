"""Interchangeable health policies; clients are owned by the service lifespan."""

from abc import ABC, abstractmethod
from datetime import datetime, UTC

import httpx
from pydantic import ValidationError

from sentinel.common.models import ServiceStatus
from sentinel.common.transport import Health
from sentinel.watchdog.models import MonitoredService


class HealthCheckStrategy(ABC):
    @abstractmethod
    async def check(self, service: MonitoredService) -> bool:
        raise NotImplementedError


class HTTPHealthCheckStrategy(HealthCheckStrategy):
    def __init__(self, client: httpx.AsyncClient, timeout: float = 2.0) -> None:
        self.client = client
        self.timeout = timeout

    async def check(self, service: MonitoredService) -> bool:
        try:
            response = await self.client.get(
                service.address.rstrip("/") + "/health",
                timeout=self.timeout,
            )
            if response.status_code != 200:
                return False
            health = Health.model_validate(response.json())
            return (
                health.service_id == service.service_id
                and health.role is service.role
                and health.status is ServiceStatus.HEALTHY
            )
        except httpx.HTTPError, ValueError, ValidationError:
            return False


class HeartbeatHealthCheckStrategy(HealthCheckStrategy):
    def __init__(self, timeout: float = 15.0) -> None:
        self.timeout = timeout

    async def check(self, service: MonitoredService) -> bool:
        timestamp = service.last_seen
        if not isinstance(timestamp, datetime) or timestamp.tzinfo is None:
            return False
        try:
            age = (datetime.now(UTC) - timestamp).total_seconds()
            return 0 <= age <= self.timeout
        except ValueError, TypeError, OverflowError:
            return False
