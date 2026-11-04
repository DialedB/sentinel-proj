"""Discovery and resilient sequential checks, independent of API requests."""

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timezone
from urllib.parse import urlunsplit

import httpx
from pydantic import TypeAdapter

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole, ServiceStatus
from sentinel.common.transport import RegisteredService
from sentinel.watchdog.models import CheckResult, MonitoredService
from sentinel.watchdog.strategies import (
    HealthCheckStrategy,
    HTTPHealthCheckStrategy,
)

logger = logging.getLogger(__name__)


class ServiceMonitor:
    def __init__(
        self,
        config: ServiceConfig,
        client: httpx.AsyncClient,
        strategy: HealthCheckStrategy,
    ) -> None:
        self.config = config
        self.client = client
        self.strategy = strategy
        self.central_strategy = HTTPHealthCheckStrategy(
            client, config.health_check_timeout,
        )
        central = MonitoredService(
            config.main_server_id, ServiceRole.SERVER, config.server_url,
        )
        self.services = {central.service_id: central}
        self._results = {
            central.service_id: CheckResult(central.service_id, central.role),
        }

    def snapshot(self) -> list[CheckResult]:
        return list(self._results.values())

    async def discover(self) -> None:
        response = await self.client.get(
            self.config.server_url.rstrip("/") + "/services",
            timeout=self.config.health_check_timeout,
        )
        response.raise_for_status()
        records = TypeAdapter(list[RegisteredService]).validate_python(
            response.json(),
        )
        for record in records:
            if record.service_id == self.config.main_server_id:
                raise ValueError(
                    "Registry identity conflicts with configured central ID",
                )
        for key, result in self._results.items():
            self._results[key] = replace(result, discoverable=False)
        self._results[self.config.main_server_id] = replace(
            self._results[self.config.main_server_id], discoverable=True,
        )
        for record in records:
            host = f"[{record.host}]" if ":" in record.host else record.host
            address = urlunsplit(("http", f"{host}:{record.port}", "", "", ""))
            service = MonitoredService(
                record.service_id, record.role, address, record.last_seen,
            )
            self.services[service.service_id] = service
            old = self._results.get(
                service.service_id,
                CheckResult(service.service_id, service.role),
            )
            self._results[service.service_id] = replace(
                old, role=service.role, discoverable=True,
            )

    async def check_once(self) -> None:
        discovery_error = None
        try:
            await self.discover()
        except Exception as error:
            logger.exception("Central discovery failed")
            discovery_error = f"Discovery failed: {error}"
            for key, result in self._results.items():
                self._results[key] = replace(result, discoverable=False)
        for key, service in list(self.services.items()):
            error_message = None
            try:
                strategy = (
                    self.central_strategy
                    if key == self.config.main_server_id else self.strategy
                )
                healthy = await strategy.check(service)
                if not healthy:
                    error_message = "Health check failed"
            except Exception as error:
                logger.exception("Health check failed service_id=%s", key)
                healthy = False
                error_message = str(error) or type(error).__name__
            if key == self.config.main_server_id and discovery_error:
                healthy = False
                error_message = discovery_error
            now = datetime.now(timezone.utc)
            old = self._results[key]
            status = (
                ServiceStatus.HEALTHY if healthy else ServiceStatus.UNHEALTHY
            )
            self._results[key] = replace(
                old, status=status, last_checked=now,
                last_successful=now if healthy else old.last_successful,
                error=error_message,
            )
            if old.status is not status:
                logger.info(
                    "[WATCHDOG] %s %s -> %s", key,
                    old.status.name if old.status is not None else "UNCHECKED",
                    status.name,
                )

    async def run(self) -> None:
        while True:
            try:
                await self.check_once()
            except Exception:
                logger.exception("Monitor iteration failed")
            await asyncio.sleep(self.config.monitor_interval)
