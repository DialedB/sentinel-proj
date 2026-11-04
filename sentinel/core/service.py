"""Factory-created services with explicit ASGI application lifecycles."""

import asyncio
import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress

import httpx
from fastapi import FastAPI, HTTPException

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceInfo, ServiceRole, ServiceStatus
from sentinel.common.transport import Health, Heartbeat, RegisteredService, Registration
from sentinel.server.registry import ServiceRegistry

logger = logging.getLogger(__name__)


class BaseService(ABC):
    def __init__(self, config: ServiceConfig) -> None:
        self.config = config
        self.status = ServiceStatus.STARTING

    @property
    def service_id(self) -> str:
        return self.config.service_id

    @property
    def role(self) -> ServiceRole:
        return self.config.role

    def get_info(self) -> ServiceInfo:
        return ServiceInfo(
            service_id=self.service_id,
            role=self.role,
            status=self.status,
            host=self.config.host,
            port=self.config.port,
        )

    def create_app(self) -> FastAPI:
        app = FastAPI(lifespan=self._lifespan)
        app.state.service = self

        @app.get("/health", response_model=Health)
        async def health() -> Health:
            return Health(service_id=self.service_id, role=self.role,
                          status=self.status)

        return app

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI):
        logger.info("Starting %s service_id=%s", self.role.value, self.service_id)
        self.status = ServiceStatus.HEALTHY
        try:
            if self.role in (ServiceRole.PRODUCER, ServiceRole.WORKER):
                async with httpx.AsyncClient(
                    base_url=self.config.server_url.rstrip("/") + "/",
                    timeout=self.config.http_timeout,
                ) as client:
                    task = asyncio.create_task(
                        self._communicate(client), name=f"heartbeat-{self.service_id}",
                    )
                    try:
                        yield
                    finally:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task
            else:
                yield
        finally:
            self.status = ServiceStatus.STOPPED
            logger.info("Shutdown complete service_id=%s", self.service_id)

    async def _communicate(self, client: httpx.AsyncClient) -> None:
        registered = False
        registration = Registration(
            service_id=self.service_id, role=self.role,
            host=self.config.host, port=self.config.port,
        ).model_dump(mode="json")
        while True:
            try:
                if not registered:
                    response = await client.post("register", json=registration)
                    response.raise_for_status()
                    registered = True
                    logger.info("Registered service_id=%s", self.service_id)
                else:
                    response = await client.post(
                        "heartbeat", json={"service_id": self.service_id},
                    )
                    if response.status_code == 404:
                        registered = False
                        logger.debug("Unknown heartbeat; re-registering service_id=%s",
                                     self.service_id)
                        continue
                    response.raise_for_status()
                    logger.debug("Heartbeat service_id=%s", self.service_id)
            except httpx.HTTPError as error:
                if registered:
                    logger.debug("Heartbeat failed service_id=%s: %s",
                                 self.service_id, error)
                else:
                    logger.warning("Registration failed service_id=%s: %s",
                                   self.service_id, error)
            await asyncio.sleep(self.config.heartbeat_interval)

    @abstractmethod
    def run(self) -> None:
        """Run the service; execution is outside Phase 1's scope."""
        raise NotImplementedError


class ServerService(BaseService):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        self.registry = ServiceRegistry()

    def create_app(self) -> FastAPI:
        app = super().create_app()
        app.state.registry = self.registry

        @app.post("/register", response_model=RegisteredService)
        async def register(payload: Registration) -> RegisteredService:
            record = self.registry.register(payload)
            logger.info("Registered %s service_id=%s", record.role.value,
                        record.service_id)
            return record

        @app.post("/heartbeat", response_model=RegisteredService)
        async def heartbeat(payload: Heartbeat) -> RegisteredService:
            try:
                record = self.registry.heartbeat(payload.service_id)
            except KeyError as error:
                raise HTTPException(status_code=404, detail="Unknown service ID") from error
            logger.debug("Heartbeat service_id=%s", payload.service_id)
            return record

        @app.get("/services", response_model=list[RegisteredService])
        async def services() -> list[RegisteredService]:
            return self.registry.snapshot()

        return app

    def run(self) -> None:
        raise NotImplementedError("Server execution is not implemented yet")


class ProducerService(BaseService):
    def run(self) -> None:
        raise NotImplementedError("Producer execution is not implemented yet")


class WorkerService(BaseService):
    def run(self) -> None:
        raise NotImplementedError("Worker execution is not implemented yet")


class WatchdogService(BaseService):
    def run(self) -> None:
        raise NotImplementedError("Watchdog execution is not implemented yet")
