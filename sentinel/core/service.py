"""Factory-created services with explicit ASGI application lifecycles."""

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import httpx
from fastapi import FastAPI, HTTPException

from sentinel.common.config import ServiceConfig
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.models import ServiceInfo, ServiceRole, ServiceStatus
from sentinel.common.transport import (
    Health,
    Heartbeat,
    RegisteredService,
    Registration,
)
from sentinel.server.registry import ServiceRegistry
from sentinel.storage.task_repository import TaskRepository
from sentinel.storage.event_repository import EventRepository
from sentinel.core.event_bus import EventBus
from sentinel.reliability.event_dispatcher import EventDispatcher
from sentinel.storage.event_projections import EventProjections
from sentinel.observers.metrics import MetricsSubscriber
from sentinel.reliability.recovery import recover_once, run_recovery, storage_call
from sentinel.watchdog.models import CheckResult
from sentinel.watchdog.monitor import ServiceMonitor
from sentinel.watchdog.strategies import (
    HeartbeatHealthCheckStrategy,
    HTTPHealthCheckStrategy,
)

logger = logging.getLogger(__name__)


class BaseService:
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
            host=self.config.advertised_host,
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
    async def _lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        logger.info(
            "Starting %s service_id=%s", self.role.value, self.service_id,
        )
        self.status = ServiceStatus.HEALTHY
        try:
            if self.role in (ServiceRole.PRODUCER, ServiceRole.WORKER):
                async with ServiceHTTPClient(
                    self.config.server_url, self.config.http_timeout,
                ) as client:
                    self._registered = asyncio.Event()
                    from sentinel.core.pipeline import run_pipeline
                    pipeline = asyncio.create_task(
                        run_pipeline(self.config, client, self._registered),
                        name=f"pipeline-{self.service_id}",
                    )
                    task = asyncio.create_task(
                        self._communicate(client),
                        name=f"heartbeat-{self.service_id}",
                    )
                    try:
                        yield
                    finally:
                        task.cancel()
                        pipeline.cancel()
                        await asyncio.gather(task, pipeline, return_exceptions=True)
            else:
                yield
        finally:
            self.status = ServiceStatus.STOPPED
            logger.info("Shutdown complete service_id=%s", self.service_id)

    async def _communicate(self, client: ServiceHTTPClient) -> None:
        registered = False
        registration = Registration(
            service_id=self.service_id, role=self.role,
            host=self.config.advertised_host, port=self.config.port,
        )
        while True:
            try:
                if not registered:
                    await client.register(registration)
                    registered = True
                    if hasattr(self, '_registered'):
                        self._registered.set()
                    logger.info("Registered service_id=%s", self.service_id)
                else:
                    await client.heartbeat(self.service_id)
                    logger.debug("Heartbeat service_id=%s", self.service_id)
            except httpx.HTTPError as error:
                if registered:
                    logger.warning("Heartbeat failed service_id=%s: %s",
                                   self.service_id, error)
                    if (isinstance(error, httpx.HTTPStatusError)
                            and error.response.status_code == 404):
                        registered = False
                        continue
                else:
                    logger.warning("Registration failed service_id=%s: %s",
                                   self.service_id, error)
            await asyncio.sleep(self.config.heartbeat_interval)


class ServerService(BaseService):
    def __init__(
        self, config: ServiceConfig, repository: TaskRepository | None = None,
        *, event_bus: EventBus | None = None,
    ) -> None:
        super().__init__(config)
        self.registry = ServiceRegistry()
        self.tasks = repository if repository is not None else TaskRepository(
            config.database_path, lease_seconds=config.task_lease_seconds,
            max_attempts=config.task_max_attempts,
        )
        self.events = EventRepository(self.tasks.database)
        self.event_bus = event_bus if event_bus is not None else EventBus()
        self.projections = EventProjections(self.tasks.database)
        self.event_bus.register(MetricsSubscriber(self.projections))
        self.dispatcher = EventDispatcher(self.events, self.event_bus)

    def create_app(self) -> FastAPI:
        app = super().create_app()
        app.state.registry = self.registry
        app.state.tasks = self.tasks
        from sentinel.server.task_routes import install_task_routes
        install_task_routes(app, self.tasks, self.registry)
        from sentinel.server.demo import install_demo_routes
        install_demo_routes(app, self.config)

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
                raise HTTPException(
                    status_code=404, detail="Unknown service ID",
                ) from error
            logger.debug("Heartbeat service_id=%s", payload.service_id)
            return record

        @app.get("/services", response_model=list[RegisteredService])
        async def services() -> list[RegisteredService]:
            return self.registry.snapshot()

        return app

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        await storage_call(self.tasks.initialize)
        await recover_once(self.tasks)
        async with httpx.AsyncClient(
            timeout=self.config.http_timeout,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
            follow_redirects=False,
        ) as client:
            app.state.demo_client = client
            recovery = asyncio.create_task(
                run_recovery(self.tasks, self.config.recovery_interval_seconds),
                name=f"recovery-{self.service_id}",
            )
            dispatch = asyncio.create_task(
                self.dispatcher.run(self.config.event_dispatch_interval),
                name=f"events-{self.service_id}",
            )
            try:
                async with super()._lifespan(app):
                    yield
            finally:
                recovery.cancel()
                dispatch.cancel()
                await asyncio.gather(recovery, dispatch, return_exceptions=True)


class ProducerService(BaseService):
    """Producer role using the shared application and lifespan."""


class WorkerService(BaseService):
    """Worker role using the shared application and lifespan."""


class WatchdogService(BaseService):
    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config)
        self.monitor: ServiceMonitor | None = None

    def create_app(self) -> FastAPI:
        app = super().create_app()

        @app.get("/status", response_model=list[CheckResult])
        async def statuses() -> list[CheckResult]:
            return self.monitor.snapshot() if self.monitor else []

        @app.get("/status/{service_id}", response_model=CheckResult)
        async def status(service_id: str) -> CheckResult:
            for result in self.monitor.snapshot() if self.monitor else []:
                if result.service_id == service_id:
                    return result
            raise HTTPException(status_code=404, detail="Unknown service ID")

        return app

    @asynccontextmanager
    async def _lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        logger.info("Starting watchdog service_id=%s", self.service_id)
        try:
            async with httpx.AsyncClient(
                timeout=self.config.health_check_timeout,
            ) as client:
                strategy = (
                    HeartbeatHealthCheckStrategy(self.config.heartbeat_timeout)
                    if self.config.health_check_strategy == "heartbeat"
                    else HTTPHealthCheckStrategy(
                        client, self.config.health_check_timeout,
                    )
                )
                self.monitor = ServiceMonitor(self.config, client, strategy)
                task = asyncio.create_task(
                    self.monitor.run(), name=f"monitor-{self.service_id}",
                )
                self.status = ServiceStatus.HEALTHY
                try:
                    yield
                finally:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
        finally:
            self.status = ServiceStatus.STOPPED
            logger.info("Shutdown complete service_id=%s", self.service_id)
