"""Immutable service configuration and validated environment loading."""

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlsplit

from sentinel.common.models import ServiceRole


@dataclass(frozen=True)
class ServiceConfig:
    service_id: str
    role: ServiceRole
    host: str
    port: int
    server_url: str
    heartbeat_interval: float = 5.0
    http_timeout: float = 5.0

    main_server_id: str = "server-1"
    monitor_interval: float = 5.0
    health_check_timeout: float = 2.0
    heartbeat_timeout: float = 15.0
    health_check_strategy: str = "http"
    advertise_host: str | None = None
    event_generation_interval: float = 2.0
    worker_poll_interval: float = 1.0
    processing_delay: float = 0.0
    watchdog_url: str = "http://127.0.0.1:8005"
    database_path: str = "data/sentinel.db"
    task_lease_seconds: float = 30.0
    task_max_attempts: int = 3
    recovery_interval_seconds: float = 5.0

    @property
    def advertised_host(self) -> str:
        return self.host if self.advertise_host is None else self.advertise_host

    @classmethod
    def from_env(
        cls, role: ServiceRole, environ: Mapping[str, str] | None = None,
    ) -> "ServiceConfig":
        env = os.environ if environ is None else environ
        if not isinstance(role, ServiceRole):
            raise ValueError("Unsupported service role")
        configured_role = ServiceRole(env.get("SERVICE_ROLE", role.value))
        if configured_role is not role:
            raise ValueError(
                f"SERVICE_ROLE must be {role.value} for this entrypoint",
            )
        ports = {
            ServiceRole.SERVER: 8000,
            ServiceRole.PRODUCER: 8001,
            ServiceRole.WORKER: 8002,
            ServiceRole.WATCHDOG: 8005,
        }
        default_host = "127.0.0.1"
        default_port = str(ports[role])
        if role is ServiceRole.WATCHDOG:
            default_host = env.get("WATCHDOG_HOST", default_host)
            default_port = env.get("WATCHDOG_PORT", default_port)
        return cls(
            service_id=env.get("SERVICE_ID", f"{role.value}-1"),
            role=configured_role,
            host=env.get("SERVICE_HOST", default_host),
            port=int(env.get("SERVICE_PORT", default_port)),
            server_url=env.get("MAIN_SERVER_URL", "http://127.0.0.1:8000"),
            heartbeat_interval=float(env.get("HEARTBEAT_INTERVAL", "5")),
            http_timeout=float(env.get("HTTP_TIMEOUT", "5")),
            main_server_id=env.get("MAIN_SERVER_ID", "server-1"),
            monitor_interval=float(env.get("MONITOR_INTERVAL", "5")),
            health_check_timeout=float(env.get("HEALTH_CHECK_TIMEOUT", "2")),
            heartbeat_timeout=float(env.get("HEARTBEAT_TIMEOUT", "15")),
            health_check_strategy=env.get("HEALTH_CHECK_STRATEGY", "http"),
            advertise_host=env.get("SERVICE_ADVERTISE_HOST"),
            event_generation_interval=float(env.get("EVENT_GENERATION_INTERVAL", "2")),
            worker_poll_interval=float(env.get("WORKER_POLL_INTERVAL", "1")),
            processing_delay=float(env.get("PROCESSING_DELAY", "0")),
            watchdog_url=env.get("WATCHDOG_URL", "http://127.0.0.1:8005"),
            database_path=env.get("DATABASE_PATH", "data/sentinel.db"),
            task_lease_seconds=float(env.get("TASK_LEASE_SECONDS", "30")),
            task_max_attempts=int(env.get("TASK_MAX_ATTEMPTS", "3")),
            recovery_interval_seconds=float(env.get("RECOVERY_INTERVAL_SECONDS", "5")),
        )

    def __post_init__(self) -> None:
        for name in ("service_id", "host", "server_url", "main_server_id", "database_path"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if self.database_path == ":memory:":
            raise ValueError("database_path must name a file")
        if type(self.task_max_attempts) is not int or self.task_max_attempts <= 0:
            raise ValueError("task_max_attempts must be a positive integer")
        if not isinstance(self.role, ServiceRole):
            raise ValueError("role must be a ServiceRole")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port must be an integer between 1 and 65535")
        for name in ("host", "advertise_host"):
            value = getattr(self, name)
            if name == "advertise_host" and value is None:
                continue
            if (not isinstance(value, str) or not value.strip()
                    or any(char.isspace() for char in value)
                    or any(char in value for char in "/?#@")
                    or "://" in value):
                raise ValueError(
                    f"{name} must be a hostname or IP address, not a URL",
                )
        for name in (
            "heartbeat_interval", "http_timeout", "monitor_interval",
            "health_check_timeout", "heartbeat_timeout",
            "event_generation_interval", "worker_poll_interval",
            "task_lease_seconds", "recovery_interval_seconds",
        ):
            value = getattr(self, name)
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be a finite positive number")
        if (isinstance(self.processing_delay, bool)
                or not isinstance(self.processing_delay, (int, float))
                or not math.isfinite(self.processing_delay) or self.processing_delay < 0):
            raise ValueError("processing_delay must be a finite nonnegative number")
        if self.health_check_strategy not in ("http", "heartbeat"):
            raise ValueError("health_check_strategy must be http or heartbeat")
        for name in ("server_url", "watchdog_url"):
            value = getattr(self, name)
            try:
                url = urlsplit(value) if isinstance(value, str) else None
                valid = (
                    url is not None
                    and url.scheme in ("http", "https")
                    and bool(url.hostname)
                    and not any(char.isspace() for char in value)
                    and (url.port is None or 1 <= url.port <= 65535)
                )
            except ValueError as error:
                raise ValueError(
                    f"{name} must be a valid HTTP(S) URL",
                ) from error
            if not valid:
                raise ValueError(f"{name} must be a valid HTTP(S) URL")
