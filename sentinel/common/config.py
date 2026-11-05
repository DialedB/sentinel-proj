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
            env.get("SERVICE_ID", f"{role.value}-1"), configured_role,
            env.get("SERVICE_HOST", default_host),
            int(env.get("SERVICE_PORT", default_port)),
            env.get("MAIN_SERVER_URL", "http://127.0.0.1:8000"),
            float(env.get("HEARTBEAT_INTERVAL", "5")),
            float(env.get("HTTP_TIMEOUT", "5")),
            env.get("MAIN_SERVER_ID", "server-1"),
            float(env.get("MONITOR_INTERVAL", "5")),
            float(env.get("HEALTH_CHECK_TIMEOUT", "2")),
            float(env.get("HEARTBEAT_TIMEOUT", "15")),
            env.get("HEALTH_CHECK_STRATEGY", "http"),
            env.get("SERVICE_ADVERTISE_HOST"),
            float(env.get("EVENT_GENERATION_INTERVAL", "2")),
            float(env.get("WORKER_POLL_INTERVAL", "1")),
            float(env.get("PROCESSING_DELAY", "0")),
        )

    def __post_init__(self) -> None:
        for name in ("service_id", "host", "server_url", "main_server_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
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
        try:
            url = urlsplit(self.server_url)
            valid = (
                url.scheme in ("http", "https")
                and bool(url.hostname)
                and not any(char.isspace() for char in self.server_url)
                and (url.port is None or 1 <= url.port <= 65535)
            )
        except ValueError as error:
            raise ValueError(
                "server_url must be a valid HTTP(S) URL",
            ) from error
        if not valid:
            raise ValueError("server_url must be a valid HTTP(S) URL")
