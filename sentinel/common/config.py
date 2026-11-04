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

    @classmethod
    def from_env(
        cls, role: ServiceRole, environ: Mapping[str, str] | None = None,
    ) -> "ServiceConfig":
        env = os.environ if environ is None else environ
        if role not in (ServiceRole.SERVER, ServiceRole.PRODUCER, ServiceRole.WORKER):
            raise ValueError("Only server, producer and worker entrypoints are supported")
        configured_role = ServiceRole(env.get("SERVICE_ROLE", role.value))
        if configured_role is not role:
            raise ValueError(f"SERVICE_ROLE must be {role.value} for this entrypoint")
        ports = {ServiceRole.SERVER: 8000, ServiceRole.PRODUCER: 8001,
                 ServiceRole.WORKER: 8002}
        return cls(
            env.get("SERVICE_ID", f"{role.value}-1"), configured_role,
            env.get("SERVICE_HOST", "127.0.0.1"),
            int(env.get("SERVICE_PORT", str(ports[role]))),
            env.get("MAIN_SERVER_URL", "http://127.0.0.1:8000"),
            float(env.get("HEARTBEAT_INTERVAL", "5")),
            float(env.get("HTTP_TIMEOUT", "5")),
        )

    def __post_init__(self) -> None:
        for name in ("service_id", "host", "server_url"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.role, ServiceRole):
            raise ValueError("role must be a ServiceRole")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port must be an integer between 1 and 65535")
        if any(char.isspace() for char in self.host) or any(
            char in self.host for char in "/?#@"
        ) or "://" in self.host:
            raise ValueError("host must be a hostname or IP address, not a URL")
        for name in ("heartbeat_interval", "http_timeout"):
            value = getattr(self, name)
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value <= 0):
                raise ValueError(f"{name} must be a finite positive number")
        try:
            url = urlsplit(self.server_url)
            valid = (
                url.scheme in ("http", "https")
                and bool(url.hostname)
                and not any(char.isspace() for char in self.server_url)
                and (url.port is None or 1 <= url.port <= 65535)
            )
        except ValueError as error:
            raise ValueError("server_url must be a valid HTTP(S) URL") from error
        if not valid:
            raise ValueError("server_url must be a valid HTTP(S) URL")
