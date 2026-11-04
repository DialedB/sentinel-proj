"""Explicit service configuration; environment loading is not implemented."""

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

    def __post_init__(self) -> None:
        for name in ("service_id", "host", "server_url"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.role, ServiceRole):
            raise ValueError("role must be a ServiceRole")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("port must be an integer between 1 and 65535")
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
