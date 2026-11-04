"""Shared service identities and status snapshots."""

from dataclasses import dataclass
from enum import Enum


class ServiceRole(Enum):
    SERVER = "server"
    PRODUCER = "producer"
    WORKER = "worker"
    WATCHDOG = "watchdog"


class ServiceStatus(Enum):
    STARTING = "starting"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    STOPPED = "stopped"


@dataclass
class ServiceInfo:
    service_id: str
    role: ServiceRole
    status: ServiceStatus
    host: str
    port: int
