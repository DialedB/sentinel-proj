"""Immutable monitor inputs and public observation snapshots."""

from dataclasses import dataclass
from datetime import datetime

from sentinel.common.models import ServiceRole


@dataclass(frozen=True)
class MonitoredService:
    service_id: str
    role: ServiceRole
    address: str
    last_seen: datetime | None = None


@dataclass(frozen=True)
class CheckResult:
    service_id: str
    role: ServiceRole
    status: str | None = None
    last_checked: datetime | None = None
    last_successful: datetime | None = None
    error: str | None = None
    discoverable: bool = False
