"""Thread-safe, process-local service registry with immutable snapshots."""

from collections.abc import Collection
from datetime import datetime, UTC
from threading import Lock

from sentinel.common.errors import DomainError
from sentinel.common.models import ServiceStatus
from sentinel.common.transport import RegisteredService, Registration


class ServiceRegistry:
    def __init__(self, reserved_ids: Collection[str] = ()) -> None:
        self._reserved_ids = frozenset(reserved_ids)
        self._lock = Lock()
        self._services: dict[str, RegisteredService] = {}

    def register(self, registration: Registration) -> RegisteredService:
        if registration.service_id in self._reserved_ids:
            raise DomainError(
                "conflict",
                "Service ID is reserved for central",
                "Choose a service ID distinct from the configured central IDs.",
            )
        with self._lock:
            now = datetime.now(UTC)
            record = RegisteredService(
                **registration.model_dump(),
                status=ServiceStatus.HEALTHY,
                registered_at=now,
                last_seen=now,
            )
            self._services[record.service_id] = record
            return record.model_copy(deep=True)

    def heartbeat(self, service_id: str) -> RegisteredService:
        with self._lock:
            record = self._services[service_id].model_copy(
                update={
                    "last_seen": datetime.now(UTC),
                    "status": ServiceStatus.HEALTHY,
                }
            )
            self._services[service_id] = record
            return record.model_copy(deep=True)

    def snapshot(self) -> list[RegisteredService]:
        with self._lock:
            return [record.model_copy(deep=True) for record in self._services.values()]
