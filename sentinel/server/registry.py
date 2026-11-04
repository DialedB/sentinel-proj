"""Thread-safe, process-local service registry with immutable snapshots."""

from datetime import datetime, timezone
from threading import Lock

from sentinel.common.models import ServiceStatus
from sentinel.common.transport import RegisteredService, Registration


class ServiceRegistry:
    def __init__(self) -> None:
        self._lock = Lock()
        self._services: dict[str, RegisteredService] = {}

    def register(self, registration: Registration) -> RegisteredService:
        with self._lock:
            now = datetime.now(timezone.utc)
            record = RegisteredService(
                **registration.model_dump(), status=ServiceStatus.HEALTHY,
                registered_at=now, last_seen=now,
            )
            self._services[record.service_id] = record
            return record.model_copy(deep=True)

    def heartbeat(self, service_id: str) -> RegisteredService:
        with self._lock:
            record = self._services[service_id].model_copy(update={
                "last_seen": datetime.now(timezone.utc),
                "status": ServiceStatus.HEALTHY,
            })
            self._services[service_id] = record
            return record.model_copy(deep=True)

    def snapshot(self) -> list[RegisteredService]:
        with self._lock:
            return [record.model_copy(deep=True)
                    for record in self._services.values()]
