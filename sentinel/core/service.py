"""Service contracts and intentionally unimplemented role entry points."""

from abc import ABC, abstractmethod

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceInfo, ServiceRole, ServiceStatus


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

    @abstractmethod
    def run(self) -> None:
        """Run the service; execution is outside Phase 1's scope."""
        raise NotImplementedError


class ServerService(BaseService):
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
