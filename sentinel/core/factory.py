"""Factory Method creators for the four service roles."""

from abc import ABC, abstractmethod

from sentinel.common.config import ServiceConfig
from sentinel.core.service import (
    BaseService,
    ProducerService,
    ServerService,
    WatchdogService,
    WorkerService,
)


class ServiceFactory(ABC):
    @abstractmethod
    def create_service(self, config: ServiceConfig) -> BaseService:
        """Construct a service without starting it."""
        raise NotImplementedError


class ServerFactory(ServiceFactory):
    def create_service(self, config: ServiceConfig) -> BaseService:
        return ServerService(config)


class ProducerFactory(ServiceFactory):
    def create_service(self, config: ServiceConfig) -> BaseService:
        return ProducerService(config)


class WorkerFactory(ServiceFactory):
    def create_service(self, config: ServiceConfig) -> BaseService:
        return WorkerService(config)


class WatchdogFactory(ServiceFactory):
    def create_service(self, config: ServiceConfig) -> BaseService:
        return WatchdogService(config)
