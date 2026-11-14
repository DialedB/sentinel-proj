"""Shared CLI bootstrapping; construction still goes through Factory Method."""

import uvicorn

from sentinel.common.config import ServiceConfig
from sentinel.common.logging import LoggingConfigurator
from sentinel.common.models import ServiceRole
from sentinel.core.factory import ServiceFactory


def serve(factory: ServiceFactory, role: ServiceRole) -> None:
    config = ServiceConfig.from_env(role)
    LoggingConfigurator().configure()
    service = factory.create_service(config)
    uvicorn.run(service.create_app(), host=config.host, port=config.port)
