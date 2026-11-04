"""Shared CLI bootstrapping; construction still goes through Factory Method."""

import logging

import uvicorn

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.core.factory import ServiceFactory


def serve(factory: ServiceFactory, role: ServiceRole) -> None:
    config = ServiceConfig.from_env(role)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    service = factory.create_service(config)
    uvicorn.run(service.create_app(), host=config.host, port=config.port)
