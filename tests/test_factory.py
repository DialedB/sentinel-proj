"""Tests for factory construction contracts; no services are started."""

import unittest
from dataclasses import FrozenInstanceError, replace

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceInfo, ServiceRole, ServiceStatus
from sentinel.core.factory import (
    ProducerFactory,
    ServerFactory,
    ServiceFactory,
    WatchdogFactory,
    WorkerFactory,
)
from sentinel.core.service import (
    BaseService,
    ProducerService,
    ServerService,
    WatchdogService,
    WorkerService,
)


class FactoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ServiceConfig(
            service_id="service-1",
            role=ServiceRole.SERVER,
            host="localhost",
            port=8000,
            server_url="http://localhost:8000",
        )
        self.cases = (
            (ServerFactory, ServerService, ServiceRole.SERVER),
            (ProducerFactory, ProducerService, ServiceRole.PRODUCER),
            (WorkerFactory, WorkerService, ServiceRole.WORKER),
            (WatchdogFactory, WatchdogService, ServiceRole.WATCHDOG),
        )

    def test_factories_create_expected_services(self) -> None:
        for factory, service_type, role in self.cases:
            with self.subTest(role=role):
                config = replace(self.config, role=role)
                service = factory().create_service(config)
                self.assertIsInstance(service, service_type)
                self.assertIs(service.config, config)
                self.assertEqual(service.service_id, config.service_id)
                self.assertIs(service.role, role)
                self.assertIs(service.status, ServiceStatus.STARTING)
                self.assertEqual(
                    service.get_info(),
                    ServiceInfo("service-1", role, ServiceStatus.STARTING,
                                "localhost", 8000),
                )

    def test_identity_properties_are_read_only(self) -> None:
        service = ServerFactory().create_service(self.config)
        for name, value in (
            ("service_id", "replacement"),
            ("role", ServiceRole.WORKER),
        ):
            with self.subTest(name=name), self.assertRaises(AttributeError):
                setattr(service, name, value)
        with self.assertRaises(FrozenInstanceError):
            setattr(self.config, "service_id", "replacement")

    def test_get_info_reflects_current_status_and_returns_snapshot(self) -> None:
        service = ServerFactory().create_service(self.config)
        original = service.get_info()
        service.status = ServiceStatus.HEALTHY
        self.assertIs(service.get_info().status, ServiceStatus.HEALTHY)
        self.assertIs(original.status, ServiceStatus.STARTING)

    def test_base_service_provides_shared_application(self) -> None:
        service = BaseService(self.config)
        app = service.create_app()
        self.assertIs(app.state.service, service)
        self.assertIn("/health", {route.path for route in app.routes})
        self.assertIs(service.status, ServiceStatus.STARTING)

    def test_factory_contract_is_abstract(self) -> None:
        with self.assertRaises(TypeError):
            ServiceFactory()

    def test_invalid_configuration_is_rejected(self) -> None:
        invalid_values = {
            "service_id": ("", "   ", None, 123),
            "role": ("server", None, 1),
            "host": ("", " \t", None, 123),
            "port": (0, -1, 65536, True, False, 8000.0, "8000", None),
            "server_url": (
                "", " ", None, 123, "localhost:8000", "ftp://localhost",
                "http://", "https://host:bad", "http://host:65536",
                "http://host:0", "http://[", "http://bad host",
            ),
        }
        for field, values in invalid_values.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        replace(self.config, **{field: value})

    def test_valid_configuration_boundaries(self) -> None:
        for port in (1, 65535):
            for url in ("http://localhost", "https://example.org:443",
                        "http://[::1]:8000"):
                with self.subTest(port=port, url=url):
                    config = replace(self.config, port=port, server_url=url)
                    self.assertEqual(config.port, port)
                    self.assertEqual(config.server_url, url)


if __name__ == "__main__":
    unittest.main()
