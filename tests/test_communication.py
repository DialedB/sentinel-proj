"""Phase 2 registry, API, configuration and asynchronous lifecycle contracts."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta
import unittest
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient
from pydantic import ValidationError

from sentinel.common.config import ServiceConfig
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.models import ServiceRole, ServiceStatus
from sentinel.common.transport import Registration
from sentinel.core.factory import ProducerFactory, ServerFactory, WorkerFactory
from sentinel.server.registry import ServiceRegistry


def config(role=ServiceRole.PRODUCER):
    return ServiceConfig("test-1", role, "127.0.0.1", 8001,
                         "http://central:8000", 0.01, 0.1)


def payload(**changes):
    return dict(service_id="producer-1", role="producer", host="localhost",
                port=8001, **changes)


class RegistryTests(unittest.TestCase):
    def test_reconnect_snapshots_utc_and_unknown(self):
        registry = ServiceRegistry()
        first = registry.register(Registration(**payload()))
        registry.register(Registration(**{**payload(), "port": 9001}))
        self.assertEqual(len(registry.snapshot()), 1)
        self.assertEqual(registry.snapshot()[0].port, 9001)
        self.assertEqual(first.port, 8001)
        beat = registry.heartbeat(first.service_id)
        self.assertEqual(beat.last_seen.utcoffset(), timedelta(0))
        self.assertGreaterEqual(beat.last_seen, first.last_seen)
        snapshot = registry.snapshot()
        snapshot.clear()
        self.assertEqual(len(registry.snapshot()), 1)
        with self.assertRaises(ValidationError):
            first.port = 1000
        with self.assertRaises(KeyError):
            registry.heartbeat("unknown")

    def test_concurrent_reconnect_has_no_duplicates(self):
        registry = ServiceRegistry()
        def register_and_beat(_):
            registry.register(Registration(**payload()))
            registry.heartbeat("producer-1")
            registry.snapshot()
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(register_and_beat, range(100)))
        self.assertEqual(len(registry.snapshot()), 1)


class APITests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.service = ServerFactory().create_service(replace(
            config(ServiceRole.SERVER), database_path=str(Path(directory.name) / "tasks.db"),
        ))
        self.client = TestClient(self.service.create_app())
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def test_health_registration_heartbeat_and_reconnect(self):
        self.assertEqual(self.client.get("/health").json(), {
            "service_id": "test-1", "role": "server", "status": "healthy"})
        first = self.client.post("/register", json=payload())
        self.assertEqual(first.status_code, 200)
        before = datetime.fromisoformat(first.json()["last_seen"])
        self.client.post("/register", json={**payload(), "port": 9000})
        heartbeat = self.client.post("/heartbeat", json={"service_id": "producer-1"})
        self.assertEqual(heartbeat.status_code, 200)
        self.assertGreaterEqual(datetime.fromisoformat(heartbeat.json()["last_seen"]), before)
        services = self.client.get("/services").json()
        self.assertEqual(len(services), 1)
        self.assertEqual(services[0]["port"], 9000)
        self.assertEqual(self.client.post("/heartbeat", json={
            "service_id": "missing"}).status_code, 404)

    def test_invalid_transport_returns_422(self):
        for change in ({"role": "server"}, {"role": "watchdog"},
                       {"role": "invalid"}, {"port": 0}, {"port": True},
                       {"port": "8001"}, {"service_id": " "}, {"host": ""},
                       {"unexpected": 1}):
            with self.subTest(change=change):
                self.assertEqual(self.client.post("/register", json={
                    **payload(), **change}).status_code, 422)
        for body in ({}, {"service_id": " "}, {"service_id": 3}):
            self.assertEqual(self.client.post("/heartbeat", json=body).status_code, 422)
        self.assertEqual(self.client.post("/register", json={
            **payload(), "role": "worker"}).status_code, 200)


class ConfigurationTests(unittest.TestCase):
    def test_defaults_overrides_and_immutability(self):
        for role, port in ((ServiceRole.SERVER, 8000), (ServiceRole.PRODUCER, 8001),
                           (ServiceRole.WORKER, 8002)):
            loaded = ServiceConfig.from_env(role, {})
            self.assertEqual(loaded.port, port)
            self.assertEqual(loaded.heartbeat_interval, 5)
            self.assertEqual(loaded.http_timeout, 5)
            self.assertEqual(loaded.service_id, role.value + "-1")
        loaded = ServiceConfig.from_env(ServiceRole.WORKER, {
            "SERVICE_ID": "w2", "SERVICE_ROLE": "worker",
            "SERVICE_HOST": "0.0.0.0", "SERVICE_PORT": "9002",
            "MAIN_SERVER_URL": "https://central:8443", "HEARTBEAT_INTERVAL": "2",
            "HTTP_TIMEOUT": "3"})
        self.assertEqual(loaded, ServiceConfig("w2", ServiceRole.WORKER,
                         "0.0.0.0", 9002, "https://central:8443", 2, 3))
        with self.assertRaises(FrozenInstanceError):
            loaded.port = 1

    def test_unprefixed_environment_variables_are_not_configuration(self):
        loaded = ServiceConfig.from_env(ServiceRole.WORKER, {
            "ROLE": "server", "HOST": "bad host", "PORT": "invalid",
        })
        self.assertEqual(loaded.role, ServiceRole.WORKER)
        self.assertEqual(loaded.host, "127.0.0.1")
        self.assertEqual(loaded.port, 8002)

    def test_invalid_environment_and_timing(self):
        for key, values in {
            "SERVICE_ROLE": ["server", "bad", ""], "SERVICE_ID": ["", " "],
            "SERVICE_HOST": ["", "bad host", "http://localhost"],
            "SERVICE_PORT": ["0", "65536", "nan", "1.5"],
            "MAIN_SERVER_URL": ["bad", "ftp://host"],
            "HEARTBEAT_INTERVAL": ["0", "-1", "nan", "inf", "bad"],
            "HTTP_TIMEOUT": ["0", "-1", "nan", "inf", "bad"],
        }.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    ServiceConfig.from_env(ServiceRole.PRODUCER, {key: value})
        for name in ("heartbeat_interval", "http_timeout"):
            for value in (True, None, "1", 0, float("nan")):
                with self.assertRaises(ValueError):
                    replace(config(), **{name: value})


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_mock_client_methods_retry_and_reregister(self):
        client = AsyncMock(spec=ServiceHTTPClient)
        completed = asyncio.Event()
        request = httpx.Request("POST", "http://central/heartbeat")
        unknown = httpx.Response(404, request=request)
        client.register.side_effect = [httpx.ConnectTimeout("offline"), None, None]
        heartbeat_errors = iter([
            httpx.ConnectError("connection lost"),
            httpx.ReadTimeout("heartbeat timed out"),
            httpx.HTTPStatusError("unknown service", request=request, response=unknown),
            None,
        ])
        async def heartbeat(service_id):
            self.assertEqual(service_id, "test-1")
            error = next(heartbeat_errors)
            if error is not None:
                raise error
            completed.set()
        client.heartbeat.side_effect = heartbeat
        service = ProducerFactory().create_service(config())
        app = service.create_app()
        with patch("sentinel.core.service.ServiceHTTPClient", autospec=True) as constructor:
            context = constructor.return_value
            context.__aenter__.return_value = client
            with self.assertLogs("sentinel.core.service", level="WARNING") as logs:
                async with app.router.lifespan_context(app):
                    await asyncio.wait_for(completed.wait(), 1)
            constructor.assert_called_once_with("http://central:8000", 0.1)
            context.__aenter__.assert_awaited_once()
            context.__aexit__.assert_awaited_once_with(None, None, None)
        self.assertEqual(client.register.await_count, 3)
        for call in client.register.await_args_list:
            self.assertEqual(call.args[0], Registration(
                service_id="test-1", role=ServiceRole.PRODUCER,
                host="127.0.0.1", port=8001))
        self.assertEqual(client.heartbeat.await_count, 4)
        self.assertEqual(sum("Registration failed" in line for line in logs.output), 1)
        self.assertEqual(sum("Heartbeat failed" in line for line in logs.output), 3)
        self.assertTrue(all("WARNING" in line and "test-1" in line for line in logs.output))
        self.assertEqual(service.status, ServiceStatus.STOPPED)

    async def test_retry_unknown_reregistration_health_and_client_close(self):
        for factory in (ProducerFactory, WorkerFactory):
            role = ServiceRole.PRODUCER if factory is ProducerFactory else ServiceRole.WORKER
            service = factory().create_service(config(role))
            calls = []
            completed = asyncio.Event()
            def handler(request):
                calls.append(request.url.path)
                if len(calls) == 1:
                    raise httpx.ConnectError("offline", request=request)
                if len(calls) == 2:
                    return httpx.Response(503)
                if len(calls) == 4:
                    return httpx.Response(404)
                if len(calls) >= 6:
                    completed.set()
                return httpx.Response(200, json={})
            client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                      base_url="http://central:8000/")
            app = service.create_app()
            with patch("sentinel.common.http_client.httpx.AsyncClient", return_value=client) as constructor:
                with self.assertLogs("sentinel.core.service", level="DEBUG") as logs:
                    async with app.router.lifespan_context(app):
                        self.assertEqual(service.status, ServiceStatus.HEALTHY)
                        await asyncio.wait_for(completed.wait(), 1)
                    self.assertTrue(client.is_closed)
                    self.assertEqual(service.status, ServiceStatus.STOPPED)
                constructor.assert_called_once_with(base_url="http://central:8000/", timeout=0.1)
            self.assertEqual(calls[:6], ["/register", "/register", "/register",
                                         "/heartbeat", "/register", "/heartbeat"])
            output = "\n".join(logs.output)
            for text in ("Starting", "Registration failed", "Registered", "Heartbeat",
                         "Shutdown complete", "test-1"):
                self.assertIn(text, output)

    async def test_heartbeat_failures_retry_without_duplicate_registration(self):
        calls = []
        completed = asyncio.Event()
        def handler(request):
            calls.append(request.url.path)
            if len(calls) == 2:
                raise httpx.ReadTimeout("slow heartbeat", request=request)
            if len(calls) == 3:
                return httpx.Response(503)
            if len(calls) == 4:
                completed.set()
            return httpx.Response(200)
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                  base_url="http://central/")
        service = ProducerFactory().create_service(config())
        app = service.create_app()
        with patch("sentinel.common.http_client.httpx.AsyncClient", return_value=client):
            with self.assertLogs("sentinel.core.service", level="WARNING") as logs:
                async with app.router.lifespan_context(app):
                    await asyncio.wait_for(completed.wait(), 1)
            self.assertEqual(len(logs.output), 2)
            self.assertTrue(all("WARNING" in line and "Heartbeat failed" in line
                                and "test-1" in line for line in logs.output))
        self.assertEqual(calls[:4], ["/register", "/heartbeat", "/heartbeat", "/heartbeat"])
        self.assertTrue(client.is_closed)

    async def test_shutdown_cancels_and_awaits_inflight_request(self):
        entered, cancelled = asyncio.Event(), asyncio.Event()
        async def handler(request):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                  base_url="http://central/")
        service = ProducerFactory().create_service(config())
        app = service.create_app()
        with patch("sentinel.common.http_client.httpx.AsyncClient", return_value=client):
            async with app.router.lifespan_context(app):
                await asyncio.wait_for(entered.wait(), 1)
            self.assertTrue(cancelled.is_set())
            self.assertTrue(client.is_closed)
            self.assertEqual(service.status, ServiceStatus.STOPPED)
        self.assertFalse(any(task.get_name() == "heartbeat-test-1"
                             for task in asyncio.all_tasks()))

    async def test_health_available_while_registration_blocked(self):
        entered = asyncio.Event()
        async def handler(request):
            entered.set()
            await asyncio.Event().wait()
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler),
                                  base_url="http://central/")
        service = WorkerFactory().create_service(config(ServiceRole.WORKER))
        app = service.create_app()
        health_client = httpx.AsyncClient(transport=httpx.ASGITransport(app),
                                         base_url="http://worker")
        with patch("sentinel.common.http_client.httpx.AsyncClient", return_value=client):
            async with app.router.lifespan_context(app):
                await asyncio.wait_for(entered.wait(), 1)
                async with health_client:
                    response = await asyncio.wait_for(health_client.get("/health"), 1)
                    self.assertEqual(response.json(), {"service_id": "test-1",
                                     "role": "worker", "status": "healthy"})


if __name__ == "__main__":
    unittest.main()
