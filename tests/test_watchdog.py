"""Monitor state, API, configuration and lifecycle regression coverage."""

import asyncio
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole, ServiceStatus
from sentinel.core.factory import WatchdogFactory
from sentinel.watchdog.monitor import Monitor
from sentinel.watchdog.strategies import HTTPHealthCheckStrategy, HeartbeatHealthCheckStrategy


def config() -> ServiceConfig:
    return ServiceConfig("watchdog-1", ServiceRole.WATCHDOG, "127.0.0.1", 8005,
                         "http://central", monitor_interval=0.01)


class MonitorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.records = [{"service_id": "w", "role": "worker", "host": "worker", "port": 80,
                         "status": "healthy", "registered_at": datetime.now(timezone.utc).isoformat(),
                         "last_seen": datetime.now(timezone.utc).isoformat()}]
        self.discovery_code = 200
        self.worker_healthy = True
        self.requests = []
        def respond(request: httpx.Request) -> httpx.Response:
            self.requests.append(str(request.url))
            if request.url.path == "/services":
                return httpx.Response(self.discovery_code, json=self.records)
            central = request.url.host == "central"
            return httpx.Response(200 if central or self.worker_healthy else 503, json={
                "service_id": "server-1" if central else "w",
                "role": "server" if central else "worker", "status": "healthy",
            })
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        self.monitor = Monitor(config(), self.client, HTTPHealthCheckStrategy(self.client))

    async def asyncTearDown(self) -> None:
        await self.client.aclose()

    def result(self, key: str):
        return next(row for row in self.monitor.snapshot() if row.service_id == key)

    async def test_transitions_recovery_snapshots_missing_and_outage(self) -> None:
        self.assertIsNone(self.result("server-1").status)
        await self.monitor.check_once()
        first = self.result("w")
        self.assertEqual(first.status, "healthy")
        with self.assertRaises(FrozenInstanceError):
            first.status = "bad"
        self.monitor.snapshot().clear()
        self.worker_healthy = False
        await self.monitor.check_once()
        self.assertEqual(self.result("w").status, "unhealthy")
        self.assertEqual(self.result("w").last_successful, first.last_successful)
        self.assertEqual(first.status, "healthy")
        self.worker_healthy = True
        self.records = []
        await self.monitor.check_once()
        self.assertEqual(self.result("w").status, "healthy")
        self.assertFalse(self.result("w").discoverable)
        self.discovery_code = 503
        with self.assertLogs("sentinel.watchdog.monitor", level="ERROR"):
            await self.monitor.check_once()
        self.assertEqual(self.result("server-1").status, "unhealthy")
        self.assertFalse(self.result("server-1").discoverable)
        self.assertEqual(self.result("w").status, "healthy")
        self.assertIn("http://worker/health", self.requests)
        self.discovery_code = 200
        await self.monitor.check_once()
        self.assertEqual(self.result("server-1").status, "healthy")

    async def test_heartbeat_still_http_checks_central(self) -> None:
        self.monitor.strategy = HeartbeatHealthCheckStrategy()
        await self.monitor.check_once()
        self.assertEqual(self.result("w").status, "healthy")
        self.assertIn("http://central/health", self.requests)
        self.assertNotIn("http://worker/health", self.requests)

    async def test_per_service_exception_and_cancellation(self) -> None:
        self.monitor.strategy = AsyncMock()
        self.monitor.strategy.check.side_effect = RuntimeError("broken policy")
        with self.assertLogs("sentinel.watchdog.monitor", level="ERROR"):
            await self.monitor.check_once()
        self.assertEqual(self.result("w").error, "broken policy")
        self.monitor.strategy.check.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await self.monitor.check_once()

    async def test_outer_loop_recovers(self) -> None:
        self.monitor.check_once = AsyncMock(side_effect=[RuntimeError("iteration"), asyncio.CancelledError])
        with self.assertLogs("sentinel.watchdog.monitor", level="ERROR"):
            with self.assertRaises(asyncio.CancelledError):
                await self.monitor.run()
        self.assertEqual(self.monitor.check_once.await_count, 2)

    async def test_bad_discovery_retains_addresses(self) -> None:
        await self.monitor.check_once()
        for records in ([{}], [{**self.records[0], "service_id": "server-1"}]):
            self.records = records
            with self.assertLogs("sentinel.watchdog.monitor", level="ERROR"):
                await self.monitor.check_once()
            self.assertEqual(self.result("w").status, "healthy")
            self.assertEqual(self.monitor.services["server-1"].address, "http://central")


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_api_nonblocking_and_shutdown(self) -> None:
        service = WatchdogFactory().create_service(config())
        app = service.create_app()
        started = asyncio.Event()
        cancelled = asyncio.Event()
        async def blocked(monitor: Monitor) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        with patch.object(Monitor, "run", blocked):
            async with app.router.lifespan_context(app):
                await started.wait()
                client = service.monitor.client
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as api:
                    health = await asyncio.wait_for(api.get("/health"), 0.2)
                    self.assertEqual(health.json()["status"], "healthy")
                    rows = (await api.get("/status")).json()
                    self.assertIsNone(rows[0]["status"])
                    self.assertEqual((await api.get("/status/server-1")).json(), rows[0])
                    self.assertEqual((await api.get("/status/missing")).status_code, 404)
            self.assertTrue(cancelled.is_set())
            self.assertTrue(client.is_closed)
            self.assertIs(service.status, ServiceStatus.STOPPED)


class ConfigurationTests(unittest.TestCase):
    def test_defaults_and_environment(self) -> None:
        value = ServiceConfig.from_env(ServiceRole.WATCHDOG, {})
        self.assertEqual((value.host, value.port, value.monitor_interval,
                          value.health_check_timeout, value.heartbeat_timeout,
                          value.health_check_strategy, value.main_server_id),
                         ("127.0.0.1", 8005, 5, 2, 15, "http", "server-1"))
        env = {"WATCHDOG_HOST": "localhost", "WATCHDOG_PORT": "8015",
               "MONITOR_INTERVAL": "1", "HEALTH_CHECK_TIMEOUT": "0.5",
               "HEARTBEAT_TIMEOUT": "3", "HEALTH_CHECK_STRATEGY": "heartbeat",
               "MAIN_SERVER_ID": "central"}
        value = ServiceConfig.from_env(ServiceRole.WATCHDOG, env)
        self.assertEqual((value.host, value.port, value.main_server_id), ("localhost", 8015, "central"))
        self.assertEqual(value.health_check_strategy, "heartbeat")
        for name in ("monitor_interval", "health_check_timeout", "heartbeat_timeout"):
            for invalid in (True, False, 0, -1, float("nan"), float("inf"), "2", None):
                with self.subTest(name=name, invalid=invalid), self.assertRaises(ValueError):
                    replace(config(), **{name: invalid})
        for invalid in ("", "tcp", True, None):
            with self.assertRaises(ValueError):
                replace(config(), health_check_strategy=invalid)
        with self.assertRaises(ValueError):
            replace(config(), main_server_id=" ")
        with self.assertRaises(ValueError):
            ServiceConfig.from_env(ServiceRole.WATCHDOG, {"SERVICE_ROLE": "worker"})
