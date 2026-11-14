"""Read-only demo, bounded watchdog proxy, and lifespan cleanup contracts."""

import unittest
import tempfile
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole, ServiceStatus
from sentinel.core.service import ServerService


class DemoTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.config = ServiceConfig.from_env(
            ServiceRole.SERVER,
            {
                "DATABASE_PATH": str(Path(directory.name) / "tasks.db"),
            },
        )

    def test_page_and_fixed_proxy_share_lifespan_client(self) -> None:
        requests = []
        observations = [
            {
                "service_id": "worker-1",
                "role": "worker",
                "status": "unhealthy",
                "last_checked": "2026-11-05T12:00:00Z",
                "error": "Health check failed",
            }
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            self.assertEqual(str(request.url), "http://watchdog:8000/status")
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.extensions["timeout"]["read"], 0.5)
            return httpx.Response(200, json=observations)

        config = replace(
            self.config, watchdog_url="http://watchdog:8000/", http_timeout=0.5
        )
        service = ServerService(config)
        app = service.create_app()
        original_client = httpx.AsyncClient
        with patch(
            "sentinel.core.service.httpx.AsyncClient",
            side_effect=lambda **kwargs: original_client(
                transport=httpx.MockTransport(handler),
                **kwargs,
            ),
        ) as factory:
            with TestClient(app) as client:
                proxy_client = app.state.demo_client
                page = client.get("/")
                self.assertEqual(page.status_code, 200)
                self.assertIn("text/html", page.headers["content-type"])
                self.assertIn("Read-only demo", page.text)
                self.assertIn("/demo/status", page.text)
                self.assertIn("Promise.allSettled", page.text)
                self.assertIn(".slice(0, 10)", page.text)
                self.assertNotIn("innerHTML", page.text)
                self.assertEqual(requests, [])
                for _ in range(2):
                    response = client.get("/demo/status?url=http://untrusted.invalid")
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.json()[0]["status"], "unhealthy")
                    self.assertEqual(response.json()[0]["error"], "Health check failed")
                self.assertEqual(client.get("/stats").json()["total"], 0)
                self.assertEqual(client.get("/tasks").json(), [])
                self.assertEqual(client.post("/demo/status").status_code, 405)
                self.assertFalse(proxy_client.is_closed)
            self.assertTrue(proxy_client.is_closed)
            factory.assert_called_once()
        self.assertEqual(len(requests), 2)
        self.assertEqual(service.status, ServiceStatus.STOPPED)

    def test_watchdog_failures_are_unavailable_not_healthy(self) -> None:
        def disconnected(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("offline", request=request)

        def timed_out(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("slow", request=request)

        handlers = [
            disconnected,
            timed_out,
            lambda request: httpx.Response(500),
            lambda request: httpx.Response(
                302, headers={"location": "http://other.invalid"}
            ),
            lambda request: httpx.Response(200, text="not JSON"),
            lambda request: httpx.Response(200, json={"status": "healthy"}),
            lambda request: httpx.Response(
                200, json=[{"service_id": "w", "role": "worker", "status": "fake"}]
            ),
        ]
        original_client = httpx.AsyncClient
        for handler in handlers:
            with self.subTest(handler=handler):
                app = ServerService(self.config).create_app()
                with patch(
                    "sentinel.core.service.httpx.AsyncClient",
                    side_effect=lambda **kwargs: original_client(
                        transport=httpx.MockTransport(handler),
                        **kwargs,
                    ),
                ):
                    with TestClient(app) as client:
                        response = client.get("/demo/status")
                        self.assertEqual(response.status_code, 503)
                        self.assertIn("Watchdog unavailable", response.json()["detail"])
                        self.assertEqual(client.get("/stats").status_code, 200)
                    self.assertTrue(app.state.demo_client.is_closed)

    def test_watchdog_config_defaults_override_and_validation(self) -> None:
        self.assertEqual(self.config.watchdog_url, "http://127.0.0.1:8005")
        config = ServiceConfig.from_env(
            ServiceRole.SERVER, {"WATCHDOG_URL": "http://watchdog:8000"}
        )
        self.assertEqual(config.watchdog_url, "http://watchdog:8000")
        for url in (
            "",
            None,
            123,
            "ftp://host",
            "http://",
            "http://bad host",
            "http://host:0",
            "http://[",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                replace(self.config, watchdog_url=url)


if __name__ == "__main__":
    unittest.main()
