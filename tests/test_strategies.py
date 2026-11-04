"""Health policy tests without external networking."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest

import httpx

from sentinel.common.models import ServiceRole
from sentinel.watchdog.models import MonitoredService
from sentinel.watchdog.strategies import (
    HealthCheckStrategy, HTTPHealthCheckStrategy, HeartbeatHealthCheckStrategy,
)


class StrategyTests(unittest.IsolatedAsyncioTestCase):
    async def test_abstract_contract(self) -> None:
        with self.assertRaises(TypeError):
            HealthCheckStrategy()

    async def test_http_response_matrix(self) -> None:
        service = MonitoredService("w", ServiceRole.WORKER, "http://worker")
        good = {"service_id": "w", "role": "worker", "status": "healthy"}
        cases = [(200, good, True), (503, good, False),
                 (200, {**good, "service_id": "other"}, False),
                 (200, {**good, "role": "producer"}, False),
                 (200, {**good, "status": "starting"}, False),
                 (200, {}, False), (200, [], False), (200, "invalid", False)]
        for code, payload, expected in cases:
            with self.subTest(code=code, payload=payload):
                def respond(request: httpx.Request) -> httpx.Response:
                    self.assertEqual(request.url.path, "/health")
                    self.assertEqual(request.extensions["timeout"]["read"], 0.25)
                    return httpx.Response(code, json=payload)
                async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                    self.assertEqual(await HTTPHealthCheckStrategy(client, 0.25).check(service), expected)
        for failure in (httpx.ConnectError, httpx.ReadTimeout):
            def fail(request: httpx.Request) -> httpx.Response:
                raise failure("offline", request=request)
            async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
                self.assertFalse(await HTTPHealthCheckStrategy(client).check(service))
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"not json"),
        )) as client:
            self.assertFalse(await HTTPHealthCheckStrategy(client).check(service))

    async def test_heartbeat_timestamp_matrix(self) -> None:
        now = datetime.now(timezone.utc)
        service = MonitoredService("w", ServiceRole.WORKER, "http://worker")
        for timestamp, expected in (
            (now - timedelta(seconds=1), True),
            (now - timedelta(seconds=16), False), (None, False),
            ("bad", False), (datetime.now(), False),
            (now + timedelta(seconds=1), False),
            ((now - timedelta(seconds=1)).astimezone(timezone(timedelta(hours=3))), True),
        ):
            with self.subTest(timestamp=timestamp):
                self.assertEqual(await HeartbeatHealthCheckStrategy().check(
                    replace(service, last_seen=timestamp)), expected)
