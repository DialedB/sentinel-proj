"""Shared HTTP client serialization, error propagation, and context cleanup."""

import json
import unittest
from unittest.mock import patch

import httpx

from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.transport import Registration


def registration():
    return Registration(service_id="p1", role="producer", host="localhost", port=8001)


class HTTPClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_registration_heartbeat_and_context_close(self):
        requests = []
        def handler(request):
            requests.append(request)
            return httpx.Response(200)
        transport = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://central/api/",
        )
        with patch("sentinel.common.http_client.httpx.AsyncClient",
                   return_value=transport) as constructor:
            async with ServiceHTTPClient("http://central/api///", 2.5) as client:
                self.assertFalse(transport.is_closed)
                await client.register(registration())
                await client.heartbeat("p1")
            constructor.assert_called_once_with(base_url="http://central/api/", timeout=2.5)
        self.assertTrue(transport.is_closed)
        self.assertEqual([str(request.url) for request in requests], [
            "http://central/api/register", "http://central/api/heartbeat"])
        self.assertEqual(json.loads(requests[0].content), {
            "service_id": "p1", "role": "producer", "host": "localhost", "port": 8001})
        self.assertEqual(json.loads(requests[1].content), {"service_id": "p1"})

    async def test_connection_and_timeout_errors_propagate_and_close(self):
        for method in ("register", "heartbeat"):
            for error_type in (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout):
                with self.subTest(method=method, error_type=error_type):
                    error = error_type("offline")
                    def handler(request):
                        raise error
                    transport = httpx.AsyncClient(
                        transport=httpx.MockTransport(handler), base_url="http://central/",
                    )
                    with patch("sentinel.common.http_client.httpx.AsyncClient",
                               return_value=transport):
                        with self.assertRaises(error_type) as raised:
                            async with ServiceHTTPClient("http://central", 1) as client:
                                if method == "register":
                                    await client.register(registration())
                                else:
                                    await client.heartbeat("p1")
                    self.assertIs(raised.exception, error)
                    self.assertTrue(transport.is_closed)

    async def test_http_errors_propagate_including_unknown_heartbeat(self):
        for method, status in (("register", 422), ("register", 503),
                               ("heartbeat", 404), ("heartbeat", 503)):
            with self.subTest(method=method, status=status):
                transport = httpx.AsyncClient(
                    transport=httpx.MockTransport(lambda request: httpx.Response(status)),
                    base_url="http://central/",
                )
                with patch("sentinel.common.http_client.httpx.AsyncClient",
                           return_value=transport):
                    with self.assertRaises(httpx.HTTPStatusError) as raised:
                        async with ServiceHTTPClient("http://central", 1) as client:
                            if method == "register":
                                await client.register(registration())
                            else:
                                await client.heartbeat("p1")
                self.assertEqual(raised.exception.response.status_code, status)
                self.assertTrue(transport.is_closed)


if __name__ == "__main__":
    unittest.main()
