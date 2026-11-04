"""Bind/advertise separation without requiring Docker or external networking."""

import asyncio
import json
import unittest
from dataclasses import replace
from unittest.mock import patch

import httpx

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.core.entrypoint import serve
from sentinel.core.factory import WorkerFactory


class DeploymentTests(unittest.IsolatedAsyncioTestCase):
    def config(self):
        return ServiceConfig.from_env(ServiceRole.WORKER, {
            "SERVICE_ID": "worker1", "SERVICE_HOST": "0.0.0.0",
            "SERVICE_PORT": "8000", "SERVICE_ADVERTISE_HOST": "worker1",
            "MAIN_SERVER_URL": "http://main-server:8000",
        })

    def test_defaults_validation_and_positional_compatibility(self):
        old = ServiceConfig("w", ServiceRole.WORKER, "localhost", 8000,
                            "http://central", 1, 2, "central", 3, 4, 5, "http")
        self.assertIsNone(old.advertise_host)
        self.assertEqual(old.advertised_host, "localhost")
        for invalid in ("", " ", "bad host", "http://worker1", "w/path",
                        "w?query", "w#fragment", "user@host", 123, False):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                replace(old, advertise_host=invalid)
        for valid in ("worker1", "127.0.0.1", "::1"):
            self.assertEqual(replace(old, advertise_host=valid).advertised_host, valid)
        with self.assertRaises(ValueError):
            ServiceConfig.from_env(ServiceRole.WORKER, {"SERVICE_ADVERTISE_HOST": ""})

    def test_entrypoint_binds_wildcard_but_info_advertises_dns(self):
        config = self.config()
        self.assertEqual(WorkerFactory().create_service(config).get_info().host, "worker1")
        with patch("sentinel.core.entrypoint.ServiceConfig.from_env", return_value=config):
            with patch("sentinel.core.entrypoint.uvicorn.run") as run:
                serve(WorkerFactory(), ServiceRole.WORKER)
        self.assertEqual(run.call_args.kwargs, {"host": "0.0.0.0", "port": 8000})

    async def test_registration_actually_sends_advertised_dns(self):
        for factory, role in ((WorkerFactory, ServiceRole.WORKER),):
            sent = asyncio.Event()
            payloads = []

            def respond(request):
                self.assertEqual(str(request.url), "http://main-server:8000/register")
                payloads.append(json.loads(request.content))
                sent.set()
                return httpx.Response(200)

            client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
                                       base_url="http://main-server:8000/")
            service = factory().create_service(replace(self.config(), role=role))
            app = service.create_app()
            with patch("sentinel.common.http_client.httpx.AsyncClient", return_value=client):
                async with app.router.lifespan_context(app):
                    await asyncio.wait_for(sent.wait(), 1)
            self.assertEqual(payloads, [{"service_id": "worker1", "role": "worker",
                                        "host": "worker1", "port": 8000}])
            self.assertTrue(client.is_closed)
