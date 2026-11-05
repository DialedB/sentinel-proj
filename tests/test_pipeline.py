"""Retry identity, worker execution isolation, shared client and cancellation."""

import asyncio
import json
import threading
import unittest
from contextlib import suppress
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID

import httpx

from sentinel.common.config import ServiceConfig
from sentinel.common.http_client import ServiceHTTPClient
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import Failure, SecurityEvent, Task, TaskResult
from sentinel.core.pipeline import produce, work
from sentinel.core.service import ProducerService, WorkerService
from sentinel.worker.executor import WorkerExecutor


def config(role: ServiceRole = ServiceRole.WORKER) -> ServiceConfig:
    return ServiceConfig(
        "w",
        role,
        "localhost",
        8001,
        "http://central",
        heartbeat_interval=0.005,
        worker_poll_interval=0.005,
        event_generation_interval=0.005,
    )


def task(command: str = "analyze_security_event") -> Task:
    return (
        Task(
            event=SecurityEvent(source="p", event_type="login_success", payload={}),
            command=command,
        )
        .ready()
        .claim("w")
    )


async def cancel(runner: asyncio.Task[None]) -> None:
    runner.cancel()
    with suppress(asyncio.CancelledError):
        await runner


class PipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_producer_retains_exact_event_until_ack(self) -> None:
        client = AsyncMock(spec=ServiceHTTPClient)
        sent = []
        done = asyncio.Event()

        async def submit(event: SecurityEvent) -> None:
            sent.append(event.model_dump(mode="json"))
            if len(sent) == 1:
                raise httpx.ReadTimeout("ack lost")
            if len(sent) == 3:
                done.set()

        client.submit_event.side_effect = submit
        runner = asyncio.create_task(produce(config(), client))
        try:
            await asyncio.wait_for(done.wait(), 1)
        finally:
            await cancel(runner)
        self.assertEqual(sent[0], sent[1])
        self.assertNotEqual(sent[1]["event_id"], sent[2]["event_id"])
        self.assertEqual(sent[0]["payload"]["source_ip"], "192.0.2.10")

    async def test_worker_retries_result_without_execution_or_claim_retry(self) -> None:
        client = AsyncMock(spec=ServiceHTTPClient)
        t = task()
        client.claim_task.side_effect = [t, None]
        done = asyncio.Event()
        results = []

        async def submit(task_id: UUID, result: TaskResult) -> None:
            results.append(result.model_dump(mode="json"))
            if len(results) < 3:
                raise httpx.ConnectError("offline")
            done.set()

        client.submit_result.side_effect = submit
        main_thread = threading.get_ident()
        threads = []
        original = WorkerExecutor.execute

        def execute(
            executor: WorkerExecutor,
            command: str,
            payload: dict[str, Any],
        ) -> dict[str, Any]:
            threads.append(threading.get_ident())
            return original(executor, command, payload)

        with patch.object(WorkerExecutor, "execute", execute):
            runner = asyncio.create_task(work(config(), client))
            try:
                await asyncio.wait_for(done.wait(), 1)
            finally:
                await cancel(runner)
        self.assertEqual(client.claim_task.await_count, 1)
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])
        self.assertEqual(len(threads), 1)
        self.assertNotEqual(threads[0], main_thread)

    async def test_unknown_command_failed_and_terminal_conflict_continues(self) -> None:
        for status in (404, 409):
            client = AsyncMock(spec=ServiceHTTPClient)
            done = asyncio.Event()
            calls = 0

            async def claim(worker_id: str) -> Task | None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    return task("unknown")
                done.set()
                return None

            client.claim_task.side_effect = claim
            request = httpx.Request("POST", "http://central/result")
            client.submit_result.side_effect = httpx.HTTPStatusError(
                "terminal",
                request=request,
                response=httpx.Response(status, request=request),
            )
            runner = asyncio.create_task(work(config(), client))
            try:
                await asyncio.wait_for(done.wait(), 1)
            finally:
                await cancel(runner)
            result = client.submit_result.call_args.args[1]
            self.assertEqual(result.outcome.status, "failure")
            self.assertIn("Unknown command", result.outcome.error)
            self.assertEqual(client.submit_result.await_count, 1)

    async def test_real_client_serialization_and_cleanup(self) -> None:
        requests = []
        t = task()

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.method == "GET" and len(requests) == 2:
                return httpx.Response(204)
            return httpx.Response(200, json=t.model_dump(mode="json"))

        transport = httpx.AsyncClient(
            base_url="http://central/", transport=httpx.MockTransport(handler)
        )

        with patch(
            "sentinel.common.http_client.httpx.AsyncClient", return_value=transport
        ):
            async with ServiceHTTPClient("http://central", 1) as client:
                await client.submit_event(t.event)
                self.assertIsNone(await client.claim_task("w"))
                await client.claim_task("w")
                await client.submit_result(
                    t.task_id,
                    TaskResult(
                        worker_id="w", outcome=Failure(status="failure", error="bad")
                    ),
                )
        self.assertTrue(transport.is_closed)
        self.assertEqual(requests[1].url.params["worker_id"], "w")
        self.assertEqual(json.loads(requests[-1].content)["worker_id"], "w")
        self.assertEqual(
            json.loads(requests[0].content)["event_id"], str(t.event.event_id)
        )

    async def test_lifespan_both_loops_heartbeat_and_cancellation(self) -> None:
        for service_type, role in (
            (ProducerService, ServiceRole.PRODUCER),
            (WorkerService, ServiceRole.WORKER),
        ):
            client = AsyncMock(spec=ServiceHTTPClient)
            entered, cancelled, beat = asyncio.Event(), asyncio.Event(), asyncio.Event()

            async def blocked(*args: object) -> None:
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            client.submit_event.side_effect = blocked
            client.claim_task.side_effect = blocked

            async def heartbeat(*args: object) -> None:
                if entered.is_set():
                    beat.set()

            client.heartbeat.side_effect = heartbeat
            service = service_type(config(role))
            app = service.create_app()
            with patch("sentinel.core.service.ServiceHTTPClient") as constructor:
                context = constructor.return_value
                context.__aenter__ = AsyncMock(return_value=client)
                context.__aexit__ = AsyncMock(return_value=False)
                async with app.router.lifespan_context(app):
                    await asyncio.wait_for(beat.wait(), 1)
                context.__aexit__.assert_awaited_once()
            self.assertTrue(cancelled.is_set())
            self.assertFalse(
                any(
                    t.get_name() in ("pipeline-w", "heartbeat-w")
                    for t in asyncio.all_tasks()
                )
            )
