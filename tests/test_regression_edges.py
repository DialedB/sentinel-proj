"""Remaining concrete failure paths, background races and runtime configuration."""

import asyncio
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, tzinfo
from pathlib import Path
from unittest.mock import AsyncMock, Mock

from sentinel.common.config import ServiceConfig
from sentinel.common.errors import DomainError
from sentinel.common.models import ServiceRole
from sentinel.core.event_bus import EventBus, EventSubscriber
from sentinel.core.pipeline import work
from sentinel.core.service import ProducerService
from sentinel.reliability.event_dispatcher import EventDispatcher
from sentinel.storage.database import Database
from sentinel.storage.event_repository import EventRepository
from sentinel.watchdog.models import MonitoredService
from sentinel.watchdog.strategies import HeartbeatHealthCheckStrategy
from tests.test_pipeline import cancel, config, task


class EdgeTests(unittest.TestCase):
    def test_invalid_config_database_and_event_repository_bounds(self):
        with self.assertRaises(ValueError):
            ServiceConfig.from_env("worker", {})  # type: ignore[arg-type]  # Invalid-input contract.
        for path in ("", " ", None, ":memory:"):
            with self.assertRaises(ValueError):
                Database(path)  # type: ignore[arg-type]  # Invalid-input contract.
        subscriber = Mock(spec=EventSubscriber)
        subscriber.subscriber_id = "edge"
        with self.assertRaises(ValueError):
            EventBus().register(subscriber, "invalid")  # type: ignore[arg-type]  # Invalid-input contract.
        with tempfile.TemporaryDirectory() as directory:
            database = Database(str(Path(directory) / "db"))
            database.initialize()
            events = EventRepository(database)
            events.seed_deliveries({"empty": frozenset()})
            self.assertEqual(events.pending("empty"), [])
            for limit in (0, 101, True, "1"):
                with self.assertRaises(ValueError):
                    events.pending("empty", limit=limit)  # type: ignore[arg-type]  # Invalid-input contract.


class AsyncEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_unregistration_does_not_acknowledge_delivery(self):
        subscriber = Mock(spec=EventSubscriber)
        subscriber.subscriber_id = "edge"
        bus = EventBus()
        bus.register(subscriber)
        repository = Mock(spec=EventRepository)

        def pending(identity):
            bus.unregister(identity)
            return [Mock()]

        repository.pending.side_effect = pending
        dispatcher = EventDispatcher(repository, bus)
        await dispatcher.dispatch_once()
        repository.record_delivery.assert_not_called()

    async def test_worker_claim_application_failure_then_processing_delay_and_success(
        self,
    ):
        client = AsyncMock()
        t = task()
        client.claim_task.side_effect = [
            DomainError("service_unavailable", "offline", "Retry with backoff.", 503),
            t,
        ]
        done = asyncio.Event()
        client.submit_result.side_effect = lambda *args: done.set()
        runner = asyncio.create_task(
            work(replace(config(), processing_delay=0.001), client)
        )
        try:
            with self.assertLogs("sentinel.core.pipeline", level="WARNING"):
                await asyncio.wait_for(done.wait(), 1)
        finally:
            await cancel(runner)
        self.assertEqual(client.claim_task.await_count, 2)
        self.assertEqual(
            client.submit_result.call_args.args[1].outcome.status, "success"
        )

    async def test_direct_communication_without_pipeline_gate(self):
        service = ProducerService(config(ServiceRole.PRODUCER))
        client = AsyncMock()
        client.heartbeat.side_effect = asyncio.CancelledError
        with self.assertRaises(asyncio.CancelledError):
            await service._communicate(client)
        client.register.assert_awaited_once()

    async def test_broken_timezone_in_heartbeat_is_unhealthy(self):
        class BrokenTimezone(tzinfo):
            def utcoffset(self, dt):
                raise ValueError("invalid timezone")

        target = MonitoredService(
            "w",
            ServiceRole.WORKER,
            "http://worker",
            datetime(2026, 1, 1, tzinfo=BrokenTimezone()),
        )
        self.assertFalse(await HeartbeatHealthCheckStrategy().check(target))
