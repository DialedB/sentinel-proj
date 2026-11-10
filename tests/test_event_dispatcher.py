"""Dispatcher ordering, independent retries, durable replay and lifecycle ownership."""

import asyncio
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import SecurityEvent
from sentinel.core.event_bus import EventBus
from sentinel.core.service import ServerService
from sentinel.reliability.event_dispatcher import EventDispatcher
from sentinel.storage.event_repository import EventRepository
from sentinel.storage.task_repository import TaskRepository
from tests.test_events import RecordingSubscriber


class DispatcherTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.tasks = TaskRepository(str(Path(directory.name) / "events.db"))
        self.tasks.initialize()
        self.repo = EventRepository(self.tasks.database)
        self.bus = EventBus()
        self.dispatcher = EventDispatcher(self.repo, self.bus)
        self.tasks.submit(SecurityEvent(source="p", event_type="login_success", payload={}))

    async def test_order_failure_isolation_unregister_and_restart(self):
        bad, good = RecordingSubscriber("bad", True), RecordingSubscriber("good")
        self.bus.register(bad)
        self.bus.register(good)
        with self.assertLogs("sentinel.core.event_bus", level="ERROR"):
            await self.dispatcher.dispatch_once()
        self.assertEqual(len(good.events), 2)
        self.assertEqual(len(self.repo.pending("bad")), 2)
        self.assertEqual(self.repo.pending("good"), [])
        self.bus.unregister("bad")
        await self.dispatcher.dispatch_once()
        self.assertEqual(len(self.repo.pending("bad")), 2)
        restarted_bus = EventBus()
        retried = RecordingSubscriber("bad")
        restarted_bus.register(retried)
        await EventDispatcher(EventRepository(self.tasks.database), restarted_bus).dispatch_once()
        self.assertEqual([e.sequence for e in retried.events], [1, 2])
        self.assertEqual(self.repo.pending("bad"), [])

    async def test_ack_failure_replays_and_loop_survives_database_error(self):
        subscriber = RecordingSubscriber()
        self.bus.register(subscriber)
        with patch.object(self.repo, "record_delivery", side_effect=sqlite3.OperationalError("busy")):
            with self.assertRaises(sqlite3.OperationalError):
                await self.dispatcher.dispatch_once()
        self.assertEqual(len(subscriber.events), 1)
        await self.dispatcher.dispatch_once()
        self.assertEqual([e.sequence for e in subscriber.events], [1, 1, 2])
        completed = asyncio.Event()
        calls = 0
        async def dispatch():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise sqlite3.OperationalError("busy")
            completed.set()
        with patch.object(self.dispatcher, "dispatch_once", dispatch):
            with self.assertLogs("sentinel.reliability.event_dispatcher", level="ERROR"):
                runner = asyncio.create_task(self.dispatcher.run(.001))
                try:
                    await asyncio.wait_for(completed.wait(), 1)
                finally:
                    runner.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await runner

    async def test_bounded_batches_fairness_and_lifespan_cancellation(self):
        for _ in range(55):
            self.tasks.submit(SecurityEvent(source="p", event_type="login_success", payload={}))
        first, second = RecordingSubscriber("first"), RecordingSubscriber("second")
        self.bus.register(first)
        self.bus.register(second)
        await self.dispatcher.dispatch_once()
        self.assertEqual((len(first.events), len(second.events)), (100, 100))
        config = ServiceConfig("s", ServiceRole.SERVER, "localhost", 8000, "http://central",
                               event_dispatch_interval=.001)
        service = ServerService(config, self.tasks, event_bus=self.bus)
        app = service.create_app()
        async with app.router.lifespan_context(app):
            for _ in range(100):
                if len(second.events) == 112:
                    break
                await asyncio.sleep(.005)
            self.assertEqual(len(second.events), 112)
        self.assertFalse(any(t.get_name() == "events-s" for t in asyncio.all_tasks()))

    def test_interval_config(self):
        config = ServiceConfig.from_env(ServiceRole.SERVER, {})
        self.assertEqual(config.event_dispatch_interval, 1)
        self.assertEqual(ServiceConfig.from_env(ServiceRole.SERVER,
                         {"EVENT_DISPATCH_INTERVAL": ".05"}).event_dispatch_interval, .05)
        for value in (0, -1, True, "1", float("inf"), float("nan")):
            with self.assertRaises(ValueError):
                replace(config, event_dispatch_interval=value)
