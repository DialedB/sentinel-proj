"""Lifecycle contracts and Observer behavior."""

import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pydantic import ValidationError

from sentinel.core.events import LifecycleMetadata, TaskLifecycleEvent, TaskEventType
from sentinel.core.event_bus import EventBus, EventSubscriber


def event(**changes):
    values = dict(
        task_id=uuid4(),
        event_type="task.created",
        payload=LifecycleMetadata(
            security_event_id=uuid4(),
            source="producer",
            command="analyze_security_event",
            attempts=0,
            max_attempts=3,
            reason="submitted",
        ),
    )
    values.update(changes)
    return TaskLifecycleEvent(**values)


class EventModelTests(unittest.TestCase):
    def test_roundtrip_identity_and_deep_immutability(self):
        first, second = event(), event()
        self.assertNotEqual(first.event_id, second.event_id)
        self.assertNotEqual(first.event_id, first.task_id)
        self.assertNotEqual(first.event_id, first.payload.security_event_id)
        self.assertEqual(
            TaskLifecycleEvent.model_validate_json(first.model_dump_json()), first
        )
        with self.assertRaises(ValidationError):
            first.payload.attempts = 2
        with self.assertRaises(ValidationError):
            first.event_type = "task.failed"
        self.assertNotIn("lease_token", first.model_dump_json())

    def test_validation_and_utc(self):
        stamp = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2)))
        self.assertEqual(event(timestamp=stamp).timestamp.utcoffset(), timedelta(0))
        for changes in (
            {"timestamp": datetime(2026, 1, 1)},
            {"timestamp": 123},
            {"event_type": "login_failed"},
            {"sequence": 0},
            {"event_id": "bad"},
            {"surprise": 1},
        ):
            with self.assertRaises(ValidationError):
                event(**changes)
        for changes in (
            {"attempts": -1},
            {"attempts": True},
            {"reason": " "},
            {"worker_id": ""},
            {"lease_expires_at": datetime(2026, 1, 1)},
        ):
            with self.assertRaises(ValidationError):
                event().payload.model_validate(
                    {**event().payload.model_dump(), **changes}
                )


class RecordingSubscriber(EventSubscriber):
    def __init__(self, identity="recording", fail=False):
        self.identity, self.fail, self.events = identity, fail, []

    @property
    def subscriber_id(self):
        return self.identity

    async def handle(self, event):
        if self.fail:
            raise RuntimeError("controlled failure")
        self.events.append(event)


class BusTests(unittest.IsolatedAsyncioTestCase):
    async def test_routing_duplicates_wildcard_removal_and_stable_ids(self):
        bus = EventBus()
        first, second = RecordingSubscriber(), RecordingSubscriber("other")
        bus.register(first, TaskEventType.CREATED)
        bus.register(first, TaskEventType.CREATED)
        bus.register(second)
        snapshot = bus.snapshot()
        created = event()
        self.assertEqual(set(await bus.publish(created)), {"recording", "other"})
        self.assertEqual(first.events, [created])
        await bus.publish(event(event_type="task.ready"))
        self.assertEqual(len(first.events), 1)
        self.assertEqual(len(second.events), 2)
        with self.assertRaises(ValueError):
            bus.register(RecordingSubscriber())
        with self.assertRaises(ValueError):
            bus.register(RecordingSubscriber(" "))
        bus.unregister("recording")
        self.assertEqual(len(snapshot), 2)
        self.assertEqual(set(await bus.publish(created)), {"other"})
        self.assertEqual(await bus.publish(created, subscriber_ids=["recording"]), {})

    async def test_failure_isolated_and_retry_and_cancellation(self):
        import asyncio

        bus = EventBus()
        bad, good = RecordingSubscriber("bad", True), RecordingSubscriber("good")
        bus.register(bad)
        bus.register(good)
        created = event()
        with self.assertLogs("sentinel.core.event_bus", level="ERROR"):
            outcomes = await bus.publish(created)
        self.assertFalse(outcomes["bad"].success)
        self.assertIn("controlled failure", outcomes["bad"].error)
        self.assertTrue(outcomes["good"].success)
        bad.fail = False
        await bus.publish(created, subscriber_ids=["bad"])
        self.assertEqual(bad.events, [created])
        self.assertEqual(good.events, [created])

        async def cancel(event):
            raise asyncio.CancelledError

        bad.handle = cancel
        with self.assertRaises(asyncio.CancelledError):
            await bus.publish(created)
