"""Lifecycle contracts and Observer behavior."""

import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from pydantic import ValidationError

from sentinel.core.events import LifecycleMetadata, TaskLifecycleEvent


def event(**changes):
    values = dict(task_id=uuid4(), event_type="task.created", payload=LifecycleMetadata(
        security_event_id=uuid4(), source="producer", command="analyze_security_event",
        attempts=0, max_attempts=3, reason="submitted",
    ))
    values.update(changes)
    return TaskLifecycleEvent(**values)


class EventModelTests(unittest.TestCase):
    def test_roundtrip_identity_and_deep_immutability(self):
        first, second = event(), event()
        self.assertNotEqual(first.event_id, second.event_id)
        self.assertNotEqual(first.event_id, first.task_id)
        self.assertNotEqual(first.event_id, first.payload.security_event_id)
        self.assertEqual(TaskLifecycleEvent.model_validate_json(first.model_dump_json()), first)
        with self.assertRaises(ValidationError):
            first.payload.attempts = 2
        with self.assertRaises(ValidationError):
            first.event_type = "task.failed"
        self.assertNotIn("lease_token", first.model_dump_json())

    def test_validation_and_utc(self):
        stamp = datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=2)))
        self.assertEqual(event(timestamp=stamp).timestamp.utcoffset(), timedelta(0))
        for changes in ({"timestamp": datetime(2026, 1, 1)}, {"timestamp": 123},
                        {"event_type": "login_failed"}, {"sequence": 0},
                        {"event_id": "bad"}, {"surprise": 1}):
            with self.assertRaises(ValidationError):
                event(**changes)
        for changes in ({"attempts": -1}, {"attempts": True}, {"reason": " "},
                        {"worker_id": ""}, {"lease_expires_at": datetime(2026, 1, 1)}):
            with self.assertRaises(ValidationError):
                event().payload.model_validate({**event().payload.model_dump(), **changes})
