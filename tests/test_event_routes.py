"""History pagination, route compatibility, restart and eventual metrics."""

import tempfile
import time
import unittest
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import SecurityEvent
from sentinel.core.service import ServerService
from sentinel.storage.event_projections import METRICS_SUBSCRIBER_ID


class EventAPITests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.config = ServiceConfig(
            "s",
            ServiceRole.SERVER,
            "localhost",
            8000,
            "http://central",
            database_path=str(Path(directory.name) / "api.db"),
            event_dispatch_interval=0.005,
        )

    def submit(self, client):
        return client.post(
            "/events",
            json=SecurityEvent(
                source="p", event_type="login_success", payload={}
            ).model_dump(mode="json"),
        )

    def test_routes_cursor_recent_validation_and_compatibility(self):
        service = ServerService(self.config)
        with TestClient(service.create_app()) as client:
            submitted = self.submit(client)
            self.assertEqual(submitted.status_code, 200)
            task = submitted.json()
            self.assertEqual(client.post("/events", json=task["event"]).json(), task)
            self.submit(client)
            all_events = client.get("/events/lifecycle").json()
            self.assertEqual([e["sequence"] for e in all_events], [1, 2, 3, 4])
            self.assertEqual(
                client.get("/events/lifecycle?limit=2").json(), all_events[2:]
            )
            first = client.get("/events/lifecycle?limit=2&after_sequence=0").json()
            following = client.get(
                f"/events/lifecycle?after_sequence={first[-1]['sequence']}"
            ).json()
            self.assertEqual(first + following, all_events)
            self.assertEqual(
                client.get(f"/tasks/{task['task_id']}/events").json(), first
            )
            self.assertEqual(
                client.get(f"/tasks/{task['task_id']}/events?limit=1").json(), first[1:]
            )
            self.assertEqual(client.get(f"/tasks/{uuid4()}/events").status_code, 404)
            self.assertEqual(client.get("/tasks/not-a-uuid/events").status_code, 422)
            for query in ("limit=0", "limit=201", "limit=x", "after_sequence=-1"):
                for path in ("/events/lifecycle", f"/tasks/{task['task_id']}/events"):
                    self.assertEqual(client.get(f"{path}?{query}").status_code, 422)
            self.assertEqual(
                client.get("/events/lifecycle?after_sequence=99999").json(), []
            )
            self.assertEqual(client.get("/stats").json()["total"], 2)
            self.assertEqual(
                client.get("/tasks/next?worker_id=unknown").status_code, 404
            )
        with TestClient(ServerService(self.config).create_app()) as client:
            self.assertEqual(client.get("/events/lifecycle").json(), all_events)

    def test_metrics_are_eventual_and_durable_not_current_task_state(self):
        service = ServerService(self.config)
        service.event_bus.unregister(METRICS_SUBSCRIBER_ID)
        with TestClient(service.create_app()) as client:
            self.submit(client)
            self.assertEqual(client.get("/stats").json()["total"], 1)
            self.assertEqual(client.get("/metrics/events").json()["created"], 0)
        with TestClient(ServerService(self.config).create_app()) as client:
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                metrics = client.get("/metrics/events").json()
                if metrics["created"] == 1:
                    break
                time.sleep(0.005)
            self.assertEqual(
                metrics,
                dict(
                    created=1,
                    started=0,
                    completed=0,
                    permanently_failed=0,
                    requeued=0,
                    retry_attempts=0,
                ),
            )
        with TestClient(ServerService(self.config).create_app()) as client:
            self.assertEqual(client.get("/metrics/events").json(), metrics)
