"""Best-effort structured logging; the append-only event log is authoritative."""

import logging

from sentinel.core.event_bus import EventSubscriber
from sentinel.core.events import TaskLifecycleEvent
from sentinel.reliability.recovery import storage_call
from sentinel.storage.event_projections import AUDIT_SUBSCRIBER_ID, EventProjections

logger = logging.getLogger(__name__)


class AuditSubscriber(EventSubscriber):
    subscriber_id = AUDIT_SUBSCRIBER_ID

    def __init__(self, projections: EventProjections) -> None:
        self.projections = projections

    async def handle(self, event: TaskLifecycleEvent) -> None:
        serialized = event.model_dump_json()
        if await storage_call(lambda: self.projections.receipt(event.event_id, self.subscriber_id)):
            # A crash after receipt commit can lose this line. SQLite and a log
            # sink cannot commit atomically; do not advertise exactly-once logs.
            logger.info("%s", serialized)
