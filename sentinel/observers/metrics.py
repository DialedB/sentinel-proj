"""Durable eventually consistent lifecycle counters."""

from sentinel.core.event_bus import EventSubscriber
from sentinel.core.events import TaskLifecycleEvent
from sentinel.reliability.recovery import storage_call
from sentinel.storage.event_projections import EventProjections, METRICS_SUBSCRIBER_ID


class MetricsSubscriber(EventSubscriber):
    subscriber_id = METRICS_SUBSCRIBER_ID

    def __init__(self, projections: EventProjections) -> None:
        self.projections = projections

    async def handle(self, event: TaskLifecycleEvent) -> None:
        await storage_call(lambda: self.projections.apply_metrics(event))
