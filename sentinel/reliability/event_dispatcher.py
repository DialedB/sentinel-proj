"""Single lifespan-owned outbox reader, with bounded per-subscriber batches."""

import asyncio
import logging

from sentinel.core.event_bus import EventBus
from sentinel.reliability.recovery import storage_call
from sentinel.storage.event_repository import EventRepository

logger = logging.getLogger(__name__)


class EventDispatcher:
    def __init__(self, repository: EventRepository, bus: EventBus) -> None:
        self.repository, self.bus = repository, bus

    async def dispatch_once(self) -> None:
        subscriptions = self.bus.snapshot()
        targets = {
            entry.subscriber.subscriber_id: entry.event_types for entry in subscriptions
        }
        await storage_call(lambda: self.repository.seed_deliveries(targets))
        for identity in targets:
            events = await storage_call(lambda: self.repository.pending(identity))
            for event in events:
                outcomes = await self.bus.publish(event, subscriber_ids=[identity])
                outcome = outcomes.get(identity)
                if outcome is None:
                    # Unregistration never acknowledges outstanding work.
                    break
                await storage_call(
                    lambda: self.repository.record_delivery(
                        event.event_id,
                        identity,
                        success=outcome.success,
                        error=outcome.error,
                    )
                )
                if not outcome.success:
                    # Retain strict sequence order for this subscriber only.
                    break

    async def run(self, interval: float) -> None:
        while True:
            try:
                await self.dispatch_once()
            except Exception:
                logger.exception("Lifecycle dispatch failed; retrying committed outbox")
            await asyncio.sleep(interval)
