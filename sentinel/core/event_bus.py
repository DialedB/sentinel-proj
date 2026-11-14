"""In-process Observer routing; durability belongs to the outbox dispatcher."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections.abc import Iterable

from sentinel.core.events import TaskEventType, TaskLifecycleEvent

logger = logging.getLogger(__name__)


class EventSubscriber(ABC):
    @property
    @abstractmethod
    def subscriber_id(self) -> str:
        """Stable across restarts; never reuse for different projection semantics."""

    @abstractmethod
    async def handle(self, event: TaskLifecycleEvent) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class Subscription:
    subscriber: EventSubscriber
    event_types: frozenset[TaskEventType | None]

    def matches(self, event_type: TaskEventType) -> bool:
        return None in self.event_types or event_type in self.event_types


@dataclass(frozen=True)
class DeliveryOutcome:
    success: bool
    error: str | None = None


class EventBus:
    def __init__(self) -> None:
        self._subscriptions: dict[str, Subscription] = {}

    def register(
        self,
        subscriber: EventSubscriber,
        event_type: TaskEventType | None = None,
    ) -> None:
        identity = subscriber.subscriber_id
        if not isinstance(identity, str) or not identity.strip():
            raise ValueError("subscriber_id must be nonblank")
        if event_type is not None and not isinstance(event_type, TaskEventType):
            raise ValueError("event_type must be a TaskEventType or None (wildcard)")
        existing = self._subscriptions.get(identity)
        if existing is not None and existing.subscriber is not subscriber:
            raise ValueError(f"subscriber_id already registered: {identity}")
        types = existing.event_types if existing else frozenset()
        self._subscriptions[identity] = Subscription(subscriber, types | {event_type})

    def unregister(self, subscriber_id: str) -> None:
        self._subscriptions.pop(subscriber_id, None)

    def snapshot(self) -> tuple[Subscription, ...]:
        return tuple(self._subscriptions.values())

    async def publish(
        self,
        event: TaskLifecycleEvent,
        *,
        subscriber_ids: Iterable[str] | None = None,
    ) -> dict[str, DeliveryOutcome]:
        targets = set(subscriber_ids) if subscriber_ids is not None else None
        outcomes = {}
        for subscription in self.snapshot():
            subscriber = subscription.subscriber
            identity = subscriber.subscriber_id
            if not subscription.matches(event.event_type) or (
                targets is not None and identity not in targets
            ):
                continue
            try:
                await subscriber.handle(event)
            except Exception as error:
                logger.exception(
                    "Lifecycle subscriber failed subscriber_id=%s event_id=%s",
                    identity,
                    event.event_id,
                )
                outcomes[identity] = DeliveryOutcome(
                    False, f"{type(error).__name__}: {error}"
                )
            else:
                outcomes[identity] = DeliveryOutcome(True)
        return outcomes
