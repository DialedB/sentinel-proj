"""Synthetic data only: fictional users and RFC 5737 documentation addresses."""

from itertools import cycle
from collections.abc import Iterator

from sentinel.common.tasks import EventType, SecurityEvent
from sentinel.producer.builder import SecurityEventBuilder


class SecurityEventGenerator:
    def __init__(self, source: str) -> None:
        self.source = source
        self.types: Iterator[EventType] = cycle(EventType)

    def generate(self) -> SecurityEvent:
        return (
            SecurityEventBuilder()
            .from_source(self.source)
            .of_type(next(self.types))
            .with_payload({"user": "fictional-alex", "source_ip": "192.0.2.10"})
            .build()
        )
