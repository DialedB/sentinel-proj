"""Synthetic data only: fictional users and RFC 5737 documentation addresses."""

from itertools import cycle
from typing import Iterator

from sentinel.common.tasks import EventType, SecurityEvent


class SecurityEventGenerator:
    def __init__(self, source: str) -> None:
        self.source = source
        self.types: Iterator[EventType] = cycle(EventType)

    def generate(self) -> SecurityEvent:
        return SecurityEvent(
            source=self.source,
            event_type=next(self.types),
            payload={"user": "fictional-alex", "source_ip": "192.0.2.10"},
        )
