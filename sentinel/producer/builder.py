"""Fluent event construction; build validates and isolates each product."""

from copy import deepcopy
from datetime import datetime
from typing import Any
from uuid import UUID

from sentinel.common.tasks import EventType, SecurityEvent


class SecurityEventBuilder:
    """Reusable builder with optional replay identity and timestamp."""

    def __init__(self) -> None:
        self._fields: dict[str, Any] = {}

    def from_source(self, source: str) -> SecurityEventBuilder:
        self._fields["source"] = source
        return self

    def of_type(self, event_type: EventType) -> SecurityEventBuilder:
        self._fields["event_type"] = event_type
        return self

    def with_payload(self, payload: dict[str, Any]) -> SecurityEventBuilder:
        self._fields["payload"] = deepcopy(payload)
        return self

    def identified_by(self, event_id: UUID) -> SecurityEventBuilder:
        self._fields["event_id"] = event_id
        return self

    def at(self, timestamp: datetime) -> SecurityEventBuilder:
        self._fields["timestamp"] = timestamp
        return self

    def build(self) -> SecurityEvent:
        return SecurityEvent.model_validate(deepcopy(self._fields))
