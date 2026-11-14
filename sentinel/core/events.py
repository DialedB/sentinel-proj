"""Immutable lifecycle contracts, distinct from producer security events."""

from datetime import datetime, UTC
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel.common.tasks import utcnow


class TaskEventType(str, Enum):
    CREATED = "task.created"
    READY = "task.ready"
    STARTED = "task.started"
    COMPLETED = "task.completed"
    FAILED = "task.failed"
    REQUEUED = "task.requeued"


class LifecycleMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    security_event_id: UUID
    source: str = Field(min_length=1, pattern=r"\S")
    command: str = Field(min_length=1, pattern=r"\S")
    worker_id: str | None = Field(default=None, min_length=1, pattern=r"\S")
    attempts: int = Field(ge=0, strict=True)
    max_attempts: int = Field(gt=0, strict=True)
    recovery_count: int = Field(default=0, ge=0, strict=True)
    reason: str = Field(min_length=1, pattern=r"\S")
    error: str | None = None
    lease_expires_at: datetime | None = None

    @field_validator("lease_expires_at")
    @classmethod
    def aware_expiry(cls, value: datetime | None) -> datetime | None:
        return aware_utc(value) if value is not None else None


def aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone aware")
    return value.astimezone(UTC)


class TaskLifecycleEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    event_id: UUID = Field(default_factory=uuid4)
    task_id: UUID
    event_type: TaskEventType
    timestamp: datetime = Field(default_factory=utcnow)
    payload: LifecycleMetadata
    sequence: int | None = Field(default=None, gt=0, strict=True)

    @field_validator("timestamp", mode="before")
    @classmethod
    def timestamp_input(cls, value: object) -> object:
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamp must be an aware datetime or ISO timestamp")
        return value

    @field_validator("timestamp")
    @classmethod
    def timestamp_utc(cls, value: datetime) -> datetime:
        return aware_utc(value)
