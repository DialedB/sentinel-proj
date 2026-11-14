"""Validated event transport and explicit task state machine."""

from datetime import datetime, UTC
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from sentinel.common.json_payload import validate_payload
from sentinel.common.limits import MAX_SOURCE_LENGTH
from sentinel.common.task_states import handler_for


def utcnow() -> datetime:
    return datetime.now(UTC)


class EventType(str, Enum):
    LOGIN_FAILED = "login_failed"
    LOGIN_SUCCESS = "login_success"
    ACCESS_DENIED = "access_denied"


class SecurityEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    event_id: UUID = Field(default_factory=uuid4)
    source: str = Field(min_length=1, max_length=MAX_SOURCE_LENGTH, pattern=r"\S")
    event_type: EventType
    timestamp: datetime = Field(default_factory=utcnow)
    payload: dict[str, Any]

    _payload_json = field_validator("payload", mode="before")(validate_payload)

    @field_validator("timestamp", mode="before")
    @classmethod
    def timestamp_input(cls, value: object) -> str | datetime:
        if not isinstance(value, (str, datetime)):
            raise ValueError("timestamp must be an aware datetime or ISO timestamp")
        return value

    @field_validator("timestamp")
    @classmethod
    def aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must be timezone aware")
        try:
            return value.astimezone(UTC)
        except (OverflowError, ValueError) as error:
            raise ValueError("timestamp is outside the UTC datetime range") from error


class Success(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["success"]
    result: dict[str, Any]

    _result_json = field_validator("result", mode="before")(validate_payload)


class Failure(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["failure"]
    error: str = Field(min_length=1, pattern=r"\S")


Outcome = Annotated[Success | Failure, Field(discriminator="status")]


class TaskResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    worker_id: str = Field(min_length=1, pattern=r"\S")
    lease_token: UUID
    outcome: Outcome


class TaskState(str, Enum):
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Task(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    task_id: UUID = Field(default_factory=uuid4)
    event: SecurityEvent
    command: str = "analyze_security_event"
    state: TaskState = TaskState.PENDING
    worker_id: str | None = None
    outcome: Outcome | None = None
    attempts: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=3, gt=0, strict=True)
    lease_token: UUID | None = None
    lease_expires_at: datetime | None = None
    recovery_count: int = Field(default=0, ge=0)
    error: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    def _transition(
        self,
        expected: TaskState,
        target: TaskState,
        now: datetime | None = None,
        **changes: object,
    ) -> Task:
        if self.state != expected:
            raise ValueError(
                f"Cannot transition {self.state.value} to {target.value}; "
                f"expected {expected.value}"
            )
        return self.model_copy(
            deep=True, update=dict(state=target, updated_at=now or utcnow(), **changes)
        )

    def ready(self, now: datetime | None = None) -> Task:
        return handler_for(self).ready(self, now or utcnow())

    def claim(
        self,
        worker_id: str,
        lease_seconds: float = 30,
        now: datetime | None = None,
    ) -> Task:
        return handler_for(self).claim(self, worker_id, lease_seconds, now or utcnow())

    def finish(self, result: TaskResult, now: datetime | None = None) -> Task:
        return handler_for(self).finish(self, result, now or utcnow())

    def recover_expired(self, now: datetime | None = None) -> Task:
        return handler_for(self).recover(self, now or utcnow())
