"""Validated event transport and explicit task state machine."""

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventType(str, Enum):
    LOGIN_FAILED = "login_failed"
    LOGIN_SUCCESS = "login_success"
    ACCESS_DENIED = "access_denied"


class SecurityEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    event_id: UUID = Field(default_factory=uuid4)
    source: str = Field(min_length=1, pattern=r"\S")
    event_type: EventType
    timestamp: datetime = Field(default_factory=utcnow)
    payload: dict[str, Any]

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
        return value.astimezone(timezone.utc)


class Success(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    status: Literal["success"]
    result: dict[str, Any]


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
    ) -> "Task":
        if self.state != expected:
            raise ValueError(
                f"Cannot transition {self.state.value} to {target.value}; "
                f"expected {expected.value}"
            )
        return self.model_copy(
            deep=True, update=dict(state=target, updated_at=now or utcnow(), **changes)
        )

    def ready(self, now: datetime | None = None) -> "Task":
        return self._transition(TaskState.PENDING, TaskState.READY, now=now)

    def claim(
        self, worker_id: str, lease_seconds: float = 30,
        now: datetime | None = None,
    ) -> "Task":
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        now = now or utcnow()
        if self.attempts >= self.max_attempts:
            raise ValueError("Task attempts exhausted")
        return self._transition(
            TaskState.READY, TaskState.RUNNING, now=now, worker_id=worker_id,
            attempts=self.attempts + 1, lease_token=uuid4(),
            lease_expires_at=now + timedelta(seconds=lease_seconds),
        )

    def finish(self, result: TaskResult, now: datetime | None = None) -> "Task":
        now = now or utcnow()
        if (self.state != TaskState.RUNNING
                or self.worker_id != result.worker_id
                or self.lease_token != result.lease_token
                or self.lease_expires_at is None or now >= self.lease_expires_at):
            raise ValueError("Result does not own a current active lease")
        if (
            isinstance(result.outcome, Success)
            and self.command == "analyze_security_event"
            and result.outcome.result.get("event_id") != str(self.event.event_id)
        ):
            raise ValueError("Result event_id does not match task event")
        target = (
            TaskState.COMPLETED
            if isinstance(result.outcome, Success)
            else (TaskState.READY if self.attempts < self.max_attempts else TaskState.FAILED)
        )
        changes: dict[str, Any] = dict(
            outcome=result.outcome.model_copy(deep=True),
            error=result.outcome.error if isinstance(result.outcome, Failure) else None,
            lease_token=None, lease_expires_at=None,
        )
        if target == TaskState.READY:
            changes["worker_id"] = None
        return self._transition(TaskState.RUNNING, target, now=now, **changes)

    def recover_expired(self, now: datetime | None = None) -> "Task":
        now = now or utcnow()
        if (self.state != TaskState.RUNNING or self.lease_expires_at is None
                or self.lease_expires_at > now):
            raise ValueError("Only an expired RUNNING task may be recovered")
        target = TaskState.READY if self.attempts < self.max_attempts else TaskState.FAILED
        failure = Failure(status="failure", error="Task lease expired")
        return self._transition(
            TaskState.RUNNING, target, now=now, worker_id=None,
            lease_token=None, lease_expires_at=None,
            recovery_count=self.recovery_count + 1,
            error=failure.error, outcome=failure,
        )
