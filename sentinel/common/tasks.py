"""Validated event transport and explicit task state machine."""

from datetime import datetime, timezone
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
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    def _transition(
        self,
        expected: TaskState,
        target: TaskState,
        **changes: object,
    ) -> "Task":
        if self.state != expected:
            raise ValueError(
                f"Cannot transition {self.state.value} to {target.value}; "
                f"expected {expected.value}"
            )
        return self.model_copy(
            deep=True, update=dict(state=target, updated_at=utcnow(), **changes)
        )

    def ready(self) -> "Task":
        return self._transition(TaskState.PENDING, TaskState.READY)

    def claim(self, worker_id: str) -> "Task":
        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        return self._transition(TaskState.READY, TaskState.RUNNING, worker_id=worker_id)

    def finish(self, result: TaskResult) -> "Task":
        if self.worker_id != result.worker_id:
            raise ValueError("Result worker does not own task")
        if (
            isinstance(result.outcome, Success)
            and self.command == "analyze_security_event"
        ):
            if result.outcome.result.get("event_id") != str(self.event.event_id):
                raise ValueError("Result event_id does not match task event")
        target = (
            TaskState.COMPLETED
            if isinstance(result.outcome, Success)
            else TaskState.FAILED
        )
        return self._transition(
            TaskState.RUNNING, target, outcome=result.outcome.model_copy(deep=True)
        )
