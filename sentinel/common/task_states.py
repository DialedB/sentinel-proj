"""Stateless State objects own legal operations; Task remains an immutable context."""

import math
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from sentinel.common.tasks import Task, TaskResult


class TaskStateHandler:
    """Default rejection makes terminal states closed to every operation."""

    def ready(self, task: Task, now: datetime) -> Task:
        raise ValueError(f"Cannot ready a {task.state.value} task")

    def claim(
        self, task: Task, worker_id: str, lease_seconds: float, now: datetime
    ) -> Task:
        raise ValueError(f"Cannot claim a {task.state.value} task")

    def finish(self, task: Task, result: TaskResult, now: datetime) -> Task:
        raise ValueError("Result does not own a current active lease")

    def recover(self, task: Task, now: datetime) -> Task:
        raise ValueError("Only an expired RUNNING task may be recovered")


class PendingState(TaskStateHandler):
    def ready(self, task: Task, now: datetime) -> Task:
        from sentinel.common.tasks import TaskState

        return task._transition(TaskState.PENDING, TaskState.READY, now=now)


class ReadyState(TaskStateHandler):
    def claim(
        self, task: Task, worker_id: str, lease_seconds: float, now: datetime
    ) -> Task:
        from sentinel.common.tasks import TaskState

        if not isinstance(worker_id, str) or not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be finite and positive")
        if task.attempts >= task.max_attempts:
            raise ValueError("Task attempts exhausted")
        try:
            expires = now + timedelta(seconds=lease_seconds)
        except OverflowError as error:
            raise ValueError("lease expiry is out of datetime range") from error
        return task._transition(
            TaskState.READY,
            TaskState.RUNNING,
            now=now,
            worker_id=worker_id,
            attempts=task.attempts + 1,
            lease_token=uuid4(),
            lease_expires_at=expires,
        )


class RunningState(TaskStateHandler):
    def finish(self, task: Task, result: TaskResult, now: datetime) -> Task:
        from sentinel.common.tasks import Failure, Success, TaskState

        if (
            task.worker_id != result.worker_id
            or task.lease_token != result.lease_token
            or task.lease_expires_at is None
            or now >= task.lease_expires_at
        ):
            raise ValueError("Result does not own a current active lease")
        if (
            isinstance(result.outcome, Success)
            and task.command == "analyze_security_event"
            and result.outcome.result.get("event_id") != str(task.event.event_id)
        ):
            raise ValueError("Result event_id does not match task event")
        target = (
            TaskState.COMPLETED
            if isinstance(result.outcome, Success)
            else TaskState.READY
            if task.attempts < task.max_attempts
            else TaskState.FAILED
        )
        changes: dict[str, Any] = dict(
            outcome=result.outcome.model_copy(deep=True),
            error=result.outcome.error if isinstance(result.outcome, Failure) else None,
            lease_token=None,
            lease_expires_at=None,
        )
        if target == TaskState.READY:
            changes["worker_id"] = None
        return task._transition(TaskState.RUNNING, target, now=now, **changes)

    def recover(self, task: Task, now: datetime) -> Task:
        from sentinel.common.tasks import Failure, TaskState

        if task.lease_expires_at is None or task.lease_expires_at > now:
            raise ValueError("Only an expired RUNNING task may be recovered")
        target = (
            TaskState.READY if task.attempts < task.max_attempts else TaskState.FAILED
        )
        failure = Failure(status="failure", error="Task lease expired")
        return task._transition(
            TaskState.RUNNING,
            target,
            now=now,
            worker_id=None,
            lease_token=None,
            lease_expires_at=None,
            recovery_count=task.recovery_count + 1,
            error=failure.error,
            outcome=failure,
        )


class TerminalState(TaskStateHandler):
    """COMPLETED and FAILED share the same no-further-transitions behavior."""


_HANDLERS = {
    "PENDING": PendingState(),
    "READY": ReadyState(),
    "RUNNING": RunningState(),
    "COMPLETED": TerminalState(),
    "FAILED": TerminalState(),
}


def handler_for(task: Task) -> TaskStateHandler:
    return _HANDLERS[task.state.value]
