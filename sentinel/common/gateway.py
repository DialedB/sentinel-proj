"""Subject contract for the central service and its remote HTTP proxy."""

from abc import ABC, abstractmethod
from uuid import UUID

from sentinel.common.tasks import SecurityEvent, Task, TaskResult
from sentinel.common.transport import Registration


class ServiceGateway(ABC):
    """Application operations independent of serialization and connection ownership.

    Expected application failures raise DomainError with a stable code, message,
    next action and status. Remote connection/deadline failures may raise
    httpx.HTTPError; unexpected programming errors propagate to the lifecycle.
    """

    @abstractmethod
    async def submit_event(self, event: SecurityEvent) -> Task:
        raise NotImplementedError

    @abstractmethod
    async def claim_task(self, worker_id: str) -> Task | None:
        raise NotImplementedError

    @abstractmethod
    async def submit_result(self, task_id: UUID, result: TaskResult) -> Task:
        raise NotImplementedError

    @abstractmethod
    async def register(self, registration: Registration) -> None:
        raise NotImplementedError

    @abstractmethod
    async def heartbeat(self, service_id: str) -> None:
        raise NotImplementedError
