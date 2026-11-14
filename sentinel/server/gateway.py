"""Local Subject behind the HTTP routes; the remote proxy shares this contract."""

from uuid import UUID

from sentinel.common.errors import DomainError, application_errors
from sentinel.common.gateway import ServiceGateway
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import SecurityEvent, Task, TaskResult
from sentinel.common.transport import Registration
from sentinel.reliability.recovery import storage_call
from sentinel.server.registry import ServiceRegistry
from sentinel.storage.task_repository import TaskRepository


class LocalServiceGateway(ServiceGateway):
    def __init__(self, tasks: TaskRepository, registry: ServiceRegistry) -> None:
        self.tasks = tasks
        self.registry = registry

    def _require_worker(self, worker_id: str) -> None:
        records = {record.service_id: record for record in self.registry.snapshot()}
        if worker_id not in records:
            raise DomainError(
                "unknown_worker", "Unknown worker ID", "Register the worker first.", 404
            )
        if records[worker_id].role != ServiceRole.WORKER:
            raise DomainError(
                "wrong_role",
                "Registered service is not a worker",
                "Use a registered worker ID.",
            )

    async def submit_event(self, event: SecurityEvent) -> Task:
        with application_errors("task"):
            return await storage_call(lambda: self.tasks.submit(event))

    async def claim_task(self, worker_id: str) -> Task | None:
        self._require_worker(worker_id)
        with application_errors("task"):
            return await storage_call(lambda: self.tasks.claim(worker_id))

    async def submit_result(self, task_id: UUID, result: TaskResult) -> Task:
        self._require_worker(result.worker_id)
        with application_errors("task"):
            return await storage_call(lambda: self.tasks.finish(task_id, result))

    async def register(self, registration: Registration) -> None:
        with application_errors("service"):
            self.registry.register(registration)

    async def heartbeat(self, service_id: str) -> None:
        with application_errors("service"):
            self.registry.heartbeat(service_id)
