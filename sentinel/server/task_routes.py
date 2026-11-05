"""Task HTTP API; registration is identity validation, not authentication."""
from uuid import UUID
from fastapi import HTTPException, Query, Response
from sentinel.common.models import ServiceRole
from sentinel.common.tasks import SecurityEvent, Task, TaskResult


def install_task_routes(app, queue, registry):
    def worker(worker_id):
        records = {r.service_id: r for r in registry.snapshot()}
        if worker_id not in records:
            raise HTTPException(404, 'Unknown worker ID')
        if records[worker_id].role != ServiceRole.WORKER:
            raise HTTPException(409, 'Registered service is not a worker')

    def operation(call):
        try:
            return call()
        except KeyError as error:
            raise HTTPException(404, 'Unknown task ID') from error
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    @app.post('/events', response_model=Task)
    async def submit(event: SecurityEvent):
        return operation(lambda: queue.submit(event))

    @app.get('/tasks/next', response_model=Task)
    async def claim(worker_id: str = Query(min_length=1, pattern=r'\S')):
        worker(worker_id)
        task = operation(lambda: queue.claim(worker_id))
        return task if task is not None else Response(status_code=204)

    @app.get('/tasks', response_model=list[Task])
    async def tasks():
        return queue.snapshot()

    @app.get('/stats')
    async def stats():
        return queue.stats()

    @app.get('/tasks/{task_id}', response_model=Task)
    async def task(task_id: UUID):
        return operation(lambda: queue.get(task_id))

    @app.post('/tasks/{task_id}/result', response_model=Task)
    async def result(task_id: UUID, result: TaskResult):
        worker(result.worker_id)
        return operation(lambda: queue.finish(task_id, result))
