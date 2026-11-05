"""Single-process lock-protected FIFO queue. No execution occurs here."""
from collections import deque
import json
from threading import Lock
from uuid import UUID
from sentinel.common.tasks import SecurityEvent, Task, TaskResult, TaskState


class TaskQueue:
    def __init__(self):
        self._lock = Lock()
        self._tasks = {}
        self._events = {}
        self._ready = deque()

    def submit(self, event: SecurityEvent) -> Task:
        with self._lock:
            if event.event_id in self._events:
                task = self._tasks[self._events[event.event_id]]
                # JSON comparison distinguishes booleans from numbers (True == 1 in Python).
                if (json.dumps(task.event.model_dump(mode='json'), sort_keys=True)
                        != json.dumps(event.model_dump(mode='json'), sort_keys=True)):
                    raise ValueError('Event ID already exists with different content')
            else:
                task = Task(event=event.model_copy(deep=True)).ready()
                self._tasks[task.task_id] = task
                self._events[event.event_id] = task.task_id
                self._ready.append(task.task_id)
            return task.model_copy(deep=True)

    def claim(self, worker_id: str) -> Task | None:
        with self._lock:
            if not self._ready:
                return None
            task_id = self._ready[0]
            task = self._tasks[task_id].claim(worker_id)
            self._ready.popleft()
            self._tasks[task_id] = task
            return task.model_copy(deep=True)

    def finish(self, task_id: UUID, result: TaskResult) -> Task:
        with self._lock:
            task = self._tasks[task_id].finish(result)
            self._tasks[task_id] = task
            return task.model_copy(deep=True)

    def get(self, task_id: UUID) -> Task:
        with self._lock:
            return self._tasks[task_id].model_copy(deep=True)

    def snapshot(self) -> list[Task]:
        with self._lock:
            return [task.model_copy(deep=True) for task in self._tasks.values()]

    def stats(self):
        with self._lock:
            return dict(total=len(self._tasks), queue_depth=len(self._ready), **{
                state.value.lower(): sum(t.state == state for t in self._tasks.values())
                for state in TaskState})
