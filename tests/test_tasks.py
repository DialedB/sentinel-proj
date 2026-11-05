"""Phase 5 domain, atomic queue, command and HTTP contracts."""
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from uuid import uuid4
from fastapi.testclient import TestClient
from pydantic import ValidationError
from sentinel.common.tasks import (SecurityEvent, Task, TaskState, TaskResult,
                                   Success, Failure, EventType)
from sentinel.common.config import ServiceConfig
from sentinel.common.models import ServiceRole
from sentinel.core.service import ServerService
from sentinel.server.tasks import TaskQueue
from sentinel.worker.commands import SecurityEvaluator, WorkerExecutor, AnalyzeSecurityEventCommand


def event(**changes):
    return SecurityEvent(**dict(producer_id='producer-1', event_type='benign',
                               payload={'nested': {'value': 1}}, **changes))


def success(task, worker='w'):
    return TaskResult(worker_id=worker, outcome=Success(status='success', result={
        'event_id': str(task.event.event_id)}))


class DomainTests(unittest.TestCase):
    def test_event_validation_and_utc(self):
        first = event()
        self.assertEqual(first.timestamp.utcoffset(), timedelta(0))
        self.assertNotEqual(first.event_id, event().event_id)
        for field, value in [('timestamp', True), ('timestamp', 123),
                             ('timestamp', datetime(2026, 1, 1)), ('event_id', 'bad'),
                             ('event_id', True), ('event_type', 'invalid'),
                             ('payload', []), ('producer_id', ' ')]:
            data = first.model_dump()
            data[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValidationError):
                SecurityEvent.model_validate(data)
        data = first.model_dump()
        data['timestamp'] = '2026-01-01T03:00:00+03:00'
        self.assertEqual(SecurityEvent(**data).timestamp.hour, 0)

    def test_explicit_lifecycle_and_no_terminal_transitions(self):
        initial = Task(event=event())
        with self.assertRaises(ValueError):
            initial.claim('w')
        ready = initial.ready()
        running = ready.claim('w')
        with self.assertRaises(ValueError):
            running.finish(success(running, 'other'))
        complete = running.finish(success(running))
        failed = running.finish(TaskResult(worker_id='w', outcome=Failure(status='failure', error='bad')))
        self.assertEqual(complete.state, TaskState.COMPLETED)
        self.assertEqual(failed.state, TaskState.FAILED)
        for terminal in (complete, failed):
            for call in (terminal.ready, lambda: terminal.claim('w'), lambda: terminal.finish(success(terminal))):
                with self.assertRaises(ValueError):
                    call()
        self.assertEqual(initial.state, TaskState.PENDING)
        with self.assertRaises(ValidationError):
            initial.state = TaskState.READY

    def test_result_union_requires_exactly_one(self):
        for outcome in ({'status': 'success'}, {'status': 'failure'},
                        {'status': 'success', 'result': {}, 'error': 'bad'},
                        {'status': 'failure', 'error': 'bad', 'result': {}},
                        {'status': 'failure', 'error': ' '}):
            with self.assertRaises(ValidationError):
                TaskResult(worker_id='w', outcome=outcome)

    def test_commands_evaluator_and_unknown(self):
        executor = WorkerExecutor()
        for kind, score in [(EventType.BENIGN, 0), (EventType.SUSPICIOUS, 40), (EventType.HIGH_RISK, 80)]:
            e = SecurityEvent(producer_id='p', event_type=kind, payload={})
            result = executor.execute('analyze_security_event', e.model_dump(mode='json'))
            self.assertEqual(result['score'], score)
            self.assertEqual(result['rule'], kind.value)
            self.assertEqual(result['event_id'], str(e.event_id))
            self.assertEqual(datetime.fromisoformat(result['timestamp']).utcoffset(), timedelta(0))
        with self.assertRaisesRegex(ValueError, 'Unknown command'):
            executor.execute('missing', {})
        class Evaluator:
            def evaluate(self, value):
                return {'delegated': str(value.event_id)}
        e = event()
        self.assertEqual(AnalyzeSecurityEventCommand(Evaluator()).execute(e.model_dump()),
                         {'delegated': str(e.event_id)})


class QueueTests(unittest.TestCase):
    def test_fifo_dedup_conflicts_and_copy_isolation(self):
        q = TaskQueue()
        e = event()
        first = q.submit(e)
        self.assertEqual(q.submit(e).task_id, first.task_id)
        changed = e.model_copy(deep=True)
        changed.payload['nested']['value'] = 2
        with self.assertRaises(ValueError):
            q.submit(changed)
        changed.payload['nested']['value'] = True
        with self.assertRaises(ValueError):
            q.submit(changed)
        e.payload['nested']['value'] = 3
        first.event.payload.clear()
        self.assertEqual(q.get(first.task_id).event.payload['nested']['value'], 1)
        second = q.submit(event())
        self.assertEqual(q.claim('w').task_id, first.task_id)
        self.assertEqual(q.claim('w2').task_id, second.task_id)
        self.assertIsNone(q.claim('w'))
        with self.assertRaises(ValueError):
            q.finish(first.task_id, success(q.get(first.task_id), 'w2'))
        result = success(q.get(first.task_id))
        q.finish(first.task_id, result)
        result.outcome.result.clear()
        self.assertIn('event_id', q.get(first.task_id).outcome.result)
        with self.assertRaises(ValueError):
            q.finish(first.task_id, success(q.get(first.task_id)))
        self.assertEqual(q.stats(), dict(total=2, pending=0, ready=0, running=1,
                                        completed=1, failed=0, queue_depth=0))
        snapshot = q.snapshot()
        snapshot[0].event.payload.clear()
        snapshot.clear()
        self.assertTrue(q.get(first.task_id).event.payload)
        with self.assertRaises(KeyError):
            q.get(uuid4())

    def test_concurrent_atomic_claims_and_submissions(self):
        q = TaskQueue()
        e = event()
        with ThreadPoolExecutor(max_workers=16) as pool:
            ids = list(pool.map(lambda _: q.submit(e).task_id, range(100)))
        self.assertEqual(len(set(ids)), 1)
        for _ in range(49):
            q.submit(event())
        with ThreadPoolExecutor(max_workers=16) as pool:
            claimed = list(pool.map(lambda i: q.claim(f'w{i}'), range(100)))
        tasks = [t for t in claimed if t]
        self.assertEqual(len(tasks), 50)
        self.assertEqual(len({t.task_id for t in tasks}), 50)
        self.assertEqual(q.stats()['running'], 50)


class TaskAPITests(unittest.TestCase):
    def setUp(self):
        self.service = ServerService(ServiceConfig('s', ServiceRole.SERVER, 'localhost', 8000, 'http://central'))
        self.client = TestClient(self.service.create_app())
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)
        for name, role in [('w', 'worker'), ('w2', 'worker'), ('p', 'producer')]:
            self.client.post('/register', json=dict(service_id=name, role=role, host='localhost', port=8001))

    def test_pipeline_identity_dedup_conflicts_and_stats(self):
        c = self.client
        self.assertEqual(c.get('/tasks/next').status_code, 422)
        self.assertEqual(c.get('/tasks/next?worker_id=unknown').status_code, 404)
        self.assertEqual(c.get('/tasks/next?worker_id=p').status_code, 409)
        empty = c.get('/tasks/next?worker_id=w')
        self.assertEqual((empty.status_code, empty.content), (204, b''))
        e = event().model_dump(mode='json')
        task = c.post('/events', json=e).json()
        self.assertEqual(task['state'], 'READY')
        self.assertEqual(c.post('/events', json=e).json()['task_id'], task['task_id'])
        self.assertEqual(c.post('/events', json={**e, 'payload': {}}).status_code, 409)
        task = c.get('/tasks/next?worker_id=w').json()
        path = '/tasks/' + task['task_id'] + '/result'
        body = dict(worker_id='w', outcome=dict(status='success', result={'event_id': e['event_id']}))
        self.assertEqual(c.post(path, json={**body, 'worker_id': 'w2'}).status_code, 409)
        self.assertEqual(c.post(path, json={**body, 'worker_id': 'p'}).status_code, 409)
        self.assertEqual(c.post(path, json={**body, 'worker_id': 'missing'}).status_code, 404)
        self.assertEqual(c.post(path, json={**body, 'outcome': dict(status='success', result={})}).status_code, 409)
        self.assertEqual(c.post('/tasks/' + str(uuid4()) + '/result', json=body).status_code, 404)
        self.assertEqual(c.post(path, json=body).json()['state'], 'COMPLETED')
        self.assertEqual(c.post(path, json=body).status_code, 409)
        self.assertEqual(c.get('/stats').json()['completed'], 1)
        self.assertEqual(len(c.get('/tasks').json()), 1)
        self.assertEqual(c.get('/tasks/' + task['task_id']).json()['worker_id'], 'w')
        self.assertEqual(c.get('/tasks/' + str(uuid4())).status_code, 404)
        self.assertEqual(c.post('/events', json={**e, 'timestamp': True}).status_code, 422)

    def test_failure_route(self):
        self.client.post('/events', json=event().model_dump(mode='json'))
        task = self.client.get('/tasks/next?worker_id=w').json()
        response = self.client.post('/tasks/' + task['task_id'] + '/result', json={
            'worker_id': 'w', 'outcome': {'status': 'failure', 'error': 'unknown command'}})
        self.assertEqual(response.json()['state'], 'FAILED')
        self.assertEqual(self.client.get('/stats').json()['failed'], 1)


class PipelineConfigTests(unittest.TestCase):
    def test_defaults_overrides_and_validation(self):
        config = ServiceConfig.from_env(ServiceRole.WORKER, {})
        self.assertEqual((config.event_generation_interval, config.worker_poll_interval, config.processing_delay), (2, 1, 0))
        loaded = ServiceConfig.from_env(ServiceRole.WORKER, {'EVENT_GENERATION_INTERVAL': '.5', 'WORKER_POLL_INTERVAL': '.2', 'PROCESSING_DELAY': '.1'})
        self.assertEqual((loaded.event_generation_interval, loaded.worker_poll_interval, loaded.processing_delay), (.5, .2, .1))
        for name in ('event_generation_interval', 'worker_poll_interval', 'processing_delay'):
            for value in (True, -1, float('nan'), float('inf'), '1'):
                with self.assertRaises(ValueError):
                    replace(config, **{name: value})
        for name in ('EVENT_GENERATION_INTERVAL', 'WORKER_POLL_INTERVAL'):
            with self.assertRaises(ValueError):
                ServiceConfig.from_env(ServiceRole.WORKER, {name: '0'})
