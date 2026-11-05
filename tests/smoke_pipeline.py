"""Bounded six-independent-process Phase 5 smoke; no Docker or third-party client."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]


def get(port, path):
    with urlopen(f'http://127.0.0.1:{port}{path}', timeout=1) as response:
        return json.load(response)


def exercise(timeout=35, base_port=18760):
    entries = [('server-1', 'server'), ('producer-1', 'producer'),
               ('producer-2', 'producer'), ('worker-1', 'worker'),
               ('worker-2', 'worker'), ('watchdog', 'watchdog')]
    for offset in range(6):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', base_port + offset))
    children = []
    with tempfile.TemporaryDirectory(prefix='sentinel-pipeline-') as directory:
        try:
            for offset, (identity, role) in enumerate(entries):
                env = dict(os.environ, SERVICE_ID=identity, SERVICE_ROLE=role,
                    SERVICE_HOST='127.0.0.1', SERVICE_ADVERTISE_HOST='127.0.0.1',
                    SERVICE_PORT=str(base_port + offset), MAIN_SERVER_ID='server-1',
                    MAIN_SERVER_URL=f'http://127.0.0.1:{base_port}',
                    HEARTBEAT_INTERVAL='.2', HTTP_TIMEOUT='1', MONITOR_INTERVAL='.2',
                    HEALTH_CHECK_TIMEOUT='1', HEARTBEAT_TIMEOUT='3', HEALTH_CHECK_STRATEGY='http',
                    EVENT_GENERATION_INTERVAL='.15', WORKER_POLL_INTERVAL='.07', PROCESSING_DELAY='.04')
                with open(Path(directory) / f'{identity}.log', 'w') as log:
                    children.append(subprocess.Popen([sys.executable, '-m', f'sentinel.{role}.main'],
                        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT))
            deadline = time.monotonic() + timeout
            baseline = None
            error = None
            while time.monotonic() < deadline:
                try:
                    assert all(p.poll() is None for p in children), 'Child exited unexpectedly'
                    for offset, (identity, _) in enumerate(entries):
                        health = get(base_port + offset, '/health')
                        assert health['service_id'] == identity and health['status'] == 'healthy'
                    services = get(base_port, '/services')
                    assert {s['service_id'] for s in services} == {e[0] for e in entries[1:5]}
                    tasks = get(base_port, '/tasks')
                    complete = [t for t in tasks if t['state'] == 'COMPLETED']
                    assert {t['event']['producer_id'] for t in complete} == {'producer-1', 'producer-2'}
                    assert {t['worker_id'] for t in complete} == {'worker-1', 'worker-2'}
                    assert all(t['outcome']['result']['event_id'] == t['event']['event_id'] for t in complete)
                    statuses = get(base_port + 5, '/status')
                    assert len(statuses) == 5 and all(s['status'] == 'healthy' and s['discoverable'] and s['last_checked'] for s in statuses)
                    stats = get(base_port, '/stats')
                    assert stats['failed'] == 0 and stats['total'] == sum(stats[k] for k in ('pending', 'ready', 'running', 'completed', 'failed'))
                    beats = {s['service_id']: s['last_seen'] for s in services}
                    if baseline is None:
                        baseline = (stats, beats)
                    elif stats['completed'] > baseline[0]['completed'] and all(beats[k] > v for k, v in baseline[1].items()):
                        print(json.dumps(dict(before=baseline[0], after=stats, producers=sorted({t['event']['producer_id'] for t in complete}),
                            workers=sorted({t['worker_id'] for t in complete}), heartbeats_advanced=sorted(beats), watchdog_healthy=len(statuses)), indent=2))
                        break
                except (OSError, ValueError, KeyError, AssertionError) as caught:
                    error = caught
                time.sleep(.1)
            else:
                raise RuntimeError(f'Pipeline smoke timed out after {timeout}s; last error: {error!r}')
        except BaseException:
            for identity, _ in entries:
                path = Path(directory) / f'{identity}.log'
                if path.exists():
                    print(f'--- {identity} ---\n{path.read_text()[-6000:]}', file=sys.stderr)
            raise
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate()
            for child in children:
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            assert all(child.poll() is not None for child in children)
            print(f'Cleaned up {len(children)} child processes')
            for identity, _ in entries:
                path = Path(directory) / f'{identity}.log'
                if path.exists():
                    text = path.read_text()
                    for warning in ('Task was destroyed', 'Unclosed client', 'Unclosed transport',
                                    'Task exception was never retrieved'):
                        assert warning not in text, f'{identity}: {warning}'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=float, default=35)
    parser.add_argument('--base-port', type=int, default=18760)
    args = parser.parse_args()
    if not 0 < args.timeout <= 300 or not 1024 <= args.base_port <= 65530:
        parser.error('timeout must be 0..300 seconds and base-port 1024..65530')
    exercise(args.timeout, args.base_port)
