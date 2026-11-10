"""Bounded six-process durability/worker-death smoke using temporary SQLite data."""

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

from smoke_pipeline import ROOT, get, verify_result


def exercise(timeout=60, base_port=18960, *, lifecycle=False):
    dispatch_interval = "3600" if lifecycle else ".05"
    entries = [("server-1", "server"), ("producer-1", "producer"),
               ("producer-2", "producer"), ("worker-1", "worker"),
               ("worker-2", "worker"), ("watchdog", "watchdog")]
    for offset in range(6):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", base_port + offset))
    deadline = time.monotonic() + timeout
    children = []
    active = {}
    with tempfile.TemporaryDirectory(prefix="sentinel-recovery-") as directory:
        def start(offset):
            identity, role = entries[offset]
            env = dict(os.environ, SERVICE_ID=identity, SERVICE_ROLE=role,
                       SERVICE_HOST="127.0.0.1", SERVICE_ADVERTISE_HOST="127.0.0.1",
                       SERVICE_PORT=str(base_port + offset), MAIN_SERVER_ID="server-1",
                       MAIN_SERVER_URL=f"http://127.0.0.1:{base_port}",
                       WATCHDOG_URL=f"http://127.0.0.1:{base_port + 5}",
                       DATABASE_PATH=str(Path(directory) / "tasks.db"),
                       TASK_LEASE_SECONDS="3", TASK_MAX_ATTEMPTS="3",
                       RECOVERY_INTERVAL_SECONDS=".1", HEARTBEAT_INTERVAL=".1",
                       EVENT_DISPATCH_INTERVAL=dispatch_interval,
                       HTTP_TIMEOUT=".5", MONITOR_INTERVAL=".1",
                       HEALTH_CHECK_TIMEOUT=".5", HEARTBEAT_TIMEOUT="2",
                       HEALTH_CHECK_STRATEGY="http", EVENT_GENERATION_INTERVAL=".6",
                       WORKER_POLL_INTERVAL=".05", PROCESSING_DELAY=".5")
            path = Path(directory) / f"{identity}-{len(children)}.log"
            with path.open("w") as log:
                child = subprocess.Popen([sys.executable, "-m", f"sentinel.{role}.main"],
                                         cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            children.append((child, path))
            active[offset] = child
            return child

        def stop(offset, kill=False):
            child = active.pop(offset, None)
            if child is not None and child.poll() is None:
                child.kill() if kill else child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
                    raise AssertionError("Graceful stop timed out")

        def eventually(check):
            last = None
            while time.monotonic() < deadline:
                try:
                    assert all(p.poll() is None for p in active.values()), "Unexpected child exit"
                    value = check()
                    if value:
                        return value
                except (OSError, ValueError, KeyError, AssertionError) as error:
                    last = error
                time.sleep(.03)
            raise AssertionError(f"Recovery smoke deadline exceeded: {last!r}")

        def post(path, body):
            request = Request(f"http://127.0.0.1:{base_port}{path}",
                              data=json.dumps(body).encode(),
                              headers={"Content-Type": "application/json"})
            with urlopen(request, timeout=1) as response:
                return json.load(response)

        def fleet():
            for offset, (identity, role) in enumerate(entries):
                health = get(base_port + offset, "/health")
                assert health == dict(service_id=identity, role=role, status="healthy")
            services = get(base_port, "/services")
            assert {s["service_id"] for s in services} == {e[0] for e in entries[1:5]}
            statuses = get(base_port + 5, "/status")
            assert len(statuses) == 5 and all(s["status"] == "healthy" and s["discoverable"] for s in statuses)
            return {s["service_id"]: s["last_seen"] for s in services}

        try:
            for offset in range(6):
                start(offset)
            eventually(fleet)
            def pipeline():
                completed = [t for t in get(base_port, "/tasks") if t["state"] == "COMPLETED"]
                assert {t["worker_id"] for t in completed} == {"worker-1", "worker-2"}
                assert {t["event"]["source"] for t in completed} == {"producer-1", "producer-2"}
                assert {verify_result(t) for t in completed} == {"BENIGN", "SUSPICIOUS", "HIGH_RISK"}
                return completed
            complete = eventually(pipeline)[0]
            for offset in (1, 2):
                stop(offset)
            eventually(lambda: get(base_port, "/stats")["queue_depth"] == 0 and
                       get(base_port, "/stats")["running"] == 0)
            for offset in (3, 4):
                stop(offset)
            event = dict(event_id=str(uuid4()), source="manual-recovery", event_type="login_success",
                         payload={}, timestamp=complete["event"]["timestamp"])
            submitted = post("/events", event)
            path = f'/tasks/{submitted["task_id"]}'
            start(3)
            def claimed():
                task = get(base_port, path)
                return task if task["state"] == "RUNNING" else None
            old = eventually(claimed)
            assert old["worker_id"] == "worker-1" and old["attempts"] == 1
            stop(3, kill=True)
            stop(0)
            if lifecycle:
                with sqlite3.connect(Path(directory) / "tasks.db") as connection:
                    backlog = connection.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
                    projected = connection.execute("SELECT created FROM event_metrics").fetchone()[0]
                assert backlog > 0 and projected == 0
                print(f"Committed backlog before restart: {backlog} events, zero projected creations")
                dispatch_interval = ".05"
            start(0)
            eventually(lambda: get(base_port, "/health"))
            assert get(base_port, f'/tasks/{complete["task_id"]}') == complete
            assert post("/events", event)["task_id"] == submitted["task_id"]
            start(4)
            newer = eventually(claimed)
            # The persisted old RUNNING lease can still be present before expiry.
            newer = eventually(lambda: (t if t["attempts"] == 2 and t["state"] == "RUNNING" else None)
                               if (t := get(base_port, path)) else None)
            assert newer["worker_id"] == "worker-2" and newer["lease_token"] != old["lease_token"]
            before = get(base_port, path)
            stale = dict(worker_id="worker-1", lease_token=old["lease_token"],
                         outcome=dict(status="failure", error="late result from dead worker"))
            post("/register", dict(service_id="worker-1", role="worker", host="127.0.0.1",
                                   port=base_port + 3))
            try:
                post(path + "/result", stale)
                raise AssertionError("Stale lease accepted")
            except HTTPError as error:
                assert error.code == 409
            assert get(base_port, path) == before
            recovered = eventually(lambda: (t if t["state"] == "COMPLETED" else None)
                                   if (t := get(base_port, path)) else None)
            verify_result(recovered)
            assert recovered["attempts"] == 2 and recovered["recovery_count"] == 1
            history = get(base_port, path + "/events?after_sequence=0")
            assert [e["event_type"] for e in history] == [
                "task.created", "task.ready", "task.started", "task.requeued",
                "task.started", "task.completed"]
            assert history[3]["payload"]["reason"] == "lease_expired"
            assert [e["payload"]["attempts"] for e in history] == [0, 0, 1, 1, 2, 2]
            assert all("lease_token" not in e["payload"] for e in history)
            if lifecycle:
                def projection_caught_up():
                    events, cursor = [], 0
                    while True:
                        page = get(base_port, f"/events/lifecycle?limit=200&after_sequence={cursor}")
                        if not page:
                            break
                        events.extend(page)
                        cursor = page[-1]["sequence"]
                    assert len({e["event_id"] for e in events}) == len(events)
                    expected = {key: 0 for key in ("created", "started", "completed",
                                                  "permanently_failed", "requeued", "retry_attempts")}
                    for item in events:
                        kind = item["event_type"].split(".")[1]
                        if kind != "ready":
                            expected["permanently_failed" if kind == "failed" else kind] += 1
                        if kind == "started" and item["payload"]["attempts"] > 1:
                            expected["retry_attempts"] += 1
                    assert get(base_port, "/metrics/events") == expected
                    with sqlite3.connect(Path(directory) / "tasks.db") as connection:
                        connection.execute("BEGIN")
                        receipts = connection.execute("SELECT COUNT(*) FROM subscriber_receipts").fetchone()[0]
                        acked = connection.execute("SELECT COUNT(*) FROM event_deliveries "
                                                   "WHERE delivered_at IS NOT NULL").fetchone()[0]
                    assert receipts == acked == 2 * len(events)
                    return dict(event_count=len(events), receipts=receipts,
                                acknowledgements=acked, metrics=expected)
                evidence = eventually(projection_caught_up)
            stop(0)
            start(0)
            eventually(lambda: get(base_port, "/health"))
            assert get(base_port, path) == recovered
            assert post("/events", event) == recovered
            assert get(base_port, path + "/events?after_sequence=0") == history
            if lifecycle:
                assert eventually(projection_caught_up) == evidence
                print(json.dumps(dict(lifecycle_replay=evidence, restart_idempotent=True), indent=2))
            for offset in (1, 2, 3):
                start(offset)
            beats = eventually(fleet)
            eventually(lambda: all(value > beats[key] for key, value in fleet().items()))
            print(json.dumps(dict(durable_task_id=recovered["task_id"],
                                  prior_completed_id=complete["task_id"],
                                  old_token=old["lease_token"], new_token=newer["lease_token"],
                                  attempts=recovered["attempts"], recoveries=recovered["recovery_count"],
                                  stale_status=409, healthy_services=6, watchdog_observations=5,
                                  heartbeats_advanced=sorted(beats), stats=get(base_port, "/stats")), indent=2))
        except BaseException:
            for _, path in children:
                print(f"--- {path.name} ---\n{path.read_text()[-4000:]}", file=sys.stderr)
            raise
        finally:
            for child, _ in children:
                if child.poll() is None:
                    child.terminate()
            for child, _ in children:
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
            for _, path in children:
                text = path.read_text()
                for warning in ("Task was destroyed", "Unclosed client", "Unclosed transport",
                                "Task exception was never retrieved", "never awaited"):
                    assert warning not in text, f"{path.name}: {warning}"
            print(f"Cleaned up {len(children)} child process instances; temporary database removed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--base-port", type=int, default=18960)
    args = parser.parse_args()
    if not 0 < args.timeout <= 300 or not 1024 <= args.base_port <= 65530:
        parser.error("timeout must be 0..300 seconds and base-port 1024..65530")
    exercise(args.timeout, args.base_port)
