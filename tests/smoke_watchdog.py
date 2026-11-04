"""Manual bounded independent-process watchdog recovery test (both policies)."""

import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile


from smoke_communication import ROOT, eventually, get


def exercise(strategy: str) -> None:
    ports = {"server": 8000, "producer": 8001, "worker": 8002, "watchdog": 8005}
    for port in ports.values():
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    entries = []
    with tempfile.TemporaryDirectory(prefix="sentinel-watchdog-") as directory:
        def start(role: str):
            log = open(Path(directory) / f"{role}-{len(entries)}.log", "w+")
            env = {**os.environ, "SERVICE_ID": f"smoke-{role}", "SERVICE_ROLE": role,
                   "SERVICE_HOST": "127.0.0.1", "SERVICE_PORT": str(ports[role]),
                   "MAIN_SERVER_ID": "smoke-server", "MAIN_SERVER_URL": "http://127.0.0.1:8000",
                   "HEARTBEAT_INTERVAL": "0.1", "HTTP_TIMEOUT": "0.2",
                   "MONITOR_INTERVAL": "0.1", "HEALTH_CHECK_TIMEOUT": "0.2",
                   "HEARTBEAT_TIMEOUT": "0.6", "HEALTH_CHECK_STRATEGY": strategy}
            process = subprocess.Popen([sys.executable, "-m", f"sentinel.{role}.main"],
                                       cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            entry = (role, process, log)
            entries.append(entry)
            eventually(lambda: get(ports[role], "/health"))
            return entry

        def stop(entry) -> None:
            role, process, log = entry
            if process.poll() is not None:
                return
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
                raise AssertionError(f"{role} forced termination")
            log.flush()
            log.seek(0)
            output = log.read()
            assert process.returncode in (0, -signal.SIGTERM), output
            assert "Shutdown complete service_id=" in output, output
            assert "Application shutdown complete" in output, output
            # Expected discovery exceptions are logged with tracebacks during central outage.
            lines = output.splitlines()
            unexpected = [line for line in lines if "ERROR" in line and
                          "Central discovery failed" not in line]
            assert not unexpected, output
            for warning in ("Task was destroyed", "never retrieved", "Unclosed", "never awaited"):
                assert warning not in output, output

        def status(role: str):
            return get(8005, f"/status/smoke-{role}")

        try:
            central = start("server")
            start("producer")
            worker = start("worker")
            start("watchdog")
            eventually(lambda: all(status(role)["status"] == "healthy" and
                                   status(role)["discoverable"]
                                   for role in ("server", "producer", "worker")))
            print(f"{strategy}: discovery and all health checks passed")
            stop(worker)
            eventually(lambda: status("worker")["status"] == "unhealthy")
            start("worker")
            eventually(lambda: status("worker")["status"] == "healthy")
            print(f"{strategy}: worker failure and same-ID/port recovery passed")
            before = status("worker")["last_checked"]
            stop(central)
            eventually(lambda: status("server")["status"] == "unhealthy")
            eventually(lambda: status("worker")["last_checked"] > before and
                       not status("worker")["discoverable"])
            assert get(8005, "/health")["status"] == "healthy"
            assert len(get(8005, "/status")) == 3
            if strategy == "http":
                assert status("worker")["status"] == "healthy"
                assert status("producer")["status"] == "healthy"
            else:
                eventually(lambda: all(status(role)["status"] == "unhealthy"
                                       for role in ("worker", "producer")))
            print(f"{strategy}: central outage retained services and continued checks")
        finally:
            errors = []
            for entry in reversed(entries):
                try:
                    stop(entry)
                except Exception as error:
                    errors.append(error)
                finally:
                    entry[2].close()
            if errors:
                raise AssertionError(errors)
    print(f"{strategy}: graceful shutdown, no leaked-client/task warnings")


def main() -> None:
    for strategy in ("http", "heartbeat"):
        exercise(strategy)
    print("PASS: watchdog independent-process recovery and outage smoke")


if __name__ == "__main__":
    main()
