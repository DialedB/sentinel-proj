"""Bounded real-process smoke test; run explicitly, not during unit discovery."""

from datetime import datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[1]


def get(port, path):
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=0.5) as response:
        return json.load(response)


def eventually(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.05)
    raise AssertionError("Smoke check did not succeed before its deadline")


def main():
    # Fail clearly rather than accidentally talking to unrelated local services.
    for port in (8000, 8001, 8002):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    processes = []
    with tempfile.TemporaryDirectory(prefix="sentinel-smoke-") as directory:
        def start(role, generation=""):
            log = open(Path(directory) / f"{role}{generation}.log", "w+")
            env = {**os.environ, "SERVICE_ID": f"smoke-{role}", "SERVICE_ROLE": role,
                   "SERVICE_HOST": "127.0.0.1", "SERVICE_PORT": str({"server": 8000,
                   "producer": 8001, "worker": 8002}[role]),
                   "MAIN_SERVER_URL": "http://127.0.0.1:8000",
                   "HEARTBEAT_INTERVAL": "0.2", "HTTP_TIMEOUT": "0.3"}
            process = subprocess.Popen(
                [sys.executable, "-m", f"sentinel.{role}.main"], cwd=ROOT,
                env=env, stdout=log, stderr=subprocess.STDOUT,
            )
            entry = (role + generation, process, log)
            processes.append(entry)
            return entry

        def stop(entry):
            name, process, log = entry
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
                raise AssertionError(f"{name} required forced termination")
            log.flush()
            log.seek(0)
            output = log.read()
            assert "Shutdown complete service_id=" in output, output
            assert "Application shutdown complete" in output, output
            assert "Traceback" not in output and "ERROR" not in output, output
            # Uvicorn may re-raise the received signal after graceful teardown.
            assert process.returncode in (0, -signal.SIGTERM), output
            print(f"{name}: graceful shutdown, returncode={process.returncode}")
            return output

        try:
            producer = start("producer")
            worker = start("worker")
            for role, port in (("producer", 8001), ("worker", 8002)):
                health = eventually(lambda: get(port, "/health"))
                assert health == {"service_id": f"smoke-{role}", "role": role,
                                  "status": "healthy"}, health
                print(f"{role} health while central offline: {health}")
            time.sleep(0.3)
            central = start("server")
            health = eventually(lambda: get(8000, "/health"))
            assert health == {"service_id": "smoke-server", "role": "server",
                              "status": "healthy"}, health
            print(f"server health: {health}")

            def registered():
                records = get(8000, "/services")
                return records if len(records) == 2 else None

            first = eventually(registered)
            assert {row["service_id"] for row in first} == {"smoke-producer", "smoke-worker"}
            before = {row["service_id"]: datetime.fromisoformat(row["last_seen"])
                      for row in first}

            def advanced():
                rows = registered()
                if rows and all(datetime.fromisoformat(row["last_seen"]) >
                                before[row["service_id"]] for row in rows):
                    return rows
                return None

            after = eventually(advanced)
            for row in after:
                print(f"{row['service_id']} last_seen: {before[row['service_id']].isoformat()} -> {row['last_seen']}")
            stop(central)
            central = start("server", "-restart")
            recovered = eventually(registered)
            assert {row["service_id"] for row in recovered} == set(before)
            print("Central restart: both services re-registered without duplicates")
        finally:
            errors = []
            for entry in reversed(processes):
                try:
                    output = stop(entry)
                    if entry[0] in ("producer", "worker"):
                        assert "Registration failed" in output, output
                        print(f"{entry[0]}: startup registration failure and recovery logged")
                except Exception as error:
                    errors.append(error)
                finally:
                    entry[2].close()
            if errors:
                raise AssertionError(errors)
    print("PASS: independent processes, retries, health, advancing heartbeats, restart, clean shutdown")


if __name__ == "__main__":
    main()
