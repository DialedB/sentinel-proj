"""Read-only, stdlib-only Phase 4 smoke; never starts or stops Docker."""

import argparse
import json
import math
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen


EXPECTED = {"producer-1": "producer", "producer-2": "producer",
            "worker-1": "worker", "worker-2": "worker"}


def get(url, timeout):
    with urlopen(url, timeout=timeout) as response:
        return json.load(response)


def validate(rows, expected, statuses=False):
    if not isinstance(rows, list) or len(rows) != len(expected):
        raise ValueError(f"Expected {len(expected)} records, got {rows!r}")
    if {row["service_id"]: row["role"] for row in rows} != expected:
        raise ValueError(f"Unexpected identities/roles: {rows!r}")
    if any(row["status"] != "healthy" for row in rows):
        raise ValueError(f"Not all services healthy: {rows!r}")
    if statuses and any(not row["discoverable"] or not row["last_checked"]
                        or not row["last_successful"] for row in rows):
        raise ValueError(f"Discovery/checks not ready: {rows!r}")


def check(server_url, watchdog_url, timeout=60):
    deadline = time.monotonic() + timeout
    last_error = "No attempt completed"
    while time.monotonic() < deadline:
        try:
            def fetch(base, path):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Smoke deadline reached")
                return get(base.rstrip("/") + path, min(2, remaining))

            for base, identity, role in ((server_url, "main-server", "server"),
                                         (watchdog_url, "watchdog", "watchdog")):
                health = fetch(base, "/health")
                if health != {"service_id": identity, "role": role, "status": "healthy"}:
                    raise ValueError(f"Unexpected /health: {health!r}")
            validate(fetch(server_url, "/services"), EXPECTED)
            validate(fetch(watchdog_url, "/status"),
                     {"main-server": "server", **EXPECTED}, statuses=True)
            return
        except (URLError, OSError, ValueError, KeyError, TypeError) as error:
            last_error = str(error)
        time.sleep(min(0.5, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"Deployment not ready within {timeout:g}s: {last_error}")


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", default="http://127.0.0.1:8000")
    parser.add_argument("--watchdog-url", default="http://127.0.0.1:8005")
    parser.add_argument("--timeout", type=positive, default=60)
    args = parser.parse_args()
    try:
        check(args.server_url, args.watchdog_url, args.timeout)
    except TimeoutError as error:
        print(f"FAIL: {error}", file=sys.stderr)
        return 1
    print("PASS: central/watchdog health, four registered IDs/roles, five healthy observations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
