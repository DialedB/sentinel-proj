"""Six-process lifecycle/outbox smoke, delayed dispatch, recovery and two restarts.

Uses only the standard library; all data and logs are temporary. Reuses Phase 6's
bounded fleet harness so worker-death and heartbeat assertions stay identical.
"""

import argparse

from smoke_recovery import exercise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--base-port", type=int, default=19060)
    args = parser.parse_args()
    if not 0 < args.timeout <= 300 or not 1024 <= args.base_port <= 65530:
        parser.error("timeout must be 0..300 seconds and base-port 1024..65530")
    exercise(args.timeout, args.base_port, lifecycle=True)
