# Sentinel

Sentinel targets Python 3.12. Phase 4 adds a six-container Docker Compose deployment
to Phase 3 strategy-based monitoring and Phase 2 registration/heartbeats, preserving
the Phase 1 Factory Method API. The local-process workflow remains supported.

## Setup and tests

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python tests/smoke_watchdog.py
.venv/bin/python tests/smoke_communication.py
```

Dependencies are FastAPI, httpx, and uvicorn; see `requirements.txt` for ranges.
Unit tests need no network. The Phase 2/3 smoke scripts launch bounded independent subprocesses,
require permission to listen on localhost and free ports 8000–8002 and (for the
watchdog smoke) 8005. Allow TCP TIME_WAIT to expire before running the unchanged
Phase 2 smoke immediately after another smoke using the same ports. Every child
is stopped and awaited in `finally`, with bounded waits and a kill fallback.

## Phase 4: Docker Compose

Requires Docker with a running daemon and Docker Compose v2+ (or compatible v5).
Do not run the local setup simultaneously: host ports 8000 and 8005 must be free.
From the repository root:

```bash
docker version
docker compose config --quiet
docker compose build
docker compose up -d --wait --wait-timeout 90
docker compose ps
python3 tests/smoke_deployment.py --timeout 60
curl --fail --max-time 3 http://127.0.0.1:8000/health
curl --fail --max-time 3 http://127.0.0.1:8000/services
curl --fail --max-time 3 http://127.0.0.1:8005/health
curl --fail --max-time 3 http://127.0.0.1:8005/status
curl --fail --max-time 3 http://127.0.0.1:8005/status/worker1
docker compose logs --tail 100 main-server watchdog worker1
```

All six services use the same `sentinel:phase4` image built from `python:3.12-slim`.
The image installs only `requirements.txt`, copies the application, runs unbuffered
as UID/GID 10001, and starts Python directly (exec-form Compose commands, no shell,
reload, or extra uvicorn workers). Build context excludes Git, virtualenvs, caches,
tests, scripts, and common secret files; only requirements and application code
are copied. Never store secrets in application source. Initial build needs access
to Docker Hub and PyPI; dependencies use compatible ranges, not a reproducible lock.

| Compose service / ID | Role | Internal listen / advertised address |
| --- | --- | --- |
| `main-server` | server | `0.0.0.0:8000` / `main-server:8000` |
| `producer1`, `producer2` | producer | `0.0.0.0:8000` / respective DNS name, port 8000 |
| `worker1`, `worker2` | worker | `0.0.0.0:8000` / respective DNS name, port 8000 |
| `watchdog` | watchdog | `0.0.0.0:8000` / `watchdog:8000` |

Communication is HTTP over Docker DNS on the named bridge `sentinel-network`;
`MAIN_SERVER_URL=http://main-server:8000` and `MAIN_SERVER_ID=main-server` everywhere.
This is an isolated application network, not an authentication or egress security
boundary. Only central and watchdog publish ports, bound to host loopback at
`127.0.0.1:8000` and `127.0.0.1:8005`. Producers/workers are reachable inside the
network only. No Docker socket, volumes, or restart policies are configured.
The local instructions below deliberately retain `server-1`, `producer-1`, etc.;
Compose uses `main-server`, `producer1`, etc. Do not mix these identities.

Central and watchdog have stdlib `/health` healthchecks (2s HTTP timeout, 3s
process timeout, 5s intervals, 5s startup grace, five retries). Their healthchecks
measure their own APIs, not fleet readiness. All other services wait for central
health at initial startup; the watchdog uses the HTTP strategy. `depends_on` is
startup ordering only: application registration retries and 404 re-registration
still provide recovery after later central outages. `--wait` alone does not prove
all registrations or monitoring cycles completed; run the smoke check too.

The read-only stdlib smoke script queries both health endpoints, requires exactly
four registered IDs with producer/worker roles, and five healthy, discoverable,
checked watchdog observations including central. It retries startup for 60 seconds
by default, bounds HTTP waits, and exits nonzero with the last error on failure.
It neither launches processes nor controls Docker, so it stays outside the image.
Optional `--server-url`, `--watchdog-url`, and `--timeout` customize its targets.
To verify heartbeats, read `/services` twice at least five seconds apart and confirm
`last_seen` advances for all four IDs (registry status itself is not live health).

### Container failure and recovery exercise

Start with a passing smoke check. Run each step separately, allowing subsequent
monitoring cycles (normally 5–15 seconds; requests are sequential and timeout-bound).
Use repeated bounded curl requests rather than assuming an immediate transition:

```bash
docker compose stop --timeout 10 worker1
curl --fail --max-time 3 http://127.0.0.1:8005/status/worker1
docker compose start worker1
python3 tests/smoke_deployment.py --timeout 60
docker compose stop --timeout 10 main-server
curl --fail --max-time 3 http://127.0.0.1:8005/health
curl --fail --max-time 3 http://127.0.0.1:8005/status
docker compose start main-server
python3 tests/smoke_deployment.py --timeout 60
docker compose logs --tail 100 watchdog main-server worker1
```

While worker1 is stopped, its watchdog status becomes `unhealthy`; the other four
observations stay healthy. Starting it with the same ID restores health without
restarting the watchdog. While central is stopped, the watchdog API stays reachable,
`main-server` becomes unhealthy, and all five known observations remain. Discovery
flags become false, but the four live producer/worker HTTP checks remain healthy.
The normal smoke is intentionally expected to fail during outages. After central
starts, its empty in-memory registry is repopulated automatically: producer/worker
heartbeats receive 404 and trigger re-registration. Discovery and all five healthy
statuses recover. Expected connection/discovery failures are logged during outages.

Python is PID 1 and receives Docker's SIGTERM directly. Existing lifespan teardown
cancels/awaits background tasks and closes HTTP clients; Compose allows ten seconds
before forced termination. Inspect logs for application shutdown completion. No
restart policy heals a stopped container: `start` is an explicit operator action.
Registry and cached watchdog state are not persisted; restarting the watchdog
during a central outage loses previously discovered targets until discovery returns.

Cleanup only the stack you started (do not stop unrelated projects):

```bash
docker compose down --timeout 10
# Optional: remove the shared local image after cleanup.
docker image rm sentinel:phase4
```

## Six separate processes

Activate the virtual environment in **six separate terminals**, then run one of
these commands in each (all IDs, bind addresses, ports, and central addresses are
explicit):

```bash
SERVICE_ID=server-1 SERVICE_ROLE=server SERVICE_HOST=127.0.0.1 SERVICE_PORT=8000 MAIN_SERVER_URL=http://127.0.0.1:8000 python -m sentinel.server.main
```
```bash
SERVICE_ID=producer-1 SERVICE_ROLE=producer SERVICE_HOST=127.0.0.1 SERVICE_PORT=8001 MAIN_SERVER_URL=http://127.0.0.1:8000 python -m sentinel.producer.main
```
```bash
SERVICE_ID=producer-2 SERVICE_ROLE=producer SERVICE_HOST=127.0.0.1 SERVICE_PORT=8003 MAIN_SERVER_URL=http://127.0.0.1:8000 python -m sentinel.producer.main
```
```bash
SERVICE_ID=worker-1 SERVICE_ROLE=worker SERVICE_HOST=127.0.0.1 SERVICE_PORT=8002 MAIN_SERVER_URL=http://127.0.0.1:8000 python -m sentinel.worker.main
```
```bash
SERVICE_ID=worker-2 SERVICE_ROLE=worker SERVICE_HOST=127.0.0.1 SERVICE_PORT=8004 MAIN_SERVER_URL=http://127.0.0.1:8000 python -m sentinel.worker.main
```
```bash
SERVICE_ID=watchdog-1 SERVICE_ROLE=watchdog WATCHDOG_HOST=127.0.0.1 WATCHDOG_PORT=8005 MAIN_SERVER_ID=server-1 MAIN_SERVER_URL=http://127.0.0.1:8000 MONITOR_INTERVAL=5 HEALTH_CHECK_TIMEOUT=2 HEARTBEAT_TIMEOUT=15 HEALTH_CHECK_STRATEGY=http python -m sentinel.watchdog.main
```

Use Ctrl-C for graceful shutdown. Producers/workers may start before central:
registration failures are retried; unknown heartbeat IDs trigger re-registration.
Use unique IDs and ports; restarting with the same ID intentionally replaces the
registry entry. The watchdog does not register itself.

## Configuration

`ServiceConfig` is immutable and preserves its original five positional arguments
and existing optional heartbeat/HTTP timeout arguments. Environment configuration
is validated before uvicorn starts.

| Variable | Default | Purpose |
| --- | --- | --- |
| `SERVICE_ID` | `<role>-1` | Nonblank stable identity |
| `SERVICE_ROLE` | Entrypoint role | Must match entrypoint |
| `SERVICE_HOST` | `127.0.0.1` | Listen hostname/IP, not URL |
| `SERVICE_ADVERTISE_HOST` | `SERVICE_HOST` | Optional peer-reachable hostname/IP used by registration and `get_info()` |
| `SERVICE_PORT` | server 8000, producer 8001, worker 8002, watchdog 8005 | Integer 1–65535 |
| `WATCHDOG_HOST` / `WATCHDOG_PORT` | `127.0.0.1` / `8005` | Watchdog defaults; explicit `SERVICE_HOST` / `SERVICE_PORT` take precedence |
| `MAIN_SERVER_URL` | `http://127.0.0.1:8000` | Valid HTTP(S) central address |
| `MAIN_SERVER_ID` | `server-1` | Expected central identity, independent of registry |
| `HEARTBEAT_INTERVAL` | `5` | Heartbeat and registration retry seconds |
| `HTTP_TIMEOUT` | `5` | Producer/worker HTTP timeout seconds |
| `MONITOR_INTERVAL` | `5` | Delay in seconds between completed monitoring cycles |
| `HEALTH_CHECK_TIMEOUT` | `2` | Explicit watchdog HTTP request timeout seconds |
| `HEARTBEAT_TIMEOUT` | `15` | Maximum heartbeat age seconds |
| `HEALTH_CHECK_STRATEGY` | `http` | Exactly `http` or `heartbeat` |

All timing values must be finite positive numbers (booleans invalid in the Python
constructor). Legacy unprefixed role/host/port variables are ignored. Binding to
`0.0.0.0` listens on all interfaces; set `SERVICE_ADVERTISE_HOST` to a peer-reachable
DNS hostname/IP instead. The optional Python `advertise_host` field is appended to
preserve all prior positional arguments; `advertised_host` resolves its fallback.
Empty, non-string, whitespace-containing, URL/path/query/fragment/userinfo values
are rejected before startup, using the same address rules as the bind host.

## APIs and verification

```bash
curl http://127.0.0.1:8000/services
curl http://127.0.0.1:8005/health
curl http://127.0.0.1:8005/status
curl http://127.0.0.1:8005/status/worker-1
```

All roles expose `GET /health` with their own `service_id`, `role`, and local
`status`. Watchdog health describes its running application, not fleet health.
Central exposes:

- `POST /register`: `{ "service_id": "w1", "role": "worker", "host": "127.0.0.1", "port": 8002 }`.
  Only producer/worker roles register; creates/replaces by ID.
- `POST /heartbeat`: `{ "service_id": "w1" }`; unknown IDs return 404.
- `GET /services`: immutable registry snapshots including UTC `registered_at`
  and `last_seen`. Malformed registration/heartbeat payloads return 422.

Watchdog `GET /status` returns a snapshot list. `GET /status/{service_id}` returns
one observation or 404 for unknown IDs. Each observation contains `service_id`,
`role`, `status` (null before checking, then healthy/unhealthy), `last_checked`,
`last_successful`, `error`, and `discoverable`. Failed checks preserve the last
successful timestamp. Snapshots are immutable and not shared mutable state.

### Failure, recovery, and strategy selection

Stop worker-1: HTTP monitoring marks it unhealthy on a subsequent check. Restart
the same command/ID/port: it becomes healthy again. Health requires HTTP 200,
valid JSON with the expected **ID and role**, and healthy status. Network errors,
timeouts, malformed JSON, and identity mismatches are unhealthy.

To select heartbeat monitoring, restart the watchdog command with
`HEALTH_CHECK_STRATEGY=heartbeat`. This checks registry `last_seen` age (default
15 seconds), rejecting missing, invalid, naive, expired, or future timestamps.
It measures recent communication with central, **not** live HTTP reachability:
a stopped worker remains healthy until its last heartbeat expires.

Central is always HTTP-checked under either strategy: it has no heartbeat.
Stop central: discovery failure makes central unhealthy even if its health route
were reachable. Previously discovered services and addresses remain in memory,
are marked `discoverable=false`, and checks continue. Under HTTP strategy, live
producer/worker endpoints remain healthy. Under heartbeat strategy their cached
heartbeats eventually expire even if they are running. Missing registrations in
a successful discovery likewise do not delete known records. After central
restart, producers/workers re-register and monitoring recovers automatically.

## Architecture and lifecycle

- `common/config.py`, `models.py`, `transport.py`: immutable configuration,
  original domain types, validated transport contracts.
- `common/http_client.py`: reusable registration/heartbeat transport.
- `server/registry.py`: lock-protected in-memory registry.
- `core/factory.py`: existing Factory Method creators; construction has no I/O.
- `core/service.py`: `create_app()`, APIs and lifespan ownership. WatchdogService
  composes a `ServiceMonitor` rather than replacing the factory or Phase 1 class.
- `core/entrypoint.py` and role `main.py`: factory-driven uvicorn entrypoints.
- `watchdog/strategies.py`: async Strategy ABC and HTTP/heartbeat policies.
- `watchdog/monitor.py`: `ServiceMonitor` with constructor-injected strategy,
  discovery, and retained targets; `models.py`: frozen, `ServiceStatus`-typed results.

Watchdog discovery and HTTP checks share one reusable lifespan-scoped AsyncClient.
Checks are sequential asynchronous requests in a background task, never performed
by status API handlers. Each service exception is logged and checking continues;
outer loop failures are logged and retried. INFO transition logs are emitted only
when status changes, including initial checks (`UNCHECKED`) and recovery, e.g.
`[WATCHDOG] worker-1 HEALTHY -> UNHEALTHY`. Cancellation propagates. Shutdown
cancels and awaits the task **before** closing its client. All original `run()`
methods deliberately still raise `NotImplementedError` for Phase 1 compatibility.

Tests cover HTTP failures/identity/timeouts, heartbeat timestamp validation,
monitor transitions, missing discovery, outages and recovery, snapshot isolation,
configuration, API responsiveness, cancellation, client closure, and prior phases.
The watchdog smoke runs both policies through worker stop/restart and central
outage, checking retained observations and graceful shutdown. Expected central
outage discovery exceptions are logged; leaked-task/client warnings are failures.
The Phase 2 smoke verifies offline startup and central restart re-registration.

## Scope and limits

Registry and watchdog state are process-local and lost on restart. Run one
central process and one watchdog process, not multiple uvicorn workers. Registry
records never automatically expire or disappear; the watchdog reports observations
without mutating central registry status. Checks are sequential: large fleets can
make cycles slow, and interval is added after each cycle. HTTP timeouts bound
network operations, not a strict whole-cycle deadline. No authentication, TLS
configuration, events, analysis, persistence, automatic restarts, or later
features are implemented. This is a development system, not suitable for exposure
to untrusted networks. Registration addresses are trusted configuration.
