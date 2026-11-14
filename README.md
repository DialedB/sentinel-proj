# Sentinel

Python 3.14 distributed security-event processing: two synthetic producers submit
work to a central SQLite-backed server, two workers evaluate it, and an independent
watchdog checks health. Each role runs as its own process or container.

## Install and verify

Use Python 3.14 explicitly; `python3` may still select an older installation.
The project uses one virtual environment, `.venv`, created with Python 3.14.
If replacing an older environment, deactivate and delete it before recreating it.

```bash
python3.14 -m venv .venv
source .venv/bin/activate
python --version  # must report Python 3.14.x
python -m pip install -r requirements-dev.txt -c requirements.lock
python -m unittest discover -s tests -v
python -m coverage run -m unittest discover -s tests
python -m coverage report
ruff check sentinel tests
ruff format --check sentinel tests
python -m pip check
```

Verified on **Python 3.14.7** with coverage 7.17.3: the suite has **120 tests**
and measures **100% statement and branch coverage**
of concrete application code, including CLI wiring. `pyproject.toml` enforces the
100% threshold; only abstract-method declarations are excluded, not concrete
failure paths. Tests use temporary databases, ASGI transports, mock HTTP and
explicit cancellation; unit/integration discovery needs no external services.
Coverage is evidence of exercised paths, not proof that all possible bugs are absent.

`requirements.txt` declares compatible ranges; `requirements.lock` records the
tested runtime resolution used by Docker and the install command above. The HTTP
stack is FastAPI 0.162.7 / Starlette 0.52.1, replacing the old Starlette 0.46.2
installation (the reported GHSA-7f5h-v6xp-fcq8 fix starts at 0.49.1). No claim of
a full dependency/security audit is implied. Developer tools are separate from runtime.
The Python 3.14.7 verification used Ruff 0.17.5 and passed all five process smokes,
`pip check`, lint and `docker compose config --quiet`. Docker's daemon was unavailable,
so an image build/container runtime was not verified. Warning-enabled tests retain a
third-party Starlette TestClient/AnyIO `BlockingPortal` deprecation warning; no
application SQLite resource warnings remain. This interpreter migration changes
no database schema.

## Run

```bash
docker compose up --build
# Open http://localhost:8000/ (read-only live demo)
docker compose down
```

Compose starts six services and retains tasks/outbox data in a named volume.
`docker compose down -v` **deletes** that data. Docker runs as a non-root user.
For six local-process commands, configuration and API examples, see the
[operations guide](docs/operations.md#six-separate-processes).

Run bounded real-process checks independently of Docker:

```bash
python tests/smoke_pipeline.py  # six processes, both producers/workers, results/health
python tests/smoke_recovery.py  # worker death, leases, stale results, central restart
python tests/smoke_events.py    # committed outbox replay, receipts, metrics, restarts
python tests/smoke_watchdog.py  # HTTP and heartbeat policies, outage/recovery
python tests/smoke_communication.py  # offline startup and re-registration
```

Scripts stop and await their children and remove temporary databases. Pipeline,
recovery and events use separate configurable port ranges. Watchdog/communication
use ports 8000–8002/8005; run sequentially and allow TCP TIME_WAIT to expire if the
next script reports an occupied port. The read-only Compose deployment check is
`python tests/smoke_deployment.py` against an already-running stack.

## Design and rubric evidence

See the [class diagram, layers and rubric map](docs/architecture.md) for concrete
classes, runtime call sites and test evidence. The application uses **Singleton,
Builder, Factory Method, Template Method, Abstract Factory, Proxy, Command,
Observer, State and Strategy**. These participate in normal startup, production,
execution, transitions or monitoring—not separate pattern demos.

- **Persistence:** transactional repository with atomic FIFO claims, idempotent
  event IDs, expiring UUID leases and bounded attempts. Task transitions and
  lifecycle outbox appends commit together. Workers execute outside central.
- **Observers:** ordered, at-least-once delivery with durable acknowledgements;
  audit/metrics receipts make replay idempotent. Metrics are eventually consistent.
- **Isolation:** configuration/models are frozen; nested snapshots are copied.
  Tasks, registries, clients and databases are application-owned, never singleton
  globals. Only the process logging bootstrap is a Singleton.
- **Logging:** standard application/lifecycle logs, structured JSON audit records,
  health transitions, recovery/subscriber failures and unexpected-error tracebacks.
  Audit records exclude raw payloads and lease tokens.
- **Source control:** the existing repository retains its phased implementation
  history. This work leaves changes uncommitted; no commit, branch or push is made.
  Cache files, databases, credentials and virtual environments are excluded from
  version control/build context. Never commit `.env`, tokens or private data.

## API changes and safety

Central exposes registration/heartbeat/services, `/events`, `/tasks`, `/stats`,
lifecycle history and event metrics; watchdog exposes `/status`. All roles expose
`/health`; central also serves the demo and a fixed-destination watchdog proxy.
Full examples are in the [task API guide](docs/operations.md#task-api).

- `GET /tasks?limit=50` returns the **oldest bounded page**, in insertion order;
  limits are 1–200. Use `after_task_id=<last-id>` to traverse all history forward.
  Unknown cursors are 404. `latest=true` starts from the most recent window;
  the demo requests `/tasks?limit=10&latest=true`. A cursor takes precedence over
  `latest`. This replaces unbounded reads without making older history unreachable.
- **Only** `GET /tasks/next?worker_id=...` returns a lease token. Other task responses,
  including duplicate event submission, omit it. The claim remains a mutating GET
  for backward compatibility; do not prefetch or cache it.
- Event payloads/success results require JSON-safe values, finite numbers, depth
  at most 32, at most 10,000 nodes and at most 65,536 encoded bytes. Event sources
  are limited to 256 characters. Service and main-server IDs use the same limit:
  oversized configured IDs fail startup, and oversized registration/heartbeat IDs
  return 422. Invalid input/UTC overflow is rejected with 422 before storage;
  exact saved task JSON is revalidated before commit.
- Registration accepts only hostname/IP addresses with separate ports, and rejects
  central's configured reserved IDs through both local and HTTP gateways.
  Lifecycle cursors fit SQLite's signed 64-bit integers.
- Unexpected producer/worker pipeline or heartbeat-task termination logs a traceback
  (for exceptions) and makes `/health` report `unhealthy` until shutdown/restart.
  Successful heartbeats do not clear this failure; normal shutdown cancellation
  does not report it.
- Errors keep a string `detail` and add a consistent actionable envelope:

```json
{
  "detail": "Unknown worker ID",
  "error": {
    "code": "unknown_worker",
    "message": "Unknown worker ID",
    "next_action": "Register the worker first."
  }
}
```

Local and HTTP `ServiceGateway` implementations raise `DomainError` for expected
application failures, preserving `code`, message, `next_action` and `status`.
HTTP status failures no longer escape the proxy as `httpx.HTTPStatusError`;
connection and timeout failures remain `httpx.HTTPError` transport exceptions.

Validation responses add sanitized `fields` without echoing raw input. Conflicts
are 409; storage unavailability/corruption is 503, not a misleading conflict.
Unexpected failures return a safe 500 and log the traceback. Existing corrupt task
rows are **not** silently skipped/deleted: stop central, back up the database,
then restore/repair the identified task. Corrupt expired tasks can prevent startup
recovery; the logs identify the failure. Back up existing databases before upgrade;
there is no schema change, but stricter validation can reject formerly accepted data.

## Scope

This is a trusted-network university/development system, **not a complete security
product**. Registration is not authentication. Customer/admin programs, login,
authorization, OTP, TLS, general request-body/rate limits, automatic process restart
and registry/watchdog persistence are **not implemented**. Host validation is not
an SSRF allowlist. Run one central and one watchdog process (not multiple uvicorn
workers); registry/watchdog observations are process-local. Payload bounds are
not a defense against arbitrary oversized HTTP requests. The rubric map covers
the requested engineering criteria, not unprovided booklet requirements or an
external professor's grading decision.
