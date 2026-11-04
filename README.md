# Sentinel

Sentinel is a distributed security monitoring project targeting **Python 3.12**.
Phase 2 implements HTTP service registration and heartbeats; event generation,
analysis, watchdog monitoring, persistence, and Docker are not implemented.

## Setup

From the repository root:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Dependencies are FastAPI (0.115 series), httpx (0.27–0.28), and uvicorn
(0.30–0.49), with compatible transitive dependencies resolved by pip.

## Run three services

Activate the virtual environment in each terminal, then run one command per
terminal:

```bash
python -m sentinel.server.main       # 127.0.0.1:8000, server-1
python -m sentinel.producer.main     # 127.0.0.1:8001, producer-1
python -m sentinel.worker.main       # 127.0.0.1:8002, worker-1
```

Use Ctrl-C to shut down gracefully. Producers and workers can start before the
server: registration failures are logged and retried without blocking health
requests. Each owns one lifespan-scoped async HTTP client and background task.
Shutdown cancels and awaits that task before closing its HTTP client.

### Environment configuration

All configuration is loaded and validated before uvicorn starts. `ServiceConfig`
is frozen and retains its original five positional arguments (`service_id`,
`role`, `host`, `port`, `server_url`); timing fields are optional additions.

| Variable | Default | Validation / purpose |
| --- | --- | --- |
| `SERVICE_ID` | `server-1`, `producer-1`, or `worker-1` | Nonblank stable identity; choose a unique ID per concurrent service |
| `ROLE` | Entry module's role | Must match `server`, `producer`, or `worker` entrypoint |
| `HOST` | `127.0.0.1` | Nonblank hostname/IP, not a URL; bind and advertised registration host |
| `PORT` | 8000 / 8001 / 8002 respectively | Integer 1–65535 |
| `MAIN_SERVER_URL` | `http://127.0.0.1:8000` | Valid HTTP(S) central URL |
| `HEARTBEAT_INTERVAL` | `5` | Finite positive seconds; also registration retry delay |
| `HTTP_TIMEOUT` | `5` | Finite positive seconds; httpx request timeout |

For additional instances, set **both** a unique ID and unused port:

```bash
SERVICE_ID=producer-2 PORT=8011 python -m sentinel.producer.main
SERVICE_ID=worker-2 PORT=8012 python -m sentinel.worker.main
SERVICE_ID=worker-remote PORT=8013 MAIN_SERVER_URL=http://127.0.0.1:8000 HEARTBEAT_INTERVAL=2 HTTP_TIMEOUT=3 python -m sentinel.worker.main
```

The default IDs are deterministic, not random. Reusing an ID intentionally
replaces the existing registry record, even if its role/address changes. Restart
with the same `SERVICE_ID` to retain identity. `HOST=0.0.0.0` binds all interfaces
but is not a useful address for remote clients; choose an appropriate interface
address for network use. This phase has no separate advertised-host setting.

## HTTP API and verification

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8001/health
curl http://127.0.0.1:8002/health
curl http://127.0.0.1:8000/services
sleep 6
curl http://127.0.0.1:8000/services
```

Each health response includes the service's own `service_id`, `role`, and
`status`. Health means the local application is running, **not** that the central
server is reachable. The registry should list both producer and worker with
increasing UTC `last_seen` timestamps and no duplicate IDs.

Central endpoints (interactive schema at `/docs`):

- `GET /health`: central service identity and local status.
- `POST /register`: JSON `{"service_id":"p1","role":"producer","host":"127.0.0.1","port":8001}`.
  Only producers and workers may register. Creates/replaces by ID and returns
  the record with healthy status, UTC `registered_at`, and UTC `last_seen`.
  Re-registration resets both timestamps.
- `POST /heartbeat`: JSON `{"service_id":"p1"}`. Updates `last_seen`, preserving
  `registered_at`. Unknown IDs return 404; clients immediately re-register.
- `GET /services`: a snapshot list of registered service records.

Malformed/missing payloads, unsupported registration roles, extra fields,
blank identities, and invalid ports return 422. A lock protects every registry
operation; returned records are independent, immutable snapshots.

Registration success, startup, registration failures, and shutdown are logged
with IDs; heartbeat details are DEBUG-level messages. Default application logging
is INFO. Uvicorn also emits HTTP access logs.

## Architecture and compatibility

- `sentinel/common/config.py`: immutable configuration and environment loading.
- `sentinel/common/models.py`: original Phase 1 role/status enums and snapshots.
- `sentinel/common/transport.py`: Pydantic HTTP request/response contracts.
- `sentinel/server/registry.py`: lock-protected, in-memory registry.
- `sentinel/core/service.py`: shared ASGI lifespan, health, communication loop,
  and central routes.
- `sentinel/core/factory.py`: original Factory Method creators for each role.
- `sentinel/core/entrypoint.py` and role `main.py` modules: factory construction
  and uvicorn serving.

Construction has no networking side effects. `create_app()` exposes serving
without changing the original abstract contract: **all `run()` methods still
raise `NotImplementedError`**, as required by the Phase 1 tests. The watchdog
remains only its Phase 1 placeholder.

## Tests and real-process smoke check

```bash
python -m unittest discover -s tests -v
python tests/smoke_communication.py
```

Unit tests cover Phase 1 compatibility, registry concurrency and snapshots,
API validation, configuration, mocked connection/HTTP failures, re-registration,
nonblocking health, and cancellation/client cleanup. No network is needed for
unit tests.

The explicitly invoked smoke harness requires free ports **8000–8002** and
permission to listen on localhost. It uses this interpreter to launch independent
server, producer, and worker subprocesses. It verifies offline-start retries,
all health responses, registry entries, increasing timestamps, re-registration
after central restart, and graceful shutdown without error logs. Every child is
terminated and waited for in `finally`, with bounded waits and a kill fallback.
Uvicorn may exit with SIGTERM (`-15`) after completing graceful teardown; the
harness verifies the application shutdown messages as well as exit status.

## Scope and limitations

The registry is process-local and disappears on central restart; clients recover
through unknown-heartbeat re-registration. Run **one central uvicorn process**:
multiple workers would have separate registries. Records are not removed on
client shutdown and never expire or become unhealthy automatically. There is no
watchdog, authentication, TLS setup, event transport, analysis, database, or
Docker support. Treat this as a development-only communication layer; do not
expose it to untrusted networks. Fixed-interval retries deliberately avoid extra
backoff machinery in this phase.
