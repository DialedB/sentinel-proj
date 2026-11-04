# Sentinel

Sentinel is a distributed security event analysis and monitoring system being
built for a university Computer Science course. This repository contains only
its **Phase 1 architecture**, targeting Python 3.12 with no external dependencies.

## Planned service responsibilities

- **Server:** coordinate services, accept registration, and provide a central
  point for communication.
- **Producers:** generate or collect security events for analysis.
- **Workers:** analyze events and produce analysis results.
- **Watchdog:** observe service health and identify unavailable services.

These are planned responsibilities, not implemented features. The system does
not yet perform distributed communication or security event processing.

## Phase 1 structure

- `sentinel/common/models.py`: role and status enums and service-info snapshots.
- `sentinel/common/config.py`: immutable, explicitly supplied configuration.
  `ServiceConfig` takes `service_id`, `role`, `host`, `port`, and `server_url`
  (the central server URL). It validates required strings, role membership,
  ports from 1–65535, and HTTP(S) URLs. It does not load environment variables
  or check network reachability.
- `sentinel/core/service.py`: an abstract service with read-only ID and role,
  initial `STARTING` status, and `get_info()`. Four concrete role placeholders
  retain their configuration; their `run()` methods raise `NotImplementedError`.
- `sentinel/core/factory.py`: service construction using Factory Method.
- `sentinel/server/`, `producer/`, `worker/`, and `watchdog/`: empty packages
  reserved for future role-specific implementation.
- `tests/test_factory.py`: standard-library unit tests.

### Factory Method

`ServiceFactory` defines the abstract `create_service(config)` method.
`ServerFactory`, `ProducerFactory`, `WorkerFactory`, and `WatchdogFactory`
implement it by constructing their corresponding service subclasses. Callers
can construct services through a common contract without knowing constructor
implementation details. Construction does not start execution. Callers choose
an explicit factory and supply a configuration with the corresponding role;
there is no automatic factory selection or role inference.

### Phase 2 extension points

The configuration already supplies the host, port, and central server URL for
future HTTP communication. `ServiceInfo` provides a shared registration/status
shape, while the base contract and factories separate construction from future
role behavior. Phase 2 can add HTTP registration and role-specific execution
behind these contracts; serialization and HTTP transport are not implemented.

There are no lifecycle loops, threads, asynchronous execution, queues, HTTP
endpoints, detection rules, monitoring logic, databases, or Docker configuration.

## Run the tests

From the repository root using Python 3.12:

```bash
python -m unittest discover -s tests -v
```

No dependency installation or running services are required.
