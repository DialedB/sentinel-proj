"""Process-local logging bootstrap; never stores application or database state."""

import logging
from threading import Lock
from typing import ClassVar


class LoggingConfigurator:
    """One bootstrap coordinator per process, safe for concurrent entrypoints."""

    _instance: ClassVar[LoggingConfigurator | None] = None
    _lock: ClassVar[Lock] = Lock()

    def __new__(cls) -> LoggingConfigurator:
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    def configure(self) -> None:
        # basicConfig is idempotent and preserves handlers owned by the host/test runner.
        with self._lock:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(levelname)s %(name)s %(message)s",
            )
