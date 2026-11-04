"""Run a worker with python -m sentinel.worker.main."""

from sentinel.common.models import ServiceRole
from sentinel.core.entrypoint import serve
from sentinel.core.factory import WorkerFactory


def main() -> None:
    serve(WorkerFactory(), ServiceRole.WORKER)


if __name__ == "__main__":
    main()
