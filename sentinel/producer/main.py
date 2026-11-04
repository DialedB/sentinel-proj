"""Run a producer with python -m sentinel.producer.main."""

from sentinel.common.models import ServiceRole
from sentinel.core.entrypoint import serve
from sentinel.core.factory import ProducerFactory


def main() -> None:
    serve(ProducerFactory(), ServiceRole.PRODUCER)


if __name__ == "__main__":
    main()
