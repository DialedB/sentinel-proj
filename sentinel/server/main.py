"""Run the central server with python -m sentinel.server.main."""

from sentinel.common.models import ServiceRole
from sentinel.core.entrypoint import serve
from sentinel.core.factory import ServerFactory


def main() -> None:
    serve(ServerFactory(), ServiceRole.SERVER)


if __name__ == "__main__":
    main()
