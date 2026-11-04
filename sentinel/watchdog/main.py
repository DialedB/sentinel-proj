"""Run the watchdog through the existing Factory Method entrypoint."""

from sentinel.common.models import ServiceRole
from sentinel.core.entrypoint import serve
from sentinel.core.factory import WatchdogFactory


def main() -> None:
    serve(WatchdogFactory(), ServiceRole.WATCHDOG)


if __name__ == "__main__":
    main()
