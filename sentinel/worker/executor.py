"""Small worker-local command registry."""

from typing import Any

from sentinel.core.commands import AnalyzeSecurityEventCommand, TaskCommand


class WorkerExecutor:
    def __init__(self) -> None:
        self.commands: dict[str, TaskCommand] = {
            "analyze_security_event": AnalyzeSecurityEventCommand(),
        }

    def execute(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command not in self.commands:
            raise ValueError(f"Unknown command: {command}")
        return self.commands[command].execute(payload)
