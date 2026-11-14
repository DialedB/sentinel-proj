"""Small worker-local command registry."""

from typing import Any

from sentinel.core.analysis_factory import (
    AnalysisComponentFactory,
    SyntheticSecurityFactory,
)
from sentinel.core.commands import TaskCommand


class WorkerExecutor:
    def __init__(self, factory: AnalysisComponentFactory | None = None) -> None:
        factory = factory if factory is not None else SyntheticSecurityFactory()
        self.commands: dict[str, TaskCommand] = {
            "analyze_security_event": factory.create_analysis_command(),
        }

    def execute(self, command: str, payload: dict[str, Any]) -> dict[str, Any]:
        if command not in self.commands:
            raise ValueError(f"Unknown command: {command}")
        return self.commands[command].execute(payload)
