"""Commands dispatched by workers, never by the central server."""

from abc import ABC, abstractmethod
from typing import Any

from sentinel.common.tasks import SecurityEvent
from sentinel.security.evaluator import SecurityEvaluator


class TaskCommand(ABC):
    @abstractmethod
    def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError


class AnalyzeSecurityEventCommand(TaskCommand):
    def __init__(self, evaluator: SecurityEvaluator | None = None) -> None:
        self.evaluator = evaluator if evaluator is not None else SecurityEvaluator()

    def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        event = SecurityEvent.model_validate(payload)
        return dict(self.evaluator.evaluate(event))
