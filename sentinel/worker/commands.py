"""Worker-local Command pattern and pure security evaluation."""
from abc import ABC, abstractmethod
from sentinel.common.tasks import SecurityEvent, EventType, utcnow


class SecurityEvaluator:
    def evaluate(self, event: SecurityEvent) -> dict:
        score = {EventType.BENIGN: 0, EventType.SUSPICIOUS: 40, EventType.HIGH_RISK: 80}[event.event_type]
        return dict(event_id=str(event.event_id), score=score,
                    rule=event.event_type.value, timestamp=utcnow().isoformat())


class Command(ABC):
    @abstractmethod
    def execute(self, payload: dict) -> dict:
        raise NotImplementedError


class AnalyzeSecurityEventCommand(Command):
    def __init__(self, evaluator=None):
        self.evaluator = evaluator or SecurityEvaluator()

    def execute(self, payload: dict) -> dict:
        return self.evaluator.evaluate(SecurityEvent.model_validate(payload))


class WorkerExecutor:
    def __init__(self):
        self.commands = {'analyze_security_event': AnalyzeSecurityEventCommand()}

    def execute(self, command: str, payload: dict) -> dict:
        if command not in self.commands:
            raise ValueError(f'Unknown command: {command}')
        return self.commands[command].execute(payload)
