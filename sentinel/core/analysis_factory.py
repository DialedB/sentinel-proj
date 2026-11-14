"""Abstract Factory for a compatible event-generator / analysis-command family."""

from abc import ABC, abstractmethod

from sentinel.core.commands import AnalyzeSecurityEventCommand, TaskCommand
from sentinel.producer.generator import SecurityEventGenerator
from sentinel.security.evaluator import SecurityEvaluator


class AnalysisComponentFactory(ABC):
    """Select the producer vocabulary and its matching worker policy together."""

    @abstractmethod
    def create_generator(self, source: str) -> SecurityEventGenerator:
        raise NotImplementedError

    @abstractmethod
    def create_analysis_command(self) -> TaskCommand:
        raise NotImplementedError


class SyntheticSecurityFactory(AnalysisComponentFactory):
    """The shipped family: synthetic security events and deterministic rules."""

    def create_generator(self, source: str) -> SecurityEventGenerator:
        return SecurityEventGenerator(source)

    def create_analysis_command(self) -> TaskCommand:
        return AnalyzeSecurityEventCommand(SecurityEvaluator())
