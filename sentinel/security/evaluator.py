"""Pure event classification; event types describe activity, not risk."""

from typing import Literal, TypedDict

from sentinel.common.tasks import EventType, SecurityEvent, utcnow

Classification = Literal["BENIGN", "SUSPICIOUS", "HIGH_RISK"]


class SecurityEvaluation(TypedDict):
    event_id: str
    classification: Classification
    risk_score: int
    triggered_rule: str
    processing_timestamp: str


class SecurityEvaluator:
    def evaluate(self, event: SecurityEvent) -> SecurityEvaluation:
        classification, risk_score = self._rules[event.event_type]
        return SecurityEvaluation(
            event_id=str(event.event_id),
            classification=classification,
            risk_score=risk_score,
            triggered_rule=event.event_type.value,
            processing_timestamp=utcnow().isoformat(),
        )

    _rules: dict[EventType, tuple[Classification, int]] = {
        EventType.LOGIN_SUCCESS: ("BENIGN", 0),
        EventType.LOGIN_FAILED: ("SUSPICIOUS", 40),
        EventType.ACCESS_DENIED: ("HIGH_RISK", 80),
    }
