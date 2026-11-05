"""Synthetic data only: fictional users and RFC 5737 documentation addresses."""
from itertools import cycle
from sentinel.common.tasks import EventType, SecurityEvent


class EventGenerator:
    def __init__(self, producer_id):
        self.producer_id = producer_id
        self.types = cycle(EventType)

    def generate(self):
        return SecurityEvent(producer_id=self.producer_id, event_type=next(self.types),
                             payload={'user': 'fictional-alex', 'source_ip': '192.0.2.10'})
