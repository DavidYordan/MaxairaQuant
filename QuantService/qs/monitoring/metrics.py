from __future__ import annotations
import time
from dataclasses import dataclass

@dataclass
class MetricsSnapshot:
    ts: float
    requests: int
    successes: int
    failures: int
    inserted_rows: int

class Metrics:
    def __init__(self):
        self.requests = 0
        self.successes = 0
        self.failures = 0
        self.inserted_rows = 0

    def inc_requests(self, n: int = 1):
        self.requests += n

    def inc_successes(self, n: int = 1):
        self.successes += n

    def inc_failures(self, n: int = 1):
        self.failures += n

    def add_inserted_rows(self, n: int):
        self.inserted_rows += n

    def snapshot(self) -> MetricsSnapshot:
        return MetricsSnapshot(
            time.time(),
            self.requests,
            self.successes,
            self.failures,
            self.inserted_rows,
        )