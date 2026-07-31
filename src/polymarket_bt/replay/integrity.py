from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polymarket_bt.constants import QualityState


@dataclass(frozen=True, slots=True)
class QualityInterval:
    start_utc_ns: int
    end_utc_ns: int | None
    state: QualityState
    category: str
    details: dict[str, Any]

    def contains(self, timestamp_ns: int) -> bool:
        return timestamp_ns >= self.start_utc_ns and (
            self.end_utc_ns is None or timestamp_ns <= self.end_utc_ns
        )


class ReplayIntegrity:
    def __init__(self, intervals: list[QualityInterval], *, reject_on_gap: bool) -> None:
        self.intervals = intervals
        self.reject_on_gap = reject_on_gap
        self.used_degraded: list[QualityInterval] = []

    def check(self, timestamp_ns: int) -> QualityState:
        state = QualityState.COMPLETE
        for interval in self.intervals:
            if not interval.contains(timestamp_ns):
                continue
            if interval.state in {QualityState.UNRELIABLE, QualityState.EXCLUDED}:
                if self.reject_on_gap:
                    raise ValueError(
                        f"replay rejected at {timestamp_ns}: {interval.category} ({interval.state})"
                    )
                if interval not in self.used_degraded:
                    self.used_degraded.append(interval)
                state = interval.state
            elif interval.state == QualityState.DEGRADED:
                if interval not in self.used_degraded:
                    self.used_degraded.append(interval)
                if state == QualityState.COMPLETE:
                    state = QualityState.DEGRADED
        return state
