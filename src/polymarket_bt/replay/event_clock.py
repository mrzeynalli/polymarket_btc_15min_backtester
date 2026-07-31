from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polymarket_bt.constants import ReplayClockMode


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    event_type: str
    exchange_timestamp_ns: int | None
    received_utc_ns: int
    received_monotonic_ns: int
    connection_id: str
    sequence: int
    parent_change_index: int
    source_priority: int
    payload: Any

    def timestamp(self, mode: ReplayClockMode) -> int:
        if mode == ReplayClockMode.LOCAL_RECEIVE_TIME:
            return self.received_utc_ns
        if self.exchange_timestamp_ns is None:
            raise ValueError(
                f"event {self.event_type} sequence {self.sequence} lacks exchange timestamp"
            )
        return self.exchange_timestamp_ns


class EventClock:
    def __init__(self, mode: ReplayClockMode) -> None:
        self.mode = mode
        self.current_ns: int | None = None

    def advance(self, event: ReplayEvent) -> int:
        timestamp = event.timestamp(self.mode)
        if self.current_ns is not None and timestamp < self.current_ns:
            raise ValueError("replay clock moved backwards")
        self.current_ns = timestamp
        return timestamp
