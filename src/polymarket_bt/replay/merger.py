from __future__ import annotations

from collections.abc import Iterable

from polymarket_bt.constants import ReplayClockMode
from polymarket_bt.replay.event_clock import ReplayEvent


def deterministic_event_key(
    event: ReplayEvent, mode: ReplayClockMode
) -> tuple[int, int, int, str, int, int]:
    replay_time = event.timestamp(mode)
    return (
        replay_time,
        event.received_utc_ns,
        event.source_priority,
        event.connection_id,
        event.sequence,
        event.parent_change_index,
    )


def merge_events(events: Iterable[ReplayEvent], mode: ReplayClockMode) -> list[ReplayEvent]:
    return sorted(events, key=lambda event: deterministic_event_key(event, mode))
