from __future__ import annotations

import pytest

from polymarket_bt.constants import ReplayClockMode
from polymarket_bt.replay.event_clock import EventClock, ReplayEvent
from polymarket_bt.replay.merger import merge_events


def event(*, receive: int, sequence: int, connection: str = "c", change: int = 0) -> ReplayEvent:
    return ReplayEvent(
        event_type="test",
        exchange_timestamp_ns=receive - 1,
        received_utc_ns=receive,
        received_monotonic_ns=receive,
        connection_id=connection,
        sequence=sequence,
        parent_change_index=change,
        source_priority=10,
        payload=None,
    )


def test_deterministic_tie_breaking() -> None:
    values = [event(receive=10, sequence=2), event(receive=10, sequence=1, change=1)]
    ordered = merge_events(values, ReplayClockMode.LOCAL_RECEIVE_TIME)
    assert [item.sequence for item in ordered] == [1, 2]


def test_clock_refuses_backwards_events() -> None:
    clock = EventClock(ReplayClockMode.LOCAL_RECEIVE_TIME)
    clock.advance(event(receive=10, sequence=1))
    with pytest.raises(ValueError, match="backwards"):
        clock.advance(event(receive=9, sequence=2))


def test_exchange_clock_requires_exchange_timestamp() -> None:
    value = event(receive=10, sequence=1)
    missing = ReplayEvent(
        event_type=value.event_type,
        exchange_timestamp_ns=None,
        received_utc_ns=value.received_utc_ns,
        received_monotonic_ns=value.received_monotonic_ns,
        connection_id=value.connection_id,
        sequence=value.sequence,
        parent_change_index=value.parent_change_index,
        source_priority=value.source_priority,
        payload=None,
    )
    with pytest.raises(ValueError, match="lacks exchange timestamp"):
        missing.timestamp(ReplayClockMode.EXCHANGE_TIME)
