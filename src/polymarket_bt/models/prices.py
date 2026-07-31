from __future__ import annotations

from polymarket_bt.constants import SCHEMA_VERSION
from polymarket_bt.models.events import FrozenModel


class BtcPriceEvent(FrozenModel):
    schema_version: int = SCHEMA_VERSION
    price_event_id: str
    sequence: int
    source: str
    topic: str
    symbol: str
    rtds_envelope_timestamp_ns: int | None
    underlying_source_timestamp_ns: int | None
    received_utc_ns: int
    received_monotonic_ns: int
    price_scaled: int
    connection_id: str
    raw_event_reference: str | None = None
