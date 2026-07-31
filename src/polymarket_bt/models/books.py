from __future__ import annotations

from typing import Literal

from polymarket_bt.constants import SCHEMA_VERSION
from polymarket_bt.models.events import FrozenModel


class BookLevel(FrozenModel):
    price_scaled: int
    size_scaled: int


class BookSnapshot(FrozenModel):
    schema_version: int = SCHEMA_VERSION
    snapshot_id: str
    sequence: int
    run_id: str
    connection_id: str
    source: str
    condition_id: str
    token_id: str
    outcome: str
    exchange_timestamp_ns: int | None
    received_utc_ns: int
    received_monotonic_ns: int
    book_hash: str | None = None
    tick_size_scaled: int
    minimum_order_size_scaled: int
    last_trade_price_scaled: int | None = None
    neg_risk: bool = False
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    raw_event_reference: str | None = None


class BookLevelChange(FrozenModel):
    schema_version: int = SCHEMA_VERSION
    parent_event_id: str
    change_index: int
    sequence: int
    condition_id: str
    token_id: str
    outcome: str
    exchange_timestamp_ns: int | None
    received_utc_ns: int
    received_monotonic_ns: int
    side: Literal["BUY", "SELL"]
    price_scaled: int
    new_size_scaled: int
    book_hash: str | None = None
    reported_best_bid_scaled: int | None = None
    reported_best_ask_scaled: int | None = None
    connection_id: str
    raw_event_reference: str | None = None


class TopOfBook(FrozenModel):
    condition_id: str
    token_id: str
    sequence: int
    received_utc_ns: int
    best_bid_scaled: int | None
    best_ask_scaled: int | None
    spread_scaled: int | None
    midpoint_scaled: int | None
    bid_size_scaled: int | None
    ask_size_scaled: int | None
