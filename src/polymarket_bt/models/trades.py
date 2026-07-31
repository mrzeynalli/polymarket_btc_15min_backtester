from __future__ import annotations

from polymarket_bt.constants import SCHEMA_VERSION
from polymarket_bt.models.events import FrozenModel


class TradeEvent(FrozenModel):
    schema_version: int = SCHEMA_VERSION
    trade_event_id: str
    sequence: int
    condition_id: str
    token_id: str
    outcome: str
    exchange_timestamp_ns: int | None
    received_utc_ns: int
    received_monotonic_ns: int
    price_scaled: int
    size_scaled: int | None
    notional_scaled: int | None
    reported_side: str | None
    fee_rate_bps_scaled: int | None
    transaction_hash: str | None
    trade_id_when_available: str | None
    source: str
    is_reconciled: bool = False
    reconciliation_status: str = "unreconciled"
    raw_event_reference: str | None = None
