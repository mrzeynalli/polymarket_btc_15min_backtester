from __future__ import annotations

from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.normalization.deduplication import deduplicate_trades


def trade(sequence: int) -> TradeEvent:
    return TradeEvent(
        trade_event_id=f"event-{sequence}",
        sequence=sequence,
        condition_id="condition",
        token_id="token",
        outcome="UP",
        exchange_timestamp_ns=100,
        received_utc_ns=100 + sequence,
        received_monotonic_ns=sequence,
        price_scaled=500_000,
        size_scaled=1_000_000,
        notional_scaled=500_000,
        reported_side="BUY",
        fee_rate_bps_scaled=0,
        transaction_hash="0xhash",
        trade_id_when_available=None,
        source="fixture",
    )


def test_composite_trade_deduplication() -> None:
    assert len(deduplicate_trades([trade(1), trade(2)])) == 1
