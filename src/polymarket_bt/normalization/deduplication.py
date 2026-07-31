from __future__ import annotations

from collections.abc import Iterable

from polymarket_bt.models.trades import TradeEvent


def trade_deduplication_key(trade: TradeEvent) -> tuple[object, ...]:
    if trade.trade_id_when_available:
        return ("trade_id", trade.trade_id_when_available)
    return (
        "composite",
        trade.transaction_hash,
        trade.condition_id,
        trade.token_id,
        trade.exchange_timestamp_ns,
        trade.price_scaled,
        trade.size_scaled,
        trade.reported_side,
    )


def deduplicate_trades(trades: Iterable[TradeEvent]) -> list[TradeEvent]:
    unique: dict[tuple[object, ...], TradeEvent] = {}
    for trade in trades:
        unique.setdefault(trade_deduplication_key(trade), trade)
    return sorted(unique.values(), key=lambda item: (item.received_utc_ns, item.sequence))
