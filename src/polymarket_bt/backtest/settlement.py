from __future__ import annotations

from polymarket_bt.backtest.portfolio import Portfolio


def settle_categorical_market(
    portfolio: Portfolio,
    *,
    condition_id: str,
    winning_token_id: str,
    resolution_available_utc_ns: int,
) -> int:
    return portfolio.settle_market(condition_id, winning_token_id, resolution_available_utc_ns)
