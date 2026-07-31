from __future__ import annotations

from pydantic import Field

from polymarket_bt.models.events import FrozenModel


class PortfolioSnapshot(FrozenModel):
    timestamp_ns: int
    available_cash_scaled: int
    reserved_cash_scaled: int
    realized_pnl_scaled: int
    unrealized_pnl_scaled: int
    fees_paid_scaled: int
    settlement_receivable_scaled: int
    inventory: dict[str, int] = Field(default_factory=dict)


class BacktestSummary(FrozenModel):
    backtest_run_id: str
    strategy_id: str
    starting_capital_scaled: int
    ending_capital_scaled: int
    net_pnl_scaled: int
    gross_pnl_scaled: int
    fees_scaled: int
    order_intents: int
    accepted_orders: int
    rejected_orders: int
    fills: int
    partial_fills: int
    share_volume_scaled: int
    notional_volume_scaled: int
    maximum_drawdown_scaled: int
    average_slippage_scaled: int | None
    fill_ratio_ppm: int
    degraded_time_ppm: int
    markets: int
    skipped_markets: int
    reproducibility: dict[str, str | int]
