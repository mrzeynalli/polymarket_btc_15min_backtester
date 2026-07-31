from __future__ import annotations

from polymarket_bt.backtest.strategy import (
    MarketCloseEvent,
    StrategyContext,
    TimerEvent,
)
from polymarket_bt.models.orders import OrderIntent
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.replay.event_clock import ReplayEvent


class NoOpStrategy:
    strategy_id = "no_op-v1"

    def on_start(self, context: StrategyContext) -> None:
        return

    def on_market_event(self, event: ReplayEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_btc_price(self, event: BtcPriceEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_trade(self, event: TradeEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_timer(self, event: TimerEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_market_close(
        self, event: MarketCloseEvent, context: StrategyContext
    ) -> list[OrderIntent]:
        return []
