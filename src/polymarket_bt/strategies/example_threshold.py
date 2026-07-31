from __future__ import annotations

from polymarket_bt.backtest.strategy import (
    MarketCloseEvent,
    StrategyContext,
    TimerEvent,
)
from polymarket_bt.models.orders import OrderIntent, Side, SimulatedOrderType
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.replay.event_clock import ReplayEvent


class ExampleThresholdStrategy:
    """Demonstration only; it has no claim of profitability or production suitability."""

    strategy_id = "example_threshold-v1-nonproduction"

    def __init__(self, *, threshold_ppm: int = 100, shares_scaled: int = 5_000_000) -> None:
        self.threshold_ppm = threshold_ppm
        self.shares_scaled = shares_scaled
        self.previous_by_source: dict[str, int] = {}
        self.traded_conditions: set[str] = set()

    def on_start(self, context: StrategyContext) -> None:
        return

    def on_market_event(self, event: ReplayEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_btc_price(self, event: BtcPriceEvent, context: StrategyContext) -> list[OrderIntent]:
        previous = self.previous_by_source.get(event.source)
        self.previous_by_source[event.source] = event.price_scaled
        if previous is None or previous == 0:
            return []
        change_ppm = (event.price_scaled - previous) * 1_000_000 // previous
        if abs(change_ppm) < self.threshold_ppm:
            return []
        target_outcome = "UP" if change_ppm > 0 else "DOWN"
        candidates = sorted(
            (
                book
                for book in context.valid_books()
                if book.outcome == target_outcome
                and book.condition_id not in self.traded_conditions
            ),
            key=lambda book: book.condition_id,
        )
        if not candidates:
            return []
        book = candidates[0]
        self.traded_conditions.add(book.condition_id)
        return [
            OrderIntent(
                strategy_id=self.strategy_id,
                decision_timestamp_ns=context.now_ns,
                token_id=book.token_id,
                condition_id=book.condition_id,
                outcome=book.outcome,
                side=Side.BUY,
                order_type=SimulatedOrderType.MARKETABLE_TAKER,
                requested_shares_scaled=self.shares_scaled,
                time_in_force="IOC",
                metadata={
                    "demonstration_only": "true",
                    "btc_source": event.source,
                    "change_ppm": str(change_ppm),
                },
            )
        ]

    def on_trade(self, event: TradeEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_timer(self, event: TimerEvent, context: StrategyContext) -> list[OrderIntent]:
        return []

    def on_market_close(
        self, event: MarketCloseEvent, context: StrategyContext
    ) -> list[OrderIntent]:
        return []
