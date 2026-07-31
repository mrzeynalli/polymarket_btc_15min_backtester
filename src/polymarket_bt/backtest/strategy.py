from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from polymarket_bt.backtest.portfolio import Portfolio
from polymarket_bt.models.orders import OrderIntent
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.orderbook.state import OrderBook
from polymarket_bt.replay.event_clock import ReplayEvent


@dataclass(slots=True)
class TimerEvent:
    timestamp_ns: int


@dataclass(slots=True)
class MarketCloseEvent:
    timestamp_ns: int
    condition_id: str
    winning_token_id: str


@dataclass(slots=True)
class StrategyContext:
    portfolio: Portfolio
    now_ns: int = 0
    books: dict[str, OrderBook] = field(default_factory=dict)
    btc_prices: dict[str, BtcPriceEvent] = field(default_factory=dict)

    def book(self, token_id: str) -> OrderBook | None:
        return self.books.get(token_id)

    def valid_books(self) -> tuple[OrderBook, ...]:
        return tuple(book for book in self.books.values() if book.valid)


class Strategy(Protocol):
    strategy_id: str

    def on_start(self, context: StrategyContext) -> None: ...

    def on_market_event(
        self, event: ReplayEvent, context: StrategyContext
    ) -> list[OrderIntent]: ...

    def on_btc_price(self, event: BtcPriceEvent, context: StrategyContext) -> list[OrderIntent]: ...

    def on_trade(self, event: TradeEvent, context: StrategyContext) -> list[OrderIntent]: ...

    def on_timer(self, event: TimerEvent, context: StrategyContext) -> list[OrderIntent]: ...

    def on_market_close(
        self, event: MarketCloseEvent, context: StrategyContext
    ) -> list[OrderIntent]: ...
