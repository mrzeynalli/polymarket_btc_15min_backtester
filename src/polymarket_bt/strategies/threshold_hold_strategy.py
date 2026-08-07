"""Threshold-and-hold as a `Strategy` for the audited event engine.

The tape simulator in `backtest.fastsim` exists for speed; this class exists so the
same parameters can be replayed through `BacktestEngine`, which applies every event
in its documented deterministic order and enforces its own no-look-ahead rules.
Agreement between the two is what makes the fast results trustworthy — see
`backtest.verify`.

The decision logic here is intentionally a direct transcription of the simulator's:
trigger on the executable ask inside the entry window, exit on the executable bid at
the stop-loss price.  Where the engine's own execution model differs (it has no
liquidity haircut and no volume participation cap), that difference is the point of
the comparison and is controlled by the caller.
"""

from __future__ import annotations

from polymarket_bt.backtest.episodes import Episode
from polymarket_bt.backtest.strategy import (
    MarketCloseEvent,
    StrategyContext,
    TimerEvent,
)
from polymarket_bt.backtest.threshold_hold import STRATEGY_VERSION, ThresholdHoldParams
from polymarket_bt.models.orders import OrderIntent, Side, SimulatedOrderType
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.replay.event_clock import ReplayEvent


class ThresholdHoldStrategy:
    """Engine-compatible implementation of `ThresholdHoldParams`."""

    def __init__(self, params: ThresholdHoldParams, episode: Episode) -> None:
        self.params = params
        self.episode = episode
        self.strategy_id = f"threshold_hold-{STRATEGY_VERSION}-{params.label}"
        self.entry_from_ns = episode.start_utc_ns + int(params.entry_from_minute * 60e9)
        self.entry_to_ns = episode.start_utc_ns + int(params.entry_to_minute * 60e9)
        self.entries = 0
        self.position_token: str | None = None
        self.stop_sent = False

    def on_start(self, context: StrategyContext) -> None:
        return

    def on_market_event(self, event: ReplayEvent, context: StrategyContext) -> list[OrderIntent]:
        now = context.now_ns
        if now > self.episode.end_utc_ns:
            return []
        if self.position_token is not None:
            return self._exit_intents(context, now)
        if self.entries >= self.params.max_entries_per_episode:
            return []
        if not (self.entry_from_ns <= now <= self.entry_to_ns):
            return []
        for token_id in self._candidate_tokens():
            book = context.book(token_id)
            if book is None or not book.valid:
                continue
            ask = book.best_ask
            if ask is None or ask < self.params.entry_trigger_price_scaled:
                continue
            if ask > self.params.entry_limit_price_scaled:
                continue
            self.entries += 1
            self.position_token = token_id
            return [
                OrderIntent(
                    strategy_id=self.strategy_id,
                    decision_timestamp_ns=now,
                    token_id=token_id,
                    condition_id=self.episode.condition_id,
                    outcome=self.episode.token_outcome(token_id),
                    side=Side.BUY,
                    order_type=SimulatedOrderType.MARKETABLE_TAKER,
                    limit_price_scaled=self.params.entry_limit_price_scaled,
                    requested_shares_scaled=self._entry_size(ask),
                    time_in_force="IOC",
                    metadata={"trigger_ask_scaled": str(ask), "kind": "entry"},
                )
            ]
        return []

    def _exit_intents(self, context: StrategyContext, now: int) -> list[OrderIntent]:
        if self.stop_sent or self.params.stop_loss_price_scaled is None:
            return []
        token_id = self.position_token
        assert token_id is not None
        held = context.portfolio.inventory(token_id)
        if held <= 0:
            return []
        book = context.book(token_id)
        if book is None:
            return []
        bid = book.best_bid
        if bid is not None and bid > self.params.stop_loss_price_scaled:
            return []
        self.stop_sent = True
        return [
            OrderIntent(
                strategy_id=self.strategy_id,
                decision_timestamp_ns=now,
                token_id=token_id,
                condition_id=self.episode.condition_id,
                outcome=self.episode.token_outcome(token_id),
                side=Side.SELL,
                order_type=SimulatedOrderType.MARKETABLE_TAKER,
                requested_shares_scaled=held * self.params.stop_loss_fraction_ppm // 1_000_000,
                time_in_force="IOC",
                metadata={"trigger_bid_scaled": str(bid), "kind": "stop_loss"},
            )
        ]

    def _candidate_tokens(self) -> tuple[str, ...]:
        if self.params.side_selection == "up":
            return (self.episode.up_token_id,)
        if self.params.side_selection == "down":
            return (self.episode.down_token_id,)
        return (self.episode.up_token_id, self.episode.down_token_id)

    def _entry_size(self, ask_scaled: int) -> int:
        if self.params.order_shares_scaled is not None:
            return self.params.order_shares_scaled
        assert self.params.order_notional_scaled is not None
        return self.params.order_notional_scaled * 1_000_000 // max(ask_scaled, 1)

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
