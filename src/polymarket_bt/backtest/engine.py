from __future__ import annotations

import heapq
import itertools
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

from polymarket_bt.backtest.execution import TakerExecutionSimulator
from polymarket_bt.backtest.fees import FeeModel
from polymarket_bt.backtest.latency import LatencyModel
from polymarket_bt.backtest.metrics import maximum_drawdown, percentile
from polymarket_bt.backtest.portfolio import Portfolio
from polymarket_bt.backtest.settlement import settle_categorical_market
from polymarket_bt.backtest.strategy import MarketCloseEvent, Strategy, StrategyContext
from polymarket_bt.clock import decimal_to_scaled
from polymarket_bt.config import BacktestConfig
from polymarket_bt.constants import USDC_SCALE, ReplayClockMode
from polymarket_bt.models.orders import OrderIntent, OrderResult, SimulatedOrderType
from polymarket_bt.models.results import BacktestSummary
from polymarket_bt.orderbook.reconstructor import BookReconstructor
from polymarket_bt.replay.event_clock import EventClock, ReplayEvent
from polymarket_bt.replay.integrity import QualityInterval, ReplayIntegrity
from polymarket_bt.replay.merger import merge_events


@dataclass(slots=True)
class BacktestArtifacts:
    backtest_run_id: str
    summary: BacktestSummary
    order_results: list[OrderResult]
    portfolio_events: list[dict[str, int | str]]
    market_results: list[dict[str, Any]]
    quality_intervals_used: list[QualityInterval]
    slippages_scaled: list[int] = field(default_factory=list)


class BacktestEngine:
    def __init__(
        self,
        config: BacktestConfig,
        strategy: Strategy,
        *,
        quality_intervals: list[QualityInterval] | None = None,
    ) -> None:
        self.config = config
        self.strategy = strategy
        self.mode = ReplayClockMode(config.replay_clock)
        self.clock = EventClock(self.mode)
        self.integrity = ReplayIntegrity(
            quality_intervals or [], reject_on_gap=config.reject_on_gap
        )
        starting_cash = decimal_to_scaled(config.starting_cash, USDC_SCALE, field="starting_cash")
        self.portfolio = Portfolio(
            starting_cash,
            allow_negative_cash=config.allow_negative_cash,
            allow_short_positions=config.allow_short_positions,
        )
        self.reconstructor = BookReconstructor()
        self.context = StrategyContext(portfolio=self.portfolio, books=self.reconstructor.books)
        self.latency = LatencyModel(config.latency, seed=config.random_seed)
        self.execution = TakerExecutionSimulator(FeeModel(config.fee))
        self.pending: list[tuple[int, int, OrderIntent]] = []
        self._intent_counter = itertools.count()
        self.order_results: list[OrderResult] = []
        self.market_results: list[dict[str, Any]] = []
        self.slippages: list[int] = []
        self.resolved_conditions: set[str] = set()

    def _schedule(self, intents: list[OrderIntent]) -> None:
        for intent in intents:
            counter = next(self._intent_counter)
            deterministic_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{self.strategy.strategy_id}:{intent.decision_timestamp_ns}:{counter}:{intent.token_id}",
                )
            )
            book = self.reconstructor.books.get(intent.token_id)
            reference = None
            if book:
                reference = book.best_ask if intent.side.value == "BUY" else book.best_bid
            metadata = dict(intent.metadata)
            if reference is not None:
                metadata["decision_reference_price_scaled"] = str(reference)
            deterministic = intent.model_copy(
                update={"order_intent_id": deterministic_id, "metadata": metadata}
            )
            if deterministic.order_type == SimulatedOrderType.LIMIT_GTC_SIMULATED:
                self.order_results.append(
                    OrderResult(
                        intent=deterministic,
                        status="rejected",
                        rejection_reason=(
                            "maker simulation requires explicit queue-event workflow; "
                            f"configured model={self.config.maker_queue_model}"
                        ),
                    )
                )
                continue
            arrival = self.latency.schedule(deterministic.decision_timestamp_ns)
            heapq.heappush(self.pending, (arrival, counter, deterministic))

    def _execute_pending(self, cutoff_ns: int, *, inclusive: bool) -> None:
        while self.pending and (
            self.pending[0][0] < cutoff_ns or (inclusive and self.pending[0][0] == cutoff_ns)
        ):
            arrival, _, intent = heapq.heappop(self.pending)
            if intent.condition_id in self.resolved_conditions:
                result = OrderResult(
                    intent=intent, status="rejected", rejection_reason="market already resolved"
                )
            else:
                book = self.reconstructor.books.get(intent.token_id)
                if book is None:
                    result = OrderResult(
                        intent=intent, status="rejected", rejection_reason="token book unavailable"
                    )
                else:
                    result = self.execution.execute(
                        intent, book, self.portfolio, arrival_time_ns=arrival
                    )
            self.order_results.append(result)
            if result.fills and "decision_reference_price_scaled" in intent.metadata:
                reference = int(intent.metadata["decision_reference_price_scaled"])
                total_size = sum(fill.size_scaled for fill in result.fills)
                average = (
                    sum(fill.price_scaled * fill.size_scaled for fill in result.fills) // total_size
                )
                slippage = (
                    average - reference if intent.side.value == "BUY" else reference - average
                )
                self.slippages.append(slippage)

    def _apply_event(self, event: ReplayEvent) -> list[OrderIntent]:
        self.context.now_ns = event.timestamp(self.mode)
        payload = event.payload
        if event.event_type == "book_snapshot":
            self.reconstructor.apply_snapshot(payload)
            return self.strategy.on_market_event(event, self.context)
        if event.event_type == "book_update":
            self.reconstructor.apply_change(payload)
            return self.strategy.on_market_event(event, self.context)
        if event.event_type == "btc_price":
            self.context.btc_prices[payload.source] = payload
            return self.strategy.on_btc_price(payload, self.context)
        if event.event_type == "trade":
            return self.strategy.on_trade(payload, self.context)
        if event.event_type == "market_resolution":
            condition_id = str(payload["condition_id"])
            winning_token_id = str(payload["winning_token_id"])
            payout = settle_categorical_market(
                self.portfolio,
                condition_id=condition_id,
                winning_token_id=winning_token_id,
                resolution_available_utc_ns=self.context.now_ns,
            )
            self.resolved_conditions.add(condition_id)
            self.market_results.append(
                {
                    "condition_id": condition_id,
                    "winning_token_id": winning_token_id,
                    "winning_outcome": str(payload.get("winning_outcome") or ""),
                    "settlement_timestamp_ns": self.context.now_ns,
                    "payout_scaled": payout,
                }
            )
            return self.strategy.on_market_close(
                MarketCloseEvent(
                    timestamp_ns=self.context.now_ns,
                    condition_id=condition_id,
                    winning_token_id=winning_token_id,
                ),
                self.context,
            )
        return []

    @staticmethod
    def _degraded_time_ppm(events: list[ReplayEvent], intervals: list[QualityInterval]) -> int:
        if not events or not intervals:
            return 0
        replay_start = min(event.received_utc_ns for event in events)
        replay_end = max(event.received_utc_ns for event in events)
        total = replay_end - replay_start
        if total <= 0:
            return 1_000_000
        spans = sorted(
            (
                max(replay_start, interval.start_utc_ns),
                min(replay_end, interval.end_utc_ns or replay_end),
            )
            for interval in intervals
        )
        merged: list[tuple[int, int]] = []
        for start, end in spans:
            if end <= start:
                continue
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        degraded = sum(end - start for start, end in merged)
        return min(1_000_000, degraded * 1_000_000 // total)

    def run(self, events: list[ReplayEvent]) -> BacktestArtifacts:
        ordered = merge_events(events, self.mode)
        self.strategy.on_start(self.context)
        index = 0
        while index < len(ordered):
            timestamp = ordered[index].timestamp(self.mode)
            self._execute_pending(timestamp, inclusive=False)
            group: list[ReplayEvent] = []
            while index < len(ordered) and ordered[index].timestamp(self.mode) == timestamp:
                group.append(ordered[index])
                index += 1
            for event in group:
                self.integrity.check(event.received_utc_ns)
                self.clock.advance(event)
                self._schedule(self._apply_event(event))
            self._execute_pending(timestamp, inclusive=True)
        while self.pending:
            arrival = self.pending[0][0]
            self._execute_pending(arrival, inclusive=True)
        midpoint_prices = {
            token: top.midpoint_scaled
            for token, book in self.reconstructor.books.items()
            if (top := book.top()).midpoint_scaled is not None
        }
        self.portfolio.mark_to_market(midpoint_prices)
        ending = self.portfolio.equity_scaled(midpoint_prices)
        fills = [fill for result in self.order_results for fill in result.fills]
        requested = sum(result.intent.requested_shares_scaled or 0 for result in self.order_results)
        filled = sum(fill.size_scaled for fill in fills)
        cash_series = [self.portfolio.starting_cash_scaled] + [
            int(event["cash_scaled"]) for event in self.portfolio.events
        ]
        run_fingerprint = json.dumps(
            {
                "strategy": self.strategy.strategy_id,
                "seed": self.config.random_seed,
                "clock": self.config.replay_clock,
                "event_count": len(events),
                "first_sequence": min((event.sequence for event in events), default=0),
                "last_sequence": max((event.sequence for event in events), default=0),
            },
            sort_keys=True,
        )
        backtest_run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, run_fingerprint))
        summary = BacktestSummary(
            backtest_run_id=backtest_run_id,
            strategy_id=self.strategy.strategy_id,
            starting_capital_scaled=self.portfolio.starting_cash_scaled,
            ending_capital_scaled=ending,
            net_pnl_scaled=ending - self.portfolio.starting_cash_scaled,
            gross_pnl_scaled=ending
            - self.portfolio.starting_cash_scaled
            + self.portfolio.fees_paid_scaled,
            fees_scaled=self.portfolio.fees_paid_scaled,
            order_intents=len(self.order_results),
            accepted_orders=sum(
                result.status in {"filled", "partial"} for result in self.order_results
            ),
            rejected_orders=sum(result.status == "rejected" for result in self.order_results),
            fills=len(fills),
            partial_fills=sum(result.status == "partial" for result in self.order_results),
            share_volume_scaled=filled,
            notional_volume_scaled=sum(fill.notional_scaled for fill in fills),
            maximum_drawdown_scaled=maximum_drawdown(cash_series),
            average_slippage_scaled=(
                sum(self.slippages) // len(self.slippages) if self.slippages else None
            ),
            fill_ratio_ppm=filled * 1_000_000 // requested if requested else 0,
            degraded_time_ppm=self._degraded_time_ppm(ordered, self.integrity.used_degraded),
            markets=len({book.condition_id for book in self.reconstructor.books.values()}),
            skipped_markets=0,
            reproducibility={
                "random_seed": self.config.random_seed,
                "fee_model_version": self.config.fee.version,
                "latency_model_version": self.latency.version,
                "execution_model_version": self.execution.version,
                "queue_model_version": self.config.maker_queue_model,
                "replay_clock_mode": self.config.replay_clock,
                "simulation_fingerprint": str(uuid.uuid5(uuid.NAMESPACE_URL, run_fingerprint)),
                "slippage_p50_scaled": percentile(self.slippages, 0.50) or 0,
                "slippage_p95_scaled": percentile(self.slippages, 0.95) or 0,
            },
        )
        return BacktestArtifacts(
            backtest_run_id=backtest_run_id,
            summary=summary,
            order_results=self.order_results,
            portfolio_events=self.portfolio.events,
            market_results=self.market_results,
            quality_intervals_used=self.integrity.used_degraded,
            slippages_scaled=self.slippages,
        )
