"""Cross-check the tape simulator against the audited event engine.

The fast simulator is only worth using if it agrees with the reference
implementation on the same data.  This module runs both over one episode and
compares what actually matters — which orders were sent, what they filled, at what
prices, and the resulting P&L.

Agreement requires making the two comparable on purpose:

- the realism model is stripped to the engine's own assumptions (no depth haircut,
  no participation caps, fixed latency), because the engine models none of them;
- the engine is given enough cash that its balance checks never bind;
- the engine needs a resolution event to settle, which the collector's pipeline
  does not currently produce, so one is synthesised from the episode's derived
  winner at a stated availability time.

A residual difference is expected only where the tape is truncated to its stored
depth. Anything else is a defect in the fast path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from polymarket_bt.backtest.engine import BacktestEngine
from polymarket_bt.backtest.episodes import Episode, EpisodeWindow
from polymarket_bt.backtest.fastsim import EpisodeResult, ThresholdHoldSimulator
from polymarket_bt.backtest.realism import ExecutionRealism
from polymarket_bt.backtest.tape import (
    KIND_SNAPSHOT,
    KIND_TICK,
    TapeBuilder,
    load_tape,
)
from polymarket_bt.backtest.threshold_hold import ThresholdHoldParams
from polymarket_bt.config import BacktestConfig, LatencyConfig
from polymarket_bt.constants import USDC_SCALE
from polymarket_bt.orderbook.reconstructor import BookReconstructor
from polymarket_bt.orderbook.state import InvalidBookState
from polymarket_bt.replay.event_clock import ReplayEvent
from polymarket_bt.strategies.threshold_hold_strategy import ThresholdHoldStrategy

# How long after the scheduled boundary a resolution is assumed to be visible.
# Only settlement timing depends on this; it never reaches a trading decision.
DEFAULT_RESOLUTION_DELAY_NS = 30 * 1_000_000_000


def episode_events(
    storage_root: Path,
    episode: Episode,
    *,
    window: EpisodeWindow | None = None,
    resolution_delay_ns: int = DEFAULT_RESOLUTION_DELAY_NS,
    include_resolution: bool = True,
) -> list[ReplayEvent]:
    """Load one episode's book events for the reference engine, plus settlement.

    The engine's own `EventReader` materialises every dataset in the archive; at
    roughly twenty million book updates per day of collection that is not usable
    per episode.  This reader pushes the episode's token and time bounds into the
    Parquet scan instead.
    """
    window = window or EpisodeWindow()
    start_ns = episode.start_utc_ns - window.pre_start_ns
    end_ns = episode.end_utc_ns + window.post_end_ns
    events: list[ReplayEvent] = []
    # The engine has no resynchronisation path: a level change that contradicts the
    # source's reported top, or that arrives before the token's first snapshot,
    # raises and ends the run.  Real recorded frames contain both.  Applying the
    # events here first and forwarding only those that a book accepts reproduces
    # exactly what the tape builder does, which is what makes the two comparable.
    probe = BookReconstructor()
    with TapeBuilder(storage_root, Path("/nonexistent"), window=window) as builder:
        for row in builder.iter_book_events(episode, start_ns, end_ns):
            kind = int(row["kind"])
            token_id = str(row["token_id"])
            try:
                if kind == KIND_SNAPSHOT:
                    payload: Any = builder.snapshot_of(row, episode)
                    probe.apply_snapshot(payload)
                    event_type, priority = "book_snapshot", 10
                elif kind == KIND_TICK:
                    payload = builder.tick_of(row, episode)
                    probe.apply_tick_size_change(payload)
                    event_type, priority = "tick_size_change", 15
                else:
                    payload = builder.change_of(row, episode)
                    probe.apply_change(payload)
                    event_type, priority = "book_update", 20
            except (InvalidBookState, ValueError):
                book = probe.books.get(token_id)
                if book is not None:
                    book.valid = False
                continue
            events.append(
                ReplayEvent(
                    event_type=event_type,
                    exchange_timestamp_ns=row["exchange_timestamp_ns"],
                    received_utc_ns=int(row["received_utc_ns"]),
                    received_monotonic_ns=int(row["received_monotonic_ns"]),
                    connection_id=str(row["connection_id"] or ""),
                    sequence=int(row["sequence"]),
                    parent_change_index=int(row["change_index"] or 0),
                    source_priority=priority,
                    payload=payload,
                )
            )
    if include_resolution and episode.winner_token_id:
        events.append(_resolution_event(episode, resolution_delay_ns))
    return events


def _resolution_event(episode: Episode, resolution_delay_ns: int) -> ReplayEvent:
    """Synthesise the settlement event the current pipeline does not record.

    Provenance travels with the payload: the engine settles on a *derived* winner,
    and any report built from it must say so.
    """
    available_ns = episode.end_utc_ns + resolution_delay_ns
    return ReplayEvent(
        event_type="market_resolution",
        exchange_timestamp_ns=None,
        received_utc_ns=available_ns,
        received_monotonic_ns=available_ns,
        connection_id="derived-resolution",
        sequence=2**62,
        parent_change_index=0,
        source_priority=50,
        payload={
            "condition_id": episode.condition_id,
            "winning_token_id": episode.winner_token_id,
            "winning_outcome": episode.winner_outcome or "",
            "received_utc_ns": available_ns,
            "source": f"derived:{episode.winner_source}",
            "winner_confidence": episode.winner_confidence,
        },
    )


def engine_comparable_realism(realism: ExecutionRealism, *, latency_ms: int) -> ExecutionRealism:
    """Strip the realism model down to what the reference engine also models."""
    return realism.model_copy(
        update={
            "name": f"{realism.name}-engine-comparable",
            "latency": LatencyConfig(model="constant", constant_ms=latency_ms),
            "displayed_depth_haircut_ppm": 0,
            "max_level_participation_ppm": 1_000_000,
            "max_levels_swept": None,
            "volume_participation_ppm": None,
            "stale_book_max_age_ms": None,
            "max_ticks_through_touch": None,
            "max_price_through_touch_scaled": None,
        }
    )


@dataclass(slots=True)
class Comparison:
    condition_id: str
    market_slug: str
    engine_entry_shares_scaled: int
    fast_entry_shares_scaled: int
    engine_entry_notional_scaled: int
    fast_entry_notional_scaled: int
    engine_exit_shares_scaled: int
    fast_exit_shares_scaled: int
    engine_exit_notional_scaled: int
    fast_exit_notional_scaled: int
    engine_fees_scaled: int
    fast_fees_scaled: int
    engine_net_pnl_scaled: int
    fast_net_pnl_scaled: int
    fast_result: EpisodeResult | None = None

    @property
    def entry_matches(self) -> bool:
        return (
            self.engine_entry_shares_scaled == self.fast_entry_shares_scaled
            and self.engine_entry_notional_scaled == self.fast_entry_notional_scaled
        )

    @property
    def exit_matches(self) -> bool:
        return (
            self.engine_exit_shares_scaled == self.fast_exit_shares_scaled
            and self.engine_exit_notional_scaled == self.fast_exit_notional_scaled
        )

    @property
    def pnl_difference_scaled(self) -> int:
        return self.engine_net_pnl_scaled - self.fast_net_pnl_scaled

    @property
    def agrees(self) -> bool:
        return self.entry_matches and self.exit_matches and self.pnl_difference_scaled == 0

    def to_row(self) -> dict[str, Any]:
        return {
            "condition_id": self.condition_id,
            "market_slug": self.market_slug,
            "entry_matches": self.entry_matches,
            "exit_matches": self.exit_matches,
            "engine_net_pnl": self.engine_net_pnl_scaled / USDC_SCALE,
            "fast_net_pnl": self.fast_net_pnl_scaled / USDC_SCALE,
            "pnl_difference": self.pnl_difference_scaled / USDC_SCALE,
            "agrees": self.agrees,
        }


def compare_episode(
    storage_root: Path,
    workspace: Path,
    episode: Episode,
    params: ThresholdHoldParams,
    realism: ExecutionRealism,
    *,
    latency_ms: int = 50,
    starting_cash: str = "1000000.000000",
    seed: int = 1729,
) -> Comparison:
    """Run both simulators over one episode under matched assumptions."""
    comparable = engine_comparable_realism(realism, latency_ms=latency_ms)
    fast = ThresholdHoldSimulator(params, comparable, seed=seed).run(load_tape(workspace, episode))

    config = BacktestConfig(
        strategy="threshold_hold",
        starting_cash=starting_cash,
        replay_clock="local_receive_time",
        reject_on_gap=False,
        random_seed=seed,
        latency=LatencyConfig(model="constant", constant_ms=latency_ms),
        fee=comparable.fee,
    )
    engine = BacktestEngine(config, ThresholdHoldStrategy(params, episode))
    artifacts = engine.run(episode_events(storage_root, episode))

    entry_shares = entry_notional = exit_shares = exit_notional = fees = 0
    for order in artifacts.order_results:
        for fill in order.fills:
            if order.intent.side.value == "BUY":
                entry_shares += fill.size_scaled
                entry_notional += fill.notional_scaled
            else:
                exit_shares += fill.size_scaled
                exit_notional += fill.notional_scaled
            fees += fill.fee_scaled
    engine_net = artifacts.summary.net_pnl_scaled
    return Comparison(
        condition_id=episode.condition_id,
        market_slug=episode.market_slug,
        engine_entry_shares_scaled=entry_shares,
        fast_entry_shares_scaled=fast.entry_shares_scaled,
        engine_entry_notional_scaled=entry_notional,
        fast_entry_notional_scaled=fast.entry_notional_scaled,
        engine_exit_shares_scaled=exit_shares,
        fast_exit_shares_scaled=fast.exit_shares_scaled,
        engine_exit_notional_scaled=exit_notional,
        fast_exit_notional_scaled=fast.exit_notional_scaled,
        engine_fees_scaled=fees,
        fast_fees_scaled=fast.fees_scaled,
        engine_net_pnl_scaled=engine_net,
        fast_net_pnl_scaled=fast.net_pnl_scaled,
        fast_result=fast,
    )
