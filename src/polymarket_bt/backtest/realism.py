"""Execution realism for taker orders in a thin, fast, 15-minute book.

The difference between a backtest that reproduces a 90% win rate and one that
predicts live P&L is entirely in this module.  Four effects dominate, and each is
modelled explicitly and configurably rather than assumed away:

**Latency.** A decision is made on a book state that is already old, and the order
matches against the book that exists when it arrives.  In the last three minutes of
these markets the top of book changes every few milliseconds, so a 60-100 ms round
trip means acting on information that is dozens of events stale.  Calibrate from
the collector's own measurements (`calibrate_latency`), not from a guess.

**Displayed depth is an upper bound.** Level-2 shows resting size, not size you can
be certain to take.  Quotes are pulled the instant the underlying moves, and a
retail stop competes with every other taker reacting to the same tick.  Two
independent haircuts express this: a fraction of displayed size assumed
unreachable, and a cap on the fraction of any one level a single order may take.

**Competition for the same liquidity.** A causal, time-sliced participation policy
may cap child orders by volume already observed before each decision.  Immediate
FAK sweeps do not use later public prints: trades that happen after arrival cannot
change what the matching engine gave an order at arrival.

**Self-impact.** Simulated fills consume the displayed levels they take, and that
depth does not reappear until a recorded event replenishes it.

Nothing here models hidden liquidity, exchange rejects, or matching-engine
priority races beyond the haircut, so a realistic setting is deliberately
pessimistic rather than neutral.
"""

from __future__ import annotations

import math
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import Field

from polymarket_bt.backtest.fees import FeeModel
from polymarket_bt.config import FeeConfig, LatencyConfig, StrictModel
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE

EXECUTION_MODEL_VERSION = "taker-realism-v2"

PPM = 1_000_000


class ExecutionRealism(StrictModel):
    """Configurable pessimism applied to every simulated taker order."""

    name: str = "base"
    latency: LatencyConfig = Field(default_factory=LatencyConfig)
    # Fraction of displayed size assumed to be unreachable (cancelled, stale, or
    # taken by someone faster) at the instant the order arrives.
    displayed_depth_haircut_ppm: int = Field(default=0, ge=0, lt=PPM)
    # Largest fraction of a single displayed level one order may consume.
    max_level_participation_ppm: int = Field(default=PPM, gt=0, le=PPM)
    # Optional scenario constraint on how many levels a marketable order may
    # sweep.  `None` means the complete ladder stored in the tape; a finite value
    # must never be mistaken for missing venue depth.
    max_levels_swept: int | None = Field(default=None, ge=1)
    # Optional cap supplied by a causal participation policy.  The immediate FAK
    # simulator always passes `None` because post-arrival prints are future data.
    volume_participation_ppm: int | None = Field(default=None, gt=0)
    volume_participation_window_ms: int = Field(default=2_000, gt=0)
    # Refuse to act on a book that has not been updated recently enough.
    stale_book_max_age_ms: int | None = Field(default=None, gt=0)
    # Refuse to trade a token whose reconstruction is inside an uncertainty span.
    require_valid_book: bool = True
    # Worst price the order may pay/accept, expressed as ticks through the touch.
    # These markets quote on two different ticks (0.001 and 0.01), so a tick count
    # alone means ten times more price on one than the other; the absolute bound
    # below keeps the two comparable. Whichever is tighter applies.
    max_ticks_through_touch: int | None = Field(default=None, ge=0)
    max_price_through_touch_scaled: int | None = Field(default=None, ge=0)
    fee: FeeConfig

    @property
    def version(self) -> str:
        return EXECUTION_MODEL_VERSION


@dataclass(frozen=True, slots=True)
class FillSlice:
    """One consumed price level."""

    price_scaled: int
    size_scaled: int
    notional_scaled: int
    fee_scaled: int
    level_rank: int


@dataclass(slots=True)
class ExecutionOutcome:
    """Result of one simulated marketable order."""

    requested_shares_scaled: int
    filled_shares_scaled: int
    notional_scaled: int
    fees_scaled: int
    slices: list[FillSlice] = field(default_factory=list)
    rejected_reason: str | None = None
    binding_constraint: str | None = None
    arrival_utc_ns: int = 0
    decision_utc_ns: int = 0
    reference_price_scaled: int | None = None
    # Which direction counts as adverse when measuring slippage.
    side_is_buy: bool = True

    @property
    def filled(self) -> bool:
        return self.filled_shares_scaled > 0

    @property
    def average_price_scaled(self) -> int | None:
        if not self.filled_shares_scaled:
            return None
        return sum(s.price_scaled * s.size_scaled for s in self.slices) // self.filled_shares_scaled

    @property
    def slippage_scaled(self) -> int | None:
        """Signed distance between the average fill and the price that triggered it.

        Positive means the fill was worse than the reference: a buy paid more, a
        sell received less.  Negative means it was better.  Reporting the absolute
        value here would count a favourable stop fill as slippage and inflate the
        average, which is the opposite of what the number is read for.
        """
        average = self.average_price_scaled
        if average is None or self.reference_price_scaled is None:
            return None
        difference = average - self.reference_price_scaled
        return difference if self.side_is_buy else -difference


def notional_scaled(size_scaled: int, price_scaled: int) -> int:
    raw = Decimal(size_scaled) * Decimal(price_scaled) / POLYMARKET_PRICE_SCALE
    return int(raw.quantize(Decimal(1), rounding=ROUND_HALF_UP))


class TakerExecutor:
    """Applies `ExecutionRealism` to a depth ladder taken from a tape row."""

    def __init__(self, realism: ExecutionRealism, *, seed: int) -> None:
        self.realism = realism
        self.fee_model = FeeModel(realism.fee)
        self.random = random.Random(seed)

    def sample_latency_ns(self) -> int:
        config = self.realism.latency
        if config.model == "disabled":
            milliseconds = 0.0
        elif config.model == "constant":
            milliseconds = float(config.constant_ms)
        elif config.model == "empirical":
            if not config.samples_ms:
                raise ValueError("empirical latency requires samples_ms")
            milliseconds = float(self.random.choice(config.samples_ms))
        else:
            milliseconds = math.exp(
                self.random.normalvariate(config.lognormal_mu, config.lognormal_sigma)
            )
        return max(0, int(milliseconds * 1_000_000))

    def execute(
        self,
        *,
        side: Literal["BUY", "SELL"],
        ladder: Sequence[tuple[int, int]],
        requested_shares_scaled: int,
        limit_price_scaled: int | None,
        reference_price_scaled: int | None,
        realised_volume_scaled: int | None,
        decision_utc_ns: int,
        arrival_utc_ns: int,
        book_age_ns: int,
        minimum_order_size_scaled: int,
        tick_size_scaled: int,
    ) -> ExecutionOutcome:
        """Walk `ladder` (best level first) under every configured constraint."""
        outcome = ExecutionOutcome(
            requested_shares_scaled=requested_shares_scaled,
            filled_shares_scaled=0,
            notional_scaled=0,
            fees_scaled=0,
            decision_utc_ns=decision_utc_ns,
            arrival_utc_ns=arrival_utc_ns,
            reference_price_scaled=reference_price_scaled,
            side_is_buy=side == "BUY",
        )
        realism = self.realism
        if requested_shares_scaled <= 0:
            outcome.rejected_reason = "non_positive_size"
            return outcome
        if not ladder:
            outcome.rejected_reason = "empty_book_side"
            return outcome
        # The venue minimum governs the order you may submit, not how much of it
        # the book happens to match.  Checking the fill instead discarded ordinary
        # partial fills as if they had never happened, and — because the strategy
        # retries — produced one phantom rejection per book state for the rest of
        # the market.
        if requested_shares_scaled < minimum_order_size_scaled:
            outcome.rejected_reason = "below_minimum_order_size"
            return outcome
        if realism.stale_book_max_age_ms is not None and book_age_ns > (
            realism.stale_book_max_age_ms * 1_000_000
        ):
            outcome.rejected_reason = "stale_book"
            return outcome

        touch = ladder[0][0]
        limit, limit_source = self._effective_limit(
            side, touch, limit_price_scaled, tick_size_scaled
        )
        volume_cap = self._volume_cap(realised_volume_scaled, minimum_order_size_scaled)
        remaining = requested_shares_scaled
        slices: list[FillSlice] = []
        consumed = 0
        # Which rules actually cut this order short, as opposed to merely applying.
        # A haircut that leaves more depth than the order wanted has bound nothing,
        # and reporting it as the binding constraint blames the book for a fill it
        # was never asked to give.
        hit_limit = False
        cut_by_depth = False
        cut_by_volume = False

        visible_ladder = (
            ladder if realism.max_levels_swept is None else ladder[: realism.max_levels_swept]
        )
        hit_level_cap = (
            realism.max_levels_swept is not None and len(ladder) > realism.max_levels_swept
        )
        for rank, (price, displayed) in enumerate(visible_ladder, start=1):
            if limit is not None and (
                (side == "BUY" and price > limit) or (side == "SELL" and price < limit)
            ):
                hit_limit = True
                break
            reachable = displayed * (PPM - realism.displayed_depth_haircut_ppm) // PPM
            allowed = min(reachable, displayed * realism.max_level_participation_ppm // PPM)
            take = min(remaining, allowed)
            if allowed < remaining:
                cut_by_depth = True
            if volume_cap is not None:
                headroom = max(0, volume_cap - consumed)
                if take > headroom:
                    take = headroom
                    cut_by_volume = True
            if take <= 0:
                if volume_cap is not None and consumed >= volume_cap:
                    break
                continue
            fee = self.fee_model.calculate(take, price)
            slices.append(
                FillSlice(
                    price_scaled=price,
                    size_scaled=take,
                    notional_scaled=notional_scaled(take, price),
                    fee_scaled=fee,
                    level_rank=rank,
                )
            )
            remaining -= take
            consumed += take
            if remaining <= 0:
                break

        binding: str | None = None
        if remaining > 0:
            if hit_limit:
                # The strategy's own ceiling and the model's refusal to chase the
                # book are different answers to "why didn't I get filled", and only
                # one of them is something the user can change.
                binding = limit_source or "limit_price"
            elif cut_by_volume:
                binding = (
                    "no_recent_public_volume"
                    if not realised_volume_scaled
                    else "volume_participation"
                )
            elif hit_level_cap:
                binding = "max_levels_swept"
            elif cut_by_depth:
                binding = "displayed_depth_haircut"
            else:
                binding = "book_depth_exhausted"

        filled = sum(item.size_scaled for item in slices)
        if not filled:
            outcome.rejected_reason = binding or "no_executable_liquidity"
            return outcome
        outcome.slices = slices
        outcome.filled_shares_scaled = filled
        outcome.notional_scaled = sum(item.notional_scaled for item in slices)
        outcome.fees_scaled = sum(item.fee_scaled for item in slices)
        outcome.binding_constraint = binding
        return outcome

    def _effective_limit(
        self,
        side: Literal["BUY", "SELL"],
        touch_scaled: int,
        limit_price_scaled: int | None,
        tick_size_scaled: int,
    ) -> tuple[int | None, str | None]:
        """The worst price this order may accept, and which rule set it."""
        ticks = self.realism.max_ticks_through_touch
        absolute = self.realism.max_price_through_touch_scaled
        if ticks is None and absolute is None:
            return limit_price_scaled, "limit_price" if limit_price_scaled is not None else None
        # Two ways of saying "I will not chase the book further than this", and a
        # real order carries both: at most N quote increments, and at most so many
        # cents.  Using a fixed cent as if it were a tick — which this did — let an
        # order sweep ten times further on a fine-tick market than a coarse one.
        bounds = []
        if ticks is not None:
            bounds.append(ticks * max(tick_size_scaled, 1))
        if absolute is not None:
            bounds.append(absolute)
        slack = min(bounds)
        bound = touch_scaled + slack if side == "BUY" else touch_scaled - slack
        if limit_price_scaled is None:
            return bound, "max_price_through_touch"
        tighter = (
            min(limit_price_scaled, bound) if side == "BUY" else max(limit_price_scaled, bound)
        )
        return tighter, (
            "limit_price" if tighter == limit_price_scaled else "max_price_through_touch"
        )

    def _volume_cap(
        self, realised_volume_scaled: int | None, minimum_order_size_scaled: int
    ) -> int | None:
        """How much of the local flow one order may claim.

        The order's own fill would itself have printed, so the constraint is
        `X / (V + X) <= p`, giving `X <= V*p/(1-p)` — not `X <= V*p`, which asks
        the order to be a share of a tape it is not part of.

        The floor matters more than the formula.  Half of the book states in this
        data see no public print at all on the same token in the following two
        seconds, and a bare proportional cap turns that into a refusal to trade.
        Nobody else trading says nothing about whether the resting book would have
        filled you; it must bound size, never veto the order.
        """
        participation = self.realism.volume_participation_ppm
        if participation is None or realised_volume_scaled is None:
            return None
        cap = realised_volume_scaled * participation // max(PPM - participation, 1)
        return max(cap, minimum_order_size_scaled)


def calibrate_latency(
    storage_root: Path,
    *,
    connection: duckdb.DuckDBPyConnection | None = None,
    strategy_compute_ms: int = 5,
) -> dict[str, float | int]:
    """Derive a latency distribution from what this server actually measured.

    Two public measurements bound the decision-to-match path: the CLOB WebSocket
    heartbeat round trip (network to the same venue) and the REST request duration
    (a full request/response to the CLOB host, which is the closest public analogue
    of an order submission).  The one-way inbound leg is already inside the event
    timestamps, so the modelled latency covers strategy compute plus the outbound
    submission and exchange processing.

    A live authenticated system must re-measure this against its own order
    acknowledgements; these values are the honest public-data starting point, not
    a substitute for that.
    """
    owned = connection is None
    connection = connection or duckdb.connect()
    try:
        heartbeat = (
            storage_root / "normalized" / "heartbeat_events" / "**" / "*.parquet"
        ).as_posix()
        rest = (storage_root / "normalized" / "rest_requests" / "**" / "*.parquet").as_posix()
        row = connection.execute(
            f"""
            select
              (select quantile_cont(round_trip_ns / 1e6, 0.50)
                 from read_parquet('{heartbeat}', hive_partitioning=false)
                 where round_trip_ns is not null) as heartbeat_p50_ms,
              (select quantile_cont(duration_ns / 1e6, 0.50)
                 from read_parquet('{rest}', hive_partitioning=false)) as rest_p50_ms,
              (select quantile_cont(duration_ns / 1e6, 0.95)
                 from read_parquet('{rest}', hive_partitioning=false)) as rest_p95_ms
            """
        ).fetchone()
    finally:
        if owned:
            connection.close()
    measured = row or (None, None, None)
    heartbeat_p50 = float(measured[0] or 40.0)
    rest_p50 = float(measured[1] or 40.0)
    rest_p95 = float(measured[2] or 90.0)
    median = strategy_compute_ms + rest_p50
    tail = strategy_compute_ms + rest_p95
    mu = math.log(max(median, 1.0))
    sigma = max(0.05, (math.log(max(tail, median + 1.0)) - mu) / 1.6449)
    return {
        "heartbeat_rtt_p50_ms": round(heartbeat_p50, 3),
        "rest_p50_ms": round(rest_p50, 3),
        "rest_p95_ms": round(rest_p95, 3),
        "median_ms": round(median, 3),
        "p95_ms": round(tail, 3),
        "lognormal_mu": round(mu, 6),
        "lognormal_sigma": round(sigma, 6),
    }


DEFAULT_FEE = FeeConfig(
    version="polymarket-fee-schedule-v2-2026-03-31",
    effective_start="2026-03-31T00:00:00Z",
    market_type="crypto",
    liquidity_role="taker",
    formula="shares_rate_p_one_minus_p",
    rate="0.07",
    exponent=1,
    minimum_fee="0.000001",
    rounding="half_up",
)


def preset(
    name: Literal["optimistic", "base", "pessimistic"],
    *,
    latency: LatencyConfig | None = None,
    fee: FeeConfig | None = None,
) -> ExecutionRealism:
    """Three defensible points on the pessimism axis.

    Report all three.  A strategy whose conclusion flips between `optimistic` and
    `pessimistic` has not been shown to work; it has been shown to depend on
    assumptions the public data cannot settle.
    """
    fee = fee or DEFAULT_FEE
    if name == "optimistic":
        return ExecutionRealism(
            name="optimistic",
            latency=latency or LatencyConfig(model="constant", constant_ms=25),
            displayed_depth_haircut_ppm=0,
            max_level_participation_ppm=PPM,
            max_levels_swept=None,
            volume_participation_ppm=None,
            stale_book_max_age_ms=None,
            require_valid_book=True,
            fee=fee,
        )
    if name == "pessimistic":
        return ExecutionRealism(
            name="pessimistic",
            latency=latency or LatencyConfig(model="constant", constant_ms=250),
            displayed_depth_haircut_ppm=500_000,
            max_level_participation_ppm=250_000,
            max_levels_swept=5,
            volume_participation_ppm=None,
            stale_book_max_age_ms=2_000,
            require_valid_book=True,
            max_ticks_through_touch=5,
            max_price_through_touch_scaled=5_000,
            fee=fee,
        )
    return ExecutionRealism(
        name="base",
        latency=latency or LatencyConfig(model="lognormal", lognormal_mu=3.8, lognormal_sigma=0.45),
        displayed_depth_haircut_ppm=250_000,
        max_level_participation_ppm=500_000,
        max_levels_swept=None,
        volume_participation_ppm=None,
        stale_book_max_age_ms=5_000,
        require_valid_book=True,
        max_ticks_through_touch=10,
        max_price_through_touch_scaled=10_000,
        fee=fee,
    )


def shares(count: float) -> int:
    """Convenience: whole shares to the fixed-point share scale."""
    return round(count * SHARE_SIZE_SCALE)
