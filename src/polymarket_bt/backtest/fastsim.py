"""Tape-driven simulator for threshold-and-hold variants.

This is the sweep engine: it replays one episode's reconstructed book states and
answers what a strategy variant would have done, fast enough to evaluate hundreds
of parameter combinations over months of episodes.  It is deliberately *not* the
authority.  `polymarket_bt.backtest.engine.BacktestEngine` remains the reference
simulator, and `verify.py` cross-checks agreement on sampled episodes; a
disagreement is a bug here, not there.

Two rules keep the results honest:

**Decisions read only past state.** A trigger is evaluated on a tape row, and the
resulting order is matched against the book at `decision + latency`.  Nothing
between those instants is consulted, and the settled winner is applied only after
the market boundary.

**Failure to trade is a result, not an error.** An entry that cannot fill, a stop
that fills a third of the position, an exit that prints ten cents through the
stop price — each is recorded with the constraint that bound it, because those
are the outcomes that separate a live 90% win rate from a backtested one.

**Sequential parents own their execution horizon.** TWAP/POV children are replayed
through the parent deadline before the accumulated position is handed to the hold
strategy. Stops and other exits arm only at that deadline; replaying an earlier
outer-loop row would combine a past decision with self-impact state from the
future end of the parent horizon.
"""

from __future__ import annotations

import hashlib
from bisect import bisect_left, bisect_right
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, cast

from polymarket_bt.backtest.episodes import Episode
from polymarket_bt.backtest.optimal_execution import (
    AdaptivePovPolicy,
    CausalVolumeCursor,
    ExecutionObjectiveResult,
    ExecutionPlan,
    PolicyLimits,
    SequentialExecutionPolicy,
    SequentialExecutionSession,
    TwapPolicy,
)
from polymarket_bt.backtest.realism import (
    ExecutionOutcome,
    ExecutionRealism,
    FillSlice,
    TakerExecutor,
    notional_scaled,
)
from polymarket_bt.backtest.tape import (
    BOOK_SIDE_ASKS,
    BOOK_SIDE_BIDS,
    KIND_SNAPSHOT,
    KIND_UPDATE,
    EpisodeTape,
)
from polymarket_bt.backtest.threshold_hold import STRATEGY_VERSION, ThresholdHoldParams
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE, USDC_SCALE

SIMULATOR_VERSION = "tape-sim-v2"

CashAvailability = int | Callable[[int], int]

ExitReason = Literal[
    "settlement",
    "stop_loss",
    "take_profit",
    "flatten_before_end",
    "stop_unfilled_held_to_settlement",
    "no_entry",
]


@dataclass(slots=True)
class SimulatedOrder:
    """One order the strategy sent, and what the book gave it."""

    kind: Literal["entry", "stop_loss", "take_profit", "flatten"]
    side: Literal["BUY", "SELL"]
    decision_utc_ns: int
    arrival_utc_ns: int
    decision_minute: float
    trigger_price_scaled: int | None
    requested_shares_scaled: int
    filled_shares_scaled: int
    notional_scaled: int
    fees_scaled: int
    average_price_scaled: int | None
    slippage_scaled: int | None
    levels_consumed: int
    rejected_reason: str | None
    binding_constraint: str | None

    @classmethod
    def from_outcome(
        cls,
        *,
        kind: Literal["entry", "stop_loss", "take_profit", "flatten"],
        side: Literal["BUY", "SELL"],
        outcome: ExecutionOutcome,
        episode: Episode,
        trigger_price_scaled: int | None,
    ) -> SimulatedOrder:
        return cls(
            kind=kind,
            side=side,
            decision_utc_ns=outcome.decision_utc_ns,
            arrival_utc_ns=outcome.arrival_utc_ns,
            decision_minute=round(episode.minute_offset(outcome.decision_utc_ns), 4),
            trigger_price_scaled=trigger_price_scaled,
            requested_shares_scaled=outcome.requested_shares_scaled,
            filled_shares_scaled=outcome.filled_shares_scaled,
            notional_scaled=outcome.notional_scaled,
            fees_scaled=outcome.fees_scaled,
            average_price_scaled=outcome.average_price_scaled,
            slippage_scaled=outcome.slippage_scaled,
            levels_consumed=len(outcome.slices),
            rejected_reason=outcome.rejected_reason,
            binding_constraint=outcome.binding_constraint,
        )


@dataclass(slots=True)
class EpisodeResult:
    """Everything one episode contributed, including why it contributed nothing."""

    condition_id: str
    market_slug: str
    start_utc_ns: int
    end_utc_ns: int
    winner_outcome: str | None
    traded: bool
    outcome_token: str | None = None
    outcome_side: str | None = None
    entered: bool = False
    won: bool | None = None
    exit_reason: ExitReason = "no_entry"
    entry_price_scaled: int | None = None
    entry_shares_scaled: int = 0
    entry_notional_scaled: int = 0
    exit_price_scaled: int | None = None
    exit_shares_scaled: int = 0
    exit_notional_scaled: int = 0
    settled_shares_scaled: int = 0
    settlement_payout_scaled: int = 0
    fees_scaled: int = 0
    gross_pnl_scaled: int = 0
    net_pnl_scaled: int = 0
    peak_adverse_bid_scaled: int | None = None
    stop_slippage_scaled: int | None = None
    unfilled_exit_shares_scaled: int = 0
    execution_policy: str = "immediate"
    execution_target_shares_scaled: int = 0
    execution_unfilled_shares_scaled: int = 0
    execution_fill_rate_ppm: int = 0
    execution_implementation_shortfall_scaled: int | None = None
    execution_non_fill_penalty_scaled: int | None = None
    execution_adverse_selection_penalty_scaled: int | None = None
    execution_objective_scaled: int | None = None
    skip_reason: str | None = None
    orders: list[SimulatedOrder] = field(default_factory=list)

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["orders"] = len(self.orders)
        return row


class ThresholdHoldSimulator:
    """Runs one parameter variant over one episode tape."""

    version = SIMULATOR_VERSION

    def __init__(
        self,
        params: ThresholdHoldParams,
        realism: ExecutionRealism,
        *,
        seed: int = 1729,
    ) -> None:
        self.params = params
        self.realism = realism
        self.seed = seed

    def _episode_seed(self, condition_id: str) -> int:
        digest = hashlib.blake2b(f"{self.seed}:{condition_id}".encode(), digest_size=8).digest()
        return int.from_bytes(digest, "big")

    # -- tape navigation ----------------------------------------------------
    @staticmethod
    def _token_rows(tape: EpisodeTape, token_index: int) -> tuple[list[int], list[int]]:
        """Row indices and timestamps for one token, in tape order."""
        rows: list[int] = []
        stamps: list[int] = []
        for index, value in enumerate(tape.token_index):
            if value == token_index:
                rows.append(index)
                stamps.append(tape.received_utc_ns[index])
        return rows, stamps

    @staticmethod
    def _row_at(rows: list[int], stamps: list[int], timestamp_ns: int) -> int | None:
        """Last row for this token at or before `timestamp_ns`.

        This is the state an order arriving at that instant would meet: the most
        recent book the venue had published by then.
        """
        position = bisect_right(stamps, timestamp_ns) - 1
        return rows[position] if position >= 0 else None

    # -- execution ----------------------------------------------------------
    def _send(
        self,
        *,
        tape: EpisodeTape,
        executor: TakerExecutor,
        token_index: int,
        rows: list[int],
        stamps: list[int],
        side: Literal["BUY", "SELL"],
        decision_ns: int,
        shares_scaled: int,
        max_spend_scaled: int | None,
        available_cash_scaled: CashAvailability | None,
        require_full_fill: bool,
        limit_price_scaled: int | None,
        reference_price_scaled: int | None,
        consumed: dict[tuple[int, int, int], int],
        impact_cursor: dict[int, int],
        taker_delay_ns: int,
        deadline_ns: int | None = None,
    ) -> ExecutionOutcome:
        arrival_ns = decision_ns + executor.sample_latency_ns() + taker_delay_ns
        if deadline_ns is not None and arrival_ns > deadline_ns:
            return self._rejected_outcome(
                shares_scaled=shares_scaled,
                reason="parent_deadline_elapsed",
                decision_ns=decision_ns,
                arrival_ns=arrival_ns,
                reference_price_scaled=reference_price_scaled,
                side=side,
            )
        if arrival_ns >= tape.episode.end_utc_ns:
            return self._rejected_outcome(
                shares_scaled=shares_scaled,
                reason="market_closed_at_arrival",
                decision_ns=decision_ns,
                arrival_ns=arrival_ns,
                reference_price_scaled=reference_price_scaled,
                side=side,
            )

        cash_available = self._cash_at(available_cash_scaled, arrival_ns)
        if side == "BUY" and cash_available is not None and cash_available <= 0:
            return self._rejected_outcome(
                shares_scaled=shares_scaled,
                reason="insufficient_available_cash",
                decision_ns=decision_ns,
                arrival_ns=arrival_ns,
                reference_price_scaled=reference_price_scaled,
                side=side,
            )
        # A notional-sized BUY is a requested dollar amount and an all-in spending
        # ceiling.  Quietly shrinking it when a settlement is still pending turns a
        # capital-timing failure into a different strategy, so wait and retry.
        if (
            side == "BUY"
            and max_spend_scaled is not None
            and cash_available is not None
            and cash_available < max_spend_scaled
        ):
            return self._rejected_outcome(
                shares_scaled=shares_scaled,
                reason="insufficient_available_cash",
                decision_ns=decision_ns,
                arrival_ns=arrival_ns,
                reference_price_scaled=reference_price_scaled,
                side=side,
            )

        row = self._row_at(rows, stamps, arrival_ns)
        if row is None:
            return self._rejected_outcome(
                shares_scaled=shares_scaled,
                reason="no_book_at_arrival",
                decision_ns=decision_ns,
                arrival_ns=arrival_ns,
                reference_price_scaled=reference_price_scaled,
                side=side,
            )
        token_id = tape.token_id_of(token_index)
        if self.realism.require_valid_book and (
            not tape.book_valid[row] or tape.is_uncertain(token_id, arrival_ns)
        ):
            return self._rejected_outcome(
                shares_scaled=shares_scaled,
                reason="book_state_uncertain",
                decision_ns=decision_ns,
                arrival_ns=arrival_ns,
                reference_price_scaled=reference_price_scaled,
                side=side,
            )

        self._reconcile_self_impact(
            tape=tape,
            token_index=token_index,
            rows=rows,
            through_row=row,
            consumed=consumed,
            impact_cursor=impact_cursor,
        )
        book_side = BOOK_SIDE_ASKS if side == "BUY" else BOOK_SIDE_BIDS
        ladder = self._apply_self_impact(
            tape.taker_ladder(row, side),
            {
                price: size
                for (impact_token, impact_side, price), size in consumed.items()
                if impact_token == token_index and impact_side == book_side
            },
        )
        spend_cap = max_spend_scaled
        if side == "BUY" and spend_cap is None and cash_available is not None:
            spend_cap = cash_available
        requested_shares = shares_scaled
        notional_sized = side == "BUY" and max_spend_scaled is not None
        if notional_sized and ladder:
            assert max_spend_scaled is not None
            requested_shares = self._max_affordable_shares(
                max_spend_scaled,
                ladder[0][0],
                executor,
            )
        outcome = executor.execute(
            side=side,
            ladder=ladder,
            requested_shares_scaled=requested_shares,
            limit_price_scaled=limit_price_scaled,
            reference_price_scaled=reference_price_scaled,
            # Future public prints cannot causally constrain an order that is
            # matched immediately at arrival.  A time-sliced POV policy may supply
            # causal volume observations in its own execution loop.
            realised_volume_scaled=None,
            decision_utc_ns=decision_ns,
            arrival_utc_ns=arrival_ns,
            book_age_ns=arrival_ns - tape.received_utc_ns[row],
            minimum_order_size_scaled=tape.episode.minimum_order_size_scaled,
            tick_size_scaled=tape.episode.tick_size_scaled,
        )
        cash_bound = False
        if side == "BUY" and spend_cap is not None and outcome.filled:
            cash_bound = self._cap_buy_to_spend(outcome, spend_cap, executor)
        full_fill = outcome.filled_shares_scaled == outcome.requested_shares_scaled
        if notional_sized and cash_bound:
            # Spending the complete dollar budget is a complete market BUY even
            # though the resulting share count is lower at deeper prices.
            full_fill = True
        if require_full_fill and outcome.filled and not full_fill:
            outcome.slices = []
            outcome.filled_shares_scaled = 0
            outcome.notional_scaled = 0
            outcome.fees_scaled = 0
            outcome.rejected_reason = "partial_entry_disallowed"
        # Depth this order consumed is unavailable to its own later orders until a
        # snapshot replaces the token book or an update explicitly restates that
        # exact side and price.
        if outcome.filled:
            for item in outcome.slices:
                key = (token_index, book_side, item.price_scaled)
                consumed[key] = consumed.get(key, 0) + item.size_scaled
        return outcome

    @staticmethod
    def _cash_at(source: CashAvailability | None, timestamp_ns: int) -> int | None:
        if source is None:
            return None
        value = source(timestamp_ns) if callable(source) else source
        return max(0, int(value))

    @staticmethod
    def _rejected_outcome(
        *,
        shares_scaled: int,
        reason: str,
        decision_ns: int,
        arrival_ns: int,
        reference_price_scaled: int | None,
        side: Literal["BUY", "SELL"],
    ) -> ExecutionOutcome:
        return ExecutionOutcome(
            requested_shares_scaled=shares_scaled,
            filled_shares_scaled=0,
            notional_scaled=0,
            fees_scaled=0,
            rejected_reason=reason,
            decision_utc_ns=decision_ns,
            arrival_utc_ns=arrival_ns,
            reference_price_scaled=reference_price_scaled,
            side_is_buy=side == "BUY",
        )

    @staticmethod
    def _max_affordable_shares(
        spend_scaled: int,
        price_scaled: int,
        executor: TakerExecutor,
        *,
        upper: int | None = None,
    ) -> int:
        """Largest share quantity whose notional plus taker fee fits `spend_scaled`."""
        if spend_scaled <= 0 or price_scaled <= 0:
            return 0
        high = upper
        if high is None:
            high = spend_scaled * POLYMARKET_PRICE_SCALE // price_scaled + 1
        low = 0
        while low < high:
            middle = (low + high + 1) // 2
            cost = notional_scaled(middle, price_scaled) + executor.fee_model.calculate(
                middle, price_scaled
            )
            if cost <= spend_scaled:
                low = middle
            else:
                high = middle - 1
        return low

    def _cap_buy_to_spend(
        self,
        outcome: ExecutionOutcome,
        spend_scaled: int,
        executor: TakerExecutor,
    ) -> bool:
        """Trim the terminal fill slice so BUY notional plus fees cannot exceed cash."""
        remaining = max(0, spend_scaled)
        kept: list[FillSlice] = []
        cash_bound = False
        for item in outcome.slices:
            cost = item.notional_scaled + item.fee_scaled
            if cost <= remaining:
                kept.append(item)
                remaining -= cost
                continue
            size = self._max_affordable_shares(
                remaining,
                item.price_scaled,
                executor,
                upper=item.size_scaled,
            )
            if size > 0:
                kept.append(
                    FillSlice(
                        price_scaled=item.price_scaled,
                        size_scaled=size,
                        notional_scaled=notional_scaled(size, item.price_scaled),
                        fee_scaled=executor.fee_model.calculate(size, item.price_scaled),
                        level_rank=item.level_rank,
                    )
                )
            cash_bound = True
            break
        if len(kept) < len(outcome.slices):
            cash_bound = True
        outcome.slices = kept
        outcome.filled_shares_scaled = sum(item.size_scaled for item in kept)
        outcome.notional_scaled = sum(item.notional_scaled for item in kept)
        outcome.fees_scaled = sum(item.fee_scaled for item in kept)
        if cash_bound:
            outcome.binding_constraint = "cash_budget"
        if not outcome.filled:
            outcome.rejected_reason = "insufficient_cash_including_fees"
        return cash_bound

    @staticmethod
    def _reconcile_self_impact(
        *,
        tape: EpisodeTape,
        token_index: int,
        rows: list[int],
        through_row: int,
        consumed: dict[tuple[int, int, int], int],
        impact_cursor: dict[int, int],
    ) -> None:
        last_row = impact_cursor.get(token_index, -1)
        # Sequential replay may inspect scheduled clock ticks that precede the
        # previous child's arrival. Never rewind the replenishment cursor: doing so
        # would let an unrelated old row resurrect depth consumed in the future
        # arrival state.
        if through_row <= last_row:
            return
        first = bisect_right(rows, last_row)
        stop = bisect_right(rows, through_row)
        for event_row in rows[first:stop]:
            kind = tape.event_kind[event_row]
            if kind == KIND_SNAPSHOT:
                stale = [key for key in consumed if key[0] == token_index]
                for key in stale:
                    consumed.pop(key, None)
            elif kind == KIND_UPDATE:
                consumed.pop(
                    (
                        token_index,
                        tape.event_side[event_row],
                        tape.event_price_scaled[event_row],
                    ),
                    None,
                )
        impact_cursor[token_index] = through_row

    def _execution_deadline_ns(
        self,
        *,
        tape: EpisodeTape,
        decision_ns: int,
        entry_to_ns: int,
    ) -> int:
        return min(
            entry_to_ns,
            tape.episode.end_utc_ns - 1,
            decision_ns + int(self.params.execution_horizon_seconds * 1_000_000_000),
        )

    @staticmethod
    def _apply_self_impact(
        ladder: list[tuple[int, int]], consumed: dict[int, int]
    ) -> list[tuple[int, int]]:
        if not consumed:
            return ladder
        remaining: list[tuple[int, int]] = []
        for price, size in ladder:
            left = size - consumed.get(price, 0)
            if left > 0:
                remaining.append((price, left))
        return remaining

    def _sequential_entry(
        self,
        *,
        tape: EpisodeTape,
        executor: TakerExecutor,
        token_index: int,
        rows: list[int],
        stamps: list[int],
        decision_ns: int,
        reference_price_scaled: int,
        entry_to_ns: int,
        max_spend_scaled: int | None,
        available_cash_scaled: CashAvailability | None,
        consumed: dict[tuple[int, int, int], int],
        impact_cursor: dict[int, int],
        taker_delay_ns: int,
    ) -> tuple[list[ExecutionOutcome], ExecutionObjectiveResult]:
        """Execute one parent BUY through a deterministic causal child policy."""
        params = self.params
        max_spend = max_spend_scaled
        if params.order_shares_scaled is not None:
            target_shares = params.order_shares_scaled
        else:
            assert max_spend is not None
            target_shares = self._max_affordable_shares(
                max_spend,
                reference_price_scaled,
                executor,
            )
        deadline_ns = self._execution_deadline_ns(
            tape=tape,
            decision_ns=decision_ns,
            entry_to_ns=entry_to_ns,
        )
        if deadline_ns <= decision_ns:
            raise ValueError("sequential entry needs a deadline after its decision")
        plan = ExecutionPlan(
            side="BUY",
            target_shares_scaled=target_shares,
            start_utc_ns=decision_ns,
            deadline_utc_ns=deadline_ns,
            benchmark_price_scaled=reference_price_scaled,
            limit_price_scaled=params.entry_limit_price_scaled,
        )
        decision_interval_ns = 250_000_000
        limits = PolicyLimits(
            minimum_action_interval_ns=decision_interval_ns,
            max_l2_participation_ppm=250_000,
            fee_reserve_ppm=50_000,
        )
        policy: TwapPolicy | AdaptivePovPolicy
        if params.entry_execution_policy == "twap":
            policy = TwapPolicy(slices=params.execution_slices, limits=limits)
        else:
            policy = AdaptivePovPolicy(
                participation_ppm=params.execution_participation_ppm,
                limits=limits,
            )
        cursor = CausalVolumeCursor(
            tape.trade_utc_ns,
            tape.trade_token_index,
            tape.trade_size_scaled,
            token_index=token_index,
            start_utc_ns=decision_ns,
        )
        if max_spend is not None:
            session_cash = max_spend
        else:
            external = self._cash_at(available_cash_scaled, deadline_ns)
            session_cash = external if external is not None else 2**62
        session = SequentialExecutionSession(
            plan,
            cast(SequentialExecutionPolicy, policy),
            cursor,
            initial_cash_scaled=session_cash,
            initial_inventory_shares_scaled=0,
        )

        outcomes: list[ExecutionOutcome] = []
        externally_spent = 0
        # A causal parent executor has its own clock. Driving it only from book
        # changes makes a static book incapable of TWAP/deadline execution and
        # hides trade-only information from POV. Decisions therefore occur on the
        # fixed child clock, every relevant book/trade event, and the exact
        # deadline. Each uses only the latest book at-or-before that instant.
        decisions = {decision_ns, deadline_ns}
        tick_ns = decision_ns + decision_interval_ns
        while tick_ns < deadline_ns:
            decisions.add(tick_ns)
            tick_ns += decision_interval_ns
        book_first = bisect_left(stamps, decision_ns)
        book_stop = bisect_right(stamps, deadline_ns)
        decisions.update(stamps[book_first:book_stop])
        trade_first = bisect_left(tape.trade_utc_ns, decision_ns)
        trade_stop = bisect_right(tape.trade_utc_ns, deadline_ns)
        decisions.update(
            tape.trade_utc_ns[index]
            for index in range(trade_first, trade_stop)
            if tape.trade_token_index[index] == token_index
        )
        token_id = tape.token_id_of(token_index)
        for now_ns in sorted(decisions):
            row = self._row_at(rows, stamps, now_ns)
            if row is None:
                continue
            if self.realism.require_valid_book and (
                not tape.book_valid[row] or tape.is_uncertain(token_id, now_ns)
            ):
                continue
            self._reconcile_self_impact(
                tape=tape,
                token_index=token_index,
                rows=rows,
                through_row=row,
                consumed=consumed,
                impact_cursor=impact_cursor,
            )

            def impacted(
                book_side: int,
                name: Literal["bids", "asks"],
                book_row: int = cast(int, row),
            ) -> tuple[tuple[int, int], ...]:
                taken = {
                    price: size
                    for (impact_token, impact_side, price), size in consumed.items()
                    if impact_token == token_index and impact_side == book_side
                }
                return tuple(self._apply_self_impact(tape.ladder(book_row, name), taken))

            bids = impacted(BOOK_SIDE_BIDS, "bids")
            asks = impacted(BOOK_SIDE_ASKS, "asks")
            action = session.decide(
                now_utc_ns=now_ns,
                bids=bids,
                asks=asks,
                minimum_order_size_scaled=tape.episode.minimum_order_size_scaled,
                current_reference_price_scaled=tape.best_ask_scaled[row],
            )
            if action.kind == "WAIT":
                continue

            def child_cash(
                arrival_ns: int,
                spent_before_child: int = externally_spent,
            ) -> int:
                external = self._cash_at(available_cash_scaled, arrival_ns)
                if external is None:
                    external = max_spend if max_spend is not None else session.available_cash_scaled
                return min(
                    session.available_cash_scaled,
                    max(0, external - spent_before_child),
                )

            outcome = self._send(
                tape=tape,
                executor=executor,
                token_index=token_index,
                rows=rows,
                stamps=stamps,
                side="BUY",
                decision_ns=now_ns,
                shares_scaled=action.requested_shares_scaled,
                max_spend_scaled=None,
                available_cash_scaled=child_cash,
                require_full_fill=False,
                limit_price_scaled=action.limit_price_scaled,
                reference_price_scaled=reference_price_scaled,
                consumed=consumed,
                impact_cursor=impact_cursor,
                taker_delay_ns=taker_delay_ns,
                deadline_ns=deadline_ns,
            )
            outcomes.append(outcome)
            session.record_outcome(outcome)
            externally_spent += outcome.notional_scaled + outcome.fees_scaled
            if session.remaining_shares_scaled <= 0:
                break

        mark_row = self._row_at(rows, stamps, deadline_ns)
        deadline_mark = (
            tape.best_ask_scaled[mark_row]
            if mark_row is not None and tape.best_ask_scaled[mark_row] is not None
            else reference_price_scaled
        )
        objective = session.evaluate(
            evaluation_utc_ns=deadline_ns,
            deadline_mark_price_scaled=deadline_mark,
        )
        return outcomes, objective

    @staticmethod
    def _aggregate_entry_outcomes(
        *,
        target_shares_scaled: int,
        decision_ns: int,
        reference_price_scaled: int,
        outcomes: list[ExecutionOutcome],
    ) -> ExecutionOutcome:
        slices = [item for outcome in outcomes for item in outcome.slices]
        filled = sum(item.size_scaled for item in slices)
        rejected = next(
            (outcome.rejected_reason for outcome in reversed(outcomes) if outcome.rejected_reason),
            None,
        )
        binding = next(
            (
                outcome.binding_constraint
                for outcome in reversed(outcomes)
                if outcome.binding_constraint
            ),
            None,
        )
        return ExecutionOutcome(
            requested_shares_scaled=target_shares_scaled,
            filled_shares_scaled=filled,
            notional_scaled=sum(item.notional_scaled for item in slices),
            fees_scaled=sum(item.fee_scaled for item in slices),
            slices=slices,
            rejected_reason=rejected if not filled else None,
            binding_constraint=binding,
            decision_utc_ns=decision_ns,
            arrival_utc_ns=max(
                (outcome.arrival_utc_ns for outcome in outcomes if outcome.filled),
                default=decision_ns,
            ),
            reference_price_scaled=reference_price_scaled,
            side_is_buy=True,
        )

    # -- main loop ----------------------------------------------------------
    def run(
        self,
        tape: EpisodeTape,
        available_cash_scaled: CashAvailability | None = None,
    ) -> EpisodeResult:
        """Replay one episode.

        `available_cash_scaled` may be a fixed wallet balance or a function of the
        sampled arrival timestamp.  The latter lets a compound runner model funds
        that remain locked until an earlier market actually resolves.  Entry cash
        spent inside this run is deducted before every later check.
        """
        episode = tape.episode
        result = EpisodeResult(
            condition_id=episode.condition_id,
            market_slug=episode.market_slug,
            start_utc_ns=episode.start_utc_ns,
            end_utc_ns=episode.end_utc_ns,
            winner_outcome=episode.winner_outcome,
            traded=False,
            execution_policy=self.params.entry_execution_policy,
        )
        if not episode.eligible:
            result.skip_reason = episode.exclusion_reason or "ineligible_episode"
            return result

        params = self.params
        # Seeded per episode, not per run: a single seed would hand every episode
        # the identical first latency draw, silently removing the variability the
        # model exists to represent, while a time-based seed would destroy
        # reproducibility.  Deriving it from the condition ID keeps both.
        executor = TakerExecutor(
            self._episode_realism(episode), seed=self._episode_seed(episode.condition_id)
        )
        taker_delay_ns = int(getattr(episode, "taker_order_delay_ms", 0) or 0) * 1_000_000
        entry_from_ns = episode.start_utc_ns + int(params.entry_from_minute * 60e9)
        entry_to_ns = episode.start_utc_ns + int(params.entry_to_minute * 60e9)
        flatten_ns = (
            episode.end_utc_ns - int(params.flatten_before_end_seconds * 1e9)
            if params.flatten_before_end_seconds is not None
            else None
        )
        stop_armed_ns = (
            episode.start_utc_ns + int(params.stop_loss_from_minute * 60e9)
            if params.stop_loss_from_minute is not None
            else None
        )
        allowed_tokens = self._allowed_tokens(tape)
        consumed: dict[tuple[int, int, int], int] = {}
        impact_cursor: dict[int, int] = {}

        entry_cash_debited = 0

        def cash_at(timestamp_ns: int) -> int:
            external = self._cash_at(available_cash_scaled, timestamp_ns)
            # A notional-sized run without an external wallet still has exactly
            # the requested max-spend available.  This makes fixed-notional mode
            # cash-safe by default rather than requiring dashboard cooperation.
            if external is None:
                external = params.order_notional_scaled or 2**63 - 1
            return max(0, external - entry_cash_debited)

        position_shares = 0
        position_cost = 0
        position_token: int | None = None
        token_rows: dict[int, tuple[list[int], list[int]]] = {}
        entries = 0
        next_entry_allowed_ns = entry_from_ns
        # Inventory exists only once the buy has actually arrived and matched. Book
        # states between the entry decision and its fill belong to a position the
        # strategy does not yet hold, and cannot trigger an exit from it.
        position_effective_ns = 0
        peak_adverse: int | None = None
        stop_deadline_ns: int | None = None
        stop_active = False
        stop_attempts = 0
        take_profit_attempted = False
        flatten_attempted = False
        exit_reason: ExitReason = "no_entry"
        exit_shares = 0
        exit_notional = 0
        exit_prices: list[tuple[int, int]] = []

        for index in range(len(tape)):
            timestamp = tape.received_utc_ns[index]
            if timestamp > episode.end_utc_ns:
                break
            token_index = tape.token_index[index]
            if token_index not in token_rows:
                token_rows[token_index] = self._token_rows(tape, token_index)
            rows, stamps = token_rows[token_index]

            if (
                position_shares > 0
                and token_index == position_token
                and timestamp >= position_effective_ns
            ):
                bid = tape.best_bid_scaled[index]
                if bid is not None:
                    peak_adverse = bid if peak_adverse is None else min(peak_adverse, bid)
                order, reason = self._maybe_exit(
                    tape=tape,
                    executor=executor,
                    index=index,
                    token_index=token_index,
                    rows=rows,
                    stamps=stamps,
                    timestamp=timestamp,
                    position_shares=position_shares,
                    flatten_ns=flatten_ns,
                    stop_armed_ns=stop_armed_ns,
                    stop_deadline_ns=stop_deadline_ns,
                    stop_active=stop_active,
                    take_profit_attempted=take_profit_attempted,
                    flatten_attempted=flatten_attempted,
                    consumed=consumed,
                    impact_cursor=impact_cursor,
                    taker_delay_ns=taker_delay_ns,
                )
                if order is not None:
                    result.orders.append(order)
                    if order.filled_shares_scaled:
                        position_shares -= order.filled_shares_scaled
                        exit_shares += order.filled_shares_scaled
                        exit_notional += order.notional_scaled
                        exit_prices.append(
                            (order.average_price_scaled or 0, order.filled_shares_scaled)
                        )
                        result.fees_scaled += order.fees_scaled
                        if result.stop_slippage_scaled is None and order.kind == "stop_loss":
                            result.stop_slippage_scaled = order.slippage_scaled
                    exit_reason = reason or exit_reason
                    if reason == "take_profit":
                        take_profit_attempted = True
                    elif reason == "flatten_before_end":
                        flatten_attempted = True
                    # A resend cannot precede the previous attempt's arrival.
                    position_effective_ns = max(position_effective_ns, order.arrival_utc_ns)
                    if position_shares > 0 and reason == "stop_loss":
                        stop_active = True
                        stop_attempts += 1
                        retries_used = max(0, stop_attempts - 1)
                        stop_deadline_ns = (
                            max(timestamp, order.arrival_utc_ns)
                            + params.stop_retry_interval_ms * 1_000_000
                            if retries_used < params.stop_retry_limit
                            else None
                        )
                    else:
                        stop_deadline_ns = None
                    if position_shares <= 0:
                        break
                continue

            if position_shares > 0 or entries >= params.max_entries_per_episode:
                continue
            if timestamp < next_entry_allowed_ns or timestamp > entry_to_ns:
                continue
            if token_index not in allowed_tokens:
                continue
            ask = tape.best_ask_scaled[index]
            if ask is None or ask < params.entry_trigger_price_scaled:
                continue
            if ask > params.entry_limit_price_scaled:
                continue

            # Compound sizing is decided with cash that exists at the signal
            # timestamp. A payout scheduled later in the entry window is not a
            # balance the trader can size against yet. Zero cash retains the
            # configured request so the explicit cash-pending rejection remains
            # observable and the signal can retry after a release.
            entry_spend = params.order_notional_scaled
            cash_limited = False
            if entry_spend is not None and available_cash_scaled is not None:
                cash_now = cash_at(timestamp)
                cash_limited = cash_now < entry_spend
                if cash_now > 0:
                    entry_spend = min(entry_spend, cash_now)

            if params.entry_execution_policy == "immediate":
                shares = self._entry_size(ask)
                outcome = self._send(
                    tape=tape,
                    executor=executor,
                    token_index=token_index,
                    rows=rows,
                    stamps=stamps,
                    side="BUY",
                    decision_ns=timestamp,
                    shares_scaled=shares,
                    max_spend_scaled=entry_spend,
                    available_cash_scaled=cash_at,
                    require_full_fill=not params.allow_partial_entry,
                    limit_price_scaled=params.entry_limit_price_scaled,
                    reference_price_scaled=ask,
                    consumed=consumed,
                    impact_cursor=impact_cursor,
                    taker_delay_ns=taker_delay_ns,
                )
                result.orders.append(
                    SimulatedOrder.from_outcome(
                        kind="entry",
                        side="BUY",
                        outcome=outcome,
                        episode=episode,
                        trigger_price_scaled=ask,
                    )
                )
                result.execution_target_shares_scaled = outcome.requested_shares_scaled
                result.execution_unfilled_shares_scaled = max(
                    0, outcome.requested_shares_scaled - outcome.filled_shares_scaled
                )
                result.execution_fill_rate_ppm = (
                    outcome.filled_shares_scaled * 1_000_000 // outcome.requested_shares_scaled
                    if outcome.requested_shares_scaled
                    else 0
                )
                cash_pending = outcome.rejected_reason == "insufficient_available_cash" or (
                    cash_limited and not outcome.filled
                )
                if not cash_pending:
                    entries += 1
                    next_entry_allowed_ns = timestamp + int(params.reentry_cooldown_seconds * 1e9)
                entry_effective_ns = outcome.arrival_utc_ns
            else:
                deadline_ns = self._execution_deadline_ns(
                    tape=tape,
                    decision_ns=timestamp,
                    entry_to_ns=entry_to_ns,
                )
                # At the trigger window's final instant there is no legal child
                # horizon. Treat it as an expired signal rather than constructing
                # an invalid parent plan.
                if deadline_ns <= timestamp:
                    continue
                child_outcomes, objective = self._sequential_entry(
                    tape=tape,
                    executor=executor,
                    token_index=token_index,
                    rows=rows,
                    stamps=stamps,
                    decision_ns=timestamp,
                    reference_price_scaled=ask,
                    entry_to_ns=entry_to_ns,
                    max_spend_scaled=entry_spend,
                    available_cash_scaled=cash_at,
                    consumed=consumed,
                    impact_cursor=impact_cursor,
                    taker_delay_ns=taker_delay_ns,
                )
                for child in child_outcomes:
                    result.orders.append(
                        SimulatedOrder.from_outcome(
                            kind="entry",
                            side="BUY",
                            outcome=child,
                            episode=episode,
                            trigger_price_scaled=ask,
                        )
                    )
                outcome = self._aggregate_entry_outcomes(
                    target_shares_scaled=(
                        objective.filled_shares_scaled + objective.unfilled_shares_scaled
                    ),
                    decision_ns=timestamp,
                    reference_price_scaled=ask,
                    outcomes=child_outcomes,
                )
                result.execution_target_shares_scaled = (
                    objective.filled_shares_scaled + objective.unfilled_shares_scaled
                )
                result.execution_unfilled_shares_scaled = objective.unfilled_shares_scaled
                result.execution_fill_rate_ppm = objective.fill_rate_ppm
                result.execution_implementation_shortfall_scaled = (
                    objective.implementation_shortfall_scaled
                )
                result.execution_non_fill_penalty_scaled = objective.non_fill_penalty_scaled
                result.execution_adverse_selection_penalty_scaled = (
                    objective.adverse_selection_penalty_scaled
                )
                result.execution_objective_scaled = objective.total_objective_scaled
                cash_pending = (cash_limited and not outcome.filled) or (
                    bool(child_outcomes)
                    and all(
                        child.rejected_reason == "insufficient_available_cash"
                        for child in child_outcomes
                    )
                )
                if not cash_pending:
                    entries += 1
                next_entry_allowed_ns = deadline_ns + int(params.reentry_cooldown_seconds * 1e9)
                # `_sequential_entry` has already replayed book/trade state through
                # this deadline. Handing its aggregate inventory to the exit logic
                # any earlier would let the outer loop revisit old rows while the
                # shared self-impact cursor is as-of a future point.
                entry_effective_ns = max(outcome.arrival_utc_ns, deadline_ns)
            if not outcome.filled:
                continue
            entry_cash_debited += outcome.notional_scaled + outcome.fees_scaled
            position_shares = outcome.filled_shares_scaled
            position_cost = outcome.notional_scaled
            position_token = token_index
            position_effective_ns = entry_effective_ns
            result.entered = True
            result.traded = True
            result.outcome_token = tape.token_id_of(token_index)
            result.outcome_side = episode.token_outcome(result.outcome_token)
            result.entry_price_scaled = outcome.average_price_scaled
            result.entry_shares_scaled = outcome.filled_shares_scaled
            result.entry_notional_scaled = outcome.notional_scaled
            result.fees_scaled += outcome.fees_scaled
            exit_reason = "settlement"

        if not result.entered:
            result.exit_reason = "no_entry"
            return result

        if position_shares > 0:
            # Shares the strategy tried and failed to sell are the headline risk of
            # this family of strategies; they are carried into settlement and
            # reported separately from a deliberate hold.
            if exit_reason in {"stop_loss", "take_profit", "flatten_before_end"}:
                result.unfilled_exit_shares_scaled = position_shares
            if exit_reason == "stop_loss":
                exit_reason = "stop_unfilled_held_to_settlement"
            payout = position_shares if result.outcome_token == tape.episode.winner_token_id else 0
            result.settled_shares_scaled = position_shares
            result.settlement_payout_scaled = payout * USDC_SCALE // SHARE_SIZE_SCALE

        result.exit_reason = exit_reason
        result.exit_shares_scaled = exit_shares
        result.exit_notional_scaled = exit_notional
        result.exit_price_scaled = (
            sum(price * size for price, size in exit_prices) // exit_shares if exit_shares else None
        )
        result.peak_adverse_bid_scaled = peak_adverse
        result.won = result.outcome_token == tape.episode.winner_token_id
        result.gross_pnl_scaled = (
            result.exit_notional_scaled + result.settlement_payout_scaled - position_cost
        )
        result.net_pnl_scaled = result.gross_pnl_scaled - result.fees_scaled
        return result

    def _allowed_tokens(self, tape: EpisodeTape) -> set[int]:
        selection = self.params.side_selection
        if selection == "up":
            return {tape.token_index_of(tape.episode.up_token_id)}
        if selection == "down":
            return {tape.token_index_of(tape.episode.down_token_id)}
        return {0, 1}

    def _entry_size(self, ask_scaled: int) -> int:
        params = self.params
        if params.order_shares_scaled is not None:
            return params.order_shares_scaled
        assert params.order_notional_scaled is not None
        # This is only the decision-time quantity shown on a rejection before a
        # usable arrival book exists.  A notional BUY's executable share quantity
        # is recomputed at arrival and capped inclusive of fees in `_send`.
        return params.order_notional_scaled * POLYMARKET_PRICE_SCALE // max(ask_scaled, 1)

    def _episode_realism(self, episode: Episode) -> ExecutionRealism:
        """Overlay fee fields carried by the market, retaining config fallbacks."""
        rate = getattr(episode, "fee_rate", None)
        exponent = getattr(episode, "fee_exponent", None)
        if rate is None and exponent is None:
            return self.realism
        fee_update: dict[str, object] = {}
        if rate is not None:
            fee_update["rate"] = str(rate)
        if exponent is not None:
            fee_update["exponent"] = int(exponent)
        return self.realism.model_copy(
            update={"fee": self.realism.fee.model_copy(update=fee_update)}
        )

    def _maybe_exit(
        self,
        *,
        tape: EpisodeTape,
        executor: TakerExecutor,
        index: int,
        token_index: int,
        rows: list[int],
        stamps: list[int],
        timestamp: int,
        position_shares: int,
        flatten_ns: int | None,
        stop_armed_ns: int | None,
        stop_deadline_ns: int | None,
        stop_active: bool,
        take_profit_attempted: bool,
        flatten_attempted: bool,
        consumed: dict[tuple[int, int, int], int],
        impact_cursor: dict[int, int],
        taker_delay_ns: int,
    ) -> tuple[SimulatedOrder | None, ExitReason | None]:
        params = self.params
        bid = tape.best_bid_scaled[index]
        kind: Literal["stop_loss", "take_profit", "flatten"] | None = None
        target = position_shares
        trigger = bid
        # Before the arming minute the stop does not exist, so the price is not even
        # consulted; a retry, which can only follow an armed stop, is always allowed.
        stop_armed = stop_armed_ns is None or timestamp >= stop_armed_ns

        if stop_active:
            if stop_deadline_ns is not None and timestamp >= stop_deadline_ns:
                kind = "stop_loss"
            elif not flatten_attempted and flatten_ns is not None and timestamp >= flatten_ns:
                kind = "flatten"
            else:
                return None, None
        elif (
            params.stop_loss_price_scaled is not None
            and stop_armed
            and (
                # No bid at all is the most dangerous state of the two, not a reason
                # to skip the stop: the attempt is made and recorded as unfillable.
                bid is None or bid <= params.stop_loss_price_scaled
            )
        ):
            kind = "stop_loss"
            target = position_shares * params.stop_loss_fraction_ppm // 1_000_000
        elif (
            params.take_profit_price_scaled is not None
            and not take_profit_attempted
            and bid is not None
            and bid >= params.take_profit_price_scaled
        ):
            kind = "take_profit"
            target = position_shares * params.take_profit_fraction_ppm // 1_000_000
        elif not flatten_attempted and flatten_ns is not None and timestamp >= flatten_ns:
            kind = "flatten"

        if kind is None or target <= 0:
            return None, None
        outcome = self._send(
            tape=tape,
            executor=executor,
            token_index=token_index,
            rows=rows,
            stamps=stamps,
            side="SELL",
            decision_ns=timestamp,
            shares_scaled=target,
            max_spend_scaled=None,
            available_cash_scaled=None,
            require_full_fill=False,
            limit_price_scaled=None,
            reference_price_scaled=(
                params.stop_loss_price_scaled if kind == "stop_loss" else trigger
            ),
            consumed=consumed,
            impact_cursor=impact_cursor,
            taker_delay_ns=taker_delay_ns,
        )
        order = SimulatedOrder.from_outcome(
            kind=kind,
            side="SELL",
            outcome=outcome,
            episode=tape.episode,
            trigger_price_scaled=trigger,
        )
        reason: ExitReason = (
            "stop_loss"
            if kind == "stop_loss"
            else "take_profit"
            if kind == "take_profit"
            else "flatten_before_end"
        )
        return order, reason


def strategy_fingerprint(
    params: ThresholdHoldParams, realism: ExecutionRealism, seed: int
) -> dict[str, Any]:
    return {
        "strategy_version": STRATEGY_VERSION,
        "simulator_version": SIMULATOR_VERSION,
        "execution_model_version": realism.version,
        "realism_preset": realism.name,
        "random_seed": seed,
        "params": params.model_dump(mode="json"),
        "realism": realism.model_dump(mode="json"),
    }
