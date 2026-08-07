"""Causal sequential execution policies for tape replay.

This module is deliberately narrower than a claim of globally optimal execution.
It supplies deterministic baselines that can be evaluated against an explicit
implementation-shortfall objective and improved without changing the strategy's
signal.  A policy sees only an immutable state as of the decision timestamp:
current L2, inventory, cash, elapsed time, and public volume observed so far.
Future prints and the terminal mark are never policy inputs.

The currently supported actions mirror the reference engine: wait, or submit an
immediately marketable child slice.  Passive placement is intentionally absent
until maker queue priority can be simulated by the reference engine.

Objective, evaluated at or after the deadline::

    J = w_is * implementation_shortfall
        + w_nf * benchmark_notional(unfilled)
        + w_as * adverse_markout(fills, deadline_mark)

For a buy, implementation shortfall is all-in fill cost minus benchmark cost; for
a sell it is benchmark proceeds minus net fill proceeds.  Adverse markout is the
loss versus the deadline mark, floored at zero.  Coefficients are fixed-point PPM.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Literal, Protocol

from polymarket_bt.backtest.realism import ExecutionOutcome, notional_scaled
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE

PPM = 1_000_000
EXECUTION_POLICY_VERSION = "causal-sequential-v1"

ExecutionSide = Literal["BUY", "SELL"]
ActionKind = Literal["WAIT", "TAKER_SLICE"]
Ladder = tuple[tuple[int, int], ...]


def _ppm(value: int, rate_ppm: int) -> int:
    """Multiply by a PPM rate, truncating symmetrically toward zero."""
    sign = -1 if value < 0 else 1
    return sign * (abs(value) * rate_ppm // PPM)


def _ceil_div(numerator: int, denominator: int) -> int:
    return -(-numerator // denominator)


@dataclass(frozen=True, slots=True)
class ObjectiveWeights:
    """Relative weights in the deadline execution objective."""

    implementation_shortfall_ppm: int = PPM
    non_fill_penalty_ppm: int = PPM
    adverse_selection_penalty_ppm: int = PPM

    def __post_init__(self) -> None:
        for name in (
            "implementation_shortfall_ppm",
            "non_fill_penalty_ppm",
            "adverse_selection_penalty_ppm",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True, slots=True)
class ExecutionPlan:
    """A finite-horizon parent order and the benchmark used to score it."""

    side: ExecutionSide
    target_shares_scaled: int
    start_utc_ns: int
    deadline_utc_ns: int
    benchmark_price_scaled: int
    limit_price_scaled: int | None = None
    objective: ObjectiveWeights = field(default_factory=ObjectiveWeights)

    def __post_init__(self) -> None:
        if self.side not in ("BUY", "SELL"):
            raise ValueError("side must be BUY or SELL")
        if self.target_shares_scaled <= 0:
            raise ValueError("target_shares_scaled must be positive")
        if self.deadline_utc_ns <= self.start_utc_ns:
            raise ValueError("deadline must be after start")
        if self.benchmark_price_scaled <= 0:
            raise ValueError("benchmark_price_scaled must be positive")
        if self.limit_price_scaled is not None and self.limit_price_scaled <= 0:
            raise ValueError("limit_price_scaled must be positive")


@dataclass(frozen=True, slots=True)
class ExecutionState:
    """Everything a causal policy may know at one decision timestamp.

    ``observed_market_volume_scaled`` is cumulative exogenous public volume from
    ``plan.start_utc_ns`` through ``now_utc_ns`` (inclusive).  It must never include
    prints after ``now_utc_ns``.  Immediate child orders have no outstanding state;
    the caller incorporates each FAK outcome before asking for another decision.
    """

    plan: ExecutionPlan
    now_utc_ns: int
    filled_shares_scaled: int
    inventory_shares_scaled: int
    available_cash_scaled: int
    bids: Ladder
    asks: Ladder
    observed_market_volume_scaled: int = 0
    minimum_order_size_scaled: int = 0
    last_action_utc_ns: int | None = None
    current_reference_price_scaled: int | None = None

    def __post_init__(self) -> None:
        if not 0 <= self.filled_shares_scaled <= self.plan.target_shares_scaled:
            raise ValueError("filled shares must be between zero and target")
        for name in (
            "inventory_shares_scaled",
            "available_cash_scaled",
            "observed_market_volume_scaled",
            "minimum_order_size_scaled",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.last_action_utc_ns is not None and self.last_action_utc_ns > self.now_utc_ns:
            raise ValueError("last action cannot be in the future")
        if self.current_reference_price_scaled is not None:
            if self.current_reference_price_scaled <= 0:
                raise ValueError("current_reference_price_scaled must be positive")
        self._validate_ladder(self.bids, descending=True, name="bids")
        self._validate_ladder(self.asks, descending=False, name="asks")

    @staticmethod
    def _validate_ladder(ladder: Ladder, *, descending: bool, name: str) -> None:
        previous: int | None = None
        for price, size in ladder:
            if price <= 0 or size <= 0:
                raise ValueError(f"{name} must contain positive price and size")
            if previous is not None:
                ordered = price < previous if descending else price > previous
                if not ordered:
                    raise ValueError(f"{name} must be strictly price ordered")
            previous = price

    @property
    def remaining_shares_scaled(self) -> int:
        return self.plan.target_shares_scaled - self.filled_shares_scaled

    @property
    def taker_ladder(self) -> Ladder:
        return self.asks if self.plan.side == "BUY" else self.bids

    @property
    def touch_price_scaled(self) -> int | None:
        ladder = self.taker_ladder
        return ladder[0][0] if ladder else None

    @property
    def progress_ppm(self) -> int:
        if self.now_utc_ns <= self.plan.start_utc_ns:
            return 0
        if self.now_utc_ns >= self.plan.deadline_utc_ns:
            return PPM
        elapsed = self.now_utc_ns - self.plan.start_utc_ns
        duration = self.plan.deadline_utc_ns - self.plan.start_utc_ns
        return elapsed * PPM // duration

    @property
    def adverse_move_scaled(self) -> int:
        """Observable price deterioration from the arrival benchmark."""
        price = self.current_reference_price_scaled or self.touch_price_scaled
        if price is None:
            return 0
        if self.plan.side == "BUY":
            return max(0, price - self.plan.benchmark_price_scaled)
        return max(0, self.plan.benchmark_price_scaled - price)


@dataclass(frozen=True, slots=True)
class ExecutionAction:
    """A policy decision; waits always have zero requested size."""

    kind: ActionKind
    requested_shares_scaled: int
    limit_price_scaled: int | None
    reason: str
    policy_name: str

    def __post_init__(self) -> None:
        if self.kind == "WAIT" and self.requested_shares_scaled != 0:
            raise ValueError("WAIT actions cannot request shares")
        if self.kind == "TAKER_SLICE" and self.requested_shares_scaled <= 0:
            raise ValueError("TAKER_SLICE actions require positive shares")


class SequentialExecutionPolicy(Protocol):
    """Pure deterministic policy interface used by tape replay."""

    name: str
    version: str

    def decide(self, state: ExecutionState) -> ExecutionAction: ...


@dataclass(frozen=True, slots=True)
class PolicyLimits:
    """Venue and risk bounds shared by the baseline policies."""

    minimum_action_interval_ns: int = 250_000_000
    max_child_shares_scaled: int | None = None
    max_l2_participation_ppm: int = 250_000
    # Planning reserve only. The execution engine remains responsible for an exact
    # notional-plus-fee cash cap at the actual arrival ladder.
    fee_reserve_ppm: int = 20_000

    def __post_init__(self) -> None:
        if self.minimum_action_interval_ns < 0:
            raise ValueError("minimum_action_interval_ns must be non-negative")
        if self.max_child_shares_scaled is not None and self.max_child_shares_scaled <= 0:
            raise ValueError("max_child_shares_scaled must be positive")
        if not 0 < self.max_l2_participation_ppm <= PPM:
            raise ValueError("max_l2_participation_ppm must be in (0, 1_000_000]")
        if self.fee_reserve_ppm < 0:
            raise ValueError("fee_reserve_ppm must be non-negative")


class _PolicyBase:
    name = "base"
    version = EXECUTION_POLICY_VERSION
    limits: PolicyLimits

    def _wait(self, reason: str) -> ExecutionAction:
        return ExecutionAction("WAIT", 0, None, reason, self.name)

    def _preflight(self, state: ExecutionState) -> ExecutionAction | None:
        if state.now_utc_ns < state.plan.start_utc_ns:
            return self._wait("before_start")
        if state.now_utc_ns > state.plan.deadline_utc_ns:
            return self._wait("deadline_passed")
        if state.remaining_shares_scaled <= 0:
            return self._wait("parent_complete")
        if not state.taker_ladder:
            return self._wait("empty_taker_ladder")
        if state.plan.side == "SELL" and state.inventory_shares_scaled <= 0:
            return self._wait("no_inventory")
        if (
            state.now_utc_ns < state.plan.deadline_utc_ns
            and state.last_action_utc_ns is not None
            and state.now_utc_ns - state.last_action_utc_ns < self.limits.minimum_action_interval_ns
        ):
            return self._wait("minimum_action_interval")
        return None

    def _child_action(
        self, state: ExecutionState, requested_shares_scaled: int, reason: str
    ) -> ExecutionAction:
        size = min(requested_shares_scaled, state.remaining_shares_scaled)
        if self.limits.max_child_shares_scaled is not None:
            size = min(size, self.limits.max_child_shares_scaled)
        if state.plan.side == "SELL":
            size = min(size, state.inventory_shares_scaled)

        executable_depth = self._executable_depth(state)
        size = min(size, _ppm(executable_depth, self.limits.max_l2_participation_ppm))
        if state.plan.side == "BUY":
            size = min(size, self._cash_affordable_shares(state))

        venue_minimum = state.minimum_order_size_scaled
        if size <= 0:
            return self._wait("no_executable_capacity")
        if size < venue_minimum:
            return self._wait("below_minimum_order_size")
        return ExecutionAction(
            kind="TAKER_SLICE",
            requested_shares_scaled=size,
            limit_price_scaled=state.plan.limit_price_scaled,
            reason=reason,
            policy_name=self.name,
        )

    @staticmethod
    def _executable_levels(state: ExecutionState) -> Ladder:
        limit = state.plan.limit_price_scaled
        if limit is None:
            return state.taker_ladder
        if state.plan.side == "BUY":
            return tuple(level for level in state.asks if level[0] <= limit)
        return tuple(level for level in state.bids if level[0] >= limit)

    def _executable_depth(self, state: ExecutionState) -> int:
        return sum(size for _, size in self._executable_levels(state))

    def _cash_affordable_shares(self, state: ExecutionState) -> int:
        levels = self._executable_levels(state)
        if not levels:
            return 0
        # Conservatively plan against the worst currently executable level or the
        # explicit limit, whichever can cost more. Arrival-time cash enforcement is
        # still required because the book may move during latency.
        worst_price = levels[-1][0]
        if state.plan.limit_price_scaled is not None:
            worst_price = max(worst_price, state.plan.limit_price_scaled)
        denominator = worst_price * (PPM + self.limits.fee_reserve_ppm)
        return state.available_cash_scaled * POLYMARKET_PRICE_SCALE * PPM // denominator


@dataclass(frozen=True, slots=True)
class TwapPolicy(_PolicyBase):
    """Evenly scheduled child slices with present-L2 and wallet bounds."""

    slices: int = 6
    limits: PolicyLimits = field(default_factory=PolicyLimits)
    name: str = field(default="twap", init=False)
    version: str = field(default=EXECUTION_POLICY_VERSION, init=False)

    def __post_init__(self) -> None:
        if self.slices <= 0:
            raise ValueError("slices must be positive")

    def decide(self, state: ExecutionState) -> ExecutionAction:
        blocked = self._preflight(state)
        if blocked is not None:
            return blocked
        duration = state.plan.deadline_utc_ns - state.plan.start_utc_ns
        elapsed = max(0, state.now_utc_ns - state.plan.start_utc_ns)
        slot = min(self.slices, elapsed * self.slices // duration + 1)
        scheduled = _ceil_div(state.plan.target_shares_scaled * slot, self.slices)
        due = scheduled - state.filled_shares_scaled
        if due <= 0:
            return self._wait("ahead_of_twap_schedule")
        reason = (
            "deadline_catch_up"
            if state.now_utc_ns == state.plan.deadline_utc_ns
            else "twap_schedule"
        )
        return self._child_action(state, due, reason)


@dataclass(frozen=True, slots=True)
class AdaptivePovPolicy(_PolicyBase):
    """Causal POV with a TWAP floor, deadline urgency, and adverse-price throttle.

    The POV target uses only cumulative public volume observed by ``now_utc_ns``.
    If public volume is ``V`` and participation is ``p``, the parent target is
    ``V*p/(1-p)`` so our hypothetical fills would be ``p`` of total volume.  A
    configurable fraction of the linear TWAP schedule prevents a silent tape from
    causing a guaranteed non-fill.  At the deadline the remaining target is due.

    Before the deadline, an observable adverse move scales the due child down.
    The throttle decays with time and vanishes at the deadline, expressing the
    tradeoff between implementation shortfall and the terminal non-fill penalty.
    """

    participation_ppm: int = 200_000
    twap_floor_ppm: int = 500_000
    adverse_move_tolerance_scaled: int | None = 20_000
    minimum_adverse_multiplier_ppm: int = 100_000
    limits: PolicyLimits = field(default_factory=PolicyLimits)
    name: str = field(default="adaptive_pov", init=False)
    version: str = field(default=EXECUTION_POLICY_VERSION, init=False)

    def __post_init__(self) -> None:
        if not 0 < self.participation_ppm < PPM:
            raise ValueError("participation_ppm must be in (0, 1_000_000)")
        if not 0 <= self.twap_floor_ppm <= PPM:
            raise ValueError("twap_floor_ppm must be in [0, 1_000_000]")
        if self.adverse_move_tolerance_scaled is not None:
            if self.adverse_move_tolerance_scaled <= 0:
                raise ValueError("adverse_move_tolerance_scaled must be positive")
        if not 0 <= self.minimum_adverse_multiplier_ppm <= PPM:
            raise ValueError("minimum_adverse_multiplier_ppm must be in [0, 1_000_000]")

    def decide(self, state: ExecutionState) -> ExecutionAction:
        blocked = self._preflight(state)
        if blocked is not None:
            return blocked

        if state.now_utc_ns == state.plan.deadline_utc_ns:
            return self._child_action(state, state.remaining_shares_scaled, "deadline_catch_up")

        flow_target = (
            state.observed_market_volume_scaled
            * self.participation_ppm
            // (PPM - self.participation_ppm)
        )
        linear_target = _ppm(state.plan.target_shares_scaled, state.progress_ppm)
        schedule_floor = _ppm(linear_target, self.twap_floor_ppm)
        desired_cumulative = min(state.plan.target_shares_scaled, max(flow_target, schedule_floor))
        due = desired_cumulative - state.filled_shares_scaled
        if due <= 0:
            return self._wait("participation_target_satisfied")

        due = _ppm(due, self._adverse_multiplier(state))
        if due <= 0:
            return self._wait("adverse_price_wait")
        return self._child_action(state, due, "adaptive_participation")

    def _adverse_multiplier(self, state: ExecutionState) -> int:
        tolerance = self.adverse_move_tolerance_scaled
        if tolerance is None or state.adverse_move_scaled <= 0:
            return PPM
        price_pressure_ppm = min(PPM, state.adverse_move_scaled * PPM // tolerance)
        time_relief_ppm = state.progress_ppm
        reduction = _ppm(price_pressure_ppm, PPM - time_relief_ppm)
        return max(self.minimum_adverse_multiplier_ppm, PPM - reduction)


@dataclass(frozen=True, slots=True)
class ExecutionFill:
    """One child-fill slice used by the objective evaluator."""

    timestamp_utc_ns: int
    shares_scaled: int
    price_scaled: int
    fee_scaled: int

    def __post_init__(self) -> None:
        if self.shares_scaled <= 0 or self.price_scaled <= 0:
            raise ValueError("fill shares and price must be positive")
        if self.fee_scaled < 0:
            raise ValueError("fill fee cannot be negative")


@dataclass(frozen=True, slots=True)
class ExecutionObjectiveResult:
    """Weighted objective terms in collateral fixed-point units."""

    filled_shares_scaled: int
    unfilled_shares_scaled: int
    fill_rate_ppm: int
    raw_implementation_shortfall_scaled: int
    implementation_shortfall_scaled: int
    non_fill_penalty_scaled: int
    adverse_selection_penalty_scaled: int
    total_objective_scaled: int
    deadline_mark_price_scaled: int | None


def evaluate_execution_objective(
    plan: ExecutionPlan,
    fills: Sequence[ExecutionFill],
    *,
    evaluation_utc_ns: int,
    deadline_mark_price_scaled: int | None,
) -> ExecutionObjectiveResult:
    """Score completed replay using information available at the deadline.

    This function is intentionally separate from ``SequentialExecutionPolicy``.
    A terminal mark is legitimate for evaluation but would be lookahead if a
    policy could inspect it while choosing child orders.
    """
    if evaluation_utc_ns < plan.deadline_utc_ns:
        raise ValueError("execution objective cannot be evaluated before the deadline")
    if plan.objective.adverse_selection_penalty_ppm and (
        deadline_mark_price_scaled is None or deadline_mark_price_scaled <= 0
    ):
        raise ValueError("a positive deadline mark is required for adverse-selection scoring")
    if any(fill.timestamp_utc_ns > plan.deadline_utc_ns for fill in fills):
        raise ValueError("fills after the parent deadline are invalid")

    filled = sum(fill.shares_scaled for fill in fills)
    if filled > plan.target_shares_scaled:
        raise ValueError("fills exceed the parent target")
    unfilled = plan.target_shares_scaled - filled
    benchmark_filled = notional_scaled(filled, plan.benchmark_price_scaled)
    actual_notional = sum(notional_scaled(fill.shares_scaled, fill.price_scaled) for fill in fills)
    fees = sum(fill.fee_scaled for fill in fills)
    if plan.side == "BUY":
        raw_shortfall = actual_notional + fees - benchmark_filled
    else:
        raw_shortfall = benchmark_filled - (actual_notional - fees)

    non_fill_base = notional_scaled(unfilled, plan.benchmark_price_scaled)
    adverse_base = 0
    if deadline_mark_price_scaled is not None:
        for fill in fills:
            if plan.side == "BUY":
                adverse_price = max(0, fill.price_scaled - deadline_mark_price_scaled)
            else:
                adverse_price = max(0, deadline_mark_price_scaled - fill.price_scaled)
            adverse_base += notional_scaled(fill.shares_scaled, adverse_price)

    weighted_shortfall = _ppm(raw_shortfall, plan.objective.implementation_shortfall_ppm)
    non_fill_penalty = _ppm(non_fill_base, plan.objective.non_fill_penalty_ppm)
    adverse_penalty = _ppm(adverse_base, plan.objective.adverse_selection_penalty_ppm)
    return ExecutionObjectiveResult(
        filled_shares_scaled=filled,
        unfilled_shares_scaled=unfilled,
        fill_rate_ppm=filled * PPM // plan.target_shares_scaled,
        raw_implementation_shortfall_scaled=raw_shortfall,
        implementation_shortfall_scaled=weighted_shortfall,
        non_fill_penalty_scaled=non_fill_penalty,
        adverse_selection_penalty_scaled=adverse_penalty,
        total_objective_scaled=weighted_shortfall + non_fill_penalty + adverse_penalty,
        deadline_mark_price_scaled=deadline_mark_price_scaled,
    )


class CausalVolumeCursor:
    """Incremental public-volume view that cannot return post-decision prints."""

    def __init__(
        self,
        timestamps_utc_ns: Sequence[int],
        token_indices: Sequence[int],
        sizes_scaled: Sequence[int],
        *,
        token_index: int,
        start_utc_ns: int,
    ) -> None:
        if not (len(timestamps_utc_ns) == len(token_indices) == len(sizes_scaled)):
            raise ValueError("trade arrays must have equal lengths")
        if any(left > right for left, right in pairwise(timestamps_utc_ns)):
            raise ValueError("trade timestamps must be sorted")
        if any(size < 0 for size in sizes_scaled):
            raise ValueError("trade sizes must be non-negative")
        self._timestamps = timestamps_utc_ns
        self._tokens = token_indices
        self._sizes = sizes_scaled
        self._token_index = token_index
        self._position = bisect_left(timestamps_utc_ns, start_utc_ns)
        self._last_utc_ns = start_utc_ns - 1
        self._volume_scaled = 0

    def advance(self, now_utc_ns: int) -> int:
        """Return cumulative target-token volume through ``now_utc_ns`` inclusive."""
        if now_utc_ns < self._last_utc_ns:
            raise ValueError("causal volume cursor cannot move backwards")
        end = bisect_right(self._timestamps, now_utc_ns, lo=self._position)
        self._volume_scaled += sum(
            self._sizes[index]
            for index in range(self._position, end)
            if self._tokens[index] == self._token_index
        )
        self._position = end
        self._last_utc_ns = now_utc_ns
        return self._volume_scaled


class SequentialExecutionSession:
    """Bounded orchestration for one parent order.

    The session owns causal volume, wallet/inventory state, fill history, and the
    outstanding immediate child.  It does not sample latency or match depth; the
    caller sends each ``TAKER_SLICE`` through the reference taker executor and
    records that outcome here.  This boundary keeps the policy deterministic while
    making target, wallet, inventory, and deadline checks difficult to omit.
    """

    def __init__(
        self,
        plan: ExecutionPlan,
        policy: SequentialExecutionPolicy,
        volume_cursor: CausalVolumeCursor,
        *,
        initial_cash_scaled: int,
        initial_inventory_shares_scaled: int,
    ) -> None:
        if initial_cash_scaled < 0:
            raise ValueError("initial_cash_scaled must be non-negative")
        if initial_inventory_shares_scaled < 0:
            raise ValueError("initial_inventory_shares_scaled must be non-negative")
        if plan.side == "SELL" and initial_inventory_shares_scaled < plan.target_shares_scaled:
            raise ValueError("sell parent target exceeds initial inventory")
        self.plan = plan
        self.policy = policy
        self.volume_cursor = volume_cursor
        self._cash_scaled = initial_cash_scaled
        self._inventory_shares_scaled = initial_inventory_shares_scaled
        self._filled_shares_scaled = 0
        self._fills: list[ExecutionFill] = []
        self._last_action_utc_ns: int | None = None
        self._last_decision_utc_ns: int | None = None
        self._next_decision_not_before_utc_ns = plan.start_utc_ns
        self._pending_action: ExecutionAction | None = None
        self._pending_decision_utc_ns: int | None = None

    @property
    def available_cash_scaled(self) -> int:
        return self._cash_scaled

    @property
    def inventory_shares_scaled(self) -> int:
        return self._inventory_shares_scaled

    @property
    def filled_shares_scaled(self) -> int:
        return self._filled_shares_scaled

    @property
    def remaining_shares_scaled(self) -> int:
        return self.plan.target_shares_scaled - self._filled_shares_scaled

    @property
    def fills(self) -> tuple[ExecutionFill, ...]:
        return tuple(self._fills)

    @property
    def pending_action(self) -> ExecutionAction | None:
        return self._pending_action

    def decide(
        self,
        *,
        now_utc_ns: int,
        bids: Ladder,
        asks: Ladder,
        minimum_order_size_scaled: int = 0,
        current_reference_price_scaled: int | None = None,
    ) -> ExecutionAction:
        """Return the next child decision from a present-time book state."""
        if self._last_decision_utc_ns is not None and now_utc_ns < self._last_decision_utc_ns:
            raise ValueError("execution session cannot decide backwards in time")
        observed_volume = self.volume_cursor.advance(now_utc_ns)
        self._last_decision_utc_ns = now_utc_ns
        if self._pending_action is not None:
            return ExecutionAction("WAIT", 0, None, "child_outstanding", self.policy.name)
        if now_utc_ns < self._next_decision_not_before_utc_ns:
            return ExecutionAction("WAIT", 0, None, "previous_child_not_arrived", self.policy.name)
        state = ExecutionState(
            plan=self.plan,
            now_utc_ns=now_utc_ns,
            filled_shares_scaled=self._filled_shares_scaled,
            inventory_shares_scaled=self._inventory_shares_scaled,
            available_cash_scaled=self._cash_scaled,
            bids=bids,
            asks=asks,
            observed_market_volume_scaled=observed_volume,
            minimum_order_size_scaled=minimum_order_size_scaled,
            last_action_utc_ns=self._last_action_utc_ns,
            current_reference_price_scaled=current_reference_price_scaled,
        )
        action = self.policy.decide(state)
        if action.kind == "TAKER_SLICE":
            self._pending_action = action
            self._pending_decision_utc_ns = now_utc_ns
            self._last_action_utc_ns = now_utc_ns
        return action

    def record_outcome(self, outcome: ExecutionOutcome) -> None:
        """Atomically apply a reference ``TakerExecutor`` outcome."""
        action, decision_ns = self._require_pending()
        if outcome.requested_shares_scaled != action.requested_shares_scaled:
            raise ValueError("outcome requested size does not match pending child")
        if outcome.decision_utc_ns != decision_ns:
            raise ValueError("outcome decision timestamp does not match pending child")
        if outcome.arrival_utc_ns < decision_ns:
            raise ValueError("outcome arrival cannot precede its decision")
        if outcome.arrival_utc_ns > self.plan.deadline_utc_ns and outcome.filled:
            raise ValueError("a child arriving after the parent deadline cannot fill")

        slice_shares = sum(item.size_scaled for item in outcome.slices)
        slice_notional = sum(item.notional_scaled for item in outcome.slices)
        slice_fees = sum(item.fee_scaled for item in outcome.slices)
        if slice_shares != outcome.filled_shares_scaled:
            raise ValueError("outcome slices do not equal aggregate filled shares")
        if slice_notional != outcome.notional_scaled:
            raise ValueError("outcome slices do not equal aggregate notional")
        if slice_fees != outcome.fees_scaled:
            raise ValueError("outcome slices do not equal aggregate fees")
        fills = tuple(
            ExecutionFill(
                timestamp_utc_ns=outcome.arrival_utc_ns,
                shares_scaled=item.size_scaled,
                price_scaled=item.price_scaled,
                fee_scaled=item.fee_scaled,
            )
            for item in outcome.slices
        )
        self._apply_fills(
            fills,
            notional_total_scaled=outcome.notional_scaled,
            fee_total_scaled=outcome.fees_scaled,
        )
        self._complete_pending(outcome.arrival_utc_ns)

    def record_fill(self, fill: ExecutionFill) -> None:
        """Apply one aggregate FAK fill for an executor without slice outcomes."""
        _action, decision_ns = self._require_pending()
        if fill.timestamp_utc_ns < decision_ns:
            raise ValueError("fill cannot precede its decision")
        if fill.timestamp_utc_ns > self.plan.deadline_utc_ns:
            raise ValueError("a child arriving after the parent deadline cannot fill")
        self._apply_fills(
            (fill,),
            notional_total_scaled=notional_scaled(fill.shares_scaled, fill.price_scaled),
            fee_total_scaled=fill.fee_scaled,
        )
        self._complete_pending(fill.timestamp_utc_ns)

    def record_no_fill(self, *, arrival_utc_ns: int) -> None:
        """Complete a rejected or zero-fill child without changing its wallet."""
        _action, decision_ns = self._require_pending()
        if arrival_utc_ns < decision_ns:
            raise ValueError("arrival cannot precede its decision")
        self._complete_pending(arrival_utc_ns)

    def evaluate(
        self,
        *,
        evaluation_utc_ns: int,
        deadline_mark_price_scaled: int | None,
    ) -> ExecutionObjectiveResult:
        """Score the session after the parent deadline."""
        if self._pending_action is not None:
            raise ValueError("cannot evaluate while a child outcome is outstanding")
        return evaluate_execution_objective(
            self.plan,
            self._fills,
            evaluation_utc_ns=evaluation_utc_ns,
            deadline_mark_price_scaled=deadline_mark_price_scaled,
        )

    def _require_pending(self) -> tuple[ExecutionAction, int]:
        if self._pending_action is None or self._pending_decision_utc_ns is None:
            raise ValueError("there is no pending child action")
        return self._pending_action, self._pending_decision_utc_ns

    def _apply_fills(
        self,
        fills: Sequence[ExecutionFill],
        *,
        notional_total_scaled: int,
        fee_total_scaled: int,
    ) -> None:
        action, _decision_ns = self._require_pending()
        added_shares = sum(fill.shares_scaled for fill in fills)
        if added_shares > action.requested_shares_scaled:
            raise ValueError("fills exceed the pending child request")
        if added_shares > self.remaining_shares_scaled:
            raise ValueError("fills exceed the parent remainder")
        if self.plan.side == "BUY":
            required_cash = notional_total_scaled + fee_total_scaled
            if required_cash > self._cash_scaled:
                raise ValueError("fill notional plus fees exceeds available cash")
            next_cash = self._cash_scaled - required_cash
            next_inventory = self._inventory_shares_scaled + added_shares
        else:
            if added_shares > self._inventory_shares_scaled:
                raise ValueError("sell fills exceed available inventory")
            next_cash = self._cash_scaled + notional_total_scaled - fee_total_scaled
            next_inventory = self._inventory_shares_scaled - added_shares
            if next_cash < 0:
                raise ValueError("sell fees would make available cash negative")
        self._cash_scaled = next_cash
        self._inventory_shares_scaled = next_inventory
        self._filled_shares_scaled += added_shares
        self._fills.extend(fills)

    def _complete_pending(self, arrival_utc_ns: int) -> None:
        self._next_decision_not_before_utc_ns = max(
            self._next_decision_not_before_utc_ns, arrival_utc_ns
        )
        self._pending_action = None
        self._pending_decision_utc_ns = None
