from __future__ import annotations

import pytest

from polymarket_bt.backtest.optimal_execution import (
    PPM,
    AdaptivePovPolicy,
    CausalVolumeCursor,
    ExecutionFill,
    ExecutionPlan,
    ExecutionState,
    ObjectiveWeights,
    PolicyLimits,
    SequentialExecutionSession,
    TwapPolicy,
    evaluate_execution_objective,
)

START = 1_000_000_000
DEADLINE = START + 10_000_000_000
SHARE = 1_000_000


def plan(**overrides: object) -> ExecutionPlan:
    values: dict[str, object] = {
        "side": "BUY",
        "target_shares_scaled": 100 * SHARE,
        "start_utc_ns": START,
        "deadline_utc_ns": DEADLINE,
        "benchmark_price_scaled": 800_000,
        "limit_price_scaled": 850_000,
    }
    values.update(overrides)
    return ExecutionPlan(**values)  # type: ignore[arg-type]


def state(at_seconds: float, **overrides: object) -> ExecutionState:
    values: dict[str, object] = {
        "plan": plan(),
        "now_utc_ns": START + int(at_seconds * 1_000_000_000),
        "filled_shares_scaled": 0,
        "inventory_shares_scaled": 0,
        "available_cash_scaled": 1_000_000_000,
        "bids": ((790_000, 1_000 * SHARE),),
        "asks": ((800_000, 1_000 * SHARE), (810_000, 1_000 * SHARE)),
    }
    values.update(overrides)
    return ExecutionState(**values)  # type: ignore[arg-type]


def unlimited() -> PolicyLimits:
    return PolicyLimits(
        minimum_action_interval_ns=0,
        max_l2_participation_ppm=PPM,
        fee_reserve_ppm=0,
    )


def test_twap_emits_deterministic_scheduled_children() -> None:
    policy = TwapPolicy(slices=5, limits=unlimited())
    first = policy.decide(state(0))
    assert first.kind == "TAKER_SLICE"
    assert first.requested_shares_scaled == 20 * SHARE

    on_schedule = state(4, filled_shares_scaled=40 * SHARE)
    assert policy.decide(on_schedule).requested_shares_scaled == 20 * SHARE
    assert policy.decide(on_schedule) == policy.decide(on_schedule)


def test_twap_waits_when_a_parent_is_ahead_of_schedule() -> None:
    action = TwapPolicy(slices=5, limits=unlimited()).decide(
        state(2, filled_shares_scaled=50 * SHARE)
    )
    assert action.kind == "WAIT"
    assert action.reason == "ahead_of_twap_schedule"


def test_policy_caps_child_by_present_depth_cash_and_inventory() -> None:
    buy_policy = TwapPolicy(slices=1, limits=unlimited())
    cash_limited = state(
        0,
        available_cash_scaled=8_000_000,
        asks=((800_000, 100 * SHARE),),
    )
    # The planning cap reserves enough for the explicit .85 limit, not merely the
    # current .80 touch: $8 / .85 = 9.411764 shares.
    assert buy_policy.decide(cash_limited).requested_shares_scaled == 9_411_764

    sell_plan = plan(side="SELL", limit_price_scaled=750_000)
    sell = state(
        0,
        plan=sell_plan,
        inventory_shares_scaled=7 * SHARE,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    assert buy_policy.decide(sell).requested_shares_scaled == 7 * SHARE


def test_child_obeys_limit_and_present_l2_participation() -> None:
    policy = TwapPolicy(
        slices=1,
        limits=PolicyLimits(
            minimum_action_interval_ns=0,
            max_l2_participation_ppm=250_000,
            fee_reserve_ppm=0,
        ),
    )
    current = state(
        0,
        asks=((800_000, 20 * SHARE), (860_000, 500 * SHARE)),
    )
    action = policy.decide(current)
    assert action.requested_shares_scaled == 5 * SHARE
    assert action.limit_price_scaled == 850_000


def test_minimum_interval_prevents_one_retry_per_book_event() -> None:
    policy = TwapPolicy(slices=1)
    action = policy.decide(state(0.001, last_action_utc_ns=START))
    assert action.kind == "WAIT"
    assert action.reason == "minimum_action_interval"


def test_adaptive_pov_uses_only_observed_volume_and_own_share_formula() -> None:
    policy = AdaptivePovPolicy(
        participation_ppm=200_000,
        twap_floor_ppm=0,
        adverse_move_tolerance_scaled=None,
        limits=unlimited(),
    )
    current = state(2, observed_market_volume_scaled=80 * SHARE)
    # X / (80 + X) = 20%, hence X=20 rather than 16.
    action = policy.decide(current)
    assert action.kind == "TAKER_SLICE"
    assert action.requested_shares_scaled == 20 * SHARE


def test_adaptive_pov_waits_without_past_flow_then_forces_deadline_catchup() -> None:
    policy = AdaptivePovPolicy(
        participation_ppm=200_000,
        twap_floor_ppm=0,
        limits=unlimited(),
    )
    assert policy.decide(state(5, observed_market_volume_scaled=0)).kind == "WAIT"
    deadline = state(10, observed_market_volume_scaled=0)
    action = policy.decide(deadline)
    assert action.kind == "TAKER_SLICE"
    assert action.requested_shares_scaled == 100 * SHARE
    assert action.reason == "deadline_catch_up"


def test_adverse_price_throttle_relaxes_as_deadline_approaches() -> None:
    policy = AdaptivePovPolicy(
        participation_ppm=500_000,
        twap_floor_ppm=0,
        adverse_move_tolerance_scaled=20_000,
        minimum_adverse_multiplier_ppm=100_000,
        limits=unlimited(),
    )
    early = state(
        1,
        observed_market_volume_scaled=100 * SHARE,
        current_reference_price_scaled=820_000,
    )
    late = state(
        9,
        observed_market_volume_scaled=100 * SHARE,
        current_reference_price_scaled=820_000,
    )
    assert policy.decide(early).requested_shares_scaled == 10 * SHARE
    assert policy.decide(late).requested_shares_scaled == 90 * SHARE


def test_volume_cursor_never_reveals_future_or_other_token_prints() -> None:
    cursor = CausalVolumeCursor(
        [START - 1, START + 1, START + 2, START + 100],
        [0, 0, 1, 0],
        [99 * SHARE, 2 * SHARE, 50 * SHARE, 7 * SHARE],
        token_index=0,
        start_utc_ns=START,
    )
    assert cursor.advance(START) == 0
    assert cursor.advance(START + 2) == 2 * SHARE
    assert cursor.advance(START + 99) == 2 * SHARE
    assert cursor.advance(START + 100) == 9 * SHARE
    with pytest.raises(ValueError, match="move backwards"):
        cursor.advance(START + 50)


def test_objective_scores_buy_shortfall_nonfill_and_adverse_markout() -> None:
    objective_plan = plan(
        target_shares_scaled=10 * SHARE,
        objective=ObjectiveWeights(
            implementation_shortfall_ppm=PPM,
            non_fill_penalty_ppm=500_000,
            adverse_selection_penalty_ppm=PPM,
        ),
    )
    fills = [
        ExecutionFill(
            timestamp_utc_ns=START + 1,
            shares_scaled=8 * SHARE,
            price_scaled=810_000,
            fee_scaled=40_000,
        )
    ]
    result = evaluate_execution_objective(
        objective_plan,
        fills,
        evaluation_utc_ns=DEADLINE,
        deadline_mark_price_scaled=790_000,
    )
    # IS: 8*(.81-.80) + .04 fee = .12. Nonfill: 50% * 2*.80 = .80.
    # Adverse markout: 8*(.81-.79) = .16. Total = 1.08.
    assert result.raw_implementation_shortfall_scaled == 120_000
    assert result.non_fill_penalty_scaled == 800_000
    assert result.adverse_selection_penalty_scaled == 160_000
    assert result.total_objective_scaled == 1_080_000
    assert result.fill_rate_ppm == 800_000


def test_objective_allows_favourable_shortfall_and_scores_sell_correctly() -> None:
    sell_plan = plan(
        side="SELL",
        target_shares_scaled=10 * SHARE,
        benchmark_price_scaled=800_000,
        objective=ObjectiveWeights(adverse_selection_penalty_ppm=0),
    )
    result = evaluate_execution_objective(
        sell_plan,
        [ExecutionFill(DEADLINE, 10 * SHARE, 810_000, 20_000)],
        evaluation_utc_ns=DEADLINE,
        deadline_mark_price_scaled=None,
    )
    assert result.raw_implementation_shortfall_scaled == -80_000
    assert result.total_objective_scaled == -80_000


def test_objective_rejects_lookahead_evaluation_and_post_deadline_fills() -> None:
    with pytest.raises(ValueError, match="before the deadline"):
        evaluate_execution_objective(
            plan(), [], evaluation_utc_ns=DEADLINE - 1, deadline_mark_price_scaled=800_000
        )
    with pytest.raises(ValueError, match="after the parent deadline"):
        evaluate_execution_objective(
            plan(),
            [ExecutionFill(DEADLINE + 1, SHARE, 800_000, 0)],
            evaluation_utc_ns=DEADLINE + 1,
            deadline_mark_price_scaled=800_000,
        )


def test_state_rejects_malformed_l2() -> None:
    with pytest.raises(ValueError, match="strictly price ordered"):
        state(1, asks=((810_000, SHARE), (800_000, SHARE)))


def volume_cursor() -> CausalVolumeCursor:
    return CausalVolumeCursor(
        [START + 1_000_000_000, START + 3_000_000_000],
        [0, 0],
        [8 * SHARE, 80 * SHARE],
        token_index=0,
        start_utc_ns=START,
    )


def session(
    *,
    session_plan: ExecutionPlan | None = None,
    policy: TwapPolicy | AdaptivePovPolicy | None = None,
    cash: int = 1_000_000_000,
    inventory: int = 0,
) -> SequentialExecutionSession:
    return SequentialExecutionSession(
        session_plan or plan(),
        policy or TwapPolicy(slices=1, limits=unlimited()),
        volume_cursor(),
        initial_cash_scaled=cash,
        initial_inventory_shares_scaled=inventory,
    )


def test_session_owns_causal_volume_and_blocks_while_child_is_outstanding() -> None:
    policy = AdaptivePovPolicy(
        participation_ppm=200_000,
        twap_floor_ppm=0,
        adverse_move_tolerance_scaled=None,
        limits=unlimited(),
    )
    execution = session(policy=policy)
    no_flow = execution.decide(
        now_utc_ns=START,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    assert no_flow.kind == "WAIT"

    with_flow = execution.decide(
        now_utc_ns=START + 1_000_000_000,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    assert with_flow.requested_shares_scaled == 2 * SHARE
    blocked = execution.decide(
        now_utc_ns=START + 2_000_000_000,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    assert blocked.kind == "WAIT"
    assert blocked.reason == "child_outstanding"


def test_session_records_outcome_and_updates_wallet_inventory_atomically() -> None:
    from polymarket_bt.backtest.realism import ExecutionOutcome, FillSlice

    execution = session(cash=10_000_000)
    action = execution.decide(
        now_utc_ns=START,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    outcome = ExecutionOutcome(
        requested_shares_scaled=action.requested_shares_scaled,
        filled_shares_scaled=5 * SHARE,
        notional_scaled=4_000_000,
        fees_scaled=20_000,
        slices=[FillSlice(800_000, 5 * SHARE, 4_000_000, 20_000, 1)],
        decision_utc_ns=START,
        arrival_utc_ns=START + 100_000_000,
    )
    execution.record_outcome(outcome)
    assert execution.filled_shares_scaled == 5 * SHARE
    assert execution.inventory_shares_scaled == 5 * SHARE
    assert execution.available_cash_scaled == 5_980_000
    assert execution.pending_action is None
    assert execution.fills == (ExecutionFill(START + 100_000_000, 5 * SHARE, 800_000, 20_000),)


def test_session_rejects_overspend_without_partial_wallet_mutation() -> None:
    execution = session(cash=1_000_000)
    action = execution.decide(
        now_utc_ns=START,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    # Planning capped the child to available cash. A worse arrival plus fees still
    # cannot overdraw the exact wallet ledger.
    fill = ExecutionFill(START + 1, action.requested_shares_scaled, 850_000, 500_000)
    with pytest.raises(ValueError, match="exceeds available cash"):
        execution.record_fill(fill)
    assert execution.available_cash_scaled == 1_000_000
    assert execution.inventory_shares_scaled == 0
    assert execution.filled_shares_scaled == 0
    assert execution.pending_action == action


def test_session_sell_fill_releases_cash_and_reduces_inventory() -> None:
    sell_plan = plan(
        side="SELL",
        target_shares_scaled=10 * SHARE,
        limit_price_scaled=750_000,
        objective=ObjectiveWeights(adverse_selection_penalty_ppm=0),
    )
    execution = session(session_plan=sell_plan, cash=1_000_000, inventory=10 * SHARE)
    action = execution.decide(
        now_utc_ns=START,
        bids=((800_000, 100 * SHARE),),
        asks=((810_000, 100 * SHARE),),
    )
    execution.record_fill(ExecutionFill(START + 1, action.requested_shares_scaled, 800_000, 50_000))
    assert execution.inventory_shares_scaled == 0
    assert execution.available_cash_scaled == 8_950_000
    result = execution.evaluate(evaluation_utc_ns=DEADLINE, deadline_mark_price_scaled=None)
    assert result.fill_rate_ppm == PPM


def test_session_refuses_post_deadline_fill_and_requires_resolved_child() -> None:
    execution = session()
    execution.decide(
        now_utc_ns=DEADLINE,
        bids=((790_000, 100 * SHARE),),
        asks=((800_000, 100 * SHARE),),
    )
    with pytest.raises(ValueError, match="after the parent deadline"):
        execution.record_fill(ExecutionFill(DEADLINE + 1, SHARE, 800_000, 0))
    with pytest.raises(ValueError, match="outstanding"):
        execution.evaluate(evaluation_utc_ns=DEADLINE, deadline_mark_price_scaled=800_000)
    execution.record_no_fill(arrival_utc_ns=DEADLINE + 1)
    result = execution.evaluate(
        evaluation_utc_ns=DEADLINE + 1,
        deadline_mark_price_scaled=800_000,
    )
    assert result.unfilled_shares_scaled == 100 * SHARE
