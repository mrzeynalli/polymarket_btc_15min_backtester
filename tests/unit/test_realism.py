from __future__ import annotations

from polymarket_bt.backtest.realism import (
    DEFAULT_FEE,
    PPM,
    ExecutionRealism,
    TakerExecutor,
    preset,
)
from polymarket_bt.config import LatencyConfig

LADDER_ASKS = [(800_000, 10_000_000), (810_000, 20_000_000), (820_000, 100_000_000)]


def executor(**overrides: object) -> TakerExecutor:
    realism = ExecutionRealism(
        name="test",
        latency=LatencyConfig(model="constant", constant_ms=0),
        fee=DEFAULT_FEE,
        **overrides,  # type: ignore[arg-type]
    )
    return TakerExecutor(realism, seed=7)


def execute(taker: TakerExecutor, shares: int, **overrides: object) -> object:
    defaults: dict[str, object] = {
        "side": "BUY",
        "ladder": LADDER_ASKS,
        "requested_shares_scaled": shares,
        "limit_price_scaled": None,
        "reference_price_scaled": 800_000,
        "realised_volume_scaled": None,
        "decision_utc_ns": 1_000,
        "arrival_utc_ns": 1_050,
        "book_age_ns": 0,
        "minimum_order_size_scaled": 0,
        "tick_size_scaled": 10_000,
    }
    defaults.update(overrides)
    return taker.execute(**defaults)  # type: ignore[arg-type]


def test_buy_walks_ascending_asks_and_never_fills_below_touch() -> None:
    outcome = execute(executor(), 35_000_000)
    assert [slice_.price_scaled for slice_ in outcome.slices] == [800_000, 810_000, 820_000]
    assert [slice_.size_scaled for slice_ in outcome.slices] == [
        10_000_000,
        20_000_000,
        5_000_000,
    ]
    assert outcome.filled_shares_scaled == 35_000_000
    assert min(slice_.price_scaled for slice_ in outcome.slices) >= LADDER_ASKS[0][0]
    assert outcome.average_price_scaled is not None
    assert outcome.average_price_scaled >= 800_000


def test_unbounded_execution_can_walk_more_than_ten_stored_levels() -> None:
    ladder = [(700_000 + rank * 1_000, 1_000_000) for rank in range(12)]
    outcome = execute(executor(max_levels_swept=None), 12_000_000, ladder=ladder)
    assert outcome.filled_shares_scaled == 12_000_000
    assert len(outcome.slices) == 12


def test_finite_level_cap_is_reported_as_a_scenario_constraint() -> None:
    ladder = [(700_000 + rank * 1_000, 1_000_000) for rank in range(12)]
    outcome = execute(executor(max_levels_swept=10), 12_000_000, ladder=ladder)
    assert outcome.filled_shares_scaled == 10_000_000
    assert outcome.binding_constraint == "max_levels_swept"


def test_depth_haircut_reduces_reachable_size() -> None:
    outcome = execute(executor(displayed_depth_haircut_ppm=500_000), 10_000_000)
    # Only half of each displayed level is assumed reachable, so the order takes
    # 5 of the touch's 10 and finds the rest one level up.
    assert outcome.slices[0].size_scaled == 5_000_000
    assert outcome.filled_shares_scaled == 10_000_000


def test_a_fully_filled_order_reports_no_binding_constraint() -> None:
    """A haircut that still left enough depth did not bind anything.

    Attributing it anyway makes the fill-quality report claim the book refused
    size it was never asked for.
    """
    outcome = execute(executor(displayed_depth_haircut_ppm=500_000), 10_000_000)
    assert outcome.filled_shares_scaled == outcome.requested_shares_scaled
    assert outcome.binding_constraint is None


def test_depth_haircut_is_named_when_it_actually_cuts_the_fill() -> None:
    outcome = execute(
        executor(displayed_depth_haircut_ppm=500_000),
        10_000_000,
        ladder=[(800_000, 10_000_000)],
    )
    assert outcome.filled_shares_scaled == 5_000_000
    assert outcome.binding_constraint == "displayed_depth_haircut"


def test_level_participation_cap_limits_single_level_take() -> None:
    outcome = execute(executor(max_level_participation_ppm=100_000), 10_000_000)
    assert outcome.slices[0].size_scaled == 1_000_000


def test_causal_participation_cap_counts_the_child_order_itself_as_volume() -> None:
    """A child fill prints too, so the bound is V*p/(1-p), not V*p."""
    taker = executor(volume_participation_ppm=500_000, volume_participation_window_ms=1_000)
    outcome = execute(taker, 100_000_000, realised_volume_scaled=4_000_000)
    assert outcome.filled_shares_scaled == 4_000_000
    assert outcome.binding_constraint == "volume_participation"


def test_no_prior_volume_bounds_a_child_but_does_not_veto_it() -> None:
    """No observed flow is not evidence that displayed resting depth is unfillable."""
    taker = executor(volume_participation_ppm=500_000)
    outcome = execute(
        taker, 100_000_000, realised_volume_scaled=0, minimum_order_size_scaled=5_000_000
    )
    assert outcome.filled_shares_scaled == 5_000_000
    assert outcome.binding_constraint == "no_recent_public_volume"


def test_limit_price_stops_the_sweep() -> None:
    outcome = execute(executor(), 100_000_000, limit_price_scaled=810_000)
    assert outcome.filled_shares_scaled == 30_000_000
    assert outcome.binding_constraint == "limit_price"


def test_ticks_through_touch_bounds_the_sweep() -> None:
    outcome = execute(executor(max_ticks_through_touch=1), 100_000_000)
    assert max(slice_.price_scaled for slice_ in outcome.slices) <= 810_000


def test_ticks_through_touch_uses_the_market_own_tick_size() -> None:
    """These markets quote on 0.001 and on 0.01; a fixed cent made them incomparable."""
    fine = execute(executor(max_ticks_through_touch=1), 100_000_000, tick_size_scaled=1_000)
    assert max(slice_.price_scaled for slice_ in fine.slices) == 800_000
    coarse = execute(executor(max_ticks_through_touch=1), 100_000_000, tick_size_scaled=10_000)
    assert max(slice_.price_scaled for slice_ in coarse.slices) == 810_000


def test_the_absolute_bound_applies_when_it_is_tighter_than_the_tick_count() -> None:
    """Ten ticks is a cent on one market and ten cents on another; cap both."""
    outcome = execute(
        executor(max_ticks_through_touch=10, max_price_through_touch_scaled=10_000),
        100_000_000,
        tick_size_scaled=10_000,
    )
    assert max(slice_.price_scaled for slice_ in outcome.slices) == 810_000
    # The model refused to chase further; the strategy's own ceiling was not reached.
    assert outcome.binding_constraint == "max_price_through_touch"


def test_stale_book_is_refused() -> None:
    outcome = execute(executor(stale_book_max_age_ms=1), 1_000_000, book_age_ns=5_000_000)
    assert not outcome.filled
    assert outcome.rejected_reason == "stale_book"


def test_empty_side_is_refused_rather_than_priced() -> None:
    outcome = execute(executor(), 1_000_000, ladder=[])
    assert not outcome.filled
    assert outcome.rejected_reason == "empty_book_side"


def test_an_order_smaller_than_the_venue_minimum_is_rejected() -> None:
    outcome = execute(executor(), 1_000_000, minimum_order_size_scaled=5_000_000)
    assert not outcome.filled
    assert outcome.rejected_reason == "below_minimum_order_size"


def test_a_legal_order_keeps_a_partial_fill_smaller_than_the_minimum() -> None:
    """The minimum applies to what you submit, not to what the book matched.

    Voiding the fill instead threw away ordinary partial fills and, since the stop
    retries, invented one rejection per book state for the rest of the market.
    """
    outcome = execute(
        executor(), 100_000_000, ladder=[(800_000, 500_000)], minimum_order_size_scaled=5_000_000
    )
    assert outcome.filled_shares_scaled == 500_000
    assert outcome.rejected_reason is None


def test_sell_walks_descending_bids() -> None:
    bids = [(750_000, 5_000_000), (740_000, 5_000_000), (700_000, 50_000_000)]
    outcome = execute(
        executor(), 20_000_000, side="SELL", ladder=bids, reference_price_scaled=750_000
    )
    assert [slice_.price_scaled for slice_ in outcome.slices] == [750_000, 740_000, 700_000]
    # Selling into a gapped book realises less than the price that triggered it.
    assert outcome.average_price_scaled is not None
    assert outcome.average_price_scaled < 750_000
    assert outcome.slippage_scaled is not None and outcome.slippage_scaled > 0


def test_slippage_is_signed_so_a_favourable_fill_is_not_counted_as_cost() -> None:
    """A stop that happened to print above its trigger has negative slippage.

    Taking the absolute value would fold those into the average and overstate what
    stopping out actually costs.
    """
    bids = [(780_000, 50_000_000)]
    outcome = execute(
        executor(), 10_000_000, side="SELL", ladder=bids, reference_price_scaled=750_000
    )
    assert outcome.slippage_scaled == -30_000


def test_engine_comparable_realism_neutralises_every_constraint() -> None:
    """`verify-sim` only proves anything if the two simulators face the same rules.

    The stripper works by naming fields, so a new constraint silently survives it
    and the cross-check starts comparing two different models.  Adding a field to
    `ExecutionRealism` must therefore fail here until it is classified.
    """
    from polymarket_bt.backtest.verify import engine_comparable_realism

    neutral = {
        "displayed_depth_haircut_ppm": 0,
        "max_level_participation_ppm": PPM,
        "max_levels_swept": None,
        "volume_participation_ppm": None,
        "stale_book_max_age_ms": None,
        "max_ticks_through_touch": None,
        "max_price_through_touch_scaled": None,
    }
    stripped = engine_comparable_realism(preset("pessimistic"), latency_ms=50)
    for name, value in neutral.items():
        assert getattr(stripped, name) == value, f"{name} still constrains the fast simulator"
    # Fields the engine either shares or that cannot change a fill.
    shared = {"name", "latency", "require_valid_book", "fee"}
    shared |= {"volume_participation_window_ms"}
    assert set(ExecutionRealism.model_fields) == set(neutral) | shared


def test_presets_are_ordered_by_pessimism() -> None:
    optimistic, base, pessimistic = preset("optimistic"), preset("base"), preset("pessimistic")
    assert optimistic.displayed_depth_haircut_ppm == 0
    assert base.displayed_depth_haircut_ppm < pessimistic.displayed_depth_haircut_ppm
    assert pessimistic.max_level_participation_ppm < base.max_level_participation_ppm <= PPM
    assert optimistic.max_levels_swept is None
    assert base.max_levels_swept is None
    assert pessimistic.max_levels_swept == 5
    assert optimistic.volume_participation_ppm is None
    assert base.volume_participation_ppm is None
    assert pessimistic.volume_participation_ppm is None
