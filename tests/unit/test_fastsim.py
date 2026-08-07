from __future__ import annotations

import pyarrow as pa

from polymarket_bt.backtest.episodes import Episode
from polymarket_bt.backtest.fastsim import ThresholdHoldSimulator
from polymarket_bt.backtest.realism import DEFAULT_FEE, ExecutionRealism
from polymarket_bt.backtest.tape import (
    BOOK_SIDE_ASKS,
    BOOK_SIDE_BIDS,
    KIND_SNAPSHOT,
    KIND_UPDATE,
    EpisodeTape,
    tape_schema,
)
from polymarket_bt.backtest.threshold_hold import ThresholdHoldParams
from polymarket_bt.config import LatencyConfig

DEPTH = 10
MINUTE = 60_000_000_000
START = 1_000_000_000_000_000_000
END = START + 15 * MINUTE
UP = "up-token"
DOWN = "down-token"


def episode(winner: str = UP) -> Episode:
    return Episode(
        condition_id="0xcondition",
        market_slug="btc-updown-15m-test",
        start_utc_ns=START,
        end_utc_ns=END,
        up_token_id=UP,
        down_token_id=DOWN,
        tick_size_scaled=10_000,
        minimum_order_size_scaled=0,
        winner_token_id=winner,
        winner_outcome="UP" if winner == UP else "DOWN",
        winner_source="terminal_book",
        winner_confidence=0.99,
        terminal_up_bid_scaled=999_000,
        terminal_down_bid_scaled=1_000,
        reference_open_scaled=None,
        reference_close_scaled=None,
        reference_source=None,
        first_event_utc_ns=START,
        last_event_utc_ns=END,
        up_event_count=10,
        down_event_count=10,
        coverage_start_lead_ns=MINUTE,
        coverage_end_lag_ns=MINUTE,
        quality_error_count=0,
        eligible=True,
        exclusion_reason=None,
    )


def tape(
    quotes: list[tuple[int, int, int | None, int | None, int, int]],
    *,
    winner: str = UP,
    trades: list[tuple[int, int, int]] | None = None,
    events: list[tuple[int, int, int | None]] | None = None,
) -> EpisodeTape:
    """Build a tape from (timestamp, token_index, bid, ask, bid_size, ask_size) rows."""
    rows: dict[str, list[object]] = {field.name: [] for field in tape_schema(DEPTH)}
    pad = [0] * (DEPTH - 1)
    if events is not None:
        assert len(events) == len(quotes)
    for row_index, (timestamp, token_index, bid, ask, bid_size, ask_size) in enumerate(quotes):
        kind, event_side, event_price = (
            events[row_index] if events is not None else (KIND_UPDATE, BOOK_SIDE_BIDS, bid)
        )
        rows["received_utc_ns"].append(timestamp)
        rows["sequence"].append(timestamp)
        rows["token_index"].append(token_index)
        rows["event_kind"].append(kind)
        rows["event_side"].append(event_side)
        rows["event_price_scaled"].append(event_price)
        rows["book_valid"].append(True)
        rows["best_bid_scaled"].append(bid)
        rows["best_ask_scaled"].append(ask)
        rows["bid_size_scaled"].append(bid_size if bid else None)
        rows["ask_size_scaled"].append(ask_size if ask else None)
        rows["tick_size_scaled"].append(10_000)
        rows["bid_prices_scaled"].append([bid or 0, *pad])
        rows["bid_sizes_scaled"].append([bid_size if bid else 0, *pad])
        rows["ask_prices_scaled"].append([ask or 0, *pad])
        rows["ask_sizes_scaled"].append([ask_size if ask else 0, *pad])
        rows["bid_depth_beyond_scaled"].append(0)
        rows["ask_depth_beyond_scaled"].append(0)
    table = pa.Table.from_pydict(rows, schema=tape_schema(DEPTH))
    trades = trades or []
    return EpisodeTape(
        episode=episode(winner),
        depth=DEPTH,
        received_utc_ns=[int(value) for value in rows["received_utc_ns"]],  # type: ignore[arg-type]
        token_index=[int(value) for value in rows["token_index"]],  # type: ignore[arg-type]
        event_kind=[int(value) for value in rows["event_kind"]],  # type: ignore[arg-type]
        event_side=[int(value) for value in rows["event_side"]],  # type: ignore[arg-type]
        event_price_scaled=[int(value or 0) for value in rows["event_price_scaled"]],
        book_valid=[True] * len(rows["received_utc_ns"]),
        best_bid_scaled=list(rows["best_bid_scaled"]),  # type: ignore[arg-type]
        best_ask_scaled=list(rows["best_ask_scaled"]),  # type: ignore[arg-type]
        trade_utc_ns=[item[0] for item in trades],
        trade_token_index=[item[1] for item in trades],
        trade_size_scaled=[item[2] for item in trades],
        uncertainty=[],
        _table=table,
    )


def realism(**overrides: object) -> ExecutionRealism:
    settings: dict[str, object] = {
        "name": "test",
        "latency": LatencyConfig(model="constant", constant_ms=0),
        "fee": DEFAULT_FEE,
    }
    settings.update(overrides)
    return ExecutionRealism(**settings)  # type: ignore[arg-type]


def simulator(params: ThresholdHoldParams, **overrides: object) -> ThresholdHoldSimulator:
    return ThresholdHoldSimulator(params, realism(**overrides), seed=11)


BASE_PARAMS = dict(
    entry_from_minute=12.0,
    entry_to_minute=14.0,
    entry_trigger_price_scaled=800_000,
    entry_limit_price_scaled=850_000,
    order_shares_scaled=10_000_000,
)


def test_holds_winning_side_to_settlement_and_is_paid_one_per_share() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (START + 14 * MINUTE, 0, 950_000, 960_000, 50_000_000, 50_000_000),
    ]
    result = simulator(params).run(tape(quotes))
    assert result.entered
    assert result.entry_shares_scaled == 10_000_000
    assert result.entry_notional_scaled == 8_000_000
    assert result.exit_reason == "settlement"
    assert result.settlement_payout_scaled == 10_000_000
    assert result.net_pnl_scaled == 10_000_000 - 8_000_000 - result.fees_scaled


def test_losing_side_held_to_settlement_pays_nothing() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    quotes = [(START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000)]
    result = simulator(params).run(tape(quotes, winner=DOWN))
    assert result.settlement_payout_scaled == 0
    assert result.won is False
    assert result.net_pnl_scaled == -8_000_000 - result.fees_scaled


def test_entry_outside_the_time_window_is_not_taken() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    quotes = [(START + 11 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000)]
    result = simulator(params).run(tape(quotes))
    assert not result.entered
    assert result.exit_reason == "no_entry"


def test_price_above_the_limit_is_not_chased() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    quotes = [(START + 13 * MINUTE, 0, 890_000, 900_000, 50_000_000, 50_000_000)]
    result = simulator(params).run(tape(quotes))
    assert not result.entered


def test_stop_loss_sells_into_the_bid_and_ends_the_position() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=750_000)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (START + 13 * MINUTE + 1_000, 0, 740_000, 750_000, 50_000_000, 50_000_000),
    ]
    result = simulator(params).run(tape(quotes))
    assert result.exit_reason == "stop_loss"
    assert result.exit_shares_scaled == 10_000_000
    assert result.exit_price_scaled == 740_000
    assert result.settlement_payout_scaled == 0


def test_stop_that_cannot_fill_is_carried_into_settlement_and_reported() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=750_000)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        # The bid disappears entirely: there is nothing to sell into.
        (START + 13 * MINUTE + 1_000, 0, None, 750_000, 0, 50_000_000),
    ]
    result = simulator(params).run(tape(quotes))
    assert result.exit_shares_scaled == 0
    assert result.unfilled_exit_shares_scaled == 10_000_000
    assert result.exit_reason == "stop_unfilled_held_to_settlement"
    assert result.settlement_payout_scaled == 10_000_000


def test_an_unarmed_stop_ignores_a_dip_before_its_minute() -> None:
    """The whole point of a late stop: ride out the early wobble, keep the winner."""
    params = ThresholdHoldParams(
        **BASE_PARAMS, stop_loss_price_scaled=750_000, stop_loss_from_minute=14.0
    )
    quotes = [
        (START + 12 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        # Well below the stop, but minute 13 is before the stop is armed.
        (START + 13 * MINUTE, 0, 700_000, 710_000, 50_000_000, 50_000_000),
        (START + 14 * MINUTE + MINUTE // 2, 0, 960_000, 970_000, 50_000_000, 50_000_000),
    ]
    result = simulator(params).run(tape(quotes))
    assert result.entered
    assert result.exit_reason == "settlement"
    assert result.settlement_payout_scaled == 10_000_000


def test_an_armed_stop_fires_on_the_first_breach_after_its_minute() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS, stop_loss_price_scaled=750_000, stop_loss_from_minute=13.5
    )
    quotes = [
        (START + 12 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (START + 13 * MINUTE, 0, 700_000, 710_000, 50_000_000, 50_000_000),
        (START + 14 * MINUTE, 0, 720_000, 730_000, 50_000_000, 50_000_000),
    ]
    result = simulator(params).run(tape(quotes))
    assert result.exit_reason == "stop_loss"
    assert result.exit_price_scaled == 720_000


def test_dust_below_the_venue_minimum_is_not_repeatedly_offered() -> None:
    """An unsendable order is not an attempt; it is noise in the rejection report."""
    fields = {name: getattr(episode(), name) for name in episode().__dataclass_fields__}
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=750_000)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        # The stop triggers repeatedly but only 2 shares remain sellable.
        *[
            (START + 13 * MINUTE + step * 1_000_000, 0, 740_000, 750_000, 2_000_000, 50_000_000)
            for step in range(1, 40)
        ],
    ]
    built = tape(quotes)
    built.episode = Episode(**{**fields, "minimum_order_size_scaled": 5_000_000})
    result = simulator(params).run(built)
    stops = [order for order in result.orders if order.kind == "stop_loss"]
    # Each attempt takes the 2 shares on offer; once under 5 the rest is unsellable
    # and the strategy stops asking, rather than asking once per book state.
    assert len(stops) <= 6, [order.rejected_reason for order in stops]
    assert result.unfilled_exit_shares_scaled > 0


def test_position_cannot_be_exited_before_the_entry_fill_arrives() -> None:
    """A stop must not act on book states that precede its own entry."""
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=750_000)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        # Inside the latency window: cheap, but the position does not exist yet.
        (START + 13 * MINUTE + 10_000_000, 0, 700_000, 710_000, 50_000_000, 50_000_000),
        (START + 13 * MINUTE + 300_000_000, 0, 960_000, 970_000, 50_000_000, 50_000_000),
    ]
    result = ThresholdHoldSimulator(
        params, realism(latency=LatencyConfig(model="constant", constant_ms=100)), seed=11
    ).run(tape(quotes))
    assert result.entered
    # The 0.70 print happened before the fill landed, so no stop was triggered by it.
    assert result.exit_reason == "settlement"
    assert result.exit_shares_scaled == 0


def test_entry_matches_the_book_at_arrival_not_at_decision() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        # Book moves away during the latency window; the fill pays the later price.
        (START + 13 * MINUTE + 50_000_000, 0, 830_000, 840_000, 50_000_000, 50_000_000),
    ]
    result = ThresholdHoldSimulator(
        params, realism(latency=LatencyConfig(model="constant", constant_ms=100)), seed=11
    ).run(tape(quotes))
    assert result.entry_price_scaled == 840_000


def test_notional_buy_is_a_cash_cap_including_fees_at_arrival() -> None:
    params = ThresholdHoldParams(
        **{
            **BASE_PARAMS,
            "order_shares_scaled": None,
            "order_notional_scaled": 100_000_000,
        },
        stop_loss_price_scaled=None,
    )
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 500_000_000, 500_000_000),
        # The price moves before arrival.  Share sizing must be recomputed here,
        # not fixed at the 0.80 decision ask.
        (START + 13 * MINUTE + 50_000_000, 0, 830_000, 840_000, 500_000_000, 500_000_000),
    ]
    result = ThresholdHoldSimulator(
        params, realism(latency=LatencyConfig(model="constant", constant_ms=100)), seed=11
    ).run(tape(quotes), available_cash_scaled=100_000_000)
    entry = next(order for order in result.orders if order.kind == "entry")
    spent = entry.notional_scaled + entry.fees_scaled
    assert result.entered
    assert spent <= 100_000_000
    assert 100_000_000 - spent <= 2
    assert result.entry_shares_scaled < 125_000_000


def test_cash_pending_rejection_does_not_consume_the_entry_limit() -> None:
    params = ThresholdHoldParams(
        **{
            **BASE_PARAMS,
            "order_shares_scaled": None,
            "order_notional_scaled": 100_000_000,
        },
        stop_loss_price_scaled=None,
        max_entries_per_episode=1,
        reentry_cooldown_seconds=0,
    )
    first = START + 13 * MINUTE
    second = first + 1_000_000_000
    quotes = [
        (first, 0, 790_000, 800_000, 500_000_000, 500_000_000),
        (second, 0, 790_000, 800_000, 500_000_000, 500_000_000),
    ]
    result = simulator(params).run(
        tape(quotes),
        available_cash_scaled=lambda arrival_ns: 0 if arrival_ns < second else 100_000_000,
    )
    assert result.entered
    assert [order.rejected_reason for order in result.orders[:2]] == [
        "insufficient_available_cash",
        None,
    ]


def test_disallowing_partial_entry_has_atomic_fok_semantics() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        allow_partial_entry=False,
    )
    quotes = [(START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 5_000_000)]
    result = simulator(params).run(tape(quotes))
    assert not result.entered
    assert result.orders[0].filled_shares_scaled == 0
    assert result.orders[0].fees_scaled == 0
    assert result.orders[0].rejected_reason == "partial_entry_disallowed"


def test_allowing_partial_entry_keeps_the_fak_fill() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        allow_partial_entry=True,
    )
    quotes = [(START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 5_000_000)]
    result = simulator(params).run(tape(quotes))
    assert result.entry_shares_scaled == 5_000_000


def test_stop_retry_limit_zero_sends_only_the_initial_attempt() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=750_000,
        stop_retry_limit=0,
        stop_retry_interval_ms=250,
    )
    breach = START + 13 * MINUTE + 1_000_000
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (breach, 0, 740_000, 750_000, 1_000_000, 50_000_000),
        *[
            (breach + step * 1_000_000, 0, 740_000, 750_000, 1_000_000, 50_000_000)
            for step in range(1, 10)
        ],
    ]
    events = [
        (KIND_UPDATE, BOOK_SIDE_ASKS, 800_000),
        (KIND_UPDATE, BOOK_SIDE_BIDS, 740_000),
        *[(KIND_UPDATE, BOOK_SIDE_ASKS, 750_000) for _ in range(9)],
    ]
    result = simulator(params).run(tape(quotes, events=events))
    stops = [order for order in result.orders if order.kind == "stop_loss"]
    assert len(stops) == 1
    assert stops[0].filled_shares_scaled == 1_000_000


def test_self_impact_persists_until_exact_level_is_restated() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=750_000,
        stop_loss_fraction_ppm=500_000,
        stop_retry_limit=2,
        stop_retry_interval_ms=1,
    )
    breach = START + 13 * MINUTE + 10_000_000
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (breach, 0, 740_000, 750_000, 5_000_000, 50_000_000),
        # An ask update does not replenish the bid consumed by the first stop.
        (breach + 1_000_000, 0, 740_000, 750_000, 5_000_000, 50_000_000),
        # This exact bid restatement does replenish it for the second retry.
        (breach + 2_000_000, 0, 740_000, 750_000, 5_000_000, 50_000_000),
    ]
    events = [
        (KIND_UPDATE, BOOK_SIDE_ASKS, 800_000),
        (KIND_UPDATE, BOOK_SIDE_BIDS, 740_000),
        (KIND_UPDATE, BOOK_SIDE_ASKS, 750_000),
        (KIND_UPDATE, BOOK_SIDE_BIDS, 740_000),
    ]
    result = simulator(params).run(tape(quotes, events=events))
    stops = [order for order in result.orders if order.kind == "stop_loss"]
    assert [order.filled_shares_scaled for order in stops] == [5_000_000, 0, 5_000_000]
    assert result.exit_shares_scaled == 10_000_000


def test_snapshot_replenishes_all_self_consumed_levels() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=750_000,
        stop_loss_fraction_ppm=500_000,
        stop_retry_limit=1,
        stop_retry_interval_ms=1,
    )
    breach = START + 13 * MINUTE + 10_000_000
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (breach, 0, 740_000, 750_000, 5_000_000, 50_000_000),
        (breach + 1_000_000, 0, 740_000, 750_000, 5_000_000, 50_000_000),
    ]
    events = [
        (KIND_UPDATE, BOOK_SIDE_ASKS, 800_000),
        (KIND_UPDATE, BOOK_SIDE_BIDS, 740_000),
        (KIND_SNAPSHOT, -1, None),
    ]
    result = simulator(params).run(tape(quotes, events=events))
    stops = [order for order in result.orders if order.kind == "stop_loss"]
    assert [order.filled_shares_scaled for order in stops] == [5_000_000, 5_000_000]


def test_partial_take_profit_is_attempted_once() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        take_profit_price_scaled=900_000,
        take_profit_fraction_ppm=500_000,
    )
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        *[
            (START + 13 * MINUTE + step * 1_000_000, 0, 950_000, 960_000, 50_000_000, 50_000_000)
            for step in range(1, 5)
        ],
    ]
    result = simulator(params).run(tape(quotes))
    profits = [order for order in result.orders if order.kind == "take_profit"]
    assert len(profits) == 1
    assert profits[0].filled_shares_scaled == 5_000_000
    assert result.settled_shares_scaled == 5_000_000


def test_partial_flatten_is_attempted_once() -> None:
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        flatten_before_end_seconds=30,
    )
    flatten = END - 30_000_000_000
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        *[
            (flatten + step * 1_000_000, 0, 780_000, 790_000, 2_000_000, 50_000_000)
            for step in range(4)
        ],
    ]
    result = simulator(params).run(tape(quotes))
    flattens = [order for order in result.orders if order.kind == "flatten"]
    assert len(flattens) == 1
    assert flattens[0].filled_shares_scaled == 2_000_000
    assert result.settled_shares_scaled == 8_000_000


def test_order_arriving_at_or_after_market_end_is_rejected() -> None:
    params = ThresholdHoldParams(
        entry_from_minute=14.9,
        entry_to_minute=15.0,
        entry_trigger_price_scaled=800_000,
        entry_limit_price_scaled=850_000,
        order_shares_scaled=10_000_000,
        stop_loss_price_scaled=None,
    )
    quotes = [(END - 10_000_000, 0, 790_000, 800_000, 50_000_000, 50_000_000)]
    result = ThresholdHoldSimulator(
        params, realism(latency=LatencyConfig(model="constant", constant_ms=100)), seed=11
    ).run(tape(quotes))
    assert not result.entered
    assert result.orders[0].rejected_reason == "market_closed_at_arrival"


def test_episode_taker_delay_is_added_to_sampled_latency() -> None:
    params = ThresholdHoldParams(
        entry_from_minute=14.9,
        entry_to_minute=15.0,
        entry_trigger_price_scaled=800_000,
        entry_limit_price_scaled=850_000,
        order_shares_scaled=10_000_000,
        stop_loss_price_scaled=None,
    )
    built = tape([(END - 100_000_000, 0, 790_000, 800_000, 50_000_000, 50_000_000)])
    fields = {name: getattr(built.episode, name) for name in built.episode.__dataclass_fields__}
    built.episode = Episode(**{**fields, "taker_order_delay_ms": 250})
    result = simulator(params).run(built)
    assert not result.entered
    assert result.orders[0].arrival_utc_ns == END + 150_000_000
    assert result.orders[0].rejected_reason == "market_closed_at_arrival"


def test_episode_fee_schedule_overrides_the_configured_fallback() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    built = tape([(START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000)])
    fields = {name: getattr(built.episode, name) for name in built.episode.__dataclass_fields__}
    built.episode = Episode(**{**fields, "fee_rate": "0", "fee_exponent": 1})
    result = simulator(params).run(built)
    assert result.entered
    assert result.orders[0].fees_scaled == 0


def test_ineligible_episodes_are_skipped_with_a_reason() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=None)
    built = tape([(START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000)])
    fields = {name: getattr(built.episode, name) for name in built.episode.__dataclass_fields__}
    built.episode = Episode(
        **{**fields, "eligible": False, "exclusion_reason": "winner_not_determinable"}
    )
    result = simulator(params).run(built)
    assert not result.entered
    assert result.skip_reason == "winner_not_determinable"


def test_results_are_reproducible_for_a_fixed_seed() -> None:
    params = ThresholdHoldParams(**BASE_PARAMS, stop_loss_price_scaled=750_000)
    quotes = [
        (START + 13 * MINUTE, 0, 790_000, 800_000, 50_000_000, 50_000_000),
        (START + 13 * MINUTE + 1_000_000, 0, 740_000, 750_000, 50_000_000, 50_000_000),
    ]
    lognormal = realism(latency=LatencyConfig(model="lognormal"))
    first = ThresholdHoldSimulator(params, lognormal, seed=99).run(tape(quotes))
    second = ThresholdHoldSimulator(params, lognormal, seed=99).run(tape(quotes))
    assert first.net_pnl_scaled == second.net_pnl_scaled
    assert [order.arrival_utc_ns for order in first.orders] == [
        order.arrival_utc_ns for order in second.orders
    ]


def test_twap_executes_on_clock_ticks_when_the_book_is_static() -> None:
    trigger = START + 13 * MINUTE
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        entry_execution_policy="twap",
        execution_horizon_seconds=1,
        execution_slices=4,
    )
    result = simulator(params).run(tape([(trigger, 0, 790_000, 800_000, 100_000_000, 100_000_000)]))
    entries = [order for order in result.orders if order.kind == "entry"]
    assert [order.decision_utc_ns for order in entries] == [
        trigger,
        trigger + 250_000_000,
        trigger + 500_000_000,
        trigger + 750_000_000,
    ]
    assert [order.filled_shares_scaled for order in entries] == [2_500_000] * 4
    assert result.entry_shares_scaled == 10_000_000
    assert result.execution_policy == "twap"
    assert result.execution_target_shares_scaled == 10_000_000
    assert result.execution_unfilled_shares_scaled == 0
    assert result.execution_fill_rate_ppm == 1_000_000
    assert result.execution_non_fill_penalty_scaled == 0
    assert result.execution_adverse_selection_penalty_scaled == 0
    assert result.execution_objective_scaled == result.execution_implementation_shortfall_scaled


def test_adaptive_pov_reacts_to_a_trade_only_timestamp_causally() -> None:
    trigger = START + 13 * MINUTE
    trade_ns = trigger + 375_000_000
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        entry_execution_policy="adaptive_pov",
        execution_horizon_seconds=1,
        execution_participation_ppm=200_000,
    )
    built = tape(
        [(trigger, 0, 790_000, 800_000, 100_000_000, 100_000_000)],
        trades=[(trade_ns, 0, 80_000_000)],
    )
    fields = {name: getattr(built.episode, name) for name in built.episode.__dataclass_fields__}
    # Suppress the small pre-trade TWAP floor; the observed trade then makes a
    # legal parent-sized child exactly at its own timestamp, between clock ticks.
    built.episode = Episode(**{**fields, "minimum_order_size_scaled": 5_000_000})
    result = simulator(params).run(built)
    entries = [order for order in result.orders if order.kind == "entry"]
    assert len(entries) == 1
    assert entries[0].decision_utc_ns == trade_ns
    assert entries[0].filled_shares_scaled == 10_000_000
    assert result.execution_policy == "adaptive_pov"


def test_sequential_entry_never_spends_more_than_the_available_wallet() -> None:
    trigger = START + 13 * MINUTE
    params = ThresholdHoldParams(
        **{
            **BASE_PARAMS,
            "order_shares_scaled": None,
            "order_notional_scaled": 100_000_000,
        },
        stop_loss_price_scaled=None,
        entry_execution_policy="twap",
        execution_horizon_seconds=1,
        execution_slices=4,
    )
    result = simulator(params).run(
        tape([(trigger, 0, 790_000, 800_000, 500_000_000, 500_000_000)]),
        available_cash_scaled=50_000_000,
    )
    spent = result.entry_notional_scaled + sum(
        order.fees_scaled for order in result.orders if order.kind == "entry"
    )
    assert result.entered
    assert spent <= 50_000_000
    # The parent itself is sized from cash present at the signal, not from the
    # larger configured ceiling or a credit arriving later in the entry window.
    assert result.execution_target_shares_scaled < 70_000_000
    assert result.execution_unfilled_shares_scaled > 0


def test_sequential_signal_retries_after_untradable_cash_is_released() -> None:
    trigger = START + 13 * MINUTE
    released = trigger + 2_000_000_000
    params = ThresholdHoldParams(
        **{
            **BASE_PARAMS,
            "order_shares_scaled": None,
            "order_notional_scaled": 100_000_000,
        },
        stop_loss_price_scaled=None,
        entry_execution_policy="twap",
        execution_horizon_seconds=1,
        execution_slices=4,
        reentry_cooldown_seconds=0,
    )
    built = tape(
        [
            (trigger, 0, 790_000, 800_000, 500_000_000, 500_000_000),
            (released, 0, 790_000, 800_000, 500_000_000, 500_000_000),
        ]
    )
    fields = {name: getattr(built.episode, name) for name in built.episode.__dataclass_fields__}
    built.episode = Episode(**{**fields, "minimum_order_size_scaled": 5_000_000})

    result = simulator(params).run(
        built,
        available_cash_scaled=lambda at_ns: 1_000_000 if at_ns < released else 100_000_000,
    )

    assert result.entered
    entries = [order for order in result.orders if order.kind == "entry"]
    assert entries
    assert min(order.decision_utc_ns for order in entries) >= released


def test_sequential_children_keep_self_impact_on_a_static_level() -> None:
    trigger = START + 13 * MINUTE
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        entry_execution_policy="twap",
        execution_horizon_seconds=1,
        execution_slices=4,
    )
    result = simulator(params).run(tape([(trigger, 0, 790_000, 800_000, 100_000_000, 3_000_000)]))
    # No event restates the ask. Every child sees what earlier children left, so
    # the aggregate counterfactual fill cannot exceed the original resting size.
    assert 0 < result.entry_shares_scaled <= 3_000_000
    assert result.execution_unfilled_shares_scaled > 0


def test_sequential_child_arriving_after_parent_deadline_is_rejected() -> None:
    trigger = START + 13 * MINUTE
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=None,
        entry_execution_policy="twap",
        execution_horizon_seconds=1,
        execution_slices=4,
    )
    result = ThresholdHoldSimulator(
        params,
        realism(latency=LatencyConfig(model="constant", constant_ms=300)),
        seed=11,
    ).run(tape([(trigger, 0, 790_000, 800_000, 100_000_000, 100_000_000)]))
    entries = [order for order in result.orders if order.kind == "entry"]
    assert entries[-1].decision_utc_ns == trigger + 1_000_000_000
    assert entries[-1].rejected_reason == "parent_deadline_elapsed"
    assert entries[-1].filled_shares_scaled == 0
    assert result.entry_shares_scaled == 7_500_000
    assert result.execution_unfilled_shares_scaled == 2_500_000


def test_sequential_exit_logic_arms_only_after_parent_deadline() -> None:
    trigger = START + 13 * MINUTE
    deadline = trigger + 1_000_000_000
    params = ThresholdHoldParams(
        **BASE_PARAMS,
        stop_loss_price_scaled=750_000,
        entry_execution_policy="twap",
        execution_horizon_seconds=1,
        execution_slices=4,
    )
    quotes = [
        (trigger, 0, 790_000, 800_000, 100_000_000, 100_000_000),
        # The remaining ask disappears and the bid breaches the stop during the
        # parent horizon. The sequential executor continues observing through its
        # deadline, so the outer hold strategy must not replay this earlier row.
        (trigger + 100_000_000, 0, 700_000, None, 100_000_000, 0),
        (deadline, 0, 950_000, None, 100_000_000, 0),
    ]
    events = [
        (KIND_UPDATE, BOOK_SIDE_BIDS, 790_000),
        (KIND_UPDATE, BOOK_SIDE_ASKS, 800_000),
        (KIND_UPDATE, BOOK_SIDE_BIDS, 950_000),
    ]
    result = simulator(params).run(tape(quotes, events=events))
    assert result.entry_shares_scaled == 2_500_000
    assert not [order for order in result.orders if order.kind == "stop_loss"]
    assert result.exit_reason == "settlement"
    assert result.settled_shares_scaled == 2_500_000
