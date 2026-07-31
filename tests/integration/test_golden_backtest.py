from __future__ import annotations

from polymarket_bt.backtest.engine import BacktestEngine
from polymarket_bt.config import BacktestConfig, FeeConfig, LatencyConfig
from polymarket_bt.constants import QualityState
from polymarket_bt.models.books import BookLevel, BookSnapshot
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.replay.event_clock import ReplayEvent
from polymarket_bt.replay.integrity import QualityInterval
from polymarket_bt.strategies import ExampleThresholdStrategy, NoOpStrategy


def config() -> BacktestConfig:
    return BacktestConfig(
        strategy="example_threshold",
        starting_cash="10.000000",
        replay_clock="local_receive_time",
        reject_on_gap=True,
        random_seed=42,
        latency=LatencyConfig(model="disabled"),
        fee=FeeConfig(
            version="polymarket-fee-schedule-v2-test",
            effective_start="2026-03-31T00:00:00Z",
            market_type="crypto",
            liquidity_role="taker",
            formula="shares_rate_p_one_minus_p",
            rate="0.07",
            exponent=1,
            minimum_fee="0.000001",
            rounding="half_up",
        ),
    )


def events() -> list[ReplayEvent]:
    snapshot = BookSnapshot(
        snapshot_id="golden",
        sequence=1,
        run_id="run",
        connection_id="clob",
        source="fixture",
        condition_id="condition",
        token_id="up-token",
        outcome="UP",
        exchange_timestamp_ns=1,
        received_utc_ns=1,
        received_monotonic_ns=1,
        tick_size_scaled=100_000,
        minimum_order_size_scaled=0,
        bids=(BookLevel(price_scaled=300_000, size_scaled=10_000_000),),
        asks=(
            BookLevel(price_scaled=400_000, size_scaled=2_000_000),
            BookLevel(price_scaled=500_000, size_scaled=3_000_000),
        ),
    )
    first_price = BtcPriceEvent(
        price_event_id="btc-1",
        sequence=2,
        source="CHAINLINK_BTCUSD",
        topic="crypto_prices_chainlink",
        symbol="btc/usd",
        rtds_envelope_timestamp_ns=2,
        underlying_source_timestamp_ns=2,
        received_utc_ns=2,
        received_monotonic_ns=2,
        price_scaled=6_700_000_000_000,
        connection_id="rtds",
    )
    second_price = first_price.model_copy(
        update={
            "price_event_id": "btc-2",
            "sequence": 3,
            "received_utc_ns": 3,
            "received_monotonic_ns": 3,
            "underlying_source_timestamp_ns": 3,
            "price_scaled": 6_710_000_000_000,
        }
    )
    return [
        ReplayEvent("book_snapshot", 1, 1, 1, "clob", 1, 0, 10, snapshot),
        ReplayEvent("btc_price", 2, 2, 2, "rtds", 2, 0, 40, first_price),
        ReplayEvent("btc_price", 3, 3, 3, "rtds", 3, 0, 40, second_price),
        ReplayEvent(
            "market_resolution",
            4,
            4,
            4,
            "resolution",
            4,
            0,
            50,
            {
                "condition_id": "condition",
                "winning_token_id": "up-token",
                "winning_outcome": "UP",
            },
        ),
    ]


def test_golden_partial_fill_fee_settlement_and_pnl() -> None:
    strategy = ExampleThresholdStrategy(threshold_ppm=1, shares_scaled=6_000_000)
    result = BacktestEngine(config(), strategy).run(events())
    assert result.summary.order_intents == 1
    assert result.summary.partial_fills == 1
    assert result.summary.fills == 2
    assert result.summary.share_volume_scaled == 5_000_000
    assert result.summary.notional_volume_scaled == 2_300_000
    assert result.summary.fees_scaled == 86_100
    assert result.summary.ending_capital_scaled == 12_613_900
    assert result.summary.net_pnl_scaled == 2_613_900


def test_no_op_replay_has_no_trades() -> None:
    result = BacktestEngine(config(), NoOpStrategy()).run(events())
    assert result.summary.order_intents == 0
    assert result.summary.fills == 0
    assert result.summary.ending_capital_scaled == 10_000_000


def test_replay_is_financially_deterministic() -> None:
    first = BacktestEngine(
        config(), ExampleThresholdStrategy(threshold_ppm=1, shares_scaled=6_000_000)
    ).run(events())
    second = BacktestEngine(
        config(), ExampleThresholdStrategy(threshold_ppm=1, shares_scaled=6_000_000)
    ).run(events())
    assert first.summary == second.summary
    assert [fill for result in first.order_results for fill in result.fills] == [
        fill for result in second.order_results for fill in result.fills
    ]


def test_degraded_time_uses_interval_duration_not_presence() -> None:
    quality = [
        QualityInterval(
            start_utc_ns=2,
            end_utc_ns=3,
            state=QualityState.DEGRADED,
            category="fixture_gap",
            details={},
        )
    ]
    result = BacktestEngine(config(), NoOpStrategy(), quality_intervals=quality).run(events())
    assert result.summary.degraded_time_ppm == 333_333
