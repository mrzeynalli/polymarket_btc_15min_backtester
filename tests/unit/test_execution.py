from __future__ import annotations

from polymarket_bt.backtest.execution import TakerExecutionSimulator
from polymarket_bt.backtest.fees import FeeModel
from polymarket_bt.backtest.latency import LatencyModel
from polymarket_bt.backtest.portfolio import Portfolio
from polymarket_bt.config import FeeConfig, LatencyConfig
from polymarket_bt.models.orders import OrderIntent, Side
from polymarket_bt.orderbook.state import OrderBook
from tests.unit.test_orderbook import snapshot


def fee_config() -> FeeConfig:
    return FeeConfig(
        version="test-v2",
        effective_start="2026-03-31T00:00:00Z",
        market_type="crypto",
        liquidity_role="taker",
        formula="shares_rate_p_one_minus_p",
        rate="0.07",
        exponent=1,
        minimum_fee="0.000001",
        rounding="half_up",
    )


def test_current_crypto_fee_curve() -> None:
    fee = FeeModel(fee_config()).calculate(100_000_000, 500_000)
    assert fee == 1_750_000


def test_taker_sweep_is_price_ordered_and_partial() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    portfolio = Portfolio(100_000_000)
    intent = OrderIntent(
        strategy_id="test",
        decision_timestamp_ns=10,
        token_id="token",
        condition_id="condition",
        outcome="UP",
        side=Side.BUY,
        requested_shares_scaled=10_000_000,
    )
    result = TakerExecutionSimulator(FeeModel(fee_config())).execute(
        intent, book, portfolio, arrival_time_ns=20
    )
    assert result.status == "partial"
    assert [fill.price_scaled for fill in result.fills] == [400_000, 500_000]
    assert sum(fill.size_scaled for fill in result.fills) == 5_000_000
    assert portfolio.inventory("token") == 5_000_000
    assert portfolio.available_cash_scaled < portfolio.starting_cash_scaled


def test_sell_requires_inventory() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    intent = OrderIntent(
        strategy_id="test",
        decision_timestamp_ns=10,
        token_id="token",
        condition_id="condition",
        outcome="UP",
        side=Side.SELL,
        requested_shares_scaled=1_000_000,
    )
    result = TakerExecutionSimulator(FeeModel(fee_config())).execute(
        intent, book, Portfolio(10_000_000), arrival_time_ns=20
    )
    assert result.status == "rejected"


def test_latency_seed_is_deterministic() -> None:
    config = LatencyConfig(model="empirical", samples_ms=[5, 10, 20])
    first = LatencyModel(config, seed=7)
    second = LatencyModel(config, seed=7)
    assert [first.sample_ns() for _ in range(20)] == [second.sample_ns() for _ in range(20)]


def test_settlement_pays_winner_and_zeroes_inventory() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    portfolio = Portfolio(10_000_000)
    intent = OrderIntent(
        strategy_id="test",
        decision_timestamp_ns=10,
        token_id="token",
        condition_id="condition",
        outcome="UP",
        side=Side.BUY,
        requested_shares_scaled=2_000_000,
    )
    result = TakerExecutionSimulator(FeeModel(fee_config())).execute(
        intent, book, portfolio, arrival_time_ns=20
    )
    assert result.status == "filled"
    cash_before = portfolio.available_cash_scaled
    payout = portfolio.settle_market("condition", "token", 30)
    assert payout == 2_000_000
    assert portfolio.available_cash_scaled == cash_before + payout
    assert portfolio.inventory("token") == 0


def test_portfolio_rejects_direct_overspend() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    portfolio = Portfolio(1)
    intent = OrderIntent(
        strategy_id="test",
        decision_timestamp_ns=10,
        token_id="token",
        condition_id="condition",
        outcome="UP",
        side=Side.BUY,
        requested_shares_scaled=1_000_000,
    )
    result = TakerExecutionSimulator(FeeModel(fee_config())).execute(
        intent, book, portfolio, arrival_time_ns=20
    )
    assert result.status == "rejected"
    assert portfolio.available_cash_scaled == 1
