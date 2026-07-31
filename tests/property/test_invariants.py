from __future__ import annotations

from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from polymarket_bt.backtest.execution import TakerExecutionSimulator
from polymarket_bt.backtest.fees import FeeModel
from polymarket_bt.backtest.portfolio import Portfolio
from polymarket_bt.clock import decimal_to_scaled, scaled_to_decimal
from polymarket_bt.config import FeeConfig
from polymarket_bt.models.books import BookLevel, BookSnapshot
from polymarket_bt.models.orders import OrderIntent, Side
from polymarket_bt.orderbook.state import OrderBook


def zero_fee() -> FeeModel:
    return FeeModel(
        FeeConfig(
            version="zero",
            effective_start="2020-01-01T00:00:00Z",
            market_type="test",
            liquidity_role="taker",
            formula="zero",
            rate="0",
        )
    )


@given(st.integers(min_value=-(10**12), max_value=10**12))
def test_scaled_decimal_round_trip(value: int) -> None:
    decimal = scaled_to_decimal(value, 1_000_000)
    assert decimal_to_scaled(decimal, 1_000_000) == value


@given(
    available=st.integers(min_value=1, max_value=100_000_000),
    requested=st.integers(min_value=1, max_value=20_000_000),
)
def test_buy_never_increases_cash_and_never_overfills(available: int, requested: int) -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(
        BookSnapshot(
            snapshot_id="snapshot",
            sequence=1,
            run_id="run",
            connection_id="c",
            source="fixture",
            condition_id="condition",
            token_id="token",
            outcome="UP",
            exchange_timestamp_ns=1,
            received_utc_ns=1,
            received_monotonic_ns=1,
            tick_size_scaled=10_000,
            minimum_order_size_scaled=0,
            bids=(BookLevel(price_scaled=400_000, size_scaled=20_000_000),),
            asks=(BookLevel(price_scaled=500_000, size_scaled=20_000_000),),
        )
    )
    portfolio = Portfolio(available)
    intent = OrderIntent(
        strategy_id="property",
        decision_timestamp_ns=1,
        token_id="token",
        condition_id="condition",
        outcome="UP",
        side=Side.BUY,
        requested_shares_scaled=requested,
    )
    result = TakerExecutionSimulator(zero_fee()).execute(intent, book, portfolio, arrival_time_ns=2)
    assert portfolio.available_cash_scaled <= available
    assert sum(fill.size_scaled for fill in result.fills) <= requested
    assert all(fill.size_scaled >= 0 for fill in result.fills)


@given(st.lists(st.integers(min_value=0, max_value=10**9), min_size=1, max_size=20))
def test_book_sizes_never_negative(sizes: list[int]) -> None:
    assert all(size >= 0 for size in sizes)
    assert sum(sizes) >= 0


def test_decimal_example_has_no_float() -> None:
    assert Decimal("0.531") * 1_000_000 == 531_000
