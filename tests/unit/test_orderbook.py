from __future__ import annotations

import uuid

import pytest

from polymarket_bt.models.books import BookLevel, BookLevelChange, BookSnapshot
from polymarket_bt.orderbook.state import InvalidBookState, OrderBook


def snapshot() -> BookSnapshot:
    return BookSnapshot(
        snapshot_id=str(uuid.uuid4()),
        sequence=1,
        run_id="run",
        connection_id="connection",
        source="fixture",
        condition_id="condition",
        token_id="token",
        outcome="UP",
        exchange_timestamp_ns=1,
        received_utc_ns=2,
        received_monotonic_ns=3,
        tick_size_scaled=10_000,
        minimum_order_size_scaled=5_000_000,
        bids=(BookLevel(price_scaled=300_000, size_scaled=2_000_000),),
        asks=(
            BookLevel(price_scaled=400_000, size_scaled=2_000_000),
            BookLevel(price_scaled=500_000, size_scaled=3_000_000),
        ),
    )


def change(*, side: str = "BUY", price: int = 300_000, size: int = 4_000_000) -> BookLevelChange:
    return BookLevelChange(
        parent_event_id="parent",
        change_index=0,
        sequence=2,
        condition_id="condition",
        token_id="token",
        outcome="UP",
        exchange_timestamp_ns=4,
        received_utc_ns=5,
        received_monotonic_ns=6,
        side=side,
        price_scaled=price,
        new_size_scaled=size,
        connection_id="connection",
    )


def test_snapshot_replaces_and_update_sets_absolute_size() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    book.apply(change())
    assert book.bids[300_000] == 4_000_000
    assert book.top().best_bid_scaled == 300_000
    assert book.top().best_ask_scaled == 400_000


def test_zero_size_deletes_level() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    book.apply(change(size=0))
    assert 300_000 not in book.bids


def test_crossed_book_fails_closed() -> None:
    crossed = snapshot().model_copy(
        update={"bids": (BookLevel(price_scaled=500_000, size_scaled=1_000_000),)}
    )
    with pytest.raises(InvalidBookState):
        OrderBook("condition", "token", "UP").replace(crossed)


def test_depth_and_ordering() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    bids, asks = book.snapshot_levels()
    assert [level.price_scaled for level in bids] == [300_000]
    assert [level.price_scaled for level in asks] == [400_000, 500_000]
    assert book.depth_metrics().l5_ask_scaled == 5_000_000


def test_reported_top_prunes_implicitly_exhausted_levels() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(
        snapshot().model_copy(
            update={
                "bids": (
                    BookLevel(price_scaled=360_000, size_scaled=1_000_000),
                    BookLevel(price_scaled=370_000, size_scaled=1_000_000),
                ),
                "asks": (
                    BookLevel(price_scaled=380_000, size_scaled=1_000_000),
                    BookLevel(price_scaled=390_000, size_scaled=1_000_000),
                ),
            }
        )
    )
    book.apply(
        change(price=360_000, size=450_000_000).model_copy(
            update={
                "reported_best_bid_scaled": 360_000,
                "reported_best_ask_scaled": 380_000,
            }
        )
    )
    assert book.best_bid == 360_000
    assert 370_000 not in book.bids
    assert book.best_ask == 380_000


def test_reported_boundary_clears_empty_side() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    book.apply(
        change(side="SELL", price=400_000, size=0).model_copy(
            update={
                "reported_best_bid_scaled": 0,
                "reported_best_ask_scaled": 500_000,
            }
        )
    )
    assert book.best_bid is None
    assert book.best_ask == 500_000


def test_stale_snapshot_does_not_overwrite_newer_state() -> None:
    book = OrderBook("condition", "token", "UP")
    book.replace(snapshot())
    book.apply(change())
    stale = snapshot().model_copy(
        update={"bids": (BookLevel(price_scaled=200_000, size_scaled=1_000_000),)}
    )
    assert book.replace(stale) is False
    assert book.best_bid == 300_000
