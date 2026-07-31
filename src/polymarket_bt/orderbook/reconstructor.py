from __future__ import annotations

from dataclasses import dataclass

from polymarket_bt.models.books import BookLevelChange, BookSnapshot, TickSizeChange, TopOfBook
from polymarket_bt.orderbook.state import OrderBook


@dataclass(slots=True)
class UncertaintyInterval:
    token_id: str
    start_utc_ns: int
    end_utc_ns: int | None
    reason: str
    first_sequence: int | None
    last_sequence: int | None = None


class BookReconstructor:
    def __init__(self) -> None:
        self.books: dict[str, OrderBook] = {}
        self.uncertainty: list[UncertaintyInterval] = []
        self.effective_tick_sizes: dict[str, int] = {}

    def register(self, condition_id: str, token_id: str, outcome: str) -> OrderBook:
        existing = self.books.get(token_id)
        if existing and (existing.condition_id != condition_id or existing.outcome != outcome):
            raise ValueError(f"token mapping changed unexpectedly: {token_id}")
        if existing is None:
            existing = OrderBook(condition_id, token_id, outcome)
            self.books[token_id] = existing
        return existing

    def apply_snapshot(self, snapshot: BookSnapshot) -> TopOfBook:
        effective_tick = self.effective_tick_sizes.get(snapshot.token_id)
        if snapshot.source == "clob_rest" or effective_tick is None:
            effective_tick = snapshot.tick_size_scaled
            self.effective_tick_sizes[snapshot.token_id] = effective_tick
        elif snapshot.tick_size_scaled != effective_tick:
            # WebSocket snapshots do not always carry tick_size. Older normalized
            # rows therefore contain the discovery-time fallback. A preceding
            # explicit tick event remains authoritative for deterministic replay.
            snapshot = snapshot.model_copy(update={"tick_size_scaled": effective_tick})
        book = self.register(snapshot.condition_id, snapshot.token_id, snapshot.outcome)
        book.replace(snapshot)
        for interval in reversed(self.uncertainty):
            if interval.token_id == snapshot.token_id and interval.end_utc_ns is None:
                interval.end_utc_ns = snapshot.received_utc_ns
                interval.last_sequence = snapshot.sequence
                break
        return book.top()

    def apply_tick_size_change(self, change: TickSizeChange) -> TopOfBook | None:
        if change.new_tick_size_scaled <= 0:
            raise ValueError("tick size must be positive")
        self.effective_tick_sizes[change.token_id] = change.new_tick_size_scaled
        book = self.books.get(change.token_id)
        if book is None:
            return None
        if book.condition_id != change.condition_id:
            raise ValueError("tick-size token/market mapping does not match book")
        book.tick_size_scaled = change.new_tick_size_scaled
        return book.top()

    def apply_change(self, change: BookLevelChange) -> TopOfBook:
        book = self.books.get(change.token_id)
        if book is None or not book.valid:
            raise ValueError(f"cannot apply update without valid snapshot: {change.token_id}")
        book.apply(change)
        return book.top()

    def mark_uncertain(
        self,
        token_id: str,
        start_utc_ns: int,
        reason: str,
        first_sequence: int | None = None,
    ) -> None:
        book = self.books.get(token_id)
        if book:
            book.valid = False
        self.uncertainty.append(
            UncertaintyInterval(token_id, start_utc_ns, None, reason, first_sequence)
        )

    def valid(self, token_id: str) -> bool:
        book = self.books.get(token_id)
        return bool(book and book.valid)
