from __future__ import annotations

from dataclasses import dataclass

from sortedcontainers import SortedDict

from polymarket_bt.constants import POLYMARKET_PRICE_SCALE
from polymarket_bt.models.books import BookLevel, BookLevelChange, BookSnapshot, TopOfBook
from polymarket_bt.orderbook.validator import ValidationIssue, validate_cross, validate_level


class InvalidBookState(ValueError):
    def __init__(self, issues: list[ValidationIssue]) -> None:
        self.issues = issues
        super().__init__("; ".join(issue.message for issue in issues))


@dataclass(frozen=True, slots=True)
class DepthMetrics:
    l1_bid_scaled: int
    l1_ask_scaled: int
    l5_bid_scaled: int
    l5_ask_scaled: int
    l10_bid_scaled: int
    l10_ask_scaled: int
    within_1c_bid_scaled: int
    within_1c_ask_scaled: int
    within_2c_bid_scaled: int
    within_2c_ask_scaled: int
    within_5c_bid_scaled: int
    within_5c_ask_scaled: int
    imbalance_ppm: int | None


class OrderBook:
    def __init__(self, condition_id: str, token_id: str, outcome: str) -> None:
        self.condition_id = condition_id
        self.token_id = token_id
        self.outcome = outcome
        self.bids: SortedDict[int, int] = SortedDict()
        self.asks: SortedDict[int, int] = SortedDict()
        self.tick_size_scaled = 10_000
        self.minimum_order_size_scaled = 0
        self.last_sequence = 0
        self.last_received_utc_ns = 0
        self.source_hash: str | None = None
        self.valid = False

    @property
    def best_bid(self) -> int | None:
        return self.bids.peekitem(-1)[0] if self.bids else None

    @property
    def best_ask(self) -> int | None:
        return self.asks.peekitem(0)[0] if self.asks else None

    def replace(self, snapshot: BookSnapshot) -> bool:
        if snapshot.token_id != self.token_id or snapshot.condition_id != self.condition_id:
            raise InvalidBookState(
                [
                    ValidationIssue(
                        "token_mapping_invalid", "snapshot token/market does not match book"
                    )
                ]
            )
        # REST recovery is asynchronous with respect to the receive worker. A
        # response assigned an older collector sequence must never overwrite a
        # state that has already advanced past it.
        if snapshot.sequence < self.last_sequence:
            return False
        issues: list[ValidationIssue] = []
        bids: SortedDict[int, int] = SortedDict()
        asks: SortedDict[int, int] = SortedDict()
        for level in snapshot.bids:
            issues.extend(
                validate_level(level.price_scaled, level.size_scaled, snapshot.tick_size_scaled)
            )
            if level.price_scaled in bids:
                issues.append(ValidationIssue("duplicate_level", "duplicate bid price"))
            if level.size_scaled > 0:
                bids[level.price_scaled] = level.size_scaled
        for level in snapshot.asks:
            issues.extend(
                validate_level(level.price_scaled, level.size_scaled, snapshot.tick_size_scaled)
            )
            if level.price_scaled in asks:
                issues.append(ValidationIssue("duplicate_level", "duplicate ask price"))
            if level.size_scaled > 0:
                asks[level.price_scaled] = level.size_scaled
        issues.extend(
            validate_cross(
                bids.peekitem(-1)[0] if bids else None,
                asks.peekitem(0)[0] if asks else None,
            )
        )
        if issues:
            self.valid = False
            raise InvalidBookState(issues)
        self.bids = bids
        self.asks = asks
        self.tick_size_scaled = snapshot.tick_size_scaled
        self.minimum_order_size_scaled = snapshot.minimum_order_size_scaled
        self.last_sequence = snapshot.sequence
        self.last_received_utc_ns = snapshot.received_utc_ns
        self.source_hash = snapshot.book_hash
        self.valid = True
        return True

    def apply(self, change: BookLevelChange) -> None:
        if change.token_id != self.token_id or change.condition_id != self.condition_id:
            raise InvalidBookState(
                [
                    ValidationIssue(
                        "token_mapping_invalid", "update token/market does not match book"
                    )
                ]
            )
        if change.sequence < self.last_sequence:
            return
        issues = validate_level(change.price_scaled, change.new_size_scaled, self.tick_size_scaled)
        if issues:
            self.valid = False
            raise InvalidBookState(issues)
        side = self.bids if change.side == "BUY" else self.asks
        if change.new_size_scaled == 0:
            side.pop(change.price_scaled, None)
        else:
            side[change.price_scaled] = change.new_size_scaled
        # Live 2026-07-31 frames sometimes moved the reported top without a
        # separate zero-size row for the exhausted former top.  The explicit
        # best_bid/best_ask assertions let us deterministically remove only
        # levels that the source declares can no longer exist.
        if change.reported_best_bid_scaled is not None:
            if change.reported_best_bid_scaled == 0:
                self.bids.clear()
            else:
                for price in tuple(self.bids.irange(minimum=change.reported_best_bid_scaled + 1)):
                    self.bids.pop(price, None)
        if change.reported_best_ask_scaled is not None:
            if change.reported_best_ask_scaled == POLYMARKET_PRICE_SCALE:
                self.asks.clear()
            else:
                for price in tuple(self.asks.irange(maximum=change.reported_best_ask_scaled - 1)):
                    self.asks.pop(price, None)
        issues = validate_cross(self.best_bid, self.best_ask)
        expected_bid = (
            None if change.reported_best_bid_scaled == 0 else change.reported_best_bid_scaled
        )
        expected_ask = (
            None
            if change.reported_best_ask_scaled == POLYMARKET_PRICE_SCALE
            else change.reported_best_ask_scaled
        )
        if change.reported_best_bid_scaled is not None and self.best_bid != expected_bid:
            issues.append(
                ValidationIssue(
                    "reported_top_mismatch",
                    f"local bid {self.best_bid} != reported {expected_bid}",
                )
            )
        if change.reported_best_ask_scaled is not None and self.best_ask != expected_ask:
            issues.append(
                ValidationIssue(
                    "reported_top_mismatch",
                    f"local ask {self.best_ask} != reported {expected_ask}",
                )
            )
        if issues:
            self.valid = False
            raise InvalidBookState(issues)
        self.last_sequence = change.sequence
        self.last_received_utc_ns = change.received_utc_ns
        self.source_hash = change.book_hash or self.source_hash
        self.valid = True

    def top(self) -> TopOfBook:
        bid = self.best_bid
        ask = self.best_ask
        spread = ask - bid if bid is not None and ask is not None else None
        midpoint = (ask + bid) // 2 if bid is not None and ask is not None else None
        return TopOfBook(
            condition_id=self.condition_id,
            token_id=self.token_id,
            sequence=self.last_sequence,
            received_utc_ns=self.last_received_utc_ns,
            best_bid_scaled=bid,
            best_ask_scaled=ask,
            spread_scaled=spread,
            midpoint_scaled=midpoint,
            bid_size_scaled=self.bids.get(bid) if bid is not None else None,
            ask_size_scaled=self.asks.get(ask) if ask is not None else None,
        )

    def snapshot_levels(self) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
        bids = tuple(
            BookLevel(price_scaled=price, size_scaled=size)
            for price, size in reversed(self.bids.items())
        )
        asks = tuple(
            BookLevel(price_scaled=price, size_scaled=size) for price, size in self.asks.items()
        )
        return bids, asks

    def depth_metrics(self) -> DepthMetrics:
        bids = list(reversed(self.bids.items()))
        asks = list(self.asks.items())
        best_bid = self.best_bid
        best_ask = self.best_ask

        def top_sum(levels: list[tuple[int, int]], count: int) -> int:
            return sum(size for _, size in levels[:count])

        def within(levels: list[tuple[int, int]], best: int | None, amount: int) -> int:
            if best is None:
                return 0
            return sum(size for price, size in levels if abs(price - best) <= amount)

        bid_depth = top_sum(bids, 5)
        ask_depth = top_sum(asks, 5)
        total = bid_depth + ask_depth
        imbalance = ((bid_depth - ask_depth) * 1_000_000 // total) if total else None
        return DepthMetrics(
            l1_bid_scaled=top_sum(bids, 1),
            l1_ask_scaled=top_sum(asks, 1),
            l5_bid_scaled=bid_depth,
            l5_ask_scaled=ask_depth,
            l10_bid_scaled=top_sum(bids, 10),
            l10_ask_scaled=top_sum(asks, 10),
            within_1c_bid_scaled=within(bids, best_bid, 10_000),
            within_1c_ask_scaled=within(asks, best_ask, 10_000),
            within_2c_bid_scaled=within(bids, best_bid, 20_000),
            within_2c_ask_scaled=within(asks, best_ask, 20_000),
            within_5c_bid_scaled=within(bids, best_bid, 50_000),
            within_5c_ask_scaled=within(asks, best_ask, 50_000),
            imbalance_ppm=imbalance,
        )
