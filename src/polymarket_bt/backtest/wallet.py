"""Timestamped collateral ledger for account-level replay.

An episode result is not cash.  Entry fills consume collateral at their arrival,
sell fills return it at their arrival, and shares held through settlement only
become spendable when resolution is observed.  This ledger keeps those states
separate so a compounded backtest cannot reuse locked collateral in an adjacent
market.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PendingCredit:
    available_utc_ns: int
    amount_scaled: int
    source: str

    def __post_init__(self) -> None:
        if self.available_utc_ns < 0:
            raise ValueError("credit timestamp must be non-negative")
        if self.amount_scaled < 0:
            raise ValueError("credit amount must be non-negative")


@dataclass(frozen=True, slots=True)
class WalletEvent:
    timestamp_utc_ns: int
    kind: str
    amount_scaled: int
    available_cash_scaled: int
    source: str


class InsufficientReplayCash(ValueError):
    """Raised when a simulated cash outflow exceeds available collateral."""


class ReplayWallet:
    """A deterministic cash ledger with future-dated settlement credits."""

    def __init__(self, starting_cash_scaled: int, *, start_utc_ns: int = 0) -> None:
        if starting_cash_scaled < 0:
            raise ValueError("starting cash must be non-negative")
        if start_utc_ns < 0:
            raise ValueError("start timestamp must be non-negative")
        self._available = starting_cash_scaled
        self._now = start_utc_ns
        self._pending: list[tuple[int, int, PendingCredit]] = []
        self._sequence = 0
        self.events: list[WalletEvent] = []

    @property
    def now_utc_ns(self) -> int:
        return self._now

    @property
    def available_cash_scaled(self) -> int:
        return self._available

    @property
    def pending_cash_scaled(self) -> int:
        return sum(item.amount_scaled for _, _, item in self._pending)

    @property
    def total_equity_scaled(self) -> int:
        return self._available + self.pending_cash_scaled

    def peek_available_at(self, timestamp_utc_ns: int) -> int:
        """Cash available by a future timestamp without mutating ledger time."""
        if timestamp_utc_ns < self._now:
            raise ValueError(
                f"cannot query cash at {timestamp_utc_ns} behind wallet time {self._now}"
            )
        return self._available + sum(
            item.amount_scaled
            for available, _, item in self._pending
            if available <= timestamp_utc_ns
        )

    def advance_to(self, timestamp_utc_ns: int) -> int:
        if timestamp_utc_ns < self._now:
            raise ValueError("wallet time cannot move backwards")
        while self._pending and self._pending[0][0] <= timestamp_utc_ns:
            available, _, credit = heapq.heappop(self._pending)
            self._available += credit.amount_scaled
            self.events.append(
                WalletEvent(
                    timestamp_utc_ns=available,
                    kind="credit_released",
                    amount_scaled=credit.amount_scaled,
                    available_cash_scaled=self._available,
                    source=credit.source,
                )
            )
        self._now = timestamp_utc_ns
        return self._available

    def debit(self, timestamp_utc_ns: int, amount_scaled: int, *, source: str) -> None:
        if amount_scaled < 0:
            raise ValueError("debit amount must be non-negative")
        self.advance_to(timestamp_utc_ns)
        if amount_scaled > self._available:
            raise InsufficientReplayCash(
                f"required {amount_scaled}, available {self._available} at {timestamp_utc_ns}"
            )
        self._available -= amount_scaled
        self.events.append(
            WalletEvent(
                timestamp_utc_ns=timestamp_utc_ns,
                kind="debit",
                amount_scaled=-amount_scaled,
                available_cash_scaled=self._available,
                source=source,
            )
        )

    def credit(self, timestamp_utc_ns: int, amount_scaled: int, *, source: str) -> None:
        if amount_scaled < 0:
            raise ValueError("credit amount must be non-negative")
        self.advance_to(timestamp_utc_ns)
        self._available += amount_scaled
        self.events.append(
            WalletEvent(
                timestamp_utc_ns=timestamp_utc_ns,
                kind="credit",
                amount_scaled=amount_scaled,
                available_cash_scaled=self._available,
                source=source,
            )
        )

    def schedule_credit(self, credit: PendingCredit) -> None:
        if credit.amount_scaled == 0:
            return
        if credit.available_utc_ns <= self._now:
            self._available += credit.amount_scaled
            self.events.append(
                WalletEvent(
                    timestamp_utc_ns=self._now,
                    kind="credit_released",
                    amount_scaled=credit.amount_scaled,
                    available_cash_scaled=self._available,
                    source=credit.source,
                )
            )
            return
        self._sequence += 1
        heapq.heappush(self._pending, (credit.available_utc_ns, self._sequence, credit))

    def release_all(self) -> int:
        if self._pending:
            self.advance_to(max(item[0] for item in self._pending))
        return self._available
