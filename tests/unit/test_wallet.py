from __future__ import annotations

import pytest

from polymarket_bt.backtest.wallet import (
    InsufficientReplayCash,
    PendingCredit,
    ReplayWallet,
)


def test_pending_settlement_is_not_spendable_before_its_timestamp() -> None:
    wallet = ReplayWallet(100, start_utc_ns=10)
    wallet.debit(20, 80, source="entry")
    wallet.schedule_credit(PendingCredit(100, 125, "settlement"))

    assert wallet.peek_available_at(99) == 20
    assert wallet.peek_available_at(100) == 145
    assert wallet.available_cash_scaled == 20
    assert wallet.pending_cash_scaled == 125


def test_advancing_releases_due_cash_in_timestamp_order() -> None:
    wallet = ReplayWallet(0)
    wallet.schedule_credit(PendingCredit(20, 2, "second"))
    wallet.schedule_credit(PendingCredit(10, 1, "first"))

    assert wallet.advance_to(15) == 1
    assert wallet.advance_to(20) == 3
    assert [event.source for event in wallet.events] == ["first", "second"]


def test_wallet_rejects_overspend_instead_of_allowing_negative_cash() -> None:
    wallet = ReplayWallet(100)
    with pytest.raises(InsufficientReplayCash, match="required 101, available 100"):
        wallet.debit(1, 101, source="entry")
    assert wallet.available_cash_scaled == 100


def test_release_all_realises_final_pending_settlement() -> None:
    wallet = ReplayWallet(100)
    wallet.debit(1, 81, source="entry-plus-fee")
    wallet.schedule_credit(PendingCredit(50, 100, "winner"))
    assert wallet.total_equity_scaled == 119
    assert wallet.release_all() == 119


def test_late_scheduled_credit_is_released_at_current_ledger_time() -> None:
    wallet = ReplayWallet(10)
    wallet.advance_to(100)
    wallet.schedule_credit(PendingCredit(50, 5, "known-late"))
    assert wallet.available_cash_scaled == 15
    assert wallet.events[-1].timestamp_utc_ns == 100


def test_historical_cash_query_is_rejected_instead_of_leaking_future_proceeds() -> None:
    wallet = ReplayWallet(100)
    wallet.credit(20, 25, source="exit")

    with pytest.raises(ValueError, match="behind wallet time"):
        wallet.peek_available_at(19)


def test_scheduling_future_proceeds_does_not_advance_or_spend_them() -> None:
    wallet = ReplayWallet(100, start_utc_ns=10)
    wallet.schedule_credit(PendingCredit(50, 25, "exit"))

    assert wallet.now_utc_ns == 10
    assert wallet.available_cash_scaled == 100
    assert wallet.pending_cash_scaled == 25
    assert wallet.peek_available_at(49) == 100
    assert wallet.peek_available_at(50) == 125
