from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

import pytest

from polymarket_bt.backtest.fastsim import EpisodeResult, SimulatedOrder
from polymarket_bt.dashboard.backtest_service import (
    BacktestRequest,
    BacktestService,
    RunOutcome,
    TapeCache,
    build_report,
)

MINUTE = 60_000_000_000
START = 1_785_600_000_000_000_000
USDC = 1_000_000


def request(**overrides: object) -> BacktestRequest:
    payload: dict[str, object] = {"entry_price": 0.8}
    payload.update(overrides)
    return BacktestRequest.model_validate(payload)


def test_dollar_stake_becomes_a_notional_order() -> None:
    params = request(stake_usd=2500).to_params()
    assert params.order_notional_scaled == 2_500_000_000
    assert params.order_shares_scaled is None


def test_entry_window_is_optional_and_defaults_to_the_whole_market() -> None:
    params = request().to_params()
    assert (params.entry_from_minute, params.entry_to_minute) == (0.0, 15.0)


def test_entry_window_is_honoured_when_supplied() -> None:
    params = request(entry_from_minute=12, entry_to_minute=14).to_params()
    assert (params.entry_from_minute, params.entry_to_minute) == (12.0, 14.0)


def test_no_stop_loss_means_hold_to_settlement() -> None:
    assert request().to_params().stop_loss_price_scaled is None


def test_stop_loss_above_the_entry_is_rejected() -> None:
    with pytest.raises(ValueError, match="stop-loss price must be below"):
        request(stop_loss_price=0.85)


def test_stop_loss_can_be_armed_from_a_minute() -> None:
    params = request(stop_loss_price=0.75, stop_loss_from_minute=13).to_params()
    assert params.stop_loss_from_minute == 13.0
    assert params.stop_loss_price_scaled == 750_000


def test_an_unarmed_stop_stays_live_for_the_whole_market() -> None:
    assert request(stop_loss_price=0.75).to_params().stop_loss_from_minute is None


def test_arming_a_minute_without_a_stop_loss_is_rejected() -> None:
    with pytest.raises(ValueError, match="stop-loss price is needed"):
        request(stop_loss_from_minute=13)


def test_fixed_is_the_default_staking_rule() -> None:
    assert request().sizing == "fixed"


def test_compound_staking_starts_from_the_stated_balance() -> None:
    assert request(sizing="compound", stake_usd=250).to_params().order_notional_scaled == 250 * USDC


def test_entry_ceiling_below_the_trigger_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot be below the entry price"):
        request(entry_limit_price=0.75)


def test_inverted_entry_window_is_rejected() -> None:
    with pytest.raises(ValueError, match="must end after it starts"):
        request(entry_from_minute=14, entry_to_minute=12)


def test_unknown_fields_are_rejected() -> None:
    """The form is a public endpoint; silently ignoring a field would hide typos."""
    with pytest.raises(ValueError):
        BacktestRequest.model_validate({"entry_price": 0.8, "leverage": 4})


def test_malformed_dates_are_rejected() -> None:
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        request(from_date="01/08/2026")


def test_date_window_covers_the_whole_closing_day() -> None:
    start, end = request(from_date="2026-08-01", to_date="2026-08-01").window_ns()
    assert start is not None and end is not None
    assert end - start == 86_400 * 1_000_000_000


def _order(requested: int, filled: int, **overrides: object) -> SimulatedOrder:
    payload: dict[str, object] = {
        "kind": "entry",
        "side": "BUY",
        "decision_utc_ns": START,
        "arrival_utc_ns": START,
        "decision_minute": 12.0,
        "trigger_price_scaled": 800_000,
        "requested_shares_scaled": requested,
        "filled_shares_scaled": filled,
        "notional_scaled": filled * 800_000 // 1_000_000,
        "fees_scaled": 0,
        "average_price_scaled": 800_000 if filled else None,
        "slippage_scaled": None,
        "levels_consumed": 1,
        "rejected_reason": None,
        "binding_constraint": "displayed_depth_haircut" if filled < requested else None,
    }
    payload.update(overrides)
    return SimulatedOrder(**payload)  # type: ignore[arg-type]


def _result(**overrides: object) -> EpisodeResult:
    payload: dict[str, object] = {
        "condition_id": "0xabc",
        "market_slug": "btc-updown-15m-1",
        "start_utc_ns": START,
        "end_utc_ns": START + 15 * MINUTE,
        "winner_outcome": "UP",
        "traded": True,
        "entered": True,
        "won": True,
        "outcome_side": "UP",
        "entry_price_scaled": 800_000,
        "entry_shares_scaled": 25_000_000,
        "entry_notional_scaled": 20_000_000,
        "net_pnl_scaled": 5_000_000,
        "gross_pnl_scaled": 5_000_000,
        "exit_reason": "settlement",
    }
    payload.update(overrides)
    return EpisodeResult(**payload)  # type: ignore[arg-type]


def _run(
    results: Sequence[EpisodeResult],
    *,
    sizing: str = "fixed",
    start_scaled: int = 0,
    ended_early: bool = False,
    selected: int | None = None,
) -> RunOutcome:
    return RunOutcome(
        results=list(results),
        sizing=sizing,  # type: ignore[arg-type]
        starting_balance_scaled=start_scaled,
        final_balance_scaled=start_scaled + sum(item.net_pnl_scaled for item in results),
        episodes_selected=len(results) if selected is None else selected,
        ended_early=ended_early,
    )


def test_report_reports_the_size_the_book_refused() -> None:
    """A stake the book cannot absorb must show up as a fill rate, not vanish."""
    results = [_result(orders=[_order(100_000_000, 25_000_000)])]
    report = build_report(
        request=request(stake_usd=80),
        params=request(stake_usd=80).to_params(),
        realism_name="base",
        run=_run(results),
        excluded=Counter(),
    )
    liquidity = report["liquidity"]
    assert liquidity["entry_shares_requested"] == 100.0
    assert liquidity["entry_shares_filled"] == 25.0
    assert liquidity["entry_fill_rate_pct"] == 25.0
    assert liquidity["entry_orders_partially_filled"] == 1
    assert liquidity["binding_constraints"] == {"displayed_depth_haircut": 1}


def test_report_separates_a_rejected_order_from_a_partial_one() -> None:
    results = [
        _result(
            entered=False,
            traded=False,
            entry_shares_scaled=0,
            entry_notional_scaled=0,
            orders=[_order(100_000_000, 0, rejected_reason="limit_price")],
        ),
        _result(orders=[_order(100_000_000, 40_000_000)]),
    ]
    liquidity = build_report(
        request=request(),
        params=request().to_params(),
        realism_name="base",
        run=_run(results),
        excluded=Counter(),
    )["liquidity"]
    assert liquidity["entry_orders_rejected"] == 1
    assert liquidity["entry_orders_partially_filled"] == 1
    assert liquidity["rejection_reasons"] == {"limit_price": 1}


def test_sequential_fill_rate_uses_parent_target_not_only_sent_children() -> None:
    result = _result(
        entry_shares_scaled=40_000_000,
        execution_target_shares_scaled=100_000_000,
        execution_unfilled_shares_scaled=60_000_000,
        execution_implementation_shortfall_scaled=1_000_000,
        execution_non_fill_penalty_scaled=2_000_000,
        execution_adverse_selection_penalty_scaled=3_000_000,
        execution_objective_scaled=6_000_000,
        orders=[
            _order(20_000_000, 20_000_000),
            _order(20_000_000, 20_000_000),
        ],
    )
    report = build_report(
        request=request(execution_policy="twap"),
        params=request(execution_policy="twap").to_params(),
        realism_name="base",
        run=_run([result]),
        excluded=Counter(),
    )

    assert report["summary"]["trades"] == 1
    assert report["summary"]["execution_objective_usd"] == 6.0
    assert report["liquidity"]["entry_fill_rate_pct"] == 40.0
    assert report["liquidity"]["entry_orders"] == 1
    assert report["liquidity"]["entry_child_orders"] == 2
    assert report["liquidity"]["entry_orders_partially_filled"] == 1
    assert report["episodes"][0]["shares_requested"] == 100.0
    assert report["episodes"][0]["shares_filled"] == 40.0


def test_report_converts_scaled_integers_to_dollars() -> None:
    report = build_report(
        request=request(),
        params=request().to_params(),
        realism_name="base",
        run=_run([_result(orders=[_order(25_000_000, 25_000_000)])]),
        excluded=Counter({"missing_book_data_for_one_side": 3}),
    )
    assert report["summary"]["net_profit_usd"] == 5.0
    assert report["summary"]["capital_deployed_usd"] == 20.0
    assert report["summary"]["return_on_capital_pct"] == 25.0
    assert report["excluded"] == {"missing_book_data_for_one_side": 3}
    assert report["equity_curve"] == [
        {
            "utc": "2026-08-01T16:00:00+00:00",
            "slug": "btc-updown-15m-1",
            "trade_usd": 5.0,
            "cumulative_usd": 5.0,
            "balance_usd": None,
        }
    ]


def test_untraded_markets_still_count_as_evaluated() -> None:
    report = build_report(
        request=request(),
        params=request().to_params(),
        realism_name="base",
        run=_run([_result(), _result(entered=False, traded=False, exit_reason="no_entry")]),
        excluded=Counter(),
    )
    assert report["summary"]["episodes_evaluated"] == 2
    assert report["summary"]["trades"] == 1
    assert report["summary"]["traded_rate_pct"] == 50.0


def test_fixed_staking_reports_no_balance() -> None:
    summary = build_report(
        request=request(),
        params=request().to_params(),
        realism_name="base",
        run=_run([_result()]),
        excluded=Counter(),
    )["summary"]
    assert summary["sizing"] == "fixed"
    assert summary["starting_balance_usd"] is None
    assert summary["total_return_pct"] is None


def test_compound_staking_reports_the_balance_path() -> None:
    """The balance, its return, and its drawdown are the point of compounding."""
    results = [
        _result(net_pnl_scaled=50 * USDC, gross_pnl_scaled=50 * USDC),
        _result(net_pnl_scaled=-75 * USDC, gross_pnl_scaled=-75 * USDC, won=False),
    ]
    summary = build_report(
        request=request(sizing="compound", stake_usd=100),
        params=request(sizing="compound", stake_usd=100).to_params(),
        realism_name="base",
        run=_run(results, sizing="compound", start_scaled=100 * USDC),
        excluded=Counter(),
    )["summary"]
    assert summary["starting_balance_usd"] == 100.0
    assert summary["final_balance_usd"] == 75.0
    assert summary["total_return_pct"] == -25.0
    # Peak was 150 after the first market, trough 75 after the second.
    assert summary["max_drawdown_pct"] == 50.0


def test_compound_curve_carries_the_running_balance() -> None:
    curve = build_report(
        request=request(sizing="compound", stake_usd=100),
        params=request(sizing="compound", stake_usd=100).to_params(),
        realism_name="base",
        run=_run([_result()], sizing="compound", start_scaled=100 * USDC),
        excluded=Counter(),
    )["equity_curve"]
    assert curve[0]["balance_usd"] == 105.0
    assert curve[0]["cumulative_usd"] == 5.0


def test_running_out_of_money_is_reported_rather_than_hidden() -> None:
    summary = build_report(
        request=request(sizing="compound"),
        params=request(sizing="compound").to_params(),
        realism_name="base",
        run=_run(
            [_result(net_pnl_scaled=-100 * USDC, gross_pnl_scaled=-100 * USDC, won=False)],
            sizing="compound",
            start_scaled=100 * USDC,
            ended_early=True,
            selected=40,
        ),
        excluded=Counter(),
    )["summary"]
    assert summary["ended_early"] is True
    assert summary["markets_never_reached"] == 39


def test_report_states_the_price_entries_actually_paid() -> None:
    """A 0.80 trigger that fills at 0.90 is the difference between win and loss."""
    summary = build_report(
        request=request(),
        params=request().to_params(),
        realism_name="base",
        run=_run(
            [
                _result(
                    entry_price_scaled=900_000,
                    entry_shares_scaled=100_000_000,
                    entry_notional_scaled=90_000_000,
                )
            ]
        ),
        excluded=Counter(),
    )["summary"]
    assert summary["average_entry_price"] == 0.9
    assert summary["entry_price_above_trigger"] == 0.1
    assert summary["break_even_win_rate_pct"] == 90.0


def test_report_contrasts_the_stake_on_winners_and_losers() -> None:
    summary = build_report(
        request=request(),
        params=request().to_params(),
        realism_name="base",
        run=_run(
            [
                _result(entry_notional_scaled=20 * USDC, net_pnl_scaled=5 * USDC),
                _result(entry_notional_scaled=80 * USDC, net_pnl_scaled=-40 * USDC, won=False),
            ]
        ),
        excluded=Counter(),
    )["summary"]
    assert summary["average_stake_on_wins_usd"] == 20.0
    assert summary["average_stake_on_losses_usd"] == 80.0


class _StubSimulator:
    """Records the notional each market was handed, and returns a fixed P&L."""

    seen: ClassVar[list[int]] = []

    def __init__(self, params: object, realism: object, *, seed: int = 0) -> None:
        self.params = params

    def run(
        self,
        tape: object,
        available_cash_scaled: object | None = None,
    ) -> EpisodeResult:
        episode = tape
        notional = self.params.order_notional_scaled  # type: ignore[attr-defined]
        _StubSimulator.seen.append(notional)
        arrival = episode.start_utc_ns + MINUTE  # type: ignore[attr-defined]
        return _result(
            condition_id=episode.condition_id,  # type: ignore[attr-defined]
            start_utc_ns=episode.start_utc_ns,  # type: ignore[attr-defined]
            end_utc_ns=episode.end_utc_ns,  # type: ignore[attr-defined]
            entry_notional_scaled=notional,
            net_pnl_scaled=notional // 2,
            gross_pnl_scaled=notional // 2,
            settled_shares_scaled=notional * 3 // 2,
            settlement_payout_scaled=notional * 3 // 2,
            orders=[
                _order(
                    notional,
                    notional,
                    decision_utc_ns=arrival,
                    arrival_utc_ns=arrival,
                    notional_scaled=notional,
                )
            ],
        )


def test_compounding_restakes_the_running_balance(monkeypatch: pytest.MonkeyPatch) -> None:
    import polymarket_bt.dashboard.backtest_service as service

    _StubSimulator.seen = []
    monkeypatch.setattr(service, "ThresholdHoldSimulator", _StubSimulator)
    outcome = service.run_episodes(
        params=request(stake_usd=100).to_params(),
        realism=None,  # type: ignore[arg-type]
        episodes=[_StubEpisode("0x1"), _StubEpisode("0x2"), _StubEpisode("0x3")],  # type: ignore[list-item]
        load=lambda episode: episode,  # type: ignore[return-value]
        seed=1,
        sizing="compound",
        starting_balance_scaled=100 * USDC,
    )
    # Each market wins half its stake, so the next one stakes 1.5x the last.
    assert _StubSimulator.seen == [100 * USDC, 150 * USDC, 225 * USDC]
    assert outcome.final_balance_scaled == 337_500_000
    assert outcome.ended_early is False


def test_fixed_staking_keeps_the_same_size_after_a_win(monkeypatch: pytest.MonkeyPatch) -> None:
    import polymarket_bt.dashboard.backtest_service as service

    _StubSimulator.seen = []
    monkeypatch.setattr(service, "ThresholdHoldSimulator", _StubSimulator)
    service.run_episodes(
        params=request(stake_usd=100).to_params(),
        realism=None,  # type: ignore[arg-type]
        episodes=[_StubEpisode("0x1"), _StubEpisode("0x2")],  # type: ignore[list-item]
        load=lambda episode: episode,  # type: ignore[return-value]
        seed=1,
        sizing="fixed",
        starting_balance_scaled=100 * USDC,
    )
    assert _StubSimulator.seen == [100 * USDC, 100 * USDC]


def test_a_broke_balance_stops_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    import polymarket_bt.dashboard.backtest_service as service

    class _Ruin(_StubSimulator):
        def run(
            self,
            tape: object,
            available_cash_scaled: object | None = None,
        ) -> EpisodeResult:
            episode = tape
            notional = self.params.order_notional_scaled  # type: ignore[attr-defined]
            _StubSimulator.seen.append(notional)
            arrival = episode.start_utc_ns + MINUTE  # type: ignore[attr-defined]
            return _result(
                condition_id=episode.condition_id,  # type: ignore[attr-defined]
                start_utc_ns=episode.start_utc_ns,  # type: ignore[attr-defined]
                end_utc_ns=episode.end_utc_ns,  # type: ignore[attr-defined]
                entry_notional_scaled=notional,
                net_pnl_scaled=-notional,
                gross_pnl_scaled=-notional,
                won=False,
                settled_shares_scaled=notional,
                settlement_payout_scaled=0,
                orders=[
                    _order(
                        notional,
                        notional,
                        decision_utc_ns=arrival,
                        arrival_utc_ns=arrival,
                        notional_scaled=notional,
                    )
                ],
            )

    _StubSimulator.seen = []
    monkeypatch.setattr(service, "ThresholdHoldSimulator", _Ruin)
    outcome = service.run_episodes(
        params=request(stake_usd=100).to_params(),
        realism=None,  # type: ignore[arg-type]
        episodes=[_StubEpisode(f"0x{index}") for index in range(5)],  # type: ignore[list-item]
        load=lambda episode: episode,  # type: ignore[return-value]
        seed=1,
        sizing="compound",
        starting_balance_scaled=100 * USDC,
    )
    assert _StubSimulator.seen == [100 * USDC]
    assert outcome.ended_early is True
    assert outcome.episodes_selected - len(outcome.results) == 4


def test_wallet_cashflows_use_fill_and_resolution_timestamps() -> None:
    import polymarket_bt.dashboard.backtest_service as service

    class _LedgerEpisode:
        condition_id = "0xledger"
        end_utc_ns = START + 15 * MINUTE
        resolution_received_utc_ns = end_utc_ns + MINUTE

    buy_ns = START + MINUTE
    sell_ns = START + 2 * MINUTE
    result = _result(
        condition_id="0xledger",
        fees_scaled=3 * USDC,
        gross_pnl_scaled=10 * USDC,
        net_pnl_scaled=7 * USDC,
        settled_shares_scaled=40 * USDC,
        settlement_payout_scaled=40 * USDC,
        orders=[
            _order(
                100 * USDC,
                100 * USDC,
                arrival_utc_ns=buy_ns,
                notional_scaled=80 * USDC,
                fees_scaled=2 * USDC,
            ),
            _order(
                50 * USDC,
                50 * USDC,
                kind="stop_loss",
                side="SELL",
                arrival_utc_ns=sell_ns,
                notional_scaled=50 * USDC,
                fees_scaled=USDC,
            ),
        ],
    )
    wallet = service.ReplayWallet(100 * USDC, start_utc_ns=START)
    service._apply_result_to_wallet(wallet, _LedgerEpisode(), result)  # type: ignore[arg-type]

    # Only the entry has happened at ledger time; neither future credit was
    # pulled forward merely because this episode was simulated first.
    assert wallet.now_utc_ns == buy_ns
    assert wallet.available_cash_scaled == 18 * USDC
    assert wallet.pending_cash_scaled == 89 * USDC
    assert wallet.total_equity_scaled == 107 * USDC
    assert wallet.peek_available_at(sell_ns - 1) == 18 * USDC
    assert wallet.peek_available_at(sell_ns) == 67 * USDC
    assert wallet.peek_available_at(_LedgerEpisode.resolution_received_utc_ns) == 107 * USDC


def test_overlapping_episode_cannot_spend_a_future_exit_credit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import polymarket_bt.dashboard.backtest_service as service

    class _OverlapEpisode:
        def __init__(self, condition_id: str, start_ns: int) -> None:
            self.condition_id = condition_id
            self.start_utc_ns = start_ns
            self.end_utc_ns = start_ns + 15 * MINUTE
            self.resolution_received_utc_ns = None

    class _OverlapSimulator:
        seen: ClassVar[list[tuple[str, int, int]]] = []

        def __init__(self, params: object, realism: object, *, seed: int = 0) -> None:
            self.params = params

        def run(
            self,
            tape: object,
            available_cash_scaled: object | None = None,
        ) -> EpisodeResult:
            notional = self.params.order_notional_scaled  # type: ignore[attr-defined]
            arrival = tape.start_utc_ns + MINUTE  # type: ignore[attr-defined]
            available = available_cash_scaled(arrival)  # type: ignore[operator]
            self.seen.append((tape.condition_id, notional, available))  # type: ignore[attr-defined]
            if available < notional:
                return _result(
                    condition_id=tape.condition_id,  # type: ignore[attr-defined]
                    start_utc_ns=tape.start_utc_ns,  # type: ignore[attr-defined]
                    end_utc_ns=tape.end_utc_ns,  # type: ignore[attr-defined]
                    entered=False,
                    traded=False,
                    won=None,
                    exit_reason="no_entry",
                    entry_notional_scaled=0,
                    gross_pnl_scaled=0,
                    net_pnl_scaled=0,
                    orders=[
                        _order(
                            notional,
                            0,
                            arrival_utc_ns=arrival,
                            rejected_reason="insufficient_available_cash",
                        )
                    ],
                )
            exit_ns = tape.start_utc_ns + 14 * MINUTE  # type: ignore[attr-defined]
            return _result(
                condition_id=tape.condition_id,  # type: ignore[attr-defined]
                start_utc_ns=tape.start_utc_ns,  # type: ignore[attr-defined]
                end_utc_ns=tape.end_utc_ns,  # type: ignore[attr-defined]
                entry_notional_scaled=notional,
                exit_notional_scaled=notional + 10 * USDC,
                gross_pnl_scaled=10 * USDC,
                net_pnl_scaled=10 * USDC,
                settled_shares_scaled=0,
                settlement_payout_scaled=0,
                orders=[
                    _order(
                        notional,
                        notional,
                        arrival_utc_ns=arrival,
                        notional_scaled=notional,
                    ),
                    _order(
                        notional,
                        notional,
                        kind="stop_loss",
                        side="SELL",
                        arrival_utc_ns=exit_ns,
                        notional_scaled=notional + 10 * USDC,
                    ),
                ],
            )

    first = _OverlapEpisode("0xfirst", START)
    second = _OverlapEpisode("0xsecond", START + 5 * MINUTE)
    _OverlapSimulator.seen = []
    monkeypatch.setattr(service, "ThresholdHoldSimulator", _OverlapSimulator)
    outcome = service.run_episodes(
        params=request(stake_usd=100).to_params(),
        realism=None,  # type: ignore[arg-type]
        # Deliberately reversed: the runner owns chronological ordering.
        episodes=[second, first],  # type: ignore[list-item]
        load=lambda episode: episode,  # type: ignore[return-value]
        seed=1,
        sizing="compound",
        starting_balance_scaled=100 * USDC,
    )

    assert _OverlapSimulator.seen == [
        ("0xfirst", 100 * USDC, 100 * USDC),
        # The first exit is at minute 14; it cannot fund an entry at minute 6.
        ("0xsecond", 110 * USDC, 0),
    ]
    assert [result.entered for result in outcome.results] == [True, False]
    assert outcome.final_balance_scaled == 110 * USDC
    assert outcome.available_balance_scaled == 110 * USDC
    assert outcome.locked_balance_scaled == 0


def test_final_compound_balance_keeps_unreleased_settlement_locked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import polymarket_bt.dashboard.backtest_service as service

    _StubSimulator.seen = []
    monkeypatch.setattr(service, "ThresholdHoldSimulator", _StubSimulator)
    episode = _StubEpisode("0xlocked")
    outcome = service.run_episodes(
        params=request(stake_usd=100).to_params(),
        realism=None,  # type: ignore[arg-type]
        episodes=[episode],  # type: ignore[list-item]
        load=lambda item: item,  # type: ignore[return-value]
        seed=1,
        sizing="compound",
        starting_balance_scaled=100 * USDC,
    )

    assert outcome.final_balance_scaled == 150 * USDC
    assert outcome.available_balance_scaled == 0
    assert outcome.locked_balance_scaled == 150 * USDC
    assert not any(event.kind == "credit_released" for event in outcome.wallet_events)


def test_wallet_rejects_result_that_would_create_unreported_cash() -> None:
    import polymarket_bt.dashboard.backtest_service as service

    class _LedgerEpisode:
        condition_id = "0xbad"
        end_utc_ns = START + 15 * MINUTE
        resolution_received_utc_ns = None

    result = _result(
        condition_id="0xbad",
        gross_pnl_scaled=10 * USDC,
        # Cashflows below make $10, so claiming $9 must fail reconciliation.
        net_pnl_scaled=9 * USDC,
        settled_shares_scaled=0,
        orders=[
            _order(100 * USDC, 100 * USDC, notional_scaled=80 * USDC),
            _order(
                100 * USDC,
                100 * USDC,
                kind="stop_loss",
                side="SELL",
                arrival_utc_ns=START + MINUTE,
                notional_scaled=90 * USDC,
            ),
        ],
    )
    wallet = service.ReplayWallet(100 * USDC, start_utc_ns=START)

    with pytest.raises(ValueError, match="wallet cashflows do not reconcile"):
        service._apply_result_to_wallet(wallet, _LedgerEpisode(), result)  # type: ignore[arg-type]


def test_missing_workspace_reports_unavailable_rather_than_failing(tmp_path: Path) -> None:
    service = BacktestService(tmp_path / "absent")
    meta = service.meta()
    assert meta["available"] is False
    assert meta["episodes_eligible"] == 0
    with pytest.raises(LookupError):
        service.submit({"entry_price": 0.8})


def test_dashboard_accepts_only_complete_full_depth_tape_publications(tmp_path: Path) -> None:
    service = BacktestService(tmp_path)
    episode = _StubEpisode("0xtape")
    directory = tmp_path / "tapes"
    directory.mkdir()
    (directory / "0xtape.parquet").touch()
    (directory / "0xtape.trades.parquet").touch()
    metadata = directory / "0xtape.meta.json"

    metadata.write_text(
        json.dumps({"tape_version": "episode-tape-v1", "depth": 10, "condition_id": "0xtape"}),
        encoding="utf-8",
    )
    assert not service._tape_ready(episode)  # type: ignore[arg-type]

    metadata.write_text(
        json.dumps({"tape_version": "episode-tape-v2", "depth": None, "condition_id": "0xtape"}),
        encoding="utf-8",
    )
    assert service._tape_ready(episode)  # type: ignore[arg-type]


class _FakeTape:
    resident_bytes = 100

    def release_depth_cache(self) -> None:
        pass


class _CountingCache(TapeCache):
    """Cache with the parquet read replaced, so eviction can be observed directly."""

    loads = 0

    def _load(self, episode: object) -> object:  # type: ignore[override]
        _CountingCache.loads += 1
        return _FakeTape()


class _StubEpisode:
    sequence = 0

    def __init__(self, condition_id: str) -> None:
        self.condition_id = condition_id
        _StubEpisode.sequence += 1
        self.start_utc_ns = START + _StubEpisode.sequence * 15 * MINUTE
        self.end_utc_ns = self.start_utc_ns + 15 * MINUTE
        self.resolution_received_utc_ns = self.end_utc_ns + 30_000_000_000


def test_tape_cache_evicts_the_least_recently_used_entry(tmp_path: Path) -> None:
    _CountingCache.loads = 0
    cache = _CountingCache(tmp_path, budget_bytes=250)
    episodes = [_StubEpisode(f"0x{index}") for index in range(4)]
    for episode in episodes:
        cache.get(episode)  # type: ignore[arg-type]
    assert cache.stats()["bytes"] <= 250
    assert cache.stats()["episodes"] == 2
    # The oldest tape was dropped, so revisiting it costs another read.
    cache.get(episodes[0])  # type: ignore[arg-type]
    assert _CountingCache.loads == 5
    cache.clear()
    assert cache.stats()["episodes"] == 0
    assert cache.stats()["bytes"] == 0


def test_tape_cache_serves_a_repeated_episode_without_reloading(tmp_path: Path) -> None:
    _CountingCache.loads = 0
    cache = _CountingCache(tmp_path, budget_bytes=10_000)
    episode = _StubEpisode("0xrepeat")
    for _ in range(3):
        cache.get(episode)  # type: ignore[arg-type]
    assert _CountingCache.loads == 1
    assert cache.stats()["hits"] == 2
