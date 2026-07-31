from __future__ import annotations

from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.normalization.reconciliation import reconcile_trades


def _trade(**overrides: object) -> TradeEvent:
    values: dict[str, object] = {
        "trade_event_id": "ws-trade-1",
        "sequence": 1,
        "condition_id": "condition",
        "token_id": "token",
        "outcome": "UP",
        "exchange_timestamp_ns": 1_785_529_838_000_000_000,
        "received_utc_ns": 1_785_529_838_100_000_000,
        "received_monotonic_ns": 100,
        "price_scaled": 956_000,
        "size_scaled": 5_000_000,
        "notional_scaled": 4_780_000,
        "reported_side": "BUY",
        "fee_rate_bps_scaled": None,
        "transaction_hash": "0xhash",
        "trade_id_when_available": None,
        "source": "clob_market_ws",
    }
    values.update(overrides)
    return TradeEvent.model_validate(values)


def test_reconciliation_matches_exact_decimals() -> None:
    raw = """[
      {
        "conditionId": "condition",
        "asset": "token",
        "price": 0.956,
        "size": 5,
        "timestamp": 1785529838,
        "transactionHash": "0xhash"
      }
    ]"""

    results = reconcile_trades([_trade()], raw)

    assert len(results) == 1
    assert results[0].status == "exact_match"
    assert results[0].details["mismatched_fields"] == []


def test_reconciliation_preserves_subscale_api_price_as_conflict() -> None:
    raw = """[
      {
        "conditionId": "condition",
        "asset": "token",
        "price": 0.9560913706,
        "size": 5,
        "timestamp": 1785529838,
        "transactionHash": "0xhash"
      }
    ]"""

    results = reconcile_trades([_trade()], raw)

    assert len(results) == 1
    assert results[0].status == "conflicting"
    assert results[0].details["api_price_raw"] == "0.9560913706"
    assert results[0].details["mismatched_fields"] == ["price"]


def test_reconciliation_does_not_abort_on_malformed_api_numeric_field() -> None:
    raw = """[
      {
        "conditionId": "condition",
        "asset": "token",
        "price": "not-a-decimal",
        "size": 5,
        "timestamp": 1785529838,
        "transactionHash": "0xdifferent"
      }
    ]"""

    results = reconcile_trades([_trade()], raw)

    assert [result.status for result in results] == ["websocket_only", "api_only"]
    assert results[1].details["api_price_raw"] == "not-a-decimal"
