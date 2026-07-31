from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from polymarket_bt.clock import parse_timestamp_ns, scaled_to_decimal
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.normalization.parser import load_json_decimal


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    status: str
    websocket_trade_id: str | None
    api_index: int | None
    details: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _Candidate:
    api_index: int
    status: str
    priority: int
    matched_fields: tuple[str, ...]
    mismatched_fields: tuple[str, ...]
    details: dict[str, Any]


def _raw_decimal(value: object, field: str) -> tuple[Decimal | None, str | None]:
    if value is None or value == "":
        return None, f"{field} is missing"
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        return None, f"{field} is invalid: {exc}"
    if not parsed.is_finite():
        return None, f"{field} is non-finite"
    return parsed, None


def _raw_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _candidate(trade: TradeEvent, index: int, row: dict[str, Any]) -> _Candidate | None:
    price, price_error = _raw_decimal(row.get("price"), "price")
    size, size_error = _raw_decimal(row.get("size"), "size")
    try:
        timestamp = parse_timestamp_ns(row.get("timestamp"))
        timestamp_error = None if timestamp is not None else "timestamp is missing"
    except (TypeError, ValueError) as exc:
        timestamp = None
        timestamp_error = f"timestamp is invalid: {exc}"

    expected_price = scaled_to_decimal(trade.price_scaled, POLYMARKET_PRICE_SCALE)
    expected_size = (
        scaled_to_decimal(trade.size_scaled, SHARE_SIZE_SCALE)
        if trade.size_scaled is not None
        else None
    )
    transaction_hash = str(row.get("transactionHash") or "") or None
    transaction_match = bool(
        transaction_hash and trade.transaction_hash and transaction_hash == trade.transaction_hash
    )
    timestamp_delta_ns = (
        abs(timestamp - trade.exchange_timestamp_ns)
        if timestamp is not None and trade.exchange_timestamp_ns is not None
        else None
    )
    timestamp_match = timestamp_delta_ns is not None and timestamp_delta_ns <= 2_000_000_000
    price_match = price is not None and price == expected_price
    size_match = size is not None and expected_size is not None and size == expected_size

    matched = ["condition_id", "token_id"]
    if transaction_match:
        matched.append("transaction_hash")
    if timestamp_match:
        matched.append("timestamp")
    if price_match:
        matched.append("price")
    if size_match:
        matched.append("size")

    mismatched: list[str] = []
    if price is not None and not price_match:
        mismatched.append("price")
    if size is not None and expected_size is not None and not size_match:
        mismatched.append("size")
    if (
        transaction_hash is not None
        and trade.transaction_hash is not None
        and not transaction_match
    ):
        mismatched.append("transaction_hash")
    if timestamp_delta_ns is not None and not timestamp_match:
        mismatched.append("timestamp")

    # Transaction hashes identify an on-chain transaction, not necessarily one
    # unique trade row. Exact matches therefore also require exact price and size.
    if transaction_match and price_match and size_match:
        status = "exact_match"
        priority = 300
    elif timestamp_match and price_match and size_match:
        status = "probable_match"
        priority = 200
    elif transaction_match and (timestamp_match or price_match or size_match):
        status = "conflicting"
        priority = 100
    else:
        return None

    errors = [error for error in (price_error, size_error, timestamp_error) if error]
    details: dict[str, Any] = {
        "api_price_raw": _raw_string(row.get("price")),
        "api_size_raw": _raw_string(row.get("size")),
        "api_timestamp_raw": _raw_string(row.get("timestamp")),
        "api_transaction_hash": transaction_hash,
        "timestamp_delta_ns": timestamp_delta_ns,
    }
    if errors:
        details["comparison_errors"] = errors
    return _Candidate(
        api_index=index,
        status=status,
        priority=priority + len(matched),
        matched_fields=tuple(matched),
        mismatched_fields=tuple(mismatched),
        details=details,
    )


def reconcile_trades(
    websocket_trades: list[TradeEvent], data_api_raw: str
) -> list[ReconciliationResult]:
    payload = load_json_decimal(data_api_raw)
    if not isinstance(payload, list):
        raise ValueError("Data API trades response must be a list")
    api_rows = [row for row in payload if isinstance(row, dict)]
    used_api: set[int] = set()
    results: list[ReconciliationResult] = []
    for trade in websocket_trades:
        candidates: list[_Candidate] = []
        for index, row in enumerate(api_rows):
            if index in used_api:
                continue
            if (
                str(row.get("conditionId")) != trade.condition_id
                or str(row.get("asset")) != trade.token_id
            ):
                continue
            candidate = _candidate(trade, index, row)
            if candidate is not None:
                candidates.append(candidate)
        if candidates:
            selected = sorted(candidates, key=lambda item: (-item.priority, item.api_index))[0]
            used_api.add(selected.api_index)
            results.append(
                ReconciliationResult(
                    status=selected.status,
                    websocket_trade_id=trade.trade_event_id,
                    api_index=selected.api_index,
                    details={
                        **selected.details,
                        "candidate_count": len(candidates),
                        "matched_fields": list(selected.matched_fields),
                        "mismatched_fields": list(selected.mismatched_fields),
                    },
                )
            )
        else:
            results.append(
                ReconciliationResult(
                    status="websocket_only",
                    websocket_trade_id=trade.trade_event_id,
                    api_index=None,
                    details={},
                )
            )
    for index, row in enumerate(api_rows):
        if index not in used_api:
            results.append(
                ReconciliationResult(
                    status="api_only",
                    websocket_trade_id=None,
                    api_index=index,
                    details={
                        "condition_id": str(row.get("conditionId") or ""),
                        "token_id": str(row.get("asset") or ""),
                        "transaction_hash": str(row.get("transactionHash") or ""),
                        "api_price_raw": _raw_string(row.get("price")),
                        "api_size_raw": _raw_string(row.get("size")),
                        "api_timestamp_raw": _raw_string(row.get("timestamp")),
                    },
                )
            )
    return results


def reconciliation_report_json(results: list[ReconciliationResult]) -> str:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return json.dumps(
        {"counts": counts, "results": [asdict(result) for result in results]},
        indent=2,
        sort_keys=True,
    )
