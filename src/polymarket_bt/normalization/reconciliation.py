from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from polymarket_bt.clock import decimal_to_scaled, parse_timestamp_ns
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.normalization.parser import load_json_decimal


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    status: str
    websocket_trade_id: str | None
    api_index: int | None
    details: dict[str, Any]


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
        exact: list[int] = []
        probable: list[int] = []
        for index, row in enumerate(api_rows):
            if index in used_api:
                continue
            if (
                str(row.get("conditionId")) != trade.condition_id
                or str(row.get("asset")) != trade.token_id
            ):
                continue
            price = decimal_to_scaled(
                str(row.get("price")), POLYMARKET_PRICE_SCALE, field="api_trade_price"
            )
            size = decimal_to_scaled(str(row.get("size")), SHARE_SIZE_SCALE, field="api_trade_size")
            timestamp = parse_timestamp_ns(row.get("timestamp"))
            transaction_hash = str(row.get("transactionHash") or "") or None
            core_match = price == trade.price_scaled and size == trade.size_scaled
            if core_match and transaction_hash and transaction_hash == trade.transaction_hash:
                exact.append(index)
            elif (
                core_match
                and timestamp is not None
                and trade.exchange_timestamp_ns is not None
                and abs(timestamp - trade.exchange_timestamp_ns) <= 2_000_000_000
            ):
                probable.append(index)
        candidates = exact or probable
        if candidates:
            selected = candidates[0]
            used_api.add(selected)
            results.append(
                ReconciliationResult(
                    status="exact_match" if exact else "probable_match",
                    websocket_trade_id=trade.trade_event_id,
                    api_index=selected,
                    details={"candidate_count": len(candidates)},
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
                        "transaction_hash": str(row.get("transactionHash") or ""),
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
