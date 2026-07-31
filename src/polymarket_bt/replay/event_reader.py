from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from polymarket_bt.constants import QualityState
from polymarket_bt.models.books import BookLevel, BookLevelChange, BookSnapshot
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.replay.event_clock import ReplayEvent
from polymarket_bt.replay.integrity import QualityInterval


def _rows(root: Path, dataset: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    directory = root / "normalized" / dataset
    if not directory.exists():
        return rows
    for path in sorted(directory.rglob("*.parquet")):
        # Read only the physical file. Dataset-style reads infer Hive partition
        # columns from the path and can shadow an exact source field.
        rows.extend(pq.ParquetFile(path).read().to_pylist())
    return rows


class EventReader:
    def __init__(self, storage_root: Path) -> None:
        self.storage_root = storage_root

    def read_events(self, *, condition_id: str | None = None) -> list[ReplayEvent]:
        events: list[ReplayEvent] = []
        level_rows = _rows(self.storage_root, "book_snapshot_levels")
        levels: dict[str, dict[str, list[BookLevel]]] = defaultdict(lambda: {"BUY": [], "SELL": []})
        for row in level_rows:
            if condition_id and row["condition_id"] != condition_id:
                continue
            levels[str(row["snapshot_id"])][str(row["side"])].append(
                BookLevel(
                    price_scaled=int(row["price_scaled"]), size_scaled=int(row["size_scaled"])
                )
            )
        for row in _rows(self.storage_root, "book_snapshots"):
            if condition_id and row["condition_id"] != condition_id:
                continue
            snapshot_levels = levels[str(row["snapshot_id"])]
            snapshot = BookSnapshot(
                **{
                    key: value
                    for key, value in row.items()
                    if key not in {"bid_level_count", "ask_level_count", "raw_file_id"}
                },
                bids=tuple(snapshot_levels["BUY"]),
                asks=tuple(snapshot_levels["SELL"]),
            )
            events.append(
                ReplayEvent(
                    event_type="book_snapshot",
                    exchange_timestamp_ns=snapshot.exchange_timestamp_ns,
                    received_utc_ns=snapshot.received_utc_ns,
                    received_monotonic_ns=snapshot.received_monotonic_ns,
                    connection_id=snapshot.connection_id,
                    sequence=snapshot.sequence,
                    parent_change_index=0,
                    source_priority=10,
                    payload=snapshot,
                )
            )
        for row in _rows(self.storage_root, "book_updates"):
            if condition_id and row["condition_id"] != condition_id:
                continue
            update = BookLevelChange.model_validate(row)
            events.append(
                ReplayEvent(
                    event_type="book_update",
                    exchange_timestamp_ns=update.exchange_timestamp_ns,
                    received_utc_ns=update.received_utc_ns,
                    received_monotonic_ns=update.received_monotonic_ns,
                    connection_id=update.connection_id,
                    sequence=update.sequence,
                    parent_change_index=update.change_index,
                    source_priority=20,
                    payload=update,
                )
            )
        for row in _rows(self.storage_root, "trades"):
            if condition_id and row["condition_id"] != condition_id:
                continue
            trade = TradeEvent.model_validate(row)
            events.append(
                ReplayEvent(
                    event_type="trade",
                    exchange_timestamp_ns=trade.exchange_timestamp_ns,
                    received_utc_ns=trade.received_utc_ns,
                    received_monotonic_ns=trade.received_monotonic_ns,
                    connection_id="trade-feed",
                    sequence=trade.sequence,
                    parent_change_index=0,
                    source_priority=30,
                    payload=trade,
                )
            )
        for row in _rows(self.storage_root, "btc_prices"):
            price = BtcPriceEvent.model_validate(row)
            events.append(
                ReplayEvent(
                    event_type="btc_price",
                    exchange_timestamp_ns=price.underlying_source_timestamp_ns,
                    received_utc_ns=price.received_utc_ns,
                    received_monotonic_ns=price.received_monotonic_ns,
                    connection_id=price.connection_id,
                    sequence=price.sequence,
                    parent_change_index=0,
                    source_priority=40,
                    payload=price,
                )
            )
        for row in _rows(self.storage_root, "market_resolutions"):
            if condition_id and row["condition_id"] != condition_id:
                continue
            events.append(
                ReplayEvent(
                    event_type="market_resolution",
                    exchange_timestamp_ns=row.get("exchange_timestamp_ns"),
                    received_utc_ns=int(row["received_utc_ns"]),
                    received_monotonic_ns=int(row["received_utc_ns"]),
                    connection_id="resolution-feed",
                    sequence=int(row["sequence"]),
                    parent_change_index=0,
                    source_priority=50,
                    payload=row,
                )
            )
        return events

    def quality_intervals(self, *, condition_id: str | None = None) -> list[QualityInterval]:
        intervals: list[QualityInterval] = []
        for row in _rows(self.storage_root, "data_quality_events"):
            if condition_id and row.get("condition_id") not in {None, condition_id}:
                continue
            replay_eligible = bool(row.get("replay_eligible", False))
            severity = str(row.get("severity", "error"))
            state = (
                QualityState.DEGRADED
                if replay_eligible
                else QualityState.UNRELIABLE
                if severity != "critical"
                else QualityState.EXCLUDED
            )
            intervals.append(
                QualityInterval(
                    start_utc_ns=int(row["start_utc_ns"]),
                    end_utc_ns=(int(row["end_utc_ns"]) if row.get("end_utc_ns") else None),
                    state=state,
                    category=str(row["category"]),
                    details={"details_json": row.get("details_json")},
                )
            )
        return intervals
