from __future__ import annotations

import os
import re
import uuid
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import orjson
import pyarrow.parquet as pq

from polymarket_bt.clock import utc_now_ns

SAFE_ID = re.compile(r"[^a-zA-Z0-9_.-]")
TOP_COLUMNS = [
    "condition_id",
    "token_id",
    "received_utc_ns",
    "sequence",
    "best_bid_scaled",
    "best_ask_scaled",
    "midpoint_scaled",
    "spread_scaled",
]


def _safe_id(condition_id: str) -> str:
    return SAFE_ID.sub("_", condition_id)


def series_path(storage_root: Path, condition_id: str) -> Path:
    return storage_root / "normalized" / "dashboard" / "markets" / f"{_safe_id(condition_id)}.json"


def _atomic_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    temporary.write_bytes(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS))
    os.replace(temporary, path)


class DashboardCacheWriter:
    """Materialize a small one-second chart cache from exact top-of-book rows."""

    def __init__(self, storage_root: Path) -> None:
        self.storage_root = storage_root
        self.index_path = storage_root / "normalized" / "dashboard-market-index.json"

    @staticmethod
    def _point(row: dict[str, Any]) -> dict[str, int | None]:
        return {
            "t_ms": int(row["received_utc_ns"]) // 1_000_000,
            "sequence": int(row["sequence"]),
            "bid": (
                int(row["best_bid_scaled"]) if row.get("best_bid_scaled") is not None else None
            ),
            "ask": (
                int(row["best_ask_scaled"]) if row.get("best_ask_scaled") is not None else None
            ),
            "mid": (
                int(row["midpoint_scaled"]) if row.get("midpoint_scaled") is not None else None
            ),
            "spread": int(row["spread_scaled"]) if row.get("spread_scaled") is not None else None,
        }

    @staticmethod
    def _merge_points(
        current: list[dict[str, Any]], incoming: Iterable[dict[str, Any]]
    ) -> list[dict[str, int | None]]:
        buckets: dict[int, dict[str, int | None]] = {}
        for raw in [*current, *incoming]:
            point_time = int(raw["t_ms"])
            point_sequence = int(raw.get("sequence", 0))
            point = {
                "t_ms": point_time,
                "sequence": point_sequence,
                "bid": int(raw["bid"]) if raw.get("bid") is not None else None,
                "ask": int(raw["ask"]) if raw.get("ask") is not None else None,
                "mid": int(raw["mid"]) if raw.get("mid") is not None else None,
                "spread": int(raw["spread"]) if raw.get("spread") is not None else None,
            }
            bucket = point_time // 1_000
            previous = buckets.get(bucket)
            if previous is None or (point_time, point_sequence) >= (
                int(previous["t_ms"] or 0),
                int(previous["sequence"] or 0),
            ):
                buckets[bucket] = point
        return [buckets[key] for key in sorted(buckets)]

    def update(self, rows: Iterable[dict[str, Any]]) -> dict[str, int]:
        grouped: dict[str, dict[str, list[dict[str, int | None]]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for row in rows:
            grouped[str(row["condition_id"])][str(row["token_id"])].append(self._point(row))
        if not grouped:
            return {"markets_updated": 0, "points_indexed": 0}

        try:
            index = orjson.loads(self.index_path.read_bytes())
        except (OSError, orjson.JSONDecodeError):
            index = {"schema_version": 1, "markets": {}}
        index_markets = index.get("markets")
        if not isinstance(index_markets, dict):
            index_markets = {}
            index["markets"] = index_markets

        total = 0
        for condition_id, token_rows in grouped.items():
            target = series_path(self.storage_root, condition_id)
            try:
                payload = orjson.loads(target.read_bytes())
            except (OSError, orjson.JSONDecodeError):
                payload = {"schema_version": 1, "condition_id": condition_id, "tokens": {}}
            tokens = payload.get("tokens")
            if not isinstance(tokens, dict):
                tokens = {}
                payload["tokens"] = tokens
            for token_id, points in token_rows.items():
                existing = tokens.get(token_id)
                tokens[token_id] = self._merge_points(
                    existing if isinstance(existing, list) else [], points
                )
            point_count = sum(len(points) for points in tokens.values() if isinstance(points, list))
            all_times = [
                int(point["t_ms"])
                for points in tokens.values()
                if isinstance(points, list)
                for point in points
            ]
            payload["updated_utc_ns"] = utc_now_ns()
            _atomic_json(target, payload)
            index_markets[condition_id] = {
                "point_count": point_count,
                "first_received_utc_ns": min(all_times, default=0) * 1_000_000,
                "last_received_utc_ns": max(all_times, default=0) * 1_000_000,
            }
            total += point_count
        index["updated_utc_ns"] = utc_now_ns()
        _atomic_json(self.index_path, index)
        return {"markets_updated": len(grouped), "points_indexed": total}

    def rebuild(self) -> dict[str, int]:
        files = sorted((self.storage_root / "normalized" / "top_of_book").rglob("*.parquet"))
        markets: set[str] = set()
        rows_read = 0
        for path in files:
            parquet = pq.ParquetFile(path)
            for batch in parquet.iter_batches(batch_size=100_000, columns=TOP_COLUMNS):
                rows = batch.to_pylist()
                rows_read += len(rows)
                markets.update(str(row["condition_id"]) for row in rows)
                self.update(rows)
        return {
            "files_processed": len(files),
            "rows_read": rows_read,
            "markets_indexed": len(markets),
        }
