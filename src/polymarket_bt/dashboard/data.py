from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import httpx

from polymarket_bt.clock import decimal_to_scaled, utc_now_ns
from polymarket_bt.constants import (
    BTC_PRICE_SCALE,
    POLYMARKET_PRICE_SCALE,
    SHARE_SIZE_SCALE,
)
from polymarket_bt.dashboard.cache import series_path
from polymarket_bt.models.markets import MarketRecord

COVERAGE_EDGE_TOLERANCE_NS = 60 * 1_000_000_000


def _quote_path(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def _parquet_source(files: list[Path]) -> str:
    paths = ",".join(_quote_path(path) for path in files)
    return f"read_parquet([{paths}], union_by_name=true, hive_partitioning=false)"


def _status(market: MarketRecord, now_ns: int) -> str:
    if now_ns < market.market_start_utc_ns:
        return "upcoming"
    if now_ns < market.market_end_utc_ns:
        return "recording"
    if market.resolved_utc_ns or market.winning_outcome:
        return "resolved"
    return "complete"


def _scaled_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None


class _ResponseCache:
    def __init__(self, maximum: int = 256) -> None:
        self.maximum = maximum
        self._lock = threading.Lock()
        self._values: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def get(self, key: str, ttl_seconds: float) -> Any | None:
        now = time.monotonic()
        with self._lock:
            item = self._values.get(key)
            if item is None or now - item[0] > ttl_seconds:
                if item is not None:
                    self._values.pop(key, None)
                return None
            self._values.move_to_end(key)
            return item[1]

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._values[key] = (time.monotonic(), value)
            self._values.move_to_end(key)
            while len(self._values) > self.maximum:
                self._values.popitem(last=False)


class DashboardData:
    """Read-only facade over the registry, normalized Parquet, and live public books."""

    def __init__(self, storage_root: Path) -> None:
        self.storage_root = storage_root.resolve()
        self.normalized_root = self.storage_root / "normalized"
        self.dashboard_index_path = self.normalized_root / "dashboard-market-index.json"
        self.registry_path = self.storage_root / "state" / "market-registry.sqlite"
        self.status_path = self.storage_root / "state" / "status.json"
        self._cache = _ResponseCache()

    def _registry(self) -> sqlite3.Connection:
        if not self.registry_path.exists():
            raise FileNotFoundError(f"market registry is missing: {self.registry_path}")
        uri = f"file:{self.registry_path.as_posix()}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        return connection

    def market_by_slug(self, slug: str) -> MarketRecord:
        with self._registry() as connection:
            row = connection.execute(
                "SELECT market_json FROM markets WHERE market_slug = ? LIMIT 1", (slug,)
            ).fetchone()
        if row is None:
            raise LookupError(f"unknown market slug: {slug}")
        return MarketRecord.model_validate_json(row["market_json"])

    def _top_files(self) -> list[Path]:
        root = self.normalized_root / "top_of_book"
        return sorted(root.rglob("*.parquet")) if root.exists() else []

    def _dashboard_index(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.dashboard_index_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        markets = payload.get("markets", {})
        return markets if isinstance(markets, dict) else {}

    @staticmethod
    def _market_data_status(market: MarketRecord, state: str, index_entry: object) -> str:
        if state == "upcoming":
            return "pending"
        if state == "recording":
            return "recording"
        if not isinstance(index_entry, dict):
            return "pending"
        token_index = index_entry.get("tokens")
        if not isinstance(token_index, dict):
            # Schema-v1 indexes did not carry per-token coverage and must not
            # make a partially normalized market look complete.
            return "partial"

        summaries: list[dict[str, Any]] = []
        for token_id in (market.up_token_id, market.down_token_id):
            summary = token_index.get(token_id)
            if isinstance(summary, dict):
                summaries.append(summary)
        if len(summaries) != 2:
            return "partial" if summaries else "pending"

        first_times = [int(summary.get("first_received_utc_ns") or 0) for summary in summaries]
        last_times = [int(summary.get("last_received_utc_ns") or 0) for summary in summaries]
        overlaps = [
            last_time >= market.market_start_utc_ns and first_time <= market.market_end_utc_ns
            for first_time, last_time in zip(first_times, last_times, strict=True)
        ]
        if not any(overlaps):
            return "pending"
        complete_edges = all(
            first_time <= market.market_start_utc_ns + COVERAGE_EDGE_TOLERANCE_NS
            and last_time >= market.market_end_utc_ns - COVERAGE_EDGE_TOLERANCE_NS
            for first_time, last_time in zip(first_times, last_times, strict=True)
        )
        return "ready" if complete_edges else "partial"

    def markets(self, *, limit: int = 500) -> dict[str, Any]:
        limit = max(1, min(limit, 2_000))
        with self._registry() as connection:
            rows = connection.execute(
                "SELECT market_json FROM markets ORDER BY market_start_utc_ns DESC LIMIT ?",
                (limit,),
            ).fetchall()
        dashboard_index = self._dashboard_index()
        now = utc_now_ns()
        markets: list[dict[str, Any]] = []
        for row in rows:
            market = MarketRecord.model_validate_json(row["market_json"])
            state = _status(market, now)
            data_status = self._market_data_status(
                market, state, dashboard_index.get(market.condition_id)
            )
            markets.append(
                {
                    "slug": market.market_slug,
                    "condition_id": market.condition_id,
                    "question": market.question,
                    "start_ms": market.market_start_utc_ns // 1_000_000,
                    "end_ms": market.market_end_utc_ns // 1_000_000,
                    "duration_minutes": round(
                        (market.market_end_utc_ns - market.market_start_utc_ns) / 60_000_000_000,
                        2,
                    ),
                    "status": state,
                    "data_status": data_status,
                    "up_token_id": market.up_token_id,
                    "down_token_id": market.down_token_id,
                    "winning_outcome": market.winning_outcome,
                    "match_score": market.match_score,
                }
            )
        return {
            "generated_at_ms": now // 1_000_000,
            "count": len(markets),
            "ready_count": sum(item["data_status"] == "ready" for item in markets),
            "partial_count": sum(item["data_status"] == "partial" for item in markets),
            "pending_count": sum(item["data_status"] == "pending" for item in markets),
            "markets": markets,
        }

    def market_payload(self, market: MarketRecord) -> dict[str, Any]:
        now = utc_now_ns()
        return {
            "slug": market.market_slug,
            "condition_id": market.condition_id,
            "question": market.question,
            "description": market.description,
            "start_ms": market.market_start_utc_ns // 1_000_000,
            "end_ms": market.market_end_utc_ns // 1_000_000,
            "status": _status(market, now),
            "active": market.active,
            "closed": market.closed,
            "accepting_orders": market.accepting_orders,
            "tick_size_scaled": market.tick_size_scaled,
            "minimum_order_size_scaled": market.minimum_order_size_scaled,
            "up_token_id": market.up_token_id,
            "down_token_id": market.down_token_id,
            "winning_outcome": market.winning_outcome,
            "resolution_source": market.resolution_source,
            "match_score": market.match_score,
        }

    @staticmethod
    def _hours(start_ns: int, end_ns: int) -> list[datetime]:
        start = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=UTC).replace(
            minute=0, second=0, microsecond=0
        )
        end = datetime.fromtimestamp(end_ns / 1_000_000_000, tz=UTC).replace(
            minute=0, second=0, microsecond=0
        )
        result: list[datetime] = []
        cursor = start
        while cursor <= end:
            result.append(cursor)
            cursor += timedelta(hours=1)
        return result

    def _files(self, dataset: str, start_ns: int, end_ns: int) -> list[Path]:
        root = self.normalized_root / dataset
        if not root.exists():
            return []
        files: list[Path] = []
        for hour in self._hours(start_ns, end_ns):
            partition = root / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}"
            if partition.exists():
                files.extend(partition.glob("*.parquet"))
        return sorted(files)

    def _btc_files(self, start_ns: int, end_ns: int) -> list[Path]:
        root = self.normalized_root / "btc_prices"
        if not root.exists():
            return []
        files: list[Path] = []
        for source in root.glob("source=*"):
            for hour in self._hours(start_ns, end_ns):
                partition = source / f"date={hour:%Y-%m-%d}" / f"hour={hour:%H}"
                if partition.exists():
                    files.extend(partition.glob("*.parquet"))
        return sorted(files)

    @staticmethod
    def _downsample_top(
        rows: list[tuple[Any, ...]],
        market: MarketRecord,
        max_points: int,
    ) -> dict[str, list[dict[str, int | None]]]:
        duration = max(1, market.market_end_utc_ns - market.market_start_utc_ns)
        width = max(1, duration // max_points)
        buckets: dict[str, dict[int, dict[str, int | None]]] = {"UP": {}, "DOWN": {}}
        outcome_for = market.token_outcomes
        for row in rows:
            token_id = str(row[0])
            outcome = outcome_for.get(token_id)
            if outcome is None:
                continue
            received_ns = int(row[1])
            bucket = max(0, (received_ns - market.market_start_utc_ns) // width)
            buckets[outcome][bucket] = {
                "t_ms": received_ns // 1_000_000,
                "bid": _scaled_or_none(row[3]),
                "ask": _scaled_or_none(row[4]),
                "mid": _scaled_or_none(row[5]),
                "spread": _scaled_or_none(row[6]),
            }
        return {
            outcome: [values[index] for index in sorted(values)]
            for outcome, values in buckets.items()
        }

    @staticmethod
    def _downsample_btc(
        rows: list[tuple[Any, ...]], start_ns: int, end_ns: int, max_points: int
    ) -> dict[str, list[dict[str, int]]]:
        width = max(1, max(1, end_ns - start_ns) // max_points)
        buckets: dict[str, dict[int, dict[str, int]]] = {}
        for source_raw, received_raw, price_raw in rows:
            source = str(source_raw)
            received_ns = int(received_raw)
            bucket = max(0, (received_ns - start_ns) // width)
            buckets.setdefault(source, {})[bucket] = {
                "t_ms": received_ns // 1_000_000,
                "price": int(price_raw),
            }
        return {
            source: [values[index] for index in sorted(values)]
            for source, values in buckets.items()
        }

    @staticmethod
    def _series_coverage(
        market: MarketRecord,
        state: str,
        series: dict[str, list[dict[str, int | None]]],
    ) -> dict[str, int | str | None]:
        outcome_times = {
            outcome: [
                int(cast(int, point["t_ms"]))
                for point in series.get(outcome, [])
                if point.get("t_ms") is not None
            ]
            for outcome in ("UP", "DOWN")
        }
        all_times = [value for values in outcome_times.values() for value in values]
        first_ms = min(all_times, default=None)
        last_ms = max(all_times, default=None)
        common_first_ms: int | None = None
        common_last_ms: int | None = None
        if all(outcome_times.values()):
            common_first_ms = max(min(values) for values in outcome_times.values())
            common_last_ms = min(max(values) for values in outcome_times.values())

        start_ms = market.market_start_utc_ns // 1_000_000
        end_ms = market.market_end_utc_ns // 1_000_000
        expected_ms = max(1, end_ms - start_ms)
        covered_ms = (
            max(0, min(common_last_ms, end_ms) - max(common_first_ms, start_ms))
            if common_first_ms is not None and common_last_ms is not None
            else 0
        )
        if state == "recording":
            coverage_state = "recording"
        elif not all_times:
            coverage_state = "pending_normalization"
        else:
            tolerance_ms = COVERAGE_EDGE_TOLERANCE_NS // 1_000_000
            complete_edges = (
                common_first_ms is not None
                and common_last_ms is not None
                and common_first_ms <= start_ms + tolerance_ms
                and common_last_ms >= end_ms - tolerance_ms
            )
            coverage_state = "ready" if complete_edges else "partial"
        return {
            "first_ms": first_ms,
            "last_ms": last_ms,
            "up_first_ms": min(outcome_times["UP"], default=None),
            "up_last_ms": max(outcome_times["UP"], default=None),
            "down_first_ms": min(outcome_times["DOWN"], default=None),
            "down_last_ms": max(outcome_times["DOWN"], default=None),
            "up_points": len(series.get("UP", [])),
            "down_points": len(series.get("DOWN", [])),
            "covered_ms": covered_ms,
            "expected_ms": expected_ms,
            "coverage_bps": min(10_000, covered_ms * 10_000 // expected_ms),
            "state": coverage_state,
        }

    def series(self, slug: str, *, max_points: int = 900) -> dict[str, Any]:
        max_points = max(120, min(max_points, 2_000))
        market = self.market_by_slug(slug)
        state = _status(market, utc_now_ns())
        ttl = 4.0 if state == "recording" else 10.0
        cache_key = f"series:{state}:{slug}:{max_points}"
        cached = self._cache.get(cache_key, ttl)
        if cached is not None:
            return cached

        start_ns = market.market_start_utc_ns
        end_ns = (
            min(market.market_end_utc_ns, utc_now_ns())
            if state == "recording"
            else market.market_end_utc_ns
        )
        top_rows: list[tuple[Any, ...]] = []
        top_files = self._files("top_of_book", start_ns, market.market_end_utc_ns)
        series_cache = series_path(self.storage_root, market.condition_id)
        cached_series: dict[str, Any] | None = None
        try:
            raw_cache = json.loads(series_cache.read_text(encoding="utf-8"))
            if isinstance(raw_cache, dict):
                cached_series = raw_cache
        except (OSError, json.JSONDecodeError):
            pass
        if cached_series is None and top_files:
            connection = duckdb.connect(":memory:")
            try:
                top_rows = connection.execute(
                    f"""
                    SELECT token_id, received_utc_ns, sequence, best_bid_scaled,
                           best_ask_scaled, midpoint_scaled, spread_scaled
                    FROM {_parquet_source(top_files)}
                    WHERE condition_id = ? AND received_utc_ns BETWEEN ? AND ?
                    ORDER BY received_utc_ns, sequence
                    """,
                    (market.condition_id, start_ns, end_ns),
                ).fetchall()
            finally:
                connection.close()
        if cached_series is not None:
            tokens = cached_series.get("tokens", {})
            series = {
                outcome: self._downsample_cached_points(
                    tokens.get(token_id, []) if isinstance(tokens, dict) else [],
                    max_points,
                    start_ms=start_ns // 1_000_000,
                    end_ms=end_ns // 1_000_000,
                )
                for outcome, token_id in (
                    ("UP", market.up_token_id),
                    ("DOWN", market.down_token_id),
                )
            }
        else:
            series = self._downsample_top(top_rows, market, max_points)

        btc_rows: list[tuple[Any, ...]] = []
        btc_files = self._btc_files(start_ns, market.market_end_utc_ns)
        if btc_files:
            connection = duckdb.connect(":memory:")
            try:
                btc_rows = connection.execute(
                    f"""
                    SELECT source, received_utc_ns, price_scaled
                    FROM {_parquet_source(btc_files)}
                    WHERE received_utc_ns BETWEEN ? AND ?
                    ORDER BY received_utc_ns, sequence
                    """,
                    (start_ns, end_ns),
                ).fetchall()
            finally:
                connection.close()

        live_book: dict[str, Any] | None = None
        if state == "recording":
            live_book = self._live_books(market)
            if live_book:
                for outcome in ("UP", "DOWN"):
                    book = live_book["books"].get(outcome)
                    if not book:
                        continue
                    series[outcome].append(
                        {
                            "t_ms": int(live_book["at_ms"]),
                            "bid": book["best_bid_scaled"],
                            "ask": book["best_ask_scaled"],
                            "mid": book["midpoint_scaled"],
                            "spread": book["spread_scaled"],
                        }
                    )

        coverage = self._series_coverage(market, state, series)
        coverage["normalized_files"] = len(top_files)
        payload = {
            "generated_at_ms": utc_now_ns() // 1_000_000,
            "market": self.market_payload(market),
            "scales": {
                "price": POLYMARKET_PRICE_SCALE,
                "shares": SHARE_SIZE_SCALE,
                "btc": BTC_PRICE_SCALE,
            },
            "series": series,
            "btc": self._downsample_btc(btc_rows, start_ns, end_ns, max_points),
            "coverage": coverage,
        }
        self._cache.put(cache_key, payload)
        return payload

    @staticmethod
    def _downsample_cached_points(
        raw_points: object, maximum: int, *, start_ms: int, end_ms: int
    ) -> list[dict[str, int | None]]:
        if not isinstance(raw_points, list):
            return []
        points = [
            point
            for point in raw_points
            if isinstance(point, dict) and start_ms <= int(point["t_ms"]) <= end_ms
        ]
        if len(points) <= maximum:
            return [
                {
                    "t_ms": int(point["t_ms"]),
                    "bid": _scaled_or_none(point.get("bid")),
                    "ask": _scaled_or_none(point.get("ask")),
                    "mid": _scaled_or_none(point.get("mid")),
                    "spread": _scaled_or_none(point.get("spread")),
                }
                for point in points
            ]
        width = max(1, (len(points) + maximum - 1) // maximum)
        sampled = [
            points[min(index + width - 1, len(points) - 1)]
            for index in range(0, len(points), width)
        ]
        return DashboardData._downsample_cached_points(
            sampled[:maximum], maximum, start_ms=start_ms, end_ms=end_ms
        )

    @staticmethod
    def _depth_payload(
        levels: dict[int, int], *, descending: bool, maximum_levels: int = 30
    ) -> list[dict[str, int]]:
        cumulative = 0
        rows: list[dict[str, int]] = []
        for price in sorted(levels, reverse=descending)[:maximum_levels]:
            size = levels[price]
            cumulative += size
            rows.append({"price": price, "size": size, "cumulative": cumulative})
        return rows

    @classmethod
    def _book_payload(
        cls, bids: dict[int, int], asks: dict[int, int], *, token_id: str
    ) -> dict[str, Any]:
        best_bid = max(bids, default=None)
        best_ask = min(asks, default=None)
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        midpoint = (
            (best_bid + best_ask) // 2 if best_bid is not None and best_ask is not None else None
        )
        return {
            "token_id": token_id,
            "best_bid_scaled": best_bid,
            "best_ask_scaled": best_ask,
            "spread_scaled": spread,
            "midpoint_scaled": midpoint,
            "bids": cls._depth_payload(bids, descending=True),
            "asks": cls._depth_payload(asks, descending=False),
        }

    def _live_books(self, market: MarketRecord) -> dict[str, Any] | None:
        cache_key = f"live-book:{market.condition_id}"
        cached = self._cache.get(cache_key, 8.0)
        if cached is not None:
            return cached
        books: dict[str, Any] = {}
        try:
            with httpx.Client(timeout=5.0) as client:
                for outcome, token_id in (
                    ("UP", market.up_token_id),
                    ("DOWN", market.down_token_id),
                ):
                    response = client.get(
                        "https://clob.polymarket.com/book", params={"token_id": token_id}
                    )
                    response.raise_for_status()
                    raw = response.json()
                    bids = {
                        decimal_to_scaled(
                            str(level["price"]), POLYMARKET_PRICE_SCALE, field="dashboard_bid"
                        ): decimal_to_scaled(
                            str(level["size"]), SHARE_SIZE_SCALE, field="dashboard_bid_size"
                        )
                        for level in raw.get("bids", [])
                    }
                    asks = {
                        decimal_to_scaled(
                            str(level["price"]), POLYMARKET_PRICE_SCALE, field="dashboard_ask"
                        ): decimal_to_scaled(
                            str(level["size"]), SHARE_SIZE_SCALE, field="dashboard_ask_size"
                        )
                        for level in raw.get("asks", [])
                    }
                    books[outcome] = self._book_payload(bids, asks, token_id=token_id)
        except (httpx.HTTPError, KeyError, TypeError, ValueError):
            return None
        payload = {
            "source": "live_clob_rest",
            "at_ms": utc_now_ns() // 1_000_000,
            "books": books,
        }
        self._cache.put(cache_key, payload)
        return payload

    def book(self, slug: str, *, at_ms: int | None = None) -> dict[str, Any]:
        market = self.market_by_slug(slug)
        now_ns = utc_now_ns()
        state = _status(market, now_ns)
        if at_ms is None and state == "recording":
            live = self._live_books(market)
            if live is not None:
                return {"market": self.market_payload(market), **live}

        at_ns = (
            min(max(at_ms * 1_000_000, market.market_start_utc_ns), market.market_end_utc_ns)
            if at_ms is not None
            else min(now_ns, market.market_end_utc_ns)
        )
        cache_key = f"book:{slug}:{at_ns // 2_000_000_000}"
        cached = self._cache.get(cache_key, 60.0 if state != "recording" else 5.0)
        if cached is not None:
            return cached

        search_start = market.market_start_utc_ns - 35 * 60 * 1_000_000_000
        snapshot_files = self._files("book_snapshots", search_start, at_ns)
        level_files = self._files("book_snapshot_levels", search_start, at_ns)
        update_files = self._files("book_updates", search_start, at_ns)
        if not snapshot_files or not level_files:
            if at_ms is None and state in {"recording", "upcoming"}:
                live = self._live_books(market)
                if live is not None:
                    return {"market": self.market_payload(market), **live}
            raise LookupError("book data is pending normalization")

        connection = duckdb.connect(":memory:")
        try:
            snapshots = connection.execute(
                f"""
                SELECT snapshot_id, token_id, received_utc_ns, sequence
                FROM {_parquet_source(snapshot_files)}
                WHERE condition_id = ? AND received_utc_ns <= ?
                  AND token_id IN (?, ?)
                QUALIFY row_number() OVER (
                    PARTITION BY token_id ORDER BY received_utc_ns DESC, sequence DESC
                ) = 1
                """,
                (market.condition_id, at_ns, market.up_token_id, market.down_token_id),
            ).fetchall()
            if len(snapshots) < 2:
                if at_ms is None and state in {"recording", "upcoming"}:
                    live = self._live_books(market)
                    if live is not None:
                        return {"market": self.market_payload(market), **live}
                raise LookupError("both outcome snapshots are not yet available")
            snapshot_ids = [str(row[0]) for row in snapshots]
            placeholders = ",".join("?" for _ in snapshot_ids)
            level_rows = connection.execute(
                f"""
                SELECT snapshot_id, side, price_scaled, size_scaled
                FROM {_parquet_source(level_files)}
                WHERE snapshot_id IN ({placeholders})
                """,
                snapshot_ids,
            ).fetchall()
            update_rows: list[tuple[Any, ...]] = []
            if update_files:
                minimum_snapshot_ns = min(int(row[2]) for row in snapshots)
                update_rows = connection.execute(
                    f"""
                    SELECT token_id, received_utc_ns, sequence, change_index,
                           side, price_scaled, new_size_scaled
                    FROM {_parquet_source(update_files)}
                    WHERE condition_id = ? AND received_utc_ns BETWEEN ? AND ?
                      AND token_id IN (?, ?)
                    ORDER BY received_utc_ns, sequence, change_index
                    """,
                    (
                        market.condition_id,
                        minimum_snapshot_ns,
                        at_ns,
                        market.up_token_id,
                        market.down_token_id,
                    ),
                ).fetchall()
        finally:
            connection.close()

        snapshot_for_token = {
            str(token_id): (str(snapshot_id), int(received_ns), int(sequence))
            for snapshot_id, token_id, received_ns, sequence in snapshots
        }
        token_for_snapshot = {
            snapshot_id: token_id
            for token_id, (snapshot_id, _received_ns, _sequence) in snapshot_for_token.items()
        }
        states: dict[str, dict[str, dict[int, int]]] = {
            market.up_token_id: {"BUY": {}, "SELL": {}},
            market.down_token_id: {"BUY": {}, "SELL": {}},
        }
        for snapshot_id_raw, side_raw, price_raw, size_raw in level_rows:
            token_id = token_for_snapshot.get(str(snapshot_id_raw))
            if token_id is None:
                continue
            states[token_id][str(side_raw)][int(price_raw)] = int(size_raw)
        for (
            token_raw,
            received_raw,
            sequence_raw,
            _change_index,
            side_raw,
            price_raw,
            size_raw,
        ) in update_rows:
            token_id = str(token_raw)
            snapshot = snapshot_for_token[token_id]
            received_ns = int(received_raw)
            sequence = int(sequence_raw)
            if received_ns < snapshot[1] or (
                received_ns == snapshot[1] and sequence <= snapshot[2]
            ):
                continue
            levels = states[token_id][str(side_raw)]
            price = int(price_raw)
            size = int(size_raw)
            if size == 0:
                levels.pop(price, None)
            else:
                levels[price] = size

        payload = {
            "market": self.market_payload(market),
            "source": "reconstructed_parquet",
            "at_ms": at_ns // 1_000_000,
            "books": {
                "UP": self._book_payload(
                    states[market.up_token_id]["BUY"],
                    states[market.up_token_id]["SELL"],
                    token_id=market.up_token_id,
                ),
                "DOWN": self._book_payload(
                    states[market.down_token_id]["BUY"],
                    states[market.down_token_id]["SELL"],
                    token_id=market.down_token_id,
                ),
            },
        }
        self._cache.put(cache_key, payload)
        return payload

    def trades(self, slug: str, *, limit: int = 250) -> dict[str, Any]:
        market = self.market_by_slug(slug)
        limit = max(1, min(limit, 1_000))
        files = self._files("trades", market.market_start_utc_ns, market.market_end_utc_ns)
        rows: list[tuple[Any, ...]] = []
        totals: list[tuple[Any, ...]] = []
        if files:
            connection = duckdb.connect(":memory:")
            try:
                source = _parquet_source(files)
                rows = connection.execute(
                    f"""
                    SELECT received_utc_ns, outcome, price_scaled, size_scaled,
                           notional_scaled, reported_side, transaction_hash
                    FROM {source}
                    WHERE condition_id = ?
                    ORDER BY received_utc_ns DESC, sequence DESC
                    LIMIT ?
                    """,
                    (market.condition_id, limit),
                ).fetchall()
                totals = connection.execute(
                    f"""
                    SELECT outcome, count(*), sum(size_scaled), sum(notional_scaled)
                    FROM {source} WHERE condition_id = ? GROUP BY outcome
                    """,
                    (market.condition_id,),
                ).fetchall()
            finally:
                connection.close()
        return {
            "market": self.market_payload(market),
            "trades": [
                {
                    "t_ms": int(row[0]) // 1_000_000,
                    "outcome": str(row[1]),
                    "price": int(row[2]),
                    "size": _scaled_or_none(row[3]),
                    "notional": _scaled_or_none(row[4]),
                    "side": str(row[5]) if row[5] is not None else None,
                    "transaction_hash": str(row[6]) if row[6] is not None else None,
                }
                for row in rows
            ],
            "totals": {
                str(row[0]): {
                    "count": int(row[1]),
                    "size": _scaled_or_none(row[2]),
                    "notional": _scaled_or_none(row[3]),
                }
                for row in totals
            },
        }

    def health(self) -> dict[str, Any]:
        collector: dict[str, Any] = {}
        if self.status_path.exists():
            try:
                collector = json.loads(self.status_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                collector = {"state": "unknown"}
        top_files = self._top_files()
        return {
            "state": "healthy" if self.registry_path.exists() else "unhealthy",
            "generated_at_ms": utc_now_ns() // 1_000_000,
            "collector_state": collector.get("state", "unknown"),
            "normalized_top_files": len(top_files),
            "latest_normalized_mtime_ms": max(
                (path.stat().st_mtime_ns // 1_000_000 for path in top_files), default=None
            ),
        }
