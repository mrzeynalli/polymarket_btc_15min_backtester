from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_bt.dashboard.cache import DashboardCacheWriter
from polymarket_bt.dashboard.data import DashboardData
from polymarket_bt.discovery.market_registry import MarketRegistry
from polymarket_bt.models.markets import MarketRecord


def _write_partition(
    root: Path,
    dataset: str,
    rows: list[dict[str, object]],
    event_ns: int,
    *,
    source: str | None = None,
) -> None:
    moment = datetime.fromtimestamp(event_ns / 1_000_000_000, tz=UTC)
    directory = root / "normalized" / dataset
    if source:
        directory /= f"source={source}"
    directory = directory / f"date={moment:%Y-%m-%d}" / f"hour={moment:%H}"
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), directory / "part-test.parquet")


def _historical_market(market: MarketRecord) -> MarketRecord:
    start = 1_700_000_000_000_000_000
    return market.model_copy(
        update={
            "market_slug": "btc-updown-15m-dashboard-test",
            "condition_id": "dashboard-condition",
            "market_start_utc_ns": start,
            "market_end_utc_ns": start + 900_000_000_000,
            "active": False,
            "closed": True,
            "accepting_orders": False,
        }
    )


def test_dashboard_series_and_book_reconstruction(tmp_path: Path, market: MarketRecord) -> None:
    storage = tmp_path / "data"
    selected = _historical_market(market)
    registry = MarketRegistry(storage / "state" / "market-registry.sqlite")
    registry.upsert(selected, "{}")
    registry.close()
    start = selected.market_start_utc_ns

    top_rows = [
        {
            "condition_id": selected.condition_id,
            "token_id": selected.up_token_id,
            "received_utc_ns": start + 5_000_000_000,
            "sequence": 10,
            "best_bid_scaled": 500_000,
            "best_ask_scaled": 520_000,
            "midpoint_scaled": 510_000,
            "spread_scaled": 20_000,
        },
        {
            "condition_id": selected.condition_id,
            "token_id": selected.down_token_id,
            "received_utc_ns": start + 5_000_000_000,
            "sequence": 10,
            "best_bid_scaled": 470_000,
            "best_ask_scaled": 490_000,
            "midpoint_scaled": 480_000,
            "spread_scaled": 20_000,
        },
        {
            "condition_id": selected.condition_id,
            "token_id": selected.up_token_id,
            "received_utc_ns": start + 895_000_000_000,
            "sequence": 11,
            "best_bid_scaled": 510_000,
            "best_ask_scaled": 530_000,
            "midpoint_scaled": 520_000,
            "spread_scaled": 20_000,
        },
        {
            "condition_id": selected.condition_id,
            "token_id": selected.down_token_id,
            "received_utc_ns": start + 895_000_000_000,
            "sequence": 11,
            "best_bid_scaled": 460_000,
            "best_ask_scaled": 480_000,
            "midpoint_scaled": 470_000,
            "spread_scaled": 20_000,
        },
    ]
    _write_partition(storage, "top_of_book", top_rows, start)
    cache_result = DashboardCacheWriter(storage).update(top_rows)
    assert cache_result["markets_updated"] == 1

    snapshots = [
        {
            "snapshot_id": "up-snapshot",
            "condition_id": selected.condition_id,
            "token_id": selected.up_token_id,
            "received_utc_ns": start + 1_000_000_000,
            "sequence": 1,
        },
        {
            "snapshot_id": "down-snapshot",
            "condition_id": selected.condition_id,
            "token_id": selected.down_token_id,
            "received_utc_ns": start + 1_000_000_000,
            "sequence": 1,
        },
    ]
    _write_partition(storage, "book_snapshots", snapshots, start)
    levels = [
        {
            "snapshot_id": "up-snapshot",
            "condition_id": selected.condition_id,
            "side": "BUY",
            "price_scaled": 500_000,
            "size_scaled": 10_000_000,
        },
        {
            "snapshot_id": "up-snapshot",
            "condition_id": selected.condition_id,
            "side": "SELL",
            "price_scaled": 520_000,
            "size_scaled": 12_000_000,
        },
        {
            "snapshot_id": "down-snapshot",
            "condition_id": selected.condition_id,
            "side": "BUY",
            "price_scaled": 470_000,
            "size_scaled": 8_000_000,
        },
        {
            "snapshot_id": "down-snapshot",
            "condition_id": selected.condition_id,
            "side": "SELL",
            "price_scaled": 490_000,
            "size_scaled": 9_000_000,
        },
    ]
    _write_partition(storage, "book_snapshot_levels", levels, start)
    updates = [
        {
            "condition_id": selected.condition_id,
            "token_id": selected.up_token_id,
            "received_utc_ns": start + 10_000_000_000,
            "sequence": 2,
            "change_index": 0,
            "side": "BUY",
            "price_scaled": 500_000,
            "new_size_scaled": 0,
        },
        {
            "condition_id": selected.condition_id,
            "token_id": selected.up_token_id,
            "received_utc_ns": start + 10_000_000_000,
            "sequence": 2,
            "change_index": 1,
            "side": "BUY",
            "price_scaled": 510_000,
            "new_size_scaled": 4_000_000,
        },
    ]
    _write_partition(storage, "book_updates", updates, start)

    trades = [
        {
            "condition_id": selected.condition_id,
            "received_utc_ns": start + 15_000_000_000,
            "sequence": 3,
            "outcome": "UP",
            "price_scaled": 520_000,
            "size_scaled": 2_000_000,
            "notional_scaled": 1_040_000,
            "reported_side": "BUY",
            "transaction_hash": "0xtest",
        }
    ]
    _write_partition(storage, "trades", trades, start)
    _write_partition(
        storage,
        "btc_prices",
        [
            {
                "source": "BINANCE_BTCUSDT",
                "received_utc_ns": start + 5_000_000_000,
                "sequence": 4,
                "price_scaled": 67_000_000_000_000_000,
            }
        ],
        start,
        source="BINANCE_BTCUSDT",
    )

    dashboard = DashboardData(storage)
    listing = dashboard.markets(limit=10)
    assert listing["ready_count"] == 1
    assert listing["partial_count"] == 0
    series = dashboard.series(selected.market_slug)
    assert series["coverage"]["state"] == "ready"
    assert series["coverage"]["coverage_bps"] > 9_800
    assert series["series"]["UP"][0]["mid"] == 510_000
    assert series["series"]["DOWN"][0]["mid"] == 480_000
    assert series["btc"]["BINANCE_BTCUSDT"][0]["price"] == 67_000_000_000_000_000

    book = dashboard.book(selected.market_slug, at_ms=(start + 20_000_000_000) // 1_000_000)
    assert book["source"] == "reconstructed_parquet"
    assert book["books"]["UP"]["best_bid_scaled"] == 510_000
    assert book["books"]["UP"]["best_ask_scaled"] == 520_000
    assert book["books"]["DOWN"]["best_bid_scaled"] == 470_000

    trade_payload = dashboard.trades(selected.market_slug)
    assert trade_payload["trades"][0]["price"] == 520_000
    assert trade_payload["totals"]["UP"]["count"] == 1


def test_dashboard_stats_and_export(tmp_path: Path, market: MarketRecord) -> None:
    storage = tmp_path / "data"
    selected = _historical_market(market)
    registry = MarketRegistry(storage / "state" / "market-registry.sqlite")
    registry.upsert(selected, "{}")
    registry.close()
    start = selected.market_start_utc_ns

    top_rows = [
        {
            "condition_id": selected.condition_id,
            "token_id": selected.up_token_id,
            "received_utc_ns": start + 5_000_000_000,
            "sequence": 10,
            "best_bid_scaled": 500_000,
            "best_ask_scaled": 520_000,
            "midpoint_scaled": 510_000,
            "spread_scaled": 20_000,
        },
        {
            "condition_id": selected.condition_id,
            "token_id": selected.down_token_id,
            "received_utc_ns": start + 5_000_000_000,
            "sequence": 10,
            "best_bid_scaled": 470_000,
            "best_ask_scaled": 490_000,
            "midpoint_scaled": 480_000,
            "spread_scaled": 20_000,
        },
    ]
    _write_partition(storage, "top_of_book", top_rows, start)

    trades = [
        {
            "condition_id": selected.condition_id,
            "received_utc_ns": start + 15_000_000_000,
            "sequence": 3,
            "outcome": "UP",
            "price_scaled": 520_000,
            "size_scaled": 2_000_000,
            "notional_scaled": 1_040_000,
            "reported_side": "BUY",
            "transaction_hash": "0xtest",
        }
    ]
    _write_partition(storage, "trades", trades, start)

    dashboard = DashboardData(storage)

    stats = dashboard.stats()
    assert stats["market_count"] == 1
    assert stats["trade_count"] == 1
    assert stats["captured_notional_usd"] == "1.04"

    everything = dashboard.export_markets()
    assert [item.market_slug for item in everything] == [selected.market_slug]

    start_ms = start // 1_000_000
    assert dashboard.export_markets(from_ms=start_ms, to_ms=start_ms) == everything
    assert dashboard.export_markets(from_ms=start_ms + 1) == []
    assert dashboard.export_markets(to_ms=start_ms - 1) == []

    assert dashboard.export_header() == (
        b"slug,condition_id,outcome,t_ms,iso_time,best_bid,best_ask,midpoint,spread\n"
    )
    rows = list(dashboard.export_rows_for_market(selected))
    assert len(rows) == 2
    up_row = next(row for row in rows if b",UP," in row)
    assert up_row.startswith(f"{selected.market_slug},{selected.condition_id},UP,".encode())
    assert b",0.5,0.52,0.51,0.02\n" in up_row
    down_row = next(row for row in rows if b",DOWN," in row)
    assert b",0.47,0.49,0.48,0.02\n" in down_row


def test_dashboard_distinguishes_partial_and_preopen_only_data(
    tmp_path: Path, market: MarketRecord
) -> None:
    storage = tmp_path / "data"
    partial = _historical_market(market).model_copy(
        update={
            "condition_id": "partial-condition",
            "market_slug": "btc-updown-15m-partial",
            "up_token_id": "partial-up",
            "down_token_id": "partial-down",
        }
    )
    pending = partial.model_copy(
        update={
            "condition_id": "pending-condition",
            "market_slug": "btc-updown-15m-pending",
            "up_token_id": "pending-up",
            "down_token_id": "pending-down",
        }
    )
    registry = MarketRegistry(storage / "state" / "market-registry.sqlite")
    registry.upsert(partial, "{}")
    registry.upsert(pending, "{}")
    registry.close()

    def point(condition_id: str, token_id: str, received_ns: int) -> dict[str, object]:
        return {
            "condition_id": condition_id,
            "token_id": token_id,
            "received_utc_ns": received_ns,
            "sequence": 1,
            "best_bid_scaled": 490_000,
            "best_ask_scaled": 510_000,
            "midpoint_scaled": 500_000,
            "spread_scaled": 20_000,
        }

    start = partial.market_start_utc_ns
    DashboardCacheWriter(storage).update(
        [
            point(partial.condition_id, partial.up_token_id, start + 300_000_000_000),
            point(partial.condition_id, partial.down_token_id, start + 300_000_000_000),
            point(pending.condition_id, pending.up_token_id, start - 120_000_000_000),
            point(pending.condition_id, pending.down_token_id, start - 120_000_000_000),
        ]
    )

    dashboard = DashboardData(storage)
    listing = dashboard.markets(limit=10)
    statuses = {item["slug"]: item["data_status"] for item in listing["markets"]}
    assert statuses[partial.market_slug] == "partial"
    assert statuses[pending.market_slug] == "pending"
    assert listing["partial_count"] == 1
    assert listing["pending_count"] == 1
    assert dashboard.series(partial.market_slug)["coverage"]["state"] == "partial"
    assert dashboard.series(pending.market_slug)["coverage"]["state"] == "pending_normalization"
