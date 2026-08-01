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
    series = dashboard.series(selected.market_slug)
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
