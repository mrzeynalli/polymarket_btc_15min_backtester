from __future__ import annotations

import itertools
from pathlib import Path

from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import Source
from polymarket_bt.models.events import make_raw_envelope
from polymarket_bt.storage.manifest import ManifestStore
from polymarket_bt.storage.parquet_writer import ParquetDatasetWriter
from polymarket_bt.storage.raw_writer import RawArchive
from polymarket_bt.storage.sqlite_state import OperationalState


async def test_raw_to_parquet_pipeline(
    collector_config: CollectorConfig, fixture_root: Path
) -> None:
    manifest = ManifestStore(collector_config.storage.root)
    archive = RawArchive(collector_config, manifest)
    await archive.start()
    sequence = itertools.count(1)

    def enqueue(source: Source, path: Path, hint: str | None = None) -> None:
        assert archive.enqueue(
            make_raw_envelope(
                collector_version="test",
                run_id="run",
                connection_id=f"{source.value}-connection",
                sequence=next(sequence),
                source=source,
                stream="fixture",
                payload=path.read_text(),
                event_type_hint=hint,
                market_id=(
                    "0xe9f9f391ea621795260318c03e8fb2ce9f9c3735c00f6b5fc36d6fa9fdf6fba7"
                    if source in {Source.CLOB_MARKET_WS, Source.CLOB_REST}
                    else None
                ),
            )
        )

    enqueue(Source.GAMMA, fixture_root / "gamma" / "current_btc_15m.json", "gamma_response")
    enqueue(Source.CLOB_MARKET_WS, fixture_root / "clob" / "ws_book.json")
    enqueue(Source.CLOB_MARKET_WS, fixture_root / "clob" / "tick_size_change.json")
    enqueue(Source.CLOB_MARKET_WS, fixture_root / "clob" / "price_change.json")
    enqueue(Source.CLOB_MARKET_WS, fixture_root / "clob" / "last_trade_price.json")
    enqueue(Source.CLOB_MARKET_WS, fixture_root / "clob" / "market_resolved.json")
    enqueue(Source.RTDS, fixture_root / "rtds" / "binance.json")
    enqueue(Source.RTDS, fixture_root / "rtds" / "chainlink.json")
    await archive.stop()

    import duckdb

    from polymarket_bt.normalization.normalizer import Normalizer

    normalizer = Normalizer(collector_config)
    try:
        result = normalizer.normalize()
        second = normalizer.normalize()
    finally:
        normalizer.close()
    assert result["invalid_events"] == 0
    assert result["unknown_events"] == 0
    assert result["rows_written"] > 0
    assert second["raw_files_processed"] == 0

    normalized = collector_config.storage.root / "normalized"
    connection = duckdb.connect()
    try:
        book_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(normalized / "book_snapshots" / "**" / "*.parquet")],
        ).fetchone()[0]
        price_sources = connection.execute(
            "SELECT source, count(*) FROM read_parquet(?) GROUP BY source ORDER BY source",
            [str(normalized / "btc_prices" / "**" / "*.parquet")],
        ).fetchall()
        tick_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [str(normalized / "tick_size_changes" / "**" / "*.parquet")],
        ).fetchone()[0]
    finally:
        connection.close()
    assert book_count == 1
    assert tick_count == 1
    assert price_sources == [("BINANCE_BTCUSDT", 1), ("CHAINLINK_BTCUSD", 1)]
    assert manifest.verify()["ok"]


async def test_tick_dataset_backfills_already_processed_raw_file(
    collector_config: CollectorConfig, fixture_root: Path
) -> None:
    manifest = ManifestStore(collector_config.storage.root)
    archive = RawArchive(collector_config, manifest)
    await archive.start()
    assert archive.enqueue(
        make_raw_envelope(
            collector_version="old-normalizer",
            run_id="old-run",
            connection_id="clob",
            sequence=1,
            source=Source.CLOB_MARKET_WS,
            stream="market",
            payload=(fixture_root / "clob" / "tick_size_change.json").read_text(),
        )
    )
    await archive.stop()

    state = OperationalState(collector_config.storage.root / "state" / "normalization.sqlite")
    try:
        for entry in manifest.entries():
            state.mark_raw_file_processed(entry.relative_path, entry.sha256, "old-normalizer-run")
    finally:
        state.close()

    from polymarket_bt.normalization.normalizer import Normalizer

    normalizer = Normalizer(collector_config)
    try:
        result = normalizer.normalize()
    finally:
        normalizer.close()

    assert result["raw_files_processed"] == 0
    tick_files = list(
        (collector_config.storage.root / "normalized" / "tick_size_changes").rglob("*.parquet")
    )
    assert len(tick_files) == 1
    import pyarrow.parquet as pq

    assert pq.ParquetFile(tick_files[0]).metadata.num_rows == 1


async def test_bounded_normalization_prioritizes_newest_raw_file(
    collector_config: CollectorConfig,
) -> None:
    manifest = ManifestStore(collector_config.storage.root)
    for sequence in (1, 2):
        archive = RawArchive(collector_config, manifest)
        await archive.start()
        assert archive.enqueue(
            make_raw_envelope(
                collector_version="test",
                run_id="priority-run",
                connection_id=f"connection-{sequence}",
                sequence=sequence,
                source=Source.INTERNAL,
                stream="operations",
                payload="{}",
                event_type_hint="connection_event",
            )
        )
        await archive.stop()

    raw_entries = sorted(
        (entry for entry in manifest.entries() if entry.format == "jsonl.zst"),
        key=lambda entry: (entry.closed_utc_ns, entry.relative_path),
    )
    assert len(raw_entries) == 2

    from polymarket_bt.normalization.normalizer import Normalizer

    normalizer = Normalizer(collector_config)
    try:
        result = normalizer.normalize(max_files=1)
    finally:
        normalizer.close()
    assert result["raw_files_processed"] == 1

    state = OperationalState(collector_config.storage.root / "state" / "normalization.sqlite")
    try:
        assert not state.raw_file_processed(raw_entries[0].relative_path, raw_entries[0].sha256)
        assert state.raw_file_processed(raw_entries[1].relative_path, raw_entries[1].sha256)
    finally:
        state.close()

    normalizer = Normalizer(collector_config)
    try:
        oldest_result = normalizer.normalize(max_files=1, newest_first=False)
    finally:
        normalizer.close()
    assert oldest_result["raw_files_processed"] == 1
    state = OperationalState(collector_config.storage.root / "state" / "normalization.sqlite")
    try:
        assert state.raw_file_processed(raw_entries[0].relative_path, raw_entries[0].sha256)
    finally:
        state.close()


def test_compaction_validates_rows_and_retains_originals(
    collector_config: CollectorConfig,
) -> None:
    manifest = ManifestStore(collector_config.storage.root)
    writer = ParquetDatasetWriter(collector_config, manifest)
    base = {
        "schema_version": 1,
        "condition_id": "condition",
        "token_id": "token",
        "received_utc_ns": 1_785_530_000_000_000_000,
        "best_bid_scaled": 400_000,
        "best_ask_scaled": 500_000,
        "spread_scaled": 100_000,
        "midpoint_scaled": 450_000,
        "bid_size_scaled": None,
        "ask_size_scaled": None,
    }
    first = writer.write_rows(
        "top_of_book", [{**base, "sequence": 2}], partition={"date": "2026-07-31"}
    )
    second = writer.write_rows(
        "top_of_book", [{**base, "sequence": 1}], partition={"date": "2026-07-31"}
    )
    assert first is not None and second is not None
    outputs = writer.compact("top_of_book")
    assert len(outputs) == 1
    import pyarrow.parquet as pq

    compacted = pq.ParquetFile(outputs[0]).read().to_pylist()
    assert [row["sequence"] for row in compacted] == [1, 2]
    assert (collector_config.storage.root / first.relative_path).exists()
    assert (collector_config.storage.root / second.relative_path).exists()
    assert manifest.verify()["ok"]
