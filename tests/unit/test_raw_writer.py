from __future__ import annotations

import io

import zstandard

from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import Source
from polymarket_bt.models.events import RawEnvelope, make_raw_envelope
from polymarket_bt.storage.manifest import ManifestStore
from polymarket_bt.storage.raw_writer import RawArchive


async def test_raw_archive_finalizes_and_manifests(collector_config: CollectorConfig) -> None:
    manifest = ManifestStore(collector_config.storage.root)
    archive = RawArchive(collector_config, manifest)
    await archive.start()
    original = '{"event_type":"fixture","price":"0.531"}'
    assert archive.enqueue(
        make_raw_envelope(
            collector_version="test",
            run_id="run",
            connection_id="connection",
            sequence=1,
            source=Source.CLOB_MARKET_WS,
            stream="market",
            payload=original,
            market_id="condition",
        )
    )
    await archive.stop()
    entries = manifest.entries()
    assert len(entries) == 1
    path = collector_config.storage.root / entries[0].relative_path
    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(path.read_bytes())) as reader:
        raw = reader.read().decode()
    envelope = RawEnvelope.model_validate_json(raw)
    assert envelope.payload_raw == original
    assert not list(collector_config.storage.root.rglob("*.partial"))
    assert manifest.verify()["ok"]


async def test_recover_abandoned_partial(collector_config: CollectorConfig) -> None:
    directory = collector_config.storage.root / "raw" / "source=test" / "date=2026-07-31"
    directory.mkdir(parents=True)
    envelope = make_raw_envelope(
        collector_version="test",
        run_id="run",
        connection_id="connection",
        sequence=1,
        source=Source.INTERNAL,
        stream="test",
        payload="{}",
    )
    partial = directory / "part-test.jsonl.zst.partial"
    partial.write_bytes(zstandard.ZstdCompressor().compress(envelope.json_line()))
    archive = RawArchive(collector_config, ManifestStore(collector_config.storage.root))
    await archive.start()
    await archive.stop()
    assert archive.stats.recovered_partial_files == 1
    assert list(directory.glob("*.abandoned-*"))
    assert list(directory.glob("recovered-*.jsonl.zst"))


async def test_idle_partition_is_finalized_without_another_event(
    collector_config: CollectorConfig,
) -> None:
    archive = RawArchive(collector_config, ManifestStore(collector_config.storage.root))
    await archive.start()
    assert archive.enqueue(
        make_raw_envelope(
            collector_version="test",
            run_id="run",
            connection_id="connection",
            sequence=1,
            source=Source.CLOB_MARKET_WS,
            stream="market",
            payload="{}",
        )
    )
    await archive.queue.join()
    assert archive._writers
    for writer in archive._writers.values():
        writer.created_utc_ns -= 61 * 60 * 1_000_000_000
    archive._flush_all(False)
    assert not archive._writers
    assert not list(collector_config.storage.root.rglob("*.partial"))
    await archive.stop()


def test_bounded_queue_records_exact_drop(collector_config: CollectorConfig) -> None:
    collector_config.queues.raw_max_events = 1
    archive = RawArchive(collector_config, ManifestStore(collector_config.storage.root))
    first = make_raw_envelope(
        collector_version="test",
        run_id="run",
        connection_id="c",
        sequence=1,
        source=Source.INTERNAL,
        stream="test",
        payload="{}",
    )
    second = first.model_copy(update={"sequence": 2})
    assert archive.enqueue(first)
    assert not archive.enqueue(second)
    assert archive.stats.first_dropped_sequence == 2
    assert archive.stats.last_dropped_sequence == 2
