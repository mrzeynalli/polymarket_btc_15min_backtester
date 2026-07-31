#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import orjson

from polymarket_bt.clock import monotonic_now_ns, utc_now_ns
from polymarket_bt.config import load_collector_config
from polymarket_bt.constants import Source
from polymarket_bt.models.books import BookLevel, BookLevelChange, BookSnapshot
from polymarket_bt.models.events import make_raw_envelope
from polymarket_bt.orderbook.state import OrderBook
from polymarket_bt.replay.event_clock import ReplayEvent
from polymarket_bt.replay.merger import merge_events
from polymarket_bt.storage.manifest import ManifestStore
from polymarket_bt.storage.parquet_writer import ParquetDatasetWriter
from polymarket_bt.storage.raw_writer import RawArchive


def rate(count: int, started: float) -> dict[str, float | int]:
    duration = time.perf_counter() - started
    return {
        "events": count,
        "duration_seconds": duration,
        "events_per_second": count / max(duration, 1e-12),
    }


def snapshot() -> BookSnapshot:
    return BookSnapshot(
        snapshot_id="benchmark-snapshot",
        sequence=1,
        run_id="benchmark",
        connection_id="benchmark",
        source="fixture",
        condition_id="benchmark-condition",
        token_id="benchmark-token",
        outcome="UP",
        exchange_timestamp_ns=1,
        received_utc_ns=1,
        received_monotonic_ns=1,
        tick_size_scaled=10_000,
        minimum_order_size_scaled=1_000_000,
        bids=(BookLevel(price_scaled=490_000, size_scaled=100_000_000),),
        asks=(BookLevel(price_scaled=510_000, size_scaled=100_000_000),),
    )


async def raw_writer_benchmark(config_path: Path, count: int) -> dict[str, float | int]:
    config = load_collector_config(config_path)
    with tempfile.TemporaryDirectory(prefix="polymarket-benchmark-") as temporary:
        config.storage.root = Path(temporary)
        config.queues.raw_max_events = count + 1
        config.storage.writer_batch_events = min(1_000, count)
        archive = RawArchive(config, ManifestStore(config.storage.root))
        await archive.start()
        started = time.perf_counter()
        for index in range(count):
            accepted = archive.enqueue(
                make_raw_envelope(
                    collector_version="benchmark",
                    run_id="benchmark",
                    connection_id="benchmark",
                    sequence=index + 1,
                    source=Source.RTDS,
                    stream="benchmark",
                    payload='{"topic":"crypto_prices","payload":{"value":"65000.00"}}',
                )
            )
            if not accepted:
                raise RuntimeError("benchmark raw queue unexpectedly filled")
        await archive.stop()
        result = rate(count, started)
        result["compressed_bytes"] = archive.stats.bytes_compressed
        result["compression_ratio"] = archive.stats.compression_ratio or 0.0
        return result


def run(config_path: Path, count: int) -> dict[str, Any]:
    started = time.perf_counter()
    envelopes = [
        make_raw_envelope(
            collector_version="benchmark",
            run_id="benchmark",
            connection_id="benchmark",
            sequence=index + 1,
            source=Source.RTDS,
            stream="benchmark",
            payload='{"topic":"crypto_prices","payload":{"value":"65000.00"}}',
        )
        for index in range(count)
    ]
    envelope_result = rate(count, started)

    raw_message = envelopes[0].payload_raw
    started = time.perf_counter()
    for _ in range(count):
        orjson.loads(raw_message)
    parsing_result = rate(count, started)

    book = OrderBook("benchmark-condition", "benchmark-token", "UP")
    book.replace(snapshot())
    started = time.perf_counter()
    for index in range(count):
        book.apply(
            BookLevelChange(
                parent_event_id=f"event-{index}",
                change_index=0,
                sequence=index + 2,
                condition_id="benchmark-condition",
                token_id="benchmark-token",
                outcome="UP",
                exchange_timestamp_ns=index + 2,
                received_utc_ns=index + 2,
                received_monotonic_ns=index + 2,
                side="BUY",
                price_scaled=480_000,
                new_size_scaled=(index % 100 + 1) * 1_000_000,
                connection_id="benchmark",
            )
        )
    book_result = rate(count, started)

    replay_events = [
        ReplayEvent(
            event_type="benchmark",
            exchange_timestamp_ns=index,
            received_utc_ns=count - index,
            received_monotonic_ns=count - index,
            connection_id="benchmark",
            sequence=index + 1,
            parent_change_index=0,
            source_priority=20,
            payload={"index": index},
        )
        for index in range(count)
    ]
    from polymarket_bt.constants import ReplayClockMode

    started = time.perf_counter()
    ordered = merge_events(replay_events, ReplayClockMode.LOCAL_RECEIVE_TIME)
    if len(ordered) != count:
        raise RuntimeError("replay benchmark lost events")
    replay_result = rate(count, started)

    config = load_collector_config(config_path)
    with tempfile.TemporaryDirectory(prefix="polymarket-parquet-benchmark-") as temporary:
        config.storage.root = Path(temporary)
        writer = ParquetDatasetWriter(config, ManifestStore(config.storage.root))
        rows = [
            {
                "schema_version": 1,
                "parent_event_id": f"event-{index}",
                "change_index": 0,
                "sequence": index + 1,
                "condition_id": "benchmark-condition",
                "token_id": "benchmark-token",
                "outcome": "UP",
                "exchange_timestamp_ns": index,
                "received_utc_ns": index,
                "received_monotonic_ns": index,
                "side": "BUY",
                "price_scaled": 480_000,
                "new_size_scaled": 1_000_000,
                "book_hash": None,
                "reported_best_bid_scaled": 480_000,
                "reported_best_ask_scaled": 510_000,
                "connection_id": "benchmark",
                "raw_event_reference": f"benchmark:{index + 1}",
            }
            for index in range(count)
        ]
        started = time.perf_counter()
        entry = writer.write_rows("book_updates", rows)
        parquet_result = rate(count, started)
        parquet_result["compressed_bytes"] = entry.compressed_bytes if entry else 0

    return {
        "generated_utc_ns": utc_now_ns(),
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": __import__("os").cpu_count(),
        },
        "count_per_benchmark": count,
        "raw_envelope": envelope_result,
        "raw_writer": asyncio.run(raw_writer_benchmark(config_path, count)),
        "json_parsing": parsing_result,
        "book_updates": book_result,
        "parquet_normalization": parquet_result,
        "replay_sort": replay_result,
        "completed_monotonic_ns": monotonic_now_ns(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local recorder/replay microbenchmarks")
    parser.add_argument("--config", type=Path, default=Path("configs/collector.yaml"))
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    if arguments.events < 1:
        parser.error("--events must be positive")
    result = run(arguments.config.resolve(), arguments.events)
    output = arguments.output
    if output is None:
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        output = Path("data/reports/benchmarks") / f"benchmark-{stamp}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps({"output": str(output), **result}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
