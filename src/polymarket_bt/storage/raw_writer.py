from __future__ import annotations

import asyncio
import functools
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

import zstandard

from polymarket_bt.clock import utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.models.events import RawEnvelope
from polymarket_bt.storage.manifest import ManifestEntry, ManifestStore, sha256_file


@dataclass(slots=True)
class RawWriterStats:
    enqueued_total: int = 0
    persisted_total: int = 0
    dropped_total: int = 0
    bytes_uncompressed: int = 0
    bytes_compressed: int = 0
    batches_total: int = 0
    queue_high_watermark: int = 0
    first_dropped_sequence: int | None = None
    last_dropped_sequence: int | None = None
    recovered_partial_files: int = 0
    unrecovered_partial_files: int = 0

    @property
    def compression_ratio(self) -> float | None:
        if not self.bytes_compressed:
            return None
        return self.bytes_uncompressed / self.bytes_compressed


@dataclass(slots=True)
class _PartitionWriter:
    storage_root: Path
    partition: Path
    collector_version: str
    compression_level: int
    manifest: ManifestStore
    fsync_on_close: bool
    file_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_utc_ns: int = field(default_factory=utc_now_ns)
    rows: int = 0
    uncompressed_bytes: int = 0
    first_sequence: int | None = None
    last_sequence: int | None = None
    minimum_event_time: int | None = None
    maximum_event_time: int | None = None
    market_ids: set[str] = field(default_factory=set)
    token_ids: set[str] = field(default_factory=set)
    final_path: Path = field(init=False)
    partial_path: Path = field(init=False)
    _raw: BinaryIO = field(init=False, repr=False)
    _compressor: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self.partition.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%S")
        self.final_path = self.partition / f"part-{stamp}-{self.file_id}.jsonl.zst"
        self.partial_path = self.final_path.with_suffix(self.final_path.suffix + ".partial")
        self._raw = self.partial_path.open("xb")
        self._compressor = zstandard.ZstdCompressor(level=self.compression_level).stream_writer(
            self._raw, closefd=False
        )

    def write(self, envelope: RawEnvelope) -> None:
        line = envelope.json_line()
        self._compressor.write(line)
        self.rows += 1
        self.uncompressed_bytes += len(line)
        self.first_sequence = (
            envelope.sequence
            if self.first_sequence is None
            else min(self.first_sequence, envelope.sequence)
        )
        self.last_sequence = (
            envelope.sequence
            if self.last_sequence is None
            else max(self.last_sequence, envelope.sequence)
        )
        self.minimum_event_time = (
            envelope.received_utc_ns
            if self.minimum_event_time is None
            else min(self.minimum_event_time, envelope.received_utc_ns)
        )
        self.maximum_event_time = (
            envelope.received_utc_ns
            if self.maximum_event_time is None
            else max(self.maximum_event_time, envelope.received_utc_ns)
        )
        if envelope.market_id:
            self.market_ids.add(envelope.market_id)
        if envelope.token_id:
            self.token_ids.add(envelope.token_id)

    def flush(self, *, fsync: bool = False) -> None:
        self._compressor.flush(zstandard.FLUSH_BLOCK)
        self._raw.flush()
        if fsync:
            os.fsync(self._raw.fileno())

    def should_rotate(self, now_ns: int, rotate_ns: int, max_uncompressed: int) -> bool:
        return (
            now_ns - self.created_utc_ns >= rotate_ns or self.uncompressed_bytes >= max_uncompressed
        )

    def close(self, *, quality_status: str = "complete") -> ManifestEntry:
        self._compressor.flush(zstandard.FLUSH_FRAME)
        self._compressor.close()
        self._raw.flush()
        if self.fsync_on_close:
            os.fsync(self._raw.fileno())
        self._raw.close()
        os.replace(self.partial_path, self.final_path)
        compressed_bytes = self.final_path.stat().st_size
        relative_path = self.final_path.relative_to(self.storage_root).as_posix()
        entry = ManifestEntry(
            file_id=self.file_id,
            relative_path=relative_path,
            dataset=f"raw/{self.partition.relative_to(self.storage_root / 'raw').as_posix()}",
            created_utc_ns=self.created_utc_ns,
            closed_utc_ns=utc_now_ns(),
            row_count=self.rows,
            first_sequence=self.first_sequence,
            last_sequence=self.last_sequence,
            minimum_event_time=self.minimum_event_time,
            maximum_event_time=self.maximum_event_time,
            uncompressed_bytes_estimate=self.uncompressed_bytes,
            compressed_bytes=compressed_bytes,
            sha256=sha256_file(self.final_path),
            collector_version=self.collector_version,
            market_ids=tuple(sorted(self.market_ids)),
            token_ids=tuple(sorted(self.token_ids)),
            quality_status=quality_status,
            format="jsonl.zst",
        )
        self.manifest.append(entry)
        return entry


class RawArchive:
    """Bounded asynchronous ingress with batched blocking work isolated in a thread."""

    def __init__(self, config: CollectorConfig, manifest: ManifestStore) -> None:
        self.config = config
        self.storage_root = config.storage.root
        self.raw_root = self.storage_root / "raw"
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.manifest = manifest
        self.queue: asyncio.Queue[RawEnvelope] = asyncio.Queue(maxsize=config.queues.raw_max_events)
        self.stats = RawWriterStats()
        self._writers: dict[str, _PartitionWriter] = {}
        self._worker: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="raw-archive")
        self._executor_shutdown = False

    async def _blocking(self, function: Any, *args: Any) -> Any:
        loop = asyncio.get_running_loop()
        future = loop.run_in_executor(self._executor, functools.partial(function, *args))
        # A short timer also guarantees progress with event-loop/extension combinations
        # that can occasionally suppress the executor completion wake-up.
        while not future.done():  # noqa: ASYNC110 - timer works around lost executor wake-ups
            await asyncio.sleep(0.01)
        return future.result()

    def _partition_for(self, envelope: RawEnvelope) -> Path:
        moment = datetime.fromtimestamp(envelope.received_utc_ns / 1_000_000_000, tz=UTC)
        base = (
            self.raw_root
            / f"source={envelope.source.value}"
            / f"date={moment:%Y-%m-%d}"
            / f"hour={moment:%H}"
        )
        if envelope.market_id:
            safe_market = envelope.market_id.replace("/", "_")
            base /= f"market={safe_market}"
        return base

    def enqueue(self, envelope: RawEnvelope) -> bool:
        try:
            self.queue.put_nowait(envelope)
        except asyncio.QueueFull:
            self.stats.dropped_total += 1
            if self.stats.first_dropped_sequence is None:
                self.stats.first_dropped_sequence = envelope.sequence
            self.stats.last_dropped_sequence = envelope.sequence
            return False
        self.stats.enqueued_total += 1
        self.stats.queue_high_watermark = max(self.stats.queue_high_watermark, self.queue.qsize())
        return True

    async def start(self) -> None:
        await self._blocking(self.recover_partials)
        self._worker = asyncio.create_task(self._run(), name="raw-archive-writer")

    async def _run(self) -> None:
        batch_wait = self.config.storage.writer_batch_wait_ms / 1000
        max_batch = self.config.storage.writer_batch_events
        while not self._stopping.is_set() or not self.queue.empty():
            batch: list[RawEnvelope] = []
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=batch_wait)
                batch.append(first)
            except TimeoutError:
                await self._blocking(self._flush_all, False)
                continue
            deadline = asyncio.get_running_loop().time() + batch_wait
            while len(batch) < max_batch and asyncio.get_running_loop().time() < deadline:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await self._blocking(self._write_batch, batch)
            for _ in batch:
                self.queue.task_done()

    def _new_writer(self, partition: Path) -> _PartitionWriter:
        return _PartitionWriter(
            storage_root=self.storage_root,
            partition=partition,
            collector_version=self.config.collector_version,
            compression_level=self.config.storage.raw_zstd_level,
            manifest=self.manifest,
            fsync_on_close=self.config.shutdown.fsync_on_close,
        )

    def _write_batch(self, batch: list[RawEnvelope]) -> None:
        with self._lock:
            for envelope in batch:
                partition = self._partition_for(envelope)
                key = partition.as_posix()
                writer = self._writers.get(key)
                if writer is None:
                    writer = self._new_writer(partition)
                    self._writers[key] = writer
                writer.write(envelope)
                self.stats.bytes_uncompressed += len(envelope.json_line())
                now = utc_now_ns()
                if writer.should_rotate(
                    now,
                    self.config.storage.rotate_minutes * 60 * 1_000_000_000,
                    self.config.storage.rotate_uncompressed_mb * 1024 * 1024,
                ):
                    entry = writer.close()
                    self.stats.bytes_compressed += entry.compressed_bytes
                    self._writers.pop(key)
            self.stats.persisted_total += len(batch)
            self.stats.batches_total += 1

    def _flush_all(self, fsync: bool) -> None:
        with self._lock:
            for writer in self._writers.values():
                writer.flush(fsync=fsync)

    async def stop(self, timeout_seconds: float | None = None) -> None:
        self._stopping.set()
        timeout = timeout_seconds or self.config.shutdown.drain_timeout_seconds
        if self._worker:
            try:
                await asyncio.wait_for(self._worker, timeout=timeout)
            except TimeoutError:
                self._worker.cancel()
                await asyncio.gather(self._worker, return_exceptions=True)
        entries = await self._blocking(self._close_all)
        self.stats.bytes_compressed += sum(entry.compressed_bytes for entry in entries)
        if not self._executor_shutdown:
            self._executor.shutdown(wait=True)
            self._executor_shutdown = True

    def _close_all(self) -> list[ManifestEntry]:
        entries: list[ManifestEntry] = []
        with self._lock:
            for writer in self._writers.values():
                entries.append(writer.close())
            self._writers.clear()
        return entries

    def recover_partials(self) -> None:
        for partial in self.raw_root.rglob("*.partial"):
            abandoned = partial.with_name(f"{partial.name}.abandoned-{uuid.uuid4().hex[:8]}")
            recovered_lines: list[bytes] = []
            buffer = b""
            try:
                with partial.open("rb") as source:
                    reader = zstandard.ZstdDecompressor().stream_reader(source)
                    while True:
                        try:
                            chunk = reader.read(1 << 20)
                        except zstandard.ZstdError:
                            break
                        if not chunk:
                            break
                        buffer += chunk
                        complete = buffer.splitlines(keepends=True)
                        buffer = b""
                        if complete and not complete[-1].endswith(b"\n"):
                            buffer = complete.pop()
                        recovered_lines.extend(complete)
            finally:
                os.replace(partial, abandoned)
            if recovered_lines:
                target = partial.with_suffix("").with_name(
                    f"recovered-{uuid.uuid4().hex}.jsonl.zst"
                )
                compressor = zstandard.ZstdCompressor(level=self.config.storage.raw_zstd_level)
                with target.open("xb") as output:
                    with compressor.stream_writer(output, closefd=False) as writer:
                        for line in recovered_lines:
                            writer.write(line)
                    output.flush()
                    os.fsync(output.fileno())
                entry = ManifestEntry(
                    relative_path=target.relative_to(self.storage_root).as_posix(),
                    dataset=f"raw/{target.parent.relative_to(self.raw_root).as_posix()}",
                    created_utc_ns=int(abandoned.stat().st_mtime_ns),
                    closed_utc_ns=utc_now_ns(),
                    row_count=len(recovered_lines),
                    uncompressed_bytes_estimate=sum(map(len, recovered_lines)),
                    compressed_bytes=target.stat().st_size,
                    sha256=sha256_file(target),
                    collector_version=self.config.collector_version,
                    quality_status="partial_recovered_degraded",
                    format="jsonl.zst",
                )
                self.manifest.append(entry)
                self.stats.recovered_partial_files += 1
            else:
                self.stats.unrecovered_partial_files += 1

    def status(self) -> dict[str, int | float | None]:
        return {
            "enqueued_total": self.stats.enqueued_total,
            "persisted_total": self.stats.persisted_total,
            "dropped_total": self.stats.dropped_total,
            "queue_size": self.queue.qsize(),
            "queue_high_watermark": self.stats.queue_high_watermark,
            "bytes_uncompressed": self.stats.bytes_uncompressed,
            "bytes_compressed": self.stats.bytes_compressed,
            "compression_ratio": self.stats.compression_ratio,
            "first_dropped_sequence": self.stats.first_dropped_sequence,
            "last_dropped_sequence": self.stats.last_dropped_sequence,
        }
