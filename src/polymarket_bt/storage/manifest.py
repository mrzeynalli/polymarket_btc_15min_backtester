from __future__ import annotations

import hashlib
import io
import itertools
import json
import os
import threading
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Literal

import orjson
import zstandard
from pydantic import BaseModel, ConfigDict, Field

from polymarket_bt.clock import utc_now_ns
from polymarket_bt.constants import SCHEMA_VERSION


class ManifestEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    file_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    relative_path: str
    dataset: str
    schema_version: int = SCHEMA_VERSION
    created_utc_ns: int
    closed_utc_ns: int
    row_count: int
    first_sequence: int | None = None
    last_sequence: int | None = None
    minimum_event_time: int | None = None
    maximum_event_time: int | None = None
    uncompressed_bytes_estimate: int
    compressed_bytes: int
    sha256: str
    collector_version: str
    normalizer_version: str | None = None
    market_ids: tuple[str, ...] = ()
    token_ids: tuple[str, ...] = ()
    quality_status: str = "complete"
    format: Literal["jsonl.zst", "parquet"]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


class ManifestStore:
    def __init__(self, storage_root: Path) -> None:
        self.storage_root = storage_root
        self.root = storage_root / "manifests"
        self.root.mkdir(parents=True, exist_ok=True)
        self.jsonl_path = self.root / "file-manifests.jsonl"
        self.parquet_path = self.root / "file-manifests.parquet"
        self._lock = threading.Lock()

    def append(self, entry: ManifestEntry) -> None:
        line = entry.model_dump_json() + "\n"
        with self._lock, self.jsonl_path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def entries(self) -> list[ManifestEntry]:
        if not self.jsonl_path.exists():
            return []
        entries: list[ManifestEntry] = []
        with self.jsonl_path.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    entries.append(ManifestEntry.model_validate_json(line))
        return entries

    def export_parquet(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        entries = self.entries()
        if not entries:
            return
        rows = [entry.model_dump(mode="json") for entry in entries]
        table = pa.Table.from_pylist(rows)
        temporary = self.parquet_path.with_suffix(".parquet.partial")
        pq.write_table(table, temporary, compression="zstd", compression_level=6)
        os.replace(temporary, self.parquet_path)

    def verify(self) -> dict[str, Any]:
        import pyarrow.parquet as pq

        results: list[dict[str, Any]] = []
        sequence_ranges: dict[str, list[tuple[int, int, str]]] = {}
        raw_sequences: dict[str, list[int]] = defaultdict(list)
        for entry in self.entries():
            path = self.storage_root / entry.relative_path
            result: dict[str, Any] = {
                "file_id": entry.file_id,
                "path": entry.relative_path,
                "exists": path.exists(),
                "checksum_ok": False,
                "readable": False,
                "error": None,
            }
            try:
                if not path.exists():
                    raise FileNotFoundError(path)
                result["checksum_ok"] = sha256_file(path) == entry.sha256
                if entry.format == "parquet":
                    parquet = pq.ParquetFile(path)
                    metadata = parquet.metadata
                    expected_dataset = entry.dataset.removeprefix("compacted/")
                    schema_ok = True
                    try:
                        from polymarket_bt.storage.schemas import SCHEMAS

                        expected_schema = SCHEMAS.get(expected_dataset)
                        if expected_schema is not None:
                            schema_ok = parquet.schema_arrow.equals(
                                expected_schema, check_metadata=False
                            )
                    except (ImportError, KeyError):
                        schema_ok = False
                    result["schema_ok"] = schema_ok
                    result["readable"] = metadata.num_rows == entry.row_count and schema_ok
                else:
                    row_count = 0
                    with path.open("rb") as compressed:
                        with zstandard.ZstdDecompressor().stream_reader(compressed) as reader:
                            with io.TextIOWrapper(reader, encoding="utf-8") as text_reader:
                                for line in text_reader:
                                    if not line.strip():
                                        continue
                                    payload = orjson.loads(line)
                                    if not isinstance(payload, dict):
                                        raise ValueError("raw JSONL row is not an object")
                                    run_id = str(payload["run_id"])
                                    raw_sequences[run_id].append(int(payload["sequence"]))
                                    row_count += 1
                    result["observed_row_count"] = row_count
                    result["readable"] = row_count == entry.row_count and row_count > 0
                if entry.first_sequence is not None and entry.last_sequence is not None:
                    range_group = f"{entry.dataset}:{Path(entry.relative_path).parent.as_posix()}"
                    sequence_ranges.setdefault(range_group, []).append(
                        (entry.first_sequence, entry.last_sequence, entry.relative_path)
                    )
            except Exception as exc:
                result["error"] = f"{type(exc).__name__}: {exc}"
            results.append(result)
        overlaps: list[dict[str, Any]] = []
        for dataset, ranges in sequence_ranges.items():
            ordered_ranges = sorted(ranges)
            for previous_range, current_range in itertools.pairwise(ordered_ranges):
                if current_range[0] <= previous_range[1]:
                    overlaps.append(
                        {
                            "dataset": dataset,
                            "previous": previous_range,
                            "current": current_range,
                        }
                    )
        sequence_duplicates: list[dict[str, int | str]] = []
        sequence_gaps: list[dict[str, int | str]] = []
        for run_id, values in raw_sequences.items():
            ordered_sequences = sorted(values)
            for previous_sequence, current_sequence in itertools.pairwise(ordered_sequences):
                if current_sequence == previous_sequence:
                    sequence_duplicates.append({"run_id": run_id, "sequence": current_sequence})
                elif current_sequence > previous_sequence + 1:
                    sequence_gaps.append(
                        {
                            "run_id": run_id,
                            "first_missing_sequence": previous_sequence + 1,
                            "last_missing_sequence": current_sequence - 1,
                        }
                    )
        partial_files = [
            path.relative_to(self.storage_root).as_posix()
            for path in self.storage_root.rglob("*.partial")
        ]
        sequence_scan_complete = not partial_files
        return {
            "verified_utc_ns": utc_now_ns(),
            "files": results,
            "overlaps": overlaps,
            "sequence_duplicates": sequence_duplicates,
            "sequence_gaps": sequence_gaps,
            "sequence_scan_complete": sequence_scan_complete,
            "partial_files": partial_files,
            "ok": all(row["exists"] and row["checksum_ok"] and row["readable"] for row in results)
            and not overlaps
            and not sequence_duplicates
            and (not sequence_scan_complete or not sequence_gaps),
            # Gaps are definitive only when no active writer is holding a partial file.
            "raw_sequence_integrity_ok": not sequence_duplicates
            and (not sequence_scan_complete or not sequence_gaps),
        }

    def write_verification_report(self) -> Path:
        report = self.verify()
        target = self.root / f"verification-{utc_now_ns()}.json"
        temporary = target.with_suffix(".json.partial")
        temporary.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(temporary, target)
        return target
