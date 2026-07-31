from __future__ import annotations

import os
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from polymarket_bt.clock import utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.storage.manifest import ManifestEntry, ManifestStore, sha256_file
from polymarket_bt.storage.schemas import SCHEMAS


class ParquetDatasetWriter:
    def __init__(self, config: CollectorConfig, manifest: ManifestStore) -> None:
        self.config = config
        self.storage_root = config.storage.root
        self.normalized_root = self.storage_root / "normalized"
        self.normalized_root.mkdir(parents=True, exist_ok=True)
        self.manifest = manifest

    def write_rows(
        self,
        dataset: str,
        rows: list[dict[str, Any]],
        *,
        partition: dict[str, str] | None = None,
        normalizer_version: str = "0.1.0",
        quality_status: str = "complete",
    ) -> ManifestEntry | None:
        if not rows:
            return None
        if dataset not in SCHEMAS:
            raise KeyError(f"unknown dataset: {dataset}")
        table = pa.Table.from_pylist(rows, schema=SCHEMAS[dataset])
        directory = self.normalized_root / dataset
        for key, value in (partition or {}).items():
            directory /= f"{key}={value}"
        directory.mkdir(parents=True, exist_ok=True)
        file_id = str(uuid.uuid4())
        target = directory / f"part-{utc_now_ns()}-{file_id}.parquet"
        return self._write_table(
            dataset,
            table,
            target,
            file_id=file_id,
            normalizer_version=normalizer_version,
            quality_status=quality_status,
        )

    def _write_table(
        self,
        dataset: str,
        table: pa.Table,
        target: Path,
        *,
        file_id: str,
        normalizer_version: str,
        quality_status: str,
    ) -> ManifestEntry:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(".parquet.partial")
        created = utc_now_ns()
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            compression_level=self.config.storage.parquet_zstd_level,
            use_dictionary=True,
            write_statistics=True,
            row_group_size=128_000,
        )
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        metadata = pq.read_metadata(temporary)
        if metadata.num_rows != table.num_rows:
            raise RuntimeError("Parquet row-count validation failed before publication")
        os.replace(temporary, target)
        columns = set(table.column_names)
        sequence_values = (
            [value for value in table["sequence"].to_pylist() if value is not None]
            if "sequence" in columns
            else []
        )
        event_column = next(
            (
                name
                for name in ("received_utc_ns", "event_utc_ns", "started_utc_ns")
                if name in columns
            ),
            None,
        )
        event_values = (
            [value for value in table[event_column].to_pylist() if value is not None]
            if event_column
            else []
        )
        market_ids = (
            sorted({str(item) for item in table["condition_id"].to_pylist() if item})
            if "condition_id" in columns
            else []
        )
        token_ids = (
            sorted({str(item) for item in table["token_id"].to_pylist() if item})
            if "token_id" in columns
            else []
        )
        entry = ManifestEntry(
            file_id=file_id,
            relative_path=target.relative_to(self.storage_root).as_posix(),
            dataset=dataset,
            created_utc_ns=created,
            closed_utc_ns=utc_now_ns(),
            row_count=table.num_rows,
            first_sequence=min(sequence_values) if sequence_values else None,
            last_sequence=max(sequence_values) if sequence_values else None,
            minimum_event_time=min(event_values) if event_values else None,
            maximum_event_time=max(event_values) if event_values else None,
            uncompressed_bytes_estimate=table.nbytes,
            compressed_bytes=target.stat().st_size,
            sha256=sha256_file(target),
            collector_version=self.config.collector_version,
            normalizer_version=normalizer_version,
            market_ids=tuple(market_ids),
            token_ids=tuple(token_ids),
            quality_status=quality_status,
            format="parquet",
        )
        self.manifest.append(entry)
        return entry

    def compact(self, dataset: str) -> list[Path]:
        if dataset not in SCHEMAS:
            raise KeyError(f"unknown dataset: {dataset}")
        source_root = self.normalized_root / dataset
        if not source_root.exists():
            return []
        grouped: dict[Path, list[Path]] = {}
        for path in source_root.rglob("*.parquet"):
            grouped.setdefault(path.parent, []).append(path)
        outputs: list[Path] = []
        for directory, paths in grouped.items():
            if len(paths) < 2:
                continue
            tables = [pq.ParquetFile(path).read() for path in sorted(paths)]
            expected = tables[0].schema
            if any(not table.schema.equals(expected, check_metadata=False) for table in tables[1:]):
                raise ValueError(f"schema mismatch in {directory}")
            combined = pa.concat_tables(tables)
            sort_keys = [
                (name, "ascending")
                for name in ("received_utc_ns", "sequence", "change_index")
                if name in combined.column_names
            ]
            if sort_keys:
                combined = pc.take(combined, pc.sort_indices(combined, sort_keys=sort_keys))
            relative_partition = directory.relative_to(source_root)
            output_dir = self.storage_root / "normalized_compacted" / dataset / relative_partition
            file_id = str(uuid.uuid4())
            output = output_dir / f"compact-{utc_now_ns()}-{file_id}.parquet"
            self._write_table(
                f"compacted/{dataset}",
                combined,
                output,
                file_id=file_id,
                normalizer_version="0.1.0-compactor",
                quality_status="compacted_verified_originals_retained",
            )
            if pq.read_metadata(output).num_rows != sum(table.num_rows for table in tables):
                raise RuntimeError("compaction validation failed")
            outputs.append(output)
        return outputs

    def datasets(self) -> Iterable[str]:
        return SCHEMAS.keys()
