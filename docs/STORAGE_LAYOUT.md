# Storage layout

The configured root defaults to the repository's `data/`. Raw data is the source of truth; every
other representation is reproducible or operational state.

## Raw archive

```text
data/raw/
  source=clob_market_ws/date=YYYY-MM-DD/hour=HH/part-*.jsonl.zst
  source=rtds/date=YYYY-MM-DD/hour=HH/part-*.jsonl.zst
  source=gamma/date=YYYY-MM-DD/hour=HH/part-*.jsonl.zst
  source=clob_rest/date=YYYY-MM-DD/hour=HH/market=<condition>/part-*.jsonl.zst
  source=data_api/date=YYYY-MM-DD/hour=HH/market=<condition>/part-*.jsonl.zst
  source=internal/date=YYYY-MM-DD/hour=HH/market=<condition>/part-*.jsonl.zst
```

An active name ends in `.partial`. Finalization flushes the compressor and file, optionally fsyncs,
atomically renames, hashes, then manifests it. Rotation occurs at the first of 15 minutes, 128 MiB
estimated uncompressed, partition/market change, or shutdown; all thresholds are configurable.

One event per file is explicitly avoided. A dedicated writer thread batches up to 1,000 envelopes or
100 ms and performs periodic block flushes. Zstandard level 3 is the reliability-oriented default.

CLOB WebSocket frames can multiplex updates for several subscribed token IDs and therefore remain
hour/source partitioned; the normalizer resolves each inner token to its condition. REST responses
have one explicit market and retain the `market=<condition>` partition.

Startup partial recovery never overwrites the damaged input. Decodable newline-complete records are
published as `recovered-*.jsonl.zst`; the original becomes `.abandoned-<id>` and the recovered
manifest quality is degraded.

## Normalized Parquet

```text
data/normalized/
  markets/part-*.parquet
  market_outcomes/part-*.parquet
  book_snapshots/date=YYYY-MM-DD/hour=HH/part-*.parquet
  book_snapshot_levels/date=YYYY-MM-DD/hour=HH/part-*.parquet
  book_updates/date=YYYY-MM-DD/hour=HH/part-*.parquet
  tick_size_changes/date=YYYY-MM-DD/hour=HH/part-*.parquet
  trades/date=YYYY-MM-DD/hour=HH/part-*.parquet
  btc_prices/source=<EXACT_SOURCE>/date=YYYY-MM-DD/hour=HH/part-*.parquet
  connection_events/date=YYYY-MM-DD/hour=HH/part-*.parquet
  heartbeat_events/date=YYYY-MM-DD/hour=HH/part-*.parquet
  rest_requests/date=YYYY-MM-DD/hour=HH/part-*.parquet
  data_quality_events/date=YYYY-MM-DD/hour=HH/part-*.parquet
  market_resolutions/date=YYYY-MM-DD/hour=HH/part-*.parquet
  normalization_runs/part-*.parquet
  dashboard/markets/<condition_id>.json
  dashboard-market-index.json
```

Files use Zstandard level 6, dictionary encoding, statistics, and 128,000-row groups. Temporary files
end in `.partial`; row count is read back before atomic publication. The intended steady-state size
is 64–256 MiB. Short smoke runs naturally produce small files and should be compacted later.

Token ID is deliberately not a top-level partition. BTC source is a partition and also a physical
field so exact source meaning survives copied files; use `hive_partitioning=false` in DuckDB/Arrow
when scanning the glob to avoid a partition-column collision.

The dashboard JSON is a derived one-second top-of-book cache, keyed by condition and token. It is
atomically replaced, safe for concurrent readers, and rebuildable with
`polymarket-bt dashboard-reindex`. Parquet and raw archives remain authoritative.

## Compaction

`polymarket-bt compact [--dataset NAME]` groups files by physical partition, checks schema
compatibility, concatenates and deterministically sorts by receipt/sequence/change index when
present, writes a temporary file, validates row counts, atomically publishes under:

```text
data/normalized_compacted/<dataset>/<same partitions>/compact-*.parquet
```

Originals are retained. The compactor never deletes automatically. Operators may archive originals
only after checksums, row counts, and independent backups are confirmed.

## Manifests

`data/manifests/file-manifests.jsonl` is append-only and fsync'd per finalized file. Each entry has a
file UUID, path, dataset, schema, lifecycle, rows, sequence/time ranges, estimated raw bytes,
compressed bytes, SHA-256, versions, market/token summaries, quality, and format.

The normalizer exports `file-manifests.parquet`, readable directly by DuckDB. Verification reports are
timestamped JSON files in this directory. `scripts/backup_manifests.sh` creates non-destructive copies.

## Operational state

```text
data/state/market-registry.sqlite
data/state/operational.sqlite
data/state/normalization.sqlite
data/state/status.json
data/state/collector.lock
```

SQLite is WAL mode and contains only low-volume registry, quarantine, run linkage, and checkpoint
state. No high-volume market event is inserted into SQLite. `status.json` is atomically replaced and
safe for external readers. The lock prevents two collectors from writing the same root.

## Reports and quarantine

- `data/reports/<backtest-run>/` contains complete reproducibility/report artifacts.
- `data/reports/data-quality/<date>/` contains validation summaries.
- `data/reports/trade-reconciliation/` contains post-close comparison reports.
- `data/quarantine/` is reserved for operator-reviewed data; ambiguous market records currently live
  in the registry's `quarantined_markets` table with their raw Gamma payload.

## Capacity and retention

Health thresholds default to 20 GiB warning, 10 GiB critical, and 5 GiB emergency. Warning degrades
health; critical stops nonessential offline jobs by operator policy; emergency causes the collector to
close safely and stop. No raw data is auto-deleted.

Retention is always explicit: verify manifests, create an off-host or encrypted backup, validate the
backup hashes, then archive selected finalized partitions. Never remove `.partial`, registry, or
manifest files blindly. Raw market data should outlive derived Parquet so new parsers remain possible.
