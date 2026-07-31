# Troubleshooting

Preserve raw files, manifests, status, and logs before attempting repair. No routine incident requires
deleting data.

## Service does not start

```bash
systemctl status polymarket-collector.service
journalctl -u polymarket-collector.service -n 100 --no-pager
systemctl cat polymarket-collector.service
```

- `status=200/CHDIR`: `/opt/polymarket-btc-backtester/current` is absent/broken. Restore the symlink to
  a complete release.
- permission denied under `/var/lib`: verify the directory is owned by `polymarket-data` and the unit
  has `StateDirectory=polymarket-btc-backtester`.
- configuration validation error: compare with `configs/collector.example.yaml`; keys are strict.
- address in use: identify the loopback listener on 9108. Do not change health to a public bind.
- missing module: rebuild that release's `.venv`; do not point it at the source workspace venv.

## `collector already running`

The lock includes PID and run UUID. Check whether the PID exists and whether systemd owns it:

```bash
cat /var/lib/polymarket-btc-backtester/state/collector.lock
systemctl is-active polymarket-collector.service
```

Do not manually remove a live lock. On startup, dead-PID locks are atomically renamed
`.stale-<timestamp>` automatically.

## No market discovered

Run:

```bash
polymarket-bt discover --asset BTC --duration 15m
```

Check Gamma reachability, UTC clock, candidate boundary, and matcher decisions in the SQLite
`quarantined_markets` table. Naming alone is insufficient. If official outcome labels, duration, or
token fields changed, preserve the payload, update `API_NOTES.md`, add a sanitized fixture, version the
matcher, and test before lowering the threshold. Never force an ambiguous token map.

## CLOB connected but no book messages

Inspect status for `active_tokens`, registry token IDs, initial REST snapshot count, and connection
events. The first valid market frame is used as an inferred acknowledgement. A newly upcoming market
may have token IDs but no changes yet; REST snapshots should still exist. Revalidate subscription
syntax against official docs before changing it.

If REST returns a different `asset_id` or condition, the client rejects it and marks recovery failed.
Do not apply it to the requested book.

## Reconnect loop or heartbeat misses

Check DNS, TLS, route, clock, and Polymarket service status. Application intervals are 10 seconds for
CLOB and 5 for RTDS. Logs should show connect, subscription, missed heartbeat, close, and jittered
backoff. Do not enable protocol pings in parallel or eliminate backoff.

A reconnect necessarily creates a degraded/unreliable interval and recovery snapshots. Zero local
drops does not make the disconnected interval complete.

## RTDS has only one BTC source

Inspect exact `topic`, `type`, `filters`, and symbol. The verified raw subscription uses
`crypto_prices` with a JSON-encoded `{"symbol":"BTCUSDT"}` Binance filter; the returned symbol remains
`btcusdt`. Chainlink uses `crypto_prices_chainlink` and a JSON-encoded `{"symbol":"btc/usd"}` filter.
RTDS requires PING sends but does not promise PONG, so do not configure PONG absence alone as a
failure. Preserve observed unknown messages before changing parsing. The normalized sources must
remain separate.

## Book validation fails

Run `validate-books` for the condition and inspect the first sequence. Common causes are unknown token
mapping, tick change not yet applied, crossed source snapshot, malformed precision, missed live-parser
event, or upstream disconnect. The live supervisor marks the book uncertain and fetches REST.

Do not reorder changes within a source event, treat size as a delta, infer trades from decreases, or
invent a source-compatible hash. Use the raw file reference to reproduce the sequence offline.

## Raw queue pressure or drops

Any `dropped_total > 0` makes health unhealthy. Record the first/last dropped sequence and archive the
status/logs. Check disk latency, fsync policy, CPU steal, compression settings, queue size, and whether
unrelated jobs competed for I/O. Increasing a queue only buys burst capacity; it does not fix a
sustained writer deficit.

If raw did not drop but the parser queue did, offline normalization can recover the events. Treat the
live in-memory state as uncertain until a REST snapshot.

## `.partial` files after crash

Restart normally. Complete JSON lines are copied to `recovered-*.jsonl.zst`; the original becomes
`.abandoned-*`. Verify both the manifest quality and recovered row count. An abandoned file is evidence
and must not be overwritten or silently deleted.

## Normalization returns zero files

The raw path/SHA may already be in `normalization.sqlite`, the date filter may not match UTC
partitions, or files may still be active `.partial`. Query manifests first. Do not delete checkpoint
rows merely to force a rerun; use a separate output root for parser-version comparisons.

## Parquet/Hive source conflict

Because `btc_prices/source=<value>/` also stores an exact `source` field, some dataset readers infer a
second partition column. Use physical-file reads or:

```sql
read_parquet('data/normalized/btc_prices/**/*.parquet', hive_partitioning=false)
```

The built-in reader uses `ParquetFile.read()` and does not infer path columns.

## Manifest verification fails

Open the timestamped report under `data/manifests/`. Distinguish missing path, checksum mismatch,
unreadable format, row-count mismatch, and sequence overlap. Stop compaction/retention for affected
partitions. Compare a trusted backup. `verify-files` is read-only; no automatic repair exists by
design.

If a copied file legitimately moved, create an explicit repair tool/change record rather than editing
the append-only manifest by hand.

## Disk is critical or emergency

Stop normalization/compaction first. At emergency the collector should close and stop; confirm
finalized files/manifests and preserve journal output. Move only verified finalized partitions to a
validated backup/archive. Never delete raw automatically, and never manipulate an active `.partial`.

## Backtest rejects a quality interval

This is expected with `reject_on_gap=true`. Inspect `data_quality_events` and the raw connection
timeline. For exploratory use only, set it false and retain `quality_report.json`; do not present that
result as comparable to complete replay.

## Backtest has no fills

Confirm the strategy generated intents, books were valid at scheduled arrival, the limit crossed,
cash/inventory was sufficient, the market was unresolved, and displayed depth existed. `NoOpStrategy`
must always have zero fills. The example strategy is not guaranteed to trade or profit.

## Service status is degraded immediately after `collect --once`

A bounded run intentionally closes sockets and writes a final status with disconnected feeds. Judge
the smoke test by returned counters, snapshots/books, RTDS sources, drops, parse errors, and graceful
file finalization. Judge continuous health only while the systemd service is running.

## Suspected API change

1. Preserve exact raw messages and request diagnostics.
2. Re-open current official documentation and changelog.
3. Compare field names, semantics, heartbeat, and rate limits.
4. Add a sanitized actual fixture and failing test.
5. Version parser/matcher/schema behavior and update `API_NOTES.md` with UTC date.
6. Run all offline checks and a bounded public smoke capture.
7. Deploy as a new release and retain rollback.

Never “fix” drift by dropping unknown events or guessing delta/absolute semantics.
