# Polymarket BTC 15-minute recorder and backtester

This project continuously discovers Polymarket BTC 15-minute Up/Down markets, records public
Level-2 and trade messages for both outcome tokens, records Polymarket RTDS Binance and Chainlink
BTC reference prices, preserves every received payload in an append-only Zstandard archive, and
normalizes those archives to versioned Parquet for deterministic replay and simulation.

> **Safety boundary:** this repository contains no authenticated client, wallet configuration,
> signing code, private-key handling, user WebSocket, or order-submission function. `OrderIntent`
> and fills exist only inside the offline simulator. The collector uses public endpoints only.

## What is implemented

- Multi-signal Gamma discovery with confidence scoring, explicit label-to-token mapping, SQLite WAL
  registry, and quarantine for ambiguous markets.
- Overlapping current/next-market subscriptions, initial and periodic public CLOB REST snapshots,
  custom market WebSocket events, application heartbeats, bounded queues, reconnect backoff, and
  recovery snapshots.
- Separate RTDS Binance BTC/USDT and Chainlink BTC/USD streams with source, envelope, arrival, and
  monotonic timestamps preserved.
- Exact raw text or reversible base64 in JSONL Zstandard files; `.partial` crash recovery, atomic
  close, SHA-256 manifests, and time/size rotation.
- Restartable raw-to-Parquet normalization, fixed-scale integers, deterministic level ranking,
  compaction, file verification, DuckDB-readable data, and data-quality intervals.
- Deterministic order-book replacement and absolute-size updates with invariant checks.
- Local- or exchange-time event replay with explicit tie breaking and optional fail-closed gap
  behavior.
- Shared-wallet backtests with depth-sweeping taker fills, partial liquidity fills, configurable
  latency, versioned fees, weighted-average inventory, no implicit leverage, and resolution-time
  settlement.
- Conservative/optimistic/proportional maker queue primitives. End-to-end maker execution remains
  explicitly experimental and is rejected by the default engine.
- Structured JSON application logs, localhost health and Prometheus endpoints, disk protection,
  systemd and Docker Compose deployment files, and SIGINT/SIGTERM shutdown.

## Requirements and installation

The audited server runs Python 3.13.5. Python 3.12 or newer is supported.

```bash
cd /root/polymarket/btc_15min_bot/polymarket-btc-backtester
bash scripts/install.sh
.venv/bin/polymarket-bt doctor --config configs/collector.yaml
```

`scripts/install.sh` creates `.venv`, installs the project and development tools, and regenerates
the fully pinned `requirements.lock`. The YAML schema is strict: unknown keys are rejected.
Environment overrides currently supported are:

```bash
POLYMARKET_BT_STORAGE_ROOT=/absolute/data/path
POLYMARKET_BT_STATUS_FILE=/absolute/data/path/state/status.json
POLYMARKET_BT_LOG_LEVEL=INFO
```

No credential variables exist. `.env.example` contains only public-data operational settings.

## Run a bounded capture

```bash
.venv/bin/polymarket-bt discover --asset BTC --duration 15m
.venv/bin/polymarket-bt collect --once --duration 60 --config configs/collector.yaml
.venv/bin/polymarket-bt status --config configs/collector.yaml
```

The receive path captures UTC and monotonic time before enqueuing. It never performs REST work,
SQLite writes, Parquet writes, strategy calculations, or per-event logging.

## Run continuously after SSH closes

Use the installed systemd unit, not a foreground SSH process:

```bash
sudo systemctl start polymarket-collector.service
sudo systemctl status polymarket-collector.service
sudo journalctl -u polymarket-collector.service -f
```

Once installed and enabled, systemd starts it at boot and restarts it after failures. See
`docs/OPERATIONS.md` for installation, upgrades, shutdown, recovery, and rollback.

Docker Compose is also supported. Stamp the build with its Git provenance:

```bash
GIT_COMMIT="$(git rev-parse HEAD)" docker compose build
docker compose up -d collector
docker compose logs -f collector
```

Ensure the bind-mounted `data/` directory is writable by container UID/GID `65532` before using
Compose. The health endpoint is published on loopback only.

## Verify collection and health

```bash
curl -sS http://127.0.0.1:9108/health
curl -sS http://127.0.0.1:9108/metrics
.venv/bin/polymarket-bt verify-files
.venv/bin/polymarket-bt validate-data
```

Healthy operation means both WebSockets are connected, Gamma and REST data are fresh, both active
books are valid, disk is above the warning threshold, and raw drops remain zero. A bounded collector
intentionally writes a final disconnected status after graceful shutdown; that is not the status of
the persistent service.

## Normalize and query

```bash
.venv/bin/polymarket-bt normalize --date 2026-07-31
.venv/bin/polymarket-bt validate-books
.venv/bin/polymarket-bt replay --clock local_receive_time
.venv/bin/polymarket-bt compact --dataset book_updates
```

Normalization is keyed by raw path and SHA-256 in SQLite and is safe to rerun. Original raw files
remain the source of truth. Compaction writes verified outputs under `data/normalized_compacted/`
and retains the originals.

Example DuckDB query:

```sql
SELECT source, symbol, count(*) AS updates,
       min(received_utc_ns) AS first_receive_ns,
       max(received_utc_ns) AS last_receive_ns
FROM read_parquet('data/normalized/btc_prices/**/*.parquet', hive_partitioning=false)
GROUP BY source, symbol;
```

Set `hive_partitioning=false` when a physical column such as `source` is also present in the path.

## Read-only web dashboard

The project includes a self-contained responsive site in `web/index.html` and a loopback JSON API.
It opens on a chooser with two destinations:

- **History** — select a time-based slug, compare UP/DOWN bid, ask, midpoint, and spread paths, click
  any chart time to reconstruct both full books, and inspect public trades and BTC reference prices.
  New registry slugs appear automatically; a bounded systemd normalization timer publishes newly
  finalized archives without interrupting collection.
- **Backtest** — set an entry price, an optional entry window, an optional stop loss that can be
  armed only from a chosen minute, a side, a fixed or compounding stake, an execution-realism preset,
  and an entry policy (adaptive POV, TWAP, or immediate) with a finite horizon, then run the strategy
  across every recorded market. Orders walk the reconstructed ladder, so size the book could not
  absorb is reported as unfilled rather than silently filled.

```bash
.venv/bin/polymarket-bt dashboard \
  --config configs/collector.yaml \
  --host 127.0.0.1 \
  --port 9110 \
  --index web/index.html \
  --backtest-workspace /var/lib/polymarket-backtest

.venv/bin/polymarket-bt dashboard-reindex --config configs/collector.yaml
```

Backtests read a prepared workspace (episode index plus one depth tape per market) that must live
outside the collector storage root; `polymarket-backtest-refresh.timer` extends it after each market
closes. Runs are queued jobs with progress, one at a time, because a run replays every recorded book
state of every selected market.

Production deployment uses `polymarket-dashboard.service`, `polymarket-normalize.timer`,
`polymarket-backtest-refresh.timer`, and nginx. See [docs/DASHBOARD.md](docs/DASHBOARD.md) for the
data flow, API, exact book semantics, backtest parameter mapping, and deployment behavior.

## Backtest

```bash
.venv/bin/polymarket-bt backtest \
  --config configs/backtest.example.yaml \
  --collector-config configs/collector.yaml \
  --strategy no_op

.venv/bin/polymarket-bt backtest \
  --config configs/backtest.example.yaml \
  --strategy example_threshold
```

`NoOpStrategy` verifies replay without trades. `ExampleThresholdStrategy` is a mechanical example,
not a profitable or recommended strategy. Every run writes configuration, environment, input
manifests, quality usage, orders, fills, portfolio events, market results, metrics, JSON summary,
and Markdown report under `data/reports/<run-id>/`.

## Episode backtesting and parameter sweeps

The commands above replay whatever the archive contains. For strategies defined per 15-minute
market — enter at a price inside a time window, hold to settlement or exit at a stop loss — use
the episode layer, which indexes markets, derives settlement ground truth, caches reconstructed
depth ladders, and sweeps parameters under explicit execution-realism assumptions.

```bash
polymarket-bt episodes --workspace ../backtest-workspace --build-tapes
polymarket-bt sweep --workspace ../backtest-workspace \
  --entry-from 12 --entry-to 14 --triggers 0.80,0.90 --stops none,0.75
polymarket-bt verify-sim --workspace ../backtest-workspace --episodes 30
```

The workspace must live outside the collector's storage root; the CLI refuses otherwise and every
read of recorded data is read-only. `verify-sim` cross-checks the fast episode simulator against
the audited event engine order by order. See [docs/BACKTEST_METHOD.md](docs/BACKTEST_METHOD.md) for
the method, calibration sources, realism presets, metric definitions, and limitations.

## CLI

```text
polymarket-bt doctor
polymarket-bt discover
polymarket-bt collect [--once] [--duration SECONDS]
polymarket-bt status
polymarket-bt normalize
polymarket-bt compact
polymarket-bt reconcile-trades
polymarket-bt validate-books
polymarket-bt validate-data
polymarket-bt verify-files
polymarket-bt inspect-market
polymarket-bt replay
polymarket-bt backtest
polymarket-bt episodes
polymarket-bt sweep
polymarket-bt verify-sim
polymarket-bt report
```

Each command has `--help`; nonzero exits distinguish invalid configuration, no result, validation
failure, and network/runtime failure.

## Data layout

```text
data/
├── raw/                   exact JSONL envelopes compressed with Zstandard
├── normalized/            versioned analytical Parquet
├── normalized_compacted/  validated compaction outputs; originals retained
├── manifests/             append-only JSONL manifest and DuckDB-readable Parquet export
├── state/                 WAL-mode market registry, checkpoints, lock, status
├── reports/               backtests, reconciliation, verification, quality
└── quarantine/            operator-managed invalid or ambiguous material
```

See `docs/STORAGE_LAYOUT.md` and `docs/DATA_DICTIONARY.md` for partitioning, field meanings, units,
scales, and timestamps.

## Development verification

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy src
.venv/bin/pytest
```

The default tests are offline and use sanitized real API fixtures. Network smoke tests must be
explicitly selected.

## Troubleshooting

- `collector already running`: inspect `data/state/collector.lock` and the PID; stale locks are
  automatically renamed on startup.
- health says stale after a bounded run: expected because both sockets were gracefully closed.
- no CLOB messages: verify discovery selected active token IDs and inspect structured logs.
- normalization reports zero raw files: the files were already normalized or are still `.partial`.
- `verify-files` fails: do not delete anything; inspect its report in `data/manifests/`.
- disk emergency: collection closes raw streams and stops; it never deletes raw data automatically.

Full diagnosis steps are in `docs/TROUBLESHOOTING.md`.

## Documentation map

- `docs/ARCHITECTURE.md` — processes, queues, rollover, recovery, replay.
- `docs/API_NOTES.md` — behavior verified against official APIs on 2026-07-31.
- `docs/DATA_DICTIONARY.md` — schemas, fields, units, exact scales.
- `docs/DATA_QUALITY.md` — validation and replay eligibility.
- `docs/STORAGE_LAYOUT.md` — raw, Parquet, manifests, retention.
- `docs/OPERATIONS.md` — deployment and incident operations.
- `docs/BACKTESTING_ASSUMPTIONS.md` — execution and no-look-ahead assumptions.
- `docs/BACKTEST_METHOD.md` — episode indexing, settlement ground truth, execution realism, sweeps.
- `docs/LIVE_TRADING_LOGGING.md` — specification only for a future authenticated system.
- `docs/TROUBLESHOOTING.md` — symptom-oriented recovery.
- `docs/SERVER_ENVIRONMENT.md` — pre-installation server audit.
