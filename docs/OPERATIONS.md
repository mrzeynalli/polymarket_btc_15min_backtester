# Operations

## Runtime locations

The source workspace is `/root/polymarket/btc_15min_bot/polymarket-btc-backtester`. The hardened
systemd deployment uses immutable release copies under `/opt/polymarket-btc-backtester/releases/`, a
`current` symlink, and persistent data under `/var/lib/polymarket-btc-backtester`. This lets the
service run as dedicated user `polymarket-data` without granting access to `/root`.

All endpoints are public. No credential file is needed or accepted.

## Preflight

Run from the source workspace:

```bash
bash scripts/install.sh
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy src
.venv/bin/pytest
.venv/bin/polymarket-bt doctor --config configs/collector.yaml
.venv/bin/polymarket-bt collect --once --duration 60 --config configs/collector.yaml
.venv/bin/polymarket-bt normalize --config configs/collector.yaml
.venv/bin/polymarket-bt verify-files --config configs/collector.yaml
.venv/bin/polymarket-bt validate-books --config configs/collector.yaml
```

Do not enable persistence until all commands succeed, the data path exists, free disk is above the
warning threshold, both token snapshots are valid, both RTDS sources appear, and raw drops are zero.

## Install systemd deployment

After preflight:

```bash
sudo bash scripts/install_systemd.sh --start
```

The installer creates an unprivileged system account when absent, copies a timestamped release,
builds its isolated virtual environment from pinned dependencies, installs the unit and external
environment file, atomically switches `current`, reloads systemd, and—only with `--start`—enables and
starts the service.

Important unit properties:

- `Restart=on-failure`, five-second delay, boot enablement;
- SIGTERM and 45-second stop budget;
- `NoNewPrivileges`, empty capability set, private devices/tmp, strict filesystem protection;
- only `/var/lib/polymarket-btc-backtester` writable;
- file-descriptor limit 65,536;
- IPv4/IPv6/Unix sockets only;
- external environment at `/etc/polymarket-collector.env`.

The production environment file contains only:

```text
POLYMARKET_BT_STORAGE_ROOT=/var/lib/polymarket-btc-backtester
POLYMARKET_BT_STATUS_FILE=/var/lib/polymarket-btc-backtester/state/status.json
POLYMARKET_BT_LOG_LEVEL=INFO
PYTHONUNBUFFERED=1
```

## Start, stop, and inspect

```bash
sudo systemctl start polymarket-collector.service
sudo systemctl stop polymarket-collector.service
sudo systemctl restart polymarket-collector.service
systemctl is-active polymarket-collector.service
systemctl is-enabled polymarket-collector.service
sudo journalctl -u polymarket-collector.service --since '30 minutes ago'
```

Stopping sends SIGTERM. A normal shutdown closes both sockets, drains queues, finalizes Zstandard
frames, hashes/manifests files, checkpoints sequence state, records a clean run, and removes the lock.
Do not use SIGKILL except when the documented stop timeout has elapsed and the filesystem is stable.

## Health and metrics

The service binds only `127.0.0.1:9108`:

```bash
curl -sS http://127.0.0.1:9108/health
curl -sS http://127.0.0.1:9108/metrics
sudo -u polymarket-data env \
  POLYMARKET_BT_STORAGE_ROOT=/var/lib/polymarket-btc-backtester \
  POLYMARKET_BT_STATUS_FILE=/var/lib/polymarket-btc-backtester/state/status.json \
  /opt/polymarket-btc-backtester/current/.venv/bin/polymarket-bt \
  status --config /opt/polymarket-btc-backtester/current/configs/collector.yaml
```

Health is `healthy`, `degraded`, or `unhealthy`. Investigate stale Gamma, CLOB, RTDS, or REST times;
disconnected sockets; invalid books; any raw drop; queue pressure; unsynchronized clock; and disk
thresholds. The status file is
`/var/lib/polymarket-btc-backtester/state/status.json`.

## Disk protection

Defaults are 20 GiB warning, 10 GiB critical, and 5 GiB emergency. At warning, health degrades and
operators should archive verified closed partitions. At critical, stop optional normalization and
compaction. At emergency, the collector initiates safe shutdown. It never deletes raw data.

Check usage:

```bash
df -h /var/lib/polymarket-btc-backtester
du -sh /var/lib/polymarket-btc-backtester/*
```

Retention is explicit: stop any job touching the selected partitions, run `verify-files`, copy raw
files plus manifests to durable off-host storage, verify SHA-256 there, then archive originals. Raw
deletion is intentionally not implemented as an automatic command.

## Routine normalization and validation

Run low-priority offline jobs as the service user against the external root:

```bash
sudo -u polymarket-data env \
  POLYMARKET_BT_STORAGE_ROOT=/var/lib/polymarket-btc-backtester \
  /opt/polymarket-btc-backtester/current/.venv/bin/polymarket-bt normalize \
  --config /opt/polymarket-btc-backtester/current/configs/collector.yaml

sudo -u polymarket-data env \
  POLYMARKET_BT_STORAGE_ROOT=/var/lib/polymarket-btc-backtester \
  /opt/polymarket-btc-backtester/current/.venv/bin/polymarket-bt verify-files \
  --config /opt/polymarket-btc-backtester/current/configs/collector.yaml
```

Schedule normalization separately from the collector; do not make collector health depend on
Parquet. `compact` is also offline and should be suppressed below the critical disk threshold.

## Crash and disconnect recovery

On process restart, the collector creates a new run UUID, links the previous run in SQLite, checks
the lock PID, renames stale locks, scans `.partial` files, preserves abandoned originals, and recovers
only complete JSON lines. It resumes sequences above the prior checkpoint and never appends to a
finalized file.

After socket reconnect it resubscribes all desired token IDs, marks prior book state uncertain, and
fetches complete REST snapshots. A reconnect never claims that the disconnected period is complete.
If reconnect storms persist, keep raw files, inspect DNS/TLS/clock and Polymarket status, and do not
disable backoff.

## Backups

At minimum back up:

- finalized `raw/` partitions;
- `manifests/file-manifests.jsonl` and verification reports;
- market registry and normalization checkpoint SQLite files (including WAL/SHM when live);
- backtest configuration/report bundles.

Use `scripts/backup_manifests.sh <destination>` for a timestamped manifest copy. For consistent
SQLite backup, use SQLite's `.backup` operation or stop the collector briefly; do not copy only the
main database while an uncheckpointed WAL exists. Validate restores on another path before relying
on them.

## Safe upgrade

1. Keep the current service running while offline tests and a bounded capture pass in the source tree.
2. Revalidate `docs/API_NOTES.md` against official docs and refresh sanitized fixtures.
3. Run `sudo bash scripts/install_systemd.sh` without `--start`; this stages and switches a release
   but does not alter a running process until restart.
4. Note the prior `readlink /opt/polymarket-btc-backtester/current`.
5. Restart the unit during a boundary-safe window and watch health, messages, drops, and snapshots.
6. Normalize/verify the first finalized new-run files.

Data and manifests remain outside releases, so switching code never overwrites them.

## Rollback

If the new release fails:

```bash
sudo systemctl stop polymarket-collector.service
sudo ln -sfn /opt/polymarket-btc-backtester/releases/<previous-release> \
  /opt/polymarket-btc-backtester/current
sudo systemctl start polymarket-collector.service
```

Do not roll back data schemas in place. Older code must be able to ignore newer raw messages; if it
cannot, keep collecting raw only or stage a separate normalized root. Verify the first post-rollback
archive and retain the failed release for diagnosis.

## Docker Compose

```bash
sudo chown -R 65532:65532 data
docker compose build
docker compose up -d collector
docker compose ps
docker compose logs -f collector
```

Inside the container health binds `0.0.0.0` only because Compose publishes it exclusively on host
`127.0.0.1`. The root filesystem is read-only, `/tmp` is tmpfs, and the process runs as UID/GID
65532. The host bind mount is the persistence boundary.

## Log rotation

Under systemd, structured stdout/stderr is retained by journald policy. The supplied logrotate file
supports deployments that redirect to `/var/log/polymarket-collector/*.log`; it uses `copytruncate`
only for application logs, never for raw archives. Raw rotation is owned by the collector.
