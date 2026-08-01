from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import shutil
import signal
import sys
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer

from polymarket_bt.backtest.engine import BacktestEngine
from polymarket_bt.backtest.strategy import Strategy
from polymarket_bt.config import (
    CollectorConfig,
    load_backtest_config,
    load_collector_config,
)
from polymarket_bt.discovery.btc_market_matcher import Btc15mMarketMatcher
from polymarket_bt.discovery.gamma_client import GammaClient
from polymarket_bt.discovery.market_registry import MarketRegistry
from polymarket_bt.ingestion.data_api import DataApiClient
from polymarket_bt.ingestion.supervisor import CollectorSupervisor
from polymarket_bt.models.trades import TradeEvent
from polymarket_bt.monitoring.logging import configure_logging
from polymarket_bt.normalization.reconciliation import (
    reconcile_trades,
    reconciliation_report_json,
)
from polymarket_bt.orderbook.reconstructor import BookReconstructor
from polymarket_bt.storage.manifest import ManifestStore
from polymarket_bt.strategies import ExampleThresholdStrategy, NoOpStrategy

app = typer.Typer(
    name="polymarket-bt",
    help="Public-data-only Polymarket BTC recorder and deterministic backtester.",
    no_args_is_help=True,
)

ConfigOption = Annotated[
    Path,
    typer.Option("--config", "-c", help="Collector YAML configuration file."),
]


def _config(path: Path) -> CollectorConfig:
    config = load_collector_config(path)
    configure_logging(config.monitoring.log_level)
    return config


def _emit(payload: Any) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True, default=str))


@app.command()
def doctor(config: ConfigOption = Path("configs/collector.yaml")) -> None:
    """Check runtime, storage, clock, public endpoints, and safety boundaries."""
    cfg = _config(config)

    async def checks() -> dict[str, Any]:
        endpoints = {
            "gamma": "https://gamma-api.polymarket.com/status",
            "clob": f"{cfg.clob.rest_base_url}/ok",
            "data_api": "https://data-api.polymarket.com/",
        }
        results: dict[str, Any] = {}
        async with httpx.AsyncClient(timeout=5) as client:
            for name, url in endpoints.items():
                try:
                    response = await client.get(url)
                    results[name] = {"status": response.status_code, "reachable": True}
                except httpx.HTTPError as exc:
                    results[name] = {
                        "reachable": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
        return results

    usage = shutil.disk_usage(cfg.storage.root)
    synchronized_marker = Path("/run/systemd/timesync/synchronized").exists()
    payload: dict[str, Any] = {
        "python": sys.version.split()[0],
        "python_supported": sys.version_info >= (3, 12),
        "timezone_storage": cfg.timezone,
        "system_utc": datetime.now(tz=UTC).isoformat(),
        "clock_sync_marker": synchronized_marker,
        "storage_root": str(cfg.storage.root),
        "disk_free_bytes": usage.free,
        "disk_above_emergency": usage.free > int(cfg.monitoring.emergency_free_gb * 1024**3),
        "health_bind": f"{cfg.monitoring.health_bind}:{cfg.monitoring.health_port}",
        "public_data_only": cfg.environment == "production-data-only",
        "credential_fields_present": False,
        "endpoints": asyncio.run(checks()),
    }
    payload["ok"] = bool(
        payload["python_supported"]
        and payload["disk_above_emergency"]
        and payload["public_data_only"]
        and all(item.get("reachable") for item in payload["endpoints"].values())
    )
    _emit(payload)
    if not payload["ok"]:
        raise typer.Exit(2)


@app.command()
def discover(
    config: ConfigOption = Path("configs/collector.yaml"),
    asset: Annotated[str, typer.Option(help="Asset symbol.")] = "BTC",
    duration: Annotated[str, typer.Option(help="Expected duration, e.g. 15m.")] = "15m",
) -> None:
    """Discover and score current/upcoming markets without changing collector state."""
    cfg = _config(config)
    cfg.discovery.asset = asset.upper()
    if not duration.endswith("m") or not duration[:-1].isdigit():
        raise typer.BadParameter("duration must look like 15m")
    cfg.discovery.duration_minutes = int(duration[:-1])

    async def run() -> list[dict[str, Any]]:
        client = GammaClient(cfg.discovery)
        matcher = Btc15mMarketMatcher(cfg.discovery)
        try:
            responses = await client.discover_responses()
            decisions: list[dict[str, Any]] = []
            for event, _response in client.extract_events(responses):
                for decision in matcher.match_event(event):
                    if decision.accepted and decision.market:
                        decisions.append(decision.market.model_dump(mode="json"))
            return decisions
        finally:
            await client.close()

    markets = asyncio.run(run())
    _emit({"count": len(markets), "markets": markets})
    if not markets:
        raise typer.Exit(3)


@app.command()
def collect(
    config: ConfigOption = Path("configs/collector.yaml"),
    once: Annotated[bool, typer.Option("--once", help="Run a bounded smoke capture.")] = False,
    duration: Annotated[
        float | None,
        typer.Option(help="Optional bounded collection duration in seconds."),
    ] = None,
) -> None:
    """Run the resilient public-data-only collector."""
    cfg = _config(config)

    async def run() -> dict[str, Any]:
        supervisor = CollectorSupervisor(cfg)
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, supervisor.request_stop)
            except NotImplementedError:
                pass
        bounded = duration if duration is not None else cfg.smoke_test_seconds if once else None
        return await supervisor.run(bounded)

    _emit(asyncio.run(run()))


@app.command()
def status(config: ConfigOption = Path("configs/collector.yaml")) -> None:
    """Show the last atomically published collector health/status snapshot."""
    cfg = _config(config)
    if not cfg.monitoring.status_file.exists():
        _emit({"state": "not_started", "status_file": str(cfg.monitoring.status_file)})
        raise typer.Exit(3)
    typer.echo(cfg.monitoring.status_file.read_text(encoding="utf-8"))


@app.command()
def dashboard(
    config: ConfigOption = Path("configs/collector.yaml"),
    host: Annotated[str, typer.Option(help="Dashboard bind address.")] = "127.0.0.1",
    port: Annotated[int, typer.Option(help="Dashboard HTTP port.")] = 9110,
    index: Annotated[Path, typer.Option(help="Single-file dashboard HTML.")] = Path(
        "web/index.html"
    ),
) -> None:
    """Serve the read-only market explorer and JSON API."""
    cfg = _config(config)
    from polymarket_bt.dashboard.server import run_dashboard

    run_dashboard(
        storage_root=cfg.storage.root,
        index_path=index.expanduser().resolve(),
        host=host,
        port=port,
    )


@app.command("dashboard-reindex")
def dashboard_reindex(config: ConfigOption = Path("configs/collector.yaml")) -> None:
    """Rebuild the fast chart index from normalized top-of-book Parquet."""
    cfg = _config(config)
    from polymarket_bt.dashboard.cache import DashboardCacheWriter

    _emit(DashboardCacheWriter(cfg.storage.root).rebuild())


@app.command()
def normalize(
    config: ConfigOption = Path("configs/collector.yaml"),
    date: Annotated[str | None, typer.Option(help="UTC date YYYY-MM-DD.")] = None,
    max_files: Annotated[
        int | None,
        typer.Option(help="Bound one run to this many finalized raw files."),
    ] = None,
    newest_first: Annotated[
        bool,
        typer.Option(
            "--newest-first/--oldest-first",
            help="Prioritize fresh dashboard data or drain the oldest backlog first.",
        ),
    ] = True,
) -> None:
    """Idempotently normalize finalized raw archives into Parquet."""
    cfg = _config(config)
    from polymarket_bt.normalization.normalizer import Normalizer

    normalizer = Normalizer(cfg)
    try:
        _emit(
            normalizer.normalize(
                date=date,
                max_files=max_files,
                newest_first=newest_first,
            )
        )
    finally:
        normalizer.close()


@app.command()
def compact(
    config: ConfigOption = Path("configs/collector.yaml"),
    dataset: Annotated[str | None, typer.Option(help="One dataset; defaults to all.")] = None,
) -> None:
    """Merge small Parquet files into validated compacted outputs, retaining originals."""
    cfg = _config(config)
    from polymarket_bt.storage.parquet_writer import ParquetDatasetWriter

    writer = ParquetDatasetWriter(cfg, ManifestStore(cfg.storage.root))
    datasets = [dataset] if dataset else list(writer.datasets())
    outputs: list[str] = []
    for name in datasets:
        outputs.extend(str(path) for path in writer.compact(name))
    _emit({"outputs": outputs, "originals_retained": True})


def _normalized_trades(root: Path, condition_id: str) -> list[TradeEvent]:
    import pyarrow.parquet as pq

    trades: list[TradeEvent] = []
    directory = root / "normalized" / "trades"
    if not directory.exists():
        return trades
    for path in directory.rglob("*.parquet"):
        for row in pq.ParquetFile(path).read().to_pylist():
            if row["condition_id"] == condition_id:
                trades.append(TradeEvent.model_validate(row))
    return trades


@app.command("reconcile-trades")
def reconcile_trades_command(
    market: Annotated[str, typer.Option("--market", help="Condition ID.")],
    config: ConfigOption = Path("configs/collector.yaml"),
) -> None:
    """Fetch public Data API trades and classify WebSocket/API differences."""
    cfg = _config(config)

    async def run() -> str:
        manifest = ManifestStore(cfg.storage.root)
        from polymarket_bt.storage.raw_writer import RawArchive

        archive = RawArchive(cfg, manifest)
        sequence = itertools.count(max(1, int(datetime.now(tz=UTC).timestamp() * 1_000_000_000)))
        await archive.start()
        client = DataApiClient(cfg, archive, lambda: next(sequence), str(uuid.uuid4()))
        try:
            return await client.fetch_market_trades(market)
        finally:
            await client.close()
            await archive.stop()

    raw = asyncio.run(run())
    results = reconcile_trades(_normalized_trades(cfg.storage.root, market), raw)
    report_dir = cfg.storage.root / "reports" / "trade-reconciliation"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = report_dir / f"{market}-{datetime.now(tz=UTC):%Y%m%dT%H%M%SZ}.json"
    report.write_text(reconciliation_report_json(results), encoding="utf-8")
    _emit({"report": str(report), "records": len(results)})


@app.command("validate-books")
def validate_books(
    config: ConfigOption = Path("configs/collector.yaml"),
    market: Annotated[str | None, typer.Option("--market", help="Condition ID.")] = None,
) -> None:
    """Rebuild books deterministically and report invariant failures."""
    cfg = _config(config)
    from polymarket_bt.replay.event_reader import EventReader

    events = EventReader(cfg.storage.root).read_events(condition_id=market)
    reconstructor = BookReconstructor()
    failures: list[dict[str, Any]] = []
    for event in sorted(events, key=lambda item: (item.sequence, item.parent_change_index)):
        try:
            if event.event_type == "book_snapshot":
                reconstructor.apply_snapshot(event.payload)
            elif event.event_type == "book_update":
                reconstructor.apply_change(event.payload)
            elif event.event_type == "tick_size_change":
                reconstructor.apply_tick_size_change(event.payload)
        except Exception as exc:
            failures.append(
                {
                    "sequence": event.sequence,
                    "event_type": event.event_type,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
    payload = {
        "books": len(reconstructor.books),
        "valid_books": sum(book.valid for book in reconstructor.books.values()),
        "failures": failures,
        "ok": not failures,
    }
    _emit(payload)
    if failures:
        raise typer.Exit(4)


@app.command("validate-data")
def validate_data(
    config: ConfigOption = Path("configs/collector.yaml"),
    market: Annotated[str | None, typer.Option("--market", help="Condition ID.")] = None,
) -> None:
    """Run file- and market-level data-quality validation and write a Markdown report."""
    cfg = _config(config)
    from polymarket_bt.replay.event_reader import EventReader

    verification = ManifestStore(cfg.storage.root).verify()
    reader = EventReader(cfg.storage.root)
    events = reader.read_events(condition_id=market)
    counts = Counter(event.event_type for event in events)
    quality = reader.quality_intervals(condition_id=market)
    now = datetime.now(tz=UTC)
    report_dir = cfg.storage.root / "reports" / "data-quality" / f"{now:%Y-%m-%d}"
    report_dir.mkdir(parents=True, exist_ok=True)
    report = report_dir / "quality-report.md"
    body = f"""# Data quality report

Generated: {now.isoformat()}

- Market filter: `{market or "all"}`
- Manifest verification: `{verification["ok"]}`
- Events: `{len(events)}`
- Book snapshots: `{counts["book_snapshot"]}`
- Book updates: `{counts["book_update"]}`
- Trades: `{counts["trade"]}`
- BTC prices: `{counts["btc_price"]}`
- Resolution events: `{counts["market_resolution"]}`
- Quality intervals: `{len(quality)}`
- Missing/corrupt/overlapping files: `{sum(not (row["exists"] and row["checksum_ok"] and row["readable"]) for row in verification["files"]) + len(verification["overlaps"])}`
"""
    report.write_text(body, encoding="utf-8")
    _emit({"report": str(report), "manifest_ok": verification["ok"], "event_counts": counts})
    if not verification["ok"]:
        raise typer.Exit(5)


@app.command("verify-files")
def verify_files(config: ConfigOption = Path("configs/collector.yaml")) -> None:
    """Recalculate checksums and check file readability without mutating data."""
    cfg = _config(config)
    store = ManifestStore(cfg.storage.root)
    report = store.write_verification_report()
    result = store.verify()
    result["report"] = str(report)
    _emit(result)
    if not result["ok"]:
        raise typer.Exit(5)


@app.command("inspect-market")
def inspect_market(
    condition_id: Annotated[str, typer.Option("--condition-id", help="Condition ID.")],
    config: ConfigOption = Path("configs/collector.yaml"),
) -> None:
    """Inspect the persisted explicit token/outcome mapping."""
    cfg = _config(config)
    registry = MarketRegistry(cfg.storage.root / "state" / "market-registry.sqlite")
    try:
        market = registry.get(condition_id)
    finally:
        registry.close()
    if market is None:
        _emit({"error": "market not found", "condition_id": condition_id})
        raise typer.Exit(3)
    _emit(market.model_dump(mode="json"))


@app.command()
def replay(
    config: ConfigOption = Path("configs/collector.yaml"),
    market: Annotated[str | None, typer.Option("--market", help="Condition ID.")] = None,
    clock: Annotated[
        str, typer.Option(help="local_receive_time or exchange_time.")
    ] = "local_receive_time",
) -> None:
    """Read and deterministically order normalized events without a strategy."""
    cfg = _config(config)
    from polymarket_bt.constants import ReplayClockMode
    from polymarket_bt.replay.event_reader import EventReader
    from polymarket_bt.replay.merger import merge_events

    mode = ReplayClockMode(clock)
    events = merge_events(EventReader(cfg.storage.root).read_events(condition_id=market), mode)
    digest = hashlib.sha256(
        "\n".join(
            f"{event.timestamp(mode)}:{event.connection_id}:{event.sequence}:{event.parent_change_index}:{event.event_type}"
            for event in events
        ).encode()
    ).hexdigest()
    _emit(
        {
            "events": len(events),
            "first_timestamp_ns": events[0].timestamp(mode) if events else None,
            "last_timestamp_ns": events[-1].timestamp(mode) if events else None,
            "ordering_sha256": digest,
        }
    )


@app.command()
def backtest(
    config: Annotated[
        Path, typer.Option("--config", "-c", help="Backtest YAML configuration file.")
    ] = Path("configs/backtest.example.yaml"),
    collector_config: Annotated[
        Path, typer.Option(help="Collector config locating normalized inputs.")
    ] = Path("configs/collector.yaml"),
    strategy: Annotated[str | None, typer.Option(help="Override strategy name.")] = None,
    market: Annotated[str | None, typer.Option(help="Optional condition ID.")] = None,
) -> None:
    """Run an event-driven, shared-wallet, no-look-ahead simulation."""
    collector_cfg = _config(collector_config)
    from polymarket_bt.backtest.report import write_backtest_report
    from polymarket_bt.replay.event_reader import EventReader

    backtest_cfg = load_backtest_config(config)
    selected = strategy or backtest_cfg.strategy
    if selected == "no_op":
        strategy_object: Strategy = NoOpStrategy()
    elif selected == "example_threshold":
        parameters = backtest_cfg.strategy_parameters
        strategy_object = ExampleThresholdStrategy(
            threshold_ppm=int(str(parameters.get("threshold_ppm", 100))),
            shares_scaled=int(str(parameters.get("shares_scaled", 5_000_000))),
        )
    else:
        raise typer.BadParameter(f"unknown strategy: {selected}")
    reader = EventReader(collector_cfg.storage.root)
    events = reader.read_events(condition_id=market)
    engine = BacktestEngine(
        backtest_cfg,
        strategy_object,
        quality_intervals=reader.quality_intervals(condition_id=market),
    )
    artifacts = engine.run(events)
    output = write_backtest_report(
        artifacts,
        backtest_cfg,
        collector_cfg,
        project_root=collector_config.resolve().parent.parent,
    )
    persisted_summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    _emit({"report_directory": str(output), "summary": persisted_summary})


@app.command()
def report(
    run_id: Annotated[str, typer.Argument(help="Backtest report run ID.")],
    config: ConfigOption = Path("configs/collector.yaml"),
) -> None:
    """Print an existing backtest summary."""
    cfg = _config(config)
    path = cfg.storage.root / "reports" / run_id / "summary.json"
    if not path.exists():
        _emit({"error": "report not found", "run_id": run_id})
        raise typer.Exit(3)
    typer.echo(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    app()
