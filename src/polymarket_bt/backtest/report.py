from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from polymarket_bt.backtest.engine import BacktestArtifacts
from polymarket_bt.backtest.metrics import percentile, profit_factor
from polymarket_bt.config import BacktestConfig, CollectorConfig
from polymarket_bt.constants import USDC_SCALE
from polymarket_bt.storage.manifest import ManifestStore


def _atomic_text(path: Path, text: str) -> None:
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _write_parquet(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(rows, schema=schema)
    temporary = path.with_suffix(".parquet.partial")
    pq.write_table(table, temporary, compression="zstd", compression_level=6)
    os.replace(temporary, path)


def _money(value: int) -> str:
    return f"{Decimal(value) / USDC_SCALE:.6f}"


def write_backtest_report(
    artifacts: BacktestArtifacts,
    backtest_config: BacktestConfig,
    collector_config: CollectorConfig,
    *,
    project_root: Path,
) -> Path:
    unique_run_id = str(uuid.uuid4())
    root = collector_config.storage.root / "reports" / unique_run_id
    root.mkdir(parents=True, exist_ok=False)
    _atomic_text(
        root / "config.yaml",
        yaml.safe_dump(backtest_config.model_dump(mode="json"), sort_keys=True),
    )
    environment = {
        "python": sys.version,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "random_seed": backtest_config.random_seed,
        "simulation_fingerprint": artifacts.backtest_run_id,
    }
    _atomic_text(root / "environment.json", json.dumps(environment, indent=2, sort_keys=True))
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        commit = "unavailable-no-git-repository"
    _atomic_text(root / "git_commit.txt", commit + "\n")
    manifest_entries = [
        entry.model_dump(mode="json")
        for entry in ManifestStore(collector_config.storage.root).entries()
    ]
    _atomic_text(
        root / "input_manifest.json", json.dumps(manifest_entries, indent=2, sort_keys=True)
    )
    quality = {
        "reject_on_gap": backtest_config.reject_on_gap,
        "degraded_intervals_used": [
            {
                "start_utc_ns": interval.start_utc_ns,
                "end_utc_ns": interval.end_utc_ns,
                "state": interval.state.value,
                "category": interval.category,
                "details": interval.details,
            }
            for interval in artifacts.quality_intervals_used
        ],
    }
    _atomic_text(root / "quality_report.json", json.dumps(quality, indent=2, sort_keys=True))

    order_rows: list[dict[str, Any]] = []
    fill_rows: list[dict[str, Any]] = []
    for result in artifacts.order_results:
        intent = result.intent
        order_rows.append(
            {
                "order_intent_id": intent.order_intent_id,
                "strategy_id": intent.strategy_id,
                "decision_timestamp_ns": intent.decision_timestamp_ns,
                "condition_id": intent.condition_id,
                "token_id": intent.token_id,
                "outcome": intent.outcome,
                "side": intent.side.value,
                "order_type": intent.order_type.value,
                "limit_price_scaled": intent.limit_price_scaled,
                "requested_shares_scaled": intent.requested_shares_scaled,
                "requested_notional_scaled": intent.requested_notional_scaled,
                "time_in_force": intent.time_in_force,
                "status": result.status,
                "rejection_reason": result.rejection_reason,
                "metadata_json": json.dumps(intent.metadata, sort_keys=True),
            }
        )
        fill_rows.extend(fill.model_dump(mode="json") for fill in result.fills)

    order_schema = pa.schema(
        [
            ("order_intent_id", pa.string()),
            ("strategy_id", pa.string()),
            ("decision_timestamp_ns", pa.int64()),
            ("condition_id", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("side", pa.string()),
            ("order_type", pa.string()),
            ("limit_price_scaled", pa.int64()),
            ("requested_shares_scaled", pa.int64()),
            ("requested_notional_scaled", pa.int64()),
            ("time_in_force", pa.string()),
            ("status", pa.string()),
            ("rejection_reason", pa.string()),
            ("metadata_json", pa.string()),
        ]
    )
    fill_schema = pa.schema(
        [
            ("fill_id", pa.string()),
            ("order_intent_id", pa.string()),
            ("condition_id", pa.string()),
            ("token_id", pa.string()),
            ("side", pa.string()),
            ("price_scaled", pa.int64()),
            ("size_scaled", pa.int64()),
            ("notional_scaled", pa.int64()),
            ("fee_scaled", pa.int64()),
            ("book_event_sequence", pa.int64()),
            ("decision_time_ns", pa.int64()),
            ("scheduled_arrival_time_ns", pa.int64()),
            ("fill_time_ns", pa.int64()),
            ("level_rank", pa.int32()),
            ("liquidity_remaining_after_scaled", pa.int64()),
        ]
    )
    portfolio_schema = pa.schema(
        [
            ("timestamp_ns", pa.int64()),
            ("event_type", pa.string()),
            ("token_id", pa.string()),
            ("cash_scaled", pa.int64()),
            ("inventory_scaled", pa.int64()),
            ("realized_pnl_scaled", pa.int64()),
            ("fees_scaled", pa.int64()),
        ]
    )
    market_schema = pa.schema(
        [
            ("condition_id", pa.string()),
            ("winning_token_id", pa.string()),
            ("winning_outcome", pa.string()),
            ("settlement_timestamp_ns", pa.int64()),
            ("payout_scaled", pa.int64()),
        ]
    )
    _write_parquet(root / "orders.parquet", order_rows, order_schema)
    _write_parquet(root / "fills.parquet", fill_rows, fill_schema)
    _write_parquet(root / "portfolio_events.parquet", artifacts.portfolio_events, portfolio_schema)
    _write_parquet(root / "market_results.parquet", artifacts.market_results, market_schema)

    summary = artifacts.summary.model_dump(mode="json")
    summary["backtest_run_id"] = unique_run_id
    summary["simulation_fingerprint"] = artifacts.backtest_run_id
    summary["starting_capital"] = _money(artifacts.summary.starting_capital_scaled)
    summary["ending_capital"] = _money(artifacts.summary.ending_capital_scaled)
    summary["net_pnl"] = _money(artifacts.summary.net_pnl_scaled)
    summary["gross_pnl"] = _money(artifacts.summary.gross_pnl_scaled)
    summary["fees"] = _money(artifacts.summary.fees_scaled)
    summary["return_on_starting_capital_ppm"] = (
        artifacts.summary.net_pnl_scaled
        * 1_000_000
        // max(artifacts.summary.starting_capital_scaled, 1)
    )
    summary["median_slippage_scaled"] = percentile(artifacts.slippages_scaled, 0.50)
    summary["p95_slippage_scaled"] = percentile(artifacts.slippages_scaled, 0.95)
    summary["latency_assumptions"] = backtest_config.latency.model_dump(mode="json")
    summary["fee_configuration"] = backtest_config.fee.model_dump(mode="json")
    summary["queue_model"] = backtest_config.maker_queue_model
    summary["win_rate_by_market"] = None
    summary["win_rate_by_trade"] = None
    summary["average_win_scaled"] = None
    summary["average_loss_scaled"] = None
    summary["profit_factor"] = profit_factor([])
    summary["maximum_concurrent_exposure_scaled"] = max(
        (abs(int(row["inventory_scaled"])) for row in artifacts.portfolio_events),
        default=0,
    )
    summary["time_in_market_ns"] = 0
    summary["up_exposure_scaled"] = sum(
        int(row["requested_shares_scaled"] or 0) for row in order_rows if row["outcome"] == "UP"
    )
    summary["down_exposure_scaled"] = sum(
        int(row["requested_shares_scaled"] or 0) for row in order_rows if row["outcome"] == "DOWN"
    )
    _atomic_text(root / "summary.json", json.dumps(summary, indent=2, sort_keys=True))
    metrics_schema = pa.schema(
        [
            ("backtest_run_id", pa.string()),
            ("starting_capital_scaled", pa.int64()),
            ("ending_capital_scaled", pa.int64()),
            ("net_pnl_scaled", pa.int64()),
            ("fees_scaled", pa.int64()),
            ("fills", pa.int64()),
            ("notional_volume_scaled", pa.int64()),
            ("maximum_drawdown_scaled", pa.int64()),
            ("fill_ratio_ppm", pa.int64()),
        ]
    )
    _write_parquet(
        root / "daily_or_session_metrics.parquet",
        [
            {
                "backtest_run_id": unique_run_id,
                "starting_capital_scaled": artifacts.summary.starting_capital_scaled,
                "ending_capital_scaled": artifacts.summary.ending_capital_scaled,
                "net_pnl_scaled": artifacts.summary.net_pnl_scaled,
                "fees_scaled": artifacts.summary.fees_scaled,
                "fills": artifacts.summary.fills,
                "notional_volume_scaled": artifacts.summary.notional_volume_scaled,
                "maximum_drawdown_scaled": artifacts.summary.maximum_drawdown_scaled,
                "fill_ratio_ppm": artifacts.summary.fill_ratio_ppm,
            }
        ],
        metrics_schema,
    )
    report = f"""# Backtest report

- Run ID: `{unique_run_id}`
- Simulation fingerprint: `{artifacts.backtest_run_id}`
- Strategy: `{artifacts.summary.strategy_id}`
- Replay clock: `{backtest_config.replay_clock}`
- Starting capital: `{summary["starting_capital"]}` USDC
- Ending capital: `{summary["ending_capital"]}` USDC
- Net P&L: `{summary["net_pnl"]}` USDC
- Fees: `{summary["fees"]}` USDC
- Order intents: `{artifacts.summary.order_intents}`
- Fills: `{artifacts.summary.fills}`
- Partial fills: `{artifacts.summary.partial_fills}`
- Maximum drawdown: `{_money(artifacts.summary.maximum_drawdown_scaled)}` USDC
- Degraded intervals used: `{len(artifacts.quality_intervals_used)}`

The example threshold strategy, when selected, is demonstrative only and is not a
recommendation or claim of profitability. Maker results are experimental because public
Level-2 data does not expose individual queue positions.
"""
    _atomic_text(root / "report.md", report)
    return root
