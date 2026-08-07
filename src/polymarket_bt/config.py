from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DiscoveryConfig(StrictModel):
    asset: str = "BTC"
    duration_minutes: int = 15
    normal_poll_seconds: float = 30
    boundary_poll_seconds: float = 5
    boundary_window_before_seconds: int = 90
    boundary_window_after_seconds: int = 30
    minimum_match_score: float = Field(default=0.90, ge=0, le=1)
    candidate_intervals_before: int = 1
    candidate_intervals_after: int = 2


class ClobConfig(StrictModel):
    rest_base_url: str
    websocket_url: str
    heartbeat_seconds: float = 10
    heartbeat_timeout_seconds: float = 5
    max_missed_heartbeats: int = 3
    custom_feature_enabled: bool = True
    validation_snapshot_seconds: float = 30
    market_metadata_refresh_seconds: float = Field(default=30, gt=0)
    request_timeout_seconds: float = 10


class RtdsConfig(StrictModel):
    websocket_url: str
    heartbeat_seconds: float = 5
    heartbeat_timeout_seconds: float = 5
    max_missed_heartbeats: int = 3
    binance_symbols: list[str] = Field(default_factory=lambda: ["btcusdt"])
    chainlink_symbols: list[str] = Field(default_factory=lambda: ["btc/usd"])


class StorageConfig(StrictModel):
    root: Path = Path("./data")
    raw_zstd_level: int = 3
    parquet_zstd_level: int = 6
    rotate_minutes: int = 15
    rotate_uncompressed_mb: int = 128
    writer_batch_events: int = 1_000
    writer_batch_wait_ms: int = 100
    flush_seconds: float = 1
    fsync_seconds: float = 5
    parquet_target_mb: int = 128


class QueueConfig(StrictModel):
    raw_max_events: int = 100_000
    normalization_max_events: int = 100_000
    book_max_events: int = 100_000
    high_watermark_fraction: float = Field(default=0.80, gt=0, lt=1)


class ReconnectConfig(StrictModel):
    initial_seconds: float = 0.5
    maximum_seconds: float = 30
    stable_reset_seconds: float = 60
    jitter: bool = True


class MonitoringConfig(StrictModel):
    health_bind: str = "127.0.0.1"
    health_port: int = 9108
    metrics_enabled: bool = True
    log_level: str = "INFO"
    status_file: Path = Path("./data/state/status.json")
    warning_free_gb: float = 20
    critical_free_gb: float = 10
    emergency_free_gb: float = 5

    @model_validator(mode="after")
    def disk_threshold_order(self) -> MonitoringConfig:
        if not self.warning_free_gb > self.critical_free_gb > self.emergency_free_gb:
            raise ValueError("disk thresholds must satisfy warning > critical > emergency")
        return self


class ShutdownConfig(StrictModel):
    drain_timeout_seconds: float = 30
    fsync_on_close: bool = True


class CollectorConfig(StrictModel):
    environment: Literal["production-data-only"] = "production-data-only"
    timezone: Literal["UTC"] = "UTC"
    collector_version: str = "0.1.0"
    discovery: DiscoveryConfig
    clob: ClobConfig
    rtds: RtdsConfig
    storage: StorageConfig = Field(default_factory=StorageConfig)
    queues: QueueConfig = Field(default_factory=QueueConfig)
    reconnect: ReconnectConfig = Field(default_factory=ReconnectConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    shutdown: ShutdownConfig = Field(default_factory=ShutdownConfig)
    smoke_test_seconds: int = 30


class LatencyConfig(StrictModel):
    model: Literal["constant", "empirical", "lognormal", "disabled"] = "constant"
    constant_ms: int = 50
    samples_ms: list[int] = Field(default_factory=list)
    lognormal_mu: float = 3.5
    lognormal_sigma: float = 0.4


class FeeConfig(StrictModel):
    version: str
    effective_start: str
    effective_end: str | None = None
    market_type: str = "crypto"
    liquidity_role: Literal["taker", "maker"] = "taker"
    formula: Literal["shares_rate_p_one_minus_p", "zero"]
    rate: str
    exponent: int = 1
    minimum_fee: str = "0.000001"
    rounding: Literal["half_up", "floor", "ceiling"] = "half_up"


class BacktestConfig(StrictModel):
    strategy: str = "no_op"
    strategy_parameters: dict[str, object] = Field(default_factory=dict)
    starting_cash: str = "1000.000000"
    replay_clock: Literal["exchange_time", "local_receive_time"] = "local_receive_time"
    reject_on_gap: bool = True
    random_seed: int = 1729
    allow_negative_cash: bool = False
    allow_short_positions: bool = False
    inventory_method: Literal["weighted_average"] = "weighted_average"
    resolution_mode: Literal["real_time_resolution_mode", "ex_post_resolution_mode"] = (
        "real_time_resolution_mode"
    )
    latency: LatencyConfig
    fee: FeeConfig
    maker_queue_model: Literal["optimistic", "conservative", "proportional"] = "conservative"


def _read_yaml(path: Path) -> dict[str, object]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError(f"configuration must be a mapping: {path}")
    return loaded


def load_collector_config(path: Path) -> CollectorConfig:
    resolved = path.expanduser().resolve()
    config = CollectorConfig.model_validate(_read_yaml(resolved))
    project_root = resolved.parent.parent
    storage_override = os.getenv("POLYMARKET_BT_STORAGE_ROOT")
    storage_root = Path(storage_override) if storage_override else config.storage.root
    if not storage_root.is_absolute():
        storage_root = (project_root / storage_root).resolve()
    config.storage.root = storage_root
    status_override = os.getenv("POLYMARKET_BT_STATUS_FILE")
    status_file = Path(status_override) if status_override else config.monitoring.status_file
    if not status_file.is_absolute():
        status_file = (project_root / status_file).resolve()
    config.monitoring.status_file = status_file
    if log_level := os.getenv("POLYMARKET_BT_LOG_LEVEL"):
        config.monitoring.log_level = log_level.upper()
    if health_bind := os.getenv("POLYMARKET_BT_HEALTH_BIND"):
        config.monitoring.health_bind = health_bind
    return config


def load_backtest_config(path: Path) -> BacktestConfig:
    return BacktestConfig.model_validate(_read_yaml(path.expanduser().resolve()))
