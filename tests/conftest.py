from __future__ import annotations

import json
from pathlib import Path

import pytest

from polymarket_bt.config import CollectorConfig, load_collector_config
from polymarket_bt.discovery.btc_market_matcher import Btc15mMarketMatcher
from polymarket_bt.models.markets import MarketRecord

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures"


@pytest.fixture
def fixture_root() -> Path:
    return FIXTURES


@pytest.fixture
def collector_config(tmp_path: Path) -> CollectorConfig:
    config = load_collector_config(PROJECT_ROOT / "configs" / "collector.example.yaml")
    config.storage.root = tmp_path / "data"
    config.monitoring.status_file = tmp_path / "data" / "state" / "status.json"
    config.monitoring.metrics_enabled = False
    config.storage.rotate_minutes = 60
    config.storage.rotate_uncompressed_mb = 128
    config.queues.raw_max_events = 100
    return config


@pytest.fixture
def market(collector_config: CollectorConfig, fixture_root: Path) -> MarketRecord:
    event = json.loads((fixture_root / "gamma" / "current_btc_15m.json").read_text())
    decisions = Btc15mMarketMatcher(collector_config.discovery).match_event(event)
    assert decisions[0].market is not None
    return decisions[0].market
