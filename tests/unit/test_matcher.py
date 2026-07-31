from __future__ import annotations

import copy
import json
from pathlib import Path

from polymarket_bt.config import CollectorConfig
from polymarket_bt.discovery.btc_market_matcher import Btc15mMarketMatcher


def test_current_gamma_fixture_matches(
    collector_config: CollectorConfig, fixture_root: Path
) -> None:
    event = json.loads((fixture_root / "gamma" / "current_btc_15m.json").read_text())
    decision = Btc15mMarketMatcher(collector_config.discovery).match_event(event)[0]
    assert decision.accepted
    assert decision.score == 1.0
    assert decision.market is not None
    assert decision.market.up_outcome_label == "Up"
    assert decision.market.up_token_id.startswith("345147")
    assert json.loads(decision.market.fee_fields_json)["feeSchedule"]["rate"] == 0.07


def test_mapping_uses_labels_not_array_position(
    collector_config: CollectorConfig, fixture_root: Path
) -> None:
    event = json.loads((fixture_root / "gamma" / "current_btc_15m.json").read_text())
    event["markets"][0]["outcomes"] = '["Down", "Up"]'
    event["markets"][0]["clobTokenIds"] = f'["{"2" * 30}", "{"3" * 30}"]'
    decision = Btc15mMarketMatcher(collector_config.discovery).match_event(event)[0]
    assert decision.market is not None
    assert decision.market.up_token_id == "3" * 30
    assert decision.market.down_token_id == "2" * 30


def test_ambiguous_outcomes_are_quarantinable(
    collector_config: CollectorConfig, fixture_root: Path
) -> None:
    event = json.loads((fixture_root / "gamma" / "current_btc_15m.json").read_text())
    ambiguous = copy.deepcopy(event)
    ambiguous["markets"][0]["outcomes"] = '["Yes", "No"]'
    decision = Btc15mMarketMatcher(collector_config.discovery).match_event(ambiguous)[0]
    assert not decision.accepted
    assert decision.ambiguous
    assert "ambiguous_outcomes" in decision.rejected_rules
