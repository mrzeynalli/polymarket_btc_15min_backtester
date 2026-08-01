from __future__ import annotations

import sqlite3
from pathlib import Path

from polymarket_bt.discovery.market_registry import MarketRegistry
from polymarket_bt.models.markets import MatchDecision


def test_quarantine_observations_are_deduplicated(tmp_path: Path) -> None:
    path = tmp_path / "registry.sqlite"
    registry = MarketRegistry(path)
    decision = MatchDecision(
        accepted=False,
        ambiguous=True,
        score=0.72,
        matched_rules=("bitcoin_title",),
        rejected_rules=("duration",),
        reason="ambiguous duration",
    )
    for _ in range(3):
        registry.quarantine(
            decision,
            '{"id":"event"}',
            gamma_event_id="event",
            gamma_market_id="market",
        )
    registry.close()

    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT count(*), observation_count FROM quarantined_market_latest"
        ).fetchone()
    assert row == (1, 3)
