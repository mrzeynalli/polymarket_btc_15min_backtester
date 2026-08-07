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


def test_legacy_quarantine_is_only_removed_by_explicit_maintenance(tmp_path: Path) -> None:
    path = tmp_path / "registry.sqlite"
    registry = MarketRegistry(path)
    registry.connection.executescript(
        """
        CREATE TABLE quarantined_markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gamma_event_id TEXT,
            gamma_market_id TEXT,
            score REAL NOT NULL,
            reason TEXT NOT NULL,
            decision_json TEXT NOT NULL,
            raw_gamma_payload TEXT NOT NULL,
            observed_utc_ns INTEGER NOT NULL
        );
        INSERT INTO quarantined_markets
          (gamma_event_id, gamma_market_id, score, reason, decision_json,
           raw_gamma_payload, observed_utc_ns)
        VALUES
          ('e1', 'm1', 0.1, 'bad', '{}', '{"id":1}', 1),
          ('e1', 'm1', 0.1, 'bad', '{}', '{"id":1}', 2);
        """
    )
    registry.connection.commit()

    assert registry.legacy_quarantine_rows() == 2
    backup = tmp_path / "registry.backup.sqlite"
    registry.backup_to(backup)
    outcome = registry.remove_legacy_quarantine(vacuum=True)
    assert outcome == {"legacy_rows_removed": 2}
    assert registry.legacy_quarantine_rows() == 0
    registry.close()

    with sqlite3.connect(backup) as connection:
        assert connection.execute("SELECT count(*) FROM quarantined_markets").fetchone() == (2,)
