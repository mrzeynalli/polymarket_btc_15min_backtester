from __future__ import annotations

import sqlite3
from pathlib import Path

from polymarket_bt.clock import utc_now_ns
from polymarket_bt.models.markets import MarketRecord, MatchDecision


class MarketRegistry:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self._create_schema()

    def _create_schema(self) -> None:
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS markets (
                condition_id TEXT PRIMARY KEY,
                gamma_event_id TEXT NOT NULL,
                gamma_market_id TEXT NOT NULL,
                event_slug TEXT NOT NULL,
                market_slug TEXT NOT NULL,
                market_start_utc_ns INTEGER NOT NULL,
                market_end_utc_ns INTEGER NOT NULL,
                active INTEGER NOT NULL,
                closed INTEGER NOT NULL,
                accepting_orders INTEGER NOT NULL,
                up_token_id TEXT NOT NULL,
                down_token_id TEXT NOT NULL,
                market_json TEXT NOT NULL,
                raw_gamma_payload TEXT NOT NULL,
                first_seen_utc_ns INTEGER NOT NULL,
                last_seen_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quarantined_markets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gamma_event_id TEXT,
                gamma_market_id TEXT,
                score REAL NOT NULL,
                reason TEXT NOT NULL,
                decision_json TEXT NOT NULL,
                raw_gamma_payload TEXT NOT NULL,
                observed_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS collector_runs (
                run_id TEXT PRIMARY KEY,
                previous_run_id TEXT,
                started_utc_ns INTEGER NOT NULL,
                stopped_utc_ns INTEGER,
                clean_shutdown INTEGER
            );
            """
        )
        self.connection.commit()

    def upsert(self, market: MarketRecord, raw_payload: str) -> None:
        now = utc_now_ns()
        self.connection.execute(
            """
            INSERT INTO markets (
                condition_id, gamma_event_id, gamma_market_id, event_slug, market_slug,
                market_start_utc_ns, market_end_utc_ns, active, closed, accepting_orders,
                up_token_id, down_token_id, market_json, raw_gamma_payload,
                first_seen_utc_ns, last_seen_utc_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(condition_id) DO UPDATE SET
                active=excluded.active,
                closed=excluded.closed,
                accepting_orders=excluded.accepting_orders,
                market_json=excluded.market_json,
                raw_gamma_payload=excluded.raw_gamma_payload,
                last_seen_utc_ns=excluded.last_seen_utc_ns
            """,
            (
                market.condition_id,
                market.gamma_event_id,
                market.gamma_market_id,
                market.event_slug,
                market.market_slug,
                market.market_start_utc_ns,
                market.market_end_utc_ns,
                int(market.active),
                int(market.closed),
                int(market.accepting_orders),
                market.up_token_id,
                market.down_token_id,
                market.model_dump_json(),
                raw_payload,
                now,
                now,
            ),
        )
        self.connection.commit()

    def quarantine(
        self,
        decision: MatchDecision,
        raw_payload: str,
        *,
        gamma_event_id: str = "",
        gamma_market_id: str = "",
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO quarantined_markets (
                gamma_event_id, gamma_market_id, score, reason, decision_json,
                raw_gamma_payload, observed_utc_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gamma_event_id,
                gamma_market_id,
                decision.score,
                decision.reason,
                decision.model_dump_json(),
                raw_payload,
                utc_now_ns(),
            ),
        )
        self.connection.commit()

    def active_markets(self, now_ns: int | None = None) -> list[MarketRecord]:
        now = now_ns if now_ns is not None else utc_now_ns()
        rows = self.connection.execute(
            """
            SELECT market_json FROM markets
            WHERE market_end_utc_ns >= ? AND active = 1
            ORDER BY market_start_utc_ns
            """,
            (now - 60_000_000_000,),
        ).fetchall()
        return [MarketRecord.model_validate_json(row["market_json"]) for row in rows]

    def get(self, condition_id: str) -> MarketRecord | None:
        row = self.connection.execute(
            "SELECT market_json FROM markets WHERE condition_id = ?", (condition_id,)
        ).fetchone()
        return MarketRecord.model_validate_json(row["market_json"]) if row else None

    def begin_run(self, run_id: str) -> str | None:
        row = self.connection.execute(
            "SELECT run_id FROM collector_runs ORDER BY started_utc_ns DESC LIMIT 1"
        ).fetchone()
        previous = str(row["run_id"]) if row else None
        self.connection.execute(
            "INSERT INTO collector_runs (run_id, previous_run_id, started_utc_ns) VALUES (?, ?, ?)",
            (run_id, previous, utc_now_ns()),
        )
        self.connection.commit()
        return previous

    def finish_run(self, run_id: str, *, clean: bool) -> None:
        self.connection.execute(
            "UPDATE collector_runs SET stopped_utc_ns = ?, clean_shutdown = ? WHERE run_id = ?",
            (utc_now_ns(), int(clean), run_id),
        )
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()
