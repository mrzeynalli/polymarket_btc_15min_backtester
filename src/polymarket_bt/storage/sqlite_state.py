from __future__ import annotations

import sqlite3
from pathlib import Path

from polymarket_bt.clock import utc_now_ns


class OperationalState:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS normalized_files (
                raw_relative_path TEXT PRIMARY KEY,
                raw_sha256 TEXT NOT NULL,
                normalization_run_id TEXT NOT NULL,
                normalized_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_utc_ns INTEGER NOT NULL
            );
            """
        )
        self.connection.commit()

    def raw_file_processed(self, relative_path: str, sha256: str) -> bool:
        row = self.connection.execute(
            "SELECT raw_sha256 FROM normalized_files WHERE raw_relative_path = ?",
            (relative_path,),
        ).fetchone()
        return bool(row and row[0] == sha256)

    def mark_raw_file_processed(self, relative_path: str, sha256: str, run_id: str) -> None:
        self.connection.execute(
            """
            INSERT INTO normalized_files VALUES (?, ?, ?, ?)
            ON CONFLICT(raw_relative_path) DO UPDATE SET
                raw_sha256=excluded.raw_sha256,
                normalization_run_id=excluded.normalization_run_id,
                normalized_utc_ns=excluded.normalized_utc_ns
            """,
            (relative_path, sha256, run_id, utc_now_ns()),
        )
        self.connection.commit()

    def checkpoint(self, key: str, value: str) -> None:
        self.connection.execute(
            """
            INSERT INTO checkpoints VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_utc_ns=excluded.updated_utc_ns
            """,
            (key, value, utc_now_ns()),
        )
        self.connection.commit()

    def get_checkpoint(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM checkpoints WHERE key = ?", (key,)
        ).fetchone()
        return str(row[0]) if row else None

    def close(self) -> None:
        self.connection.close()
