from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from polymarket_bt.backtest.episodes import EpisodeIndexBuilder, input_fingerprint

UP = "token-up"
DOWN = "token-down"


def builder() -> EpisodeIndexBuilder:
    return EpisodeIndexBuilder(Path("/nonexistent"))


def test_gamma_winner_is_authoritative_when_present() -> None:
    with builder() as index:
        token, source, confidence = index._resolve_winner(
            gamma_winner=UP,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=10_000,
            down_terminal_bid=990_000,
            reference={"open": 100, "close": 90, "source": "CHAINLINK_BTCUSD"},
        )
    assert (token, source, confidence) == (UP, "gamma", 1.0)


def test_terminal_book_and_reference_agreement_gives_high_confidence() -> None:
    with builder() as index:
        token, source, confidence = index._resolve_winner(
            gamma_winner=None,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=999_000,
            down_terminal_bid=1_000,
            reference={"open": 100, "close": 110, "source": "CHAINLINK_BTCUSD"},
        )
    assert token == UP
    assert source == "terminal_book"
    assert confidence > 0.95


def test_disagreement_is_reported_as_ambiguous_not_guessed() -> None:
    with builder() as index:
        token, source, confidence = index._resolve_winner(
            gamma_winner=None,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=999_000,
            down_terminal_bid=1_000,
            reference={"open": 100, "close": 90, "source": "CHAINLINK_BTCUSD"},
        )
    assert token is None
    assert source == "ambiguous"
    assert confidence == 0.0


def test_unsettled_book_falls_back_to_reference_with_lower_confidence() -> None:
    with builder() as index:
        token, source, confidence = index._resolve_winner(
            gamma_winner=None,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=780_000,
            down_terminal_bid=210_000,
            reference={"open": 100, "close": 90, "source": "CHAINLINK_BTCUSD"},
        )
    assert (token, source) == (DOWN, "reference_price")
    assert confidence < 0.9


def test_exact_reference_tie_resolves_up_per_recorded_contract_rule() -> None:
    assert (
        EpisodeIndexBuilder._winner_from_reference(
            UP, DOWN, {"open": 100, "close": 100, "source": "CHAINLINK_BTCUSD"}
        )
        == UP
    )


def test_market_resolution_supersedes_conflicting_inferences() -> None:
    with builder() as index:
        token, source, confidence = index._resolve_winner(
            gamma_winner=DOWN,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=10_000,
            down_terminal_bid=990_000,
            reference={"open": 100, "close": 90, "source": "CHAINLINK_BTCUSD"},
            resolution_winner=UP,
        )
    assert (token, source, confidence) == (UP, "market_resolution", 1.0)


def test_conflicting_market_resolution_events_are_ambiguous() -> None:
    with builder() as index:
        token, source, confidence = index._resolve_winner(
            gamma_winner=UP,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=990_000,
            down_terminal_bid=10_000,
            reference={"open": 100, "close": 110, "source": "CHAINLINK_BTCUSD"},
            resolution_conflict=True,
        )
    assert (token, source, confidence) == (None, "ambiguous", 0.0)


def test_no_evidence_is_ambiguous() -> None:
    with builder() as index:
        token, source, _ = index._resolve_winner(
            gamma_winner=None,
            up_token=UP,
            down_token=DOWN,
            up_terminal_bid=None,
            down_terminal_bid=None,
            reference=None,
        )
    assert token is None
    assert source == "ambiguous"


def test_partial_window_coverage_is_excluded() -> None:
    with builder() as index:
        reason = index._exclusion_reason(
            winner_source="terminal_book",
            up_cov={"events": 10},
            down_cov={"events": 10},
            lead=-1,
            lag=10,
            require_full_window=True,
        )
    assert reason == "book_data_starts_after_market_open"


def test_missing_one_side_is_excluded() -> None:
    with builder() as index:
        reason = index._exclusion_reason(
            winner_source="terminal_book",
            up_cov={"events": 10},
            down_cov=None,
            lead=10,
            lag=10,
            require_full_window=True,
        )
    assert reason == "missing_book_data_for_one_side"


def test_non_replay_eligible_error_is_excluded_even_without_full_window_requirement() -> None:
    with builder() as index:
        reason = index._exclusion_reason(
            winner_source="market_resolution",
            up_cov={"events": 10},
            down_cov={"events": 10},
            lead=10,
            lag=10,
            require_full_window=False,
            blocking_quality_error_count=1,
        )
    assert reason == "non_replay_eligible_quality_error"


def test_terminal_resolution_availability_uses_observation_time() -> None:
    with builder() as index:
        available = index._resolution_available_time(
            winner_source="terminal_book",
            market_end_utc_ns=1_000,
            authoritative_received_utc_ns=None,
            gamma_received_utc_ns=None,
            terminal_received_utc_ns=1_250,
            reference_received_utc_ns=None,
        )
    assert available == 1_250


def test_pre_close_terminal_quote_does_not_release_capital_at_scheduled_end() -> None:
    with EpisodeIndexBuilder(
        Path("/nonexistent"),
    ) as index:
        available = index._resolution_available_time(
            winner_source="terminal_book",
            market_end_utc_ns=1_000,
            authoritative_received_utc_ns=None,
            gamma_received_utc_ns=None,
            terminal_received_utc_ns=999,
            reference_received_utc_ns=None,
        )
    assert available == 1_000 + index.window.post_end_ns


def _write_dataset(root: Path, name: str, rows: list[dict[str, object]]) -> None:
    directory = root / "normalized" / name / "date=2026-08-01" / "hour=00"
    directory.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), directory / "part.parquet")


def test_quality_and_connection_gaps_are_assigned_to_overlapping_market(tmp_path: Path) -> None:
    start = 1_000
    end = 2_000
    market = {
        "condition_id": "condition",
        "up_token_id": UP,
        "down_token_id": DOWN,
        "market_start_utc_ns": start,
        "market_end_utc_ns": end,
    }
    _write_dataset(
        tmp_path,
        "data_quality_events",
        [
            {
                "condition_id": "condition",
                "token_id": UP,
                "start_utc_ns": 1_500,
                "end_utc_ns": None,
                "severity": "error",
                "replay_eligible": False,
            }
        ],
    )
    _write_dataset(
        tmp_path,
        "connection_events",
        [
            {
                "source": "clob_market_ws",
                "event_type": "disconnect_detected",
                "event_utc_ns": 1_600,
                "subscribed_token_count": 2,
            },
            {
                "source": "clob_market_ws",
                "event_type": "snapshot_recovery_completed",
                "event_utc_ns": 1_700,
                "subscribed_token_count": 2,
            },
        ],
    )
    with EpisodeIndexBuilder(tmp_path) as index:
        assert index._quality_summary([market])["condition"]["blocking"] == 1
        assert index._connection_gap_counts([market])["condition"] == 1


def test_snapshot_only_token_counts_as_reconstructable_book_coverage(tmp_path: Path) -> None:
    _write_dataset(
        tmp_path,
        "book_snapshots",
        [
            {
                "snapshot_id": "snapshot",
                "condition_id": "condition",
                "token_id": UP,
                "received_utc_ns": 1_000,
                "sequence": 1,
            }
        ],
    )

    with EpisodeIndexBuilder(tmp_path) as index:
        coverage = index._coverage()

    assert coverage[UP]["events"] == 1
    assert coverage[UP]["first"] == 1_000
    assert coverage[UP]["last"] == 1_000


def test_input_fingerprint_hashes_sorted_file_identity_and_digest(tmp_path: Path) -> None:
    entries = [
        {
            "dataset": "books",
            "relative_path": "b",
            "sha256": "bbb",
            "closed_utc_ns": 2,
            "row_count": 20,
        },
        {
            "dataset": "markets",
            "relative_path": "a",
            "sha256": "aaa",
            "closed_utc_ns": 1,
            "row_count": 10,
        },
    ]
    manifest = tmp_path / "manifests" / "file-manifests.jsonl"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("\n".join(json.dumps(row) for row in entries) + "\n")
    first = input_fingerprint(tmp_path)
    manifest.write_text("\n".join(json.dumps(row) for row in reversed(entries)) + "\n")
    reordered = input_fingerprint(tmp_path)
    assert first["manifest_entries_sha256"] == reordered["manifest_entries_sha256"]
    assert first["manifest_entry_count"] == 2

    entries[0]["sha256"] = "changed"
    manifest.write_text("\n".join(json.dumps(row) for row in entries) + "\n")
    changed = input_fingerprint(tmp_path)
    assert changed["manifest_entries_sha256"] != first["manifest_entries_sha256"]


def test_offline_queries_use_bounded_memory_and_a_writable_spill_path(tmp_path: Path) -> None:
    spill = tmp_path / "workspace" / ".duckdb-tmp"
    with EpisodeIndexBuilder(tmp_path, temporary_directory=spill) as index:
        configured = index.connection.execute(
            "SELECT current_setting('temp_directory'), current_setting('memory_limit')"
        ).fetchone()

    assert spill.is_dir()
    assert configured is not None
    assert configured[0] == str(spill)
    assert configured[1] == "976.5 MiB"
