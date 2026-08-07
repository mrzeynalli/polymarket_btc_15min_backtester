"""Episode index for 15-minute UP/DOWN backtesting.

An *episode* is one BTC 15-minute market: a condition ID, its two outcome tokens,
its scheduled `[start, end)` window, the recorded data covering that window, and the
settled winner.  The index is the unit of selection, gating, and reporting for every
backtest; nothing downstream reads the collector's storage root directly again.

An authoritative recorded `market_resolved` event is used whenever available.  For
older captures without that event, two independent public observations are
combined:

1. **Terminal book** — after the underlying settles, the winning token's book
   collapses to a bid at/near 1.00 and the losing token's to an ask at/near 0.00.
2. **Reference price** — the RTDS Binance/Chainlink series compared between the
   market's start and end boundary.

Agreement raises confidence; disagreement marks the episode `ambiguous` and it is
excluded from headline results rather than silently guessed.  Every derivation is
recorded with its provenance so a later authoritative source can supersede it.

All reads are read-only.  Nothing in this module writes to the collector's storage
root: outputs go to a caller-supplied workspace directory.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

import duckdb

from polymarket_bt.constants import POLYMARKET_PRICE_SCALE
from polymarket_bt.models.markets import taker_order_delay_ms_from_payload

EPISODE_INDEX_VERSION = "episode-index-v2"

# A token that has settled trades within a tick of certainty.  0.95 is far outside
# the range a genuinely contested 15-minute market quotes at its own expiry.
SETTLED_BID_SCALED = 950_000
SETTLED_ASK_SCALED = 50_000
REFERENCE_MAX_AGE_NS = 60_000_000_000
# The refresh unit has a 3 GiB cgroup limit shared by DuckDB and Python's tape
# reconstruction. Keep DuckDB below that ceiling and let it spill into the
# derived-data workspace rather than the immutable release directory.
DUCKDB_MEMORY_LIMIT = "1024MB"

WinnerSource = Literal[
    "market_resolution", "gamma", "terminal_book", "reference_price", "ambiguous"
]


@dataclass(frozen=True, slots=True)
class EpisodeWindow:
    """Replay bounds around a market, in nanoseconds relative to the schedule."""

    pre_start_ns: int = 5 * 60 * 1_000_000_000
    post_end_ns: int = 60 * 1_000_000_000


@dataclass(frozen=True, slots=True)
class Episode:
    condition_id: str
    market_slug: str
    start_utc_ns: int
    end_utc_ns: int
    up_token_id: str
    down_token_id: str
    tick_size_scaled: int
    minimum_order_size_scaled: int
    # Settlement ground truth.
    winner_token_id: str | None
    winner_outcome: str | None
    winner_source: WinnerSource
    winner_confidence: float
    terminal_up_bid_scaled: int | None
    terminal_down_bid_scaled: int | None
    reference_open_scaled: int | None
    reference_close_scaled: int | None
    reference_source: str | None
    # Coverage of the scheduled window by recorded book events.
    first_event_utc_ns: int | None
    last_event_utc_ns: int | None
    up_event_count: int
    down_event_count: int
    coverage_start_lead_ns: int | None
    coverage_end_lag_ns: int | None
    # Gating.
    quality_error_count: int
    eligible: bool
    exclusion_reason: str | None
    # Added in episode-index-v2.  Defaults keep v1 indexes loadable.
    blocking_quality_error_count: int = 0
    connection_gap_count: int = 0
    resolution_received_utc_ns: int | None = None
    fee_rate: str | None = None
    fee_exponent: int | None = None
    fee_taker_only: bool | None = None
    maker_base_fee_bps: int | None = None
    taker_base_fee_bps: int | None = None
    taker_order_delay_ms: int | None = None
    execution_metadata_source: str | None = None
    execution_metadata_received_utc_ns: int | None = None

    @property
    def duration_ns(self) -> int:
        return self.end_utc_ns - self.start_utc_ns

    def minute_offset(self, timestamp_ns: int) -> float:
        return (timestamp_ns - self.start_utc_ns) / 60e9

    def token_outcome(self, token_id: str) -> str:
        if token_id == self.up_token_id:
            return "UP"
        if token_id == self.down_token_id:
            return "DOWN"
        raise KeyError(f"token {token_id} does not belong to {self.condition_id}")


def _dataset(root: Path, name: str, partitioned: bool = True) -> str:
    pattern = "**/*.parquet" if partitioned else "*.parquet"
    return (
        f"read_parquet('{root / 'normalized' / name / pattern}', "
        "hive_partitioning=false, union_by_name=true)"
    )


def _exists(root: Path, name: str) -> bool:
    directory = root / "normalized" / name
    return directory.exists() and any(directory.rglob("*.parquet"))


def configure_duckdb(connection: duckdb.DuckDBPyConnection, temporary_directory: Path) -> None:
    """Bound offline-query memory and place spill files in a writable workspace."""
    temporary_directory.mkdir(parents=True, exist_ok=True)
    connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    connection.execute("SET temp_directory = ?", [str(temporary_directory)])


class EpisodeIndexBuilder:
    """Builds the episode index from the collector's normalized Parquet datasets."""

    def __init__(
        self,
        storage_root: Path,
        *,
        window: EpisodeWindow | None = None,
        temporary_directory: Path | None = None,
    ) -> None:
        self.storage_root = storage_root
        self.window = window or EpisodeWindow()
        self.connection = duckdb.connect()
        if temporary_directory is not None:
            configure_duckdb(self.connection, temporary_directory)

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> EpisodeIndexBuilder:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def build(
        self,
        *,
        start_utc_ns: int | None = None,
        end_utc_ns: int | None = None,
        require_full_window: bool = True,
    ) -> list[Episode]:
        markets = self._markets(start_utc_ns, end_utc_ns)
        if not markets:
            return []
        coverage = self._coverage()
        terminal = self._terminal_books()
        reference = self._reference_prices(markets)
        resolutions = self._market_resolutions()
        quality = self._quality_summary(markets)
        connection_gaps = self._connection_gap_counts(markets)
        execution_metadata = self._execution_metadata(markets)
        episodes: list[Episode] = []
        for row in markets:
            condition_id = str(row["condition_id"])
            up_token = str(row["up_token_id"])
            down_token = str(row["down_token_id"])
            up_cov = coverage.get(up_token)
            down_cov = coverage.get(down_token)
            up_terminal = terminal.get(up_token) or {}
            down_terminal = terminal.get(down_token) or {}
            up_bid = up_terminal.get("bid")
            down_bid = down_terminal.get("bid")
            gamma_winner = row.get("winning_token_id") or None
            resolution = resolutions.get(condition_id) or {}
            winner_token, winner_source, confidence = self._resolve_winner(
                gamma_winner=str(gamma_winner) if gamma_winner else None,
                up_token=up_token,
                down_token=down_token,
                up_terminal_bid=up_bid,
                down_terminal_bid=down_bid,
                reference=reference.get(condition_id),
                resolution_winner=resolution.get("winner"),
                resolution_conflict=bool(resolution.get("conflict", False)),
            )
            first_event = min((c["first"] for c in (up_cov, down_cov) if c), default=None)
            last_event = max((c["last"] for c in (up_cov, down_cov) if c), default=None)
            start_ns = int(row["market_start_utc_ns"])
            end_ns = int(row["market_end_utc_ns"])
            # Eligibility needs the later of the two starts and the earlier of
            # the two ends; min/max across the combined streams can hide one
            # side starting late or ending early.
            coverage_start = max((c["first"] for c in (up_cov, down_cov) if c), default=None)
            coverage_end = min((c["last"] for c in (up_cov, down_cov) if c), default=None)
            lead = start_ns - coverage_start if coverage_start is not None else None
            lag = coverage_end - end_ns if coverage_end is not None else None
            quality_row = quality.get(condition_id) or {}
            errors = int(quality_row.get("errors", 0))
            blocking_errors = int(quality_row.get("blocking", 0))
            gap_count = connection_gaps.get(condition_id, 0)
            reason = self._exclusion_reason(
                winner_source=winner_source,
                up_cov=up_cov,
                down_cov=down_cov,
                lead=lead,
                lag=lag,
                require_full_window=require_full_window,
                blocking_quality_error_count=blocking_errors,
                connection_gap_count=gap_count,
            )
            reference_row = reference.get(condition_id) or {}
            metadata = execution_metadata.get(condition_id) or {}
            resolution_received = self._resolution_available_time(
                winner_source=winner_source,
                market_end_utc_ns=end_ns,
                authoritative_received_utc_ns=resolution.get("received"),
                gamma_received_utc_ns=row.get("resolved_utc_ns") or row.get("discovered_utc_ns"),
                terminal_received_utc_ns=max(
                    (
                        int(item.get("received") or 0)
                        for item in (up_terminal, down_terminal)
                        if item.get("received") is not None
                    ),
                    default=None,
                ),
                reference_received_utc_ns=reference_row.get("close_received"),
            )
            episodes.append(
                Episode(
                    condition_id=condition_id,
                    market_slug=str(row["market_slug"]),
                    start_utc_ns=start_ns,
                    end_utc_ns=end_ns,
                    up_token_id=up_token,
                    down_token_id=down_token,
                    tick_size_scaled=int(row["tick_size_scaled"]),
                    minimum_order_size_scaled=int(row["minimum_order_size_scaled"]),
                    winner_token_id=winner_token,
                    winner_outcome=(
                        None
                        if winner_token is None
                        else ("UP" if winner_token == up_token else "DOWN")
                    ),
                    winner_source=winner_source,
                    winner_confidence=confidence,
                    terminal_up_bid_scaled=up_bid,
                    terminal_down_bid_scaled=down_bid,
                    reference_open_scaled=reference_row.get("open"),
                    reference_close_scaled=reference_row.get("close"),
                    reference_source=reference_row.get("source"),
                    first_event_utc_ns=first_event,
                    last_event_utc_ns=last_event,
                    up_event_count=int(up_cov["events"]) if up_cov else 0,
                    down_event_count=int(down_cov["events"]) if down_cov else 0,
                    coverage_start_lead_ns=lead,
                    coverage_end_lag_ns=lag,
                    quality_error_count=errors,
                    eligible=reason is None,
                    exclusion_reason=reason,
                    blocking_quality_error_count=blocking_errors,
                    connection_gap_count=gap_count,
                    resolution_received_utc_ns=resolution_received,
                    fee_rate=metadata.get("fee_rate"),
                    fee_exponent=metadata.get("fee_exponent"),
                    fee_taker_only=metadata.get("fee_taker_only"),
                    maker_base_fee_bps=metadata.get("maker_base_fee_bps"),
                    taker_base_fee_bps=metadata.get("taker_base_fee_bps"),
                    taker_order_delay_ms=metadata.get("taker_order_delay_ms"),
                    execution_metadata_source=metadata.get("source"),
                    execution_metadata_received_utc_ns=metadata.get("received_utc_ns"),
                )
            )
        episodes.sort(key=lambda episode: episode.start_utc_ns)
        return episodes

    def _exclusion_reason(
        self,
        *,
        winner_source: WinnerSource,
        up_cov: dict[str, Any] | None,
        down_cov: dict[str, Any] | None,
        lead: int | None,
        lag: int | None,
        require_full_window: bool,
        blocking_quality_error_count: int = 0,
        connection_gap_count: int = 0,
    ) -> str | None:
        if up_cov is None or down_cov is None:
            return "missing_book_data_for_one_side"
        if winner_source == "ambiguous":
            return "winner_not_determinable"
        if blocking_quality_error_count:
            return "non_replay_eligible_quality_error"
        if connection_gap_count:
            return "clob_connection_gap_during_market"
        if not require_full_window:
            return None
        if lead is None or lead < 0:
            return "book_data_starts_after_market_open"
        if lag is None or lag < 0:
            return "book_data_ends_before_market_close"
        return None

    def _markets(self, start_utc_ns: int | None, end_utc_ns: int | None) -> list[dict[str, Any]]:
        """Latest Gamma row per condition, restricted to markets that have book data."""
        filters = ["m.up_token_id is not null", "m.down_token_id is not null"]
        if start_utc_ns is not None:
            filters.append(f"m.market_start_utc_ns >= {int(start_utc_ns)}")
        if end_utc_ns is not None:
            filters.append(f"m.market_end_utc_ns <= {int(end_utc_ns)}")
        where = " and ".join(filters)
        source = _dataset(self.storage_root, "markets", partitioned=False)
        columns = {
            str(row[0])
            for row in self.connection.execute(f"describe select * from {source}").fetchall()
        }

        def optional(name: str, fallback: str) -> str:
            return name if name in columns else f"{fallback} as {name}"

        sql = f"""
        with ranked as (
          select *, row_number() over (
                     partition by condition_id
                     order by discovered_utc_ns desc, market_start_utc_ns desc) as rn
          from {source}
        )
        select condition_id, market_slug, market_start_utc_ns, market_end_utc_ns,
               up_token_id, down_token_id, tick_size_scaled, minimum_order_size_scaled,
               {optional("winning_token_id", "null")}, match_score,
               {optional("fee_fields_json", "'{}'")},
               {optional("discovered_utc_ns", "market_start_utc_ns")},
               {optional("resolved_utc_ns", "null")}
        from ranked m
        where rn = 1 and {where}
        order by market_start_utc_ns
        """
        return self._records(sql)

    def _coverage(self) -> dict[str, dict[str, Any]]:
        """Recorded reconstructable-book coverage per token.

        A quiet token can have no incremental update while periodic complete
        snapshots still prove that its executable book was observed. Eligibility
        therefore uses both event types that feed ``BookReconstructor``.
        """
        sources: list[str] = []
        for dataset in ("book_snapshots", "book_updates"):
            if _exists(self.storage_root, dataset):
                sources.append(
                    f"select token_id, received_utc_ns from {_dataset(self.storage_root, dataset)}"
                )
        if not sources:
            return {}
        events = " union all ".join(sources)
        sql = f"""
        select token_id,
               count(*) as events,
               min(received_utc_ns) as first,
               max(received_utc_ns) as last
        from ({events})
        group by token_id
        """
        return {str(row["token_id"]): row for row in self._records(sql)}

    def _terminal_books(self) -> dict[str, dict[str, int | None]]:
        """Best bid of each token at the last observation of its settlement window.

        The winning token's book collapses to a near-1.00 bid at settlement; the
        loser's bid disappears.  Both are read from the reconstructed top-of-book
        stream, never from a post-close REST call, so the value is one the
        collector actually observed.

        The observation extends past the scheduled boundary by the episode's
        post-end window, because the collapse follows the settlement price by a
        second or two.  This is settlement ground truth only: it is resolved
        before a run starts and is never exposed to a strategy, which sees
        resolution no earlier than the replayed event that carries it.
        """
        if not _exists(self.storage_root, "top_of_book"):
            return {}
        sql = f"""
        with mk as (
          select condition_id,
                 max(market_end_utc_ns) + {int(self.window.post_end_ns)} as end_ns
          from {_dataset(self.storage_root, "markets", partitioned=False)}
          group by condition_id
        ),
        ranked as (
          select t.token_id, t.best_bid_scaled, t.received_utc_ns,
                 row_number() over (partition by t.token_id
                                    order by t.received_utc_ns desc) as rn
          from {_dataset(self.storage_root, "top_of_book")} t
          join mk on mk.condition_id = t.condition_id
          where t.received_utc_ns <= mk.end_ns
        )
        select token_id, best_bid_scaled, received_utc_ns from ranked where rn = 1
        """
        return {
            str(row["token_id"]): {
                "bid": (
                    int(row["best_bid_scaled"]) if row["best_bid_scaled"] is not None else None
                ),
                "received": int(row["received_utc_ns"]),
            }
            for row in self._records(sql)
        }

    def _reference_prices(self, markets: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Reference BTC price at each market's open and close boundary.

        The last observation at or before each boundary is used, which is what a
        live process would have seen.  Chainlink is preferred because Polymarket's
        published resolution source for these markets is an oracle price; Binance
        is used as a fallback and disagreement is surfaced by confidence, not hidden.
        """
        if not markets or not _exists(self.storage_root, "btc_prices"):
            return {}
        bounds = ", ".join(
            f"('{row['condition_id']}', {int(row['market_start_utc_ns'])},"
            f" {int(row['market_end_utc_ns'])})"
            for row in markets
        )
        sql = f"""
        with mk(condition_id, start_ns, end_ns) as (values {bounds}),
        px as (
          select source, received_utc_ns, price_scaled
          from {_dataset(self.storage_root, "btc_prices")}
        ),
        pick as (
          select mk.condition_id, px.source,
                 max_by(px.price_scaled, px.received_utc_ns)
                   filter (where px.received_utc_ns <= mk.start_ns) as open_px,
                 max_by(px.price_scaled, px.received_utc_ns)
                   filter (where px.received_utc_ns <= mk.end_ns) as close_px,
                 max(px.received_utc_ns)
                   filter (where px.received_utc_ns <= mk.start_ns) as open_received,
                 max(px.received_utc_ns)
                   filter (where px.received_utc_ns <= mk.end_ns) as close_received
          from mk join px on px.received_utc_ns
               between mk.start_ns - 900000000000 and mk.end_ns
          group by mk.condition_id, px.source
        )
        select condition_id, source, open_px, close_px, open_received, close_received from pick
        where open_px is not null and close_px is not null
        """
        preferred = ("CHAINLINK_BTCUSD", "BINANCE_BTCUSDT")
        bounds_by_condition = {
            str(row["condition_id"]): (
                int(row["market_start_utc_ns"]),
                int(row["market_end_utc_ns"]),
            )
            for row in markets
        }
        by_condition: dict[str, dict[str, dict[str, Any]]] = {}
        for row in self._records(sql):
            by_condition.setdefault(str(row["condition_id"]), {})[str(row["source"])] = row
        result: dict[str, dict[str, Any]] = {}
        for condition_id, sources in by_condition.items():
            for name in preferred:
                if name in sources:
                    row = sources[name]
                    start_ns, end_ns = bounds_by_condition[condition_id]
                    if (
                        start_ns - int(row["open_received"]) > REFERENCE_MAX_AGE_NS
                        or end_ns - int(row["close_received"]) > REFERENCE_MAX_AGE_NS
                    ):
                        continue
                    result[condition_id] = {
                        "source": name,
                        "open": int(row["open_px"]),
                        "close": int(row["close_px"]),
                        "open_received": int(row["open_received"]),
                        "close_received": int(row["close_received"]),
                    }
                    break
        return result

    def _market_resolutions(self) -> dict[str, dict[str, Any]]:
        """Return authoritative winners, rejecting contradictory lifecycle events."""
        if not _exists(self.storage_root, "market_resolutions"):
            return {}
        rows = self._records(
            f"""
            select condition_id, winning_token_id, winning_outcome, received_utc_ns
            from {_dataset(self.storage_root, "market_resolutions")}
            order by received_utc_ns, sequence
            """
        )
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            grouped.setdefault(str(row["condition_id"]), []).append(row)
        result: dict[str, dict[str, Any]] = {}
        for condition_id, values in grouped.items():
            winners = {str(row["winning_token_id"]) for row in values}
            if len(winners) != 1:
                result[condition_id] = {"winner": None, "received": None, "conflict": True}
                continue
            winner = next(iter(winners))
            received = min(
                int(row["received_utc_ns"])
                for row in values
                if str(row["winning_token_id"]) == winner
            )
            result[condition_id] = {
                "winner": winner,
                "received": received,
                "conflict": False,
            }
        return result

    def _quality_summary(self, markets: Sequence[dict[str, Any]]) -> dict[str, dict[str, int]]:
        if not _exists(self.storage_root, "data_quality_events"):
            return {}
        bounds = ", ".join(
            "("
            + ",".join(
                (
                    f"'{row['condition_id']}'",
                    f"'{row['up_token_id']}'",
                    f"'{row['down_token_id']}'",
                    str(int(row["market_start_utc_ns"])),
                    str(int(row["market_end_utc_ns"])),
                )
            )
            + ")"
            for row in markets
        )
        if _exists(self.storage_root, "book_snapshots"):
            quality_end = f"""
            case
              when q.end_utc_ns is not null then q.end_utc_ns
              when q.token_id is not null then coalesce(
                (select min(s.received_utc_ns)
                   from {_dataset(self.storage_root, "book_snapshots")} s
                  where s.token_id = q.token_id and s.received_utc_ns > q.start_utc_ns),
                9223372036854775807
              )
              else q.start_utc_ns
            end
            """
        else:
            quality_end = "coalesce(q.end_utc_ns, q.start_utc_ns)"
        sql = f"""
        with mk(condition_id, up_token, down_token, start_ns, end_ns) as (values {bounds})
        select mk.condition_id,
               count(*) filter (where q.severity in ('error', 'critical')) as errors,
               count(*) filter (
                 where q.severity in ('error', 'critical') and not q.replay_eligible
               ) as blocking
        from mk
        join {_dataset(self.storage_root, "data_quality_events")} q
          on (
               q.condition_id = mk.condition_id
               or q.token_id in (mk.up_token, mk.down_token)
               or (q.condition_id is null and q.token_id is null)
             )
         and q.start_utc_ns <= mk.end_ns
         and ({quality_end}) >= mk.start_ns
        group by mk.condition_id
        """
        return {
            str(row["condition_id"]): {
                "errors": int(row["errors"]),
                "blocking": int(row["blocking"]),
            }
            for row in self._records(sql)
        }

    def _connection_gap_counts(self, markets: Sequence[dict[str, Any]]) -> dict[str, int]:
        """Count known CLOB disconnect intervals overlapping each market window."""
        if not _exists(self.storage_root, "connection_events"):
            return {}
        events = self._records(
            f"""
            select event_type, event_utc_ns, subscribed_token_count
            from {_dataset(self.storage_root, "connection_events")}
            where source = 'clob_market_ws'
            order by event_utc_ns
            """
        )
        gaps: list[tuple[int, int | None]] = []
        for index, event in enumerate(events):
            if event["event_type"] != "disconnect_detected":
                continue
            if int(event.get("subscribed_token_count") or 0) == 0:
                continue
            start = int(event["event_utc_ns"])
            after = events[index + 1 :]
            before_next_disconnect: list[dict[str, Any]] = []
            for candidate in after:
                if candidate["event_type"] == "disconnect_detected":
                    break
                before_next_disconnect.append(candidate)
            recovered = next(
                (
                    int(candidate["event_utc_ns"])
                    for candidate in before_next_disconnect
                    if candidate["event_type"] == "snapshot_recovery_completed"
                ),
                None,
            )
            fallback = next(
                (
                    int(candidate["event_utc_ns"])
                    for candidate in before_next_disconnect
                    if candidate["event_type"] in {"subscription_acknowledged", "connected"}
                ),
                None,
            )
            gaps.append((start, recovered if recovered is not None else fallback))
        counts: dict[str, int] = {}
        for market in markets:
            start = int(market["market_start_utc_ns"])
            end = int(market["market_end_utc_ns"])
            count = sum(
                1
                for gap_start, gap_end in gaps
                if gap_start <= end and (gap_end is None or gap_end >= start)
            )
            if count:
                counts[str(market["condition_id"])] = count
        return counts

    def _execution_metadata(self, markets: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Select the latest venue parameters observed no later than market open."""
        candidates: dict[str, list[dict[str, Any]]] = {}
        if _exists(self.storage_root, "market_execution_metadata"):
            for row in self._records(
                f"""
                select * from {_dataset(self.storage_root, "market_execution_metadata")}
                order by received_utc_ns, sequence
                """
            ):
                normalized = dict(row)
                if str(normalized.get("source") or "").startswith("clob_rest"):
                    # Re-project from immutable raw metadata so archives produced
                    # before `itode` was correctly understood do not retain the
                    # old invented 250 ms value.
                    try:
                        payload = json.loads(str(normalized.get("metadata_json") or "{}"))
                    except (json.JSONDecodeError, TypeError):
                        payload = {}
                    if isinstance(payload, dict):
                        normalized["taker_order_delay_ms"] = taker_order_delay_ms_from_payload(
                            payload
                        )
                candidates.setdefault(str(normalized["condition_id"]), []).append(normalized)
        gamma_history: dict[str, list[dict[str, Any]]] = {}
        market_source = _dataset(self.storage_root, "markets", partitioned=False)
        market_columns = {
            str(row[0])
            for row in self.connection.execute(f"describe select * from {market_source}").fetchall()
        }
        if {"fee_fields_json", "discovered_utc_ns"}.issubset(market_columns):
            for row in self._records(
                f"""
                select condition_id, discovered_utc_ns, fee_fields_json
                from {market_source}
                order by discovered_utc_ns
                """
            ):
                gamma_history.setdefault(str(row["condition_id"]), []).append(row)
        result: dict[str, dict[str, Any]] = {}
        for market in markets:
            condition_id = str(market["condition_id"])
            start = int(market["market_start_utc_ns"])
            observed: list[dict[str, Any]] = [
                row
                for row in candidates.get(condition_id, [])
                if int(row["received_utc_ns"]) <= start
            ]
            if observed:
                # CLOB is the venue authority. Market detail owns seconds_delay;
                # compact market info owns the fee curve. Merge their latest
                # causal observations rather than letting the later HTTP response
                # erase fields carried only by the other endpoint.
                clob_observed: list[dict[str, Any]] = [
                    row for row in observed if str(row.get("source") or "").startswith("clob_rest")
                ]
                gamma_observed: list[dict[str, Any]] = [
                    row for row in observed if row.get("source") == "gamma"
                ]

                def observation_order(row: dict[str, Any]) -> tuple[int, int]:
                    return (
                        int(row["received_utc_ns"]),
                        int(row.get("sequence") or 0),
                    )

                authority = clob_observed if clob_observed else observed
                latest_observation = max(authority, key=observation_order)
                selected = dict(latest_observation)
                merge_fields = (
                    "fee_rate",
                    "fee_exponent",
                    "fee_taker_only",
                    "maker_base_fee_bps",
                    "taker_base_fee_bps",
                    "tick_size_scaled",
                    "minimum_order_size_scaled",
                )
                if clob_observed:
                    newest_first = sorted(clob_observed, key=observation_order, reverse=True)
                    for field in merge_fields:
                        value = next(
                            (row.get(field) for row in newest_first if row.get(field) is not None),
                            None,
                        )
                        if value is not None:
                            selected[field] = value

                    details = [
                        row for row in clob_observed if row.get("source") == "clob_rest_market"
                    ]
                    delay_source = max(details or clob_observed, key=observation_order)
                    # Preserve null from authoritative detail: true-without an
                    # explicit duration is unknown, not a guessed delay.
                    selected["taker_order_delay_ms"] = delay_source.get("taker_order_delay_ms")
                if clob_observed and gamma_observed:
                    gamma_fallback = max(gamma_observed, key=observation_order)
                    for field in merge_fields:
                        if selected.get(field) is None:
                            selected[field] = gamma_fallback.get(field)
                result[condition_id] = selected
                continue
            historical_gamma = [
                row
                for row in gamma_history.get(condition_id, [])
                if int(row["discovered_utc_ns"]) <= start
            ]
            if not historical_gamma:
                continue
            gamma_row = max(historical_gamma, key=lambda row: int(row["discovered_utc_ns"]))
            try:
                fee_fields = json.loads(str(gamma_row.get("fee_fields_json") or "{}"))
            except json.JSONDecodeError:
                fee_fields = {}
            schedule = fee_fields.get("feeSchedule", {}) if isinstance(fee_fields, dict) else {}
            if not isinstance(schedule, dict):
                schedule = {}
            if schedule:
                result[condition_id] = {
                    "fee_rate": str(schedule.get("rate"))
                    if schedule.get("rate") is not None
                    else None,
                    "fee_exponent": (
                        int(schedule["exponent"]) if schedule.get("exponent") is not None else None
                    ),
                    "fee_taker_only": (
                        bool(schedule["takerOnly"]) if "takerOnly" in schedule else None
                    ),
                    "maker_base_fee_bps": fee_fields.get("makerBaseFee"),
                    "taker_base_fee_bps": fee_fields.get("takerBaseFee"),
                    "taker_order_delay_ms": None,
                    "source": "gamma",
                    "received_utc_ns": gamma_row.get("discovered_utc_ns"),
                }
        return result

    def _resolve_winner(
        self,
        *,
        gamma_winner: str | None,
        up_token: str,
        down_token: str,
        up_terminal_bid: int | None,
        down_terminal_bid: int | None,
        reference: dict[str, Any] | None,
        resolution_winner: str | None = None,
        resolution_conflict: bool = False,
    ) -> tuple[str | None, WinnerSource, float]:
        if resolution_conflict:
            return None, "ambiguous", 0.0
        if resolution_winner is not None:
            if resolution_winner in {up_token, down_token}:
                return resolution_winner, "market_resolution", 1.0
            # A lifecycle event naming a token outside the condition is corrupt
            # ground truth and must not silently fall through to an inference.
            return None, "ambiguous", 0.0
        if gamma_winner in {up_token, down_token}:
            return gamma_winner, "gamma", 1.0
        book_winner = self._winner_from_terminal_book(
            up_token, down_token, up_terminal_bid, down_terminal_bid
        )
        reference_winner = self._winner_from_reference(up_token, down_token, reference)
        if book_winner is not None and reference_winner is not None:
            if book_winner == reference_winner:
                return book_winner, "terminal_book", 0.99
            return None, "ambiguous", 0.0
        if book_winner is not None:
            return book_winner, "terminal_book", 0.90
        if reference_winner is not None:
            return reference_winner, "reference_price", 0.70
        return None, "ambiguous", 0.0

    def _resolution_available_time(
        self,
        *,
        winner_source: WinnerSource,
        market_end_utc_ns: int,
        authoritative_received_utc_ns: int | None,
        gamma_received_utc_ns: int | None,
        terminal_received_utc_ns: int | None,
        reference_received_utc_ns: int | None,
    ) -> int | None:
        """Return the earliest causal time settlement evidence was available.

        Recorded lifecycle and Gamma winners use their local receive/discovery
        timestamps.  A terminal-book winner becomes available only when the
        later side was observed, never before the scheduled boundary.  A
        reference-only fallback cannot establish venue settlement, so capital is
        conservatively held through the configured post-end observation window.
        """
        if winner_source == "market_resolution" and authoritative_received_utc_ns is not None:
            return max(market_end_utc_ns, int(authoritative_received_utc_ns))
        if winner_source == "gamma" and gamma_received_utc_ns is not None:
            received = int(gamma_received_utc_ns)
            return (
                received
                if received > market_end_utc_ns
                else market_end_utc_ns + self.window.post_end_ns
            )
        if winner_source == "terminal_book" and terminal_received_utc_ns is not None:
            received = int(terminal_received_utc_ns)
            return (
                received
                if received > market_end_utc_ns
                else market_end_utc_ns + self.window.post_end_ns
            )
        if winner_source == "reference_price":
            _ = reference_received_utc_ns  # retained for provenance, not early capital release
            return market_end_utc_ns + self.window.post_end_ns
        return None

    @staticmethod
    def _winner_from_terminal_book(
        up_token: str,
        down_token: str,
        up_bid: int | None,
        down_bid: int | None,
    ) -> str | None:
        up_settled = up_bid is not None and up_bid >= SETTLED_BID_SCALED
        down_settled = down_bid is not None and down_bid >= SETTLED_BID_SCALED
        up_dead = up_bid is None or up_bid <= SETTLED_ASK_SCALED
        down_dead = down_bid is None or down_bid <= SETTLED_ASK_SCALED
        if up_settled and down_dead:
            return up_token
        if down_settled and up_dead:
            return down_token
        return None

    @staticmethod
    def _winner_from_reference(
        up_token: str,
        down_token: str,
        reference: dict[str, Any] | None,
    ) -> str | None:
        if not reference:
            return None
        open_px = int(reference["open"])
        close_px = int(reference["close"])
        # The BTC 15-minute contract resolves UP when close is greater than or
        # equal to open; the equality rule is explicit in the recorded market.
        return up_token if close_px >= open_px else down_token

    def _records(self, sql: str) -> list[dict[str, Any]]:
        cursor = self.connection.execute(sql)
        columns = [description[0] for description in cursor.description or []]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def write_episode_index(
    episodes: Iterable[Episode],
    workspace: Path,
    *,
    storage_root: Path,
    input_fingerprint: dict[str, Any] | None = None,
) -> Path:
    """Persist the index plus the provenance needed to reproduce a selection."""
    workspace.mkdir(parents=True, exist_ok=True)
    rows = [asdict(episode) for episode in episodes]
    path = workspace / "episodes.json"
    payload = {
        "episode_index_version": EPISODE_INDEX_VERSION,
        "storage_root": str(storage_root),
        "episode_count": len(rows),
        "eligible_count": sum(1 for row in rows if row["eligible"]),
        "input_fingerprint": input_fingerprint or {},
        "episodes": rows,
    }
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)
    return path


def read_episode_index(workspace: Path) -> list[Episode]:
    path = workspace / "episodes.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [Episode(**row) for row in payload["episodes"]]


def input_fingerprint(storage_root: Path) -> dict[str, Any]:
    """Identify the exact input files a run consumed.

    The collector appends new Parquet files continuously.  Recording the file set
    and its digests is what makes a backtest result reproducible against a moving
    data root; results computed on different fingerprints are not comparable.
    """
    manifest = storage_root / "manifests" / "file-manifests.jsonl"
    counts: dict[str, int] = {}
    latest = 0
    entries: list[tuple[str, str, int, int]] = []
    if manifest.exists():
        with manifest.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                dataset = str(entry.get("dataset", "unknown"))
                counts[dataset] = counts.get(dataset, 0) + 1
                closed = entry.get("closed_utc_ns") or 0
                latest = max(latest, int(closed))
                entries.append(
                    (
                        str(entry.get("relative_path") or ""),
                        str(entry.get("sha256") or ""),
                        int(closed),
                        int(entry.get("row_count") or 0),
                    )
                )
    entries.sort()
    manifest_digest = hashlib.sha256(
        json.dumps(entries, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    return {
        "manifest_path": str(manifest),
        "manifest_entry_count": len(entries),
        "manifest_entries_sha256": manifest_digest,
        "files_by_dataset": counts,
        "latest_closed_utc_ns": latest,
        "price_scale": POLYMARKET_PRICE_SCALE,
    }
