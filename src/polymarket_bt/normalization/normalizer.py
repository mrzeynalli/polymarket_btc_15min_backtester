from __future__ import annotations

import hashlib
import json
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import zstandard

from polymarket_bt.clock import decimal_to_scaled, parse_timestamp_ns, utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import (
    POLYMARKET_PRICE_SCALE,
    SCHEMA_VERSION,
    SHARE_SIZE_SCALE,
    Source,
)
from polymarket_bt.dashboard.cache import DashboardCacheWriter
from polymarket_bt.discovery.btc_market_matcher import Btc15mMarketMatcher
from polymarket_bt.discovery.market_registry import MarketRegistry
from polymarket_bt.models.books import BookLevel, BookSnapshot
from polymarket_bt.models.events import RawEnvelope
from polymarket_bt.models.markets import MarketRecord
from polymarket_bt.normalization.deduplication import deduplicate_trades
from polymarket_bt.normalization.parser import (
    EventParser,
    ParsedEvents,
    load_json_decimal,
    parse_tick_size_change,
)
from polymarket_bt.storage.manifest import ManifestEntry, ManifestStore
from polymarket_bt.storage.parquet_writer import ParquetDatasetWriter
from polymarket_bt.storage.sqlite_state import OperationalState


class Normalizer:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.manifest = ManifestStore(config.storage.root)
        self.writer = ParquetDatasetWriter(config, self.manifest)
        self.registry = MarketRegistry(config.storage.root / "state" / "market-registry.sqlite")
        self.state = OperationalState(config.storage.root / "state" / "normalization.sqlite")
        self.matcher = Btc15mMarketMatcher(config.discovery)
        self.token_market: dict[str, MarketRecord] = {}
        for market in self.registry.active_markets(0):
            self.token_market[market.up_token_id] = market
            self.token_market[market.down_token_id] = market
        self.parser = EventParser(self.token_market.get)
        self.dashboard_cache = DashboardCacheWriter(config.storage.root)

    def close(self) -> None:
        self.registry.close()
        self.state.close()

    def _read_envelopes(self, entry: ManifestEntry) -> list[tuple[RawEnvelope, str]]:
        path = self.config.storage.root / entry.relative_path
        envelopes: list[tuple[RawEnvelope, str]] = []
        with path.open("rb") as handle:
            reader = zstandard.ZstdDecompressor().stream_reader(handle)
            text = reader.read().decode("utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line:
                continue
            reference = f"{entry.file_id}:{line_number}"
            envelopes.append((RawEnvelope.model_validate_json(line), reference))
        return envelopes

    def _tick_backfill_rows(self, entries: list[ManifestEntry]) -> list[dict[str, Any]]:
        """Backfill tick events from raw files normalized before this dataset existed."""
        import pyarrow.parquet as pq

        existing_ids: set[str] = set()
        directory = self.config.storage.root / "normalized" / "tick_size_changes"
        if directory.exists():
            for path in directory.rglob("*.parquet"):
                table = pq.ParquetFile(path).read(columns=["tick_change_id"])
                existing_ids.update(str(value) for value in table["tick_change_id"].to_pylist())

        rows: list[dict[str, Any]] = []
        seen = set(existing_ids)
        clob_entries = sorted(
            (
                entry
                for entry in entries
                if entry.format == "jsonl.zst"
                and entry.relative_path.startswith("raw/source=clob_market_ws/")
            ),
            key=lambda entry: (entry.first_sequence or 0, entry.created_utc_ns),
        )
        for entry in clob_entries:
            for envelope, reference in self._read_envelopes(entry):
                try:
                    payload = load_json_decimal(envelope.payload_raw)
                except (json.JSONDecodeError, TypeError, ValueError):
                    continue
                messages = payload if isinstance(payload, list) else [payload]
                for message_index, message in enumerate(messages):
                    if (
                        not isinstance(message, dict)
                        or message.get("event_type") != "tick_size_change"
                    ):
                        continue
                    try:
                        change = parse_tick_size_change(envelope, message, message_index, reference)
                    except (KeyError, TypeError, ValueError):
                        continue
                    if change.tick_change_id not in seen:
                        rows.append(change.model_dump())
                        seen.add(change.tick_change_id)
        return rows

    def _market_rows(
        self, envelope: RawEnvelope, reference: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        payload = load_json_decimal(envelope.payload_raw)
        events = payload if isinstance(payload, list) else [payload]
        markets: list[dict[str, Any]] = []
        outcomes: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, dict):
                continue
            decisions = self.matcher.match_event(event, discovered_ns=envelope.received_utc_ns)
            for decision in decisions:
                if not decision.accepted or not decision.market:
                    continue
                market = decision.market.model_copy(update={"raw_payload_reference": reference})
                self.token_market[market.up_token_id] = market
                self.token_market[market.down_token_id] = market
                row = market.model_dump()
                row["matched_rules_json"] = json.dumps(row.pop("matched_rules"))
                row["rejected_rules_json"] = json.dumps(row.pop("rejected_rules"))
                markets.append(row)
                for index, (token_id, label, normalized) in enumerate(
                    (
                        (market.up_token_id, market.up_outcome_label, "UP"),
                        (market.down_token_id, market.down_outcome_label, "DOWN"),
                    )
                ):
                    outcomes.append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "condition_id": market.condition_id,
                            "token_id": token_id,
                            "outcome": label,
                            "normalized_outcome": normalized,
                            "outcome_index": index,
                            "valid_from_utc_ns": envelope.received_utc_ns,
                            "raw_event_reference": reference,
                        }
                    )
        return markets, outcomes

    def _rest_snapshot(self, envelope: RawEnvelope, reference: str) -> BookSnapshot | None:
        if envelope.event_type_hint != "book" or not envelope.token_id:
            return None
        market = self.token_market.get(envelope.token_id)
        if not market:
            market = self.registry.get(envelope.market_id or "")
        if not market:
            return None
        payload = load_json_decimal(envelope.payload_raw)
        if not isinstance(payload, dict):
            return None

        def levels(name: str) -> tuple[BookLevel, ...]:
            values = payload.get(name, [])
            return tuple(
                BookLevel(
                    price_scaled=decimal_to_scaled(
                        str(item["price"]), POLYMARKET_PRICE_SCALE, field="book_price"
                    ),
                    size_scaled=decimal_to_scaled(
                        str(item["size"]), SHARE_SIZE_SCALE, field="book_size"
                    ),
                )
                for item in values
                if isinstance(item, dict)
            )

        token_id = envelope.token_id
        last_trade = payload.get("last_trade_price")
        return BookSnapshot(
            snapshot_id=str(
                uuid.uuid5(uuid.NAMESPACE_URL, f"{envelope.run_id}:{envelope.sequence}:rest-book")
            ),
            sequence=envelope.sequence,
            run_id=envelope.run_id,
            connection_id=envelope.connection_id,
            source=Source.CLOB_REST.value,
            condition_id=str(payload.get("market") or market.condition_id),
            token_id=token_id,
            outcome=market.token_outcomes[token_id],
            exchange_timestamp_ns=parse_timestamp_ns(payload.get("timestamp")),
            received_utc_ns=envelope.received_utc_ns,
            received_monotonic_ns=envelope.received_monotonic_ns,
            book_hash=str(payload.get("hash")) if payload.get("hash") else None,
            tick_size_scaled=decimal_to_scaled(
                str(payload.get("tick_size")), POLYMARKET_PRICE_SCALE, field="tick_size"
            ),
            minimum_order_size_scaled=decimal_to_scaled(
                str(payload.get("min_order_size")), SHARE_SIZE_SCALE, field="minimum_order_size"
            ),
            last_trade_price_scaled=(
                decimal_to_scaled(str(last_trade), POLYMARKET_PRICE_SCALE, field="last_trade")
                if last_trade not in {None, ""}
                else None
            ),
            neg_risk=bool(payload.get("neg_risk", False)),
            bids=levels("bids"),
            asks=levels("asks"),
            raw_event_reference=reference,
        )

    @staticmethod
    def _snapshot_rows(snapshot: BookSnapshot) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        header = snapshot.model_dump(exclude={"bids", "asks"})
        header["bid_level_count"] = len(snapshot.bids)
        header["ask_level_count"] = len(snapshot.asks)
        header["raw_file_id"] = (
            snapshot.raw_event_reference.split(":", 1)[0] if snapshot.raw_event_reference else None
        )
        levels: list[dict[str, Any]] = []
        for side, values in (
            ("BUY", sorted(snapshot.bids, key=lambda item: item.price_scaled, reverse=True)),
            ("SELL", sorted(snapshot.asks, key=lambda item: item.price_scaled)),
        ):
            for rank, level in enumerate(values, start=1):
                levels.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "snapshot_id": snapshot.snapshot_id,
                        "condition_id": snapshot.condition_id,
                        "token_id": snapshot.token_id,
                        "outcome": snapshot.outcome,
                        "side": side,
                        "price_scaled": level.price_scaled,
                        "size_scaled": level.size_scaled,
                        "level_rank_from_best": rank,
                        "received_utc_ns": snapshot.received_utc_ns,
                    }
                )
        return header, levels

    @staticmethod
    def _operational_row(envelope: RawEnvelope) -> tuple[str, dict[str, Any]] | None:
        if envelope.event_type_hint not in {
            "connection_event",
            "heartbeat_event",
            "rest_request",
            "data_quality_event",
        }:
            return None
        payload = load_json_decimal(envelope.payload_raw)
        if not isinstance(payload, dict):
            return None
        if envelope.event_type_hint == "connection_event":
            return (
                "connection_events",
                {
                    "schema_version": SCHEMA_VERSION,
                    "event_id": str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"{envelope.run_id}:{envelope.sequence}:connection",
                        )
                    ),
                    "source": str(payload.get("source") or envelope.source.value),
                    "connection_id": str(payload.get("connection_id") or envelope.connection_id),
                    "event_type": str(payload.get("connection_event_type") or "unknown"),
                    "event_utc_ns": int(payload.get("event_utc_ns") or envelope.received_utc_ns),
                    "event_monotonic_ns": int(
                        payload.get("event_monotonic_ns") or envelope.received_monotonic_ns
                    ),
                    "attempt_number": int(payload.get("attempt_number") or 0),
                    "close_code": payload.get("close_code"),
                    "reason": str(payload["reason"]) if payload.get("reason") else None,
                    "backoff_ms": payload.get("backoff_ms"),
                    "subscribed_market_count": int(payload.get("subscribed_market_count") or 0),
                    "subscribed_token_count": int(payload.get("subscribed_token_count") or 0),
                },
            )
        if envelope.event_type_hint == "heartbeat_event":
            return (
                "heartbeat_events",
                {
                    "schema_version": SCHEMA_VERSION,
                    "source": str(payload.get("source") or envelope.source.value),
                    "connection_id": str(payload.get("connection_id") or envelope.connection_id),
                    "heartbeat_sent_utc_ns": int(payload["heartbeat_sent_utc_ns"]),
                    "heartbeat_sent_monotonic_ns": int(payload["heartbeat_sent_monotonic_ns"]),
                    "heartbeat_received_utc_ns": payload.get("heartbeat_received_utc_ns"),
                    "heartbeat_received_monotonic_ns": payload.get(
                        "heartbeat_received_monotonic_ns"
                    ),
                    "round_trip_ns": payload.get("round_trip_ns"),
                    "missed_heartbeat_count": int(payload.get("missed_heartbeat_count") or 0),
                    "sequence": envelope.sequence,
                },
            )
        if envelope.event_type_hint == "rest_request":
            return (
                "rest_requests",
                {
                    "schema_version": SCHEMA_VERSION,
                    "request_id": str(payload["request_id"]),
                    "source": envelope.source.value,
                    "method": str(payload["method"]),
                    "url": str(payload["url"]),
                    "condition_id": envelope.market_id,
                    "token_id": envelope.token_id,
                    "request_start_utc_ns": int(payload["request_start_utc_ns"]),
                    "request_start_monotonic_ns": int(payload["request_start_monotonic_ns"]),
                    "response_received_utc_ns": int(payload["response_received_utc_ns"]),
                    "response_received_monotonic_ns": int(
                        payload["response_received_monotonic_ns"]
                    ),
                    "http_status": int(payload["http_status"]),
                    "response_headers_json": json.dumps(payload.get("response_headers", {})),
                    "duration_ns": int(payload["duration_ns"]),
                    "retry_count": int(payload["retry_count"]),
                    "raw_event_reference": f"sequence:{envelope.sequence}",
                },
            )
        row = dict(payload)
        row["schema_version"] = SCHEMA_VERSION
        return "data_quality_events", row

    @staticmethod
    def _append_parsed(rows: dict[str, list[dict[str, Any]]], parsed: ParsedEvents) -> None:
        for snapshot in parsed.snapshots:
            header, levels = Normalizer._snapshot_rows(snapshot)
            rows["book_snapshots"].append(header)
            rows["book_snapshot_levels"].extend(levels)
        rows["book_updates"].extend(item.model_dump() for item in parsed.updates)
        rows["tick_size_changes"].extend(item.model_dump() for item in parsed.tick_size_changes)
        rows["top_of_book"].extend(
            {"schema_version": SCHEMA_VERSION, **item.model_dump()} for item in parsed.top_of_book
        )
        rows["trades"].extend(item.model_dump() for item in parsed.trades)
        rows["btc_prices"].extend(item.model_dump() for item in parsed.btc_prices)
        rows["market_resolutions"].extend(parsed.resolutions)
        rows["data_quality_events"].extend(
            {"schema_version": SCHEMA_VERSION, **item.model_dump()} for item in parsed.quality
        )

    @staticmethod
    def _partition_for(dataset: str, row: dict[str, Any]) -> dict[str, str]:
        if dataset in {"markets", "market_outcomes", "normalization_runs"}:
            return {}
        event_ns = next(
            (
                int(row[name])
                for name in (
                    "received_utc_ns",
                    "event_utc_ns",
                    "response_received_utc_ns",
                    "start_utc_ns",
                    "started_utc_ns",
                )
                if row.get(name) is not None
            ),
            utc_now_ns(),
        )
        moment = datetime.fromtimestamp(event_ns / 1_000_000_000, tz=UTC)
        partition = {"date": f"{moment:%Y-%m-%d}", "hour": f"{moment:%H}"}
        if dataset == "btc_prices":
            partition = {"source": str(row["source"]), **partition}
        return partition

    def normalize(
        self,
        *,
        date: str | None = None,
        max_files: int | None = None,
        newest_first: bool = True,
    ) -> dict[str, Any]:
        started = utc_now_ns()
        run_id = str(uuid.uuid4())
        all_entries = self.manifest.entries()
        tick_backfill_key = "normalization_migration:tick_size_changes:v1"
        tick_backfill_needed = self.state.get_checkpoint(tick_backfill_key) is None
        raw_entries = [
            entry
            for entry in all_entries
            if entry.format == "jsonl.zst"
            and entry.relative_path.startswith("raw/")
            and (date is None or f"date={date}" in entry.relative_path)
            and not self.state.raw_file_processed(entry.relative_path, entry.sha256)
        ]
        # Bounded scheduled runs keep the dashboard fresh by publishing the
        # newest finalized archives first. Unbounded operator runs retain
        # chronological ordering for predictable full-history normalization.
        raw_entries.sort(
            key=lambda entry: (entry.closed_utc_ns, entry.relative_path),
            reverse=max_files is not None and newest_first,
        )
        if max_files is not None:
            if max_files < 1:
                raise ValueError("max_files must be positive")
            raw_entries = raw_entries[:max_files]
        batch_digest = hashlib.sha256(
            "".join(sorted(entry.sha256 for entry in raw_entries)).encode()
        ).hexdigest()
        if (
            raw_entries
            and self.state.get_checkpoint(f"normalization_batch:{batch_digest}")
            and not tick_backfill_needed
        ):
            return {"status": "already_normalized", "raw_files": len(raw_entries)}
        rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        if tick_backfill_needed:
            rows["tick_size_changes"].extend(self._tick_backfill_rows(all_entries))
        invalid = 0
        unknown = 0
        for entry in raw_entries:
            for envelope, reference in self._read_envelopes(entry):
                try:
                    if envelope.source == Source.GAMMA:
                        market_rows, outcome_rows = self._market_rows(envelope, reference)
                        rows["markets"].extend(market_rows)
                        rows["market_outcomes"].extend(outcome_rows)
                    elif envelope.source == Source.CLOB_REST:
                        operational = self._operational_row(envelope)
                        if operational:
                            rows[operational[0]].append(operational[1])
                        snapshot = self._rest_snapshot(envelope, reference)
                        if snapshot:
                            header, levels = self._snapshot_rows(snapshot)
                            rows["book_snapshots"].append(header)
                            rows["book_snapshot_levels"].extend(levels)
                    elif envelope.source in {Source.CLOB_MARKET_WS, Source.RTDS}:
                        operational = self._operational_row(envelope)
                        if operational:
                            rows[operational[0]].append(operational[1])
                        else:
                            parsed = self.parser.parse(envelope, reference)
                            invalid += parsed.invalid_count
                            unknown += parsed.unknown_count
                            self._append_parsed(rows, parsed)
                    elif envelope.source == Source.INTERNAL:
                        operational = self._operational_row(envelope)
                        if operational:
                            rows[operational[0]].append(operational[1])
                except Exception as exc:
                    invalid += 1
                    rows["data_quality_events"].append(
                        {
                            "schema_version": SCHEMA_VERSION,
                            "quality_event_id": str(uuid.uuid4()),
                            "severity": "error",
                            "category": "normalization_error",
                            "condition_id": envelope.market_id,
                            "token_id": envelope.token_id,
                            "start_utc_ns": envelope.received_utc_ns,
                            "end_utc_ns": None,
                            "first_sequence": envelope.sequence,
                            "last_sequence": envelope.sequence,
                            "details_json": json.dumps(
                                {
                                    "error": f"{type(exc).__name__}: {exc}",
                                    "raw_event_reference": reference,
                                },
                                separators=(",", ":"),
                            ),
                            "replay_eligible": False,
                        }
                    )
        if rows["markets"]:
            rows["markets"] = list(
                {str(row["condition_id"]): row for row in rows["markets"]}.values()
            )
        if rows["market_outcomes"]:
            rows["market_outcomes"] = list(
                {
                    (str(row["condition_id"]), str(row["token_id"])): row
                    for row in rows["market_outcomes"]
                }.values()
            )
        if rows["trades"]:
            from polymarket_bt.models.trades import TradeEvent

            rows["trades"] = [
                trade.model_dump()
                for trade in deduplicate_trades(
                    TradeEvent.model_validate(row) for row in rows["trades"]
                )
            ]
        if rows["tick_size_changes"]:
            rows["tick_size_changes"] = list(
                {str(row["tick_change_id"]): row for row in rows["tick_size_changes"]}.values()
            )
        written_rows = 0
        output_files: list[str] = []
        for dataset, dataset_rows in list(rows.items()):
            grouped: dict[tuple[tuple[str, str], ...], list[dict[str, Any]]] = defaultdict(list)
            for row in dataset_rows:
                partition = self._partition_for(dataset, row)
                grouped[tuple(partition.items())].append(row)
            for key, group in grouped.items():
                output_entry = self.writer.write_rows(dataset, group, partition=dict(key))
                if output_entry:
                    output_files.append(output_entry.relative_path)
                    written_rows += output_entry.row_count
        self.dashboard_cache.update(rows["top_of_book"])
        completed = utc_now_ns()
        normalization_row = {
            "schema_version": SCHEMA_VERSION,
            "normalization_run_id": run_id,
            "started_utc_ns": started,
            "completed_utc_ns": completed,
            "normalizer_version": self.config.collector_version,
            "raw_files_processed": len(raw_entries),
            "rows_written": written_rows,
            "invalid_events": invalid,
            "unknown_events": unknown,
            "status": "complete",
        }
        run_entry = self.writer.write_rows("normalization_runs", [normalization_row])
        if run_entry:
            output_files.append(run_entry.relative_path)
        for raw_entry in raw_entries:
            self.state.mark_raw_file_processed(raw_entry.relative_path, raw_entry.sha256, run_id)
        if raw_entries:
            self.state.checkpoint(f"normalization_batch:{batch_digest}", run_id)
        self.manifest.export_parquet()
        if tick_backfill_needed:
            self.state.checkpoint(tick_backfill_key, run_id)
        return {
            "status": "complete",
            "normalization_run_id": run_id,
            "raw_files_processed": len(raw_entries),
            "rows_written": written_rows,
            "invalid_events": invalid,
            "unknown_events": unknown,
            "output_files": output_files,
        }
