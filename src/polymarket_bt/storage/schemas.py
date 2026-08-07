from __future__ import annotations

import pyarrow as pa

from polymarket_bt.constants import (
    BPS_SCALE,
    BTC_PRICE_SCALE,
    POLYMARKET_PRICE_SCALE,
    SCHEMA_VERSION,
    SHARE_SIZE_SCALE,
    USDC_SCALE,
)

SCHEMA_METADATA = {
    b"schema_version": str(SCHEMA_VERSION).encode(),
    b"polymarket_price_scale": str(POLYMARKET_PRICE_SCALE).encode(),
    b"share_size_scale": str(SHARE_SIZE_SCALE).encode(),
    b"usdc_scale": str(USDC_SCALE).encode(),
    b"btc_price_scale": str(BTC_PRICE_SCALE).encode(),
    b"bps_scale": str(BPS_SCALE).encode(),
}


def _schema(fields: list[tuple[str, pa.DataType, bool]]) -> pa.Schema:
    return pa.schema(
        [pa.field(name, data_type, nullable=nullable) for name, data_type, nullable in fields],
        metadata=SCHEMA_METADATA,
    )


MARKETS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("gamma_event_id", pa.string(), False),
        ("gamma_market_id", pa.string(), False),
        ("condition_id", pa.string(), False),
        ("event_slug", pa.string(), False),
        ("market_slug", pa.string(), False),
        ("question", pa.string(), False),
        ("description", pa.string(), False),
        ("market_start_utc_ns", pa.int64(), False),
        ("market_end_utc_ns", pa.int64(), False),
        ("discovered_utc_ns", pa.int64(), False),
        ("closed_utc_ns", pa.int64(), True),
        ("resolved_utc_ns", pa.int64(), True),
        ("active", pa.bool_(), False),
        ("closed", pa.bool_(), False),
        ("accepting_orders", pa.bool_(), False),
        ("orderbook_enabled", pa.bool_(), False),
        ("neg_risk", pa.bool_(), False),
        ("tick_size_scaled", pa.int64(), False),
        ("minimum_order_size_scaled", pa.int64(), False),
        ("fee_fields_json", pa.string(), False),
        ("resolution_source", pa.string(), False),
        ("resolution_rules", pa.string(), False),
        ("up_token_id", pa.string(), False),
        ("down_token_id", pa.string(), False),
        ("up_outcome_label", pa.string(), False),
        ("down_outcome_label", pa.string(), False),
        ("winning_token_id", pa.string(), True),
        ("winning_outcome", pa.string(), True),
        ("matcher_version", pa.string(), False),
        ("match_score", pa.float64(), False),
        ("matched_rules_json", pa.string(), False),
        ("rejected_rules_json", pa.string(), False),
        ("raw_payload_reference", pa.string(), True),
    ]
)

MARKET_OUTCOMES = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("outcome", pa.string(), False),
        ("normalized_outcome", pa.string(), False),
        ("outcome_index", pa.int32(), False),
        ("valid_from_utc_ns", pa.int64(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

MARKET_EXECUTION_METADATA = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("condition_id", pa.string(), False),
        ("received_utc_ns", pa.int64(), False),
        ("sequence", pa.int64(), False),
        ("fee_rate", pa.string(), True),
        ("fee_exponent", pa.int32(), True),
        ("fee_taker_only", pa.bool_(), True),
        ("maker_base_fee_bps", pa.int64(), True),
        ("taker_base_fee_bps", pa.int64(), True),
        ("taker_order_delay_ms", pa.int32(), True),
        ("tick_size_scaled", pa.int64(), True),
        ("minimum_order_size_scaled", pa.int64(), True),
        ("source", pa.string(), False),
        ("metadata_json", pa.string(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

BOOK_SNAPSHOTS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("snapshot_id", pa.string(), False),
        ("sequence", pa.int64(), False),
        ("run_id", pa.string(), False),
        ("connection_id", pa.string(), False),
        ("source", pa.string(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("outcome", pa.string(), False),
        ("exchange_timestamp_ns", pa.int64(), True),
        ("received_utc_ns", pa.int64(), False),
        ("received_monotonic_ns", pa.int64(), False),
        ("book_hash", pa.string(), True),
        ("tick_size_scaled", pa.int64(), False),
        ("minimum_order_size_scaled", pa.int64(), False),
        ("last_trade_price_scaled", pa.int64(), True),
        ("neg_risk", pa.bool_(), False),
        ("bid_level_count", pa.int32(), False),
        ("ask_level_count", pa.int32(), False),
        ("raw_file_id", pa.string(), True),
        ("raw_event_reference", pa.string(), True),
    ]
)

BOOK_SNAPSHOT_LEVELS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("snapshot_id", pa.string(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("outcome", pa.string(), False),
        ("side", pa.string(), False),
        ("price_scaled", pa.int64(), False),
        ("size_scaled", pa.int64(), False),
        ("level_rank_from_best", pa.int32(), False),
        ("received_utc_ns", pa.int64(), False),
    ]
)

BOOK_UPDATES = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("parent_event_id", pa.string(), False),
        ("change_index", pa.int32(), False),
        ("sequence", pa.int64(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("outcome", pa.string(), False),
        ("exchange_timestamp_ns", pa.int64(), True),
        ("received_utc_ns", pa.int64(), False),
        ("received_monotonic_ns", pa.int64(), False),
        ("side", pa.string(), False),
        ("price_scaled", pa.int64(), False),
        ("new_size_scaled", pa.int64(), False),
        ("book_hash", pa.string(), True),
        ("reported_best_bid_scaled", pa.int64(), True),
        ("reported_best_ask_scaled", pa.int64(), True),
        ("connection_id", pa.string(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

TICK_SIZE_CHANGES = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("tick_change_id", pa.string(), False),
        ("sequence", pa.int64(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("exchange_timestamp_ns", pa.int64(), True),
        ("received_utc_ns", pa.int64(), False),
        ("received_monotonic_ns", pa.int64(), False),
        ("old_tick_size_scaled", pa.int64(), True),
        ("new_tick_size_scaled", pa.int64(), False),
        ("connection_id", pa.string(), False),
        ("source", pa.string(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

TOP_OF_BOOK = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("sequence", pa.int64(), False),
        ("received_utc_ns", pa.int64(), False),
        ("best_bid_scaled", pa.int64(), True),
        ("best_ask_scaled", pa.int64(), True),
        ("spread_scaled", pa.int64(), True),
        ("midpoint_scaled", pa.int64(), True),
        ("bid_size_scaled", pa.int64(), True),
        ("ask_size_scaled", pa.int64(), True),
    ]
)

TRADES = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("trade_event_id", pa.string(), False),
        ("sequence", pa.int64(), False),
        ("condition_id", pa.string(), False),
        ("token_id", pa.string(), False),
        ("outcome", pa.string(), False),
        ("exchange_timestamp_ns", pa.int64(), True),
        ("received_utc_ns", pa.int64(), False),
        ("received_monotonic_ns", pa.int64(), False),
        ("price_scaled", pa.int64(), False),
        ("size_scaled", pa.int64(), True),
        ("notional_scaled", pa.int64(), True),
        ("reported_side", pa.string(), True),
        ("fee_rate_bps_scaled", pa.int64(), True),
        ("transaction_hash", pa.string(), True),
        ("trade_id_when_available", pa.string(), True),
        ("source", pa.string(), False),
        ("is_reconciled", pa.bool_(), False),
        ("reconciliation_status", pa.string(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

BTC_PRICES = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("price_event_id", pa.string(), False),
        ("sequence", pa.int64(), False),
        ("source", pa.string(), False),
        ("topic", pa.string(), False),
        ("symbol", pa.string(), False),
        ("rtds_envelope_timestamp_ns", pa.int64(), True),
        ("underlying_source_timestamp_ns", pa.int64(), True),
        ("received_utc_ns", pa.int64(), False),
        ("received_monotonic_ns", pa.int64(), False),
        ("price_scaled", pa.int64(), False),
        ("connection_id", pa.string(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

CONNECTION_EVENTS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("event_id", pa.string(), False),
        ("source", pa.string(), False),
        ("connection_id", pa.string(), False),
        ("event_type", pa.string(), False),
        ("event_utc_ns", pa.int64(), False),
        ("event_monotonic_ns", pa.int64(), False),
        ("attempt_number", pa.int32(), False),
        ("close_code", pa.int32(), True),
        ("reason", pa.string(), True),
        ("backoff_ms", pa.int64(), True),
        ("subscribed_market_count", pa.int32(), False),
        ("subscribed_token_count", pa.int32(), False),
    ]
)

HEARTBEAT_EVENTS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("source", pa.string(), False),
        ("connection_id", pa.string(), False),
        ("heartbeat_sent_utc_ns", pa.int64(), False),
        ("heartbeat_sent_monotonic_ns", pa.int64(), False),
        ("heartbeat_received_utc_ns", pa.int64(), True),
        ("heartbeat_received_monotonic_ns", pa.int64(), True),
        ("round_trip_ns", pa.int64(), True),
        ("missed_heartbeat_count", pa.int32(), False),
        ("sequence", pa.int64(), False),
    ]
)

REST_REQUESTS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("request_id", pa.string(), False),
        ("source", pa.string(), False),
        ("method", pa.string(), False),
        ("url", pa.string(), False),
        ("condition_id", pa.string(), True),
        ("token_id", pa.string(), True),
        ("request_start_utc_ns", pa.int64(), False),
        ("request_start_monotonic_ns", pa.int64(), False),
        ("response_received_utc_ns", pa.int64(), False),
        ("response_received_monotonic_ns", pa.int64(), False),
        ("http_status", pa.int32(), False),
        ("response_headers_json", pa.string(), False),
        ("duration_ns", pa.int64(), False),
        ("retry_count", pa.int32(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

DATA_QUALITY_EVENTS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("quality_event_id", pa.string(), False),
        ("severity", pa.string(), False),
        ("category", pa.string(), False),
        ("condition_id", pa.string(), True),
        ("token_id", pa.string(), True),
        ("start_utc_ns", pa.int64(), False),
        ("end_utc_ns", pa.int64(), True),
        ("first_sequence", pa.int64(), True),
        ("last_sequence", pa.int64(), True),
        ("details_json", pa.string(), False),
        ("replay_eligible", pa.bool_(), False),
    ]
)

NORMALIZATION_RUNS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("normalization_run_id", pa.string(), False),
        ("started_utc_ns", pa.int64(), False),
        ("completed_utc_ns", pa.int64(), False),
        ("normalizer_version", pa.string(), False),
        ("raw_files_processed", pa.int32(), False),
        ("rows_written", pa.int64(), False),
        ("invalid_events", pa.int64(), False),
        ("unknown_events", pa.int64(), False),
        ("status", pa.string(), False),
    ]
)

FILE_MANIFESTS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("file_id", pa.string(), False),
        ("relative_path", pa.string(), False),
        ("dataset", pa.string(), False),
        ("created_utc_ns", pa.int64(), False),
        ("closed_utc_ns", pa.int64(), False),
        ("row_count", pa.int64(), False),
        ("first_sequence", pa.int64(), True),
        ("last_sequence", pa.int64(), True),
        ("sha256", pa.string(), False),
        ("quality_status", pa.string(), False),
    ]
)

MARKET_RESOLUTIONS = _schema(
    [
        ("schema_version", pa.int32(), False),
        ("condition_id", pa.string(), False),
        ("winning_token_id", pa.string(), False),
        ("winning_outcome", pa.string(), False),
        ("exchange_timestamp_ns", pa.int64(), True),
        ("received_utc_ns", pa.int64(), False),
        ("sequence", pa.int64(), False),
        ("source", pa.string(), False),
        ("raw_event_reference", pa.string(), True),
    ]
)

SCHEMAS: dict[str, pa.Schema] = {
    "markets": MARKETS,
    "market_outcomes": MARKET_OUTCOMES,
    "market_execution_metadata": MARKET_EXECUTION_METADATA,
    "book_snapshots": BOOK_SNAPSHOTS,
    "book_snapshot_levels": BOOK_SNAPSHOT_LEVELS,
    "book_updates": BOOK_UPDATES,
    "tick_size_changes": TICK_SIZE_CHANGES,
    "top_of_book": TOP_OF_BOOK,
    "trades": TRADES,
    "btc_prices": BTC_PRICES,
    "connection_events": CONNECTION_EVENTS,
    "heartbeat_events": HEARTBEAT_EVENTS,
    "rest_requests": REST_REQUESTS,
    "data_quality_events": DATA_QUALITY_EVENTS,
    "normalization_runs": NORMALIZATION_RUNS,
    "file_manifests": FILE_MANIFESTS,
    "market_resolutions": MARKET_RESOLUTIONS,
}
