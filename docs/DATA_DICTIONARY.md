# Data dictionary

Schema version is `1`. Parquet files embed scale metadata. Unless stated otherwise, identifiers and
labels are UTF-8 strings, timestamps are signed 64-bit nanoseconds, and scaled financial values are
signed 64-bit integers. Nullability below describes the normalized Arrow schema.

## Exact numeric scales

| Constant | Scale | Unit represented by integer `1` | Used for |
|---|---:|---:|---|
| `POLYMARKET_PRICE_SCALE` | 1,000,000 | 0.000001 probability/USDC per share | prices, spread, midpoint |
| `SHARE_SIZE_SCALE` | 1,000,000 | 0.000001 share | sizes and inventory |
| `USDC_SCALE` | 1,000,000 | 0.000001 USDC | notional, cash, P&L, fee |
| `BTC_PRICE_SCALE` | 1,000,000,000,000 | 0.000000000001 quote unit/BTC | Binance/Chainlink reference price; selected from observed Chainlink precision |
| `BPS_SCALE` | 1,000,000 | 0.000001 basis point | reported fee-rate fields |

Raw decimal strings are never converted through binary floating point. `Decimal` is multiplied by
the declared scale and must be integral. Excess precision raises `PrecisionError`; the event is
preserved and quarantined as a normalization quality failure rather than rounded silently.

Example: `0.531` becomes `531000`; `12.345678` shares becomes `12345678`; BTC price `67234.50`
becomes `6723450000000`.

`match_score` is a non-financial classifier score and is the sole normalized floating-point field.

## Timestamp vocabulary

| Name | Meaning |
|---|---|
| `exchange_timestamp_ns` | Timestamp carried by the CLOB event/snapshot; nullable when absent. |
| `underlying_source_timestamp_ns` | Binance or Chainlink's timestamp inside an RTDS payload. |
| `rtds_envelope_timestamp_ns` | Timestamp on the outer RTDS event. It is not substituted for source time. |
| `received_utc_ns` | Local `CLOCK_REALTIME` UTC capture immediately after frame receipt. Default replay clock. |
| `received_monotonic_ns` | Local monotonic capture paired with receipt; meaningful for durations on one boot only. |
| `event_utc_ns` | Local UTC time of an operational state transition. |
| `event_monotonic_ns` | Monotonic companion for that transition. |
| `request_start_*` / `response_received_*` | Local bounds around an HTTP request. |

No source timestamp is silently replaced with local receipt time. Exchange-time replay rejects or
deterministically falls back only according to `EventClock`; local-receive replay is the realistic
default.

## Raw event envelope (`data/raw/**/*.jsonl.zst`)

One JSON object per compressed line. Every field is non-null unless marked nullable.

| Field | Type/null | Source and meaning |
|---|---|---|
| `schema_version` | int | Envelope schema. |
| `collector_version` | string | Release/Git version configured for the process. |
| `run_id` | UUID string | Collector process run. |
| `connection_id` | string | Socket/request connection identity. |
| `sequence` | int64 | Process-wide increasing receive/operation sequence. |
| `source` | enum string | `gamma`, `clob_rest`, `clob_market_ws`, `rtds`, `data_api`, or `internal`. |
| `stream` | string | Logical stream/operation name. |
| `event_type_hint` | string/null | Cheap routing hint; not an authoritative parse. |
| `received_utc_ns` | int64 | Earliest local UTC capture. |
| `received_monotonic_ns` | int64 | Earliest local monotonic capture. |
| `source_timestamp_raw` | string/null | Exact source timestamp text if cheaply available. |
| `market_id` | string/null | Known condition ID. |
| `token_id` | string/null | Known CLOB asset ID. |
| `payload_raw` | string | Exact received text, or reversible base64 for bytes. |
| `content_type` | enum | JSON, text, or octet stream. |
| `payload_encoding` | enum | `utf-8` or `base64`. |
| `parse_status` | enum | Raw-layer status; source of truth remains independent of parser success. |

## `markets`

One latest normalized row per condition in a normalization batch. Source: Gamma plus matcher.

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `gamma_event_id` | string | Gamma event ID. |
| `gamma_market_id` | string | Gamma market ID. |
| `condition_id` | string | CLOB condition/market identity. |
| `event_slug` | string | Gamma event slug. |
| `market_slug` | string | Gamma market slug. |
| `question` | string | Market question/title. |
| `description` | string | Gamma description. |
| `market_start_utc_ns` | int64 | Derived/explicit opening boundary. |
| `market_end_utc_ns` | int64 | Gamma end time. |
| `discovered_utc_ns` | int64 | Gamma response receipt time. |
| `closed_utc_ns` | int64/null | Close time if available. |
| `resolved_utc_ns` | int64/null | Resolution availability if available. |
| `active` | bool | Gamma active flag. |
| `closed` | bool | Gamma closed flag. |
| `accepting_orders` | bool | Public market accepting-orders flag; informational only. |
| `orderbook_enabled` | bool | CLOB enablement. |
| `neg_risk` | bool | Negative-risk market flag. |
| `tick_size_scaled` | int64 | Price tick at 1e6 scale. |
| `minimum_order_size_scaled` | int64 | Minimum shares at 1e6 scale. |
| `fee_fields_json` | string | Unmodified selected Gamma fee fields serialized as JSON. |
| `resolution_source` | string | Gamma resolution source. |
| `resolution_rules` | string | Question/description-derived rules text. |
| `up_token_id`, `down_token_id` | string | Explicit outcome token IDs. |
| `up_outcome_label`, `down_outcome_label` | string | Original Gamma labels. |
| `winning_token_id` | string/null | Winner once known. |
| `winning_outcome` | string/null | Winning label once known. |
| `matcher_version` | string | Version of multi-signal rules. |
| `match_score` | float64 | Non-financial confidence in `[0,1]`. |
| `matched_rules_json` | string | JSON list of positive rules. |
| `rejected_rules_json` | string | JSON list of absent/failed rules. |
| `raw_payload_reference` | string/null | Raw file UUID and line or process sequence. |

## `market_outcomes`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `condition_id` | string | Market identity. |
| `token_id` | string | CLOB asset ID. |
| `outcome` | string | Original label (`Up`/`Down` spelling preserved). |
| `normalized_outcome` | string | `UP` or `DOWN`. |
| `outcome_index` | int32 | Normalizer emission order; never used to infer the label. |
| `valid_from_utc_ns` | int64 | Mapping observation receipt. |
| `raw_event_reference` | string/null | Gamma source envelope. |

## `market_execution_metadata`

Timestamped observations of parameters that change how an order executes. Gamma observations retain
the discovery fee schedule. CLOB `GET /clob-markets/{condition_id}` observations are authoritative
for the compact `fd` fee curve, while `GET /markets/{condition_id}` carries the authoritative
`seconds_delay` duration. The compact `itode` field is only an enable flag: false establishes zero,
but true without a duration remains null rather than being converted to a guessed delay.

| Field | Type | Meaning |
|---|---:|---|
| `condition_id` | string | Market condition. |
| `received_utc_ns`, `sequence` | int64 | Causal local availability and raw ordering. |
| `fee_rate` | string/null | Exact decimal fee-curve rate, without binary-float coercion. |
| `fee_exponent` | int32/null | Fee-curve exponent. |
| `fee_taker_only` | bool/null | Whether the curve applies only to takers. |
| `maker_base_fee_bps`, `taker_base_fee_bps` | int64/null | Venue base-fee fields. |
| `taker_order_delay_ms` | int32/null | Exact `seconds_delay × 1000`; `0` when explicitly zero/disabled, null when unknown. |
| `tick_size_scaled`, `minimum_order_size_scaled` | int64/null | Venue order constraints at the observation. |
| `source` | string | `gamma`, `clob_rest_market_info`, or authoritative-duration `clob_rest_market`. |
| `metadata_json` | string | Exact selected source object serialized as JSON. |
| `raw_event_reference` | string/null | Raw archive provenance. |

## `book_snapshots`

Header for a complete REST or WebSocket book replacement.

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `snapshot_id` | string | Deterministic WS or unique REST snapshot ID. |
| `sequence` | int64 | Parent raw process sequence. |
| `run_id` | string | Collector run UUID. |
| `connection_id` | string | Socket or `clob-rest`. |
| `source` | string | `clob_market_ws` or `clob_rest`. |
| `condition_id`, `token_id`, `outcome` | string | Explicit market/token/mapped direction. |
| `exchange_timestamp_ns` | int64/null | CLOB timestamp. |
| `received_utc_ns`, `received_monotonic_ns` | int64 | Local receipt pair. |
| `book_hash` | string/null | Opaque source hash. |
| `tick_size_scaled` | int64 | Price tick, 1e6. |
| `minimum_order_size_scaled` | int64 | Shares, 1e6. |
| `last_trade_price_scaled` | int64/null | Source last trade, price 1e6. |
| `neg_risk` | bool | Source/market flag. |
| `bid_level_count`, `ask_level_count` | int32 | Full recorded level counts. |
| `raw_file_id` | string/null | Manifest file UUID when normalized offline. |
| `raw_event_reference` | string/null | Raw file/line reference. |

## `book_snapshot_levels`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `snapshot_id` | string | Foreign key to snapshot header. |
| `condition_id`, `token_id`, `outcome` | string | Market mapping. |
| `side` | string | `BUY` bid or `SELL` ask. |
| `price_scaled` | int64 | Price, 1e6. |
| `size_scaled` | int64 | Resting shares, 1e6. |
| `level_rank_from_best` | int32 | Bids highest-first; asks lowest-first. |
| `received_utc_ns` | int64 | Parent snapshot receipt, used for partitioning. |

Every available level is retained.

## `book_updates`

One row per `price_changes[]` element.

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `parent_event_id` | string | Stable parent frame/event UUID. |
| `change_index` | int32 | Original array order. |
| `sequence` | int64 | Parent raw sequence. |
| `condition_id`, `token_id`, `outcome` | string | Market mapping. |
| `exchange_timestamp_ns` | int64/null | CLOB event time. |
| `received_utc_ns`, `received_monotonic_ns` | int64 | Local receipt pair. |
| `side` | string | `BUY` or `SELL`. |
| `price_scaled` | int64 | Changed price, 1e6. |
| `new_size_scaled` | int64 | Absolute current level shares, 1e6; zero means delete. |
| `book_hash` | string/null | Opaque per-change source hash when present. |
| `reported_best_bid_scaled`, `reported_best_ask_scaled` | int64/null | Source top, price 1e6. |
| `connection_id` | string | Source connection UUID. |
| `raw_event_reference` | string/null | Raw file/line. |

## `tick_size_changes`

One row per explicit CLOB tick transition. These rows are replayed before later price changes and
field-omitting WebSocket snapshots.

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `tick_change_id` | string | Deterministic UUID from run, sequence, and message index. |
| `sequence` | int64 | Raw frame process sequence. |
| `condition_id`, `token_id` | string | Explicit market/token mapping. |
| `exchange_timestamp_ns` | int64/null | CLOB event timestamp. |
| `received_utc_ns`, `received_monotonic_ns` | int64 | Local receipt pair. |
| `old_tick_size_scaled` | int64/null | Reported prior tick at the 1e6 price scale. |
| `new_tick_size_scaled` | int64 | New effective tick at the 1e6 price scale. |
| `connection_id` | string | Source WebSocket connection UUID. |
| `source` | string | `clob_market_ws`. |
| `raw_event_reference` | string/null | Raw file/line. |

## `top_of_book`

Optional reproducible derived table. The current normalizer declares this schema but does not emit it
by default; replay derives it from snapshots/updates.

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `condition_id`, `token_id` | string | Market/token. |
| `sequence`, `received_utc_ns` | int64 | State cutoff. |
| `best_bid_scaled`, `best_ask_scaled` | int64/null | Executable top prices, 1e6. |
| `spread_scaled`, `midpoint_scaled` | int64/null | Derived price values, 1e6. |
| `bid_size_scaled`, `ask_size_scaled` | int64/null | L1 shares, 1e6. |

## `trades`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `trade_event_id` | string | Reported trade ID or deterministic event UUID. |
| `sequence` | int64 | Raw frame sequence. |
| `condition_id`, `token_id`, `outcome` | string | Market mapping. |
| `exchange_timestamp_ns` | int64/null | CLOB trade time. |
| `received_utc_ns`, `received_monotonic_ns` | int64 | Local receipt pair. |
| `price_scaled` | int64 | Executed price, 1e6. |
| `size_scaled` | int64/null | Executed shares, 1e6; null remains null. |
| `notional_scaled` | int64/null | `price × size`, USDC 1e6. |
| `reported_side` | string/null | Source side without reinterpretation. |
| `fee_rate_bps_scaled` | int64/null | Reported bps at 1e6 bps scale. |
| `transaction_hash` | string/null | Public transaction identifier. |
| `trade_id_when_available` | string/null | Source trade ID. |
| `source` | string | Public source. |
| `is_reconciled` | bool | Whether post-close comparison has been applied. |
| `reconciliation_status` | string | Initial or match classification. |
| `raw_event_reference` | string/null | Raw file/line. |

Displayed size changes are never counted as executed volume.

## `btc_prices`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `price_event_id` | string | Deterministic event UUID. |
| `sequence` | int64 | Raw frame sequence. |
| `source` | string | Exact normalized source: `BINANCE_BTCUSDT` or `CHAINLINK_BTCUSD`. |
| `topic` | string | Raw RTDS topic. |
| `symbol` | string | Original source spelling. |
| `rtds_envelope_timestamp_ns` | int64/null | Outer RTDS timestamp. |
| `underlying_source_timestamp_ns` | int64/null | Exchange/oracle timestamp. |
| `received_utc_ns`, `received_monotonic_ns` | int64 | Local receipt pair. |
| `price_scaled` | int64 | Quote price per BTC, 1e12. |
| `connection_id` | string | RTDS connection UUID. |
| `raw_event_reference` | string/null | Raw file/line. |

## `connection_events`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `event_id` | string | Stable normalized UUID. |
| `source`, `connection_id`, `event_type` | string | Feed, connection, transition. |
| `event_utc_ns`, `event_monotonic_ns` | int64 | Local event clock pair. |
| `attempt_number` | int32 | Connection attempt. |
| `close_code` | int32/null | WebSocket close code. |
| `reason` | string/null | Sanitized diagnostic reason. |
| `backoff_ms` | int64/null | Scheduled reconnect delay. |
| `subscribed_market_count`, `subscribed_token_count` | int32 | Subscription cardinality. |

## `heartbeat_events`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `source`, `connection_id` | string | Feed and socket. |
| `heartbeat_sent_utc_ns`, `heartbeat_sent_monotonic_ns` | int64 | PING clocks. |
| `heartbeat_received_utc_ns`, `heartbeat_received_monotonic_ns` | int64/null | PONG clocks. |
| `round_trip_ns` | int64/null | Monotonic receive minus send. |
| `missed_heartbeat_count` | int32 | Consecutive misses. |
| `sequence` | int64 | Operational raw sequence. |

## `rest_requests`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `request_id` | string | Request UUID. |
| `source`, `method`, `url` | string | Public client and request. |
| `condition_id`, `token_id` | string/null | Request scope. |
| `request_start_utc_ns`, `request_start_monotonic_ns` | int64 | Start clocks. |
| `response_received_utc_ns`, `response_received_monotonic_ns` | int64 | Completion clocks. |
| `http_status` | int32 | HTTP response code. |
| `response_headers_json` | string | Allow-listed diagnostic headers only. |
| `duration_ns` | int64 | Monotonic duration. |
| `retry_count` | int32 | Zero-based retry count. |
| `raw_event_reference` | string/null | Response archive reference. |

## `data_quality_events`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `quality_event_id` | string | UUID. |
| `severity`, `category` | string | Machine-readable classification. |
| `condition_id`, `token_id` | string/null | Scope. |
| `start_utc_ns` | int64 | Interval start. |
| `end_utc_ns` | int64/null | End; null means open-ended/point event by category. |
| `first_sequence`, `last_sequence` | int64/null | Affected raw range. |
| `details_json` | string | Structured diagnostics. |
| `replay_eligible` | bool | Whether replay may continue when `reject_on_gap=false`. |

## `normalization_runs`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `normalization_run_id` | string | UUID. |
| `started_utc_ns`, `completed_utc_ns` | int64 | Run bounds. |
| `normalizer_version` | string | Code version. |
| `raw_files_processed` | int32 | Successfully checkpointed inputs. |
| `rows_written` | int64 | Rows excluding this run row. |
| `invalid_events`, `unknown_events` | int64 | Parse quality counts. |
| `status` | string | Terminal status. |

## `file_manifests`

The authoritative append-only representation is `data/manifests/file-manifests.jsonl`; a Parquet
export at `data/manifests/file-manifests.parquet` is DuckDB-readable. Fields are:

| Field | Type/null | Meaning |
|---|---|---|
| `file_id`, `relative_path`, `dataset` | string | UUID, storage path, logical dataset/partition. |
| `schema_version` | int | File schema. |
| `created_utc_ns`, `closed_utc_ns` | int64 | Write lifecycle. |
| `row_count` | int64 | Envelopes or Parquet rows. |
| `first_sequence`, `last_sequence` | int64/null | Observed range. |
| `minimum_event_time`, `maximum_event_time` | int64/null | Local event-time bounds. |
| `uncompressed_bytes_estimate`, `compressed_bytes` | int64 | Size accounting. |
| `sha256` | string | Finalized file digest. |
| `collector_version`, `normalizer_version` | string/null | Producing versions. |
| `market_ids`, `token_ids` | list<string> | Contents summary. |
| `quality_status` | string | Complete, degraded recovery, or compacted status. |
| `format` | string | `jsonl.zst` or `parquet`. |

The compact Arrow `FILE_MANIFESTS` compatibility schema contains the identity, version, lifecycle,
row/range, checksum, and quality subset. The exported operational manifest retains all fields above.

## `market_resolutions`

| Field | Type/null | Meaning |
|---|---|---|
| `schema_version` | int32 | Schema version. |
| `condition_id`, `winning_token_id`, `winning_outcome` | string | Official resolution mapping. |
| `exchange_timestamp_ns` | int64/null | Source event time. |
| `received_utc_ns` | int64 | Availability to realistic replay. |
| `sequence` | int64 | Raw event sequence. |
| `source` | string | Public lifecycle source. |
| `raw_event_reference` | string/null | Source envelope. |

## Backtest report datasets

`orders.parquet` stores every intent plus acceptance/rejection; `fills.parquet` stores one row per
consumed level; `portfolio_events.parquet` stores cash/inventory/P&L mutations;
`market_results.parquet` stores resolution payouts; and `daily_or_session_metrics.parquet` stores the
run-level capital, P&L, fee, fill, volume, drawdown, and ratio fields. Their exact configuration,
input manifest, environment, Git commit, seed, and model versions accompany them in the same report
directory.
