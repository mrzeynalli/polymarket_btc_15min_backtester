# Data quality

Data quality is explicit state, not an absence of exceptions. The raw archive is retained even when
a message is invalid or unknown. A parser or reconstruction failure never authorizes deletion.

## Quality states

| State | Meaning | Default replay behavior |
|---|---|---|
| `complete` | No known fidelity issue in the interval. | Allowed. |
| `degraded` | Known issue that may be usable for a stated analysis. | Rejected when `reject_on_gap=true`; disclosed otherwise. |
| `unreliable` | Book/event state cannot be reconstructed confidently. | Rejected by default. |
| `excluded` | Critical loss/corruption; should not be used. | Always exclude from production conclusions. |

`replay_eligible` means only that an operator may opt into a degraded interval. It does not mean the
data is complete. Every interval used is embedded in `quality_report.json`.

## Message-level validation

The normalizer checks JSON shape, known event type, identifiers, token mapping, timestamps, decimal
precision, side, and source-specific fields. Failures emit `malformed_json`, `malformed_message`,
`unknown_event_type`, `clob_parse_error`, `normalization_error`, or precision categories. Unknown
messages remain byte-for-byte in raw storage and can be supported by a later parser.

No missing trade size is replaced by zero. No trade is inferred from resting-size reduction.

## Book-level validation

Full snapshots replace all state. Every snapshot and update is checked for:

- price strictly inside the binary market bounds;
- nonnegative size and zero-as-delete behavior;
- unique normalized price level;
- tick-size compliance;
- deterministic bid descending / ask ascending traversal;
- best bid strictly below best ask;
- token-to-condition/outcome consistency;
- reported best bid/ask agreement when those fields exist.

An invalid book is marked uncertain and a public REST recovery snapshot is scheduled. The uncertainty
start and affected sequence are recorded. A later snapshot replaces state; no event is invented for
the gap. Source book hashes are preserved, but a local equivalent is not claimed without an official
algorithm.

## Market-level validation

A high-fidelity market requires an unambiguous UP and DOWN token mapping, plausible 15-minute window,
both token streams, initial snapshots, overlapping BTC reference data, and eventual official
resolution. The registry quarantines uncertain mapping decisions. `validate-data --market` and
`validate-books --market` report available counts and reconstruction errors; post-close resolution
and Data API reconciliation may occur later than initial capture.

The episode index rejects a market when an error/critical quality event marked
`replay_eligible=false` overlaps its scheduled window, or when a recorded CLOB disconnect interval
overlaps it. Coverage is checked independently for both tokens: one side starting late or ending
early cannot be hidden by the other side's wider range.

## File-level validation

`polymarket-bt verify-files` is read-only. It recalculates SHA-256, checks existence, validates
Parquet metadata and row count, ensures compressed raw files are nonempty, and reports overlapping
manifest ranges. It writes a timestamped JSON report under `data/manifests/`.

`polymarket-bt validate-data` combines file verification with normalized event counts and writes:

```text
data/reports/data-quality/<UTC-date>/quality-report.md
```

A manifest failure, unreadable Parquet file, unexplained overlap, zero-row finalized analytical
file, or abandoned partial should be investigated before replay. Verification never repairs or
deletes files.

## Queue loss and gaps

The raw queue is bounded. When it is full, the writer increments `dropped_total` and records the
first and last dropped process sequences. Health becomes unhealthy. This is an irrecoverable local
capture gap even if a later REST snapshot makes current book state valid.

The live parser queue is independently bounded. A parser-queue miss can make the live in-memory book
lag while the raw envelope remains safe; offline normalization is still authoritative. Connection
loss and heartbeat timeout create uncertain intervals even with zero local queue drops because
events may have occurred upstream while disconnected.

## Common categories

`websocket_disconnect`, `heartbeat_timeout`, `raw_queue_near_capacity`, `event_drop`,
`malformed_json`, `unknown_event_type`, `missing_exchange_timestamp`, `book_crossed`,
`negative_size`, `invalid_price`, `snapshot_mismatch`, `book_validation_failed`,
`token_mapping_ambiguous`, `trade_reconciliation_difference`, `partial_file_recovered`,
`normalization_error`, `notional_precision_exceeded`, and `clock_offset_warning`.

## Clock quality

System clock synchronization is checked through the systemd synchronization marker and included in
health. The audited host was synchronized. The collector never alters source timestamps or silently
offsets them. If UTC is unsynchronized, health is unhealthy and local-receive-time conclusions must
be rejected until the interval is explicitly classified.

## Validation schedule

- Every frame: envelope and queue accounting.
- Every parsed event: cheap numeric/mapping/book checks.
- Every 30 seconds by default: new public REST snapshots for active/recent markets.
- Every 5 seconds: health/status/disk state.
- On disconnect, malformed state, or invalid book: immediate uncertainty plus recovery snapshot.
- After raw finalization: checksum manifest.
- After normalization/compaction: Arrow schema, row count, atomic publication, checksum manifest.
- Post-close: public trade reconciliation and resolution completeness.

## Suitability decision

For strategy evaluation, use `local_receive_time`, `reject_on_gap=true`, both token snapshots, and
both BTC sources. Relaxing gap rejection is appropriate only for exploratory analysis and the report
must retain every degraded interval. Never compare a strategy across runs with different data-quality
filters without labeling the difference.
