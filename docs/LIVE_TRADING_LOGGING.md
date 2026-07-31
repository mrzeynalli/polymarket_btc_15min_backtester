# Future live-trading logging specification

> **Specification only.** The current repository does not contain authenticated API clients,
> signing, credentials, user-stream connectivity, or real-order submission. Adding those functions
> requires a separate security review, deployment, and explicit authorization.

## 1. Purpose

Future live logs must make it possible to reconstruct every trading decision, measure true latency,
compare simulated with real fills, diagnose rejected or missing orders, reconcile wallet balances and
positions, detect risk-control failures, and produce an auditable trading record. They must be
append-only, schema-versioned, tamper-evident, time-synchronized, and correlated with the immutable
market-data archive.

Every record needs `schema_version`, `application_version`, `run_id`, stable event ID, UTC nanoseconds,
local monotonic nanoseconds where applicable, and references to causally prior records. Wall time is
for cross-system correlation; monotonic time is authoritative for local durations.

## 2. Signal log

Write one record for every evaluated actionable or non-actionable signal. Required fields:

```text
signal_id
strategy_version
model_version
feature_version
configuration_hash
generated_utc_ns
generated_monotonic_ns
market_data_cutoff_sequence
condition_id
token_id
outcome
signal_name
signal_value
signal_confidence
BTC source values
book-state reference
best bid
best ask
spread
depth metrics
seconds to market end
decision
reason codes
```

Preserve exact references to every input event used: raw file ID/line, normalized event ID, token,
sequence, source timestamp, and local receipt timestamp. Record Binance and Chainlink values
separately, including age. `book-state reference` must identify the snapshot plus last applied update
sequence/hash and quality state. Feature vectors may be stored as a versioned blob with a content
hash, but the human-auditable key features above remain columns.

Log signals that are suppressed due to stale data, warm-up, or risk—not just signals that create
orders. No feature may refer to data later than `market_data_cutoff_sequence`.

## 3. Decision and risk log

For every signal evaluation, record:

```text
decision_id
signal_id
decision_utc_ns
decision_monotonic_ns
requested side
requested size
requested price
order type
time in force
available cash
reserved cash
current inventory
market exposure
total exposure
daily realized P&L
daily drawdown
risk limits
risk checks
risk decision
rejection reason
operator override
```

Every risk check produces its own explicit `check_name`, version, input, configured limit, measured
value, and `pass`/`fail` result. Required checks include stale CLOB/BTC data, valid book, market open,
minimum/tick compliance, balance, inventory, per-order size, per-market exposure, aggregate exposure,
daily loss/drawdown, duplicate intent, rate limit, and kill-switch state.

An operator override needs operator identity, ticket/reason, authorization scope, requested and
effective times, expiry, and before/after risk decision. Overrides must never bypass authentication,
secret-handling, or immutable audit logging.

## 4. Order lifecycle log

One immutable event per state transition, joined by `client_order_id`. Capture these UTC timestamps and
their local monotonic equivalents:

```text
order_intent_created_utc_ns
serialization_started_utc_ns
serialization_completed_utc_ns
signing_started_utc_ns
signing_completed_utc_ns
HTTP_send_started_utc_ns
HTTP_response_received_utc_ns
exchange_ack_timestamp
first_user_ws_update_utc_ns
first_fill_utc_ns
last_fill_utc_ns
cancel_requested_utc_ns
cancel_acknowledged_utc_ns
final_state_utc_ns
```

Required identity/state fields:

```text
client_order_id
exchange_order_id
trade_ids
transaction_hashes when available
condition_id
token_id
side
order_type
limit_price
original_size
remaining_size
filled_size
average_fill_price
status
HTTP status
exchange response code
exchange response body with secrets removed
retry count
idempotency key
```

Also retain `decision_id`, request-body hash, canonical serialized-body hash, credential identifier,
host/run ID, connection/request ID, and causal prior status. Store price, size, notional, fee, and
balance as raw decimal string plus exact scaled integer. State transitions must be monotonic under a
documented state machine; out-of-order updates are retained and flagged, not discarded.

An HTTP timeout is not an order rejection. Mark it `submission_outcome_unknown`, query by idempotency
and client ID, and reconcile before retrying. Never retry a potentially accepted order without a safe
idempotency contract.

## 5. Fill log

Write exactly one row per exchange fill:

```text
fill_id
client_order_id
exchange_order_id
trade_id
transaction_hash
fill_timestamp
local_receive_timestamp
token_id
side
price
size
notional
fee
liquidity role
book state at decision
book state at simulated arrival
book state at actual fill receipt
predicted fill price
actual fill price
predicted slippage
actual slippage
```

Add condition/outcome, user-stream connection/sequence, public-trade match, fee schedule/version,
maker/taker classification confidence, settlement currency, and all UTC/monotonic receive clocks.
Preserve the raw fill update reference. Corrections are new linked records; never update history in
place.

Prediction fields must identify the simulator version, input book sequence, latency assumption, and
queue model. A null prediction is preferable to an invented comparison.

## 6. User WebSocket log

Archive the exact authenticated user-stream messages in a separately encrypted raw store. Record:

```text
connection ID
subscription state
raw messages
order updates
trade updates
disconnects
reconnects
sequence/order identifiers
local receipt timestamps
```

Also capture connect start, authentication sent/acknowledged, heartbeat send/receive, close code,
backoff, resubscription, and uncertainty interval. Raw messages must pass a redaction gateway before
logging; if the protocol echoes credentials, store a redacted payload plus a cryptographic hash of the
original only when security review approves.

The user stream must be reconciled to HTTP order responses. A user update without a known HTTP
response is an incident-worthy `unexpected_user_update`, not discarded. On reconnect, query open
orders, recent trades, positions, and balances before declaring the account state healthy.

## 7. Wallet and position log

Take periodic snapshots and event-triggered snapshots after intent reservation, acknowledgement,
fill, cancel, rejection, resolution, deposit/withdrawal, and reconciliation correction. Fields:

```text
wallet address or non-secret identifier
cash balance
available balance
reserved amount
token balances
open orders
market positions
average cost
realized P&L
unrealized P&L
fees
settlement receivables
allowances
balance source
snapshot timestamp
```

Add snapshot ID, prior snapshot ID, account/chain, block number when on-chain, API response reference,
valuation cutoffs, and quality status. Balances from local ledger, exchange API, and chain are distinct
sources and must not overwrite one another. Reconciliation may choose an authoritative value but must
retain all observations.

Never log private keys, seed phrases, API secrets, or passphrases.

## 8. Reconciliation log

Continuously and end-of-day reconcile:

```text
strategy intent
HTTP submission
HTTP response
user WebSocket
public market trades
open-order query
position query
wallet balance
on-chain settlement
```

Statuses are:

```text
matched
partially matched
missing HTTP response
missing WebSocket update
unexpected fill
duplicate update
balance discrepancy
position discrepancy
unresolved
```

Each reconciliation row needs `reconciliation_id`, entity type/ID, sources compared, source event
references, expected/observed values, exact difference, confidence, first detected time, last checked
time, owner, automatic action, manual action, and resolution reference. Transaction hash alone is not
assumed unique; use exchange trade ID, order ID, token, timestamp, price, size, and side together.

Reconciliation must be idempotent and corrections additive. Unresolved discrepancies block new risk
in the affected market/account according to policy.

## 9. Latency log

Measure, as separate distributions:

```text
market feed latency
strategy compute latency
risk-check latency
order construction latency
signing latency
network request latency
exchange acknowledgement latency
time to first fill
time to full fill
cancel latency
user WebSocket notification latency
```

Durations on one host use monotonic clocks. Cross-system differences use UTC only when clock quality
and source timestamp semantics are known; record NTP offset/uncertainty with each aggregation.
Include route/endpoint, host, connection reuse, response status, payload bytes, retry, market, order
type, and percentile window. Never subtract unrelated source clocks and label the result latency.

## 10. Risk and incident log

Incident categories include:

```text
risk limit breach
stale market data
stale BTC data
crossed or invalid local book
clock drift
excess reconnects
queue saturation
order rejected
unexpected position
balance mismatch
daily loss limit
kill switch triggered
manual intervention
```

Every incident has:

```text
incident_id
severity
start time
end time
affected markets
affected orders
automatic action
operator action
resolution
postmortem reference
```

Also retain detection rule/version, measurements/thresholds, alert delivery attempts, acknowledgement,
timeline events, correlated deployments, evidence hashes, and recovery validation. Critical incidents
should automatically disable new submissions through a structurally independent gate while allowing
cancel/reconcile where safe. Kill-switch activation and reset require distinct immutable records and
two-person review when capital warrants it.

## 11. Deployment and version log

Record at startup, deployment, configuration reload, strategy/model activation, and shutdown:

```text
application version
Git commit
container image digest
configuration hash
strategy version
model hash
dependency lock hash
host ID
process ID
startup time
shutdown time
deployment actor
```

Include database/schema versions, infrastructure release, OS/kernel/Python, clock status, feature
flags, risk-policy hash, secret-manager reference versions (not values), prior deployment ID, change
ticket, canary scope, health result, rollback ID, and clean/incomplete shutdown. Configuration snapshots
must be redacted, canonicalized, and content-addressed.

## 12. Security rules

Never record:

```text
private keys
seed phrases
full API secrets
API passphrases
authentication signatures that should remain confidential
unencrypted credential files
```

Use only:

```text
credential identifier
last four characters where safe
one-way fingerprint
secret-manager reference
```

Redact before structured logging, not during later export. Use an allow-list serializer for HTTP
headers, bodies, environment, exceptions, and WebSocket data. Treat URLs/query strings as potentially
secret. Automated tests must inject canary secrets and prove they never reach logs. Restrict audit
stores by least privilege, encrypt in transit and at rest, authenticate writers, make retention
deletion auditable, and alert on redaction failure.

Authentication signatures may be replay-sensitive or reveal identity; store only a one-way digest if
needed for correlation and approved by security. Stack traces must pass the same redaction policy.

## 13. Retention and backups

Define separate retention and access classes for:

```text
raw market data
application logs
order/fill logs
wallet snapshots
security logs
metrics
```

Authenticated trading records and all backups require encryption at rest, managed key rotation,
off-host copies, integrity hashes, restore tests, geographic/legal review, and access auditing. Order,
fill, wallet, reconciliation, deployment, and incident records should have the longest compliance-
appropriate retention. High-volume metrics can be shorter if the underlying audit events remain.

Backups must be point-in-time consistent across manifests and databases. Document recovery point and
recovery time targets. Test that a restored archive can reproduce an order timeline and ending wallet
without production credentials.

## 14. Live-vs-backtest calibration report

A future scheduled report must compare by strategy, market, order type, liquidity role, time-to-end,
and latency regime:

```text
predicted order arrival
actual acknowledgement
predicted fill probability
actual fill outcome
predicted execution price
actual execution price
predicted fee
actual fee
predicted latency
actual latency
```

Include sample counts, unmatched records, bias, MAE/RMSE where meaningful, quantile calibration,
slippage distributions, partial-fill error, queue-model sensitivity, fee discrepancies, and data-
quality exclusions. Inputs must link to the exact simulated book, live signal/decision, lifecycle,
fills, and deployment versions.

The report drives versioned changes to latency, fee, and maker queue models. Never overwrite old
calibration: publish a new model version, effective range, training window, validation result, and
approval. Highlight any strategy profitable only under optimistic queue or latency assumptions.

## Audit invariants

Future implementation acceptance should require:

- every accepted decision has exactly one lifecycle root;
- every fill maps to an order or is an `unexpected fill` incident;
- every money/inventory mutation maps to a fill, fee, transfer, or settlement;
- retries preserve client ID/idempotency semantics;
- all local durations use monotonic clocks;
- no secret-canary appears in any sink or backup;
- record counts and hashes reconcile across raw, normalized, and report layers;
- a clean-room replay can reproduce decisions and ending wallet from logs alone.
