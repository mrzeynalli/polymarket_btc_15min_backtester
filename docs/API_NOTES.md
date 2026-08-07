# API notes

Verified against official Polymarket documentation and live public responses through
**2026-08-02 UTC**.
Revalidate this file, fixtures, subscription schemas, and fees after every dependency or API upgrade.

## Endpoint summary

| Purpose | Verified endpoint | Authentication |
|---|---|---|
| Discovery/metadata | `https://gamma-api.polymarket.com` | none |
| Book initialization/recovery | `https://clob.polymarket.com/book`, `/books` | none |
| Per-market execution parameters | `https://clob.polymarket.com/clob-markets/{condition_id}`, `/markets/{condition_id}` | none |
| Optional comparison | `https://clob.polymarket.com/prices-history` | none |
| Level-2/trades/lifecycle | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | none |
| BTC references | `wss://ws-live-data.polymarket.com` | none |
| Post-close trades | `https://data-api.polymarket.com/trades` | none |

No authenticated route is implemented or configured.

## Gamma observations

The live BTC 15-minute family used event slugs like `btc-updown-15m-<unix-boundary>`. This is not
treated as a contract: the matcher also checks text, duration, tags/series, directional outcomes,
token IDs, book enablement, and time plausibility.

Gamma returned `outcomes` and `clobTokenIds` as JSON-encoded strings in the observed market payload.
The arrays are zipped only after outcome labels are normalized; array position is never assumed to
mean UP. The observed payload included `conditionId`, `enableOrderBook`, `orderPriceMinTickSize`,
`orderMinSize`, `acceptingOrders`, `negRisk`, `resolutionSource`, and `feeSchedule`.

An actual sanitized 2026-07-31 response is in `tests/fixtures/gamma/current_btc_15m.json`.

The collector refreshes both `GET /clob-markets/{condition_id}` and
`GET /markets/{condition_id}` for relevant markets. The first response's compact `fd.r`, `fd.e`, and
`fd.to` fee fields and the second response's `seconds_delay` are archived independently and
normalized as timestamped `market_execution_metadata`. `itode` is only an enable flag, not a
duration: live verification found `itode=true` alongside `seconds_delay=0`, so true without an
explicit duration is recorded as unknown and false as zero. Replay converts exact seconds to
milliseconds and merges the latest causal observations from both endpoints; Gamma remains a
fallback for fields the venue responses did not provide.

## CLOB REST books

`GET /book?token_id=<id>` returned a complete object with `market`, `asset_id`, `timestamp`, `hash`,
`bids`, `asks`, `min_order_size`, `tick_size`, `neg_risk`, and `last_trade_price`. The request and
response clocks, status, diagnostic headers, retry count, and exact body are archived separately.

The official schema describes bid/ask order, but the 2026-07-31 live response did not reliably arrive
in executable order. The implementation therefore never trusts wire array order: bids are sorted
descending and asks ascending during snapshot ranking and in-memory reconstruction. This is a
defensive ordering difference from a literal reading of the schema, not a change to raw payloads.

`POST /books` accepts a JSON array of `{ "token_id": "..." }`. `GET /prices-history` uses `market`,
`startTs`, `endTs`, and `fidelity`. They are public comparison/recovery helpers, not hot-path polling.

A sanitized live book is in `tests/fixtures/clob/rest_book.json`.

## CLOB market WebSocket

Initial subscription verified from current official documentation:

```json
{
  "assets_ids": ["<UP_TOKEN_ID>", "<DOWN_TOKEN_ID>"],
  "type": "market",
  "custom_feature_enabled": true
}
```

Dynamic changes use `operation: "subscribe"` or `"unsubscribe"` with `assets_ids`. The channel does
not provide a durable sequence contract used by this project, so each received frame gets a local
process sequence and connection UUID. A reconnect creates an explicit uncertainty interval and new
REST snapshots; it is never treated as exact gap repair.

Application heartbeat behavior is text `PING` every 10 seconds with text `PONG` expected. The
collector disables protocol-level WebSocket ping timers so these semantics are explicit and logged.

Verified standard event names are `book`, `price_change`, `last_trade_price`, and
`tick_size_change`. With custom events enabled, official docs list `best_bid_ask`, `new_market`, and
`market_resolved`. Unknown events are preserved and produce a replay-eligible warning.

### Price-change semantics

Current official market-channel documentation defines `price_changes[].size` as the **current/new
absolute size at that level**, not a delta. A size of `0` removes the level. The normalized field is
therefore named `new_size_scaled`; the reconstructor sets or removes it. This conclusion is covered by
snapshot/update fixtures, zero-removal tests, and property tests. If Polymarket changes this contract,
reconstruction must fail closed until the parser and this note are versioned.

The optional per-change `best_bid` and `best_ask` are compared to reconstructed top-of-book. Source
`hash` is stored but not recomputed because no official equivalent hash algorithm was documented in
the reviewed pages.

At the 2026-07-31 live tick transition, the feed sent explicit `tick_size_change` frames from `0.01`
to `0.001` before prices such as `0.004` and `0.996`. Later full WebSocket `book` frames did not
always repeat a `tick_size` field, while public REST `/book` did report `tick_size="0.001"`. The
collector therefore persists tick changes as their own normalized events, retains the last explicit
tick per token, applies it to subsequent field-omitting WebSocket snapshots, and refreshes it from
authoritative REST recovery snapshots. Replay applies the same events in source order. The raw live
transition is covered by a sanitized fixture/regression test; absent or contradictory tick state
still fails closed.

During the 2026-07-31 smoke capture, an exhausted former best level was not always accompanied by a
separate zero-size change. For example, a frame set the `0.36` bid to `450` and reported best bid
`0.36`, while the preceding source state had best bid `0.37`; no intervening `0.37/0` row was present.
The paired custom `best_bid_ask` event asserted the same new top. The reconstructor therefore treats
the reported top as an explicit source invariant and removes only locally retained levels strictly
better than it. Boundary values `best_bid=0` and `best_ask=1` clear the respective side. This rule is
deterministic, covered by a live-derived regression test, and leaves the exact original messages in
the raw archive. A remaining mismatch still fails closed and triggers a REST recovery snapshot.

The `last_trade_price` schema may include `price`, `size`, `side`, `fee_rate_bps`, `timestamp`, and
`transaction_hash`; fields other than price and identity are treated as nullable. Missing size stays
null and never becomes zero or inferred book volume.

Sanitized event fixtures are under `tests/fixtures/clob/`.

## RTDS

The current web documentation showed a plain/comma-separated Binance filter. A controlled live probe
on 2026-07-31 found that form did not deliver the requested Binance updates when combined with the
Chainlink subscription. The live raw socket did deliver `crypto_prices:update` frames using the
JSON-symbol filter also shown by the official TypeScript client:

```json
{
  "action": "subscribe",
  "subscriptions": [
    {"topic": "crypto_prices", "type": "update", "filters": "{\"symbol\":\"BTCUSDT\"}"},
    {"topic": "crypto_prices_chainlink", "type": "*", "filters": "{\"symbol\":\"btc/usd\"}"}
  ]
}
```

This is an observed API difference from the current web example, so the collector uses one compact
JSON filter per Binance symbol. The probe received an empty subscription control frame, one
`crypto_prices:subscribe` history payload, then regular `crypto_prices:update` events whose original
symbol was `btcusdt`. Empty frames and subscribe history remain in raw storage; they are known control
messages and are not misreported as malformed live updates.

The current SDK documentation also presents friendly stream names such as
`prices.crypto.binance`/`prices.crypto.chainlink`; those are SDK abstractions. The raw endpoint
continued to use the topic subscription above during verification. This project talks to the raw
socket and retains the original topic and symbol.

RTDS requires text `PING` every 5 seconds. Neither the current public documentation nor the verified
live socket promised/returned an application `PONG`, so liveness is based on inbound price/control
activity and not a fabricated acknowledgement requirement. PING sends are logged with nullable
receive/round-trip fields. A real PONG will still be preserved and timed if one is ever received.
Price events retain RTDS envelope time, underlying source time, local UTC receipt, and monotonic
receipt separately. Sources are stored as
`BINANCE_BTCUSDT` and `CHAINLINK_BTCUSD`; original `btcusdt` and `btc/usd` spelling is retained.

Observed Chainlink values carried up to 12 fractional digits, which exceeded the brief's illustrative
1e8 scale. `BTC_PRICE_SCALE` is therefore 1e12; this preserves the source decimal exactly and remains
safe in signed int64 for BTC prices below roughly 9.22 million quote units. A sanitized high-precision
live event is `tests/fixtures/rtds/chainlink_high_precision.json`.

Sanitized Binance and Chainlink fixtures are under `tests/fixtures/rtds/`.

## Fees and changelog differences

The current fee documentation uses a versioned market schedule. For the observed crypto market,
Gamma exposed `feeSchedule = {"exponent":1,"rate":0.07,"takerOnly":true,"rebateRate":0.2}`. The
documented crypto taker curve is modeled as:

```text
fee = shares × rate × price × (1 - price)
```

At 100 shares and price 0.50 this is 1.75 USDC. The configuration effective date is 2026-03-31,
matching the changelog expansion of crypto fees. The formula, parameters, effective dates, rounding,
role, and minimum are configuration, not constants. Before a historical run, select the schedule
observed for each market. The episode index projects the latest authoritative CLOB observation
available before market open (with Gamma as a field-level fallback) and records its source/time;
the legacy event engine still accepts an explicit run-level schedule for compatibility.

The changelog also notes fee-related REST response fields added in March 2026. Accordingly, raw fee
objects are retained as JSON instead of forcing an older fixed schema.

## Rate limits

Official rate-limit documentation says limits are IP-based and may throttle rather than immediately
reject. The reviewed table listed high allowances for `/book` and `/books`, but this project uses
bounded retries and a default 30-second validation interval instead of approaching those limits.
`429`, `5xx`, timeout, and disconnect handling uses exponential backoff; `Retry-After` is respected
when present.

## Known current limitations

- A live public Data API response observed on 2026-07-31 contained trade prices with more than six
  fractional digits (for example `0.9560913706`), while the corresponding CLOB book/trade scale is
  1e6. The raw response is retained exactly. Reconciliation now compares arbitrary-precision
  decimals directly, records the original API values in its report, and classifies a differing
  strongly identified row as `conflicting`; it never rounds the API value into the normalized CLOB
  scale or aborts the whole job.
- Data API reconciliation currently requests up to 10,000 public trades in one response. A market
  exceeding that count needs pagination before reconciliation can be called complete.
- Gamma close/resolution metadata is refreshed while events remain discoverable, but lifecycle
  completeness ultimately depends on retained `market_resolved` events and later reconciliation.
- The exact source hash algorithm is not documented, so source hashes are recorded but not labeled as
  locally verified.
- An inferred subscription acknowledgement is logged on the first non-heartbeat market frame because
  this channel does not document a dedicated acknowledgement in every deployment.

## Official references reviewed

- Polymarket Market WebSocket channel and realtime-data documentation
- Polymarket RTDS/realtime-data documentation
- CLOB `GET /book`, `POST /books`, and prices-history API reference
- Polymarket rate limits
- Polymarket trading fees
- Polymarket changelog

The URLs are linked from the final implementation report and should be re-opened during upgrades.
