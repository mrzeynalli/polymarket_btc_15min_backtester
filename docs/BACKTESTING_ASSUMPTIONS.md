# Backtesting assumptions

The engine is event-driven and intended to answer, “What could this server have done using data
available by that time?” It is not a candle simulator and does not claim exchange-internal queue
knowledge.

## Replay clock and ordering

Supported modes are `local_receive_time` and `exchange_time`. The default is local receipt because a
live process cannot act before its network stack receives an event. Exchange time is useful for
source-time studies but may reorder events across unsynchronized producers and cannot remove network
latency.

The deterministic key is:

1. selected replay timestamp;
2. local receive timestamp;
3. documented source priority only as a final cross-source tie breaker;
4. connection ID;
5. process sequence;
6. parent change index.

Source priorities are snapshot 10, book update 20, trade 30, BTC 40, and resolution 50. They matter
only after timestamp equality; they do not permit an event to cross a later arrival.

Orders scheduled strictly before a timestamp execute against the prior book. All source events at the
timestamp are applied in deterministic order, then orders arriving exactly at the timestamp execute.
This convention is stable and covered by golden/replay tests.

## No-look-ahead protections

Strategy context contains only reconstructed books and BTC values already applied by the event
clock. A strategy cannot query future files, final resolution, later REST recovery, or post-close
reconciled trades. Resolution settlement happens at `received_utc_ns` in real-time mode, never at the
market's scheduled end.

`ex_post_resolution_mode` is reserved for explicitly labeled retrospective analysis. The current
event engine still requires a captured resolution event; it does not inject a winner from a future
registry row.

## Data gaps

Quality intervals are loaded with the events. With `reject_on_gap=true`, entering an unreliable or
excluded interval raises and stops the run. With it false, the run records every degraded interval
used in `quality_report.json`; results must not be compared to complete-data runs without disclosure.

A recovery snapshot repairs current state from receipt onward. It does not reconstruct trades,
transient depth, or fill opportunities inside the preceding gap.

## Taker execution

A marketable buy walks actual asks lowest-to-highest. A sell walks bids highest-to-lowest. One fill is
created per consumed historical level with its rank, remaining displayed size, arrival time, and book
event sequence. Midpoint is never executable unless it is an actual level. Insufficient displayed
liquidity produces a partial fill; insufficient cash or inventory rejects the intent before mutating
the wallet. Displayed book size is reduced by simulated fills to prevent two simulated orders from
consuming the same liquidity.

This conservative mutation assumes the strategy's own taker fill would remove that displayed depth.
It does not simulate the exchange's subsequent replenishment until an actual recorded book event
arrives.

Notional uses integer half-up arithmetic. Buys debit notional plus fee. Sells require inventory and
credit net proceeds. Default settings forbid negative cash and short positions.

## Latency

The scheduled arrival is decision time plus a seeded model covering strategy calculation, order
construction, network submission, and exchange processing as one aggregate. Models:

- constant milliseconds;
- deterministic sampling from an empirical millisecond list;
- seeded lognormal milliseconds;
- disabled/zero for debugging only.

Execution uses the book at scheduled arrival, not signal time. Pending arrivals earlier than the next
event execute before it; equal arrivals execute after that event. The model version, distribution,
parameters, and seed are stored with the report. A future authenticated implementation should split
and recalibrate these components using `LIVE_TRADING_LOGGING.md`.

## Fees

The fee model is versioned configuration with effective dates, market type, liquidity role, formula,
rate, exponent, minimum, and rounding. The supplied current crypto default is
`shares × 0.07 × p × (1-p)` at 1e6 scales and half-up rounding, effective 2026-03-31. It is not an
eternal rule. Historical runs must choose the schedule effective for the input markets.

One configuration applies to the current run. Mixed-era multi-market runs need a future schedule
selector by market timestamp; until then, split runs by fee regime.

## Maker simulation

Public Level-2 has no order IDs or exact queue position. Maker results cannot be exact.

- Conservative: initial queue ahead equals displayed size; only confirmed aggressive volume at the
  level advances it. Displayed cancellation does not help.
- Optimistic: all displayed decreases may reduce queue ahead.
- Proportional: cancellation is assigned ahead in proportion to displayed-ahead share.

Queue primitives are implemented and tested, but the default end-to-end engine rejects
`LIMIT_GTC_SIMULATED` with an explicit message instead of presenting an incomplete model as a fill.
Future maker integration must disclose the chosen queue model, and profitability only under
optimistic assumptions must be highlighted.

## Portfolio and settlement

One wallet spans every market. It tracks available/reserved cash, per-token shares and weighted-
average cost, realized/unrealized P&L, fees, receivables, settlement cash, and event history. No trade
gets a fresh bankroll.

At a captured official resolution, each winning share pays 1 USDC and each losing share pays zero.
Inventory and cost basis are cleared and P&L realized. Settlement before its replay availability is
forbidden in real-time mode.

Current mark-to-market uses reconstructed midpoint only at report end. A missing midpoint contributes
no marked value; this limitation is material for an unresolved, one-sided final book and should be
read alongside open inventory.

Tick-size changes are market events, ordered by the selected replay clock and source sequence. They
update per-token validation state before later book events. A preceding explicit tick overrides the
discovery-time fallback on WebSocket snapshots that omit `tick_size`; public REST recovery snapshots
can refresh the effective value.

## Strategies

`NoOpStrategy` must produce zero orders/fills and proves the event stream can replay. The threshold
example demonstrates callbacks, reference-price state, latency scheduling, and taker execution. It is
not recommended, optimized, or claimed profitable.

## Metrics and reproducibility

Each run captures configuration, Python/platform, Git commit when available, every input manifest and
hash, random seed, fee/latency/execution/queue versions, clock mode, and a deterministic simulation
fingerprint. Identical ordered events, parameters, and seed produce identical simulation artifacts;
the outer report directory UUID is unique by design.

Summary fields include capital, gross/net P&L, fees, return, intents, accepts/rejects, fills/partials,
share/notional volume, UP/DOWN requested exposure, drawdown, slippage, fill ratio, market count,
quality usage, and assumptions. Win rates, average win/loss, profit factor, and time-in-market are null
or zero when they cannot be derived meaningfully; the report never fabricates them. Win rate must
always be interpreted with total P&L and fees.

## Known limitations

- Public feeds do not prove queue position or packet completeness.
- Taker simulation does not model hidden liquidity, matching-engine races, rejected exchange orders,
  or self-impact beyond consuming the replayed local book.
- One fee schedule applies per run.
- Post-close reconciliation is analytical and cannot be exposed to a real-time strategy.
- Current reports do not compute a reliable trade-level win rate or duration-in-market statistic.
- Final midpoint mark-to-market is imperfect for unresolved or one-sided books.
