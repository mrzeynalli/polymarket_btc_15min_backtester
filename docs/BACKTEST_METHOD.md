# Backtesting method for 15-minute threshold-and-hold strategies

This document describes how to backtest the strategy family "buy the favoured side
once it reaches a price late in the market, then hold to settlement unless it falls
back to a stop-loss price" against recorded Polymarket BTC 15-minute markets, in a way
whose results can be believed. The stop can be armed only from a chosen minute, and
the stake can be fixed per market or compounded from a running balance.

It complements [BACKTESTING_ASSUMPTIONS.md](BACKTESTING_ASSUMPTIONS.md), which
documents the reference event engine. This document covers the episode-oriented
layer built on top of it: `polymarket_bt.backtest.episodes`, `.tape`, `.realism`,
`.fastsim`, `.sweep`, and `.verify`.

## Why the naive version of this backtest lies

Four things make this strategy family unusually easy to backtest incorrectly.

1. **The signal price is not a tradable price.** A midpoint of 0.80 in a 0.78/0.82
   book cannot be bought. Every trigger here is evaluated on the executable side —
   the ask for entries, the bid for exits.
2. **Reaching a trigger is not paying it.** If the ask is already past the trigger
   when the entry window opens, the entry fires at that higher price, not at the
   trigger. Because a binary bought at *p* needs to settle in your favour *p* of the
   time just to break even, this silently raises the bar the strategy must clear —
   a 0.85 trigger that fills at 0.89 needs four more points of accuracy. The
   size-weighted average entry price and the break-even rate it implies are
   reported next to the win rate for exactly this reason.
3. **The exit happens exactly when liquidity disappears.** The stop-loss price is
   reached during a directional move, which is when quotes are pulled and every
   other holder is hitting the same bids. A backtest that assumes the stop fills at
   the stop price measures a strategy nobody can trade.
4. **Settlement is where the P&L is.** A recorded `market_resolved` lifecycle event
   is authoritative. Older captures without one need a defensible derived fallback,
   since the winner decides every trade's outcome.

## Layers

| Layer | Module | Responsibility |
|---|---|---|
| Episode index | `backtest/episodes.py` | One row per 15-minute market: window, tokens, data coverage, derived winner, eligibility. |
| Book tape | `backtest/tape.py` | Full depth ladders reconstructed per episode and cached, so sweeps replay states instead of re-deriving them. |
| Execution realism | `backtest/realism.py` | Latency, depth haircuts, participation caps, staleness rules, fees. |
| Strategy | `backtest/threshold_hold.py` | The parameter set, stated precisely. |
| Simulator | `backtest/fastsim.py` | Fast per-episode replay producing a trade ledger. |
| Sweep | `backtest/sweep.py` | Aggregation, realism bands, walk-forward. |
| Verification | `backtest/verify.py` | Cross-check against the audited event engine. |

Everything reads the collector's storage root **read-only**. All outputs go to a
separate workspace; the CLI refuses a workspace inside the storage root.

## 1. Pin the inputs

The collector appends continuously, so "the same backtest" run twice is otherwise
not the same experiment. `episodes.json` records file counts, latest close time, and a deterministic
SHA-256 over every sorted `(relative path, file digest, close time, row count)` manifest entry.
Results
computed against different fingerprints are not comparable and should not be
plotted on the same axis.

## 2. Establish settlement ground truth

Each episode first uses a normalized `market_resolved` event, retaining its local receive timestamp
as the earliest causal settlement availability. In captures without one, the winner is derived from
two independent public observations:

- **Terminal book.** After settlement, the winning token's book collapses to a bid
  at or above 0.95 and the loser's to at most 0.05. Observed up to 60 s past the
  boundary, because the collapse follows the settlement price by a second or two.
- **Reference price.** The RTDS Chainlink series (Binance as fallback) compared
  between the market's open and close boundary, using the last observation at or
  before each.

Agreement yields confidence 0.99; a single source yields 0.90 (book) or 0.70
(reference); **disagreement marks the episode ambiguous and excludes it**. On the
first 24 hours of collection this produced 91 resolved and 7 ambiguous episodes,
with **90 of 90 decidable episodes agreeing across both sources** — which is also
the evidence that the reference-price rule matches the venue's actual resolution.

This is post-hoc information. It is resolved before a run starts and is never
visible to a strategy: the simulator applies it only after the market boundary, and
the reference engine receives it as a synthesised resolution event timestamped 30 s
after the boundary, carrying its provenance (`derived:terminal_book`).

## 3. Gate episodes before measuring anything

An episode is eligible only if both tokens independently have recorded book data, both cover the
open-to-close window, a winner was determined, no non-replay-eligible error overlaps the window, and
no recorded CLOB disconnect gap overlaps it.
Excluded episodes are reported with reasons rather than dropped silently — a
strategy that appears to work only on episodes with complete data would be a
selection effect worth seeing.

## 4. Reconstruct the book, then trade against it

Tapes replay snapshots and absolute-size updates through the same
`BookReconstructor` the engine uses, storing the complete reconstructed bid and ask
ladders at every state change. Production tape construction has no implicit depth
cutoff; a finite depth is available only as an explicit test or experiment setting
and must not be interpreted as venue depth. Where a level change contradicts the
source's own reported top, the book is marked invalid until the next snapshot
resynchronises it, and the simulator **refuses to trade inside those spans** rather
than guessing.

## 5. Execution realism

Calibrate latency from the collector's own measurements rather than assuming it:

```bash
.venv/bin/python -c "from pathlib import Path; \
from polymarket_bt.backtest.realism import calibrate_latency; \
print(calibrate_latency(Path('/var/lib/polymarket-btc-backtester')))"
```

On this server that returns a median of 45 ms and a p95 of 85 ms (WebSocket
heartbeat RTT 36 ms, CLOB REST p50 40 ms / p95 80 ms, plus 5 ms of strategy
compute). For context, in the last three minutes of these markets the top of book
changes every 3.4 ms at the median, so a decision is routinely acted on ten or more
events late.

Three presets bracket the uncertainty. Report all three:

| | optimistic | base | pessimistic |
|---|---|---|---|
| Latency | 25 ms fixed | lognormal, 45 ms median | 250 ms fixed |
| Displayed depth assumed reachable | 100% | 75% | 50% |
| Max share of one level | 100% | 50% | 25% |
| Future public-volume cap on an immediate order | none | none | none |
| Furthest through the touch | unbounded | 10 ticks, ≤ 1¢ | 5 ticks, ≤ ½¢ |
| Ladder levels eligible per child | complete ladder | complete ladder | first 5 |
| Stale-book refusal | none | 5 s | 2 s |

The signal and its entry execution are separate. The dashboard defaults to **adaptive POV**, which
sends causal taker child orders from volume observed so far, maintains a partial TWAP floor,
throttles an adverse present-price move, and catches up at its deadline. **TWAP** follows an even
child schedule; **immediate** is the one-shot marketable-sweep baseline. Adaptive POV and TWAP use a
configurable horizon (2 seconds by default), capped by the entry window and market close. Immediate
ignores the horizon. Results retain the policy and horizon, and policies must be compared under the
same signal, stake, and realism preset. These are deterministic execution baselines, not a claim
that a globally optimal or calibrated live policy has been learned; see
[EXECUTION_POLICY.md](EXECUTION_POLICY.md).

The sequential policies also apply a policy-level cap of 25% of currently executable L2 to each
child before the arrival-time realism rules are evaluated. Adaptive POV's default flow target is
20% participation, computed only from same-token public volume already observed at that decision;
TWAP does not use public volume to set its schedule. These are policy parameters, not differences
between the three realism presets.

**How far an order will chase the book** is what binds most entries in this data,
and it has to be expressed twice. These markets quote on two different ticks —
0.001 on 147 of the 259 indexed markets and 0.01 on the rest — so a tick count
alone means ten times more price on one than the other, and an absolute cent alone
ignores the venue's own granularity. Both bounds are stated and the tighter one
applies. (Until 2026-08-02 this was a tick count implemented as a hardcoded cent,
which let orders sweep ten times further on fine-tick markets and made the two
kinds of market incomparable.)

**Public-volume causality differs by policy.** An immediate FAK order walks only the
arrival-time L2 ladder. Later public prints are future information and never cap or
validate that fill. Adaptive POV advances an incremental cursor only through
same-token trades with `trade_utc_ns <= decision_utc_ns`; if the observed volume is
`V` and participation is `p`, its flow target is `V·p/(1−p)` so the hypothetical
child fills are included in the denominator. TWAP follows its time schedule and
does not use a public-volume target. Neither sequential policy consults prints
after its current decision, and the old two-second post-arrival volume window is
not part of production episode replay.

**Only constraints that actually cut a fill are reported.** A haircut that still
left more depth than the order wanted has bound nothing; recording it anyway made
every fully-filled order look depth-limited and made the fill-quality panel
unreadable.

**A conclusion that does not survive `pessimistic` has not been demonstrated.**

### What the book actually holds

Worth knowing before reading any fill rate, measured over 26,368 sampled ask-side
states in minutes 10–14:

| | p10 | median | p90 |
|---|---:|---:|---:|
| Touch level | 7 sh | 45 sh | 312 sh |
| Top 3 levels | 59 sh | 190 sh | 1,199 sh |
| Top 10 levels | 301 sh | 806 sh | 4,055 sh |

A $100 order at 0.85 wants ~118 shares. The touch alone covers it in 24% of
states; the top ten levels cover it in 98%. So a low fill rate under `base` is
almost never the book being empty — it is the model refusing to pay 1¢ through the
touch to reach the rest of it, which is a statement about price, not depth.
The top-ten figures are a diagnostic slice of the ladder, not a tape-storage cap.

## 6. Run it

```bash
# Index episodes and build tapes (read-only over the collector's data)
polymarket-bt episodes --workspace ../backtest-workspace --build-tapes

# Sweep the strategy family across realism presets, with walk-forward
polymarket-bt sweep --workspace ../backtest-workspace \
  --entry-from 12 --entry-to 14 \
  --triggers 0.80,0.85,0.90,0.95 --stops none,0.60,0.70,0.75 --sizes 100 \
  --execution adaptive_pov,twap,immediate

# The same sweep with every stop armed only from minute 13
polymarket-bt sweep --workspace ../backtest-workspace \
  --entry-from 12 --entry-to 14 --stops 0.70,0.75 --stop-from 13

# Cross-check the fast simulator against the audited event engine
polymarket-bt verify-sim --workspace ../backtest-workspace --episodes 30
```

## 6a. Run it from the website

The same simulator is exposed interactively at `/#/backtest` on the public
dashboard, over a prepared workspace refreshed after each market closes. The form
maps one-to-one onto `ThresholdHoldParams`; see
[DASHBOARD.md](DASHBOARD.md) for the mapping, the job model, and why fill quality is
reported as a headline result rather than a footnote.

The website adds one thing the sweep does not have: a staking rule. Fixed staking
risks the same amount on every market and is what `sweep` measures. Compounding
stakes the cash balance available when each signal fires; pending exit/settlement
credits cannot enlarge an earlier parent order. This makes the ordering of the
episodes part of the result and can end the run early; both are replayed literally
rather than rescaled from a single pass, because fills are not linear in size.

Loading a tape is what a run actually spends its time on, so scalar columns are
copied out of Arrow buffers into flat typed arrays rather than boxed into Python
lists, and depth ladders are read only from the row groups an order touches. That
is a 5x reduction in load time and a 15x reduction in resident memory per episode,
with results identical to the previous loader — verified by re-running a recorded
48-variant sweep and requiring every metric to match.

## 7. Verify the fast path

The fast simulator is only usable because it agrees with the reference engine.
`verify-sim` runs both over each episode with the realism model stripped to the
engine's own assumptions and compares entry fills, exit fills, and net P&L.
Current status: **30 of 30 episodes agree exactly**, 13 of them with trades.
`tests/integration/test_episode_backtest.py` locks the same comparison offline.

Two defects were found and fixed by this comparison, both of which had inflated
results:

- The reference engine subtracted simulated fills directly from its reconstructed
  book, so a later update's reported-top assertion failed against a book the
  simulator itself had edited, ending the replay. Consumption is now an overlay
  (`OrderBook.consumed_bids`/`consumed_asks`); the recorded feed stays authoritative.
- The fast simulator evaluated stops against book states between the entry decision
  and its fill — exiting a position it did not yet hold. Exits now wait for the
  entry's arrival timestamp.

## 8. Metrics, and which ones to distrust

Reported per variant: entries, **side-correct rate** (was the bought side the
winner) and **profitable-trade rate** (did the trade make money) — these differ
sharply once a stop is involved and conflating them is the single easiest way to
believe a losing strategy is winning. Also: net and gross P&L, fees, stop-out rate,
**unfilled exit shares**, and average stop slippage.

Walk-forward selects a variant on past episodes and scores it on the next unseen
block. With 90 episodes the folds are ~22 episodes each; treat the sweep as a
hypothesis generator. On the current sample, in-sample selection produced +67.71
on fold 1 and **−6.84 out of sample** — the cost of choosing parameters on the same
data, in one number.

## 9. First findings (superseded execution model)

> These numbers were measured before the 2026-08-02 execution-model corrections
> described in §5 — the through-touch bound was ten times too loose on fine-tick
> markets, the volume cap vetoed any order arriving in a quiet two seconds, and
> stop slippage was reported unsigned so favourable fills inflated it. The
> qualitative conclusion about stops survives and is re-measured in §9a; treat the
> specific figures here as historical.

From 90 eligible episodes (24 h of collection, entry window minutes 12–14, 100
shares, base realism):

- Entering at 0.80–0.85 and **holding to settlement**: 24 entries, 79% side-correct,
  **+53.92** net. The same entry with a **0.75 stop loss**: the same 24 entries,
  88% stopped out, only 12.5% profitable, **−37.73** net.
- Of 19 entries that were on the eventual winner, **16 would have been stopped out**
  by a 0.75 stop. The stop does not protect the position; it converts winners into
  losses.
- When a stop does trigger, it fills a median of **3.4 cents** and up to **9 cents**
  through the stop price. Separately, 17% of the times the bid crossed 0.75 it
  gapped below 0.70 in a single event, so there was no 0.75 to sell at.
- Every stop variant underperformed its hold counterpart at every trigger price and
  every realism preset.

This is the quantified version of "sometimes exiting was difficult": the exit is not
merely expensive, it is systematically adverse, because the stop price is reached
precisely in the states where the market is about to move against the position.

Fees are material and worth verifying independently before trusting any of the
above: at 0.07·p·(1−p) per share, a round trip at 0.80 costs ~1.1% of notional,
which is a large fraction of this strategy's edge.

## 9a. What the stop costs, and what the entry ceiling costs

From 167 eligible episodes (entry window minutes 10–14, favoured side, $100 per
market, base realism, corrected execution model), a 0.85 trigger:

| variant | trades | net | win rate | side correct | avg entry | break-even |
|---|---:|---:|---:|---:|---:|---:|
| **ceiling 0.95, no stop** | 128 | **+47.23** | 90.6% | 90.6% | 0.8908 | 89.1% |
| ceiling 0.95, 0.75 stop | 128 | −64.60 | 65.6% | 90.6% | 0.8908 | 89.1% |
| ceiling 0.95, stop armed from 13 | 128 | −74.56 | 71.1% | 90.6% | 0.8908 | 89.1% |
| ceiling 0.95, stop armed from 14 | 128 | −61.86 | 76.6% | 90.6% | 0.8908 | 89.1% |
| ceiling 0.86, no stop | 87 | −7.99 | 86.2% | 86.2% | 0.8517 | 85.2% |
| ceiling 0.86, 0.75 stop | 87 | −25.37 | 52.9% | 86.2% | 0.8517 | 85.2% |

**The stop is the expensive part.** Every row with a stop is worse than the same
row without one, by 17 to 112 dollars on identical entries. The side-correct rate
never moves — the stop changes nothing about which side was right — while the win
rate collapses from 90.6% to 65.6%. That gap *is* the stop: a quarter of the
entries that were on the eventual winner were sold at a loss before settlement.

**Arming the stop later does not rescue it.** Delaying to minute 13 or 14 raises
the count win rate (65.6% → 76.6%, since fewer positions are stopped at all) while
making the ones that do stop fire with no time left to recover and the book at its
thinnest. Both are worse than not stopping.

**The ceiling decides the price, and the price decides the bar.** The trigger was
0.85 in every row, but a five-cent ceiling let the size-weighted fill drift to
0.8908, where 89.1% accuracy merely breaks even; a one-cent ceiling held it at
0.8517 for an 85.2% bar. Here the loose ceiling wins on net because it also
admits 41 more trades — but it wins by 1.5 points of margin over its own
break-even, which is not a margin at all at this sample size.

**Compounding amplifies whichever is true rather than changing it**: the
0.86-ceiling hold took $100 to $95.79 (−4.2%, 60% peak drawdown), the 0.95-ceiling
stop variant to $37.36 (−62.6%, 80% drawdown). Read the drawdown column first.

These figures moved substantially — one variant reversed sign — when the execution
model was corrected on 2026-08-02. That is the honest measure of how much a
conclusion here depends on assumptions the public data cannot settle.

## Known limitations

- **Sample size.** A day or two of markets. Every number above is provisional, and
  §9a moved substantially when the execution model was corrected — which is the
  best available evidence that these are hypotheses, not measurements.
- **Taker only.** Resting a limit order at the trigger price (rather than crossing
  the spread) is a materially different strategy. The engine rejects
  `LIMIT_GTC_SIMULATED`; the queue primitives exist but are not integrated, and
  public Level-2 cannot prove queue position.
- **No competing-order model beyond the caps.** Matching-engine races, hidden
  liquidity, and exchange rejects are represented only by the haircuts.
- **Derived resolution.** Winners come from the terminal book and reference price,
  not from an authoritative settlement feed. Recording resolutions in the collector
  would remove this dependency.
- **Single fee schedule per run.** Split runs by fee regime if the schedule changes.
- **The reference engine's ordering key** places `connection_id` ahead of the
  process sequence, so same-timestamp events from two connections for one token
  could in principle be applied out of receive order. No instance was observed in
  this data.
