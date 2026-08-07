# Causal sequential execution baseline

`backtest/optimal_execution.py` separates the strategy's signal from the policy
used to acquire or liquidate its position. It supports two deterministic baselines:

- `TwapPolicy`: equal parent-order slices on a finite schedule.
- `AdaptivePovPolicy`: participation in public volume observed so far, with a
  partial TWAP floor, present-price throttle, and deadline catch-up.

Both policies can only wait or send an immediate taker child. They are execution
baselines, not a claim that a globally optimal policy has been learned. Passive
actions should only be added once the reference engine supports maker queue
position, cancellation latency, and partial maker fills.

## Objective

At or after the parent deadline, `evaluate_execution_objective` evaluates:

```text
J = w_is * implementation shortfall
  + w_nf * benchmark notional of unfilled shares
  + w_as * adverse fill-to-deadline markout
```

Fees are included in implementation shortfall. A favourable fill may therefore
have negative shortfall. Non-fill and adverse-selection terms are non-negative.
Weights use parts-per-million fixed point.

The deadline mark is accepted only by the evaluator. It is not present in the
policy interface, preventing it from leaking into decisions.

## Tape integration

For each candidate token, create one `ExecutionPlan` and
`SequentialExecutionSession` when the strategy signal fires. The session owns
causal volume, fills, cash, inventory, and the outstanding child. Drive it with a
sorted, deduplicated union of events from parent start through parent deadline:

- the fixed 250 ms policy clock;
- every same-token book-state timestamp;
- every same-token public-trade timestamp; and
- the exact parent deadline, even when no book or trade event occurs there.

At each decision event:

1. Select the latest reconstructed L2 state at or before the event. Never select a
   later book row to manufacture a decision-time state.
2. Call `session.decide(...)`. The session advances `CausalVolumeCursor` only
   through same-token public prints with `trade_utc_ns <= decision_utc_ns` and
   constructs `ExecutionState` from that causal book and volume view.
3. For `WAIT`, send nothing. For `TAKER_SLICE`, submit
   `requested_shares_scaled` through `TakerExecutor` at decision plus sampled
   latency. Apply exact arrival-time depth, fee, price-limit, market-status,
   deadline, and cash checks there.
4. Call `session.record_outcome` before the next decision. These baselines use
   immediate FAK children, so no child remains outstanding between decisions.
5. Do not pass a post-arrival public-volume window into `TakerExecutor`.
   Sequential participation comes solely from the causal cursor.
6. Keep consumed depth unavailable until that exact price level is restated or a
   full snapshot replaces the ladder. Reject any child whose arrival is after the
   parent deadline or at/after market close.

The policy receives an exact-deadline decision, subject to the same arrival-time
deadline check. Then call `session.evaluate` at that exact timestamp, using the
latest book mark at or before it; do not wait for the next book row. The evaluator
scores the recorded slices through `evaluate_execution_objective`. Compare
immediate sweep, TWAP, and adaptive POV using identical signal timestamps and
realism assumptions. Policy parameters should ultimately be calibrated
out-of-sample against authenticated live order acknowledgements and fills.
