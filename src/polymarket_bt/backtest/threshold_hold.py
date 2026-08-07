"""The threshold-and-hold strategy, stated precisely enough to be falsifiable.

Informally: *buy the favoured side once it reaches a price, inside a time window
late in the 15-minute market; then hold to settlement unless it falls back to a
stop-loss price, in which case get out.*

Every clause of that sentence hides a decision that changes the measured result,
so each is an explicit parameter with an explicit default:

- "reaches a price" — measured on the **executable ask**, the price actually
  payable, never the midpoint. A midpoint of 0.80 in a 0.78/0.82 book is not a
  trade anyone can do. Note that *reaching* a price is not the same as *crossing*
  it: if the ask is already past the trigger when the window opens, the entry
  fires immediately at that higher price, which is what `entry_limit_price_scaled`
  exists to bound.
- "the favoured side" — the token whose ask is at or above the trigger. Both sides
  cannot qualify at once while the pair prices near 1.00.
- "falls back to a stop-loss price" — measured on the **executable bid**, the
  price actually receivable, and acted on with latency like any other decision.
  The stop can be armed late via `stop_loss_from_minute`, so an early wobble is
  ridden out and only a late collapse is sold into.
- "gets out" — a marketable sell that walks real bids. It can fill partially or
  fail outright, which is the risk this whole exercise exists to quantify.

A stop is not a price guarantee. When the book gaps straight through the stop
price, the exit prints where the remaining bids are, and `stop_slippage_scaled`
records the difference.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from polymarket_bt.config import StrictModel
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE

STRATEGY_VERSION = "threshold-hold-v1"

SideSelection = Literal["favoured", "up", "down"]
EntryExecutionPolicy = Literal["immediate", "twap", "adaptive_pov"]


class ThresholdHoldParams(StrictModel):
    """Parameters of one strategy variant."""

    # --- entry ---------------------------------------------------------------
    entry_from_minute: float = Field(default=12.0, ge=0)
    entry_to_minute: float = Field(default=14.0, gt=0)
    entry_trigger_price_scaled: int = Field(default=800_000, gt=0, lt=POLYMARKET_PRICE_SCALE)
    # Never pay more than this, whatever the book does between decision and arrival.
    entry_limit_price_scaled: int = Field(default=850_000, gt=0, le=POLYMARKET_PRICE_SCALE)
    side_selection: SideSelection = "favoured"
    order_shares_scaled: int | None = Field(default=100 * SHARE_SIZE_SCALE, gt=0)
    order_notional_scaled: int | None = Field(default=None, gt=0)
    allow_partial_entry: bool = True
    max_entries_per_episode: int = Field(default=1, ge=1)
    reentry_cooldown_seconds: float = Field(default=30.0, ge=0)
    # Separate the signal from the way the parent BUY is executed.  Immediate is
    # the audited FAK baseline; the sequential policies make causal child-order
    # decisions over a finite horizon and are scored on implementation shortfall.
    entry_execution_policy: EntryExecutionPolicy = "immediate"
    execution_horizon_seconds: float = Field(default=2.0, gt=0, le=60)
    execution_slices: int = Field(default=4, ge=1, le=100)
    execution_participation_ppm: int = Field(default=200_000, gt=0, lt=1_000_000)

    # --- exit ----------------------------------------------------------------
    stop_loss_price_scaled: int | None = Field(default=750_000, gt=0, lt=POLYMARKET_PRICE_SCALE)
    # A stop armed from minute N ignores the price before it. The move that
    # threatens settlement is the late one; an early dip in a market that still
    # has ten minutes to recover is noise, and selling into it is how a stop turns
    # eventual winners into realised losses.
    stop_loss_from_minute: float | None = Field(default=None, ge=0)
    stop_loss_fraction_ppm: int = Field(default=1_000_000, gt=0, le=1_000_000)
    take_profit_price_scaled: int | None = Field(default=None, gt=0, le=POLYMARKET_PRICE_SCALE)
    take_profit_fraction_ppm: int = Field(default=1_000_000, gt=0, le=1_000_000)
    # A stop that fills partially is re-sent, as a live bot would; each retry pays
    # latency again and meets whatever book exists by then.
    stop_retry_limit: int = Field(default=3, ge=0)
    stop_retry_interval_ms: int = Field(default=250, gt=0)
    # Optional unconditional flatten before the boundary, for variants that refuse
    # to carry settlement risk.
    flatten_before_end_seconds: float | None = Field(default=None, ge=0)
    hold_to_settlement: bool = True

    @model_validator(mode="after")
    def _check(self) -> ThresholdHoldParams:
        if self.entry_to_minute <= self.entry_from_minute:
            raise ValueError("entry_to_minute must exceed entry_from_minute")
        if self.entry_limit_price_scaled < self.entry_trigger_price_scaled:
            raise ValueError("entry_limit_price_scaled must be at least the trigger price")
        if (self.order_shares_scaled is None) == (self.order_notional_scaled is None):
            raise ValueError("exactly one of order_shares_scaled or order_notional_scaled")
        if self.stop_loss_price_scaled is not None and (
            self.stop_loss_price_scaled >= self.entry_trigger_price_scaled
        ):
            raise ValueError("stop_loss_price_scaled must be below the entry trigger")
        if (
            self.take_profit_price_scaled is not None
            and self.take_profit_price_scaled <= self.entry_trigger_price_scaled
        ):
            raise ValueError("take_profit_price_scaled must be above the entry trigger")
        if self.stop_loss_from_minute is not None and self.stop_loss_price_scaled is None:
            raise ValueError("stop_loss_from_minute needs a stop_loss_price_scaled to arm")
        if self.entry_execution_policy != "immediate" and not self.allow_partial_entry:
            raise ValueError(
                "sequential entry execution requires allow_partial_entry; "
                "a multi-child parent cannot be made atomic"
            )
        return self

    @property
    def label(self) -> str:
        """Stable human-readable identity used in reports and sweep tables."""
        stop = (
            "hold"
            if self.stop_loss_price_scaled is None
            else f"{self.stop_loss_price_scaled / 1e6:.2f}"
        )
        armed = "" if self.stop_loss_from_minute is None else f"@{self.stop_loss_from_minute:g}"
        return (
            f"m{self.entry_from_minute:g}-{self.entry_to_minute:g}"
            f"_e{self.entry_trigger_price_scaled / 1e6:.2f}"
            f"_x{stop}{armed}"
            f"_s{(self.order_shares_scaled or 0) / SHARE_SIZE_SCALE:g}"
            f"_p{self.entry_execution_policy}"
        )
