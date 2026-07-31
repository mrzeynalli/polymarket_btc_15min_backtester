from __future__ import annotations

import uuid
from enum import StrEnum
from typing import Literal

from pydantic import Field

from polymarket_bt.models.events import FrozenModel


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class SimulatedOrderType(StrEnum):
    MARKETABLE_TAKER = "MARKETABLE_TAKER"
    LIMIT_GTC_SIMULATED = "LIMIT_GTC_SIMULATED"


class OrderIntent(FrozenModel):
    order_intent_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy_id: str
    decision_timestamp_ns: int
    token_id: str
    condition_id: str
    outcome: str
    side: Side
    order_type: SimulatedOrderType = SimulatedOrderType.MARKETABLE_TAKER
    limit_price_scaled: int | None = None
    requested_shares_scaled: int | None = None
    requested_notional_scaled: int | None = None
    time_in_force: str = "IOC"
    metadata: dict[str, str] = Field(default_factory=dict)


class SimulatedFill(FrozenModel):
    fill_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_intent_id: str
    condition_id: str
    token_id: str
    side: Side
    price_scaled: int
    size_scaled: int
    notional_scaled: int
    fee_scaled: int
    book_event_sequence: int
    decision_time_ns: int
    scheduled_arrival_time_ns: int
    fill_time_ns: int
    level_rank: int
    liquidity_remaining_after_scaled: int


class OrderResult(FrozenModel):
    intent: OrderIntent
    status: Literal["accepted", "rejected", "partial", "filled"]
    rejection_reason: str | None = None
    fills: tuple[SimulatedFill, ...] = ()
