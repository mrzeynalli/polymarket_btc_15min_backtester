from __future__ import annotations

from pydantic import Field

from polymarket_bt.constants import MATCHER_VERSION, SCHEMA_VERSION
from polymarket_bt.models.events import FrozenModel


class MarketOutcome(FrozenModel):
    condition_id: str
    token_id: str
    outcome: str
    normalized_outcome: str
    outcome_index: int


class MarketRecord(FrozenModel):
    schema_version: int = SCHEMA_VERSION
    gamma_event_id: str
    gamma_market_id: str
    condition_id: str
    event_slug: str
    market_slug: str
    question: str
    description: str = ""
    market_start_utc_ns: int
    market_end_utc_ns: int
    discovered_utc_ns: int
    closed_utc_ns: int | None = None
    resolved_utc_ns: int | None = None
    active: bool
    closed: bool
    accepting_orders: bool
    orderbook_enabled: bool
    neg_risk: bool
    tick_size_scaled: int
    minimum_order_size_scaled: int
    fee_fields_json: str = "{}"
    resolution_source: str = ""
    resolution_rules: str = ""
    up_token_id: str
    down_token_id: str
    up_outcome_label: str
    down_outcome_label: str
    winning_token_id: str | None = None
    winning_outcome: str | None = None
    matcher_version: str = MATCHER_VERSION
    match_score: float = Field(ge=0, le=1)
    matched_rules: tuple[str, ...]
    rejected_rules: tuple[str, ...]
    raw_payload_reference: str | None = None

    @property
    def token_outcomes(self) -> dict[str, str]:
        return {self.up_token_id: "UP", self.down_token_id: "DOWN"}


class MatchDecision(FrozenModel):
    accepted: bool
    ambiguous: bool
    score: float = Field(ge=0, le=1)
    matched_rules: tuple[str, ...]
    rejected_rules: tuple[str, ...]
    reason: str
    market: MarketRecord | None = None
