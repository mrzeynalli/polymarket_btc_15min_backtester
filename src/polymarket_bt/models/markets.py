from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import Field

from polymarket_bt.clock import decimal_to_scaled
from polymarket_bt.constants import (
    MATCHER_VERSION,
    POLYMARKET_PRICE_SCALE,
    SCHEMA_VERSION,
    SHARE_SIZE_SCALE,
)
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
    fee_rate: str | None = None
    fee_exponent: int | None = None
    fee_taker_only: bool | None = None
    maker_base_fee_bps: int | None = None
    taker_base_fee_bps: int | None = None
    # None means the collector did not observe an authoritative duration. Zero
    # means the venue explicitly reported zero seconds or disabled the delay.
    taker_order_delay_ms: int | None = None
    execution_metadata_source: str | None = None
    execution_metadata_received_utc_ns: int | None = None
    execution_metadata_json: str = "{}"
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


def _json_default(value: object) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _decimal_text(value: object | None) -> str | None:
    if value in {None, ""}:
        return None
    try:
        return format(Decimal(str(value)), "f")
    except (InvalidOperation, ValueError):
        return None


def _optional_int(value: object | None) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _seconds_to_exact_ms(value: object | None) -> int | None:
    if value in {None, ""}:
        return None
    try:
        milliseconds = Decimal(str(value)) * 1000
    except (InvalidOperation, ValueError):
        return None
    if not milliseconds.is_finite() or milliseconds < 0:
        return None
    integral = milliseconds.to_integral_value()
    if milliseconds != integral:
        return None
    return int(integral)


def _optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, Decimal)) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    return None


def taker_order_delay_ms_from_payload(payload: dict[str, Any]) -> int | None:
    """Project an explicit duration without inventing one from an enable flag.

    ``GET /markets/{condition_id}`` publishes the authoritative
    ``seconds_delay`` value. The compact ``itode`` field only says whether the
    feature is enabled: false proves a zero delay, while true supplies no
    duration and therefore remains unknown.
    """
    for key in (
        "taker_order_delay_ms",
        "takerOrderDelayMs",
        "orderDelayMs",
        "order_delay_ms",
    ):
        if payload.get(key) not in {None, ""}:
            value = _optional_int(payload[key])
            return max(value, 0) if value is not None else None
    for key in ("seconds_delay", "secondsDelay"):
        if key in payload:
            return _seconds_to_exact_ms(payload[key])
    for key in ("itode", "takerOrderDelayEnabled", "orderDelayEnabled"):
        if key in payload:
            enabled = _optional_bool(payload[key])
            return 0 if enabled is False else None
    return None


class MarketExecutionMetadata(FrozenModel):
    """A timestamped observation of venue execution parameters.

    CLOB market-info uses compact wire keys (`fd.r`, `fd.e`, `fd.to`, and
    `itode`); CLOB market detail carries the authoritative `seconds_delay`.
    This model preserves the exact source object while projecting the fields
    needed by replay into stable, explicit columns.
    """

    schema_version: int = SCHEMA_VERSION
    condition_id: str
    received_utc_ns: int
    sequence: int
    fee_rate: str | None = None
    fee_exponent: int | None = None
    fee_taker_only: bool | None = None
    maker_base_fee_bps: int | None = None
    taker_base_fee_bps: int | None = None
    taker_order_delay_ms: int | None = None
    tick_size_scaled: int | None = None
    minimum_order_size_scaled: int | None = None
    source: str
    metadata_json: str
    raw_event_reference: str | None = None

    @classmethod
    def from_clob_payload(
        cls,
        *,
        condition_id: str,
        payload: dict[str, Any],
        received_utc_ns: int,
        sequence: int,
        raw_event_reference: str | None,
        source: str = "clob_rest_market_info",
    ) -> MarketExecutionMetadata:
        fee = payload.get("fd") or payload.get("fee_details") or payload.get("feeDetails") or {}
        if not isinstance(fee, dict):
            fee = {}
        tick = payload.get("mts", payload.get("minimum_tick_size"))
        minimum = payload.get("mos", payload.get("minimum_order_size"))
        return cls(
            condition_id=condition_id,
            received_utc_ns=received_utc_ns,
            sequence=sequence,
            fee_rate=_decimal_text(fee.get("r", fee.get("rate"))),
            fee_exponent=_optional_int(fee.get("e", fee.get("exponent"))),
            fee_taker_only=(
                bool(fee.get("to", fee.get("taker_only")))
                if "to" in fee or "taker_only" in fee
                else None
            ),
            maker_base_fee_bps=_optional_int(payload.get("mbf", payload.get("maker_base_fee"))),
            taker_base_fee_bps=_optional_int(payload.get("tbf", payload.get("taker_base_fee"))),
            taker_order_delay_ms=taker_order_delay_ms_from_payload(payload),
            tick_size_scaled=(
                decimal_to_scaled(str(tick), POLYMARKET_PRICE_SCALE, field="market_tick_size")
                if tick not in {None, ""}
                else None
            ),
            minimum_order_size_scaled=(
                decimal_to_scaled(str(minimum), SHARE_SIZE_SCALE, field="market_minimum_order")
                if minimum not in {None, ""}
                else None
            ),
            source=source,
            metadata_json=json.dumps(
                payload, default=_json_default, separators=(",", ":"), sort_keys=True
            ),
            raw_event_reference=raw_event_reference,
        )

    @classmethod
    def from_market_record(
        cls,
        market: MarketRecord,
        *,
        received_utc_ns: int,
        sequence: int,
        raw_event_reference: str | None,
    ) -> MarketExecutionMetadata:
        try:
            fields = json.loads(market.fee_fields_json)
        except (json.JSONDecodeError, TypeError):
            fields = {}
        if not isinstance(fields, dict):
            fields = {}
        schedule = fields.get("feeSchedule") or fields.get("fee_schedule") or {}
        if not isinstance(schedule, dict):
            schedule = {}
        return cls(
            condition_id=market.condition_id,
            received_utc_ns=received_utc_ns,
            sequence=sequence,
            fee_rate=market.fee_rate or _decimal_text(schedule.get("rate")),
            fee_exponent=(
                market.fee_exponent
                if market.fee_exponent is not None
                else _optional_int(schedule.get("exponent"))
            ),
            fee_taker_only=(
                market.fee_taker_only
                if market.fee_taker_only is not None
                else bool(schedule["takerOnly"])
                if "takerOnly" in schedule
                else None
            ),
            maker_base_fee_bps=(
                market.maker_base_fee_bps
                if market.maker_base_fee_bps is not None
                else _optional_int(fields.get("makerBaseFee"))
            ),
            taker_base_fee_bps=(
                market.taker_base_fee_bps
                if market.taker_base_fee_bps is not None
                else _optional_int(fields.get("takerBaseFee"))
            ),
            taker_order_delay_ms=(
                market.taker_order_delay_ms
                if market.taker_order_delay_ms is not None
                else taker_order_delay_ms_from_payload(fields)
            ),
            tick_size_scaled=market.tick_size_scaled,
            minimum_order_size_scaled=market.minimum_order_size_scaled,
            source="gamma",
            metadata_json=market.fee_fields_json,
            raw_event_reference=raw_event_reference,
        )

    def apply_to_market(self, market: MarketRecord) -> MarketRecord:
        return market.model_copy(
            update={
                "fee_rate": self.fee_rate if self.fee_rate is not None else market.fee_rate,
                "fee_exponent": self.fee_exponent
                if self.fee_exponent is not None
                else market.fee_exponent,
                "fee_taker_only": (
                    self.fee_taker_only
                    if self.fee_taker_only is not None
                    else market.fee_taker_only
                ),
                "maker_base_fee_bps": (
                    self.maker_base_fee_bps
                    if self.maker_base_fee_bps is not None
                    else market.maker_base_fee_bps
                ),
                "taker_base_fee_bps": (
                    self.taker_base_fee_bps
                    if self.taker_base_fee_bps is not None
                    else market.taker_base_fee_bps
                ),
                # A CLOB observation is allowed to clear an earlier inferred or
                # fallback value: true-without-duration is genuinely unknown.
                "taker_order_delay_ms": (
                    self.taker_order_delay_ms
                    if self.source.startswith("clob_rest") or self.taker_order_delay_ms is not None
                    else market.taker_order_delay_ms
                ),
                "execution_metadata_source": self.source,
                "execution_metadata_received_utc_ns": self.received_utc_ns,
                "execution_metadata_json": self.metadata_json,
                "tick_size_scaled": self.tick_size_scaled or market.tick_size_scaled,
                "minimum_order_size_scaled": (
                    self.minimum_order_size_scaled
                    if self.minimum_order_size_scaled is not None
                    else market.minimum_order_size_scaled
                ),
            }
        )


class MatchDecision(FrozenModel):
    accepted: bool
    ambiguous: bool
    score: float = Field(ge=0, le=1)
    matched_rules: tuple[str, ...]
    rejected_rules: tuple[str, ...]
    reason: str
    market: MarketRecord | None = None
