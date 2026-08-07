from __future__ import annotations

import json
import re
from decimal import Decimal
from typing import Any

import orjson

from polymarket_bt.clock import decimal_to_scaled, parse_timestamp_ns, utc_now_ns
from polymarket_bt.config import DiscoveryConfig
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE
from polymarket_bt.models.markets import MarketExecutionMetadata, MarketRecord, MatchDecision

_DIRECTION_WORDS = {"up", "down"}
_ASSET_PATTERN = re.compile(r"\b(bitcoin|btc)\b", re.IGNORECASE)


def _json_array(value: object) -> list[object]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = orjson.loads(value)
        if isinstance(parsed, list):
            return parsed
    return []


def _normalized_label(value: object) -> str:
    return str(value).strip().casefold()


def _event_start_ns(
    event: dict[str, Any], market: dict[str, Any], duration_minutes: int
) -> int | None:
    for value in (
        market.get("eventStartTime"),
        event.get("startTime"),
        event.get("eventStartTime"),
    ):
        if value:
            return parse_timestamp_ns(str(value))
    slug = str(market.get("slug") or event.get("slug") or "")
    suffix = slug.rsplit("-", 1)[-1]
    if suffix.isdigit() and len(suffix) >= 9:
        return parse_timestamp_ns(suffix)
    end = parse_timestamp_ns(str(market.get("endDate") or event.get("endDate") or ""))
    if end is not None:
        return end - duration_minutes * 60 * 1_000_000_000
    return None


class Btc15mMarketMatcher:
    """Multi-signal matcher that never derives token outcomes from array position alone."""

    def __init__(self, config: DiscoveryConfig) -> None:
        self.config = config

    def match_event(
        self, event: dict[str, Any], *, discovered_ns: int | None = None
    ) -> list[MatchDecision]:
        markets = event.get("markets")
        if not isinstance(markets, list):
            return []
        return [
            self.match_market(event, market, discovered_ns=discovered_ns)
            for market in markets
            if isinstance(market, dict)
        ]

    def match_market(
        self,
        event: dict[str, Any],
        market: dict[str, Any],
        *,
        discovered_ns: int | None = None,
    ) -> MatchDecision:
        matched: list[str] = []
        rejected: list[str] = []
        score = Decimal("0")
        text = " ".join(
            str(value)
            for value in (
                event.get("title"),
                market.get("question"),
                event.get("ticker"),
                event.get("slug"),
            )
            if value
        )

        if _ASSET_PATTERN.search(text):
            score += Decimal("0.25")
            matched.append("asset_bitcoin_or_btc")
        else:
            rejected.append("asset_not_bitcoin")

        words = {word.casefold() for word in re.findall(r"[A-Za-z]+", text)}
        if _DIRECTION_WORDS.issubset(words) or "updown" in text.casefold():
            score += Decimal("0.15")
            matched.append("direction_up_down")
        else:
            rejected.append("direction_missing")

        start_ns = _event_start_ns(event, market, self.config.duration_minutes)
        end_ns = parse_timestamp_ns(str(market.get("endDate") or event.get("endDate") or ""))
        target_ns = self.config.duration_minutes * 60 * 1_000_000_000
        if (
            start_ns is not None
            and end_ns is not None
            and abs((end_ns - start_ns) - target_ns) <= 60_000_000_000
        ):
            score += Decimal("0.25")
            matched.append("duration_approximately_15m")
        else:
            rejected.append("duration_not_15m")

        raw_tag_values = event.get("tags")
        tag_values: list[Any] = raw_tag_values if isinstance(raw_tag_values, list) else []
        tags = {
            str(tag.get("slug") or tag.get("label") or "").casefold()
            for tag in tag_values
            if isinstance(tag, dict)
        }
        raw_series = event.get("series")
        series: list[Any] = raw_series if isinstance(raw_series, list) else []
        series_text = " ".join(
            str(item.get("recurrence") or item.get("title") or item.get("slug") or "")
            for item in series
            if isinstance(item, dict)
        ).casefold()
        if {"bitcoin", "15m"}.issubset(tags) or ("btc" in series_text and "15m" in series_text):
            score += Decimal("0.10")
            matched.append("tags_or_series_btc_15m")
        else:
            rejected.append("tags_or_series_weak")

        slug = str(market.get("slug") or event.get("slug") or "").casefold()
        if "btc" in slug and ("15m" in slug or "15-min" in slug):
            score += Decimal("0.05")
            matched.append("supporting_slug_pattern")
        else:
            rejected.append("slug_pattern_absent")

        outcomes = [str(item) for item in _json_array(market.get("outcomes"))]
        tokens = [str(item) for item in _json_array(market.get("clobTokenIds"))]
        normalized = [_normalized_label(item) for item in outcomes]
        directional = len(outcomes) == 2 and set(normalized) == _DIRECTION_WORDS
        if directional:
            score += Decimal("0.10")
            matched.append("exact_directional_outcomes")
        else:
            rejected.append("ambiguous_outcomes")
        valid_tokens = len(tokens) == 2 and all(
            token.isdigit() and len(token) > 20 for token in tokens
        )
        if valid_tokens:
            score += Decimal("0.05")
            matched.append("two_valid_clob_tokens")
        else:
            rejected.append("invalid_clob_tokens")

        orderbook = bool(market.get("enableOrderBook", event.get("enableOrderBook", False)))
        plausible = start_ns is not None and end_ns is not None and start_ns < end_ns
        if orderbook and plausible:
            score += Decimal("0.05")
            matched.append("orderbook_and_time_plausible")
        else:
            rejected.append("orderbook_or_time_invalid")

        numeric_score = float(score)
        ambiguous = not directional or not valid_tokens or start_ns is None or end_ns is None
        accepted = numeric_score >= self.config.minimum_match_score and not ambiguous
        if not accepted:
            return MatchDecision(
                accepted=False,
                ambiguous=ambiguous,
                score=numeric_score,
                matched_rules=tuple(matched),
                rejected_rules=tuple(rejected),
                reason="ambiguous token/time mapping" if ambiguous else "score below threshold",
            )

        outcome_to_token = dict(zip(normalized, tokens, strict=True))
        assert start_ns is not None
        assert end_ns is not None
        fee_fields = {
            key: market.get(key)
            for key in (
                "feesEnabled",
                "feeType",
                "feeSchedule",
                "makerBaseFee",
                "takerBaseFee",
                "makerRebatesFeeShareBps",
                "takerOrderDelayMs",
                "orderDelayMs",
                "takerOrderDelayEnabled",
                "orderDelayEnabled",
                "secondsDelay",
                "itode",
            )
            if key in market
        }
        tick_raw = str(market.get("orderPriceMinTickSize", "0.01"))
        min_size_raw = str(market.get("orderMinSize", "0"))
        market_record = MarketRecord(
            gamma_event_id=str(event.get("id", "")),
            gamma_market_id=str(market.get("id", "")),
            condition_id=str(market.get("conditionId", "")),
            event_slug=str(event.get("slug", "")),
            market_slug=str(market.get("slug", "")),
            question=str(market.get("question") or event.get("title") or ""),
            description=str(market.get("description") or event.get("description") or ""),
            market_start_utc_ns=start_ns,
            market_end_utc_ns=end_ns,
            discovered_utc_ns=discovered_ns if discovered_ns is not None else utc_now_ns(),
            closed_utc_ns=parse_timestamp_ns(market.get("closedTime")),
            resolved_utc_ns=None,
            active=bool(market.get("active", event.get("active", False))),
            closed=bool(market.get("closed", event.get("closed", False))),
            accepting_orders=bool(market.get("acceptingOrders", False)),
            orderbook_enabled=orderbook,
            neg_risk=bool(market.get("negRisk", event.get("negRisk", False))),
            tick_size_scaled=decimal_to_scaled(tick_raw, POLYMARKET_PRICE_SCALE, field="tick_size"),
            minimum_order_size_scaled=decimal_to_scaled(
                min_size_raw, SHARE_SIZE_SCALE, field="minimum_order_size"
            ),
            fee_fields_json=json.dumps(
                fee_fields,
                default=lambda value: (
                    format(value, "f") if isinstance(value, Decimal) else str(value)
                ),
                separators=(",", ":"),
                sort_keys=True,
            ),
            resolution_source=str(
                market.get("resolutionSource") or event.get("resolutionSource") or ""
            ),
            resolution_rules=str(market.get("description") or event.get("description") or ""),
            up_token_id=outcome_to_token["up"],
            down_token_id=outcome_to_token["down"],
            up_outcome_label=outcomes[normalized.index("up")],
            down_outcome_label=outcomes[normalized.index("down")],
            match_score=numeric_score,
            matched_rules=tuple(matched),
            rejected_rules=tuple(rejected),
        )
        market_record = MarketExecutionMetadata.from_market_record(
            market_record,
            received_utc_ns=market_record.discovered_utc_ns,
            sequence=0,
            raw_event_reference=None,
        ).apply_to_market(market_record)
        return MatchDecision(
            accepted=True,
            ambiguous=False,
            score=numeric_score,
            matched_rules=tuple(matched),
            rejected_rules=tuple(rejected),
            reason="accepted by multi-signal matcher",
            market=market_record,
        )
