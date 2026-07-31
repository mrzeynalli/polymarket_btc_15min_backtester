from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from polymarket_bt.clock import PrecisionError, decimal_to_scaled, parse_timestamp_ns
from polymarket_bt.constants import (
    BPS_SCALE,
    BTC_PRICE_SCALE,
    POLYMARKET_PRICE_SCALE,
    SCHEMA_VERSION,
    SHARE_SIZE_SCALE,
    USDC_SCALE,
    Source,
)
from polymarket_bt.models.books import (
    BookLevel,
    BookLevelChange,
    BookSnapshot,
    TickSizeChange,
    TopOfBook,
)
from polymarket_bt.models.events import DataQualityEvent, RawEnvelope
from polymarket_bt.models.markets import MarketRecord
from polymarket_bt.models.prices import BtcPriceEvent
from polymarket_bt.models.trades import TradeEvent


@dataclass(slots=True)
class ParsedEvents:
    snapshots: list[BookSnapshot] = field(default_factory=list)
    updates: list[BookLevelChange] = field(default_factory=list)
    trades: list[TradeEvent] = field(default_factory=list)
    btc_prices: list[BtcPriceEvent] = field(default_factory=list)
    top_of_book: list[TopOfBook] = field(default_factory=list)
    resolutions: list[dict[str, Any]] = field(default_factory=list)
    tick_size_changes: list[TickSizeChange] = field(default_factory=list)
    quality: list[DataQualityEvent] = field(default_factory=list)
    event_types: list[str] = field(default_factory=list)
    unknown_count: int = 0
    invalid_count: int = 0


def load_json_decimal(raw: str) -> Any:
    return json.loads(raw, parse_float=Decimal, parse_int=int)


def parse_tick_size_change(
    envelope: RawEnvelope,
    message: dict[str, Any],
    message_index: int,
    raw_reference: str,
) -> TickSizeChange:
    old_tick = message.get("old_tick_size")
    return TickSizeChange(
        tick_change_id=str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{envelope.run_id}:{envelope.sequence}:{message_index}:tick_size_change",
            )
        ),
        sequence=envelope.sequence,
        condition_id=str(message.get("market") or message.get("condition_id") or ""),
        token_id=str(message["asset_id"]),
        exchange_timestamp_ns=parse_timestamp_ns(message.get("timestamp")),
        received_utc_ns=envelope.received_utc_ns,
        received_monotonic_ns=envelope.received_monotonic_ns,
        old_tick_size_scaled=(
            decimal_to_scaled(str(old_tick), POLYMARKET_PRICE_SCALE, field="old_tick_size")
            if old_tick not in {None, ""}
            else None
        ),
        new_tick_size_scaled=decimal_to_scaled(
            str(message["new_tick_size"]),
            POLYMARKET_PRICE_SCALE,
            field="new_tick_size",
        ),
        connection_id=envelope.connection_id,
        source=Source.CLOB_MARKET_WS.value,
        raw_event_reference=raw_reference,
    )


def _quality(
    envelope: RawEnvelope,
    category: str,
    details: str,
    *,
    severity: str = "error",
    replay_eligible: bool = False,
) -> DataQualityEvent:
    normalized_severity = (
        severity if severity in {"info", "warning", "error", "critical"} else "error"
    )
    return DataQualityEvent(
        severity=normalized_severity,  # type: ignore[arg-type]
        category=category,
        condition_id=envelope.market_id,
        token_id=envelope.token_id,
        start_utc_ns=envelope.received_utc_ns,
        end_utc_ns=envelope.received_utc_ns,
        first_sequence=envelope.sequence,
        last_sequence=envelope.sequence,
        details_json=json.dumps({"details": details}, separators=(",", ":")),
        replay_eligible=replay_eligible,
    )


def _boundary_price(value: object) -> int | None:
    if value in {None, ""}:
        return None
    scaled = decimal_to_scaled(str(value), POLYMARKET_PRICE_SCALE, field="reported_best")
    # The feed uses 0/1 as explicit empty-side sentinels. Preserve them so the
    # reconstructor can clear an exhausted side rather than treating the field
    # as absent.
    return scaled if 0 <= scaled <= POLYMARKET_PRICE_SCALE else None


def _reported_top_values(
    raw_bid: object, raw_ask: object
) -> tuple[int | None, int | None, int | None, int | None]:
    reported_bid = _boundary_price(raw_bid)
    reported_ask = _boundary_price(raw_ask)
    bid = None if reported_bid == 0 else reported_bid
    ask = None if reported_ask == POLYMARKET_PRICE_SCALE else reported_ask
    spread = ask - bid if bid is not None and ask is not None else None
    midpoint = (ask + bid) // 2 if bid is not None and ask is not None else None
    return bid, ask, spread, midpoint


class EventParser:
    def __init__(self, market_for_token: Callable[[str], MarketRecord | None]) -> None:
        self.market_for_token = market_for_token
        self._tick_size_by_token: dict[str, int] = {}

    def set_tick_size(self, token_id: str, tick_size_scaled: int) -> None:
        self._tick_size_by_token[token_id] = tick_size_scaled

    def parse(self, envelope: RawEnvelope, raw_reference: str | None = None) -> ParsedEvents:
        if envelope.source == Source.CLOB_MARKET_WS:
            return self.parse_clob(envelope, raw_reference)
        if envelope.source == Source.RTDS:
            return self.parse_rtds(envelope, raw_reference)
        return ParsedEvents()

    def parse_clob(self, envelope: RawEnvelope, raw_reference: str | None = None) -> ParsedEvents:
        result = ParsedEvents()
        if envelope.payload_raw == "PONG":
            result.event_types.append("heartbeat_pong")
            return result
        try:
            payload = load_json_decimal(envelope.payload_raw)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            result.invalid_count += 1
            result.quality.append(_quality(envelope, "malformed_json", str(exc)))
            return result
        messages = payload if isinstance(payload, list) else [payload]
        for index, message in enumerate(messages):
            if not isinstance(message, dict):
                result.invalid_count += 1
                result.quality.append(
                    _quality(envelope, "malformed_message", "event is not an object")
                )
                continue
            event_type = str(message.get("event_type") or "unknown")
            result.event_types.append(event_type)
            try:
                self._parse_clob_message(
                    envelope,
                    message,
                    event_type,
                    index,
                    raw_reference or f"sequence:{envelope.sequence}",
                    result,
                )
            except (KeyError, ValueError, PrecisionError, TypeError) as exc:
                result.invalid_count += 1
                result.quality.append(_quality(envelope, "clob_parse_error", str(exc)))
        return result

    def _parse_clob_message(
        self,
        envelope: RawEnvelope,
        message: dict[str, Any],
        event_type: str,
        message_index: int,
        raw_reference: str,
        result: ParsedEvents,
    ) -> None:
        if event_type == "book":
            token_id = str(message["asset_id"])
            market = self.market_for_token(token_id)
            if market is None:
                raise ValueError(f"unknown token mapping: {token_id}")
            raw_tick_size = message.get("tick_size")
            tick_size_scaled = (
                decimal_to_scaled(str(raw_tick_size), POLYMARKET_PRICE_SCALE, field="tick_size")
                if raw_tick_size not in {None, ""}
                else self._tick_size_by_token.get(token_id, market.tick_size_scaled)
            )
            self.set_tick_size(token_id, tick_size_scaled)

            def parse_levels(side: str) -> tuple[BookLevel, ...]:
                raw_levels = message.get(side, [])
                if not isinstance(raw_levels, list):
                    raise ValueError(f"{side} is not a list")
                return tuple(
                    BookLevel(
                        price_scaled=decimal_to_scaled(
                            str(level["price"]), POLYMARKET_PRICE_SCALE, field="book_price"
                        ),
                        size_scaled=decimal_to_scaled(
                            str(level["size"]), SHARE_SIZE_SCALE, field="book_size"
                        ),
                    )
                    for level in raw_levels
                    if isinstance(level, dict)
                )

            snapshot = BookSnapshot(
                snapshot_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{envelope.run_id}:{envelope.sequence}:{message_index}:book",
                    )
                ),
                sequence=envelope.sequence,
                run_id=envelope.run_id,
                connection_id=envelope.connection_id,
                source=Source.CLOB_MARKET_WS.value,
                condition_id=str(message.get("market") or market.condition_id),
                token_id=token_id,
                outcome=market.token_outcomes[token_id],
                exchange_timestamp_ns=parse_timestamp_ns(message.get("timestamp")),
                received_utc_ns=envelope.received_utc_ns,
                received_monotonic_ns=envelope.received_monotonic_ns,
                book_hash=str(message.get("hash")) if message.get("hash") is not None else None,
                tick_size_scaled=tick_size_scaled,
                minimum_order_size_scaled=market.minimum_order_size_scaled,
                neg_risk=market.neg_risk,
                bids=parse_levels("bids"),
                asks=parse_levels("asks"),
                raw_event_reference=raw_reference,
            )
            result.snapshots.append(snapshot)
            best_bid = max((level.price_scaled for level in snapshot.bids), default=None)
            best_ask = min((level.price_scaled for level in snapshot.asks), default=None)
            result.top_of_book.append(
                TopOfBook(
                    condition_id=snapshot.condition_id,
                    token_id=snapshot.token_id,
                    sequence=envelope.sequence,
                    received_utc_ns=envelope.received_utc_ns,
                    best_bid_scaled=best_bid,
                    best_ask_scaled=best_ask,
                    spread_scaled=(
                        best_ask - best_bid
                        if best_bid is not None and best_ask is not None
                        else None
                    ),
                    midpoint_scaled=(
                        (best_ask + best_bid) // 2
                        if best_bid is not None and best_ask is not None
                        else None
                    ),
                    bid_size_scaled=(
                        next(
                            (
                                level.size_scaled
                                for level in snapshot.bids
                                if level.price_scaled == best_bid
                            ),
                            None,
                        )
                        if best_bid is not None
                        else None
                    ),
                    ask_size_scaled=(
                        next(
                            (
                                level.size_scaled
                                for level in snapshot.asks
                                if level.price_scaled == best_ask
                            ),
                            None,
                        )
                        if best_ask is not None
                        else None
                    ),
                )
            )
        elif event_type == "price_change":
            changes = message.get("price_changes", [])
            if not isinstance(changes, list):
                raise ValueError("price_changes is not a list")
            parent_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{envelope.run_id}:{envelope.sequence}:{message_index}:price_change",
                )
            )
            for change_index, change in enumerate(changes):
                if not isinstance(change, dict):
                    raise ValueError("price change is not an object")
                token_id = str(change["asset_id"])
                market = self.market_for_token(token_id)
                if market is None:
                    raise ValueError(f"unknown token mapping: {token_id}")
                side = str(change["side"]).upper()
                if side not in {"BUY", "SELL"}:
                    raise ValueError(f"invalid book side: {side}")
                update = BookLevelChange(
                    parent_event_id=parent_id,
                    change_index=change_index,
                    sequence=envelope.sequence,
                    condition_id=str(message.get("market") or market.condition_id),
                    token_id=token_id,
                    outcome=market.token_outcomes[token_id],
                    exchange_timestamp_ns=parse_timestamp_ns(message.get("timestamp")),
                    received_utc_ns=envelope.received_utc_ns,
                    received_monotonic_ns=envelope.received_monotonic_ns,
                    side=side,  # type: ignore[arg-type]
                    price_scaled=decimal_to_scaled(
                        str(change["price"]), POLYMARKET_PRICE_SCALE, field="update_price"
                    ),
                    new_size_scaled=decimal_to_scaled(
                        str(change["size"]), SHARE_SIZE_SCALE, field="reported_size"
                    ),
                    book_hash=(str(change.get("hash")) if change.get("hash") is not None else None),
                    reported_best_bid_scaled=_boundary_price(change.get("best_bid")),
                    reported_best_ask_scaled=_boundary_price(change.get("best_ask")),
                    connection_id=envelope.connection_id,
                    raw_event_reference=raw_reference,
                )
                result.updates.append(update)
                bid, ask, spread, midpoint = _reported_top_values(
                    change.get("best_bid"), change.get("best_ask")
                )
                if change.get("best_bid") is not None or change.get("best_ask") is not None:
                    result.top_of_book.append(
                        TopOfBook(
                            condition_id=update.condition_id,
                            token_id=token_id,
                            sequence=envelope.sequence,
                            received_utc_ns=envelope.received_utc_ns,
                            best_bid_scaled=bid,
                            best_ask_scaled=ask,
                            spread_scaled=spread,
                            midpoint_scaled=midpoint,
                            bid_size_scaled=None,
                            ask_size_scaled=None,
                        )
                    )
        elif event_type == "last_trade_price":
            token_id = str(message["asset_id"])
            market = self.market_for_token(token_id)
            if market is None:
                raise ValueError(f"unknown token mapping: {token_id}")
            raw_price = str(message["price"])
            raw_size = message.get("size")
            price_scaled = decimal_to_scaled(raw_price, POLYMARKET_PRICE_SCALE, field="trade_price")
            size_scaled = (
                decimal_to_scaled(str(raw_size), SHARE_SIZE_SCALE, field="trade_size")
                if raw_size not in {None, ""}
                else None
            )
            notional_scaled: int | None = None
            if raw_size not in {None, ""}:
                try:
                    notional_scaled = decimal_to_scaled(
                        Decimal(raw_price) * Decimal(str(raw_size)),
                        USDC_SCALE,
                        field="trade_notional",
                    )
                except PrecisionError as exc:
                    result.quality.append(
                        _quality(
                            envelope,
                            "notional_precision_exceeded",
                            str(exc),
                            severity="warning",
                            replay_eligible=True,
                        )
                    )
            result.trades.append(
                TradeEvent(
                    trade_event_id=str(
                        message.get("trade_id")
                        or uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"{envelope.run_id}:{envelope.sequence}:{message_index}:trade",
                        )
                    ),
                    sequence=envelope.sequence,
                    condition_id=str(message.get("market") or market.condition_id),
                    token_id=token_id,
                    outcome=market.token_outcomes[token_id],
                    exchange_timestamp_ns=parse_timestamp_ns(message.get("timestamp")),
                    received_utc_ns=envelope.received_utc_ns,
                    received_monotonic_ns=envelope.received_monotonic_ns,
                    price_scaled=price_scaled,
                    size_scaled=size_scaled,
                    notional_scaled=notional_scaled,
                    reported_side=str(message.get("side")) if message.get("side") else None,
                    fee_rate_bps_scaled=(
                        decimal_to_scaled(
                            str(message["fee_rate_bps"]), BPS_SCALE, field="fee_rate_bps"
                        )
                        if message.get("fee_rate_bps") not in {None, ""}
                        else None
                    ),
                    transaction_hash=(
                        str(message.get("transaction_hash"))
                        if message.get("transaction_hash")
                        else None
                    ),
                    trade_id_when_available=(
                        str(message.get("trade_id")) if message.get("trade_id") else None
                    ),
                    source=Source.CLOB_MARKET_WS.value,
                    raw_event_reference=raw_reference,
                )
            )
        elif event_type == "tick_size_change":
            tick_change = parse_tick_size_change(
                envelope,
                message,
                message_index,
                raw_reference,
            )
            self.set_tick_size(tick_change.token_id, tick_change.new_tick_size_scaled)
            result.tick_size_changes.append(tick_change)
        elif event_type == "market_resolved":
            result.resolutions.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "condition_id": str(message.get("market") or message.get("condition_id")),
                    "winning_token_id": str(message.get("winning_asset_id")),
                    "winning_outcome": str(message.get("winning_outcome")),
                    "exchange_timestamp_ns": parse_timestamp_ns(message.get("timestamp")),
                    "received_utc_ns": envelope.received_utc_ns,
                    "sequence": envelope.sequence,
                    "source": Source.CLOB_MARKET_WS.value,
                    "raw_event_reference": raw_reference,
                }
            )
        elif event_type == "best_bid_ask":
            token_id = str(message["asset_id"])
            market = self.market_for_token(token_id)
            if market is None:
                raise ValueError(f"unknown token mapping: {token_id}")
            bid, ask, spread, midpoint = _reported_top_values(
                message.get("best_bid"), message.get("best_ask")
            )
            result.top_of_book.append(
                TopOfBook(
                    condition_id=str(message.get("market") or market.condition_id),
                    token_id=token_id,
                    sequence=envelope.sequence,
                    received_utc_ns=envelope.received_utc_ns,
                    best_bid_scaled=bid,
                    best_ask_scaled=ask,
                    spread_scaled=(
                        decimal_to_scaled(
                            str(message["spread"]),
                            POLYMARKET_PRICE_SCALE,
                            field="reported_spread",
                        )
                        if message.get("spread") not in {None, ""}
                        else spread
                    ),
                    midpoint_scaled=midpoint,
                    bid_size_scaled=None,
                    ask_size_scaled=None,
                )
            )
        elif event_type == "new_market":
            return
        else:
            result.unknown_count += 1
            result.quality.append(
                _quality(
                    envelope,
                    "unknown_event_type",
                    event_type,
                    severity="warning",
                    replay_eligible=True,
                )
            )

    def parse_rtds(self, envelope: RawEnvelope, raw_reference: str | None = None) -> ParsedEvents:
        result = ParsedEvents()
        if envelope.event_type_hint == "empty_control_frame" or envelope.payload_raw == "":
            result.event_types.append("empty_control_frame")
            return result
        if envelope.payload_raw == "PONG":
            result.event_types.append("heartbeat_pong")
            return result
        try:
            message = load_json_decimal(envelope.payload_raw)
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            result.invalid_count += 1
            result.quality.append(_quality(envelope, "malformed_json", str(exc)))
            return result
        if not isinstance(message, dict):
            result.invalid_count += 1
            result.quality.append(
                _quality(envelope, "malformed_message", "RTDS event is not an object")
            )
            return result
        topic = str(message.get("topic") or "")
        event_type = str(message.get("type") or "")
        result.event_types.append(f"{topic}:{event_type}")
        payload = message.get("payload")
        if event_type == "subscribe" and topic in {
            "crypto_prices",
            "crypto_prices_chainlink",
            "prices.crypto.binance",
            "prices.crypto.chainlink",
        }:
            # Subscription history/backfill is preserved in raw storage. It is
            # intentionally not projected into the live-update table because
            # its list payload has different availability semantics.
            return result
        if (
            topic
            not in {
                "crypto_prices",
                "crypto_prices_chainlink",
                "prices.crypto.binance",
                "prices.crypto.chainlink",
            }
            or event_type != "update"
        ):
            result.unknown_count += 1
            result.quality.append(
                _quality(
                    envelope,
                    "unknown_event_type",
                    f"{topic}:{event_type}",
                    severity="warning",
                    replay_eligible=True,
                )
            )
            return result
        if not isinstance(payload, dict):
            result.invalid_count += 1
            result.quality.append(
                _quality(envelope, "malformed_rtds_payload", "payload is not object")
            )
            return result
        symbol = str(payload.get("symbol") or "")
        is_chainlink = "chainlink" in topic
        source = "CHAINLINK_BTCUSD" if is_chainlink else "BINANCE_BTCUSDT"
        try:
            price_scaled = decimal_to_scaled(
                str(payload["value"]), BTC_PRICE_SCALE, field="btc_price"
            )
        except (KeyError, ValueError, PrecisionError) as exc:
            result.invalid_count += 1
            result.quality.append(_quality(envelope, "invalid_btc_price", str(exc)))
            return result
        result.btc_prices.append(
            BtcPriceEvent(
                price_event_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{envelope.run_id}:{envelope.sequence}:{topic}:{symbol}",
                    )
                ),
                sequence=envelope.sequence,
                source=source,
                topic=topic,
                symbol=symbol,
                rtds_envelope_timestamp_ns=parse_timestamp_ns(message.get("timestamp")),
                underlying_source_timestamp_ns=parse_timestamp_ns(payload.get("timestamp")),
                received_utc_ns=envelope.received_utc_ns,
                received_monotonic_ns=envelope.received_monotonic_ns,
                price_scaled=price_scaled,
                connection_id=envelope.connection_id,
                raw_event_reference=raw_reference or f"sequence:{envelope.sequence}",
            )
        )
        return result
