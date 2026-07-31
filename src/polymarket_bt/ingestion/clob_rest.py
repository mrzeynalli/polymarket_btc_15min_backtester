from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

import httpx
import orjson

from polymarket_bt.clock import (
    decimal_to_scaled,
    monotonic_now_ns,
    parse_timestamp_ns,
    utc_now_ns,
)
from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE, Source
from polymarket_bt.models.books import BookLevel, BookSnapshot
from polymarket_bt.models.events import RawEnvelope, make_raw_envelope
from polymarket_bt.models.markets import MarketRecord
from polymarket_bt.storage.raw_writer import RawArchive


@dataclass(frozen=True, slots=True)
class RestResponse:
    request_id: str
    method: str
    url: str
    request_start_utc_ns: int
    request_start_monotonic_ns: int
    response_received_utc_ns: int
    response_received_monotonic_ns: int
    http_status: int
    headers: dict[str, str]
    retry_count: int
    raw_text: str

    @property
    def duration_ns(self) -> int:
        return self.response_received_monotonic_ns - self.request_start_monotonic_ns


class ClobRestClient:
    def __init__(
        self,
        config: CollectorConfig,
        raw_archive: RawArchive,
        next_sequence: Callable[[], int],
        run_id: str,
    ) -> None:
        self.config = config
        self.raw_archive = raw_archive
        self.next_sequence = next_sequence
        self.run_id = run_id
        self.client = httpx.AsyncClient(
            base_url=config.clob.rest_base_url.rstrip("/"),
            timeout=config.clob.request_timeout_seconds,
            headers={"User-Agent": "polymarket-btc-backtester/0.1 public-data-only"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: object | None = None,
        retries: int = 3,
    ) -> RestResponse:
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            start_utc = utc_now_ns()
            start_mono = monotonic_now_ns()
            try:
                response = await self.client.request(method, path, params=params, json=json_body)
                received_utc = utc_now_ns()
                received_mono = monotonic_now_ns()
                if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
                    retry_after = response.headers.get("retry-after")
                    delay = float(retry_after) if retry_after else min(0.5 * (2**attempt), 4)
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                headers = {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower()
                    in {
                        "date",
                        "content-type",
                        "content-length",
                        "cf-ray",
                        "x-ratelimit-limit",
                        "x-ratelimit-remaining",
                        "retry-after",
                    }
                }
                return RestResponse(
                    request_id=str(uuid.uuid4()),
                    method=method,
                    url=str(response.url),
                    request_start_utc_ns=start_utc,
                    request_start_monotonic_ns=start_mono,
                    response_received_utc_ns=received_utc,
                    response_received_monotonic_ns=received_mono,
                    http_status=response.status_code,
                    headers=headers,
                    retry_count=attempt,
                    raw_text=response.text,
                )
            except httpx.HTTPStatusError as exc:
                last_error = exc
                if exc.response.status_code not in {429, 500, 502, 503, 504}:
                    break
                if attempt < retries:
                    await asyncio.sleep(min(0.5 * (2**attempt), 4))
            except (httpx.RequestError, TimeoutError) as exc:
                last_error = exc
                if attempt < retries:
                    await asyncio.sleep(min(0.5 * (2**attempt), 4))
        assert last_error is not None
        raise last_error

    def _archive_response(
        self,
        response: RestResponse,
        *,
        stream: str,
        event_type: str,
        market_id: str | None,
        token_id: str | None,
    ) -> RawEnvelope:
        envelope = make_raw_envelope(
            collector_version=self.config.collector_version,
            run_id=self.run_id,
            connection_id="clob-rest",
            sequence=self.next_sequence(),
            source=Source.CLOB_REST,
            stream=stream,
            payload=response.raw_text,
            event_type_hint=event_type,
            market_id=market_id,
            token_id=token_id,
            received_utc_ns=response.response_received_utc_ns,
            received_monotonic_ns=response.response_received_monotonic_ns,
        )
        self.raw_archive.enqueue(envelope)
        request_payload = json.dumps(
            {
                "event_type": "rest_request",
                "request_id": response.request_id,
                "method": response.method,
                "url": response.url,
                "request_start_utc_ns": response.request_start_utc_ns,
                "request_start_monotonic_ns": response.request_start_monotonic_ns,
                "response_received_utc_ns": response.response_received_utc_ns,
                "response_received_monotonic_ns": response.response_received_monotonic_ns,
                "http_status": response.http_status,
                "response_headers": response.headers,
                "duration_ns": response.duration_ns,
                "retry_count": response.retry_count,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        self.raw_archive.enqueue(
            make_raw_envelope(
                collector_version=self.config.collector_version,
                run_id=self.run_id,
                connection_id="clob-rest",
                sequence=self.next_sequence(),
                source=Source.CLOB_REST,
                stream="operations",
                payload=request_payload,
                event_type_hint="rest_request",
                market_id=market_id,
                token_id=token_id,
            )
        )
        return envelope

    async def fetch_book(self, market: MarketRecord, token_id: str, outcome: str) -> BookSnapshot:
        response = await self._request("GET", "/book", params={"token_id": token_id})
        envelope = self._archive_response(
            response,
            stream="book",
            event_type="book",
            market_id=market.condition_id,
            token_id=token_id,
        )
        payload = orjson.loads(response.raw_text)
        if not isinstance(payload, dict):
            raise ValueError("CLOB /book response was not an object")
        if str(payload.get("asset_id")) != token_id:
            raise ValueError("CLOB /book returned a different token")
        if str(payload.get("market")) != market.condition_id:
            raise ValueError("CLOB /book returned a different condition")

        def levels(name: str) -> tuple[BookLevel, ...]:
            raw_levels = payload.get(name, [])
            if not isinstance(raw_levels, list):
                raise ValueError(f"CLOB /book {name} was not a list")
            parsed = [
                BookLevel(
                    price_scaled=decimal_to_scaled(
                        str(item["price"]), POLYMARKET_PRICE_SCALE, field="price"
                    ),
                    size_scaled=decimal_to_scaled(
                        str(item["size"]), SHARE_SIZE_SCALE, field="size"
                    ),
                )
                for item in raw_levels
                if isinstance(item, dict)
            ]
            return tuple(parsed)

        last_trade = payload.get("last_trade_price")
        return BookSnapshot(
            snapshot_id=str(uuid.uuid4()),
            sequence=envelope.sequence,
            run_id=self.run_id,
            connection_id="clob-rest",
            source=Source.CLOB_REST.value,
            condition_id=market.condition_id,
            token_id=token_id,
            outcome=outcome,
            exchange_timestamp_ns=parse_timestamp_ns(payload.get("timestamp")),
            received_utc_ns=response.response_received_utc_ns,
            received_monotonic_ns=response.response_received_monotonic_ns,
            book_hash=str(payload.get("hash")) if payload.get("hash") is not None else None,
            tick_size_scaled=decimal_to_scaled(
                str(
                    payload.get(
                        "tick_size", Decimal(market.tick_size_scaled) / POLYMARKET_PRICE_SCALE
                    )
                ),
                POLYMARKET_PRICE_SCALE,
                field="tick_size",
            ),
            minimum_order_size_scaled=decimal_to_scaled(
                str(
                    payload.get(
                        "min_order_size",
                        Decimal(market.minimum_order_size_scaled) / SHARE_SIZE_SCALE,
                    )
                ),
                SHARE_SIZE_SCALE,
                field="minimum_order_size",
            ),
            last_trade_price_scaled=(
                decimal_to_scaled(str(last_trade), POLYMARKET_PRICE_SCALE, field="last_trade_price")
                if last_trade not in {None, ""}
                else None
            ),
            neg_risk=bool(payload.get("neg_risk", market.neg_risk)),
            bids=levels("bids"),
            asks=levels("asks"),
            raw_event_reference=f"sequence:{envelope.sequence}",
        )

    async def fetch_books(self, token_ids: list[str]) -> RestResponse:
        response = await self._request(
            "POST", "/books", json_body=[{"token_id": token_id} for token_id in token_ids]
        )
        self._archive_response(
            response,
            stream="books",
            event_type="books",
            market_id=None,
            token_id=None,
        )
        return response

    async def fetch_prices_history(
        self, token_id: str, *, start_ts: int, end_ts: int, fidelity: int = 1
    ) -> RestResponse:
        response = await self._request(
            "GET",
            "/prices-history",
            params={
                "market": token_id,
                "startTs": str(start_ts),
                "endTs": str(end_ts),
                "fidelity": str(fidelity),
            },
        )
        self._archive_response(
            response,
            stream="prices_history",
            event_type="prices_history",
            market_id=None,
            token_id=token_id,
        )
        return response
