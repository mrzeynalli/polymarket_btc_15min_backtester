from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import orjson

from polymarket_bt.clock import monotonic_now_ns, utc_now_ns
from polymarket_bt.config import DiscoveryConfig


@dataclass(frozen=True, slots=True)
class GammaResponse:
    url: str
    request_started_utc_ns: int
    request_started_monotonic_ns: int
    received_utc_ns: int
    received_monotonic_ns: int
    status_code: int
    retry_count: int
    headers: dict[str, str]
    raw_text: str

    @property
    def duration_ns(self) -> int:
        return self.received_monotonic_ns - self.request_started_monotonic_ns

    def json(self) -> Any:
        return orjson.loads(self.raw_text)


class GammaClient:
    def __init__(
        self,
        config: DiscoveryConfig,
        *,
        base_url: str = "https://gamma-api.polymarket.com",
        timeout_seconds: float = 10,
    ) -> None:
        self.config = config
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout_seconds,
            headers={"User-Agent": "polymarket-btc-backtester/0.1 public-data-only"},
        )

    async def close(self) -> None:
        await self.client.aclose()

    def candidate_slugs(self, now_ns: int | None = None) -> list[str]:
        now_seconds = (now_ns if now_ns is not None else utc_now_ns()) // 1_000_000_000
        interval = self.config.duration_minutes * 60
        boundary = now_seconds - (now_seconds % interval)
        prefix = self.config.asset.lower()
        return [
            f"{prefix}-updown-{self.config.duration_minutes}m-{boundary + offset * interval}"
            for offset in range(
                -self.config.candidate_intervals_before,
                self.config.candidate_intervals_after + 1,
            )
        ]

    async def _get(
        self,
        path: str,
        *,
        params: dict[str, str | int | bool] | None = None,
        retries: int = 3,
    ) -> GammaResponse:
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            start_utc = utc_now_ns()
            start_mono = monotonic_now_ns()
            try:
                response = await self.client.get(path, params=params)
                received_utc = utc_now_ns()
                received_mono = monotonic_now_ns()
                if response.status_code >= 500 and attempt < retries:
                    await asyncio.sleep(min(0.5 * (2**attempt), 4))
                    continue
                response.raise_for_status()
                diagnostic_headers = {
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
                return GammaResponse(
                    url=str(response.url),
                    request_started_utc_ns=start_utc,
                    request_started_monotonic_ns=start_mono,
                    received_utc_ns=received_utc,
                    received_monotonic_ns=received_mono,
                    status_code=response.status_code,
                    retry_count=attempt,
                    headers=diagnostic_headers,
                    raw_text=response.text,
                )
            except (httpx.HTTPError, TimeoutError) as exc:
                last_error = exc
                if attempt < retries:
                    await asyncio.sleep(min(0.5 * (2**attempt), 4))
        assert last_error is not None
        raise last_error

    async def discover_responses(self, now_ns: int | None = None) -> list[GammaResponse]:
        """Use broad tag discovery plus interval candidates; the matcher makes the decision."""
        broad_task = self._get(
            "/events",
            params={
                "active": "true",
                "closed": "false",
                "limit": 100,
                "tag_slug": "bitcoin",
                "order": "createdAt",
                "ascending": "false",
            },
        )
        slug_tasks = [
            self._get(f"/events/slug/{slug}", retries=1) for slug in self.candidate_slugs(now_ns)
        ]
        results = await asyncio.gather(broad_task, *slug_tasks, return_exceptions=True)
        responses: list[GammaResponse] = []
        for result in results:
            if isinstance(result, GammaResponse):
                responses.append(result)
            elif isinstance(result, httpx.HTTPStatusError) and result.response.status_code == 404:
                continue
        return responses

    @staticmethod
    def extract_events(
        responses: list[GammaResponse],
    ) -> list[tuple[dict[str, Any], GammaResponse]]:
        unique: dict[str, tuple[dict[str, Any], GammaResponse]] = {}
        for response in responses:
            payload = response.json()
            events = payload if isinstance(payload, list) else [payload]
            for event in events:
                if isinstance(event, dict) and event.get("id") is not None:
                    unique[str(event["id"])] = (event, response)
        return list(unique.values())

    def seconds_until_next_poll(self, now_ns: int | None = None) -> float:
        now_seconds = (now_ns if now_ns is not None else utc_now_ns()) / 1_000_000_000
        interval = self.config.duration_minutes * 60
        distance = now_seconds % interval
        before = interval - distance
        near_boundary = (
            before <= self.config.boundary_window_before_seconds
            or distance <= self.config.boundary_window_after_seconds
        )
        return (
            self.config.boundary_poll_seconds if near_boundary else self.config.normal_poll_seconds
        )

    @staticmethod
    def current_utc() -> datetime:
        return datetime.now(tz=UTC)
