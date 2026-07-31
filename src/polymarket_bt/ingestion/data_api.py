from __future__ import annotations

from collections.abc import Callable

import httpx

from polymarket_bt.clock import monotonic_now_ns, utc_now_ns
from polymarket_bt.config import CollectorConfig
from polymarket_bt.constants import Source
from polymarket_bt.models.events import make_raw_envelope
from polymarket_bt.storage.raw_writer import RawArchive


class DataApiClient:
    def __init__(
        self,
        config: CollectorConfig,
        raw_archive: RawArchive,
        next_sequence: Callable[[], int],
        run_id: str,
        *,
        base_url: str = "https://data-api.polymarket.com",
    ) -> None:
        self.config = config
        self.raw_archive = raw_archive
        self.next_sequence = next_sequence
        self.run_id = run_id
        self.client = httpx.AsyncClient(base_url=base_url, timeout=15)

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch_market_trades(self, condition_id: str, *, limit: int = 10_000) -> str:
        started_utc = utc_now_ns()
        started_mono = monotonic_now_ns()
        response = await self.client.get(
            "/trades",
            params={"market": condition_id, "limit": limit, "offset": 0, "takerOnly": "false"},
        )
        received_utc = utc_now_ns()
        received_mono = monotonic_now_ns()
        response.raise_for_status()
        self.raw_archive.enqueue(
            make_raw_envelope(
                collector_version=self.config.collector_version,
                run_id=self.run_id,
                connection_id="data-api-rest",
                sequence=self.next_sequence(),
                source=Source.DATA_API,
                stream="trades",
                payload=response.text,
                event_type_hint="trades",
                market_id=condition_id,
                received_utc_ns=received_utc,
                received_monotonic_ns=received_mono,
                source_timestamp_raw=str(started_utc),
            )
        )
        _ = started_mono
        return response.text
