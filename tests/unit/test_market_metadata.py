from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from polymarket_bt.config import CollectorConfig
from polymarket_bt.discovery.btc_market_matcher import Btc15mMarketMatcher
from polymarket_bt.ingestion.clob_rest import ClobRestClient, RestResponse
from polymarket_bt.models.events import RawEnvelope
from polymarket_bt.models.markets import MarketExecutionMetadata


def test_clob_market_info_projects_fee_curve_without_inventing_delay(
    fixture_root: Path,
) -> None:
    payload = json.loads((fixture_root / "clob" / "market_info.json").read_text())
    metadata = MarketExecutionMetadata.from_clob_payload(
        condition_id="0xcondition",
        payload=payload,
        received_utc_ns=123,
        sequence=7,
        raw_event_reference="raw:1",
    )

    assert metadata.fee_rate == "0.07"
    assert metadata.fee_exponent == 1
    assert metadata.fee_taker_only is True
    assert metadata.taker_order_delay_ms is None
    assert metadata.tick_size_scaled == 10_000
    assert metadata.minimum_order_size_scaled == 5_000_000
    assert json.loads(metadata.metadata_json)["itode"] is True


def test_market_details_projects_exact_seconds_delay(fixture_root: Path) -> None:
    payload = json.loads((fixture_root / "clob" / "market_details.json").read_text())
    metadata = MarketExecutionMetadata.from_clob_payload(
        condition_id="0xcondition",
        payload=payload,
        received_utc_ns=123,
        sequence=7,
        raw_event_reference=None,
        source="clob_rest_market",
    )
    assert metadata.taker_order_delay_ms == 0

    payload["seconds_delay"] = "0.25"
    metadata = MarketExecutionMetadata.from_clob_payload(
        condition_id="0xcondition",
        payload=payload,
        received_utc_ns=124,
        sequence=8,
        raw_event_reference=None,
        source="clob_rest_market",
    )
    assert metadata.taker_order_delay_ms == 250


def test_boolean_delay_flag_never_invents_a_duration() -> None:
    enabled = MarketExecutionMetadata.from_clob_payload(
        condition_id="0xcondition",
        payload={"itode": True},
        received_utc_ns=123,
        sequence=7,
        raw_event_reference=None,
    )
    disabled = MarketExecutionMetadata.from_clob_payload(
        condition_id="0xcondition",
        payload={"itode": False},
        received_utc_ns=124,
        sequence=8,
        raw_event_reference=None,
    )
    assert enabled.taker_order_delay_ms is None
    assert disabled.taker_order_delay_ms == 0


def test_omitted_clob_taker_delay_is_unknown() -> None:
    metadata = MarketExecutionMetadata.from_clob_payload(
        condition_id="0xcondition",
        payload={"fd": {"r": "0.02", "e": 2, "to": True}},
        received_utc_ns=123,
        sequence=7,
        raw_event_reference=None,
    )
    assert metadata.taker_order_delay_ms is None


class _Archive:
    def __init__(self) -> None:
        self.events: list[RawEnvelope] = []

    def enqueue(self, envelope: RawEnvelope) -> bool:
        self.events.append(envelope)
        return True


async def test_collector_fetches_and_archives_both_market_metadata_endpoints(
    collector_config: CollectorConfig,
    fixture_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = json.loads((fixture_root / "gamma" / "current_btc_15m.json").read_text())
    decision = Btc15mMarketMatcher(collector_config.discovery).match_event(event)[0]
    assert decision.market is not None
    market = decision.market
    bodies = {
        f"/clob-markets/{market.condition_id}": (
            fixture_root / "clob" / "market_info.json"
        ).read_text(),
        f"/markets/{market.condition_id}": (
            fixture_root / "clob" / "market_details.json"
        ).read_text(),
    }
    requested: list[str] = []

    async def request(method: str, path: str, **_: Any) -> RestResponse:
        requested.append(path)
        return RestResponse(
            request_id=f"request-{len(requested)}",
            method=method,
            url=f"https://clob.polymarket.com{path}",
            request_start_utc_ns=100,
            request_start_monotonic_ns=100,
            response_received_utc_ns=100 + len(requested),
            response_received_monotonic_ns=110,
            http_status=200,
            headers={},
            retry_count=0,
            raw_text=bodies[path],
        )

    archive = _Archive()
    sequence = iter(range(1, 20))
    client = ClobRestClient(
        collector_config,
        archive,  # type: ignore[arg-type]
        lambda: next(sequence),
        "test-run",
    )
    monkeypatch.setattr(client, "_request", request)
    try:
        metadata = await client.fetch_market_info(market)
    finally:
        await client.close()

    assert requested == [
        f"/clob-markets/{market.condition_id}",
        f"/markets/{market.condition_id}",
    ]
    assert metadata.fee_rate == "0.07"
    assert metadata.taker_order_delay_ms == 0
    combined = json.loads(metadata.metadata_json)
    assert combined["clob_market_info"]["itode"] is True
    assert combined["market"]["seconds_delay"] == 0
    assert {event.event_type_hint for event in archive.events} >= {
        "clob_market_info",
        "clob_market_details",
    }
