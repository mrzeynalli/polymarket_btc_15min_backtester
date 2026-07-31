from __future__ import annotations

from pathlib import Path

from polymarket_bt.constants import Source
from polymarket_bt.ingestion.rtds_ws import RtdsWebSocket
from polymarket_bt.models.events import make_raw_envelope
from polymarket_bt.models.markets import MarketRecord
from polymarket_bt.normalization.parser import EventParser


def _envelope(path: Path, source: Source, sequence: int = 1):
    return make_raw_envelope(
        collector_version="test",
        run_id="run",
        connection_id="connection",
        sequence=sequence,
        source=source,
        stream="test",
        payload=path.read_text(),
        received_utc_ns=1_785_525_657_300_000_000,
        received_monotonic_ns=100,
    )


def test_clob_fixture_types(fixture_root: Path, market: MarketRecord) -> None:
    parser = EventParser(lambda token: market if token in market.token_outcomes else None)
    book = parser.parse_clob(
        _envelope(fixture_root / "clob" / "ws_book.json", Source.CLOB_MARKET_WS)
    )
    update = parser.parse_clob(
        _envelope(fixture_root / "clob" / "price_change.json", Source.CLOB_MARKET_WS, 2)
    )
    trade = parser.parse_clob(
        _envelope(fixture_root / "clob" / "last_trade_price.json", Source.CLOB_MARKET_WS, 3)
    )
    tick = parser.parse_clob(
        _envelope(fixture_root / "clob" / "tick_size_change.json", Source.CLOB_MARKET_WS, 4)
    )
    resolution = parser.parse_clob(
        _envelope(fixture_root / "clob" / "market_resolved.json", Source.CLOB_MARKET_WS, 5)
    )
    assert len(book.snapshots) == 1
    assert len(book.top_of_book) == 1
    assert update.updates[0].new_size_scaled == 40_000_000
    assert len(update.top_of_book) == 1
    assert trade.trades[0].size_scaled == 5_000_000
    assert tick.tick_size_changes[0][1] == 1_000
    assert resolution.resolutions[0]["winning_outcome"] == "Up"


def test_rtds_sources_and_timestamps(fixture_root: Path, market: MarketRecord) -> None:
    parser = EventParser(lambda _token: market)
    binance = parser.parse_rtds(
        _envelope(fixture_root / "rtds" / "binance.json", Source.RTDS)
    ).btc_prices[0]
    chainlink = parser.parse_rtds(
        _envelope(fixture_root / "rtds" / "chainlink.json", Source.RTDS, 2)
    ).btc_prices[0]
    assert binance.source == "BINANCE_BTCUSDT"
    assert chainlink.source == "CHAINLINK_BTCUSD"
    assert binance.price_scaled == 67_234_500_000_000_000
    assert binance.underlying_source_timestamp_ns != binance.received_utc_ns


def test_custom_top_of_book_fixture(fixture_root: Path, market: MarketRecord) -> None:
    parser = EventParser(lambda token: market if token in market.token_outcomes else None)
    parsed = parser.parse_clob(
        _envelope(fixture_root / "clob" / "best_bid_ask.json", Source.CLOB_MARKET_WS)
    )
    assert parsed.unknown_count == 0
    assert parsed.top_of_book[0].best_bid_scaled == 360_000
    assert parsed.top_of_book[0].best_ask_scaled == 380_000


def test_chainlink_live_precision_is_lossless(fixture_root: Path, market: MarketRecord) -> None:
    parser = EventParser(lambda _token: market)
    event = parser.parse_rtds(
        _envelope(fixture_root / "rtds" / "chainlink_high_precision.json", Source.RTDS)
    )
    assert event.invalid_count == 0
    assert event.btc_prices[0].price_scaled == 62_938_353_996_889_270


def test_rtds_binance_subscription_uses_live_json_filter(collector_config) -> None:
    websocket = RtdsWebSocket(
        collector_config,
        None,  # type: ignore[arg-type]
        None,  # type: ignore[arg-type]
        lambda: 1,
        "run",
    )
    subscriptions = websocket.subscription()["subscriptions"]
    assert isinstance(subscriptions, list)
    assert subscriptions[0]["filters"] == '{"symbol":"BTCUSDT"}'


def test_rtds_subscribe_history_is_known(fixture_root: Path, market: MarketRecord) -> None:
    parser = EventParser(lambda _token: market)
    envelope = make_raw_envelope(
        collector_version="test",
        run_id="run",
        connection_id="c",
        sequence=1,
        source=Source.RTDS,
        stream="crypto_prices",
        payload=(
            '{"topic":"crypto_prices","type":"subscribe","payload":{"symbol":"btcusdt","data":[]}}'
        ),
    )
    parsed = parser.parse_rtds(envelope)
    assert parsed.invalid_count == 0
    assert parsed.unknown_count == 0


def test_rtds_empty_control_frame_is_known(market: MarketRecord) -> None:
    parser = EventParser(lambda _token: market)
    envelope = make_raw_envelope(
        collector_version="test",
        run_id="run",
        connection_id="c",
        sequence=1,
        source=Source.RTDS,
        stream="crypto_prices",
        payload="",
        event_type_hint="empty_control_frame",
    )
    parsed = parser.parse_rtds(envelope)
    assert parsed.invalid_count == 0
    assert parsed.unknown_count == 0


def test_unknown_and_malformed_are_preserved_as_quality(market: MarketRecord) -> None:
    parser = EventParser(lambda _token: market)
    unknown = make_raw_envelope(
        collector_version="test",
        run_id="run",
        connection_id="c",
        sequence=1,
        source=Source.CLOB_MARKET_WS,
        stream="market",
        payload='{"event_type":"future_event"}',
    )
    malformed = unknown.model_copy(update={"sequence": 2, "payload_raw": "{"})
    assert parser.parse(unknown).unknown_count == 1
    assert parser.parse(malformed).invalid_count == 1
