"""End-to-end check of the episode backtesting path on a synthetic archive.

Builds a miniature storage root shaped exactly like the collector's normalized
output, then runs index -> tape -> fast simulator -> reference engine and requires
the two simulators to agree.  This is the offline guarantee that the fast path
stays faithful; `polymarket-bt verify-sim` performs the same comparison against
real recorded markets.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polymarket_bt.backtest.episodes import EpisodeIndexBuilder
from polymarket_bt.backtest.fastsim import ThresholdHoldSimulator
from polymarket_bt.backtest.realism import DEFAULT_FEE, ExecutionRealism
from polymarket_bt.backtest.tape import TAPE_VERSION, TapeBuilder, load_tape
from polymarket_bt.backtest.threshold_hold import ThresholdHoldParams
from polymarket_bt.backtest.verify import compare_episode
from polymarket_bt.config import LatencyConfig

MINUTE = 60_000_000_000
START = 1_785_600_000_000_000_000
END = START + 15 * MINUTE
CONDITION = "0xcondition"
UP = "111"
DOWN = "222"


def _write(root: Path, dataset: str, rows: dict[str, list[object]], partitioned: bool) -> None:
    directory = root / "normalized" / dataset
    if partitioned:
        directory = directory / "date=2026-08-01" / "hour=00"
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pydict(rows), directory / "part-0.parquet")


def _snapshot_rows(timestamp: int, sequence: int) -> tuple[dict[str, list[object]], ...]:
    header: dict[str, list[object]] = {
        "snapshot_id": [],
        "sequence": [],
        "run_id": [],
        "connection_id": [],
        "source": [],
        "condition_id": [],
        "token_id": [],
        "outcome": [],
        "exchange_timestamp_ns": [],
        "received_utc_ns": [],
        "received_monotonic_ns": [],
        "book_hash": [],
        "tick_size_scaled": [],
        "minimum_order_size_scaled": [],
    }
    levels: dict[str, list[object]] = {
        "snapshot_id": [],
        "condition_id": [],
        "token_id": [],
        "outcome": [],
        "side": [],
        "price_scaled": [],
        "size_scaled": [],
        "level_rank_from_best": [],
        "received_utc_ns": [],
    }
    # The opening snapshot must agree with the first recorded update, exactly as a
    # real subscription snapshot does; otherwise the first change crosses the book.
    for index, (token, outcome, bid, ask) in enumerate(
        ((UP, "UP", 500_000, 510_000), (DOWN, "DOWN", 490_000, 500_000))
    ):
        snapshot_id = f"snap-{token}"
        header["snapshot_id"].append(snapshot_id)
        header["sequence"].append(sequence + index)
        header["run_id"].append("run")
        header["connection_id"].append("conn")
        header["source"].append("clob_market_ws")
        header["condition_id"].append(CONDITION)
        header["token_id"].append(token)
        header["outcome"].append(outcome)
        header["exchange_timestamp_ns"].append(timestamp)
        header["received_utc_ns"].append(timestamp)
        header["received_monotonic_ns"].append(timestamp)
        header["book_hash"].append(None)
        header["tick_size_scaled"].append(10_000)
        header["minimum_order_size_scaled"].append(0)
        for rank, (side, price) in enumerate(((("BUY"), bid), (("SELL"), ask))):
            levels["snapshot_id"].append(snapshot_id)
            levels["condition_id"].append(CONDITION)
            levels["token_id"].append(token)
            levels["outcome"].append(outcome)
            levels["side"].append(side)
            levels["price_scaled"].append(price)
            levels["size_scaled"].append(500_000_000)
            levels["level_rank_from_best"].append(rank)
            levels["received_utc_ns"].append(timestamp)
    return header, levels


def _update(
    token: str,
    outcome: str,
    timestamp: int,
    sequence: int,
    side: str,
    price: int,
    size: int,
    bid: int | None,
    ask: int | None,
) -> dict[str, object]:
    return {
        "parent_event_id": f"evt-{sequence}",
        "change_index": 0,
        "sequence": sequence,
        "condition_id": CONDITION,
        "token_id": token,
        "outcome": outcome,
        "exchange_timestamp_ns": timestamp,
        "received_utc_ns": timestamp,
        "received_monotonic_ns": timestamp,
        "side": side,
        "price_scaled": price,
        "new_size_scaled": size,
        "book_hash": None,
        "reported_best_bid_scaled": bid,
        "reported_best_ask_scaled": ask,
        "connection_id": "conn",
    }


@pytest.fixture
def archive(tmp_path: Path) -> Path:
    root = tmp_path / "storage"
    _write(
        root,
        "markets",
        {
            "condition_id": [CONDITION],
            "market_slug": ["btc-updown-15m-test"],
            "question": ["Bitcoin Up or Down?"],
            "market_start_utc_ns": [START],
            "market_end_utc_ns": [END],
            "discovered_utc_ns": [START - MINUTE],
            "up_token_id": [UP],
            "down_token_id": [DOWN],
            "tick_size_scaled": [10_000],
            "minimum_order_size_scaled": [0],
            "winning_token_id": [None],
            "match_score": [1.0],
        },
        partitioned=False,
    )
    _write(
        root,
        "market_execution_metadata",
        {
            "condition_id": [CONDITION, CONDITION],
            "received_utc_ns": [START - 2 * MINUTE, START - 2 * MINUTE + 1],
            "sequence": [1, 2],
            "fee_rate": ["0.07", None],
            "fee_exponent": [1, None],
            "fee_taker_only": [True, None],
            "maker_base_fee_bps": [0, None],
            "taker_base_fee_bps": [0, None],
            "taker_order_delay_ms": [None, 250],
            "tick_size_scaled": [10_000, None],
            "minimum_order_size_scaled": [0, None],
            "source": ["clob_rest_market_info", "clob_rest_market"],
            "metadata_json": ['{"itode":true}', '{"seconds_delay":"0.25"}'],
            "raw_event_reference": ["raw:market-info", "raw:market-details"],
        },
        partitioned=True,
    )
    header, levels = _snapshot_rows(START - MINUTE, 1)
    _write(root, "book_snapshots", header, partitioned=True)
    _write(root, "book_snapshot_levels", levels, partitioned=True)

    updates: list[dict[str, object]] = []
    sequence = 100
    # The favoured side firms up through the entry window, then settles UP.
    path = [
        # Coverage must begin before the market opens, as a live subscription does.
        (START - MINUTE // 2, 500_000, 510_000),
        (START + 12 * MINUTE, 800_000, 810_000),
        (START + 13 * MINUTE, 820_000, 830_000),
        (START + 14 * MINUTE, 900_000, 910_000),
        (END - 1_000_000, 999_000, 0),
        # Settlement collapse observed just after the boundary, as recorded live.
        (END + 5_000_000_000, 999_000, 0),
    ]
    for timestamp, bid, ask in path:
        # A rising quote must move its ask before its bid and a falling quote the
        # reverse, or the intermediate state is crossed and the book — correctly —
        # rejects it. UP rises through this path; DOWN mirrors it downwards.
        if ask:
            sequence += 1
            updates.append(
                _update(UP, "UP", timestamp, sequence, "SELL", ask, 400_000_000, None, ask)
            )
        sequence += 1
        updates.append(
            _update(UP, "UP", timestamp + 1, sequence, "BUY", bid, 400_000_000, bid, None)
        )
        # The paired token moves inversely; both sides must be covered or the
        # episode is (correctly) excluded for incomplete data.
        down_bid = 1_000_000 - (ask or 1_000_000)
        down_ask = 1_000_000 - bid
        sequence += 1
        updates.append(
            _update(DOWN, "DOWN", timestamp, sequence, "BUY", down_bid, 400_000_000, down_bid, None)
        )
        sequence += 1
        updates.append(
            _update(
                DOWN, "DOWN", timestamp + 1, sequence, "SELL", down_ask, 400_000_000, None, down_ask
            )
        )
    _write(
        root,
        "book_updates",
        {key: [row[key] for row in updates] for key in updates[0]},
        partitioned=True,
    )
    _write(
        root,
        "top_of_book",
        {
            "condition_id": [CONDITION, CONDITION],
            "token_id": [UP, DOWN],
            "sequence": [999, 999],
            "received_utc_ns": [END - 1_000_000, END - 1_000_000],
            "best_bid_scaled": [999_000, 1_000],
            "best_ask_scaled": [None, 2_000],
            "spread_scaled": [None, 1_000],
            "midpoint_scaled": [None, 1_500],
            "bid_size_scaled": [None, None],
            "ask_size_scaled": [None, None],
        },
        partitioned=True,
    )
    _write(
        root,
        "btc_prices",
        {
            "source": ["CHAINLINK_BTCUSD", "CHAINLINK_BTCUSD"],
            "received_utc_ns": [START, END],
            "price_scaled": [60_000_000_000_000_000, 60_100_000_000_000_000],
        },
        partitioned=True,
    )
    _write(
        root,
        "market_resolutions",
        {
            "condition_id": [CONDITION],
            "winning_token_id": [UP],
            "winning_outcome": ["Up"],
            "exchange_timestamp_ns": [END + 6_000_000_000],
            "received_utc_ns": [END + 7_000_000_000],
            "sequence": [1_000],
            "source": ["clob_market_ws"],
            "raw_event_reference": ["raw:resolution"],
        },
        partitioned=True,
    )
    _write(
        root,
        "trades",
        {
            "received_utc_ns": [START + 12 * MINUTE],
            "token_id": [UP],
            "price_scaled": [810_000],
            "size_scaled": [100_000_000],
        },
        partitioned=True,
    )
    return root


def test_index_prefers_authoritative_resolution_and_captures_execution_metadata(
    archive: Path,
) -> None:
    with EpisodeIndexBuilder(archive) as builder:
        episodes = builder.build()
    assert len(episodes) == 1
    episode = episodes[0]
    assert episode.eligible
    assert episode.winner_outcome == "UP"
    assert episode.winner_source == "market_resolution"
    assert episode.resolution_received_utc_ns == END + 7_000_000_000
    assert episode.fee_rate == "0.07"
    assert episode.fee_exponent == 1
    assert episode.taker_order_delay_ms == 250
    # The independent observations are retained even though lifecycle truth wins.
    assert episode.reference_close_scaled > episode.reference_open_scaled


def test_fast_simulator_and_reference_engine_agree(archive: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    with EpisodeIndexBuilder(archive) as builder:
        episode = builder.build()[0]
    with TapeBuilder(archive, workspace) as tapes:
        stats = tapes.build(episode)
    assert stats.rows > 0
    loaded = load_tape(workspace, episode)
    assert loaded.depth is None
    loaded.ladder(0, "asks")
    assert loaded._ladders is not None and loaded._ladders._cached is not None
    loaded.release_depth_cache()
    assert loaded._ladders._cached is None
    assert len(loaded.event_kind) == stats.rows
    assert loaded.ladder(0, "asks")

    params = ThresholdHoldParams(
        entry_from_minute=12.0,
        entry_to_minute=14.0,
        entry_trigger_price_scaled=800_000,
        entry_limit_price_scaled=850_000,
        stop_loss_price_scaled=None,
        order_shares_scaled=100_000_000,
    )
    realism = ExecutionRealism(
        name="test",
        latency=LatencyConfig(model="constant", constant_ms=0),
        fee=DEFAULT_FEE,
    )
    fast = ThresholdHoldSimulator(params, realism, seed=5).run(loaded)
    assert fast.entered
    assert fast.exit_reason == "settlement"
    assert fast.settlement_payout_scaled == 100_000_000

    comparison = compare_episode(archive, workspace, episode, params, realism, latency_ms=0)
    assert comparison.entry_matches
    assert comparison.exit_matches
    assert comparison.agrees, comparison.to_row()


def test_stale_tape_metadata_forces_a_rebuild(archive: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    with EpisodeIndexBuilder(archive) as builder:
        episode = builder.build()[0]
    with TapeBuilder(archive, workspace) as tapes:
        tapes.build(episode)
        assert tapes.is_built(episode)
        metadata_path = tapes.metadata_path(episode)
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        assert metadata["tape_version"] == TAPE_VERSION
        metadata["tape_version"] = "episode-tape-v1"
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        assert not tapes.is_built(episode)
        tapes.build(episode)
        assert tapes.is_built(episode)


def test_tape_build_leaves_no_partial_publication(archive: Path, tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    with EpisodeIndexBuilder(archive) as builder:
        episode = builder.build()[0]
    with TapeBuilder(archive, workspace) as tapes:
        tapes.build(episode)

    assert not list((workspace / "tapes").glob("*.partial*"))
    loaded = load_tape(workspace, episode)
    assert loaded.depth is None
