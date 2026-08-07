"""Per-episode order-book tape: the input a realistic execution model needs.

The audited event engine reconstructs books from snapshots and absolute-size
updates while replaying every event.  That is the correct reference, but it is far
too slow to sweep hundreds of parameter combinations over months of data.  A *tape*
is that same reconstruction, computed once per episode and stored as the sequence
of order-book states a live process would have held.

The tape is not a summary.  Each row contains the complete reconstructed ladder
by default (or an explicitly requested finite depth in a test/experiment) at the
instant a recorded event changed it, so a simulated order can walk real levels
rather than assume a price.  Scalar top-of-book columns sit alongside the ladders
so a strategy scan is a cheap linear pass.

Reconstruction uses the same `BookReconstructor` and `OrderBook` semantics as the
engine, including the deterministic ordering rules, so a tape cannot silently
disagree with the reference simulator about what the book was.

Invalid states are preserved, not repaired.  When a level change contradicts the
source's reported top the book is marked invalid and stays invalid until the next
snapshot resynchronises it; those spans are recorded as uncertainty intervals and
the simulator refuses to trade inside them.
"""

from __future__ import annotations

import array
import json
import os
import uuid
from bisect import bisect_right
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, overload

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from polymarket_bt.backtest.episodes import Episode, EpisodeWindow, configure_duckdb
from polymarket_bt.models.books import (
    BookLevel,
    BookLevelChange,
    BookSnapshot,
    TickSizeChange,
)
from polymarket_bt.orderbook.reconstructor import BookReconstructor
from polymarket_bt.orderbook.state import InvalidBookState

TAPE_VERSION = "episode-tape-v2"
# Production tapes retain the complete reconstructed ladder.  A finite depth is
# still useful in small tests and explicit experiments, but it is never the
# default: silently treating a top-N cache as the whole executable book makes a
# large order look liquidity-constrained for an implementation detail rather than
# for anything that happened at the venue.
DEFAULT_DEPTH: int | None = None

# Event kinds, in the order they must be applied when timestamps tie.  These match
# the reference engine's source priorities (snapshot 10, tick 15, update 20).
KIND_SNAPSHOT = 0
KIND_TICK = 1
KIND_UPDATE = 2

# The side changed by an event.  Polymarket calls resting bids BUY and resting
# asks SELL; the tape uses book-side names so execution code need not remember
# that an aggressive BUY consumes the opposite (ASK) side.
BOOK_SIDE_NONE = -1
BOOK_SIDE_BIDS = 0
BOOK_SIDE_ASKS = 1

_SIDE_UP = 0
_SIDE_DOWN = 1

# Ladders are read back a row group at a time rather than whole-file, so a run that
# executes at a handful of instants pays for a few megabytes instead of ~90.
TAPE_ROW_GROUP_SIZE = 8192
LADDER_COLUMNS = (
    "bid_prices_scaled",
    "bid_sizes_scaled",
    "ask_prices_scaled",
    "ask_sizes_scaled",
    "bid_depth_beyond_scaled",
    "ask_depth_beyond_scaled",
)
SCALAR_COLUMNS = (
    "received_utc_ns",
    "token_index",
    "event_kind",
    "event_side",
    "event_price_scaled",
    "book_valid",
    "best_bid_scaled",
    "best_ask_scaled",
)
# Sentinel for an absent quote. Prices are strictly positive, so it cannot collide
# with a real one, and it keeps the column a flat typed buffer instead of a list of
# boxed Python objects.
_NO_QUOTE = -1
_TYPE_CODES = {pa.int64(): "q", pa.int8(): "b"}


@dataclass(frozen=True, slots=True)
class TapeStats:
    condition_id: str
    rows: int
    trades: int
    invalid_events: int
    uncertainty_intervals: int
    first_utc_ns: int | None
    last_utc_ns: int | None


def tape_schema(depth: int | None) -> pa.Schema:
    ladder = pa.list_(pa.int64()) if depth is None else pa.list_(pa.int64(), depth)
    return pa.schema(
        [
            pa.field("received_utc_ns", pa.int64(), nullable=False),
            pa.field("sequence", pa.int64(), nullable=False),
            pa.field("token_index", pa.int8(), nullable=False),
            pa.field("event_kind", pa.int8(), nullable=False),
            pa.field("event_side", pa.int8(), nullable=False),
            pa.field("event_price_scaled", pa.int64(), nullable=True),
            pa.field("book_valid", pa.bool_(), nullable=False),
            pa.field("best_bid_scaled", pa.int64(), nullable=True),
            pa.field("best_ask_scaled", pa.int64(), nullable=True),
            pa.field("bid_size_scaled", pa.int64(), nullable=True),
            pa.field("ask_size_scaled", pa.int64(), nullable=True),
            pa.field("tick_size_scaled", pa.int64(), nullable=False),
            pa.field("bid_prices_scaled", ladder, nullable=False),
            pa.field("bid_sizes_scaled", ladder, nullable=False),
            pa.field("ask_prices_scaled", ladder, nullable=False),
            pa.field("ask_sizes_scaled", ladder, nullable=False),
            pa.field("bid_depth_beyond_scaled", pa.int64(), nullable=False),
            pa.field("ask_depth_beyond_scaled", pa.int64(), nullable=False),
        ]
    )


TRADE_SCHEMA = pa.schema(
    [
        pa.field("received_utc_ns", pa.int64(), nullable=False),
        pa.field("token_index", pa.int8(), nullable=False),
        pa.field("price_scaled", pa.int64(), nullable=False),
        pa.field("size_scaled", pa.int64(), nullable=True),
    ]
)


def _dataset(root: Path, name: str, partitioned: bool = True) -> str:
    pattern = "**/*.parquet" if partitioned else "*.parquet"
    return f"read_parquet('{root / 'normalized' / name / pattern}', hive_partitioning=false)"


class TapeBuilder:
    """Reconstructs and caches one tape per episode."""

    def __init__(
        self,
        storage_root: Path,
        workspace: Path,
        *,
        depth: int | None = DEFAULT_DEPTH,
        window: EpisodeWindow | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self.storage_root = storage_root
        self.workspace = workspace
        self.depth = depth
        self.window = window or EpisodeWindow()
        self.connection = connection or duckdb.connect()
        self.tape_directory = workspace / "tapes"

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> TapeBuilder:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def tape_path(self, episode: Episode) -> Path:
        return self.tape_directory / f"{episode.condition_id}.parquet"

    def trades_path(self, episode: Episode) -> Path:
        return self.tape_directory / f"{episode.condition_id}.trades.parquet"

    def metadata_path(self, episode: Episode) -> Path:
        return self.tape_directory / f"{episode.condition_id}.meta.json"

    def is_built(self, episode: Episode) -> bool:
        metadata_path = self.metadata_path(episode)
        if not (
            metadata_path.exists()
            and self.tape_path(episode).exists()
            and self.trades_path(episode).exists()
        ):
            return False
        try:
            payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return False
        return (
            payload.get("tape_version") == TAPE_VERSION
            and payload.get("depth") == self.depth
            and payload.get("condition_id") == episode.condition_id
        )

    def build(self, episode: Episode, *, overwrite: bool = False) -> TapeStats:
        if self.is_built(episode) and not overwrite:
            payload = json.loads(self.metadata_path(episode).read_text(encoding="utf-8"))
            return TapeStats(**payload["stats"])
        configure_duckdb(self.connection, self.workspace / ".duckdb-tmp")
        self.tape_directory.mkdir(parents=True, exist_ok=True)
        start_ns = episode.start_utc_ns - self.window.pre_start_ns
        end_ns = episode.end_utc_ns + self.window.post_end_ns
        tokens = {episode.up_token_id: _SIDE_UP, episode.down_token_id: _SIDE_DOWN}

        reconstructor = BookReconstructor()
        rows: dict[str, list[Any]] = {field.name: [] for field in tape_schema(self.depth)}
        invalid_events = 0
        uncertainty: list[dict[str, Any]] = []
        pending_uncertain: dict[str, dict[str, Any]] = {}

        for event in self.iter_book_events(episode, start_ns, end_ns):
            kind = event["kind"]
            token_id = str(event["token_id"])
            token_index = tokens[token_id]
            try:
                if kind == KIND_SNAPSHOT:
                    reconstructor.apply_snapshot(self.snapshot_of(event, episode))
                    if token_id in pending_uncertain:
                        interval = pending_uncertain.pop(token_id)
                        interval["end_utc_ns"] = int(event["received_utc_ns"])
                        uncertainty.append(interval)
                elif kind == KIND_TICK:
                    reconstructor.apply_tick_size_change(self.tick_of(event, episode))
                else:
                    reconstructor.apply_change(self.change_of(event, episode))
            except (InvalidBookState, ValueError) as exc:
                invalid_events += 1
                book = reconstructor.books.get(token_id)
                if book is not None:
                    book.valid = False
                if token_id not in pending_uncertain:
                    pending_uncertain[token_id] = {
                        "token_id": token_id,
                        "start_utc_ns": int(event["received_utc_ns"]),
                        "end_utc_ns": None,
                        "reason": type(exc).__name__,
                    }
                continue
            book = reconstructor.books.get(token_id)
            if book is None:
                continue
            self._append_row(rows, event, token_index, book)

        for interval in pending_uncertain.values():
            uncertainty.append(interval)

        table = pa.Table.from_pydict(rows, schema=tape_schema(self.depth))
        trades = self._trades(episode, start_ns, end_ns, tokens)
        stats = TapeStats(
            condition_id=episode.condition_id,
            rows=table.num_rows,
            trades=trades.num_rows,
            invalid_events=invalid_events,
            uncertainty_intervals=len(uncertainty),
            first_utc_ns=rows["received_utc_ns"][0] if rows["received_utc_ns"] else None,
            last_utc_ns=rows["received_utc_ns"][-1] if rows["received_utc_ns"] else None,
        )
        metadata = {
            "tape_version": TAPE_VERSION,
            "depth": self.depth,
            "condition_id": episode.condition_id,
            "market_slug": episode.market_slug,
            "window": {"start_utc_ns": start_ns, "end_utc_ns": end_ns},
            "token_index": {
                episode.up_token_id: _SIDE_UP,
                episode.down_token_id: _SIDE_DOWN,
            },
            "uncertainty_intervals": uncertainty,
            "stats": asdict(stats),
        }

        # The dashboard may read this workspace while the refresh service is
        # rebuilding it.  Publish the two Parquet files first and the metadata
        # marker last; readers accept an episode only when that marker describes
        # the current tape format.  A unique temporary name also keeps concurrent
        # manual builds from writing into the same file.
        suffix = f".{uuid.uuid4().hex}.partial"
        tape_path = self.tape_path(episode)
        trades_path = self.trades_path(episode)
        metadata_path = self.metadata_path(episode)
        tape_temporary = tape_path.with_name(tape_path.name + suffix)
        trades_temporary = trades_path.with_name(trades_path.name + suffix)
        metadata_temporary = metadata_path.with_name(metadata_path.name + suffix)
        building_temporary = metadata_path.with_name(metadata_path.name + suffix + ".building")
        temporary_paths = (
            tape_temporary,
            trades_temporary,
            metadata_temporary,
            building_temporary,
        )
        try:
            pq.write_table(
                table,
                tape_temporary,
                compression="zstd",
                row_group_size=TAPE_ROW_GROUP_SIZE,
            )
            pq.write_table(trades, trades_temporary, compression="zstd")
            metadata_temporary.write_text(
                json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8"
            )
            building_temporary.write_text(
                json.dumps(
                    {
                        "tape_version": "building",
                        "condition_id": episode.condition_id,
                    },
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            os.replace(building_temporary, metadata_path)
            os.replace(tape_temporary, tape_path)
            os.replace(trades_temporary, trades_path)
            os.replace(metadata_temporary, metadata_path)
        finally:
            for temporary in temporary_paths:
                temporary.unlink(missing_ok=True)
        return stats

    def _append_row(
        self,
        rows: dict[str, list[Any]],
        event: dict[str, Any],
        token_index: int,
        book: Any,
    ) -> None:
        bids = list(reversed(book.bids.items()))
        asks = list(book.asks.items())
        kept_bids = bids if self.depth is None else bids[: self.depth]
        kept_asks = asks if self.depth is None else asks[: self.depth]
        bid_prices = [price for price, _ in kept_bids]
        bid_sizes = [size for _, size in kept_bids]
        ask_prices = [price for price, _ in kept_asks]
        ask_sizes = [size for _, size in kept_asks]
        rows["received_utc_ns"].append(int(event["received_utc_ns"]))
        rows["sequence"].append(int(event["sequence"]))
        rows["token_index"].append(token_index)
        rows["event_kind"].append(int(event["kind"]))
        event_side = BOOK_SIDE_NONE
        if event["kind"] == KIND_UPDATE:
            event_side = BOOK_SIDE_BIDS if event["side"] == "BUY" else BOOK_SIDE_ASKS
        rows["event_side"].append(event_side)
        rows["event_price_scaled"].append(
            int(event["price_scaled"]) if event["kind"] == KIND_UPDATE else None
        )
        rows["book_valid"].append(bool(book.valid))
        rows["best_bid_scaled"].append(bid_prices[0] if bid_prices else None)
        rows["best_ask_scaled"].append(ask_prices[0] if ask_prices else None)
        rows["bid_size_scaled"].append(bid_sizes[0] if bid_sizes else None)
        rows["ask_size_scaled"].append(ask_sizes[0] if ask_sizes else None)
        rows["tick_size_scaled"].append(int(book.tick_size_scaled))
        if self.depth is None:
            rows["bid_prices_scaled"].append(bid_prices)
            rows["bid_sizes_scaled"].append(bid_sizes)
            rows["ask_prices_scaled"].append(ask_prices)
            rows["ask_sizes_scaled"].append(ask_sizes)
            rows["bid_depth_beyond_scaled"].append(0)
            rows["ask_depth_beyond_scaled"].append(0)
        else:
            rows["bid_prices_scaled"].append(bid_prices + [0] * (self.depth - len(bid_prices)))
            rows["bid_sizes_scaled"].append(bid_sizes + [0] * (self.depth - len(bid_sizes)))
            rows["ask_prices_scaled"].append(ask_prices + [0] * (self.depth - len(ask_prices)))
            rows["ask_sizes_scaled"].append(ask_sizes + [0] * (self.depth - len(ask_sizes)))
            rows["bid_depth_beyond_scaled"].append(sum(size for _, size in bids[self.depth :]))
            rows["ask_depth_beyond_scaled"].append(sum(size for _, size in asks[self.depth :]))

    def iter_book_events(
        self, episode: Episode, start_ns: int, end_ns: int
    ) -> Iterator[dict[str, Any]]:
        """Yield book events in the reference engine's deterministic order.

        The ordering key is receipt time, then event kind (snapshot, tick, update),
        then collector sequence, then the change's index inside its parent frame.
        Predicate pushdown on the receipt-time range keeps a single episode's read
        proportional to the episode, not to the whole archive.
        """
        tokens = f"('{episode.up_token_id}', '{episode.down_token_id}')"
        bounds = f"received_utc_ns between {start_ns} and {end_ns}"
        snapshots = f"""
        select {KIND_SNAPSHOT} as kind, s.received_utc_ns, s.received_monotonic_ns, s.sequence,
               0 as change_index, s.token_id, s.snapshot_id, s.outcome, s.source,
               s.exchange_timestamp_ns, s.book_hash, s.tick_size_scaled,
               s.minimum_order_size_scaled, s.connection_id, s.run_id,
               null::varchar as side, null::bigint as price_scaled, null::bigint as new_size_scaled,
               null::bigint as reported_best_bid_scaled, null::bigint as reported_best_ask_scaled,
               null::bigint as new_tick_size_scaled, null::varchar as parent_event_id
        from {_dataset(self.storage_root, "book_snapshots")} s
        where s.token_id in {tokens} and s.{bounds}
        """
        updates = f"""
        select {KIND_UPDATE} as kind, u.received_utc_ns, u.received_monotonic_ns, u.sequence,
               u.change_index, u.token_id, null as snapshot_id, u.outcome, 'clob_market_ws' as source,
               u.exchange_timestamp_ns, u.book_hash, null::bigint as tick_size_scaled,
               null::bigint as minimum_order_size_scaled, u.connection_id, null::varchar as run_id,
               u.side, u.price_scaled, u.new_size_scaled,
               u.reported_best_bid_scaled, u.reported_best_ask_scaled,
               null::bigint as new_tick_size_scaled, u.parent_event_id
        from {_dataset(self.storage_root, "book_updates")} u
        where u.token_id in {tokens} and u.{bounds}
        """
        parts = [snapshots, updates]
        ticks_directory = self.storage_root / "normalized" / "tick_size_changes"
        if ticks_directory.exists() and any(ticks_directory.rglob("*.parquet")):
            parts.append(
                f"""
        select {KIND_TICK} as kind, t.received_utc_ns, t.received_monotonic_ns, t.sequence,
               0 as change_index, t.token_id, null as snapshot_id, null as outcome, t.source,
               t.exchange_timestamp_ns, null::varchar as book_hash, null::bigint as tick_size_scaled,
               null::bigint as minimum_order_size_scaled, t.connection_id, null::varchar as run_id,
               null::varchar as side, null::bigint as price_scaled, null::bigint as new_size_scaled,
               null::bigint as reported_best_bid_scaled, null::bigint as reported_best_ask_scaled,
               t.new_tick_size_scaled, null::varchar as parent_event_id
        from {_dataset(self.storage_root, "tick_size_changes")} t
        where t.token_id in {tokens} and t.{bounds}
                """
            )
        sql = (
            " union all ".join(f"({part})" for part in parts)
            + " order by received_utc_ns, kind, sequence, change_index"
        )
        # Snapshot levels are read first: a second `execute` on the same DuckDB
        # connection replaces the active result set, so the streaming cursor below
        # must be the last statement issued on it.
        levels = self._snapshot_levels(episode, start_ns, end_ns)
        cursor = self.connection.execute(sql)
        columns = [description[0] for description in cursor.description or []]
        while True:
            batch = cursor.fetchmany(20_000)
            if not batch:
                break
            for row in batch:
                event = dict(zip(columns, row, strict=True))
                if event["kind"] == KIND_SNAPSHOT:
                    event["levels"] = levels.get(str(event["snapshot_id"]), ([], []))
                yield event

    def _snapshot_levels(
        self, episode: Episode, start_ns: int, end_ns: int
    ) -> dict[str, tuple[list[BookLevel], list[BookLevel]]]:
        tokens = f"('{episode.up_token_id}', '{episode.down_token_id}')"
        sql = f"""
        select snapshot_id, side, price_scaled, size_scaled
        from {_dataset(self.storage_root, "book_snapshot_levels")}
        where token_id in {tokens} and received_utc_ns between {start_ns} and {end_ns}
        order by snapshot_id, side, level_rank_from_best
        """
        result: dict[str, tuple[list[BookLevel], list[BookLevel]]] = {}
        for snapshot_id, side, price, size in self.connection.execute(sql).fetchall():
            bids, asks = result.setdefault(str(snapshot_id), ([], []))
            level = BookLevel(price_scaled=int(price), size_scaled=int(size))
            (bids if side == "BUY" else asks).append(level)
        return result

    def snapshot_of(self, event: dict[str, Any], episode: Episode) -> BookSnapshot:
        bids, asks = event["levels"]
        return BookSnapshot(
            snapshot_id=str(event["snapshot_id"]),
            sequence=int(event["sequence"]),
            run_id=str(event["run_id"] or ""),
            connection_id=str(event["connection_id"] or ""),
            source=str(event["source"]),
            condition_id=episode.condition_id,
            token_id=str(event["token_id"]),
            outcome=str(event["outcome"] or ""),
            exchange_timestamp_ns=event["exchange_timestamp_ns"],
            received_utc_ns=int(event["received_utc_ns"]),
            received_monotonic_ns=int(event["received_monotonic_ns"]),
            book_hash=event["book_hash"],
            tick_size_scaled=int(event["tick_size_scaled"] or episode.tick_size_scaled),
            minimum_order_size_scaled=int(
                event["minimum_order_size_scaled"] or episode.minimum_order_size_scaled
            ),
            bids=tuple(bids),
            asks=tuple(asks),
        )

    def change_of(self, event: dict[str, Any], episode: Episode) -> BookLevelChange:
        return BookLevelChange(
            parent_event_id=str(event["parent_event_id"] or ""),
            change_index=int(event["change_index"]),
            sequence=int(event["sequence"]),
            condition_id=episode.condition_id,
            token_id=str(event["token_id"]),
            outcome=str(event["outcome"] or ""),
            exchange_timestamp_ns=event["exchange_timestamp_ns"],
            received_utc_ns=int(event["received_utc_ns"]),
            received_monotonic_ns=int(event["received_monotonic_ns"]),
            side=str(event["side"]),  # type: ignore[arg-type]
            price_scaled=int(event["price_scaled"]),
            new_size_scaled=int(event["new_size_scaled"]),
            book_hash=event["book_hash"],
            reported_best_bid_scaled=event["reported_best_bid_scaled"],
            reported_best_ask_scaled=event["reported_best_ask_scaled"],
            connection_id=str(event["connection_id"] or ""),
        )

    def tick_of(self, event: dict[str, Any], episode: Episode) -> TickSizeChange:
        return TickSizeChange(
            tick_change_id="",
            sequence=int(event["sequence"]),
            condition_id=episode.condition_id,
            token_id=str(event["token_id"]),
            exchange_timestamp_ns=event["exchange_timestamp_ns"],
            received_utc_ns=int(event["received_utc_ns"]),
            received_monotonic_ns=int(event["received_monotonic_ns"]),
            old_tick_size_scaled=None,
            new_tick_size_scaled=int(event["new_tick_size_scaled"]),
            connection_id=str(event["connection_id"] or ""),
            source=str(event["source"] or "clob_market_ws"),
        )

    def _trades(
        self,
        episode: Episode,
        start_ns: int,
        end_ns: int,
        tokens: dict[str, int],
    ) -> pa.Table:
        directory = self.storage_root / "normalized" / "trades"
        if not directory.exists() or not any(directory.rglob("*.parquet")):
            return pa.Table.from_pydict(
                {name: [] for name in TRADE_SCHEMA.names}, schema=TRADE_SCHEMA
            )
        token_list = ", ".join(f"'{token}'" for token in tokens)
        sql = f"""
        select received_utc_ns, token_id, price_scaled, size_scaled
        from {_dataset(self.storage_root, "trades")}
        where token_id in ({token_list}) and received_utc_ns between {start_ns} and {end_ns}
        order by received_utc_ns
        """
        received: list[int] = []
        index: list[int] = []
        prices: list[int] = []
        sizes: list[int | None] = []
        for row in self.connection.execute(sql).fetchall():
            received.append(int(row[0]))
            index.append(tokens[str(row[1])])
            prices.append(int(row[2]))
            sizes.append(None if row[3] is None else int(row[3]))
        return pa.Table.from_pydict(
            {
                "received_utc_ns": received,
                "token_index": index,
                "price_scaled": prices,
                "size_scaled": sizes,
            },
            schema=TRADE_SCHEMA,
        )


def _typed_column(table: pa.Table, name: str, *, null: int = 0) -> array.array[int]:
    """One column as a flat typed buffer, copied straight out of Arrow.

    `to_pylist()` boxes every value into a Python object.  On a quarter-million-row
    tape that single call dominates the cost of loading an episode, to the point
    where a sweep spends five times longer decoding tapes than simulating against
    them.  Copying the Arrow buffer into `array.array` preserves exact integer
    values and random access while skipping the boxing entirely.
    """
    column = table.column(name).combine_chunks()
    if column.type == pa.bool_():
        # Booleans are bit-packed in Arrow; widening to int8 gives one byte per row,
        # which is what a flat buffer view needs.
        column = pc.cast(column, pa.int8())
    column = pc.fill_null(column, null)
    values: array.array[int] = array.array(_TYPE_CODES[column.type])
    if column.offset:  # pragma: no cover - a combined column is not a slice
        values.fromlist(column.to_pylist())
        return values
    values.frombytes(column.buffers()[1][: len(column) * values.itemsize])
    return values


class Quotes(Sequence[int | None]):
    """A best-bid or best-ask column that reports an absent quote as `None`.

    "No bid at all" is a distinct and important state for this strategy family —
    it is the book a stop meets when every quote has been pulled — so it must stay
    distinguishable from a price, even though the column itself is a flat buffer.
    """

    __slots__ = ("_values",)

    def __init__(self, values: array.array[int]) -> None:
        self._values = values

    def __len__(self) -> int:
        return len(self._values)

    @overload
    def __getitem__(self, index: int) -> int | None: ...

    @overload
    def __getitem__(self, index: slice) -> Sequence[int | None]: ...

    def __getitem__(self, index: int | slice) -> int | Sequence[int | None] | None:
        if isinstance(index, slice):
            return [None if value == _NO_QUOTE else value for value in self._values[index]]
        value = self._values[index]
        return None if value == _NO_QUOTE else value


class LazyLadders:
    """Depth ladders read from the tape's Parquet row groups on demand.

    A run touches ladders only where an order actually executes — a handful of rows
    per episode — while holding every ladder in memory costs about ninety megabytes
    per episode.  Reading the containing row group instead costs a few, and the
    most recent one is kept because orders cluster in time.
    """

    __slots__ = ("_cached", "_group", "_path", "_starts")

    def __init__(self, path: Path) -> None:
        self._path = path
        self._starts: list[int] = []
        offset = 0
        with pq.ParquetFile(path) as handle:
            for index in range(handle.num_row_groups):
                self._starts.append(offset)
                offset += handle.metadata.row_group(index).num_rows
        self._group = -1
        self._cached: pa.Table | None = None

    def row(self, index: int) -> tuple[pa.Table, int]:
        """The ladder table containing `index`, and that row's offset within it."""
        group = bisect_right(self._starts, index) - 1
        if group < 0:
            raise IndexError(f"row {index} is outside the tape")
        if group != self._group or self._cached is None:
            with pq.ParquetFile(self._path) as handle:
                self._cached = handle.read_row_group(group, columns=list(LADDER_COLUMNS))
            self._group = group
        return self._cached, index - self._starts[group]

    def release(self) -> None:
        self._group = -1
        self._cached = None


@dataclass(slots=True)
class EpisodeTape:
    """Loaded tape in the column-oriented form the simulator scans.

    Scalar columns are materialised as flat typed buffers because every parameter
    combination scans all of them.  Depth ladders are decoded only at the few
    indices where an order actually executes; materialising them for every row
    costs seconds and ninety megabytes per episode and buys nothing.
    """

    episode: Episode
    depth: int | None
    received_utc_ns: Sequence[int]
    token_index: Sequence[int]
    event_kind: Sequence[int]
    event_side: Sequence[int]
    event_price_scaled: Sequence[int]
    book_valid: Sequence[int]
    best_bid_scaled: Sequence[int | None]
    best_ask_scaled: Sequence[int | None]
    trade_utc_ns: Sequence[int]
    trade_token_index: Sequence[int]
    trade_size_scaled: Sequence[int]
    uncertainty: list[dict[str, Any]]
    _table: pa.Table | None = None
    _ladders: LazyLadders | None = field(default=None)

    def __len__(self) -> int:
        return len(self.received_utc_ns)

    @property
    def resident_bytes(self) -> int:
        """Approximate memory held, so a cache can bound how many tapes it keeps."""
        # Use a conservative eight bytes for each scalar column. Several columns
        # are int8, but overestimating keeps the dashboard's cgroup headroom safe.
        # Lazily decoded ladder groups are released after each run and therefore
        # are not persistent cache residents.
        scalars = 8 * len(SCALAR_COLUMNS) * len(self.received_utc_ns)
        trades = 24 * len(self.trade_utc_ns)
        return scalars + trades + 4096

    def token_index_of(self, token_id: str) -> int:
        return _SIDE_UP if token_id == self.episode.up_token_id else _SIDE_DOWN

    def token_id_of(self, index: int) -> str:
        return self.episode.up_token_id if index == _SIDE_UP else self.episode.down_token_id

    def ladder(self, index: int, book_side: Literal["bids", "asks"]) -> list[tuple[int, int]]:
        """Depth ladder at row `index`, best level first, zero padding removed.

        `book_side` names the resting side of the book, not the taker's side: a
        buy consumes `asks` and a sell consumes `bids`.  Use `taker_ladder` when
        starting from an order side.
        """
        prefix = "bid" if book_side == "bids" else "ask"
        table, row = self._ladder_row(index)
        prices = table.column(f"{prefix}_prices_scaled")[row].as_py()
        sizes = table.column(f"{prefix}_sizes_scaled")[row].as_py()
        return [(price, size) for price, size in zip(prices, sizes, strict=True) if size > 0]

    def _ladder_row(self, index: int) -> tuple[pa.Table, int]:
        if self._ladders is not None:
            return self._ladders.row(index)
        if self._table is None:
            raise RuntimeError("tape carries no depth ladders")
        return self._table, index

    def taker_ladder(self, index: int, order_side: Literal["BUY", "SELL"]) -> list[tuple[int, int]]:
        """Levels a marketable order of `order_side` would consume."""
        return self.ladder(index, "asks" if order_side == "BUY" else "bids")

    def depth_beyond(self, index: int, book_side: Literal["bids", "asks"]) -> int:
        prefix = "bid" if book_side == "bids" else "ask"
        table, row = self._ladder_row(index)
        value = table.column(f"{prefix}_depth_beyond_scaled")[row].as_py()
        return int(value or 0)

    def release_depth_cache(self) -> None:
        """Drop the last decoded row group while retaining cheap scalar columns."""
        if self._ladders is not None:
            self._ladders.release()

    def is_uncertain(self, token_id: str, timestamp_ns: int) -> bool:
        for interval in self.uncertainty:
            if interval["token_id"] != token_id:
                continue
            end = interval["end_utc_ns"]
            if interval["start_utc_ns"] <= timestamp_ns and (end is None or timestamp_ns <= end):
                return True
        return False


def load_tape(workspace: Path, episode: Episode) -> EpisodeTape:
    """Load one episode's tape: scalar columns eagerly, depth ladders on demand."""
    directory = workspace / "tapes"
    path = directory / f"{episode.condition_id}.parquet"
    metadata = json.loads(
        (directory / f"{episode.condition_id}.meta.json").read_text(encoding="utf-8")
    )
    if metadata.get("tape_version") != TAPE_VERSION:
        raise ValueError(
            f"stale tape for {episode.condition_id}: "
            f"found {metadata.get('tape_version')!r}, need {TAPE_VERSION!r}; rebuild tapes"
        )
    table = pq.read_table(path, columns=list(SCALAR_COLUMNS))
    trades = pq.read_table(directory / f"{episode.condition_id}.trades.parquet")
    return EpisodeTape(
        episode=episode,
        depth=None if metadata["depth"] is None else int(metadata["depth"]),
        received_utc_ns=_typed_column(table, "received_utc_ns"),
        token_index=_typed_column(table, "token_index"),
        event_kind=_typed_column(table, "event_kind"),
        event_side=_typed_column(table, "event_side", null=BOOK_SIDE_NONE),
        event_price_scaled=_typed_column(table, "event_price_scaled"),
        book_valid=_typed_column(table, "book_valid"),
        best_bid_scaled=Quotes(_typed_column(table, "best_bid_scaled", null=_NO_QUOTE)),
        best_ask_scaled=Quotes(_typed_column(table, "best_ask_scaled", null=_NO_QUOTE)),
        trade_utc_ns=_typed_column(trades, "received_utc_ns"),
        trade_token_index=_typed_column(trades, "token_index"),
        trade_size_scaled=_typed_column(trades, "size_scaled"),
        uncertainty=metadata["uncertainty_intervals"],
        _ladders=LazyLadders(path),
    )
