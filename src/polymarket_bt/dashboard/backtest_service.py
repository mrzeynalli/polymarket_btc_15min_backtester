"""Interactive backtesting behind the public dashboard.

The CLI sweep answers "which of these 48 variants survives?".  This module answers
a different question: "what would *this* set of rules have done?", asked by a
person filling in a form and waiting for an answer.

That difference drives three design choices.

**Runs are jobs, not requests.** A backtest reads every recorded book state of
every selected market.  That is seconds to minutes of work, far past any sensible
HTTP timeout, so a submission returns an identifier and the page polls it.  One
worker runs at a time: the box also hosts the collector, and a backtest must never
be the reason a market goes unrecorded.

**Fill quality is a headline result, not a footnote.** A form that accepts a
$100,000 stake must answer what the order book would actually have absorbed at
that size.  Requested versus filled notional, the constraint that bound each
order, and shares that could not be sold at all are reported alongside profit,
because at size they *are* the result.

**Nothing here writes.** Episode index and tapes are read from a prepared
workspace; recorded collector data is never opened for writing and never opened
at all from this process.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from collections import Counter, OrderedDict
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, ValidationError, model_validator

from polymarket_bt.backtest.episodes import Episode, read_episode_index
from polymarket_bt.backtest.fastsim import EpisodeResult, ThresholdHoldSimulator
from polymarket_bt.backtest.realism import ExecutionRealism, preset
from polymarket_bt.backtest.sweep import aggregate
from polymarket_bt.backtest.tape import TAPE_VERSION, EpisodeTape, load_tape
from polymarket_bt.backtest.threshold_hold import ThresholdHoldParams
from polymarket_bt.backtest.wallet import PendingCredit, ReplayWallet, WalletEvent
from polymarket_bt.clock import utc_now_ns
from polymarket_bt.config import StrictModel
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE, USDC_SCALE

LOG = logging.getLogger("polymarket-dashboard.backtest")

MARKET_MINUTES = 15.0
NS_PER_MINUTE = 60_000_000_000
REALISM_PRESETS = ("optimistic", "base", "pessimistic")
EXECUTION_POLICIES = ("adaptive_pov", "twap", "immediate")
SIDES = ("favoured", "up", "down")
SIZING_MODES = ("fixed", "compound")

# Finished jobs are kept so a reloaded page can still collect its answer, but only
# in memory and only for a while: results are reproducible by resubmitting.
MAX_RETAINED_JOBS = 64
JOB_TTL_SECONDS = 3600
DEFAULT_TAPE_CACHE_BYTES = 512 * 1024 * 1024
# Per-episode detail is what makes a result checkable rather than merely believable,
# but a year of episodes would be megabytes of JSON; the table is capped and the
# omission is reported.
MAX_EPISODE_ROWS = 750


def _usd(scaled: int | None) -> float | None:
    return None if scaled is None else round(scaled / USDC_SCALE, 2)


def _price(scaled: int | None) -> float | None:
    return None if scaled is None else round(scaled / POLYMARKET_PRICE_SCALE, 4)


def _shares(scaled: int | None) -> float | None:
    return None if scaled is None else round(scaled / SHARE_SIZE_SCALE, 4)


def _pct(ppm: int | None) -> float | None:
    return None if ppm is None else round(ppm / 10_000, 2)


def _iso(timestamp_ns: int) -> str:
    return datetime.fromtimestamp(timestamp_ns / 1e9, tz=UTC).isoformat(timespec="seconds")


class BacktestRequest(StrictModel):
    """One strategy, as a person describes it in the form.

    Prices are decimals in the 0-1 sense a Polymarket quote is quoted in, minutes
    are offsets into the 15-minute market, and the stake is dollars.  Everything
    is converted to the simulator's fixed-scale integers exactly once, here.
    """

    entry_price: float = Field(ge=0.01, le=0.99)
    # Never pay more than this even if the book moves during the round trip.
    # Defaults to five cents of tolerance above the trigger.
    entry_limit_price: float | None = Field(default=None, ge=0.01, le=1.0)
    # Both None means "any time inside the market" — the timed entry is optional.
    entry_from_minute: float | None = Field(default=None, ge=0, le=MARKET_MINUTES)
    entry_to_minute: float | None = Field(default=None, gt=0, le=MARKET_MINUTES)
    # None means hold to settlement: no stop loss.
    stop_loss_price: float | None = Field(default=None, ge=0.01, lt=0.99)
    # None means the stop is live as soon as the position exists; otherwise the
    # price is ignored until this minute, so only a late collapse is sold into.
    stop_loss_from_minute: float | None = Field(default=None, ge=0, le=MARKET_MINUTES)
    side: Literal["favoured", "up", "down"] = "favoured"
    # In "fixed" this is the stake risked on every market; in "compound" it is the
    # starting balance and each market stakes whatever the balance has become.
    sizing: Literal["fixed", "compound"] = "fixed"
    stake_usd: float = Field(default=100.0, gt=0, le=10_000_000)
    realism: Literal["optimistic", "base", "pessimistic"] = "base"
    execution_policy: Literal["adaptive_pov", "twap", "immediate"] = "adaptive_pov"
    execution_horizon_seconds: float = Field(default=2.0, gt=0, le=60)
    from_date: str | None = None
    to_date: str | None = None
    max_episodes: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _check(self) -> BacktestRequest:
        if self.entry_from_minute is not None and self.entry_to_minute is not None:
            if self.entry_to_minute <= self.entry_from_minute:
                raise ValueError("the entry window must end after it starts")
        if self.stop_loss_price is not None and self.stop_loss_price >= self.entry_price:
            raise ValueError("the stop-loss price must be below the entry price")
        if self.stop_loss_from_minute is not None and self.stop_loss_price is None:
            raise ValueError("a stop-loss price is needed before it can be armed at a minute")
        if self.entry_limit_price is not None and self.entry_limit_price < self.entry_price:
            raise ValueError("the maximum price paid cannot be below the entry price")
        for name in ("from_date", "to_date"):
            value = getattr(self, name)
            if value is None:
                continue
            try:
                datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError as exc:
                raise ValueError(f"{name} must be a UTC date formatted YYYY-MM-DD") from exc
        return self

    @property
    def stake_scaled(self) -> int:
        return round(self.stake_usd * USDC_SCALE)

    def to_params(self) -> ThresholdHoldParams:
        trigger = round(self.entry_price * POLYMARKET_PRICE_SCALE)
        limit = (
            round(self.entry_limit_price * POLYMARKET_PRICE_SCALE)
            if self.entry_limit_price is not None
            else min(POLYMARKET_PRICE_SCALE, trigger + 50_000)
        )
        return ThresholdHoldParams(
            entry_from_minute=self.entry_from_minute or 0.0,
            entry_to_minute=self.entry_to_minute or MARKET_MINUTES,
            entry_trigger_price_scaled=trigger,
            entry_limit_price_scaled=limit,
            side_selection=self.side,
            order_shares_scaled=None,
            # In compound mode this is only the first market's stake; the runner
            # replaces it with the running balance before each subsequent market.
            order_notional_scaled=self.stake_scaled,
            entry_execution_policy=self.execution_policy,
            execution_horizon_seconds=self.execution_horizon_seconds,
            stop_loss_price_scaled=(
                round(self.stop_loss_price * POLYMARKET_PRICE_SCALE)
                if self.stop_loss_price is not None
                else None
            ),
            stop_loss_from_minute=self.stop_loss_from_minute,
        )

    def window_ns(self) -> tuple[int | None, int | None]:
        """Inclusive episode-start bounds implied by the requested dates."""

        def at(value: str | None, end_of_day: bool) -> int | None:
            if value is None:
                return None
            day = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
            seconds = day.timestamp() + (86_400 if end_of_day else 0)
            return int(seconds * 1e9)

        return at(self.from_date, False), at(self.to_date, True)


class TapeCache:
    """Bounded least-recently-used cache of loaded episode tapes.

    Tapes are immutable once loaded and a single worker runs at a time, so a hit
    can be handed out directly.  The budget is in bytes rather than entries
    because episode sizes vary by an order of magnitude with market activity.
    """

    def __init__(self, workspace: Path, budget_bytes: int = DEFAULT_TAPE_CACHE_BYTES) -> None:
        self.workspace = workspace
        self.budget_bytes = budget_bytes
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, EpisodeTape] = OrderedDict()
        self._bytes = 0
        self.hits = 0
        self.misses = 0

    def _load(self, episode: Episode) -> EpisodeTape:
        return load_tape(self.workspace, episode)

    def get(self, episode: Episode) -> EpisodeTape:
        with self._lock:
            cached = self._entries.get(episode.condition_id)
            if cached is not None:
                self._entries.move_to_end(episode.condition_id)
                self.hits += 1
                return cached
        tape = self._load(episode)
        with self._lock:
            self.misses += 1
            if episode.condition_id not in self._entries:
                self._entries[episode.condition_id] = tape
                self._bytes += tape.resident_bytes
                while self._bytes > self.budget_bytes and len(self._entries) > 1:
                    _, evicted = self._entries.popitem(last=False)
                    self._bytes -= evicted.resident_bytes
        return tape

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "episodes": len(self._entries),
                "bytes": self._bytes,
                "hits": self.hits,
                "misses": self.misses,
            }

    def clear(self) -> None:
        """Invalidate cached episodes after an atomic index refresh."""
        with self._lock:
            for tape in self._entries.values():
                tape.release_depth_cache()
            self._entries.clear()
            self._bytes = 0


@dataclass
class BacktestJob:
    """One submitted run and everything a poller needs to render it."""

    job_id: str
    request: BacktestRequest
    created_utc_ns: int
    state: Literal["queued", "running", "succeeded", "failed"] = "queued"
    total: int = 0
    completed: int = 0
    started_utc_ns: int | None = None
    finished_utc_ns: int | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            payload: dict[str, Any] = {
                "job_id": self.job_id,
                "state": self.state,
                "total_episodes": self.total,
                "completed_episodes": self.completed,
                "submitted_utc": _iso(self.created_utc_ns),
                "elapsed_seconds": round(
                    ((self.finished_utc_ns or utc_now_ns()) - (self.started_utc_ns or utc_now_ns()))
                    / 1e9,
                    1,
                ),
            }
            if self.state == "succeeded":
                payload["result"] = self.result
            if self.state == "failed":
                payload["error"] = self.error
            return payload

    def advance(self) -> None:
        with self._lock:
            self.completed += 1

    def complete(self) -> None:
        """Mark every selected market accounted for, including any never run.

        A compounded run that goes broke halfway stops evaluating markets; the
        progress bar should still finish rather than freeze at the halt.
        """
        with self._lock:
            self.completed = self.total


class BacktestService:
    """Owns the episode index, the tape cache, and the single run worker."""

    def __init__(
        self,
        workspace: Path,
        *,
        cache_bytes: int = DEFAULT_TAPE_CACHE_BYTES,
        seed: int = 1729,
    ) -> None:
        self.workspace = workspace
        self.seed = seed
        self.cache = TapeCache(workspace, cache_bytes)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="backtest")
        self._jobs: OrderedDict[str, BacktestJob] = OrderedDict()
        self._jobs_lock = threading.Lock()
        self._index_lock = threading.Lock()
        self._episodes: list[Episode] = []
        self._index_mtime: float | None = None

    # -- episode index ------------------------------------------------------
    @property
    def index_path(self) -> Path:
        return self.workspace / "episodes.json"

    def episodes(self) -> list[Episode]:
        """Episode index, reloaded whenever the refresh job rewrites it."""
        with self._index_lock:
            try:
                mtime = self.index_path.stat().st_mtime
            except OSError:
                return []
            if mtime != self._index_mtime:
                refreshed = sorted(read_episode_index(self.workspace), key=_start_of)
                self.cache.clear()
                self._episodes = refreshed
                self._index_mtime = mtime
            return self._episodes

    def _selected(self, request: BacktestRequest) -> tuple[list[Episode], Counter[str]]:
        start_from, start_to = request.window_ns()
        selected: list[Episode] = []
        excluded: Counter[str] = Counter()
        for episode in self.episodes():
            if start_from is not None and episode.start_utc_ns < start_from:
                continue
            if start_to is not None and episode.start_utc_ns > start_to:
                continue
            if not episode.eligible:
                excluded[episode.exclusion_reason or "ineligible"] += 1
                continue
            if not self._tape_ready(episode):
                excluded["tape_not_built_yet"] += 1
                continue
            selected.append(episode)
        if request.max_episodes is not None and len(selected) > request.max_episodes:
            # Keep the most recent, which is what a person means by "the last N".
            selected = selected[-request.max_episodes :]
        return selected, excluded

    def _tape_ready(self, episode: Episode) -> bool:
        """Return true only for a complete, production-depth tape publication."""
        directory = self.workspace / "tapes"
        tape_path = directory / f"{episode.condition_id}.parquet"
        trades_path = directory / f"{episode.condition_id}.trades.parquet"
        metadata_path = directory / f"{episode.condition_id}.meta.json"
        if not (tape_path.is_file() and trades_path.is_file() and metadata_path.is_file()):
            return False
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return False
        return (
            metadata.get("tape_version") == TAPE_VERSION
            and metadata.get("depth") is None
            and metadata.get("condition_id") == episode.condition_id
        )

    # -- metadata -----------------------------------------------------------
    def meta(self) -> dict[str, Any]:
        episodes = self.episodes()
        eligible = [episode for episode in episodes if episode.eligible]
        ready = [episode for episode in eligible if self._tape_ready(episode)]
        exclusions = Counter(
            episode.exclusion_reason for episode in episodes if episode.exclusion_reason
        )
        return {
            "available": bool(ready),
            "episodes_indexed": len(episodes),
            "episodes_eligible": len(eligible),
            "episodes_ready": len(ready),
            "excluded": {str(key): value for key, value in exclusions.items()},
            "earliest_utc": _iso(eligible[0].start_utc_ns) if eligible else None,
            "latest_utc": _iso(eligible[-1].end_utc_ns) if eligible else None,
            "market_minutes": MARKET_MINUTES,
            "realism_presets": list(REALISM_PRESETS),
            "execution_policies": list(EXECUTION_POLICIES),
            "sides": list(SIDES),
            "sizing_modes": list(SIZING_MODES),
            "seconds_per_episode_estimate": 0.12,
            "cache": self.cache.stats(),
        }

    # -- jobs ---------------------------------------------------------------
    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            request = BacktestRequest.model_validate(payload)
        except ValidationError as exc:
            raise ValueError(_readable(exc)) from exc
        # Reject impossible parameter combinations here rather than inside the
        # worker, so the form can show the reason immediately.
        try:
            request.to_params()
        except ValidationError as exc:
            raise ValueError(_readable(exc)) from exc
        selected, _ = self._selected(request)
        if not selected:
            raise LookupError("no episodes with reconstructed order books match that selection")

        job = BacktestJob(
            job_id=uuid.uuid4().hex,
            request=request,
            created_utc_ns=utc_now_ns(),
            total=len(selected),
        )
        with self._jobs_lock:
            self._prune()
            self._jobs[job.job_id] = job
        self._executor.submit(self._run, job)
        return {"job_id": job.job_id, "total_episodes": job.total, "state": job.state}

    def job(self, job_id: str) -> dict[str, Any]:
        with self._jobs_lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise LookupError("unknown or expired backtest job")
        return job.snapshot()

    def _prune(self) -> None:
        cutoff = utc_now_ns() - JOB_TTL_SECONDS * 1_000_000_000
        for job_id, job in list(self._jobs.items()):
            finished = job.finished_utc_ns
            if finished is not None and finished < cutoff:
                del self._jobs[job_id]
        while len(self._jobs) > MAX_RETAINED_JOBS:
            for job_id, job in list(self._jobs.items()):
                if job.state in {"succeeded", "failed"}:
                    del self._jobs[job_id]
                    break
            else:
                break

    def _run(self, job: BacktestJob) -> None:
        job.state = "running"
        job.started_utc_ns = utc_now_ns()
        try:
            selected, excluded = self._selected(job.request)
            params = job.request.to_params()
            run = run_episodes(
                params=params,
                realism=preset(job.request.realism),
                episodes=selected,
                load=self.cache.get,
                seed=self.seed,
                sizing=job.request.sizing,
                starting_balance_scaled=job.request.stake_scaled,
                on_progress=job.advance,
            )
            job.complete()
            job.result = build_report(
                request=job.request,
                params=params,
                realism_name=job.request.realism,
                run=run,
                excluded=excluded,
            )
            job.state = "succeeded"
        except Exception as exc:
            LOG.exception("backtest job failed job_id=%s", job.job_id)
            job.error = f"{type(exc).__name__}: {exc}"
            job.state = "failed"
        finally:
            job.finished_utc_ns = utc_now_ns()

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


def _start_of(episode: Episode) -> int:
    return episode.start_utc_ns


@dataclass(slots=True)
class RunOutcome:
    """One completed pass over the selected markets."""

    results: list[EpisodeResult]
    sizing: Literal["fixed", "compound"]
    starting_balance_scaled: int
    final_balance_scaled: int
    episodes_selected: int
    # True when a compounded balance fell below the venue minimum and the
    # remaining markets were never reached. Reporting this is the difference
    # between "this strategy made -$95" and "this strategy went broke in March".
    ended_early: bool
    available_balance_scaled: int = 0
    locked_balance_scaled: int = 0
    wallet_events: list[WalletEvent] = field(default_factory=list)


def run_episodes(
    *,
    params: ThresholdHoldParams,
    realism: ExecutionRealism,
    episodes: Sequence[Episode],
    load: Callable[[Episode], EpisodeTape],
    seed: int,
    sizing: Literal["fixed", "compound"] = "fixed",
    starting_balance_scaled: int = 0,
    on_progress: Callable[[], None] | None = None,
) -> RunOutcome:
    """Replay markets in time order under one of the two staking rules.

    Fixed staking risks the same amount on every market, so each result is
    independent and the order only matters for the drawdown path. Compounding
    stakes the cash available when the signal fires, which makes the sequence and
    settlement timing part of the result: one full loss ends the run, and a market
    that fills only a fraction of the balance leaves the rest idle. Both are what a
    person actually does with money, so both are simulated literally rather than
    scaled from a single fixed-stake pass — a $100 trade's fills are not a tenth
    of a $1,000 trade's when the book is thin.
    """
    ordered_episodes = sorted(episodes, key=lambda item: (item.start_utc_ns, item.condition_id))
    results: list[EpisodeResult] = []
    balance = starting_balance_scaled
    ended_early = False
    simulator = ThresholdHoldSimulator(params, realism, seed=seed)
    wallet = ReplayWallet(
        starting_balance_scaled,
        start_utc_ns=ordered_episodes[0].start_utc_ns if ordered_episodes else 0,
    )
    for episode in ordered_episodes:
        if sizing == "compound" and wallet.total_equity_scaled <= 0:
            ended_early = True
            break
        tape = load(episode)
        try:
            if sizing == "compound":
                # Total equity is only an upper bound. The simulator sizes the parent
                # from cash actually available when the signal fires and queries the
                # ledger again at each arrival, so a future settlement cannot enlarge
                # an earlier order.
                max_spend = wallet.total_equity_scaled
                # Pydantic requires a positive model value. With no cash available by
                # the window, one micro-unit still produces an explicit rejection/no
                # entry instead of silently deleting the episode from the sample.
                max_spend = max(1, max_spend)
                simulator = ThresholdHoldSimulator(
                    params.model_copy(update={"order_notional_scaled": max_spend}),
                    realism,
                    seed=seed,
                )
                result = simulator.run(tape, available_cash_scaled=wallet.peek_available_at)
                _apply_result_to_wallet(wallet, episode, result)
            else:
                result = simulator.run(tape)
        finally:
            # A full-depth row group is much larger than the scalar tape cache.
            # Keeping one decoded group per cached episode would bypass the cache's
            # byte budget after repeated dashboard runs.
            if isinstance(tape, EpisodeTape):
                tape.release_depth_cache()
        results.append(result)
        if sizing == "fixed":
            balance += result.net_pnl_scaled
        if on_progress is not None:
            on_progress()

    if sizing == "compound":
        # Finish at the observation horizon, not at the timestamp of the latest
        # future settlement.  `final_balance` includes known locked payouts, while
        # the available/locked split says what was actually spendable when the run
        # ended.  Releasing every future credit here made the two fields mutually
        # inconsistent and erased settlement timing from the report.
        horizon_ns = max((result.end_utc_ns for result in results), default=wallet.now_utc_ns)
        wallet.advance_to(max(wallet.now_utc_ns, horizon_ns))
        balance = wallet.total_equity_scaled
        available = wallet.available_cash_scaled
        locked = wallet.pending_cash_scaled
    else:
        available = balance
        locked = 0
    return RunOutcome(
        results=results,
        sizing=sizing,
        starting_balance_scaled=starting_balance_scaled,
        final_balance_scaled=balance,
        episodes_selected=len(ordered_episodes),
        ended_early=ended_early,
        available_balance_scaled=available,
        locked_balance_scaled=locked,
        wallet_events=list(wallet.events) if sizing == "compound" else [],
    )


def _apply_result_to_wallet(
    wallet: ReplayWallet,
    episode: Episode,
    result: EpisodeResult,
) -> None:
    """Apply exact fill-time cashflows without moving the ledger into the future.

    Episode simulations are run in entry-time order, but their exits can overlap
    the next market.  BUYs therefore debit at their arrival while SELL proceeds
    and settlement payouts are scheduled for their own availability timestamps.
    The result-level conservation check makes malformed fills fail loudly instead
    of creating or destroying replay cash.
    """
    equity_before = wallet.total_equity_scaled
    filled_orders = sorted(
        (order for order in result.orders if order.filled_shares_scaled),
        key=lambda order: (order.arrival_utc_ns, 0 if order.side == "SELL" else 1),
    )
    # Register credits first so a same-timestamp SELL is available to a BUY, and
    # so later BUY debits release any earlier scheduled exit without advancing to
    # exits that still lie in the future.
    for order in (item for item in filled_orders if item.side == "SELL"):
        net_proceeds = order.notional_scaled - order.fees_scaled
        if net_proceeds < 0:
            raise ValueError(f"sell fees exceed proceeds for {episode.condition_id}:{order.kind}")
        source = f"{episode.condition_id}:{order.kind}"
        wallet.schedule_credit(
            PendingCredit(
                available_utc_ns=order.arrival_utc_ns,
                amount_scaled=net_proceeds,
                source=source,
            )
        )
    if result.settled_shares_scaled:
        # V1 indexes lack a causal resolution timestamp.  Holding their payout
        # through the complete post-end observation window is conservative and
        # prevents an adjacent market from spending it at the boundary.
        available_ns = max(
            episode.end_utc_ns,
            (
                episode.resolution_received_utc_ns
                if episode.resolution_received_utc_ns is not None
                else episode.end_utc_ns + 60_000_000_000
            ),
        )
        wallet.schedule_credit(
            PendingCredit(
                available_utc_ns=available_ns,
                amount_scaled=result.settlement_payout_scaled,
                source=f"{episode.condition_id}:settlement",
            )
        )
    for order in (item for item in filled_orders if item.side == "BUY"):
        wallet.debit(
            order.arrival_utc_ns,
            order.notional_scaled + order.fees_scaled,
            source=f"{episode.condition_id}:{order.kind}",
        )

    actual_delta = wallet.total_equity_scaled - equity_before
    if actual_delta != result.net_pnl_scaled:
        raise ValueError(
            f"wallet cashflows do not reconcile for {episode.condition_id}: "
            f"cashflow_delta={actual_delta}, net_pnl={result.net_pnl_scaled}"
        )


def _readable(exc: ValidationError) -> str:
    parts = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error["loc"]) or "request"
        parts.append(f"{location}: {error['msg']}")
    return "; ".join(parts) or "invalid backtest parameters"


def build_report(
    *,
    request: BacktestRequest,
    params: ThresholdHoldParams,
    realism_name: str,
    run: RunOutcome,
    excluded: Counter[str],
) -> dict[str, Any]:
    """Turn per-episode results into the payload the page renders."""
    results = run.results
    metrics = aggregate(params.label, realism_name, results, params=params.model_dump(mode="json"))
    entered = [result for result in results if result.entered]
    deployed = sum(result.entry_notional_scaled for result in entered)
    compounding = run.sizing == "compound"

    equity = 0
    balance = run.starting_balance_scaled
    peak_balance = balance
    balance_drawdown_ppm = 0
    curve: list[dict[str, Any]] = []
    for result in sorted(entered, key=lambda item: item.start_utc_ns):
        equity += result.net_pnl_scaled
        balance += result.net_pnl_scaled
        peak_balance = max(peak_balance, balance)
        if peak_balance > 0:
            balance_drawdown_ppm = max(
                balance_drawdown_ppm, (peak_balance - balance) * 1_000_000 // peak_balance
            )
        curve.append(
            {
                "utc": _iso(result.start_utc_ns),
                "slug": result.market_slug,
                "trade_usd": _usd(result.net_pnl_scaled),
                "cumulative_usd": _usd(equity),
                "balance_usd": _usd(balance) if compounding else None,
            }
        )

    return {
        "summary": {
            "episodes_evaluated": metrics.episodes,
            "trades": metrics.entries,
            "traded_rate_pct": _pct(metrics.entry_rate_ppm),
            "net_profit_usd": _usd(metrics.net_pnl_scaled),
            "gross_profit_usd": _usd(metrics.gross_pnl_scaled),
            "fees_usd": _usd(metrics.fees_scaled),
            "capital_deployed_usd": _usd(deployed),
            "return_on_capital_pct": (
                round(100 * metrics.net_pnl_scaled / deployed, 2) if deployed else None
            ),
            "win_rate_pct": _pct(metrics.win_rate_ppm),
            "side_correct_pct": _pct(metrics.side_correct_ppm),
            "profitable_trades": metrics.profitable_trades,
            "losing_trades": metrics.entries - metrics.profitable_trades,
            "average_win_usd": _usd(metrics.average_win_scaled),
            "average_loss_usd": _usd(metrics.average_loss_scaled),
            "profit_factor": (
                round(metrics.profit_factor_ppm / 1_000_000, 3)
                if metrics.profit_factor_ppm is not None
                else None
            ),
            "net_profit_per_trade_usd": _usd(metrics.net_pnl_per_entry_scaled),
            "max_drawdown_usd": _usd(metrics.max_drawdown_scaled),
            "largest_position_usd": _usd(metrics.peak_capital_at_risk_scaled),
            "stopped_out": metrics.stopped_out,
            "stop_out_rate_pct": _pct(metrics.stop_out_ppm),
            "held_to_settlement": metrics.settled_holds,
            "execution_parent_orders": metrics.execution_parents,
            "execution_fill_rate_pct": _pct(metrics.average_entry_fill_ppm),
            "execution_implementation_shortfall_usd": _usd(
                metrics.execution_implementation_shortfall_scaled
            ),
            "execution_non_fill_penalty_usd": _usd(metrics.execution_non_fill_penalty_scaled),
            "execution_adverse_selection_penalty_usd": _usd(
                metrics.execution_adverse_selection_penalty_scaled
            ),
            "execution_objective_usd": _usd(metrics.execution_objective_scaled),
            **_staking(run, compounding, balance_drawdown_ppm),
            **_entry_quality(params, entered),
        },
        "liquidity": _liquidity(results, metrics.average_entry_fill_ppm),
        "equity_curve": curve,
        "episodes": _episode_rows(results),
        "episodes_shown": min(len(results), MAX_EPISODE_ROWS),
        "excluded": dict(excluded),
        "request": request.model_dump(mode="json"),
        "params": params.model_dump(mode="json"),
        "realism": realism_name,
        "execution_policy": params.entry_execution_policy,
    }


def _staking(run: RunOutcome, compounding: bool, drawdown_ppm: int) -> dict[str, Any]:
    """Balance-relative figures, which only mean anything when compounding."""
    start = run.starting_balance_scaled
    return {
        "sizing": run.sizing,
        "starting_balance_usd": _usd(start) if compounding else None,
        "final_balance_usd": _usd(run.final_balance_scaled) if compounding else None,
        "total_return_pct": (
            round(100 * (run.final_balance_scaled - start) / start, 2)
            if compounding and start
            else None
        ),
        "max_drawdown_pct": _pct(drawdown_ppm) if compounding else None,
        "cash_awaiting_settlement_at_horizon_usd": (
            _usd(run.locked_balance_scaled) if compounding else None
        ),
        "cash_available_at_horizon_usd": (
            _usd(run.available_balance_scaled) if compounding else None
        ),
        "wallet_cashflow_events": len(run.wallet_events) if compounding else None,
        # A compounded run that runs out of money stops early; the untested
        # markets are not evidence of anything and must not read as a flat patch.
        "ended_early": run.ended_early,
        "markets_never_reached": run.episodes_selected - len(run.results),
    }


def _entry_quality(params: ThresholdHoldParams, entered: Sequence[EpisodeResult]) -> dict[str, Any]:
    """How close the entries came to the price the strategy asked for.

    "Buy when it reaches 0.85" is not "buy at 0.85": if the ask is already past the
    trigger when the window opens, the entry fires at that higher price, bounded
    only by the maximum-price setting.  Since the break-even strike rate of a
    binary bought at price *p* is exactly *p*, an entry that drifts from 0.85 to
    0.90 raises the bar the strategy must clear by five points — which is usually
    the whole explanation for a losing result with a high side-correct rate.
    """
    shares = sum(result.entry_shares_scaled for result in entered)
    if not shares:
        return {
            "average_entry_price": None,
            "entry_price_above_trigger": None,
            "break_even_win_rate_pct": None,
            "average_stake_on_wins_usd": None,
            "average_stake_on_losses_usd": None,
        }
    # Size weighted: a fully filled entry at 0.95 says more about the result than a
    # two-share entry at 0.85.
    average = sum(result.entry_notional_scaled for result in entered) * SHARE_SIZE_SCALE // shares
    trigger = params.entry_trigger_price_scaled
    wins = [result for result in entered if result.net_pnl_scaled > 0]
    losses = [result for result in entered if result.net_pnl_scaled < 0]
    return {
        "average_entry_price": _price(average),
        "entry_price_above_trigger": _price(average - trigger),
        "break_even_win_rate_pct": round(100 * average / POLYMARKET_PRICE_SCALE, 2),
        "average_stake_on_wins_usd": _usd(
            sum(result.entry_notional_scaled for result in wins) // len(wins) if wins else None
        ),
        "average_stake_on_losses_usd": _usd(
            sum(result.entry_notional_scaled for result in losses) // len(losses)
            if losses
            else None
        ),
    }


def _liquidity(results: Sequence[EpisodeResult], entry_fill_ppm: int) -> dict[str, Any]:
    """What the order book actually absorbed, as distinct from what was asked for.

    This is the section that answers "could I have traded this size?".  A strategy
    can show a fine win rate while filling a fraction of its intended stake, and
    the difference only appears here.
    """
    entry_children = [
        order for result in results for order in result.orders if order.kind == "entry"
    ]
    # A time-sliced parent can remain incomplete even when every submitted child
    # filled. Parent completion, not child acceptance, is the honest stake-level
    # liquidity measure. The child ledger remains useful for diagnosing rejects.
    parent_entries = [
        (
            result,
            result.execution_target_shares_scaled
            or sum(
                order.requested_shares_scaled for order in result.orders if order.kind == "entry"
            ),
        )
        for result in results
        if result.execution_target_shares_scaled
        or any(order.kind == "entry" for order in result.orders)
    ]
    exits = [
        order
        for result in results
        for order in result.orders
        if order.kind in {"stop_loss", "take_profit", "flatten"}
    ]
    requested_entry = sum(target for _, target in parent_entries)
    filled_entry = sum(result.entry_shares_scaled for result, _ in parent_entries)
    requested_exit = sum(order.requested_shares_scaled for order in exits)
    filled_exit = sum(order.filled_shares_scaled for order in exits)
    slippages = [
        result.stop_slippage_scaled for result in results if result.stop_slippage_scaled is not None
    ]
    partial_entries = [
        result
        for result, target in parent_entries
        if result.entry_shares_scaled and result.entry_shares_scaled < target
    ]
    return {
        "entry_orders": len(parent_entries),
        "entry_child_orders": len(entry_children),
        "entry_shares_requested": _shares(requested_entry),
        "entry_shares_filled": _shares(filled_entry),
        "entry_fill_rate_pct": _pct(entry_fill_ppm),
        "entry_orders_partially_filled": len(partial_entries),
        "entry_orders_rejected": sum(
            1 for result, target in parent_entries if target and not result.entry_shares_scaled
        ),
        "exit_orders": len(exits),
        "exit_shares_requested": _shares(requested_exit),
        "exit_shares_filled": _shares(filled_exit),
        "exit_fill_rate_pct": (
            _pct(filled_exit * 1_000_000 // requested_exit) if requested_exit else None
        ),
        "shares_left_unsold": _shares(
            sum(result.unfilled_exit_shares_scaled for result in results)
        ),
        "average_stop_slippage": _price(sum(slippages) // len(slippages) if slippages else None),
        "worst_stop_slippage": _price(max(slippages) if slippages else None),
        # Which realism rule cut each order short: displayed depth, the per-level
        # participation cap, printed volume, or the price limit itself.
        "binding_constraints": dict(
            Counter(
                order.binding_constraint
                for result in results
                for order in result.orders
                if order.binding_constraint
            )
        ),
        "rejection_reasons": dict(
            Counter(
                order.rejected_reason
                for result in results
                for order in result.orders
                if order.rejected_reason
            )
        ),
    }


def _episode_rows(results: Sequence[EpisodeResult]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for result in sorted(results, key=lambda item: item.start_utc_ns, reverse=True):
        if len(rows) >= MAX_EPISODE_ROWS:
            break
        entry = next((order for order in result.orders if order.kind == "entry"), None)
        rows.append(
            {
                "utc": _iso(result.start_utc_ns),
                "slug": result.market_slug,
                "winner": result.winner_outcome,
                "traded": result.entered,
                "side": result.outcome_side,
                "entry_price": _price(result.entry_price_scaled),
                "entry_minute": entry.decision_minute if entry else None,
                # A sequential policy emits several child orders.  The row is a
                # parent-level market result, so show the parent's intended size
                # rather than the first child's slice.
                "shares_requested": _shares(
                    result.execution_target_shares_scaled
                    or (entry.requested_shares_scaled if entry else None)
                ),
                "shares_filled": _shares(result.entry_shares_scaled),
                "stake_usd": _usd(result.entry_notional_scaled),
                "exit_reason": result.exit_reason,
                "exit_price": _price(result.exit_price_scaled),
                "settlement_usd": _usd(result.settlement_payout_scaled),
                "fees_usd": _usd(result.fees_scaled),
                "net_usd": _usd(result.net_pnl_scaled),
                "won": result.won,
                "unsold_shares": _shares(result.unfilled_exit_shares_scaled) or None,
                "worst_bid": _price(result.peak_adverse_bid_scaled),
                "skip_reason": result.skip_reason,
                "execution_policy": result.execution_policy,
                "execution_target_shares": _shares(result.execution_target_shares_scaled),
                "execution_unfilled_shares": _shares(result.execution_unfilled_shares_scaled),
                "execution_objective_usd": _usd(result.execution_objective_scaled),
                "marks": _marks(result),
            }
        )
    return rows


def _marks(result: EpisodeResult) -> list[dict[str, Any]]:
    """Where each fill landed, for plotting on the market's own price chart.

    Timed at arrival rather than decision: the decision is where the strategy
    looked, the arrival is where it traded, and the gap between them on a moving
    chart is the latency this simulator exists to model.
    """
    return [
        {
            "kind": order.kind,
            "side": result.outcome_side,
            "utc_ms": order.arrival_utc_ns // 1_000_000,
            "price": _price(order.average_price_scaled),
            "shares": _shares(order.filled_shares_scaled),
        }
        for order in result.orders
        if order.filled_shares_scaled and order.average_price_scaled is not None
    ]
