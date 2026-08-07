"""Aggregation, parameter sweeps, and walk-forward evaluation.

Three things make the difference between a sweep that finds an edge and one that
manufactures the appearance of an edge:

**A shared bankroll in time order.** Episodes are consecutive 15-minute markets,
so results are accumulated along a single equity curve with settlement lag, not
averaged as if each trade had its own fresh capital.  Drawdown and capital
utilisation are only meaningful this way.

**Robustness bands, not point estimates.** Every variant is evaluated under each
execution-realism preset.  A variant that is profitable only under `optimistic`
has not been shown to work.

**Walk-forward.** Selecting the best of many variants on all the data measures the
sweep's ability to fit noise.  Variants are chosen on an in-sample fold and scored
on the next, unseen fold, and the gap between the two is reported as the honest
estimate of what selection bought.

With 90 episodes a sweep is a hypothesis generator, not a decision procedure.  The
report states the sample size next to every number for that reason.
"""

from __future__ import annotations

import itertools
import json
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from polymarket_bt.backtest.episodes import Episode
from polymarket_bt.backtest.fastsim import (
    EpisodeResult,
    ThresholdHoldSimulator,
    strategy_fingerprint,
)
from polymarket_bt.backtest.realism import ExecutionRealism, preset
from polymarket_bt.backtest.tape import load_tape
from polymarket_bt.backtest.threshold_hold import ThresholdHoldParams
from polymarket_bt.constants import USDC_SCALE

SWEEP_VERSION = "sweep-v1"


@dataclass(frozen=True, slots=True)
class Variant:
    """One strategy configuration evaluated under one realism preset."""

    params: ThresholdHoldParams
    realism: ExecutionRealism

    @property
    def label(self) -> str:
        return f"{self.params.label}|{self.realism.name}"


@dataclass(slots=True)
class VariantMetrics:
    """Everything needed to judge one variant, including what it failed to do."""

    label: str
    realism: str
    episodes: int
    entries: int
    entry_rate_ppm: int
    side_correct: int
    side_correct_ppm: int
    profitable_trades: int
    win_rate_ppm: int
    net_pnl_scaled: int
    gross_pnl_scaled: int
    fees_scaled: int
    average_win_scaled: int
    average_loss_scaled: int
    profit_factor_ppm: int | None
    max_drawdown_scaled: int
    peak_capital_at_risk_scaled: int
    stopped_out: int
    stop_out_ppm: int
    stops_unfilled: int
    unfilled_exit_shares_scaled: int
    average_stop_slippage_scaled: int | None
    average_entry_fill_ppm: int
    execution_parents: int
    execution_target_shares_scaled: int
    execution_unfilled_shares_scaled: int
    execution_implementation_shortfall_scaled: int | None
    execution_non_fill_penalty_scaled: int | None
    execution_adverse_selection_penalty_scaled: int | None
    execution_objective_scaled: int | None
    settled_holds: int
    net_pnl_per_entry_scaled: int
    params: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["params"] = json.dumps(self.params, sort_keys=True)
        return row


def _ppm(numerator: int, denominator: int) -> int:
    return numerator * 1_000_000 // denominator if denominator else 0


def aggregate(
    label: str,
    realism_name: str,
    results: Sequence[EpisodeResult],
    *,
    params: dict[str, Any] | None = None,
) -> VariantMetrics:
    """Collapse per-episode results into one variant's metrics.

    Results must be in episode-start order: the equity curve, drawdown, and
    capital-at-risk figures are path dependent.
    """
    entered = [result for result in results if result.entered]
    wins = [result for result in entered if result.net_pnl_scaled > 0]
    losses = [result for result in entered if result.net_pnl_scaled < 0]
    stopped = [
        result
        for result in entered
        if result.exit_reason in {"stop_loss", "stop_unfilled_held_to_settlement"}
    ]
    unfilled = [result for result in entered if result.unfilled_exit_shares_scaled > 0]
    slippages = [
        result.stop_slippage_scaled for result in entered if result.stop_slippage_scaled is not None
    ]
    # Sequential policies can wait or stop sending children before the parent is
    # complete. Measuring only submitted children can therefore report a 100%
    # fill while half the intended parent was never acquired. New results carry
    # the parent target explicitly; the ledger fallback keeps older reports and
    # fixtures readable.
    requested = sum(
        result.execution_target_shares_scaled
        or sum(order.requested_shares_scaled for order in result.orders if order.kind == "entry")
        for result in results
    )
    filled = sum(result.entry_shares_scaled for result in results)
    scored_execution = [
        result for result in results if result.execution_objective_scaled is not None
    ]

    equity = 0
    peak = 0
    drawdown = 0
    exposure = 0
    for result in entered:
        exposure = max(exposure, result.entry_notional_scaled)
        equity += result.net_pnl_scaled
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)

    gross_wins = sum(result.net_pnl_scaled for result in wins)
    gross_losses = -sum(result.net_pnl_scaled for result in losses)
    return VariantMetrics(
        label=label,
        realism=realism_name,
        episodes=len(results),
        entries=len(entered),
        entry_rate_ppm=_ppm(len(entered), len(results)),
        side_correct=sum(1 for result in entered if result.won),
        side_correct_ppm=_ppm(sum(1 for result in entered if result.won), len(entered)),
        profitable_trades=len(wins),
        win_rate_ppm=_ppm(len(wins), len(entered)),
        net_pnl_scaled=sum(result.net_pnl_scaled for result in entered),
        gross_pnl_scaled=sum(result.gross_pnl_scaled for result in entered),
        fees_scaled=sum(result.fees_scaled for result in entered),
        average_win_scaled=gross_wins // len(wins) if wins else 0,
        average_loss_scaled=gross_losses // len(losses) if losses else 0,
        profit_factor_ppm=_ppm(gross_wins, gross_losses) if gross_losses else None,
        max_drawdown_scaled=drawdown,
        peak_capital_at_risk_scaled=exposure,
        stopped_out=len(stopped),
        stop_out_ppm=_ppm(len(stopped), len(entered)),
        stops_unfilled=len(unfilled),
        unfilled_exit_shares_scaled=sum(result.unfilled_exit_shares_scaled for result in unfilled),
        average_stop_slippage_scaled=(sum(slippages) // len(slippages) if slippages else None),
        # Share of the size the strategy asked for that the book actually gave it.
        average_entry_fill_ppm=_ppm(filled, requested) if requested else 0,
        execution_parents=sum(1 for result in results if result.execution_target_shares_scaled > 0),
        execution_target_shares_scaled=requested,
        execution_unfilled_shares_scaled=max(0, requested - filled),
        execution_implementation_shortfall_scaled=(
            sum(
                result.execution_implementation_shortfall_scaled or 0 for result in scored_execution
            )
            if scored_execution
            else None
        ),
        execution_non_fill_penalty_scaled=(
            sum(result.execution_non_fill_penalty_scaled or 0 for result in scored_execution)
            if scored_execution
            else None
        ),
        execution_adverse_selection_penalty_scaled=(
            sum(
                result.execution_adverse_selection_penalty_scaled or 0
                for result in scored_execution
            )
            if scored_execution
            else None
        ),
        execution_objective_scaled=(
            sum(result.execution_objective_scaled or 0 for result in scored_execution)
            if scored_execution
            else None
        ),
        settled_holds=sum(1 for result in entered if result.exit_reason == "settlement"),
        net_pnl_per_entry_scaled=(
            sum(result.net_pnl_scaled for result in entered) // len(entered) if entered else 0
        ),
        params=params or {},
    )


class SweepRunner:
    """Evaluates many variants over a fixed episode set, loading each tape once."""

    def __init__(self, workspace: Path, episodes: Sequence[Episode], *, seed: int = 1729) -> None:
        self.workspace = workspace
        self.episodes = [episode for episode in episodes if episode.eligible]
        self.episodes.sort(key=lambda episode: episode.start_utc_ns)
        self.seed = seed

    def run(
        self, variants: Sequence[Variant]
    ) -> tuple[list[VariantMetrics], dict[str, list[EpisodeResult]]]:
        by_variant: dict[str, list[EpisodeResult]] = {variant.label: [] for variant in variants}
        for episode in self.episodes:
            tape = load_tape(self.workspace, episode)
            for variant in variants:
                simulator = ThresholdHoldSimulator(variant.params, variant.realism, seed=self.seed)
                by_variant[variant.label].append(simulator.run(tape))
        metrics = [
            aggregate(
                variant.params.label,
                variant.realism.name,
                by_variant[variant.label],
                params=variant.params.model_dump(mode="json"),
            )
            for variant in variants
        ]
        return metrics, by_variant


def grid(
    *,
    entry_windows: Iterable[tuple[float, float]],
    entry_triggers_scaled: Iterable[int],
    stop_loss_prices_scaled: Iterable[int | None],
    order_shares_scaled: Iterable[int],
    entry_limit_offset_scaled: int = 50_000,
    realism_names: Iterable[str] = ("pessimistic", "base", "optimistic"),
    **fixed: Any,
) -> list[Variant]:
    """Cartesian product of the parameters that define this strategy family."""
    presets = {name: preset(name) for name in realism_names}  # type: ignore[arg-type]
    variants: list[Variant] = []
    for (start, end), trigger, stop, size, name in itertools.product(
        entry_windows,
        entry_triggers_scaled,
        stop_loss_prices_scaled,
        order_shares_scaled,
        realism_names,
    ):
        if stop is not None and stop >= trigger:
            continue
        # An arming minute is meaningless without a stop, and the parameter model
        # rejects the combination, so hold-to-settlement variants simply drop it.
        extra = dict(fixed)
        if stop is None:
            extra.pop("stop_loss_from_minute", None)
        params = ThresholdHoldParams(
            entry_from_minute=start,
            entry_to_minute=end,
            entry_trigger_price_scaled=trigger,
            entry_limit_price_scaled=min(1_000_000, trigger + entry_limit_offset_scaled),
            stop_loss_price_scaled=stop,
            order_shares_scaled=size,
            **extra,
        )
        variants.append(Variant(params=params, realism=presets[name]))
    return variants


@dataclass(slots=True)
class WalkForwardFold:
    fold: int
    train_episodes: int
    test_episodes: int
    selected_label: str
    train_net_pnl_scaled: int
    test_net_pnl_scaled: int
    test_entries: int
    test_win_rate_ppm: int


def walk_forward(
    workspace: Path,
    episodes: Sequence[Episode],
    variants: Sequence[Variant],
    *,
    folds: int = 3,
    seed: int = 1729,
    selection_realism: str = "base",
) -> list[WalkForwardFold]:
    """Select on past episodes, score on the next unseen block, repeat.

    The reported test P&L is the only number here that estimates future
    performance; the training number is the in-sample fit it was chosen by.
    """
    ordered = sorted(
        (episode for episode in episodes if episode.eligible),
        key=lambda episode: episode.start_utc_ns,
    )
    if folds < 2 or len(ordered) < folds * 2:
        raise ValueError("not enough episodes for the requested number of folds")
    block = len(ordered) // (folds + 1)
    candidates = [variant for variant in variants if variant.realism.name == selection_realism]
    if not candidates:
        raise ValueError(f"no variants use realism preset {selection_realism}")
    outcomes: list[WalkForwardFold] = []
    for fold in range(folds):
        train = ordered[: block * (fold + 1)]
        test = ordered[block * (fold + 1) : block * (fold + 2)]
        if not test:
            break
        train_metrics, _ = SweepRunner(workspace, train, seed=seed).run(candidates)
        best_index = max(
            range(len(train_metrics)),
            key=lambda index: train_metrics[index].net_pnl_scaled,
        )
        best = candidates[best_index]
        test_metrics, _ = SweepRunner(workspace, test, seed=seed).run([best])
        outcomes.append(
            WalkForwardFold(
                fold=fold,
                train_episodes=len(train),
                test_episodes=len(test),
                selected_label=best.label,
                train_net_pnl_scaled=train_metrics[best_index].net_pnl_scaled,
                test_net_pnl_scaled=test_metrics[0].net_pnl_scaled,
                test_entries=test_metrics[0].entries,
                test_win_rate_ppm=test_metrics[0].win_rate_ppm,
            )
        )
    return outcomes


def usd(scaled: int) -> str:
    return f"{scaled / USDC_SCALE:,.2f}"


def percent(ppm: int | None) -> str:
    return "n/a" if ppm is None else f"{ppm / 10_000:.1f}%"


def markdown_report(
    metrics: Sequence[VariantMetrics],
    *,
    title: str,
    episodes: Sequence[Episode],
    fingerprint: dict[str, Any],
    folds: Sequence[WalkForwardFold] = (),
) -> str:
    """Render a report that leads with the constraints, not the headline P&L."""
    ordered = sorted(metrics, key=lambda item: item.net_pnl_scaled, reverse=True)
    start = min((episode.start_utc_ns for episode in episodes), default=0)
    end = max((episode.end_utc_ns for episode in episodes), default=0)
    lines = [
        f"# {title}",
        "",
        f"- Episodes: **{len(episodes)}** eligible 15-minute markets",
        f"- Span: {start} to {end} (UTC ns)",
        f"- Sweep version: `{SWEEP_VERSION}`; simulator `{fingerprint.get('simulator_version')}`;"
        f" execution `{fingerprint.get('execution_model_version')}`",
        "",
        "Sample size is small. Treat every ranking below as a hypothesis to re-test as",
        "the collector accumulates more markets, not as a validated edge.",
        "",
        "## Variants",
        "",
        "| variant | realism | entries | parent fill | execution objective | side correct |"
        " profitable | net P&L | fees | stop-outs | unfilled exits | avg stop slip |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in ordered:
        slip = (
            "n/a"
            if item.average_stop_slippage_scaled is None
            else f"{item.average_stop_slippage_scaled / 10_000:.2f}c"
        )
        lines.append(
            f"| `{item.label}` | {item.realism} | {item.entries} |"
            f" {percent(item.average_entry_fill_ppm)} |"
            f" {'n/a' if item.execution_objective_scaled is None else usd(item.execution_objective_scaled)} |"
            f" {percent(item.side_correct_ppm)} | {percent(item.win_rate_ppm)} |"
            f" {usd(item.net_pnl_scaled)} | {usd(item.fees_scaled)} |"
            f" {percent(item.stop_out_ppm)} | {item.stops_unfilled} | {slip} |"
        )
    if folds:
        lines += [
            "",
            "## Walk-forward",
            "",
            "In-sample selection versus the next unseen block. A large gap between the",
            "two columns is the cost of choosing parameters on the same data.",
            "",
            "| fold | train episodes | test episodes | selected | train P&L | test P&L |"
            " test entries | test win rate |",
            "|---:|---:|---:|---|---:|---:|---:|---:|",
        ]
        for fold in folds:
            lines.append(
                f"| {fold.fold} | {fold.train_episodes} | {fold.test_episodes} |"
                f" `{fold.selected_label}` | {usd(fold.train_net_pnl_scaled)} |"
                f" {usd(fold.test_net_pnl_scaled)} | {fold.test_entries} |"
                f" {percent(fold.test_win_rate_ppm)} |"
            )
    return "\n".join(lines) + "\n"


def write_sweep_artifacts(
    directory: Path,
    metrics: Sequence[VariantMetrics],
    per_episode: dict[str, list[EpisodeResult]],
    *,
    episodes: Sequence[Episode],
    variants: Sequence[Variant],
    seed: int,
    folds: Sequence[WalkForwardFold] = (),
    title: str = "Threshold-and-hold sweep",
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    fingerprint = strategy_fingerprint(variants[0].params, variants[0].realism, seed)
    (directory / "variants.json").write_text(
        json.dumps([item.to_row() for item in metrics], indent=2, sort_keys=True), encoding="utf-8"
    )
    (directory / "episodes.json").write_text(
        json.dumps(
            {
                label: [result.to_row() for result in results]
                for label, results in per_episode.items()
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        encoding="utf-8",
    )
    report = markdown_report(
        metrics, title=title, episodes=episodes, fingerprint=fingerprint, folds=folds
    )
    path = directory / "report.md"
    path.write_text(report, encoding="utf-8")
    (directory / "fingerprint.json").write_text(
        json.dumps(fingerprint, indent=2, sort_keys=True), encoding="utf-8"
    )
    return path
