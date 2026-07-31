from __future__ import annotations

from collections.abc import Sequence


def maximum_drawdown(values: Sequence[int]) -> int:
    if not values:
        return 0
    peak = values[0]
    drawdown = 0
    for value in values:
        peak = max(peak, value)
        drawdown = max(drawdown, peak - value)
    return drawdown


def percentile(values: Sequence[int], probability: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int((len(ordered) - 1) * probability)))
    return ordered[index]


def profit_factor(realized_changes: Sequence[int]) -> str | None:
    gains = sum(value for value in realized_changes if value > 0)
    losses = -sum(value for value in realized_changes if value < 0)
    if losses == 0:
        return None if gains == 0 else "Infinity"
    return f"{gains / losses:.8f}"
