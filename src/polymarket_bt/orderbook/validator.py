from __future__ import annotations

from dataclasses import dataclass

from polymarket_bt.constants import PRICE_MAX_SCALED, PRICE_MIN_SCALED


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    category: str
    message: str
    fatal: bool = True


def validate_level(
    price_scaled: int, size_scaled: int, tick_size_scaled: int
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if not PRICE_MIN_SCALED < price_scaled < PRICE_MAX_SCALED:
        issues.append(ValidationIssue("invalid_price", f"price outside (0,1): {price_scaled}"))
    if size_scaled < 0:
        issues.append(ValidationIssue("negative_size", f"negative size: {size_scaled}"))
    if tick_size_scaled <= 0 or price_scaled % tick_size_scaled:
        issues.append(
            ValidationIssue(
                "tick_noncompliance",
                f"price {price_scaled} is not aligned to tick {tick_size_scaled}",
            )
        )
    return issues


def validate_cross(best_bid: int | None, best_ask: int | None) -> list[ValidationIssue]:
    if best_bid is not None and best_ask is not None and best_bid >= best_ask:
        return [
            ValidationIssue("book_crossed", f"best bid {best_bid} is not below best ask {best_ask}")
        ]
    return []
