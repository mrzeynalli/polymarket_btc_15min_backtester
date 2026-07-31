from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Final

from dateutil.parser import isoparse

NANOSECONDS_PER_SECOND: Final = 1_000_000_000


class PrecisionError(ValueError):
    """A source decimal cannot be represented at the selected fixed scale."""


def utc_now_ns() -> int:
    return time.time_ns()


def monotonic_now_ns() -> int:
    return time.monotonic_ns()


def utc_iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / NANOSECONDS_PER_SECOND, tz=UTC).isoformat()


def parse_timestamp_ns(value: str | int | Decimal | None) -> int | None:
    """Parse Unix seconds/ms/us/ns or an ISO-8601 timestamp without silently correcting it."""
    if value is None or value == "":
        return None
    if isinstance(value, str) and not value.strip().replace(".", "", 1).isdigit():
        parsed = isoparse(value)
        if parsed.tzinfo is None:
            raise ValueError(f"timestamp lacks timezone: {value!r}")
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = parsed.astimezone(UTC) - epoch
        return (
            delta.days * 86_400 * NANOSECONDS_PER_SECOND
            + delta.seconds * NANOSECONDS_PER_SECOND
            + delta.microseconds * 1_000
        )
    try:
        numeric = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc
    absolute = abs(numeric)
    if absolute < Decimal("100000000000"):
        multiplier = Decimal(NANOSECONDS_PER_SECOND)
    elif absolute < Decimal("100000000000000"):
        multiplier = Decimal(1_000_000)
    elif absolute < Decimal("100000000000000000"):
        multiplier = Decimal(1_000)
    else:
        multiplier = Decimal(1)
    result = numeric * multiplier
    if result != result.to_integral_value():
        raise PrecisionError(f"timestamp exceeds nanosecond precision: {value!r}")
    return int(result)


def decimal_to_scaled(value: str | int | Decimal, scale: int, *, field: str = "value") -> int:
    try:
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal for {field}: {value!r}") from exc
    if not decimal.is_finite():
        raise ValueError(f"non-finite decimal for {field}: {value!r}")
    scaled = decimal * scale
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise PrecisionError(f"{field}={value!r} exceeds supported precision for scale {scale}")
    return int(integral)


def scaled_to_decimal(value: int, scale: int) -> Decimal:
    return Decimal(value) / Decimal(scale)


def multiply_scaled(left: int, right: int, right_scale: int) -> int:
    """Multiply fixed-scale values using floor-free exact integer arithmetic."""
    product = left * right
    quotient, remainder = divmod(product, right_scale)
    if remainder:
        raise PrecisionError("fixed-scale multiplication exceeds output precision")
    return quotient
