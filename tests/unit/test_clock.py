from __future__ import annotations

from decimal import Decimal

import pytest

from polymarket_bt.clock import (
    PrecisionError,
    decimal_to_scaled,
    parse_timestamp_ns,
    scaled_to_decimal,
)


def test_decimal_scaling_is_lossless() -> None:
    assert decimal_to_scaled("0.531", 1_000_000) == 531_000
    assert decimal_to_scaled("12.345678", 1_000_000) == 12_345_678
    assert scaled_to_decimal(12_345_678, 1_000_000) == Decimal("12.345678")


def test_excess_precision_fails_closed() -> None:
    with pytest.raises(PrecisionError):
        decimal_to_scaled("0.1234567", 1_000_000)


def test_timestamp_units_and_iso() -> None:
    assert parse_timestamp_ns("1785525657") == 1_785_525_657_000_000_000
    assert parse_timestamp_ns("1785525657000") == 1_785_525_657_000_000_000
    assert parse_timestamp_ns("2026-07-31T19:20:57Z") == 1_785_525_657_000_000_000


def test_naive_iso_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="lacks timezone"):
        parse_timestamp_ns("2026-07-31T19:20:57")
