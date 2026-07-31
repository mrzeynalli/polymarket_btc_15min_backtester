from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal

from polymarket_bt.config import FeeConfig
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE, USDC_SCALE

_ROUNDING = {
    "half_up": ROUND_HALF_UP,
    "floor": ROUND_FLOOR,
    "ceiling": ROUND_CEILING,
}


class FeeModel:
    def __init__(self, config: FeeConfig) -> None:
        self.config = config
        self.rate = Decimal(config.rate)
        self.minimum_scaled = int(
            (Decimal(config.minimum_fee) * USDC_SCALE).quantize(
                Decimal(1), rounding=_ROUNDING[config.rounding]
            )
        )

    def calculate(self, shares_scaled: int, price_scaled: int) -> int:
        if self.config.formula == "zero" or self.config.liquidity_role == "maker":
            return 0
        shares = Decimal(shares_scaled) / SHARE_SIZE_SCALE
        probability = Decimal(price_scaled) / POLYMARKET_PRICE_SCALE
        curve = (probability * (Decimal(1) - probability)) ** self.config.exponent
        raw_scaled = shares * self.rate * curve * USDC_SCALE
        fee = int(raw_scaled.quantize(Decimal(1), rounding=_ROUNDING[self.config.rounding]))
        if raw_scaled > 0:
            return max(fee, self.minimum_scaled)
        return 0
