from __future__ import annotations

import random

from polymarket_bt.config import ReconnectConfig


class ReconnectPolicy:
    def __init__(self, config: ReconnectConfig, *, seed: int | None = None) -> None:
        self.config = config
        self.attempt = 0
        self.random = random.Random(seed)

    def reset(self) -> None:
        self.attempt = 0

    def next_delay(self) -> float:
        base = min(self.config.initial_seconds * (2**self.attempt), self.config.maximum_seconds)
        self.attempt += 1
        if self.config.jitter:
            return self.random.uniform(base * 0.5, base)
        return base
