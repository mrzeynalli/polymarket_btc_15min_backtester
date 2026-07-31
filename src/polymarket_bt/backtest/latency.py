from __future__ import annotations

import math
import random

from polymarket_bt.config import LatencyConfig


class LatencyModel:
    version = "latency-v1"

    def __init__(self, config: LatencyConfig, *, seed: int) -> None:
        self.config = config
        self.random = random.Random(seed)

    def sample_ns(self) -> int:
        if self.config.model == "disabled":
            milliseconds = 0.0
        elif self.config.model == "constant":
            milliseconds = float(self.config.constant_ms)
        elif self.config.model == "empirical":
            if not self.config.samples_ms:
                raise ValueError("empirical latency requires samples_ms")
            milliseconds = float(self.random.choice(self.config.samples_ms))
        else:
            milliseconds = math.exp(
                self.random.normalvariate(self.config.lognormal_mu, self.config.lognormal_sigma)
            )
        return max(0, int(milliseconds * 1_000_000))

    def schedule(self, decision_timestamp_ns: int) -> int:
        return decision_timestamp_ns + self.sample_ns()
