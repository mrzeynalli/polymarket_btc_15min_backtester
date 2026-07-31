from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class QueueModel(StrEnum):
    OPTIMISTIC = "optimistic"
    CONSERVATIVE = "conservative"
    PROPORTIONAL = "proportional"


@dataclass(slots=True)
class MakerQueueState:
    order_id: str
    displayed_ahead_scaled: int
    simulated_size_scaled: int
    queue_model: QueueModel
    filled_scaled: int = 0

    def observe_trade(self, aggressive_trade_size_scaled: int) -> int:
        advance = min(self.displayed_ahead_scaled, aggressive_trade_size_scaled)
        self.displayed_ahead_scaled -= advance
        residual = aggressive_trade_size_scaled - advance
        fill = min(residual, self.simulated_size_scaled - self.filled_scaled)
        self.filled_scaled += fill
        return fill

    def observe_displayed_decrease(self, decrease_scaled: int, displayed_total_scaled: int) -> None:
        if self.queue_model == QueueModel.CONSERVATIVE:
            return
        if self.queue_model == QueueModel.OPTIMISTIC:
            self.displayed_ahead_scaled = max(0, self.displayed_ahead_scaled - decrease_scaled)
            return
        if displayed_total_scaled <= 0:
            return
        ahead_share = self.displayed_ahead_scaled * decrease_scaled // displayed_total_scaled
        self.displayed_ahead_scaled = max(0, self.displayed_ahead_scaled - ahead_share)
