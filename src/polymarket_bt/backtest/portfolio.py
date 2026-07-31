from __future__ import annotations

from dataclasses import dataclass

from polymarket_bt.models.orders import Side, SimulatedFill
from polymarket_bt.models.results import PortfolioSnapshot


@dataclass(slots=True)
class Position:
    condition_id: str
    outcome: str
    shares_scaled: int = 0
    cost_basis_scaled: int = 0

    @property
    def average_cost_price_scaled(self) -> int | None:
        if not self.shares_scaled:
            return None
        return self.cost_basis_scaled * 1_000_000 // self.shares_scaled


class Portfolio:
    def __init__(
        self,
        starting_cash_scaled: int,
        *,
        allow_negative_cash: bool = False,
        allow_short_positions: bool = False,
    ) -> None:
        self.starting_cash_scaled = starting_cash_scaled
        self.available_cash_scaled = starting_cash_scaled
        self.reserved_cash_scaled = 0
        self.allow_negative_cash = allow_negative_cash
        self.allow_short_positions = allow_short_positions
        self.positions: dict[str, Position] = {}
        self.realized_pnl_scaled = 0
        self.unrealized_pnl_scaled = 0
        self.fees_paid_scaled = 0
        self.settlement_receivable_scaled = 0
        self.settled_cash_scaled = 0
        self.events: list[dict[str, int | str]] = []

    def inventory(self, token_id: str) -> int:
        position = self.positions.get(token_id)
        return position.shares_scaled if position else 0

    def apply_fill(self, fill: SimulatedFill, *, outcome: str) -> None:
        position = self.positions.setdefault(
            fill.token_id, Position(condition_id=fill.condition_id, outcome=outcome)
        )
        if fill.side == Side.BUY:
            debit = fill.notional_scaled + fill.fee_scaled
            if not self.allow_negative_cash and debit > self.available_cash_scaled:
                raise ValueError("insufficient available cash")
            self.available_cash_scaled -= debit
            position.shares_scaled += fill.size_scaled
            position.cost_basis_scaled += fill.notional_scaled
        else:
            if not self.allow_short_positions and fill.size_scaled > position.shares_scaled:
                raise ValueError("insufficient inventory")
            old_shares = position.shares_scaled
            removed_cost = (
                position.cost_basis_scaled * fill.size_scaled // old_shares if old_shares else 0
            )
            position.shares_scaled -= fill.size_scaled
            position.cost_basis_scaled -= removed_cost
            net_proceeds = fill.notional_scaled - fill.fee_scaled
            self.available_cash_scaled += net_proceeds
            self.realized_pnl_scaled += net_proceeds - removed_cost
        self.fees_paid_scaled += fill.fee_scaled
        self.events.append(
            {
                "timestamp_ns": fill.fill_time_ns,
                "event_type": "fill",
                "token_id": fill.token_id,
                "cash_scaled": self.available_cash_scaled,
                "inventory_scaled": position.shares_scaled,
                "realized_pnl_scaled": self.realized_pnl_scaled,
                "fees_scaled": self.fees_paid_scaled,
            }
        )

    def settle_market(
        self,
        condition_id: str,
        winning_token_id: str,
        timestamp_ns: int,
    ) -> int:
        payout = 0
        for token_id, position in self.positions.items():
            if position.condition_id != condition_id or position.shares_scaled == 0:
                continue
            token_payout = position.shares_scaled if token_id == winning_token_id else 0
            payout += token_payout
            self.realized_pnl_scaled += token_payout - position.cost_basis_scaled
            position.shares_scaled = 0
            position.cost_basis_scaled = 0
        self.settlement_receivable_scaled += payout
        self.available_cash_scaled += payout
        self.settled_cash_scaled += payout
        self.settlement_receivable_scaled -= payout
        self.events.append(
            {
                "timestamp_ns": timestamp_ns,
                "event_type": "settlement",
                "token_id": winning_token_id,
                "cash_scaled": self.available_cash_scaled,
                "inventory_scaled": 0,
                "realized_pnl_scaled": self.realized_pnl_scaled,
                "fees_scaled": self.fees_paid_scaled,
            }
        )
        return payout

    def mark_to_market(self, prices: dict[str, int]) -> int:
        unrealized = 0
        for token_id, position in self.positions.items():
            price = prices.get(token_id)
            if price is not None:
                value = position.shares_scaled * price // 1_000_000
                unrealized += value - position.cost_basis_scaled
        self.unrealized_pnl_scaled = unrealized
        return unrealized

    def equity_scaled(self, prices: dict[str, int] | None = None) -> int:
        marked_value = 0
        if prices:
            for token_id, position in self.positions.items():
                if token_id in prices:
                    marked_value += position.shares_scaled * prices[token_id] // 1_000_000
        return self.available_cash_scaled + marked_value + self.settlement_receivable_scaled

    def snapshot(self, timestamp_ns: int) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp_ns=timestamp_ns,
            available_cash_scaled=self.available_cash_scaled,
            reserved_cash_scaled=self.reserved_cash_scaled,
            realized_pnl_scaled=self.realized_pnl_scaled,
            unrealized_pnl_scaled=self.unrealized_pnl_scaled,
            fees_paid_scaled=self.fees_paid_scaled,
            settlement_receivable_scaled=self.settlement_receivable_scaled,
            inventory={
                token_id: position.shares_scaled for token_id, position in self.positions.items()
            },
        )
