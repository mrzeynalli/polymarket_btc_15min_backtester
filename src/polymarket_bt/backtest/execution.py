from __future__ import annotations

import uuid
from decimal import ROUND_HALF_UP, Decimal

from polymarket_bt.backtest.fees import FeeModel
from polymarket_bt.backtest.portfolio import Portfolio
from polymarket_bt.constants import POLYMARKET_PRICE_SCALE, SHARE_SIZE_SCALE
from polymarket_bt.models.orders import OrderIntent, OrderResult, Side, SimulatedFill
from polymarket_bt.orderbook.state import OrderBook


def notional_scaled(size_scaled: int, price_scaled: int) -> int:
    raw = Decimal(size_scaled) * Decimal(price_scaled) / POLYMARKET_PRICE_SCALE
    return int(raw.quantize(Decimal(1), rounding=ROUND_HALF_UP))


class TakerExecutionSimulator:
    version = "depth-sweep-v1"

    def __init__(self, fee_model: FeeModel) -> None:
        self.fee_model = fee_model

    def execute(
        self,
        intent: OrderIntent,
        book: OrderBook,
        portfolio: Portfolio,
        *,
        arrival_time_ns: int,
    ) -> OrderResult:
        if not book.valid:
            return OrderResult(
                intent=intent, status="rejected", rejection_reason="book unavailable or invalid"
            )
        if intent.requested_shares_scaled is None and intent.requested_notional_scaled is None:
            return OrderResult(
                intent=intent, status="rejected", rejection_reason="size or notional required"
            )
        # Depth already taken by earlier simulated fills is excluded, but the
        # reconstructed book itself is left untouched so the recorded feed stays
        # authoritative for later updates.
        levels = book.available_levels("SELL" if intent.side == Side.BUY else "BUY")
        shares_remaining = intent.requested_shares_scaled
        notional_remaining = intent.requested_notional_scaled
        planned: list[tuple[int, int, int, int, int]] = []
        for rank, (price, displayed_size) in enumerate(levels, start=1):
            if intent.limit_price_scaled is not None:
                if intent.side == Side.BUY and price > intent.limit_price_scaled:
                    break
                if intent.side == Side.SELL and price < intent.limit_price_scaled:
                    break
            if shares_remaining is not None:
                desired = min(shares_remaining, displayed_size)
            else:
                assert notional_remaining is not None
                desired = min(
                    displayed_size,
                    notional_remaining * SHARE_SIZE_SCALE // max(price, 1),
                )
            if desired <= 0:
                break
            notional = notional_scaled(desired, price)
            displayed_after = displayed_size - desired
            planned.append((rank, price, desired, notional, displayed_after))
            if shares_remaining is not None:
                shares_remaining -= desired
            if notional_remaining is not None:
                notional_remaining = max(0, notional_remaining - notional)
            if (shares_remaining is not None and shares_remaining == 0) or (
                notional_remaining is not None and notional_remaining == 0
            ):
                break

        if intent.side == Side.BUY:
            required_cash = sum(
                notional + self.fee_model.calculate(size, price)
                for _, price, size, notional, _ in planned
            )
            if required_cash > portfolio.available_cash_scaled:
                return OrderResult(
                    intent=intent,
                    status="rejected",
                    rejection_reason="insufficient available cash",
                )
        else:
            required_inventory = sum(size for _, _, size, _, _ in planned)
            if required_inventory > portfolio.inventory(intent.token_id):
                return OrderResult(
                    intent=intent,
                    status="rejected",
                    rejection_reason="insufficient inventory",
                )

        fills: list[SimulatedFill] = []
        for rank, price, desired, notional, displayed_after in planned:
            fee = self.fee_model.calculate(desired, price)
            fill = SimulatedFill(
                fill_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"{intent.order_intent_id}:{rank}:{price}:{desired}",
                    )
                ),
                order_intent_id=intent.order_intent_id,
                condition_id=intent.condition_id,
                token_id=intent.token_id,
                side=intent.side,
                price_scaled=price,
                size_scaled=desired,
                notional_scaled=notional,
                fee_scaled=fee,
                book_event_sequence=book.last_sequence,
                decision_time_ns=intent.decision_timestamp_ns,
                scheduled_arrival_time_ns=arrival_time_ns,
                fill_time_ns=arrival_time_ns,
                level_rank=rank,
                liquidity_remaining_after_scaled=displayed_after,
            )
            portfolio.apply_fill(fill, outcome=intent.outcome)
            fills.append(fill)
            book.consume("SELL" if intent.side == Side.BUY else "BUY", price, desired)
        if not fills:
            return OrderResult(
                intent=intent,
                status="rejected",
                rejection_reason="no executable liquidity/resources",
            )
        filled_size = sum(fill.size_scaled for fill in fills)
        fully_filled = (
            intent.requested_shares_scaled is not None
            and filled_size == intent.requested_shares_scaled
        ) or (
            intent.requested_notional_scaled is not None
            and sum(fill.notional_scaled for fill in fills) >= intent.requested_notional_scaled
        )
        return OrderResult(
            intent=intent,
            status="filled" if fully_filled else "partial",
            fills=tuple(fills),
        )
