from __future__ import annotations

from polymarket_bt.orderbook.state import OrderBook


def microprice_scaled(book: OrderBook) -> int | None:
    bid = book.best_bid
    ask = book.best_ask
    if bid is None or ask is None:
        return None
    bid_size = book.bids[bid]
    ask_size = book.asks[ask]
    total = bid_size + ask_size
    if not total:
        return None
    return (ask * bid_size + bid * ask_size) // total


def executable_cost_scaled(book: OrderBook, shares_scaled: int) -> tuple[int, int]:
    remaining = shares_scaled
    cost = 0
    filled = 0
    for price, available in book.asks.items():
        take = min(remaining, available)
        cost += take * price // 1_000_000
        filled += take
        remaining -= take
        if remaining == 0:
            break
    return cost, filled


def executable_proceeds_scaled(book: OrderBook, shares_scaled: int) -> tuple[int, int]:
    remaining = shares_scaled
    proceeds = 0
    filled = 0
    for price, available in reversed(book.bids.items()):
        take = min(remaining, available)
        proceeds += take * price // 1_000_000
        filled += take
        remaining -= take
        if remaining == 0:
            break
    return proceeds, filled
