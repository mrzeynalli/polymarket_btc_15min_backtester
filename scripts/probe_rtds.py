#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from typing import Any

from websockets.asyncio.client import connect


async def probe(filter_value: str | None, seconds: float) -> dict[str, Any]:
    subscription: dict[str, str] = {"topic": "crypto_prices", "type": "update"}
    if filter_value is not None:
        subscription["filters"] = filter_value
    request = {"action": "subscribe", "subscriptions": [subscription]}
    counts: Counter[str] = Counter()
    symbols: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    async with connect(
        "wss://ws-live-data.polymarket.com",
        ping_interval=None,
        ping_timeout=None,
        close_timeout=3,
    ) as websocket:
        await websocket.send(json.dumps(request, separators=(",", ":")))
        deadline = asyncio.get_running_loop().time() + seconds
        next_ping = asyncio.get_running_loop().time() + 5
        while asyncio.get_running_loop().time() < deadline:
            timeout = min(deadline, next_ping) - asyncio.get_running_loop().time()
            try:
                frame = await asyncio.wait_for(websocket.recv(), timeout=max(timeout, 0.01))
            except TimeoutError:
                if asyncio.get_running_loop().time() >= next_ping:
                    await websocket.send("PING")
                    next_ping += 5
                continue
            raw = frame.decode() if isinstance(frame, bytes) else frame
            if not raw:
                counts["empty"] += 1
                continue
            message = json.loads(raw)
            topic = str(message.get("topic") or "unknown")
            message_type = str(message.get("type") or "unknown")
            counts[f"{topic}:{message_type}"] += 1
            payload = message.get("payload")
            if isinstance(payload, dict):
                symbol = str(payload.get("symbol") or "")
                if symbol:
                    symbols[symbol] += 1
                if len(examples) < 3:
                    examples.append(
                        {
                            "topic": topic,
                            "type": message_type,
                            "symbol": symbol,
                            "has_value": "value" in payload,
                            "history_points": len(payload.get("data", []))
                            if isinstance(payload.get("data"), list)
                            else 0,
                        }
                    )
    return {
        "request": request,
        "seconds": seconds,
        "counts": counts,
        "symbols": symbols,
        "examples": examples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe one anonymous Binance RTDS filter")
    parser.add_argument("--filter", help="Exact RTDS filter; omit for no filter")
    parser.add_argument("--seconds", type=float, default=8)
    arguments = parser.parse_args()
    print(json.dumps(asyncio.run(probe(arguments.filter, arguments.seconds)), indent=2))


if __name__ == "__main__":
    main()
