from __future__ import annotations

import asyncio
from typing import Protocol


class ReferencePriceSource(Protocol):
    """Public reference-price source contract for future feed adapters."""

    async def run(self, stop: asyncio.Event) -> None: ...

    async def close(self) -> None: ...
