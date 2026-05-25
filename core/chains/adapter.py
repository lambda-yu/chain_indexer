from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from core.chains.types import Block, BlockHeader, Log


@runtime_checkable
class ChainAdapter(Protocol):
    chain_id: str
    confirmations: int

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_latest_block_number(self) -> int: ...
    async def fetch_block(self, number: int) -> Block: ...
    async def fetch_logs(
        self, from_block: int, to_block: int, addresses: list[str] | None = None
    ) -> list[Log]: ...
    def subscribe_heads(self) -> AsyncIterator[BlockHeader]: ...
    # NOTE: subscribe_heads is a regular (non-async) function returning an
    # AsyncIterator. Callers iterate with `async for`; do NOT `await` the call.
