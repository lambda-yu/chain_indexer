from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any, cast

from web3 import AsyncHTTPProvider, AsyncWeb3, WebSocketProvider
from web3.types import FilterParams, TxData
from web3.utils.subscriptions import (
    NewHeadsSubscription,
    NewHeadsSubscriptionContext,
)

from core.chains.types import Block, BlockHeader, Log, Tx


def _hexify(v: object) -> str:
    """Normalize HexBytes-or-string values to a 0x-prefixed hex string.

    web3.py returns ``HexBytes`` for hash-like fields on HTTP, but eth_subscribe
    over WS yields plain hex strings. This helper accepts both.
    """
    if isinstance(v, str):
        return v
    hex_attr = getattr(v, "hex", None)
    if callable(hex_attr):
        s = str(hex_attr())
        return s if s.startswith("0x") else "0x" + s
    return str(v)


def _intify(v: object) -> int:
    """Accept ints or 0x-hex strings (eth_subscribe yields hex)."""
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return int(v, 16) if v.startswith("0x") else int(v)
    # Fallback for SupportsInt-like objects.
    return int(str(v), 0)


class EvmAdapter:
    chain_id: str
    confirmations: int

    def __init__(
        self,
        *,
        chain_id: str,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
    ) -> None:
        self.chain_id = chain_id
        self.confirmations = confirmations
        self._rpc_http = rpc_http
        self._rpc_ws = rpc_ws
        self._w3: AsyncWeb3[Any] | None = None

    async def connect(self) -> None:
        self._w3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_http))
        # Sanity ping; raises if the RPC is unreachable.
        await self._w3.eth.block_number

    async def disconnect(self) -> None:
        if self._w3 is not None:
            # AsyncHTTPProvider holds an aiohttp session. Closing it explicitly
            # avoids "Unclosed client session" warnings.
            with contextlib.suppress(Exception):  # best-effort cleanup
                await self._w3.provider.disconnect()
            self._w3 = None

    async def get_latest_block_number(self) -> int:
        assert self._w3 is not None
        return int(await self._w3.eth.block_number)

    async def fetch_block(self, number: int) -> Block:
        """Fetch block header + transactions. Logs are NOT embedded; callers
        (ChainListener / ConfirmationBuffer) call ``fetch_logs`` separately to
        avoid double RPC cost when batching."""
        assert self._w3 is not None
        raw = await self._w3.eth.get_block(number, full_transactions=True)
        header = BlockHeader(
            number=int(raw["number"]),
            hash=_hexify(raw["hash"]),
            parent_hash=_hexify(raw["parentHash"]),
            timestamp=int(raw["timestamp"]),
        )
        txs: list[Tx] = []
        # full_transactions=True guarantees TxData dicts (not HexBytes hashes).
        raw_txs: list[TxData] = list(raw.get("transactions", []))  # type: ignore[arg-type]
        for t in raw_txs:
            txs.append(
                Tx(
                    hash=_hexify(t["hash"]),
                    index=int(t.get("transactionIndex", 0) or 0),
                    from_addr=str(t["from"]),
                    to_addr=str(t["to"]) if t.get("to") else None,
                    value=int(t.get("value", 0) or 0),
                    input=_hexify(t.get("input", "0x")),
                    # M1: status is not fetched (would require per-tx receipts).
                    # Downstream parsers in M1 don't depend on status. M2 will
                    # add an optional receipts-fetch pass for failed-tx filtering.
                    status=1,
                )
            )
        return Block(header=header, txs=txs, logs=[])

    async def fetch_logs(
        self, from_block: int, to_block: int, addresses: list[str] | None = None
    ) -> list[Log]:
        assert self._w3 is not None
        params: dict[str, Any] = {"fromBlock": from_block, "toBlock": to_block}
        if addresses:
            params["address"] = addresses
        # FilterParams is a TypedDict; the runtime dict matches its shape.
        raw_logs = await self._w3.eth.get_logs(cast(FilterParams, params))
        out: list[Log] = []
        for lg in raw_logs:
            out.append(
                Log(
                    tx_hash=_hexify(lg["transactionHash"]),
                    log_index=int(lg["logIndex"]),
                    address=str(lg["address"]),
                    topics=[_hexify(t) for t in lg["topics"]],
                    data=lg["data"] if isinstance(lg["data"], str) else _hexify(lg["data"]),
                )
            )
        return out

    def subscribe_heads(self) -> AsyncIterator[BlockHeader]:
        """Return an async iterator of new heads. WS if configured, else poll.

        This is a regular (non-async) function so callers can do
        ``async for h in adapter.subscribe_heads():`` without ``await``.
        """
        if self._rpc_ws:
            return self._subscribe_heads_ws()
        return self._poll_heads()

    async def _poll_heads(self) -> AsyncIterator[BlockHeader]:
        assert self._w3 is not None
        last = -1
        while True:
            n = int(await self._w3.eth.block_number)
            if n > last:
                raw = await self._w3.eth.get_block(n)
                yield BlockHeader(
                    number=int(raw["number"]),
                    hash=_hexify(raw["hash"]),
                    parent_hash=_hexify(raw["parentHash"]),
                    timestamp=int(raw["timestamp"]),
                )
                last = n
            await asyncio.sleep(1.0)

    async def _subscribe_heads_ws(self) -> AsyncIterator[BlockHeader]:
        """WS subscription via web3.py v7 handler + queue bridge.

        v7 exposes WS subscriptions through a handler callback driven by
        ``subscription_manager.handle_subscriptions(run_forever=True)``. We
        push each head into an ``asyncio.Queue`` and yield from it so callers
        get the AsyncIterator[BlockHeader] interface the protocol promises.
        """
        assert self._rpc_ws is not None
        queue: asyncio.Queue[BlockHeader] = asyncio.Queue()

        async def _on_head(ctx: NewHeadsSubscriptionContext) -> None:
            head = ctx.result
            await queue.put(
                BlockHeader(
                    number=_intify(head["number"]),
                    hash=_hexify(head["hash"]),
                    parent_hash=_hexify(head["parentHash"]),
                    timestamp=_intify(head["timestamp"]),
                )
            )

        async with AsyncWeb3(WebSocketProvider(self._rpc_ws)) as ws:
            await ws.subscription_manager.subscribe(
                NewHeadsSubscription(label="heads", handler=_on_head)
            )
            handler_task = asyncio.create_task(
                ws.subscription_manager.handle_subscriptions(run_forever=True)
            )
            try:
                while True:
                    yield await queue.get()
            finally:
                handler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await handler_task
