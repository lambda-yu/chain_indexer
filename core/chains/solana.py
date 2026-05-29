from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx
import structlog
from solders.commitment_config import CommitmentLevel
from solders.rpc.config import RpcContextConfig
from solders.rpc.requests import GetSlot
from solders.rpc.responses import GetSlotResp

from core.chains.rpc_pool import EndpointPool
from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)
from core.metrics import track_rpc

log = structlog.get_logger(__name__)

_COMMITMENT_MAP: dict[str, CommitmentLevel] = {
    "confirmed": CommitmentLevel.Confirmed,
    "finalized": CommitmentLevel.Finalized,
}


class SolanaAdapter:
    def __init__(
        self,
        *,
        chain_id: str,
        rpc_http: str,
        commitment: str,
        poll_interval_ms: int = 2000,
        rpc_ws: str | None = None,
        rpc_http_fallbacks: list[str] | None = None,
        rpc_timeout_ms: int = 10000,
        failure_threshold: int = 3,
        cooldown_s: float = 30.0,
    ) -> None:
        self.chain_id = chain_id
        self._http_urls = [rpc_http, *(rpc_http_fallbacks or [])]
        self._rpc_ws = rpc_ws
        self._commitment = _COMMITMENT_MAP[commitment]
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._rpc_timeout_s = rpc_timeout_ms / 1000.0
        self._failure_threshold = failure_threshold
        self._cooldown_s = cooldown_s
        self._client: httpx.AsyncClient | None = None
        self._pool: EndpointPool[str] | None = None

    async def connect(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._rpc_timeout_s)
        self._pool = EndpointPool(
            self.chain_id,
            self._http_urls,
            timeout_s=self._rpc_timeout_s,
            failure_threshold=self._failure_threshold,
            cooldown_s=self._cooldown_s,
        )

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._pool = None

    async def get_latest_slot(self) -> int:
        assert self._client is not None and self._pool is not None
        async with track_rpc(self.chain_id, "getSlot"):
            req = GetSlot(RpcContextConfig(commitment=self._commitment))

            async def _do(url: str) -> str:
                assert self._client is not None
                resp = await self._client.post(
                    url,
                    content=req.to_json(),
                    headers={"content-type": "application/json"},
                )
                resp.raise_for_status()
                return resp.text

            text = await self._pool.call(_do)
            parsed = GetSlotResp.from_json(text)
            return parsed.value  # type: ignore[union-attr]

    async def fetch_block(self, slot: int) -> SolanaBlock | None:
        assert self._client is not None and self._pool is not None
        async with track_rpc(self.chain_id, "getBlock"):
            config: dict[str, Any] = {
                "commitment": str(self._commitment).lower(),
                "transactionDetails": "full",
                "rewards": False,
                "maxSupportedTransactionVersion": 0,
            }
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getBlock",
                "params": [slot, config],
            }

            async def _do(url: str) -> dict[str, Any]:
                assert self._client is not None
                resp = await self._client.post(
                    url,
                    json=payload,
                    headers={"content-type": "application/json"},
                )
                resp.raise_for_status()
                data: dict[str, Any] = resp.json()
                return data

            body = await self._pool.call(_do)
            result = body.get("result")
            return None if result is None else self._parse_block(slot, result)

    async def get_blocks(self, start_slot: int, end_slot: int) -> list[int]:
        """Return slots in [start_slot, end_slot] that contain confirmed blocks.

        Skipped/empty slots are excluded. The slot-discovery step always uses
        `finalized` commitment because `confirmed` is not guaranteed to support
        large ranges across providers. Subsequent `getBlock` calls still
        respect the chain's configured commitment.

        The caller is responsible for window sizing (per-chain
        `slot_query_range_blocks`); this method does not internally chunk.
        """
        assert self._client is not None and self._pool is not None
        async with track_rpc(self.chain_id, "getBlocks"):
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getBlocks",
                "params": [start_slot, end_slot, {"commitment": "finalized"}],
            }

            async def _do(url: str) -> dict[str, Any]:
                assert self._client is not None
                resp = await self._client.post(
                    url,
                    json=payload,
                    headers={"content-type": "application/json"},
                )
                resp.raise_for_status()
                data: dict[str, Any] = resp.json()
                return data

            body = await self._pool.call(_do)
            return list(body.get("result") or [])

    def _parse_block(self, slot: int, result: dict[str, Any]) -> SolanaBlock:
        txs: list[SolanaTransaction] = []
        for tx_obj in result.get("transactions", []):
            meta = tx_obj.get("meta", {}) or {}
            tx_data = tx_obj.get("transaction", {})
            msg = tx_data.get("message", {})

            signatures = tx_data.get("signatures", [])
            sig = signatures[0] if signatures else ""

            account_keys = [k if isinstance(k, str) else k.get("pubkey", "")
                           for k in msg.get("accountKeys", [])]

            instructions: list[SolanaInstruction] = []
            for ix in msg.get("instructions", []):
                prog_idx = ix.get("programIdIndex", 0)
                prog_id = account_keys[prog_idx] if prog_idx < len(account_keys) else ""
                acct_indices = ix.get("accounts", [])
                accounts = [account_keys[i] for i in acct_indices if i < len(account_keys)]
                instructions.append(SolanaInstruction(
                    program_id=prog_id,
                    accounts=accounts,
                    data_b58=ix.get("data", ""),
                    stack_depth=1,
                ))
            for inner in meta.get("innerInstructions", []) or []:
                for ix in inner.get("instructions", []):
                    prog_idx = ix.get("programIdIndex", 0)
                    prog_id = account_keys[prog_idx] if prog_idx < len(account_keys) else ""
                    acct_indices = ix.get("accounts", [])
                    accounts = [account_keys[i] for i in acct_indices if i < len(account_keys)]
                    instructions.append(SolanaInstruction(
                        program_id=prog_id,
                        accounts=accounts,
                        data_b58=ix.get("data", ""),
                        stack_depth=inner.get("index", 0) + 2,
                    ))

            def _parse_token_balances(raw: list[dict[str, Any]] | None) -> list[SolanaTokenBalance]:
                out: list[SolanaTokenBalance] = []
                for tb in raw or []:
                    amt_info = tb.get("uiTokenAmount", {})
                    out.append(SolanaTokenBalance(
                        account_index=tb.get("accountIndex", 0),
                        mint=tb.get("mint", ""),
                        owner=tb.get("owner"),
                        amount=int(amt_info.get("amount", "0")),
                        decimals=amt_info.get("decimals", 0),
                    ))
                return out

            txs.append(SolanaTransaction(
                signature=sig,
                slot=slot,
                success=meta.get("err") is None,
                fee=meta.get("fee", 0),
                account_keys=account_keys,
                pre_balances=meta.get("preBalances", []),
                post_balances=meta.get("postBalances", []),
                pre_token_balances=_parse_token_balances(meta.get("preTokenBalances")),
                post_token_balances=_parse_token_balances(meta.get("postTokenBalances")),
                log_messages=meta.get("logMessages", []) or [],
                instructions=instructions,
            ))

        return SolanaBlock(
            slot=slot,
            block_hash=result.get("blockhash", ""),
            parent_slot=result.get("parentSlot", slot - 1),
            block_time=result.get("blockTime"),
            transactions=txs,
        )

    def subscribe_heads(self) -> AsyncIterator[int]:
        if self._rpc_ws is not None:
            return self._ws_heads()
        return self._poll_heads()

    def _poll_heads(self) -> AsyncIterator[int]:
        async def _poll() -> AsyncIterator[int]:
            last_slot = 0
            while True:
                try:
                    current = await self.get_latest_slot()
                except Exception:  # noqa: BLE001
                    log.warning("solana_adapter.poll_slot_failed", chain_id=self.chain_id)
                    await asyncio.sleep(self._poll_interval_s)
                    continue
                if current > last_slot:
                    for s in range(last_slot + 1, current + 1):
                        yield s
                    last_slot = current
                await asyncio.sleep(self._poll_interval_s)
        return _poll()

    def _ws_heads(self) -> AsyncIterator[int]:
        import json as _json

        import websockets

        async def _gen() -> AsyncIterator[int]:
            backoff = 1.0
            last_slot = 0
            while True:
                try:
                    async with websockets.connect(self._rpc_ws) as ws:  # type: ignore[arg-type]
                        req = _json.dumps({
                            "jsonrpc": "2.0", "id": 1,
                            "method": "slotSubscribe",
                        })
                        await ws.send(req)
                        backoff = 1.0
                        async for msg in ws:
                            data = _json.loads(msg)
                            params = data.get("params", {})
                            result = params.get("result", {})
                            slot = result.get("slot")
                            if isinstance(slot, int) and slot > last_slot:
                                for s in range(last_slot + 1, slot + 1):
                                    yield s
                                last_slot = slot
                except Exception:  # noqa: BLE001
                    log.warning(
                        "solana_adapter.ws_disconnected",
                        chain_id=self.chain_id,
                        backoff_s=backoff,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 4, 60.0)
        return _gen()
