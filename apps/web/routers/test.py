from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_session
from core.abi.registry import AbiRegistry
from core.chains.evm import EvmAdapter
from core.chains.solana import SolanaAdapter
from core.config.repositories import ChainRepo
from core.config.snapshot import load_snapshot
from core.parser.abi_call import AbiCallParser
from core.parser.abi_event import AbiEventParser
from core.parser.anchor_call import AnchorIdlCallParser
from core.parser.anchor_event import AnchorIdlEventParser
from core.parser.erc20 import Erc20TransferParser
from core.parser.native import EvmNativeTransferParser
from core.parser.pipeline import EvmParserPipeline, SolanaParserPipeline
from core.parser.sol_native import SolNativeTransferParser
from core.parser.spl_ops import SplOpsParser
from core.parser.spl_transfer import SplTransferParser

router = APIRouter(prefix="/api/test", tags=["test"])


class ParseBlockRequest(BaseModel):
    chain_id: str
    block_number: int


@router.post("/parse-block")
async def parse_block(
    req: ParseBlockRequest,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> dict[str, Any]:
    chain_row = await ChainRepo(session).get(req.chain_id)
    if chain_row is None:
        raise HTTPException(status_code=404, detail="chain not found")

    snap = await load_snapshot(session)
    registry = AbiRegistry()
    registry.refresh(snap)

    kind = chain_row.kind.value

    if kind == "evm":
        adapter = EvmAdapter(
            chain_id=chain_row.id,
            rpc_http=chain_row.rpc_http,
            rpc_ws=chain_row.rpc_ws,
            confirmations=chain_row.confirmations,
        )
        try:
            await adapter.connect()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"RPC 连接失败: {exc!r}") from exc
        try:
            block = await adapter.fetch_block(req.block_number)
            logs = await adapter.fetch_logs(req.block_number, req.block_number)
            from dataclasses import replace
            block = replace(block, logs=logs)

            parsers: list[Any] = [
                EvmNativeTransferParser(chain_id=chain_row.id),
                Erc20TransferParser(chain_id=chain_row.id),
                AbiEventParser(chain_id=chain_row.id, registry=registry),
                AbiCallParser(chain_id=chain_row.id, registry=registry),
            ]
            pipeline = EvmParserPipeline(parsers)
            events = [asdict(e) for e in pipeline.run(block)]
        finally:
            await adapter.disconnect()

        return {
            "chain_id": chain_row.id,
            "kind": kind,
            "block_number": req.block_number,
            "tx_count": len(block.txs),
            "log_count": len(block.logs),
            "events": events,
        }

    if kind == "solana":
        assert chain_row.commitment is not None
        adapter_sol = SolanaAdapter(
            chain_id=chain_row.id,
            rpc_http=chain_row.rpc_http,
            commitment=chain_row.commitment,
        )
        try:
            await adapter_sol.connect()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"RPC 连接失败: {exc!r}") from exc
        try:
            sol_block = await adapter_sol.fetch_block(req.block_number)
            if sol_block is None:
                return {"chain_id": chain_row.id, "kind": kind, "block_number": req.block_number, "events": [], "error": "slot not found or skipped"}

            sol_parsers: list[Any] = [
                SolNativeTransferParser(chain_id=chain_row.id),
                SplTransferParser(chain_id=chain_row.id),
                SplOpsParser(chain_id=chain_row.id),
                AnchorIdlEventParser(chain_id=chain_row.id, registry=registry),
                AnchorIdlCallParser(chain_id=chain_row.id, registry=registry),
            ]
            sol_pipeline = SolanaParserPipeline(sol_parsers)
            events = [asdict(e) for e in sol_pipeline.run(sol_block)]
        finally:
            await adapter_sol.disconnect()

        return {
            "chain_id": chain_row.id,
            "kind": kind,
            "block_number": req.block_number,
            "tx_count": len(sol_block.transactions),
            "events": events,
        }

    raise HTTPException(status_code=400, detail=f"unsupported chain kind: {kind}")
