from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from core.bus.redis_bus import RedisBus
from core.abi.registry import AbiRegistry
from core.chains.evm import EvmAdapter
from core.chains.solana import SolanaAdapter
from core.config.repositories import ChainRepo
from core.config.snapshot import load_snapshot
from core.matcher.matcher import Matcher
from core.parser.abi_call import AbiCallParser
from core.parser.abi_event import AbiEventParser
from core.parser.anchor_call import AnchorIdlCallParser
from core.parser.anchor_event import AnchorIdlEventParser
from core.parser.erc20 import Erc20TransferParser
from core.parser.event import Event
from core.parser.native import EvmNativeTransferParser
from core.parser.pipeline import EvmParserPipeline, SolanaParserPipeline
from core.parser.sol_native import SolNativeTransferParser
from core.parser.spl_ops import SplOpsParser
from core.parser.spl_transfer import SplTransferParser

router = APIRouter(prefix="/api/test", tags=["test"])


class ParseBlockRequest(BaseModel):
    chain_id: str
    block_number: int


def _match_event(event: Event, matcher: Matcher) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    for sub, channels in matcher.match(event):
        hits.append({
            "subscription_id": sub.id,
            "subscription_name": sub.name,
            "match_kind": sub.match_kind,
            "match_name": sub.match_name,
            "channels": [{"id": ch.id, "name": ch.name, "type": ch.type} for ch in channels],
        })
    return hits


def _safe_dict(obj: Any) -> Any:
    if isinstance(obj, bytes):
        try:
            return "0x" + obj.hex()
        except Exception:
            return repr(obj)
    if isinstance(obj, dict):
        return {str(k): _safe_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_dict(v) for v in obj]
    if isinstance(obj, (int, float, bool, str)) or obj is None:
        return obj
    return str(obj)


def _enrich_events(events: list[Event], matcher: Matcher) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ev in events:
        d = _safe_dict(asdict(ev))
        hits = _match_event(ev, matcher)
        d["matched_subscriptions"] = hits
        d["matched"] = len(hits) > 0
        result.append(d)
    return result


@router.post("/parse-block")
async def parse_block(
    req: ParseBlockRequest,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> JSONResponse:
    try:
        data = await _do_parse_block(req, session)
        return JSONResponse(content=data)
    except HTTPException:
        raise
    except Exception as exc:
        return JSONResponse(status_code=500, content={"detail": f"解析失败: {exc!r}"})


async def _do_parse_block(
    req: ParseBlockRequest,
    session: AsyncSession,
) -> dict[str, Any]:
    chain_row = await ChainRepo(session).get(req.chain_id)
    if chain_row is None:
        raise HTTPException(status_code=404, detail="chain not found")

    snap = await load_snapshot(session)
    registry = AbiRegistry()
    registry.refresh(snap)
    matcher = Matcher(snap)

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
            events = list(pipeline.run(block))
        finally:
            await adapter.disconnect()

        enriched = _enrich_events(events, matcher)
        matched_count = sum(1 for e in enriched if e["matched"])

        return {
            "chain_id": chain_row.id,
            "kind": kind,
            "block_number": req.block_number,
            "tx_count": len(block.txs),
            "log_count": len(block.logs),
            "event_count": len(enriched),
            "matched_count": matched_count,
            "events": enriched,
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
                return {"chain_id": chain_row.id, "kind": kind, "block_number": req.block_number, "events": [], "matched_count": 0, "event_count": 0, "error": "slot not found or skipped"}

            sol_parsers: list[Any] = [
                SolNativeTransferParser(chain_id=chain_row.id),
                SplTransferParser(chain_id=chain_row.id),
                SplOpsParser(chain_id=chain_row.id),
                AnchorIdlEventParser(chain_id=chain_row.id, registry=registry),
                AnchorIdlCallParser(chain_id=chain_row.id, registry=registry),
            ]
            sol_pipeline = SolanaParserPipeline(sol_parsers)
            events = list(sol_pipeline.run(sol_block))
        finally:
            await adapter_sol.disconnect()

        enriched = _enrich_events(events, matcher)
        matched_count = sum(1 for e in enriched if e["matched"])

        return {
            "chain_id": chain_row.id,
            "kind": kind,
            "block_number": req.block_number,
            "tx_count": len(sol_block.transactions),
            "event_count": len(enriched),
            "matched_count": matched_count,
            "events": enriched,
        }

    raise HTTPException(status_code=400, detail=f"unsupported chain kind: {kind}")


class TestSubscriptionRequest(BaseModel):
    subscription_id: str
    block_number: int


@router.post("/test-subscription")
async def test_subscription(
    req: TestSubscriptionRequest,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> JSONResponse:
    """Parse a block, match against a single subscription, and actually deliver to its channels."""
    from apps.web.deps import get_bus as _get_bus
    from core.config.repositories import ChannelRepo, SubscriptionRepo
    from core.notifier.channel import CHANNEL_REGISTRY
    from core.notifier.payload import build_payload

    try:
        sub_row = await SubscriptionRepo(session).get(req.subscription_id)
        if sub_row is None:
            raise HTTPException(status_code=404, detail="subscription not found")

        chain_row = await ChainRepo(session).get(sub_row.chain_id)
        if chain_row is None:
            raise HTTPException(status_code=404, detail="chain not found")

        snap = await load_snapshot(session)
        registry = AbiRegistry()
        registry.refresh(snap)

        # Find the subscription in snapshot
        snap_sub = None
        for s in snap.subscriptions:
            if s.id == req.subscription_id:
                snap_sub = s
                break
        if snap_sub is None:
            raise HTTPException(status_code=404, detail="subscription not in snapshot (disabled?)")

        # Resolve channels
        channels_for_sub = [c for c in snap.channels if c.id in snap_sub.channel_ids]

        kind = chain_row.kind.value

        # Parse block
        if kind == "evm":
            adapter = EvmAdapter(chain_id=chain_row.id, rpc_http=chain_row.rpc_http, rpc_ws=chain_row.rpc_ws, confirmations=chain_row.confirmations)
            try:
                await adapter.connect()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"RPC 连接失败: {exc!r}") from exc
            try:
                block = await adapter.fetch_block(req.block_number)
                logs = await adapter.fetch_logs(req.block_number, req.block_number)
                from dataclasses import replace
                block = replace(block, logs=logs)
                parsers_list: list[Any] = [
                    EvmNativeTransferParser(chain_id=chain_row.id),
                    Erc20TransferParser(chain_id=chain_row.id),
                    AbiEventParser(chain_id=chain_row.id, registry=registry),
                    AbiCallParser(chain_id=chain_row.id, registry=registry),
                ]
                events = list(EvmParserPipeline(parsers_list).run(block))
            finally:
                await adapter.disconnect()
        elif kind == "solana":
            assert chain_row.commitment is not None
            adapter_sol = SolanaAdapter(chain_id=chain_row.id, rpc_http=chain_row.rpc_http, commitment=chain_row.commitment)
            try:
                await adapter_sol.connect()
            except Exception as exc:
                raise HTTPException(status_code=502, detail=f"RPC 连接失败: {exc!r}") from exc
            try:
                sol_block = await adapter_sol.fetch_block(req.block_number)
                if sol_block is None:
                    return JSONResponse(content={"matched": 0, "delivered": 0, "events": [], "error": "slot not found"})
                sol_parsers_list: list[Any] = [
                    SolNativeTransferParser(chain_id=chain_row.id),
                    SplTransferParser(chain_id=chain_row.id),
                    SplOpsParser(chain_id=chain_row.id),
                    AnchorIdlEventParser(chain_id=chain_row.id, registry=registry),
                    AnchorIdlCallParser(chain_id=chain_row.id, registry=registry),
                ]
                events = list(SolanaParserPipeline(sol_parsers_list).run(sol_block))
            finally:
                await adapter_sol.disconnect()
        else:
            raise HTTPException(status_code=400, detail=f"unsupported kind: {kind}")

        # Match against this single subscription
        matcher = Matcher(snap)
        matched_events: list[dict[str, Any]] = []
        delivered = 0

        for ev in events:
            for matched_sub, matched_chans in matcher.match(ev):
                if matched_sub.id != req.subscription_id:
                    continue
                matched_events.append(_safe_dict(asdict(ev)))
                # Actually deliver
                if matched_chans:
                    payload = build_payload(event=ev, subscription=matched_sub)
                    for ch_cfg in matched_chans:
                        cls = CHANNEL_REGISTRY.get(ch_cfg.type)
                        if cls is None:
                            continue
                        try:
                            ch_inst = cls(config=ch_cfg.config, bus=bus)
                            await ch_inst.start()
                            try:
                                await ch_inst.send(payload)
                                delivered += 1
                            finally:
                                await ch_inst.stop()
                        except Exception as exc:
                            matched_events[-1]["delivery_error"] = repr(exc)

        return JSONResponse(content={
            "subscription_id": req.subscription_id,
            "subscription_name": sub_row.name,
            "block_number": req.block_number,
            "total_events": len(events),
            "matched": len(matched_events),
            "delivered": delivered,
            "channels": [{"id": c.id, "name": c.name, "type": c.type} for c in channels_for_sub],
            "events": matched_events[:50],
        })
    except HTTPException:
        raise
    except Exception as exc:
        return JSONResponse(status_code=500, content={"detail": f"测试失败: {exc!r}"})
