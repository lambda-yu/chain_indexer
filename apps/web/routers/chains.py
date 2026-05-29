from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import ChainCreate, ChainOut
from core.bus.redis_bus import RedisBus
from core.config.models import ChainKind
from core.config.repositories import ChainRepo

router = APIRouter(prefix="/api/chains", tags=["chains"])


@router.post("", response_model=ChainOut, status_code=status.HTTP_201_CREATED)
async def create_chain(
    payload: ChainCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChainOut:
    repo = ChainRepo(session)
    if await repo.get(payload.id) is not None:
        raise HTTPException(status_code=409, detail="chain id already exists")
    row = await repo.create(
        id=payload.id,
        kind=ChainKind(payload.kind),
        rpc_http=payload.rpc_http,
        rpc_ws=payload.rpc_ws,
        confirmations=payload.confirmations,
        poll_interval_ms=payload.poll_interval_ms,
        enabled=payload.enabled,
        commitment=payload.commitment,
        trace_internal_calls=payload.trace_internal_calls,
        log_query_range_blocks=payload.log_query_range_blocks,
        slot_query_range_blocks=payload.slot_query_range_blocks,
    )
    await bump_and_publish(session, bus, entity="chain", entity_id=row.id, action="create")
    return ChainOut.model_validate(row)


@router.put("/{chain_id}", response_model=ChainOut)
async def update_chain(
    chain_id: str,
    payload: ChainCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChainOut:
    repo = ChainRepo(session)
    row = await repo.get(chain_id)
    if row is None:
        raise HTTPException(status_code=404, detail="chain not found")
    await repo.update(
        chain_id,
        rpc_http=payload.rpc_http,
        rpc_ws=payload.rpc_ws,
        confirmations=payload.confirmations,
        poll_interval_ms=payload.poll_interval_ms,
        enabled=payload.enabled,
        commitment=payload.commitment,
        trace_internal_calls=payload.trace_internal_calls,
        log_query_range_blocks=payload.log_query_range_blocks,
        slot_query_range_blocks=payload.slot_query_range_blocks,
    )
    await bump_and_publish(session, bus, entity="chain", entity_id=chain_id, action="update")
    await session.refresh(row)
    return ChainOut.model_validate(row)


@router.get("", response_model=list[ChainOut])
async def list_chains(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[ChainOut]:
    rows = await ChainRepo(session).list_all()
    return [ChainOut.model_validate(r) for r in rows]


@router.get("/{chain_id}", response_model=ChainOut)
async def get_chain(
    chain_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> ChainOut:
    row = await ChainRepo(session).get(chain_id)
    if row is None:
        raise HTTPException(status_code=404, detail="chain not found")
    return ChainOut.model_validate(row)


@router.get("/{chain_id}/status")
async def chain_status(
    chain_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> dict:
    from core.config.repositories import CheckpointRepo
    chain = await ChainRepo(session).get(chain_id)
    if chain is None:
        raise HTTPException(status_code=404, detail="chain not found")
    cp = await CheckpointRepo(session).get(chain_id)
    return {
        "chain_id": chain_id,
        "enabled": chain.enabled,
        "latest_block": cp.last_block if cp else None,
        "latest_block_hash": cp.last_block_hash if cp else None,
    }


class ChainLagOut(BaseModel):
    chain_id: str
    tip_block: int | None
    last_processed_block: int | None
    lag_blocks: int | None


@router.get("/{chain_id}/lag", response_model=ChainLagOut)
async def get_chain_lag(
    chain_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChainLagOut:
    from core.config.repositories import CheckpointRepo

    chain = await ChainRepo(session).get(chain_id)
    if chain is None:
        raise HTTPException(status_code=404, detail="chain not found")

    # Tip is published to Redis by the worker on every live head.
    tip_raw = await bus.client.get(f"chain:{chain_id}:tip")
    tip_block: int | None = int(tip_raw) if tip_raw is not None else None

    checkpoint = await CheckpointRepo(session).get(chain_id)
    last_processed = checkpoint.last_block if checkpoint is not None else None

    lag = (
        max(0, tip_block - last_processed)
        if tip_block is not None and last_processed is not None
        else None
    )

    return ChainLagOut(
        chain_id=chain_id,
        tip_block=tip_block,
        last_processed_block=last_processed,
        lag_blocks=lag,
    )
