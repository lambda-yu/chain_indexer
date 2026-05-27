from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
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
    )
    await bump_and_publish(session, bus, entity="chain", entity_id=chain_id, action="update")
    await session.refresh(row)
    return ChainOut.model_validate(row)
async def list_chains(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[ChainOut]:
    rows = await ChainRepo(session).list_enabled()
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
