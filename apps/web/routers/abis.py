from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import AbiCreate, AbiOut
from core.bus.redis_bus import RedisBus
from core.config.models import AbiKind
from core.config.repositories import AbiRepo

router = APIRouter(prefix="/api/abis", tags=["abis"])


@router.post("", response_model=AbiOut, status_code=status.HTTP_201_CREATED)
async def create_abi(
    payload: AbiCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> AbiOut:
    row = await AbiRepo(session).create(
        name=payload.name,
        kind=AbiKind(payload.kind),
        body=payload.body,
    )
    await bump_and_publish(session, bus, entity="abi", entity_id=row.id, action="create")
    return AbiOut.model_validate(row)


@router.get("", response_model=list[AbiOut])
async def list_abis(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[AbiOut]:
    rows = await AbiRepo(session).list_all()
    return [AbiOut.model_validate(r) for r in rows]


@router.get("/{abi_id}", response_model=AbiOut)
async def get_abi(
    abi_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> AbiOut:
    row = await AbiRepo(session).get(abi_id)
    if row is None:
        raise HTTPException(status_code=404, detail="abi not found")
    return AbiOut.model_validate(row)


@router.delete("/{abi_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_abi(
    abi_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> None:
    repo = AbiRepo(session)
    row = await repo.get(abi_id)
    if row is None:
        raise HTTPException(status_code=404, detail="abi not found")
    await repo.delete(abi_id)
    await bump_and_publish(session, bus, entity="abi", entity_id=abi_id, action="delete")
