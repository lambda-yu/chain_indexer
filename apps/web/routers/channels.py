from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import ChannelCreate, ChannelOut
from core.bus.redis_bus import RedisBus
from core.config.models import ChannelType
from core.config.repositories import ChannelRepo

router = APIRouter(prefix="/api/channels", tags=["channels"])


@router.post("", response_model=ChannelOut, status_code=status.HTTP_201_CREATED)
async def create_channel(
    payload: ChannelCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChannelOut:
    row = await ChannelRepo(session).create(
        name=payload.name,
        type=ChannelType(payload.type),
        config=payload.config,
    )
    await bump_and_publish(session, bus, entity="channel", entity_id=row.id, action="create")
    return ChannelOut.model_validate(row)


@router.get("", response_model=list[ChannelOut])
async def list_channels(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[ChannelOut]:
    rows = await ChannelRepo(session).list_all()
    return [ChannelOut.model_validate(r) for r in rows]


@router.get("/{channel_id}", response_model=ChannelOut)
async def get_channel(
    channel_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> ChannelOut:
    row = await ChannelRepo(session).get(channel_id)
    if row is None:
        raise HTTPException(status_code=404, detail="channel not found")
    return ChannelOut.model_validate(row)
