from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import ChannelCreate, ChannelOut
from core.bus.redis_bus import RedisBus
from core.config.models import ChannelType
from core.config.repositories import ChannelRepo
from core.notifier.channel import CHANNEL_REGISTRY

router = APIRouter(prefix="/api/channels", tags=["channels"])


@router.post("", response_model=ChannelOut, status_code=status.HTTP_201_CREATED)
async def create_channel(
    payload: ChannelCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChannelOut:
    cls = CHANNEL_REGISTRY.get(payload.type)
    if cls is not None and hasattr(cls, "config_schema"):
        import jsonschema
        try:
            jsonschema.validate(payload.config, cls.config_schema)
        except jsonschema.ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc.message)) from exc
    row = await ChannelRepo(session).create(
        name=payload.name,
        type=ChannelType(payload.type),
        config=payload.config,
    )
    await bump_and_publish(session, bus, entity="channel", entity_id=row.id, action="create")
    return ChannelOut.model_validate(row)


@router.put("/{channel_id}", response_model=ChannelOut)
async def update_channel(
    channel_id: str,
    payload: ChannelCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChannelOut:
    repo = ChannelRepo(session)
    row = await repo.get(channel_id)
    if row is None:
        raise HTTPException(status_code=404, detail="channel not found")
    cls = CHANNEL_REGISTRY.get(payload.type)
    if cls is not None and hasattr(cls, "config_schema"):
        import jsonschema
        try:
            jsonschema.validate(payload.config, cls.config_schema)
        except jsonschema.ValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc.message)) from exc
    await repo.update(channel_id, name=payload.name, config=payload.config)
    await bump_and_publish(session, bus, entity="channel", entity_id=channel_id, action="update")
    await session.refresh(row)
    return ChannelOut.model_validate(row)
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
