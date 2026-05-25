from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import (
    ChannelBindRequest,
    SubscriptionCreate,
    SubscriptionDetail,
    SubscriptionOut,
)
from core.bus.redis_bus import RedisBus
from core.config.models import MatchKind, SubscriptionChannel
from core.config.repositories import (
    ChainRepo,
    ChannelRepo,
    SubscriptionRepo,
)

router = APIRouter(prefix="/api/subscriptions", tags=["subscriptions"])


@router.post("", response_model=SubscriptionOut, status_code=status.HTTP_201_CREATED)
async def create_subscription(
    payload: SubscriptionCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> SubscriptionOut:
    if await ChainRepo(session).get(payload.chain_id) is None:
        raise HTTPException(status_code=404, detail="chain_id not found")
    sub = await SubscriptionRepo(session).create(
        name=payload.name,
        chain_id=payload.chain_id,
        address=payload.address,
        abi_id=payload.abi_id,
        match_kind=MatchKind(payload.match_kind),
        match_name=payload.match_name,
        arg_filters=payload.arg_filters,
        enabled=payload.enabled,
    )
    await bump_and_publish(session, bus, entity="subscription", entity_id=sub.id, action="create")
    return SubscriptionOut.model_validate(sub)


@router.get("", response_model=list[SubscriptionOut])
async def list_subscriptions(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[SubscriptionOut]:
    rows = await SubscriptionRepo(session).list_all()
    return [SubscriptionOut.model_validate(r) for r in rows]


@router.get("/{sub_id}", response_model=SubscriptionDetail)
async def get_subscription(
    sub_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> SubscriptionDetail:
    sub = await SubscriptionRepo(session).get(sub_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    # Direct query for bound channel ids — NOT `list_enabled_with_channels`, which
    # filters subs by `enabled=True` and would silently hide bindings on disabled
    # subs (an operator surprise we want to avoid).
    res = await session.execute(
        select(SubscriptionChannel.channel_id)
        .where(SubscriptionChannel.subscription_id == sub_id)
    )
    channel_ids: list[str] = list(res.scalars().all())
    return SubscriptionDetail(
        id=sub.id, name=sub.name, chain_id=sub.chain_id, address=sub.address,
        abi_id=sub.abi_id, match_kind=sub.match_kind.value, match_name=sub.match_name,
        arg_filters=sub.arg_filters, enabled=sub.enabled,
        channel_ids=channel_ids,
    )


@router.post("/{sub_id}/channels", status_code=status.HTTP_204_NO_CONTENT)
async def bind_channel(
    sub_id: str,
    payload: ChannelBindRequest,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> Response:
    sub_repo = SubscriptionRepo(session)
    if await sub_repo.get(sub_id) is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    if await ChannelRepo(session).get(payload.channel_id) is None:
        raise HTTPException(status_code=404, detail="channel not found")
    # Composite-PK (sub_id, channel_id) means a re-bind raises IntegrityError —
    # translate that to 409 instead of letting it surface as a 500.
    try:
        await sub_repo.bind_channel(sub_id, payload.channel_id)
    except IntegrityError as e:
        await session.rollback()
        raise HTTPException(status_code=409, detail="channel already bound") from e
    await bump_and_publish(session, bus, entity="subscription", entity_id=sub_id, action="bind_channel")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
