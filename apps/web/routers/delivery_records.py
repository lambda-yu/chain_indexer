from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from core.bus.redis_bus import RedisBus
from core.config.repositories import DeliveryRecordRepo
from core.notifier.channel import CHANNEL_REGISTRY

StatusFilter = Literal["success", "failed", "retrying", "resolved"]

router = APIRouter(prefix="/api/delivery-records", tags=["delivery-records"])


class DeliveryRecordOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    subscription_id: str
    channel_id: str
    chain_id: str
    event_payload: dict[str, Any]
    error: str | None
    attempts: int
    status: str
    created_at: datetime
    resolved_at: datetime | None


@router.get("", response_model=list[DeliveryRecordOut])
async def list_delivery_records(
    subscription_id: str | None = None,
    status_filter: StatusFilter | None = Query(None, alias="status"),  # noqa: B008
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[DeliveryRecordOut]:
    rows = await DeliveryRecordRepo(session).list_all(
        limit=200, subscription_id=subscription_id, status=status_filter,
    )
    return [DeliveryRecordOut.model_validate(r) for r in rows]


@router.post("/{delivery_id}/retry", status_code=status.HTTP_200_OK)
async def retry_delivery(
    delivery_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> dict[str, str]:
    repo = DeliveryRecordRepo(session)
    row = await repo.get(delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail="delivery not found")

    from core.config.repositories import ChannelRepo
    ch_row = await ChannelRepo(session).get(row.channel_id)
    if ch_row is None:
        raise HTTPException(status_code=404, detail="channel no longer exists")

    cls = CHANNEL_REGISTRY.get(ch_row.type.value)
    if cls is None:
        raise HTTPException(status_code=400, detail=f"unknown channel type: {ch_row.type}")

    try:
        ch = cls(config=ch_row.config, bus=bus)
        await ch.start()
        try:
            await ch.send(row.event_payload)
        finally:
            await ch.stop()
    except Exception as exc:
        await session.rollback()
        # Persist the failed retry so attempts and error reflect reality.
        await repo.bump_attempt(delivery_id, error=repr(exc))
        await session.commit()
        raise HTTPException(status_code=502, detail=f"重推失败: {exc!r}") from exc

    await repo.mark_resolved(delivery_id)
    await session.commit()
    return {"status": "resolved", "delivery_id": delivery_id}


@router.post("/{delivery_id}/resolve", status_code=status.HTTP_200_OK)
async def resolve_delivery(
    delivery_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> dict[str, str]:
    repo = DeliveryRecordRepo(session)
    row = await repo.get(delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    await repo.mark_resolved(delivery_id)
    await session.commit()
    return {"status": "resolved", "delivery_id": delivery_id}


@router.delete("/{delivery_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_delivery(
    delivery_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> None:
    repo = DeliveryRecordRepo(session)
    row = await repo.get(delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    await repo.delete(delivery_id)
    await session.commit()
