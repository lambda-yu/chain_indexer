from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from core.bus.redis_bus import RedisBus
from core.config.repositories import DeliveryRecordRepo
from core.notifier.channel import CHANNEL_REGISTRY

router = APIRouter(prefix="/api/failed-deliveries", tags=["failed-deliveries"])


class FailedDeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    subscription_id: str
    channel_id: str
    chain_id: str
    event_payload: dict[str, Any]
    error: str
    attempts: int
    status: str
    created_at: str
    resolved_at: str | None


@router.get("", response_model=list[FailedDeliveryOut])
async def list_failed_deliveries(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[FailedDeliveryOut]:
    rows = await DeliveryRecordRepo(session).list_all(limit=200)
    return [FailedDeliveryOut.model_validate(r) for r in rows]


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
        await repo.mark_resolved(delivery_id)
        await session.commit()
        return {"status": "resolved", "delivery_id": delivery_id}
    except Exception as exc:
        await session.rollback()
        raise HTTPException(status_code=502, detail=f"重推失败: {exc!r}") from exc


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
