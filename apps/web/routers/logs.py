from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from core.bus.redis_bus import RedisBus

router = APIRouter(prefix="/api/logs", tags=["logs"])

_LOG_KEY = "chain_indexer:logs"
_MAX_LOGS = 500


@router.get("")
async def get_logs(
    limit: int = 200,
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> list[dict[str, Any]]:
    import json
    client = bus.client
    raw = await client.lrange(_LOG_KEY, 0, limit - 1)
    logs: list[dict[str, Any]] = []
    for item in raw:
        try:
            logs.append(json.loads(item))
        except Exception:
            logs.append({"message": str(item)})
    return logs


@router.delete("")
async def clear_logs(
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> dict[str, str]:
    await bus.client.delete(_LOG_KEY)
    return {"status": "cleared"}
