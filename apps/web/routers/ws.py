from __future__ import annotations

import asyncio
import contextlib
import json
import time

import structlog
from fastapi import APIRouter, Depends, WebSocket, status

from apps.web.deps import get_bus, get_db
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChannelType
from core.config.repositories import ChannelRepo

log = structlog.get_logger(__name__)
router = APIRouter(tags=["ws"])

_QUEUE_SIZE = 256
_WARN_WINDOW_S = 60.0


async def _resolve_fanout_channel(
    channel_id: str,
    db: Database,
) -> str | None:
    async with db.session() as s:
        row = await ChannelRepo(s).get(channel_id)
    if row is None or row.type != ChannelType.ws:
        return None
    fanout: str | None = row.config.get("ws_fanout_channel")
    return fanout


@router.websocket("/ws")
async def ws_endpoint(
    websocket: WebSocket,
    channel_id: str,
    db: Database = Depends(get_db),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> None:
    fanout = await _resolve_fanout_channel(channel_id, db)
    if fanout is None:
        await websocket.accept()
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_SIZE)
    last_warn_at = 0.0

    async def _producer() -> None:
        nonlocal last_warn_at
        ready = asyncio.Event()
        gen = bus.subscribe(fanout, ready=ready)
        try:
            async for msg in gen:
                body = json.dumps(msg, separators=(",", ":"))
                try:
                    queue.put_nowait(body)
                except asyncio.QueueFull:
                    now = time.monotonic()
                    if now - last_warn_at >= _WARN_WINDOW_S:
                        log.warning(
                            "ws.slow_consumer_dropping_messages",
                            channel_id=channel_id,
                            fanout=fanout,
                        )
                        last_warn_at = now
        finally:
            await gen.aclose()  # type: ignore[attr-defined]

    async def _consumer() -> None:
        while True:
            body = await queue.get()
            await websocket.send_text(body)

    producer_task = asyncio.create_task(_producer(), name=f"ws.producer:{channel_id}")
    consumer_task = asyncio.create_task(_consumer(), name=f"ws.consumer:{channel_id}")
    try:
        done, pending = await asyncio.wait(
            {producer_task, consumer_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        for t in done | pending:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "ws.session_ended_with_exception",
                    channel_id=channel_id,
                    task=t.get_name(),
                    exc=repr(exc),
                )
    finally:
        with contextlib.suppress(RuntimeError):
            await websocket.close()
