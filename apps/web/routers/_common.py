"""Shared helpers used by every write router.

`bump_and_publish` is the single place that increments `config_version` and
fires `config_changed` to Redis. Routers MUST call it after every successful
mutation so the worker's `ConfigWatcher` (Chunk 7) refreshes its snapshot
within 5 s (spec §5.5).
"""
from __future__ import annotations

import asyncio

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from core.bus.redis_bus import RedisBus
from core.config.repositories import ConfigVersionRepo

log = structlog.get_logger(__name__)


async def bump_and_publish(
    session: AsyncSession,
    bus: RedisBus,
    *,
    entity: str,
    entity_id: str,
    action: str,
) -> int:
    """Bump config_version (in the same transaction as the caller's write),
    commit, then publish a `config_changed` notification.

    Returns the new version. The publish is best-effort: if Redis is down the
    worker still picks up the change via its 5-s poll (spec §5.5).
    """
    new_version = await ConfigVersionRepo(session).bump()
    await session.commit()
    try:
        await bus.publish(
            "config_changed",
            {"entity": entity, "id": entity_id, "action": action, "version": new_version},
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 — Redis publish is best-effort; poll fallback covers it (spec §5.5).
        log.warning(
            "web.config_changed_publish_failed",
            entity=entity,
            entity_id=entity_id,
            action=action,
            version=new_version,
            exc=repr(exc),
        )
    return new_version
