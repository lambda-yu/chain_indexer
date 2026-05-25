"""Shared helpers used by every write router.

`bump_and_publish` is the single place that increments `config_version` and
fires `config_changed` to Redis. Routers MUST call it after every successful
mutation so the worker's `ConfigWatcher` (Chunk 7) refreshes its snapshot
within 5 s (spec §5.5).
"""
from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from core.bus.redis_bus import RedisBus
from core.config.repositories import ConfigVersionRepo

log = logging.getLogger(__name__)


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
    except Exception as exc:  # noqa: BLE001 — Redis publish is best-effort
        # Poll fallback covers this — see spec §5.5.
        log.warning("config_changed publish failed: %r", exc)
    return new_version
