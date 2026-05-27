from __future__ import annotations

import asyncio
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_LOCK_PREFIX = "chain_indexer:lock:"
_LOCK_TTL_S = 30
_RENEW_INTERVAL_S = 10


class ChainLock:
    """Redis-based distributed lock for a single chain_id.

    Uses SET NX EX pattern. The lock holder must renew periodically
    (every _RENEW_INTERVAL_S) to keep the TTL alive. If the worker
    crashes, the lock auto-expires after _LOCK_TTL_S.
    """

    def __init__(self, client: Any, chain_id: str, worker_id: str) -> None:
        self._client = client
        self._key = f"{_LOCK_PREFIX}{chain_id}"
        self._worker_id = worker_id
        self._chain_id = chain_id
        self._renew_task: asyncio.Task[None] | None = None

    async def acquire(self) -> bool:
        result = await self._client.set(self._key, self._worker_id, nx=True, ex=_LOCK_TTL_S)
        if result:
            self._renew_task = asyncio.create_task(self._renew_loop())
            log.info("chain_lock.acquired", chain_id=self._chain_id, worker_id=self._worker_id)
            return True
        holder = await self._client.get(self._key)
        if holder == self._worker_id:
            self._renew_task = asyncio.create_task(self._renew_loop())
            return True
        log.debug("chain_lock.held_by_other", chain_id=self._chain_id, holder=holder)
        return False

    async def release(self) -> None:
        if self._renew_task is not None:
            self._renew_task.cancel()
            try:
                await self._renew_task
            except asyncio.CancelledError:
                pass
            self._renew_task = None
        holder = await self._client.get(self._key)
        if holder == self._worker_id:
            await self._client.delete(self._key)
            log.info("chain_lock.released", chain_id=self._chain_id)

    async def _renew_loop(self) -> None:
        while True:
            await asyncio.sleep(_RENEW_INTERVAL_S)
            try:
                holder = await self._client.get(self._key)
                if holder == self._worker_id:
                    await self._client.expire(self._key, _LOCK_TTL_S)
                else:
                    log.warning("chain_lock.lost", chain_id=self._chain_id)
                    break
            except Exception:  # noqa: BLE001
                log.warning("chain_lock.renew_failed", chain_id=self._chain_id)
