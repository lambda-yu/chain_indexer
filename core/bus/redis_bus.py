from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis


class RedisBus:
    def __init__(self, url: str) -> None:
        self._url = url
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(self._url, decode_responses=True)
        await self._client.ping()  # type: ignore[misc]

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> aioredis.Redis:
        assert self._client is not None, "RedisBus.connect() must be called first"
        return self._client

    async def ping(self) -> bool:
        """Return True if Redis is reachable. Used by the web `/healthz` route
        (Chunk 9 Task 9.1). Swallows exceptions and returns False on failure
        so callers can render a fail-open health body."""
        if self._client is None:
            return False
        try:
            return bool(await self._client.ping())  # type: ignore[misc]
        except Exception:
            return False

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        assert self._client is not None
        await self._client.publish(channel, json.dumps(payload))

    async def subscribe(
        self, channel: str, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield JSON-decoded messages from `channel`.

        Pass `ready` to be notified once the subscription is attached (useful
        in tests to avoid sleep-based races). Always call `.aclose()` on the
        returned generator when done to unsubscribe.
        """
        assert self._client is not None
        pubsub = self._client.pubsub()
        await pubsub.subscribe(channel)
        if ready is not None:
            ready.set()
        try:
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                data = msg.get("data")
                if isinstance(data, bytes):
                    data = data.decode()
                yield json.loads(data)
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()  # type: ignore[no-untyped-call]
