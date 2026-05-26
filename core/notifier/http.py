from __future__ import annotations

import hashlib
import hmac
import json
from functools import partial
from typing import Any

import httpx
import structlog

from core.notifier.channel import Channel
from core.notifier.retry import RetryAbort, retry_with_backoff

log = structlog.get_logger(__name__)

_RETRYABLE_STATUS = {408, 429}


class HttpChannel(Channel):
    """POST the payload as JSON. Adds optional `X-Signature: sha256=<hex>` HMAC.

    Retry policy (spec §9):
    - 5xx / network error → retried with exponential backoff (1/4/16s by default)
    - 4xx → not retried, except 408 (request timeout) and 429 (rate limited)
    """

    type = "http"

    def __init__(self, *, config: dict[str, Any], bus: object = None, base_delay: float = 1.0) -> None:
        del bus
        self._url: str = config["url"]
        self._method: str = config.get("method", "POST").upper()
        self._headers: dict[str, str] = dict(config.get("headers", {}))
        self._hmac_secret: str | None = config.get("hmac_secret")
        self._timeout: float = float(config.get("timeout_seconds", 10.0))
        self._base_delay = base_delay
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, payload: dict[str, Any]) -> None:
        assert self._client is not None, "HttpChannel.start() must be called first"
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = dict(self._headers)
        headers.setdefault("Content-Type", "application/json")
        if self._hmac_secret:
            sig = hmac.new(self._hmac_secret.encode(), body, hashlib.sha256).hexdigest()
            headers["X-Signature"] = f"sha256={sig}"

        await retry_with_backoff(
            partial(self._post_once, body=body, headers=headers),
            max_attempts=3,
            base_delay=self._base_delay,
        )

    async def _post_once(self, *, body: bytes, headers: dict[str, str]) -> None:
        assert self._client is not None
        resp = await self._client.request(self._method, self._url, content=body, headers=headers)
        if 200 <= resp.status_code < 300:
            return
        if 400 <= resp.status_code < 500 and resp.status_code not in _RETRYABLE_STATUS:
            raise RetryAbort(f"http {resp.status_code} from {self._url}")
        # 5xx or 408/429: raise a normal exception → retry path
        raise RuntimeError(f"http {resp.status_code} from {self._url}")
