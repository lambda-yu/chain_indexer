from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Generic, TypeVar

import structlog

from core.metrics import RPC_ENDPOINT_UP, RPC_FAILOVER_TOTAL

log = structlog.get_logger(__name__)

H = TypeVar("H")
T = TypeVar("T")


class AllEndpointsFailed(Exception):
    """Raised when every endpoint failed for one call. __cause__ = last error."""


@dataclass
class _EndpointState(Generic[H]):
    handle: H
    index: int
    consecutive_failures: int = 0
    unhealthy_until: float = 0.0  # time.monotonic() deadline; 0 = healthy


class EndpointPool(Generic[H]):
    """Ordered HTTP RPC endpoint pool with failover + circuit breaker.

    `call(fn)` tries endpoints in priority order, bounding each attempt with
    `timeout_s`. On failure it fails over to the next endpoint and bumps a
    consecutive-failure counter; once an endpoint trips `failure_threshold`
    it is skipped for `cooldown_s`. Any success resets the endpoint to healthy.
    When all endpoints are cooling down, the pool degrades to trying them all.
    """

    def __init__(
        self,
        chain_id: str,
        handles: list[H],
        *,
        timeout_s: float = 10.0,
        failure_threshold: int = 3,
        cooldown_s: float = 30.0,
    ) -> None:
        if not handles:
            raise ValueError("EndpointPool requires at least one endpoint")
        self._chain_id = chain_id
        self._timeout_s = timeout_s
        self._failure_threshold = failure_threshold
        self._cooldown_s = cooldown_s
        self._eps: list[_EndpointState[H]] = [
            _EndpointState(handle=h, index=i) for i, h in enumerate(handles)
        ]
        for ep in self._eps:
            RPC_ENDPOINT_UP.labels(
                chain=chain_id, endpoint_index=str(ep.index)
            ).set(1)

    def handles(self) -> list[H]:
        return [ep.handle for ep in self._eps]

    def _candidates(self, now: float) -> list[_EndpointState[H]]:
        healthy = [e for e in self._eps if e.unhealthy_until <= now]
        return healthy or list(self._eps)

    async def call(self, fn: Callable[[H], Awaitable[T]]) -> T:
        now = time.monotonic()
        candidates = self._candidates(now)
        last_exc: BaseException | None = None
        for i, ep in enumerate(candidates):
            try:
                result = await asyncio.wait_for(
                    fn(ep.handle), timeout=self._timeout_s
                )
                self._mark_success(ep)
                return result
            except Exception as exc:  # noqa: BLE001 — failover surface
                last_exc = exc
                self._mark_failure(ep)
                if i + 1 < len(candidates):
                    RPC_FAILOVER_TOTAL.labels(chain=self._chain_id).inc()
                    log.warning(
                        "rpc_pool.failover",
                        chain=self._chain_id,
                        from_index=ep.index,
                        error=repr(exc),
                    )
        out = AllEndpointsFailed(f"all {len(candidates)} endpoints failed")
        out.__cause__ = last_exc
        raise out

    def _mark_success(self, ep: _EndpointState[H]) -> None:
        if ep.consecutive_failures or ep.unhealthy_until:
            ep.consecutive_failures = 0
            ep.unhealthy_until = 0.0
            RPC_ENDPOINT_UP.labels(
                chain=self._chain_id, endpoint_index=str(ep.index)
            ).set(1)

    def _mark_failure(self, ep: _EndpointState[H]) -> None:
        ep.consecutive_failures += 1
        if ep.consecutive_failures >= self._failure_threshold:
            ep.unhealthy_until = time.monotonic() + self._cooldown_s
            RPC_ENDPOINT_UP.labels(
                chain=self._chain_id, endpoint_index=str(ep.index)
            ).set(0)
