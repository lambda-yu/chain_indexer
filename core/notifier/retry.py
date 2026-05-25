from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

log = structlog.get_logger(__name__)
T = TypeVar("T")


class RetryAbort(Exception):
    """Raised by an operation to signal "do not retry me" (e.g. HTTP 4xx)."""


class RetryExhausted(Exception):
    """Raised when all retry attempts have been used up. `__cause__` carries the last error."""


async def retry_with_backoff(
    op: Callable[[], Awaitable[T]],
    *,
    max_attempts: int,
    base_delay: float,
    factor: float = 4.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run `op` up to `max_attempts` times with exponential backoff.

    `op` must be a no-arg async callable (typically a `functools.partial` or lambda).
    Raises `RetryAbort` directly without retrying. Other exceptions are retried
    until `max_attempts`, then re-raised as `RetryExhausted` with the last error
    as `__cause__`.

    Default factor=4 with `max_attempts=3` and `base_delay=1.0` yields actual
    sleeps of 1s and 4s between three attempts (the third attempt fires
    immediately and either succeeds or raises `RetryExhausted`). Spec §9's
    "1/4/16s" describes the geometric sequence; with 3 attempts only the first
    two delays are ever realized.
    """
    last_err: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await op()
        except RetryAbort:
            raise
        except Exception as e:  # noqa: BLE001 — generic retry surface
            last_err = e
            if attempt >= max_attempts:
                break
            delay = base_delay * (factor ** (attempt - 1))
            log.warning(
                "retry.attempt_failed",
                attempt=attempt,
                max_attempts=max_attempts,
                next_delay=delay,
                error=str(e),
            )
            await sleep(delay)
    out = RetryExhausted(f"giving up after {max_attempts} attempts")
    out.__cause__ = last_err
    raise out
