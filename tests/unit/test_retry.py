from __future__ import annotations

import pytest

from core.notifier.retry import RetryAbort, RetryExhausted, retry_with_backoff


@pytest.mark.asyncio
async def test_succeeds_on_first_try() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    out = await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert out == "ok"
    assert calls == 1


@pytest.mark.asyncio
async def test_retries_on_retryable_error_then_succeeds() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")
        return "ok"

    out = await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert out == "ok"
    assert calls == 3


@pytest.mark.asyncio
async def test_exhausts_after_max_attempts() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("always fails")

    with pytest.raises(RetryExhausted) as exc:
        await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert calls == 3
    assert "always fails" in str(exc.value.__cause__)


@pytest.mark.asyncio
async def test_retry_abort_short_circuits() -> None:
    """RetryAbort signals "do not retry" (e.g. HTTP 4xx)."""
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise RetryAbort("client error 404")

    with pytest.raises(RetryAbort):
        await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert calls == 1


@pytest.mark.asyncio
async def test_backoff_uses_exponential_delays() -> None:
    """With base_delay=0.01, delays are 0.01, 0.04. We patch sleep to capture them."""
    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    async def op() -> str:
        raise RuntimeError("nope")

    with pytest.raises(RetryExhausted):
        await retry_with_backoff(op, max_attempts=3, base_delay=0.01, factor=4.0, sleep=fake_sleep)
    assert sleeps == [0.01, 0.04]
