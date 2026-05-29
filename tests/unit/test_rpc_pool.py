from __future__ import annotations

import asyncio

import pytest

from core.chains.rpc_pool import AllEndpointsFailed, EndpointPool
from core.metrics import RPC_ENDPOINT_UP, RPC_FAILOVER_TOTAL


def _pool(handles, **kw):
    return EndpointPool("test-pool", handles, **kw)


@pytest.mark.asyncio
async def test_single_endpoint_success() -> None:
    pool = _pool(["h0"])

    async def fn(h):
        return f"ok-{h}"

    assert await pool.call(fn) == "ok-h0"


@pytest.mark.asyncio
async def test_failover_to_second_on_first_failure() -> None:
    pool = EndpointPool("fchain", ["h0", "h1"])
    before = RPC_FAILOVER_TOTAL.labels(chain="fchain")._value.get()

    async def fn(h):
        if h == "h0":
            raise RuntimeError("down")
        return "ok-h1"

    assert await pool.call(fn) == "ok-h1"
    after = RPC_FAILOVER_TOTAL.labels(chain="fchain")._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_all_endpoints_fail_raises() -> None:
    pool = _pool(["h0", "h1"])

    async def fn(h):
        raise RuntimeError(f"down-{h}")

    with pytest.raises(AllEndpointsFailed) as exc:
        await pool.call(fn)
    assert isinstance(exc.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_timeout_triggers_failover() -> None:
    pool = _pool(["slow", "fast"], timeout_s=0.05)

    async def fn(h):
        if h == "slow":
            await asyncio.sleep(0.2)
            return "slow-result"
        return "fast-result"

    assert await pool.call(fn) == "fast-result"


@pytest.mark.asyncio
async def test_circuit_breaker_trips_after_threshold() -> None:
    pool = EndpointPool("bchain", ["h0", "h1"], failure_threshold=3, cooldown_s=100.0)

    async def fail_h0(h):
        if h == "h0":
            raise RuntimeError("down")
        return "ok"

    for _ in range(3):
        assert await pool.call(fail_h0) == "ok"

    assert RPC_ENDPOINT_UP.labels(chain="bchain", endpoint_index="0")._value.get() == 0


@pytest.mark.asyncio
async def test_unhealthy_endpoint_skipped_during_cooldown(monkeypatch) -> None:
    import core.chains.rpc_pool as mod

    clock = {"now": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["now"])

    pool = EndpointPool("cd", ["h0", "h1"], failure_threshold=1, cooldown_s=30.0)
    tried: list[str] = []

    async def fn(h):
        tried.append(h)
        if h == "h0":
            raise RuntimeError("down")
        return "ok"

    await pool.call(fn)
    tried.clear()
    clock["now"] = 1010.0
    await pool.call(fn)
    assert tried == ["h1"]


@pytest.mark.asyncio
async def test_success_resets_health(monkeypatch) -> None:
    import core.chains.rpc_pool as mod

    clock = {"now": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["now"])

    pool = EndpointPool("rs", ["h0"], failure_threshold=1, cooldown_s=30.0)
    state = {"fail": True}

    async def fn(h):
        if state["fail"]:
            raise RuntimeError("down")
        return "ok"

    with pytest.raises(AllEndpointsFailed):
        await pool.call(fn)
    assert RPC_ENDPOINT_UP.labels(chain="rs", endpoint_index="0")._value.get() == 0

    clock["now"] = 1040.0
    state["fail"] = False
    assert await pool.call(fn) == "ok"
    assert RPC_ENDPOINT_UP.labels(chain="rs", endpoint_index="0")._value.get() == 1


@pytest.mark.asyncio
async def test_all_unhealthy_degrades_to_try_all(monkeypatch) -> None:
    import core.chains.rpc_pool as mod

    clock = {"now": 1000.0}
    monkeypatch.setattr(mod.time, "monotonic", lambda: clock["now"])

    pool = EndpointPool("deg", ["h0", "h1"], failure_threshold=1, cooldown_s=30.0)

    async def always_fail(h):
        raise RuntimeError("down")

    with pytest.raises(AllEndpointsFailed):
        await pool.call(always_fail)

    clock["now"] = 1005.0
    tried: list[str] = []

    async def fn(h):
        tried.append(h)
        return "ok"

    assert await pool.call(fn) == "ok"
    assert tried[0] == "h0"


def test_empty_handles_raises() -> None:
    with pytest.raises(ValueError):
        EndpointPool("x", [])
