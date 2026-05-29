# RPC Node Pool + Failover Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give each chain an ordered pool of HTTP RPC endpoints with failover, a per-request timeout, and a circuit breaker — exposed through config (DB/API/UI) and metrics.

**Architecture:** A new `EndpointPool` class (`core/chains/rpc_pool.py`) owns the failover/health/timeout policy. EVM and Solana adapters build a pool in `connect()` (EVM handle = `AsyncWeb3` instance, Solana handle = URL string) and route each HTTP RPC through `pool.call(fn)`. Config flows through a new `rpc_http_fallbacks` JSON column + `rpc_timeout_ms` from DB → snapshot → repo → API → UI. Two new Prometheus metrics reuse sub-project B's `core/metrics.py`.

**Tech Stack:** Python 3.11+ async, web3.py, httpx, SQLAlchemy + Alembic, prometheus-client, FastAPI, React 19, pytest + pytest-asyncio.

**Reference spec:** `docs/superpowers/specs/2026-05-29-rpc-node-pool-design.md`

---

## File Structure

**New files:**
- `core/chains/rpc_pool.py` — `EndpointPool[H]` + `AllEndpointsFailed`
- `tests/unit/test_rpc_pool.py` — pool behavior (failover, timeout, breaker, degrade)
- `migrations/versions/0008_rpc_node_pool.py` — additive columns

**Modified files:**
- `core/metrics.py` — `RPC_ENDPOINT_UP`, `RPC_FAILOVER_TOTAL`
- `core/settings.py` — `RpcPoolSettings`
- `core/chains/evm.py` — pool in `connect`; RPC methods via `pool.call`; `disconnect` iterates
- `core/chains/solana.py` — pool (URL handles); RPC methods via `pool.call`
- `core/config/models.py` — Chain +`rpc_http_fallbacks`, +`rpc_timeout_ms`
- `core/config/snapshot.py` — SnapshotChain +2 fields; `load_snapshot` reads them
- `core/config/repositories.py` — `ChainRepo.create` +2 params
- `apps/web/schemas.py` — ChainCreate/ChainOut +2 fields; ChainCreate dedup validator
- `apps/web/routers/chains.py` — `create_chain` + `update_chain` thread the 2 fields
- `apps/worker/main.py` — `_make_adapter_factory(settings)` closure passing endpoints + breaker params
- `web/src/pages/Chains.tsx` — form fallbacks textarea + timeout input

**Modified test files:**
- `tests/unit/test_evm_adapter_metrics.py` — failover-transparent fetch
- `tests/integration/test_repositories.py` — chain round-trip with new fields

---

## Chunk 1: EndpointPool core + metrics + settings

Pure additive code with no callers yet. Fully unit-testable in isolation.

### Task 1.1: Add the two pool metrics

**Files:**
- Modify: `core/metrics.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_metrics_module.py`:

```python
def test_rpc_pool_metrics_exist() -> None:
    from core import metrics as M
    assert hasattr(M, "RPC_ENDPOINT_UP")
    assert hasattr(M, "RPC_FAILOVER_TOTAL")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_metrics_module.py::test_rpc_pool_metrics_exist -v`
Expected: FAIL with `AttributeError`.

- [ ] **Step 3: Add the metrics**

In `core/metrics.py`, after the existing RPC metrics (`RPC_REQUESTS_TOTAL`), add:

```python
RPC_ENDPOINT_UP = Gauge(
    "chain_indexer_rpc_endpoint_up",
    "Per-endpoint health (1=healthy, 0=in cooldown).",
    ["chain", "endpoint_index"],
)
RPC_FAILOVER_TOTAL = Counter(
    "chain_indexer_rpc_failover_total",
    "Count of failovers (a call moving from one endpoint to the next).",
    ["chain"],
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_metrics_module.py -v`
Expected: all PASS.

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check core/metrics.py`
Run: `uv run mypy core/metrics.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add core/metrics.py tests/unit/test_metrics_module.py
git commit -m "feat(metrics): add rpc_endpoint_up + rpc_failover_total"
```

### Task 1.2: Add `RpcPoolSettings`

**Files:**
- Modify: `core/settings.py`
- Create: `tests/unit/test_settings_rpc_pool.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_settings_rpc_pool.py`:

```python
from __future__ import annotations

from core.settings import Settings


def test_rpc_pool_defaults() -> None:
    s = Settings()
    assert s.rpc_pool.failure_threshold == 3
    assert s.rpc_pool.cooldown_s == 30.0


def test_rpc_pool_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_RPC_POOL__FAILURE_THRESHOLD", "5")
    monkeypatch.setenv("CHAIN_INDEXER_RPC_POOL__COOLDOWN_S", "12.5")
    s = Settings()
    assert s.rpc_pool.failure_threshold == 5
    assert s.rpc_pool.cooldown_s == 12.5
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_settings_rpc_pool.py -v`
Expected: FAIL with `AttributeError: ... has no attribute 'rpc_pool'`.

- [ ] **Step 3: Add `RpcPoolSettings` + wire into `Settings`**

In `core/settings.py`, add after `MetricsSettings`:

```python
class RpcPoolSettings(BaseModel):
    failure_threshold: int = 3
    cooldown_s: float = 30.0
```

Add to `Settings`:

```python
    rpc_pool: RpcPoolSettings = Field(default_factory=RpcPoolSettings)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_settings_rpc_pool.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Type-check**

Run: `uv run mypy core/settings.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add core/settings.py tests/unit/test_settings_rpc_pool.py
git commit -m "feat(settings): add RpcPoolSettings (breaker threshold + cooldown)"
```

### Task 1.3: Create `EndpointPool`

**Files:**
- Create: `core/chains/rpc_pool.py`
- Create: `tests/unit/test_rpc_pool.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_rpc_pool.py`:

```python
from __future__ import annotations

import asyncio

import pytest

from core.chains.rpc_pool import AllEndpointsFailed, EndpointPool
from core.metrics import RPC_ENDPOINT_UP, RPC_FAILOVER_TOTAL


def _pool(handles, **kw):
    # Use a distinct chain_id per test to isolate metric series.
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

    # 3 calls: each tries h0 first (fails), fails over to h1 (ok).
    for _ in range(3):
        assert await pool.call(fail_h0) == "ok"

    # After 3 consecutive h0 failures, h0 is marked unhealthy.
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

    await pool.call(fn)            # h0 fails (threshold=1 → unhealthy), h1 ok
    tried.clear()
    clock["now"] = 1010.0          # within cooldown
    await pool.call(fn)            # h0 skipped, only h1 tried
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
        await pool.call(fn)        # h0 fails → unhealthy
    assert RPC_ENDPOINT_UP.labels(chain="rs", endpoint_index="0")._value.get() == 0

    clock["now"] = 1040.0          # past cooldown
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
        await pool.call(always_fail)   # both → unhealthy

    # Still within cooldown: both unhealthy. Degrade = try all anyway.
    clock["now"] = 1005.0
    tried: list[str] = []

    async def fn(h):
        tried.append(h)
        return "ok"

    assert await pool.call(fn) == "ok"
    assert tried[0] == "h0"  # degrade tried the full list starting at h0


def test_empty_handles_raises() -> None:
    with pytest.raises(ValueError):
        EndpointPool("x", [])
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_rpc_pool.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'core.chains.rpc_pool'`.

- [ ] **Step 3: Create `core/chains/rpc_pool.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_rpc_pool.py -v`
Expected: all PASS (9 tests).

> If `test_unhealthy_endpoint_skipped_during_cooldown` is flaky because `_mark_failure` calls `time.monotonic()` directly (not the monkeypatched module ref): the monkeypatch targets `mod.time.monotonic`, and the pool calls `time.monotonic()` via the module-level `import time`, so patching `mod.time.monotonic` works. Confirm both `call` and `_mark_failure` read `time.monotonic()` through the module (they do).

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check core/chains/rpc_pool.py tests/unit/test_rpc_pool.py`
Run: `uv run mypy core/chains/rpc_pool.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add core/chains/rpc_pool.py tests/unit/test_rpc_pool.py
git commit -m "feat(rpc): EndpointPool — failover + timeout + circuit breaker"
```

---

## Chunk 2: EVM adapter uses the pool

### Task 2.1: Refactor EVM adapter to build + use a pool

**Files:**
- Modify: `core/chains/evm.py`
- Modify: `tests/unit/test_evm_adapter_metrics.py`

- [ ] **Step 1: Write the failing test (failover transparency)**

Append to `tests/unit/test_evm_adapter_metrics.py`:

```python
@pytest.mark.asyncio
async def test_fetch_block_fails_over_transparently() -> None:
    """With a 2-endpoint pool where endpoint 0 raises, fetch_block still
    returns a Block from endpoint 1."""
    from unittest.mock import AsyncMock, MagicMock

    from core.chains.evm import EvmAdapter
    from core.chains.rpc_pool import EndpointPool

    adapter = EvmAdapter(
        chain_id="fo-eth", rpc_http="http://a", rpc_http_fallbacks=["http://b"],
        rpc_ws=None, confirmations=1,
    )

    # Build two fake web3 handles: h0.get_block raises, h1 returns a raw block.
    raw_block = {
        "number": 7, "hash": "0xabc", "parentHash": "0xdef", "timestamp": 100,
        "transactions": [],
    }
    h0 = MagicMock()
    h0.eth.get_block = AsyncMock(side_effect=RuntimeError("node 0 down"))
    h1 = MagicMock()
    h1.eth.get_block = AsyncMock(return_value=raw_block)

    # Inject a pool directly (bypass connect()).
    adapter._pool = EndpointPool("fo-eth", [h0, h1])

    block = await adapter.fetch_block(7)
    assert block.header.number == 7
    assert block.header.hash == "0xabc"
    h1.eth.get_block.assert_awaited_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_evm_adapter_metrics.py::test_fetch_block_fails_over_transparently -v`
Expected: FAIL — `EvmAdapter.__init__` has no `rpc_http_fallbacks` param yet (TypeError), and `_pool` is not used by `fetch_block`.

- [ ] **Step 3: Refactor `EvmAdapter`**

In `core/chains/evm.py`:

**Imports** — add at module top:

```python
from core.chains.rpc_pool import EndpointPool
```

**`__init__`** — add params and store the URL list:

```python
    def __init__(
        self,
        *,
        chain_id: str,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
        poll_interval_ms: int = 1000,
        rpc_http_fallbacks: list[str] | None = None,
        rpc_timeout_ms: int = 10000,
        failure_threshold: int = 3,
        cooldown_s: float = 30.0,
    ) -> None:
        self.chain_id = chain_id
        self.confirmations = confirmations
        self._http_urls = [rpc_http, *(rpc_http_fallbacks or [])]
        self._rpc_ws = rpc_ws
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._rpc_timeout_s = rpc_timeout_ms / 1000.0
        self._failure_threshold = failure_threshold
        self._cooldown_s = cooldown_s
        self._w3: AsyncWeb3[Any] | None = None
        self._pool: EndpointPool[AsyncWeb3[Any]] | None = None
```

**`connect`**:

```python
    async def connect(self) -> None:
        from web3.middleware import ExtraDataToPOAMiddleware
        handles: list[AsyncWeb3[Any]] = []
        for url in self._http_urls:
            w3 = AsyncWeb3(AsyncHTTPProvider(url))
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            handles.append(w3)
        self._pool = EndpointPool(
            self.chain_id, handles,
            timeout_s=self._rpc_timeout_s,
            failure_threshold=self._failure_threshold,
            cooldown_s=self._cooldown_s,
        )
        self._w3 = handles[0]  # primary, used by _poll_heads + WS subscribe
        # Health probe: block_number is an awaitable property; the closure
        # returns the awaitable (do NOT add ()).
        await self._pool.call(lambda w3: w3.eth.block_number)
```

**`disconnect`**:

```python
    async def disconnect(self) -> None:
        if self._pool is not None:
            for w3 in self._pool.handles():
                with contextlib.suppress(Exception):
                    await w3.provider.disconnect()
            self._pool = None
            self._w3 = None
```

**RPC methods** — route the bare RPC through the pool, parse outside the closure, keep `track_rpc` outermost:

```python
    async def get_latest_block_number(self) -> int:
        assert self._pool is not None
        async with track_rpc(self.chain_id, "eth_blockNumber"):
            return int(await self._pool.call(lambda w3: w3.eth.block_number))

    async def fetch_block(self, number: int) -> Block:
        assert self._pool is not None
        async with track_rpc(self.chain_id, "eth_getBlockByNumber"):
            raw = await self._pool.call(
                lambda w3: w3.eth.get_block(number, full_transactions=True)
            )
            # --- existing parse of `raw` into Block, unchanged, OUTSIDE closure ---
            header = BlockHeader(...)
            ...
            return Block(header=header, txs=txs, logs=[])

    async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
        assert self._pool is not None
        async with track_rpc(self.chain_id, "eth_getLogs"):
            params: dict[str, Any] = {"fromBlock": from_block, "toBlock": to_block}
            if addresses:
                params["address"] = addresses
            if topics:
                params["topics"] = topics
            raw_logs = await self._pool.call(
                lambda w3: w3.eth.get_logs(cast(FilterParams, params))
            )
            # --- existing parse of raw_logs, unchanged ---
```

For `trace_transaction` and `trace_block`: **keep them on `self._w3` (the primary), UNPOOLED.** Do not route them through `pool.call`. Rationale: `trace_transaction` returns `None` on a `-32601` (method-not-found) error — a *capability* signal, not a *health* signal. Routing it through the pool would mark a node that simply lacks `debug_traceTransaction` as "unhealthy" and waste a failover, polluting the circuit breaker. Tracing is an opt-in secondary feature (only runs when `trace_internal_calls=True`); the primary data path (`fetch_block`/`fetch_logs`) is pooled and that's what carries the resilience win. So leave `trace_transaction` / `trace_block` bodies referencing `self._w3` exactly as they are today (they already guard with `assert self._w3` / swallow errors to `None`).

> This is a deliberate deviation from the spec's "trace_block's internal get_block also goes through the pool" line — the reviewer surfaced that pooling trace would corrupt breaker health on `-32601`. Keeping trace on the primary is the correct call. Note it in the commit message.

> **Critical**: only the bare RPC awaitable goes inside `pool.call`. All parsing/dataclass construction stays OUTSIDE so a parse error isn't mistaken for an endpoint failure.

`_poll_heads` and `_subscribe_heads_ws` are UNCHANGED — they keep using `self._w3` (primary) / their own WS provider.

- [ ] **Step 3b: Migrate existing EVM unit tests that set `_w3` directly**

These existing tests set `adapter._w3 = <stub>` and then call methods that now route through `self._pool`, so they need a single-endpoint pool injected. The stub is the SAME object in both, so readbacks via `adapter._w3` still observe calls made through the pool.

In `tests/unit/test_evm_fetch_logs_topics.py` — BOTH tests: after `adapter._w3 = _StubW3()`, add:

```python
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("x", [adapter._w3])  # type: ignore[arg-type]
```

In `tests/unit/test_evm_adapter_metrics.py`:

- `test_get_latest_block_number_records_rpc_metric`: after `type(adapter._w3.eth).block_number = property(lambda self: _bn())`, add:
  ```python
      from core.chains.rpc_pool import EndpointPool
      adapter._pool = EndpointPool("test-eth", [adapter._w3])  # type: ignore[arg-type]
  ```
  The success-counter assertion is unchanged (still 1).

- `test_rpc_error_records_error_status`: inject the pool the same way. **Also change the expected exception**: with a single endpoint that raises, `pool.call` re-raises as `AllEndpointsFailed` (not the bare `RuntimeError`). Change `with pytest.raises(RuntimeError):` to:
  ```python
      from core.chains.rpc_pool import AllEndpointsFailed
      with pytest.raises(AllEndpointsFailed):
          await adapter.get_latest_block_number()
  ```
  The `error`-counter assertion is unchanged — `track_rpc` records `status="error"` for any exception, including `AllEndpointsFailed`.

(The new `test_fetch_block_fails_over_transparently` already injects its own `_pool`, so it needs no change.)

- [ ] **Step 4: Run the new test + existing EVM tests**

Run: `uv run pytest tests/unit/test_evm_adapter_metrics.py tests/integration/test_evm_adapter.py tests/unit/test_evm_catchup_range.py tests/unit/test_evm_fetch_logs_topics.py tests/unit/test_evm_fetch_logs_degrade.py -v`
Expected: all PASS — the unit tests that set `_w3` now also inject a single-endpoint pool (Step 3b); the integration tests use real single-endpoint config (pool with one endpoint = equivalent behavior + timeout).

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check core/chains/evm.py`
Run: `uv run mypy core/chains/evm.py`
Expected: no new errors vs. baseline.

- [ ] **Step 6: Commit**

```bash
git add core/chains/evm.py tests/unit/test_evm_adapter_metrics.py
git commit -m "feat(evm): route RPC through EndpointPool with failover + timeout"
```

---

## Chunk 3: Solana adapter uses the pool

### Task 3.1: Refactor Solana adapter to build + use a pool

**Files:**
- Modify: `core/chains/solana.py`
- Modify: `tests/unit/test_solana_adapter_metrics.py`

- [ ] **Step 1: Write the failing test (failover transparency)**

Append to `tests/unit/test_solana_adapter_metrics.py`:

```python
@pytest.mark.asyncio
async def test_get_blocks_fails_over_transparently() -> None:
    """2-endpoint pool: first URL's POST raises, second returns valid body."""
    from unittest.mock import AsyncMock, MagicMock

    from core.chains.solana import SolanaAdapter
    from core.chains.rpc_pool import EndpointPool

    adapter = SolanaAdapter(
        chain_id="fo-sol", rpc_http="http://a", rpc_http_fallbacks=["http://b"],
        commitment="confirmed",
    )

    good = MagicMock()
    good.raise_for_status = MagicMock()
    good.json = MagicMock(return_value={"jsonrpc": "2.0", "result": [10, 11], "id": 1})

    async def post(url, **kwargs):
        if url == "http://a":
            raise RuntimeError("node a down")
        return good

    adapter._client = MagicMock()
    adapter._client.post = AsyncMock(side_effect=post)
    adapter._pool = EndpointPool("fo-sol", ["http://a", "http://b"])

    result = await adapter.get_blocks(10, 11)
    assert result == [10, 11]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_solana_adapter_metrics.py::test_get_blocks_fails_over_transparently -v`
Expected: FAIL — `SolanaAdapter.__init__` has no `rpc_http_fallbacks` param.

- [ ] **Step 3: Refactor `SolanaAdapter`**

In `core/chains/solana.py`:

**Imports** — add at module top:

```python
from core.chains.rpc_pool import EndpointPool
```

**`__init__`**:

```python
    def __init__(
        self,
        *,
        chain_id: str,
        rpc_http: str,
        commitment: str,
        poll_interval_ms: int = 2000,
        rpc_ws: str | None = None,
        rpc_http_fallbacks: list[str] | None = None,
        rpc_timeout_ms: int = 10000,
        failure_threshold: int = 3,
        cooldown_s: float = 30.0,
    ) -> None:
        self.chain_id = chain_id
        self._http_urls = [rpc_http, *(rpc_http_fallbacks or [])]
        self._rpc_ws = rpc_ws
        self._commitment = _COMMITMENT_MAP[commitment]
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._rpc_timeout_s = rpc_timeout_ms / 1000.0
        self._failure_threshold = failure_threshold
        self._cooldown_s = cooldown_s
        self._client: httpx.AsyncClient | None = None
        self._pool: EndpointPool[str] | None = None
```

(Note: `self._rpc_url` is removed; the pool supplies URLs.)

**`connect`**:

```python
    async def connect(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._rpc_timeout_s)
        self._pool = EndpointPool(
            self.chain_id, self._http_urls,
            timeout_s=self._rpc_timeout_s,
            failure_threshold=self._failure_threshold,
            cooldown_s=self._cooldown_s,
        )
```

**`get_latest_slot`** — preserve the `content=req.to_json()` POST form (NOT `json=`); parse with solders OUTSIDE the closure:

```python
    async def get_latest_slot(self) -> int:
        assert self._client is not None and self._pool is not None
        async with track_rpc(self.chain_id, "getSlot"):
            req = GetSlot(RpcContextConfig(commitment=self._commitment))
            async def _do(url: str) -> str:
                resp = await self._client.post(
                    url, content=req.to_json(),
                    headers={"content-type": "application/json"},
                )
                resp.raise_for_status()
                return resp.text
            text = await self._pool.call(_do)
            parsed = GetSlotResp.from_json(text)
            return parsed.value  # type: ignore[union-attr]
```

**`fetch_block`** — `result is None` (not found) returns None OUTSIDE the closure:

```python
    async def fetch_block(self, slot: int) -> SolanaBlock | None:
        assert self._client is not None and self._pool is not None
        async with track_rpc(self.chain_id, "getBlock"):
            config = {...}  # unchanged
            payload = {"jsonrpc": "2.0", "id": 1, "method": "getBlock", "params": [slot, config]}
            async def _do(url: str) -> dict[str, Any]:
                resp = await self._client.post(
                    url, json=payload, headers={"content-type": "application/json"},
                )
                resp.raise_for_status()
                return resp.json()
            body = await self._pool.call(_do)
            result = body.get("result")
            return None if result is None else self._parse_block(slot, result)
```

**`get_blocks`**:

```python
    async def get_blocks(self, start_slot: int, end_slot: int) -> list[int]:
        assert self._client is not None and self._pool is not None
        async with track_rpc(self.chain_id, "getBlocks"):
            payload = {
                "jsonrpc": "2.0", "id": 1, "method": "getBlocks",
                "params": [start_slot, end_slot, {"commitment": "finalized"}],
            }
            async def _do(url: str) -> dict[str, Any]:
                resp = await self._client.post(
                    url, json=payload, headers={"content-type": "application/json"},
                )
                resp.raise_for_status()
                return resp.json()
            body = await self._pool.call(_do)
            return list(body.get("result") or [])
```

**`disconnect`** — close the shared client + drop pool:

```python
    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._pool = None
```

- [ ] **Step 3b: Migrate existing Solana unit tests that set `_client` directly**

These set `adapter._client` and call methods now routed through `self._pool`, so inject a single-endpoint pool. The closures call `self._client.post(url, ...)` with the pool-supplied URL, so the existing `fake_post(url, ...)` stubs still work.

In `tests/unit/test_solana_get_blocks.py` — BOTH tests: after `adapter._client = ...`, add:

```python
    from core.chains.rpc_pool import EndpointPool
    adapter._pool = EndpointPool("sol", ["http://x"])
```

In `tests/unit/test_solana_adapter_metrics.py` — `test_get_latest_slot_records_rpc_metric`: after `adapter._client = MagicMock()` (with `.post` set), add the same pool injection with the test's chain_id (`"test-sol"`) and a `["http://x"]` URL list.

(The new `test_get_blocks_fails_over_transparently` injects its own `_pool`.)

- [ ] **Step 4: Run the new test + existing Solana tests**

Run: `uv run pytest tests/unit/test_solana_adapter_metrics.py tests/unit/test_solana_get_blocks.py -v`
Run: `uv run pytest tests/ -m "not e2e" -k solana -v`
Expected: all PASS — existing tests now inject a single-endpoint pool (Step 3b).

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check core/chains/solana.py`
Run: `uv run mypy core/chains/solana.py`
Expected: no new errors.

- [ ] **Step 6: Commit**

```bash
git add core/chains/solana.py tests/unit/test_solana_adapter_metrics.py
git commit -m "feat(solana): route RPC through EndpointPool with failover + timeout"
```

---

## Chunk 4: Config plumbing — model, migration, snapshot, repo, schema, router, factory

### Task 4.1: Chain model + migration

**Files:**
- Modify: `core/config/models.py`
- Create: `migrations/versions/0008_rpc_node_pool.py`
- Modify: `tests/integration/test_repositories.py`

- [ ] **Step 1: Write the failing round-trip test**

Append to `tests/integration/test_repositories.py`:

```python
@pytest.mark.asyncio
async def test_chain_rpc_pool_fields_round_trip(db) -> None:
    from core.config.repositories import ChainRepo
    async with db.session() as s:
        repo = ChainRepo(s)
        await repo.create(
            id="eth-pool", kind=ChainKind.evm,
            rpc_http="http://a", rpc_ws=None, confirmations=1, poll_interval_ms=1000,
            enabled=True,
            rpc_http_fallbacks=["http://b", "http://c"],
            rpc_timeout_ms=5000,
        )
        await s.commit()
    async with db.session() as s:
        row = await ChainRepo(s).get("eth-pool")
        assert row is not None
        assert row.rpc_http_fallbacks == ["http://b", "http://c"]
        assert row.rpc_timeout_ms == 5000


@pytest.mark.asyncio
async def test_chain_rpc_pool_fields_default_empty(db) -> None:
    from core.config.repositories import ChainRepo
    async with db.session() as s:
        repo = ChainRepo(s)
        await repo.create(
            id="eth-nopool", kind=ChainKind.evm,
            rpc_http="http://a", rpc_ws=None, confirmations=1, poll_interval_ms=1000,
            enabled=True,
        )
        await s.commit()
    async with db.session() as s:
        row = await ChainRepo(s).get("eth-nopool")
        assert row is not None
        assert row.rpc_http_fallbacks == []
        assert row.rpc_timeout_ms == 10000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_repositories.py -k rpc_pool -v`
Expected: FAIL — `ChainRepo.create` has no `rpc_http_fallbacks`/`rpc_timeout_ms` (and model lacks columns). (Repo change comes in Task 4.3; this test drives both the model and the repo. It's acceptable for the test to stay red until 4.3 — but to keep tasks atomic, we add the MODEL columns here and the REPO params in 4.3. Re-run this test at the end of 4.3.)

> Note: this test depends on BOTH Task 4.1 (model columns) and Task 4.3 (repo params). Write it now; it goes green after 4.3. If you prefer strict per-task green, split: assert columns exist via direct `Chain(...)` construction here, and move the `repo.create(...)` assertion to 4.3.

- [ ] **Step 3: Add the model columns**

In `core/config/models.py`, in the `Chain` class after `slot_query_range_blocks`:

```python
    rpc_http_fallbacks: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    rpc_timeout_ms: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10000, server_default="10000"
    )
```

Confirm `JSON` is already imported in this file (it is — used by `DeliveryRecord.event_payload`).

- [ ] **Step 4: Create the migration**

Create `migrations/versions/0008_rpc_node_pool.py`:

```python
"""add rpc_http_fallbacks and rpc_timeout_ms to chains

Revision ID: 0008
Revises: 0007
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.add_column(
            sa.Column(
                "rpc_http_fallbacks", sa.JSON(), nullable=False, server_default="[]",
            )
        )
        batch.add_column(
            sa.Column(
                "rpc_timeout_ms", sa.Integer(), nullable=False, server_default="10000",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.drop_column("rpc_timeout_ms")
        batch.drop_column("rpc_http_fallbacks")
```

- [ ] **Step 5: Verify migration applies on a fresh DB**

Run: `uv run alembic upgrade head` against a throwaway SQLite (or rely on the test fixture which calls `Base.metadata.create_all`). At minimum:
Run: `uv run python -c "from core.config.models import Chain; print(Chain.rpc_http_fallbacks, Chain.rpc_timeout_ms)"`
Expected: no import error; columns present.

- [ ] **Step 6: Commit**

```bash
git add core/config/models.py migrations/versions/0008_rpc_node_pool.py tests/integration/test_repositories.py
git commit -m "feat(db): chains.rpc_http_fallbacks + rpc_timeout_ms (migration 0008)"
```

### Task 4.2: Snapshot fields

**Files:**
- Modify: `core/config/snapshot.py`

- [ ] **Step 1: Add fields to `SnapshotChain`**

In `core/config/snapshot.py`, in the `SnapshotChain` frozen dataclass (after `slot_query_range_blocks`):

```python
    rpc_http_fallbacks: list[str] = field(default_factory=list)
    rpc_timeout_ms: int = 10000
```

- [ ] **Step 2: Read them in `load_snapshot`**

In the `SnapshotChain(...)` construction inside `load_snapshot`, add:

```python
            rpc_http_fallbacks=c.rpc_http_fallbacks,
            rpc_timeout_ms=c.rpc_timeout_ms,
```

- [ ] **Step 3: Verify existing snapshot tests still pass**

Run: `uv run pytest tests/ -m "not e2e" -k snapshot -v`
Expected: all PASS (new fields have defaults).

- [ ] **Step 4: Type-check**

Run: `uv run mypy core/config/snapshot.py`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add core/config/snapshot.py
git commit -m "feat(snapshot): SnapshotChain carries rpc_http_fallbacks + rpc_timeout_ms"
```

### Task 4.3: Repository create params

**Files:**
- Modify: `core/config/repositories.py`

- [ ] **Step 1: Add params to `ChainRepo.create`**

In `core/config/repositories.py`, extend `ChainRepo.create`'s signature and the `Chain(...)` construction:

```python
    async def create(
        self,
        *,
        id: str,
        kind: ChainKind,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
        poll_interval_ms: int,
        enabled: bool,
        commitment: str | None = None,
        trace_internal_calls: bool = False,
        log_query_range_blocks: int = 100,
        slot_query_range_blocks: int = 1000,
        rpc_http_fallbacks: list[str] | None = None,
        rpc_timeout_ms: int = 10000,
    ) -> Chain:
        c = Chain(
            id=id, kind=kind, rpc_http=rpc_http, rpc_ws=rpc_ws,
            confirmations=confirmations, poll_interval_ms=poll_interval_ms,
            enabled=enabled, commitment=commitment,
            trace_internal_calls=trace_internal_calls,
            log_query_range_blocks=log_query_range_blocks,
            slot_query_range_blocks=slot_query_range_blocks,
            rpc_http_fallbacks=rpc_http_fallbacks if rpc_http_fallbacks is not None else [],
            rpc_timeout_ms=rpc_timeout_ms,
        )
        self.s.add(c)
        await self.s.flush()
        return c
```

`ChainRepo.update(self, chain_id, **fields)` already splats kwargs — no change.

- [ ] **Step 2: Run the round-trip test from Task 4.1**

Run: `uv run pytest tests/integration/test_repositories.py -k rpc_pool -v`
Expected: both tests now PASS.

- [ ] **Step 3: Type-check**

Run: `uv run mypy core/config/repositories.py`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add core/config/repositories.py
git commit -m "feat(repo): ChainRepo.create accepts rpc_http_fallbacks + rpc_timeout_ms"
```

### Task 4.4: API schema + router

**Files:**
- Modify: `apps/web/schemas.py`
- Modify: `apps/web/routers/chains.py`
- Modify: `tests/integration/test_web_api.py` (or a focused new test)

- [ ] **Step 1: Write the failing API test**

Create `tests/integration/test_chains_rpc_pool_api.py`:

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_create_chain_with_fallbacks(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.post("/api/chains", json={
                "id": "eth-api-pool", "kind": "evm", "rpc_http": "http://a",
                "rpc_ws": None, "confirmations": 12, "poll_interval_ms": 3000,
                "rpc_http_fallbacks": ["http://b", "http://a"],  # dup primary
                "rpc_timeout_ms": 7000, "enabled": True,
            })
            assert r.status_code == 201
            body = r.json()
            # primary deduped out of fallbacks
            assert body["rpc_http_fallbacks"] == ["http://b"]
            assert body["rpc_timeout_ms"] == 7000
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_chains_rpc_pool_api.py -v`
Expected: FAIL — schema lacks the fields / no dedup.

- [ ] **Step 3: Extend schemas**

In `apps/web/schemas.py`, add to `ChainCreate` (after `slot_query_range_blocks`):

```python
    rpc_http_fallbacks: list[str] = Field(default_factory=list)
    rpc_timeout_ms: int = Field(ge=100, le=120_000, default=10000)
```

Add to the existing `@model_validator(mode="after")` (or a new one) a dedup/non-empty step:

```python
    @model_validator(mode="after")
    def _normalize_fallbacks(self) -> ChainCreate:
        cleaned = [u.strip() for u in self.rpc_http_fallbacks if u and u.strip()]
        # de-dup, preserve order, drop the primary if listed
        seen: set[str] = set()
        out: list[str] = []
        for u in cleaned:
            if u != self.rpc_http and u not in seen:
                seen.add(u)
                out.append(u)
        object.__setattr__(self, "rpc_http_fallbacks", out)
        return self
```

> Pydantic v2 models are mutable by default (not frozen here), so `self.rpc_http_fallbacks = out` works; `object.__setattr__` is a safe belt-and-suspenders. Confirm the existing `_check_kind_fields` validator stays intact (both validators run).

Add to `ChainOut`:

```python
    rpc_http_fallbacks: list[str]
    rpc_timeout_ms: int
```

- [ ] **Step 4: Thread fields through the router**

In `apps/web/routers/chains.py`, `create_chain` — add to the `repo.create(...)` call:

```python
        rpc_http_fallbacks=payload.rpc_http_fallbacks,
        rpc_timeout_ms=payload.rpc_timeout_ms,
```

`update_chain` — add to the `repo.update(...)` call:

```python
        rpc_http_fallbacks=payload.rpc_http_fallbacks,
        rpc_timeout_ms=payload.rpc_timeout_ms,
```

- [ ] **Step 5: Run tests to verify pass**

Run: `uv run pytest tests/integration/test_chains_rpc_pool_api.py tests/integration/test_web_api.py -v`
Expected: all PASS.

- [ ] **Step 6: Lint + type-check**

Run: `uv run ruff check apps/web/schemas.py apps/web/routers/chains.py`
Run: `uv run mypy apps/web/schemas.py apps/web/routers/chains.py`
Expected: no new errors.

- [ ] **Step 7: Commit**

```bash
git add apps/web/schemas.py apps/web/routers/chains.py tests/integration/test_chains_rpc_pool_api.py
git commit -m "feat(api): chains accept rpc_http_fallbacks (deduped) + rpc_timeout_ms"
```

### Task 4.5: Adapter factory wiring

**Files:**
- Modify: `apps/worker/main.py`

- [ ] **Step 1: Convert `_default_adapter_factory` into a settings-aware closure**

In `apps/worker/main.py`, replace the module-level `_default_adapter_factory` with a closure builder mirroring `_make_channel_factory(bus)`:

```python
def _make_adapter_factory(settings: Settings) -> Callable[[SnapshotChain], EvmAdapter | SolanaAdapter]:
    pool = settings.rpc_pool

    def factory(cfg: SnapshotChain) -> EvmAdapter | SolanaAdapter:
        common = dict(
            rpc_http_fallbacks=cfg.rpc_http_fallbacks,
            rpc_timeout_ms=cfg.rpc_timeout_ms,
            failure_threshold=pool.failure_threshold,
            cooldown_s=pool.cooldown_s,
        )
        if cfg.kind == "evm":
            return EvmAdapter(
                chain_id=cfg.id, rpc_http=cfg.rpc_http, rpc_ws=cfg.rpc_ws,
                confirmations=cfg.confirmations, poll_interval_ms=cfg.poll_interval_ms,
                **common,
            )
        if cfg.kind == "solana":
            assert cfg.commitment is not None, "Solana chain must have commitment set"
            return SolanaAdapter(
                chain_id=cfg.id, rpc_http=cfg.rpc_http, commitment=cfg.commitment,
                poll_interval_ms=cfg.poll_interval_ms, rpc_ws=cfg.rpc_ws,
                **common,
            )
        raise NotImplementedError(f"chain kind {cfg.kind!r} not supported")

    return factory
```

Update `_reconcile`'s `ChainRunner(...)` construction to use `adapter_factory=_make_adapter_factory(self._settings)` (the worker already has `self._settings`). If the runner is constructed in one place, build the factory once in `__init__` or inline at the call site (mirror how `_make_channel_factory(self._bus)` is called).

> Keep the old `_default_adapter_factory` only if other call sites use it; otherwise remove it. Grep first: `grep -rn _default_adapter_factory apps tests`.

- [ ] **Step 2: Run worker tests**

Run: `uv run pytest tests/ -m "not e2e" -k "worker or chain_runner or reconcile" -v`
Expected: all PASS. Adjust any test that referenced `_default_adapter_factory` directly.

- [ ] **Step 3: Type-check + lint**

Run: `uv run ruff check apps/worker/main.py`
Run: `uv run mypy apps/worker/main.py`
Expected: no new errors.

- [ ] **Step 4: Commit**

```bash
git add apps/worker/main.py
git commit -m "feat(worker): adapter factory passes pool endpoints + breaker params"
```

---

## Chunk 5: UI + final verification

### Task 5.1: Chains form — fallbacks + timeout

**Files:**
- Modify: `web/src/pages/Chains.tsx`

- [ ] **Step 1: Read current `ChainForm` to confirm structure**

It uses `FormData` (`fd.get('rpc_http')`, etc.) and `mut.mutate({...})`. The `Chain` TS interface is partial (omits `log_query_range_blocks`).

- [ ] **Step 2: Apply the edits**

In `web/src/pages/Chains.tsx`:

**A. Extend the `Chain` interface:**

```tsx
interface Chain {
  id: string; kind: string; rpc_http: string; rpc_ws: string | null
  confirmations: number; poll_interval_ms: number; commitment: string | null
  rpc_http_fallbacks?: string[]; rpc_timeout_ms?: number
  enabled?: boolean
}
```

(Keep whatever fields already exist; add the two new optional ones.)

**B. Add form inputs** in `ChainForm` (after the `rpc_ws` input, near line 70):

```tsx
        <textarea
          name="rpc_http_fallbacks"
          defaultValue={(initial?.rpc_http_fallbacks ?? []).join('\n')}
          placeholder="备用 RPC HTTP 地址（每行一个，可选）"
          rows={2}
          className="w-full border rounded px-3 py-1.5 text-sm font-mono"
        />
        <input
          name="rpc_timeout_ms" type="number"
          defaultValue={initial?.rpc_timeout_ms ?? 10000}
          placeholder="单请求超时 (ms)"
          className="w-full border rounded px-3 py-1.5 text-sm"
        />
```

**C. Include both in the submit payload.** In the `onSubmit` handler's `mut.mutate({...})`:

```tsx
    const fallbacksRaw = String(fd.get('rpc_http_fallbacks') ?? '')
    const rpc_http_fallbacks = fallbacksRaw
      .split('\n').map(s => s.trim()).filter(Boolean)
    mut.mutate({
      ...,  // existing fields
      rpc_http_fallbacks,
      rpc_timeout_ms: Number(fd.get('rpc_timeout_ms')) || 10000,
    })
```

- [ ] **Step 3: Build the frontend**

```bash
cd web && npm run build
```
Expected: build succeeds, no TypeScript errors.

- [ ] **Step 4: Commit**

```bash
cd /Users/user/yu/code/chain_indexer/.claude/worktrees/<this-worktree>
git add web/src/pages/Chains.tsx
git commit -m "feat(web): Chains form — RPC fallback endpoints + timeout"
```

(Use the actual worktree path from your shell `pwd`.)

### Task 5.2: Full-suite green-light

- [ ] **Step 1: Lint**

Run: `uv run ruff check core apps tests`
Expected: same or fewer errors than the `main` baseline (no NEW errors).

- [ ] **Step 2: Type-check**

Run: `uv run mypy core apps`
Expected: no NEW errors vs. baseline.

- [ ] **Step 3: Full unit + integration suite**

Run: `uv run pytest tests/ -m "not e2e" --tb=line -q`
Expected: all PASS (environmental Docker/testcontainer failures aside).

- [ ] **Step 4: Frontend build**

```bash
cd web && npm run build
```

- [ ] **Step 5: Inspect git log**

Run: `git log --oneline main..HEAD`
Expected: one clean commit per task.

- [ ] **Step 6: Final fix-up commit (only if needed)**

If lint/type baselines drifted, fix and commit with a `chore:` prefix. Do not amend prior task commits.

---

## Out of scope (do not implement)

- JSON-RPC batch requests.
- Concurrent block fetching during catchup.
- WS endpoint pooling (WS stays single + HTTP-poll fallback).
- Weighted / latency-aware routing.
- Per-endpoint rate limiting.
- Dynamic endpoint discovery / runtime add-remove.
- URL-format validation on fallbacks (only non-empty + dedup).
- Per-endpoint p99 latency metric (would add an `endpoint_index` label to the RPC histogram).

## References

- Spec: `docs/superpowers/specs/2026-05-29-rpc-node-pool-design.md`
- `EvmAdapter`: `core/chains/evm.py:45` (init 48, connect 65, disconnect 72, get_latest_block_number 80, fetch_block 85, fetch_logs 118, trace_transaction 215, trace_block 231)
- `SolanaAdapter`: `core/chains/solana.py:29` (init 30, connect 46, get_latest_slot 54, fetch_block 68, get_blocks 95)
- `Chain` model: `core/config/models.py:68`
- Latest migration: `migrations/versions/0007_rename_failed_deliveries.py` (revision "0007")
- `SnapshotChain` + `load_snapshot`: `core/config/snapshot.py:40`, `:87`
- `ChainRepo.create`: `core/config/repositories.py:30`
- `ChainCreate`/`ChainOut`: `apps/web/schemas.py:21`, `:43`
- `create_chain`/`update_chain`: `apps/web/routers/chains.py:16`, `:42`
- `_default_adapter_factory` / `_make_channel_factory`: `apps/worker/main.py:33`, `:54`
- `core/metrics.py` (sub-project B): Counter/Gauge pattern
- `web/src/pages/Chains.tsx:52` (ChainForm)
