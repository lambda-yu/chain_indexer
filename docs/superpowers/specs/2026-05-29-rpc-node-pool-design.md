# RPC Node Pool + Failover — Design

**Date**: 2026-05-29
**Status**: Draft
**Scope**: Multi-endpoint HTTP RPC per chain with ordered failover, per-request timeout, and a circuit breaker. Surfaced in config (DB + API + UI) and instrumented with metrics.
**Milestone**: post-m5 follow-up (sub-project A of three; B=observability done, C=subscription lifecycle to follow)

## Background

Timing instrumentation (sub-project B) confirmed that RPC dominates worker wall-clock: per-block HTTP RPC ran ~1.5s at p50 with a 32s max tail. Both chain adapters currently bind to a single HTTP endpoint:

- `EvmAdapter` creates one `AsyncWeb3(AsyncHTTPProvider(rpc_http))` in `connect()`.
- `SolanaAdapter` POSTs to a single `self._rpc_url` via a shared `httpx.AsyncClient`.

A single slow or flaky endpoint therefore stalls the entire chain runner with no recourse, and the default web3/httpx timeouts (effectively tens of seconds) let one bad request block a block for the full tail we observed.

This design adds a per-chain **ordered HTTP endpoint pool** with **failover**, a **per-request timeout** (to cut the long tail directly), and a lightweight **circuit breaker** (skip an endpoint that is consistently failing, retry it after a cooldown). It deliberately does NOT add JSON-RPC batching or concurrent block fetching — a prior spec (`2026-05-27-rpc-frequency-optimization-design.md`) judged batching low-ROI, and the user confirmed scoping this sub-project to the node-pool reliability win only.

## Goals

- Each chain accepts a primary `rpc_http` plus an ordered list of HTTP fallback endpoints.
- RPC HTTP calls try endpoints in priority order; on failure they fail over to the next.
- Each attempt is bounded by a configurable per-request timeout (default 10s), directly addressing the 32s tail.
- A circuit breaker marks an endpoint unhealthy after N consecutive failures and skips it for a cooldown window; any success resets it to healthy.
- When every endpoint is unhealthy, the pool degrades to trying them all anyway rather than hard-failing.
- The failover policy lives in one reusable `EndpointPool` class so EVM and Solana share identical behavior despite different transports.
- New metrics expose per-endpoint health and failover frequency (reusing sub-project B's `core/metrics.py`), labeled by endpoint INDEX (never the URL, which contains API keys).
- Config is exposed through the DB model, snapshot, repository, API schema, and the Chains UI form.

## Non-goals

- No JSON-RPC batch requests (prior spec: low ROI; user confirmed out).
- No concurrent block fetching during catchup (Package B; deferred).
- No WS endpoint pooling. WS stays single-endpoint; the existing HTTP-polling fallback (`_poll_heads`) covers WS outages. Only HTTP data-query calls (`fetch_block`, `fetch_logs`, `trace_*`, `get_latest_block_number`, Solana `getBlock`/`getBlocks`/`getSlot`) go through the pool.
- No weighted/priority routing beyond simple ordered fallback.
- No per-endpoint rate limiting.
- No dynamic endpoint discovery / runtime add-remove (config-driven; hot-reload already rebuilds the snapshot and reconciles runners).
- No URL-format validation on fallback entries (RPC URLs vary widely; only non-empty + dedup checks).
- No per-endpoint metric labeled by URL (cardinality + credential leak). Index only.
- No retry of business-level "not found" results (a `null` block result returns `None`, not an exception, and never triggers failover).

## Architecture

```
                  ┌──────────────────────────────────────────┐
                  │ EvmAdapter / SolanaAdapter                │
                  │   connect():                              │
                  │     build N transport handles             │
                  │     EndpointPool(chain, handles, timeout) │
                  │   fetch_block / fetch_logs / ... :        │
                  │     async with track_rpc(chain, method):  │  ← sub-project B
                  │       raw = await pool.call(fn)           │
                  │       return parse(raw)                   │
                  └────────────────┬─────────────────────────┘
                                   │ pool.call(fn)
                                   ▼
        ┌────────────────────────────────────────────────────┐
        │ EndpointPool[H]  (core/chains/rpc_pool.py)           │
        │  ordered _EndpointState[] (handle, index, health)    │
        │  call(fn):                                           │
        │    for ep in candidates(now):                        │
        │      try: await wait_for(fn(ep.handle), timeout)     │
        │           mark_success; return                       │
        │      except: mark_failure; failover→next             │
        │    raise AllEndpointsFailed                          │
        │  candidates = healthy, or ALL if none healthy        │
        └────────────────┬─────────────────────────────────────┘
                         │ emits
                         ▼
            RPC_ENDPOINT_UP{chain,endpoint_index}
            RPC_FAILOVER_TOTAL{chain}
```

`H` is the transport handle type: `AsyncWeb3` for EVM (one instance per endpoint), `str` (the URL) for Solana (one shared httpx client; URL chosen per call). The pool owns the failover/health/timeout policy; the adapter owns the transport-specific call closure and result parsing.

## Component 1: `EndpointPool`

**Location**: `core/chains/rpc_pool.py` (new)

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
            RPC_ENDPOINT_UP.labels(chain=chain_id, endpoint_index=str(ep.index)).set(1)

    def handles(self) -> list[H]:
        return [ep.handle for ep in self._eps]

    def _candidates(self, now: float) -> list[_EndpointState[H]]:
        healthy = [e for e in self._eps if e.unhealthy_until <= now]
        return healthy or list(self._eps)  # degrade: try all if none healthy

    async def call(self, fn: Callable[[H], Awaitable[T]]) -> T:
        now = time.monotonic()
        candidates = self._candidates(now)
        last_exc: BaseException | None = None
        for i, ep in enumerate(candidates):
            try:
                result = await asyncio.wait_for(fn(ep.handle), timeout=self._timeout_s)
                self._mark_success(ep)
                return result
            except Exception as exc:  # noqa: BLE001 — failover surface
                last_exc = exc
                self._mark_failure(ep)
                if i + 1 < len(candidates):
                    RPC_FAILOVER_TOTAL.labels(chain=self._chain_id).inc()
                    log.warning(
                        "rpc_pool.failover",
                        chain=self._chain_id, from_index=ep.index, error=repr(exc),
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

**Key properties:**
- `asyncio.wait_for(..., timeout=self._timeout_s)` per attempt → cuts the long tail.
- A `TimeoutError` from `wait_for` is an `Exception`, so it triggers failover like any other error.
- Consecutive-failure counter trips the breaker; any success resets health immediately.
- `_candidates` degrades to the full list when all endpoints are cooling down — better to try than to hard-fail.
- `AllEndpointsFailed.__cause__` carries the last error, so the outer `track_rpc` (sub-project B) records it as `status="error"`.
- Endpoints identified by integer index in metrics, never URL.

**Concurrency note**: `_EndpointState` mutation (failure counters, health flags) happens from the single asyncio event loop. Multiple concurrent `pool.call(...)` invocations interleave at `await` points, but the mutations themselves are synchronous (no `await` between read and write of the counter), so no lock is needed. Worst case under concurrency: a transient over- or under-count of one failure, which self-corrects on the next success/failure. This is acceptable for a health heuristic.

## Component 2: EVM adapter refactor

**Location**: `core/chains/evm.py`

`__init__` gains `rpc_http_fallbacks: list[str] | None = None`, `rpc_timeout_ms: int = 10000`, and optional `failure_threshold` / `cooldown_s` (passed from the factory). It stores `self._http_urls = [rpc_http, *(rpc_http_fallbacks or [])]`.

`connect()` builds one `AsyncWeb3` per URL, injects PoA middleware into each, constructs the pool, retains the primary handle for WS/poll use, and probes health:

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
    await self._pool.call(lambda w3: w3.eth.block_number)  # health probe
```

Each HTTP RPC method delegates the bare RPC call through the pool, keeping `track_rpc` outermost and parsing OUTSIDE the closure (so a parse error is not mistaken for an endpoint failure):

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
        # ... existing parse of `raw` into Block (unchanged, outside the closure)

async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
    assert self._pool is not None
    async with track_rpc(self.chain_id, "eth_getLogs"):
        params = {...}
        raw_logs = await self._pool.call(lambda w3: w3.eth.get_logs(cast(FilterParams, params)))
        # ... existing parse (unchanged)
```

`trace_transaction` and `trace_block` likewise route their RPC call through the pool (labels `"debug_traceTransaction"` / `"trace_block"` preserved from sub-project B). `trace_block`'s internal `eth.get_block` for the full-tx body also goes through the pool (it's a real RPC); the existing single-`get_block` semantics are kept, just pooled.

`disconnect()` iterates all handles:

```python
async def disconnect(self) -> None:
    if self._pool is not None:
        for w3 in self._pool.handles():
            with contextlib.suppress(Exception):
                await w3.provider.disconnect()
        self._pool = None
        self._w3 = None
```

`_poll_heads` and `_subscribe_heads_ws` keep using `self._w3` (the primary). They are NOT pooled — WS is single-endpoint by design, and `_poll_heads` is the existing HTTP fallback. (If the primary's HTTP is down, head-polling stalls, but `get_latest_block_number` — used by catchup — is pooled and still works; head-following resumes when the primary recovers. This is an accepted v1 limitation, documented.)

## Component 3: Solana adapter refactor

**Location**: `core/chains/solana.py`

Handle type is `str` (the URL). One shared `httpx.AsyncClient`; the pool picks which URL to POST to.

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

Each method wraps its POST in a closure; `raise_for_status()` goes INSIDE the closure (5xx → exception → failover), and the `result is None` "block not found" check goes OUTSIDE (returns `None`, no failover):

```python
async def fetch_block(self, slot: int) -> SolanaBlock | None:
    assert self._client is not None and self._pool is not None
    async with track_rpc(self.chain_id, "getBlock"):
        payload = {...}
        async def _do(url: str) -> dict[str, Any]:
            resp = await self._client.post(url, json=payload, headers={"content-type": "application/json"})
            resp.raise_for_status()
            return resp.json()
        body = await self._pool.call(_do)
        result = body.get("result")
        return None if result is None else self._parse_block(slot, result)
```

`get_latest_slot` and `get_blocks` follow the same pattern (`getSlot` uses `solders` parsing, which stays outside the closure too — the closure returns the raw `resp.text` / parsed JSON and the adapter parses after).

`subscribe_heads` (Solana's slot poller) calls `get_latest_slot`, which is now pooled — it benefits automatically.

`disconnect()` closes the shared httpx client (unchanged) and drops the pool reference.

## Component 4: Config + migration

### Chain model (`core/config/models.py`)

```python
    rpc_http_fallbacks: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    rpc_timeout_ms: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10000, server_default="10000"
    )
```

### Migration `migrations/versions/0008_rpc_node_pool.py`

```python
revision = "0008"
down_revision = "0007"

def upgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.add_column(sa.Column(
            "rpc_http_fallbacks", sa.JSON(), nullable=False, server_default="[]",
        ))
        batch.add_column(sa.Column(
            "rpc_timeout_ms", sa.Integer(), nullable=False, server_default="10000",
        ))

def downgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.drop_column("rpc_timeout_ms")
        batch.drop_column("rpc_http_fallbacks")
```

### Snapshot (`core/config/snapshot.py`)

`SnapshotChain` (frozen dataclass) gains:

```python
    rpc_http_fallbacks: list[str] = field(default_factory=list)
    rpc_timeout_ms: int = 10000
```

`load_snapshot` reads the two new columns into the snapshot.

### Repository (`core/config/repositories.py`)

`ChainRepo.create` and `update` accept and pass through `rpc_http_fallbacks` and `rpc_timeout_ms` (same pattern as the existing `log_query_range_blocks` / `slot_query_range_blocks`).

### API schema (`apps/web/schemas.py`)

`ChainCreate` and `ChainOut` gain:

```python
    rpc_http_fallbacks: list[str] = Field(default_factory=list)
    rpc_timeout_ms: int = 10000
```

`ChainCreate` adds a light validator: each fallback must be a non-empty string; the list is de-duplicated and the primary `rpc_http` is removed from it if present (no double-listing the primary). No URL-format validation.

### Adapter factory (`apps/worker/main.py`)

`_default_adapter_factory` reads `RpcPoolSettings` (via the worker's `settings`) and passes the new fields plus breaker params to the adapters:

```python
def _default_adapter_factory(cfg: SnapshotChain) -> EvmAdapter | SolanaAdapter:
    pool = settings.rpc_pool  # captured from the worker's settings
    common = dict(
        rpc_http_fallbacks=cfg.rpc_http_fallbacks,
        rpc_timeout_ms=cfg.rpc_timeout_ms,
        failure_threshold=pool.failure_threshold,
        cooldown_s=pool.cooldown_s,
    )
    if cfg.kind == "evm":
        return EvmAdapter(chain_id=cfg.id, rpc_http=cfg.rpc_http, rpc_ws=cfg.rpc_ws,
                          confirmations=cfg.confirmations, poll_interval_ms=cfg.poll_interval_ms,
                          **common)
    ...
```

> The current `_default_adapter_factory` is a module-level function with no settings access. The implementation will thread `settings` in — either by making it a closure built in `_Worker` (preferred, mirrors `_make_channel_factory(bus)`) or by reading `load_settings()` inside. **Preferred: closure factory** `_make_adapter_factory(settings)` analogous to the existing `_make_channel_factory(bus)`.

### Settings (`core/settings.py`)

```python
class RpcPoolSettings(BaseModel):
    failure_threshold: int = 3
    cooldown_s: float = 30.0
```

Wired into `Settings` as `rpc_pool: RpcPoolSettings = Field(default_factory=RpcPoolSettings)`. Env: `CHAIN_INDEXER_RPC_POOL__FAILURE_THRESHOLD`, `CHAIN_INDEXER_RPC_POOL__COOLDOWN_S`.

## Component 5: Metrics (`core/metrics.py`)

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

Cardinality: `endpoint_index` is a small integer (≤ ~3 per chain); `chain` ~5. Total < 20 series. Index labels avoid the credential-leak and cardinality risks of URL labels.

## Component 6: Web UI (`web/src/pages/Chains.tsx`)

The `ChainForm` (a `FormData`-based form) gains two inputs:

- **Fallback endpoints**: a `<textarea name="rpc_http_fallbacks">` — newline-separated URLs. On submit, split by newline, trim, drop empties → `string[]`.
- **Request timeout (ms)**: `<input type="number" name="rpc_timeout_ms" defaultValue={initial?.rpc_timeout_ms ?? 10000}>`.

The `Chain` TS interface gains `rpc_http_fallbacks: string[]` and `rpc_timeout_ms: number`. The `mut.mutate(...)` payload includes both. The chains table view may optionally show a small badge when `rpc_http_fallbacks.length > 0` ("N 备用"), but that's cosmetic.

## Error handling

- **Primary down, fallback healthy**: `pool.call` fails over transparently; `RPC_FAILOVER_TOTAL` increments; the call succeeds on the fallback.
- **All endpoints down**: `AllEndpointsFailed` raised; outer `track_rpc` records `status="error"`; the chain runner's existing per-block error handling (catchup window break / per-slot tolerance) applies, exactly as today when a single endpoint failed.
- **Per-request timeout**: `asyncio.wait_for` raises `TimeoutError`, which triggers failover. If all endpoints time out, `AllEndpointsFailed`.
- **`BlockNotFound` / business exceptions**: trigger failover (one wasted round across endpoints). Documented as acceptable — only confirmed blocks are fetched, so this is rare.
- **Empty fallback list (single endpoint)**: pool has one endpoint; behavior is equivalent to today plus a per-request timeout. All existing tests must still pass.
- **Health-state races under concurrency**: counters mutate synchronously between `await` points; no lock needed; worst case is a transient miscount that self-corrects.
- **Hot-reload changing endpoints**: a config change rebuilds the snapshot; the runner is reconciled (`apply_snapshot` rebuilds the notifier; adapter rebuild happens on runner restart for changed chains). Endpoint changes take effect when the chain runner is next (re)started for that chain — consistent with how `rpc_http` changes already propagate today.

## Testing

### Unit — `tests/unit/test_rpc_pool.py` (core, must be thorough)

- `test_single_endpoint_success` — one endpoint, returns value, no failover counter movement.
- `test_failover_to_second_on_first_failure` — endpoint 0 raises, endpoint 1 succeeds; `RPC_FAILOVER_TOTAL` +1.
- `test_all_endpoints_fail_raises` — all raise → `AllEndpointsFailed` with `__cause__` = last error.
- `test_timeout_triggers_failover` — endpoint 0 hangs past `timeout_s` (real short sleep, e.g. timeout 0.05 vs sleep 0.2) → fails over to endpoint 1.
- `test_circuit_breaker_trips_after_threshold` — endpoint 0 fails `failure_threshold` times → `unhealthy_until` set, `RPC_ENDPOINT_UP{index=0}` = 0.
- `test_unhealthy_endpoint_skipped_during_cooldown` — monkeypatch `time.monotonic`; verify a cooling endpoint is skipped.
- `test_success_resets_health` — after cooldown, a success flips `RPC_ENDPOINT_UP` back to 1 and zeroes the counter.
- `test_all_unhealthy_degrades_to_try_all` — all endpoints cooling → `_candidates` returns the full list and a call is still attempted.

Mock endpoints are plain async callables receiving a fake handle. Control cooldown via monkeypatched `time.monotonic`. Use real short sleeps for the timeout test.

### Unit — adapter level

- Extend `tests/unit/test_evm_adapter_metrics.py`: construct an `EvmAdapter` whose pool has a failing endpoint 0 + a good endpoint 1; assert `fetch_block` returns the correct `Block` (failover transparent). Stub the two `AsyncWeb3` handles.
- Add an analogous Solana smoke test (2 URLs, first POST raises, second returns a valid body).

### Integration — `tests/integration/test_repositories.py`

- `create` a chain with `rpc_http_fallbacks=["http://b"]` and `rpc_timeout_ms=5000`; read back and assert both round-trip.

### Migration

- The existing migration test path (`alembic upgrade head` against a fresh DB) must show the two new columns on `chains`.

### Regression

- All existing EVM/Solana adapter + catchup + fetch_logs tests pass unchanged (single-endpoint config is behavior-equivalent aside from the added timeout).
- Full `pytest -m "not e2e"` green (environmental Docker/testcontainer failures aside).

### Frontend

- `npm run build` succeeds with the two new form fields.

## File-level change summary

| File | Change |
|------|--------|
| `core/chains/rpc_pool.py` | NEW. `EndpointPool` + `AllEndpointsFailed`. |
| `core/metrics.py` | +`RPC_ENDPOINT_UP`, `RPC_FAILOVER_TOTAL`. |
| `core/chains/evm.py` | `connect` builds pool (N handles); RPC methods route through `pool.call`; `disconnect` iterates; keep `self._w3` = primary for WS/poll. |
| `core/chains/solana.py` | `connect` builds pool (URL handles); RPC methods route through `pool.call`. |
| `core/config/models.py` | Chain +`rpc_http_fallbacks`, +`rpc_timeout_ms`. |
| `core/config/snapshot.py` | SnapshotChain +2 fields; `load_snapshot` reads them. |
| `core/config/repositories.py` | ChainRepo create/update pass-through. |
| `apps/web/schemas.py` | ChainCreate/ChainOut +2 fields; ChainCreate dedup/non-empty validator. |
| `apps/worker/main.py` | `_make_adapter_factory(settings)` closure passes endpoints + breaker params. |
| `core/settings.py` | +`RpcPoolSettings`. |
| `migrations/versions/0008_rpc_node_pool.py` | NEW. |
| `web/src/pages/Chains.tsx` | Form gains fallbacks textarea + timeout input; `Chain` TS interface +2 fields. |
| `tests/...` | `test_rpc_pool.py` (new) + adapter/repo/migration coverage. |

## Rollout

Single PR. One Alembic migration (additive columns with server defaults — safe for existing rows: every chain becomes a single-endpoint pool with a 10s timeout). No config required to benefit from the timeout; operators add fallbacks per chain as desired. Hot-reload picks up endpoint changes on the next runner (re)start for that chain.

## Open questions

None blocking. Possible follow-ups (out of scope):

- WS endpoint pooling (currently single + HTTP-poll fallback).
- Weighted / latency-aware routing instead of strict ordered fallback.
- JSON-RPC batch and concurrent block fetching (separate future sub-projects).
- Per-endpoint p99 latency metric (would need an `endpoint_index` label on the RPC histogram — deferred to keep cardinality low until proven needed).
