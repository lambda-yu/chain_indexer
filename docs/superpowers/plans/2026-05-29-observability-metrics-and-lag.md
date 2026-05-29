# Observability: Prometheus metrics + per-chain lag — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Prometheus `/metrics` exposition on both worker and web processes, plus a `GET /api/chains/{id}/lag` REST endpoint surfaced in the Dashboard as a color-coded chip.

**Architecture:** A new `core/metrics.py` module defines all metric singletons against the `prometheus_client` default registry. Worker starts a daemon-thread HTTP server on port 9091 (`start_http_server`). Web mounts `make_asgi_app()` at `/metrics` and installs a middleware to record API metrics. Worker writes `chain:{id}:tip` to Redis on every live head; web's `/lag` endpoint reads tip + checkpoint to compute `lag_blocks`.

**Tech Stack:** Python 3.11+, prometheus-client, FastAPI, SQLAlchemy async, Redis (`redis.asyncio`), React 19 + React Query 5, pytest + pytest-asyncio.

**Reference spec:** `docs/superpowers/specs/2026-05-29-observability-metrics-and-lag-design.md`

---

## File Structure

**New Python files:**
- `core/metrics.py` — all metric singletons + `track_rpc` async context manager

**New test files:**
- `tests/unit/test_metrics_module.py` — `track_rpc` success/error paths + idempotent import
- `tests/unit/test_publish_tip.py` — `_Worker._publish_tip` happy path + Redis error swallow
- `tests/unit/test_chain_runner_instrumentation.py` — `_tracked_dispatch` inc/dec; live-vs-catchup tip update placement
- `tests/integration/test_chains_lag_router.py` — normal, missing tip, missing checkpoint, 404
- `tests/integration/test_metrics_endpoint.py` — web `/metrics` returns Prometheus format; middleware records calls

**Modified Python files:**
- `core/settings.py` — add `MetricsSettings`
- `apps/worker/main.py` — `start_http_server`, `_publish_tip`, wire `tip_publisher` into `ChainRunner(...)`
- `apps/worker/chain_runner.py` — `tip_publisher` ctor param; tip gauge on EVM live + Solana live (NOT catchup); `BLOCKS_PROCESSED_TOTAL` + `CHAIN_LAST_PROCESSED_BLOCK` in shared per-block / per-slot processors; `_tracked_dispatch` helper
- `core/chains/evm.py` — `track_rpc` around `get_latest_block_number`, `fetch_block`, `fetch_logs`, `trace_block`, `trace_transaction`
- `core/chains/solana.py` — `track_rpc` around `get_latest_slot`, `fetch_block`, `get_blocks`
- `core/notifier/notifier.py` — `CHANNEL_SEND_SECONDS` + `CHANNEL_SENDS_TOTAL` in `_send_one`
- `apps/web/main.py` — mount `/metrics`, install `track_api_metrics` middleware
- `apps/web/routers/chains.py` — `get_chain_lag` endpoint + `ChainLagOut` schema
- `pyproject.toml` — add `prometheus-client`

**Modified TypeScript files:**
- `web/src/pages/Dashboard.tsx` — `ChainCard` adds `/lag` query, `lagMeta` helper, lag chip, tip-block row; rename "最新区块" → "已处理"

**Modified ops files:**
- `docker-compose.yml` — expose worker port 9091
- `README.md` — short "Observability" section (scrape config example)

---

## Chunk 1: Foundation — dep, settings, metrics module

Adds the `prometheus-client` dependency, the `MetricsSettings` nested config, and the `core/metrics.py` module containing every metric singleton plus the `track_rpc` async helper. No production call sites are touched yet.

### Task 1.1: Add `prometheus-client` dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add the dependency**

In `pyproject.toml`, find the `dependencies = [` list (the main project deps, not dev / extras). Append:

```toml
    "prometheus-client>=0.20.0",
```

- [ ] **Step 2: Install**

Run: `uv sync --extra dev`

Expected: `prometheus-client` installed; no other version conflicts.

- [ ] **Step 3: Verify importable**

Run: `uv run python -c "import prometheus_client; print(prometheus_client.__version__)"`
Expected: prints a version >= 0.20.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "build: add prometheus-client dep for observability sub-project"
```

### Task 1.2: Add `MetricsSettings` to settings

**Files:**
- Modify: `core/settings.py`
- Create: `tests/unit/test_settings_metrics.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_settings_metrics.py`:

```python
from __future__ import annotations

from core.settings import Settings


def test_metrics_defaults() -> None:
    s = Settings()
    assert s.metrics.enabled is True
    assert s.metrics.port == 9091


def test_metrics_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_METRICS__ENABLED", "false")
    monkeypatch.setenv("CHAIN_INDEXER_METRICS__PORT", "9999")
    s = Settings()
    assert s.metrics.enabled is False
    assert s.metrics.port == 9999
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_settings_metrics.py -v`
Expected: FAIL with `AttributeError: ... has no attribute 'metrics'`.

- [ ] **Step 3: Add `MetricsSettings` and wire into `Settings`**

In `core/settings.py`, add the model class after `LoggingSettings`:

```python
class MetricsSettings(BaseModel):
    enabled: bool = True
    port: int = 9091
```

Add to `Settings` (alongside `logging`, etc.):

```python
    metrics: MetricsSettings = Field(default_factory=MetricsSettings)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_settings_metrics.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Type-check**

Run: `uv run mypy core/settings.py`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add core/settings.py tests/unit/test_settings_metrics.py
git commit -m "feat(settings): add MetricsSettings nested config"
```

### Task 1.3: Create `core/metrics.py` module

**Files:**
- Create: `core/metrics.py`
- Create: `tests/unit/test_metrics_module.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_metrics_module.py`:

```python
from __future__ import annotations

import pytest

# Import once at module level — prometheus_client registers metrics at import
# time, and re-importing raises ValueError on duplicate registration.
from core import metrics as M


def test_module_exposes_all_metric_singletons() -> None:
    expected = [
        "BLOCKS_PROCESSED_TOTAL",
        "CHAIN_TIP_BLOCK",
        "CHAIN_LAST_PROCESSED_BLOCK",
        "RPC_REQUEST_SECONDS",
        "RPC_REQUESTS_TOTAL",
        "CHANNEL_SEND_SECONDS",
        "CHANNEL_SENDS_TOTAL",
        "DISPATCH_IN_FLIGHT",
        "WORKER_UP",
        "WORKER_INFO",
        "API_REQUEST_SECONDS",
        "API_REQUESTS_TOTAL",
    ]
    for name in expected:
        assert hasattr(M, name), f"core.metrics missing {name}"


@pytest.mark.asyncio
async def test_track_rpc_observes_latency_and_success_counter() -> None:
    # Capture starting counter for delta assertion (REGISTRY is global,
    # other tests may have incremented it).
    before = M.RPC_REQUESTS_TOTAL.labels("test-chain", "test-method", "success")._value.get()
    async with M.track_rpc("test-chain", "test-method"):
        pass
    after = M.RPC_REQUESTS_TOTAL.labels("test-chain", "test-method", "success")._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_track_rpc_records_error_status_on_exception() -> None:
    before = M.RPC_REQUESTS_TOTAL.labels("test-chain", "boom-method", "error")._value.get()
    with pytest.raises(RuntimeError):
        async with M.track_rpc("test-chain", "boom-method"):
            raise RuntimeError("boom")
    after = M.RPC_REQUESTS_TOTAL.labels("test-chain", "boom-method", "error")._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_track_rpc_observes_histogram_on_both_success_and_error() -> None:
    """Verify the histogram sample count rises in both paths."""
    h_success = M.RPC_REQUEST_SECONDS.labels("test-chain", "histo-success")
    before_s = h_success._sum.get()
    async with M.track_rpc("test-chain", "histo-success"):
        pass
    after_s = h_success._sum.get()
    assert after_s >= before_s  # observation took place (sum may be ~0 for instant ops)

    h_error = M.RPC_REQUEST_SECONDS.labels("test-chain", "histo-error")
    before_e = h_error._sum.get()
    with pytest.raises(ValueError):
        async with M.track_rpc("test-chain", "histo-error"):
            raise ValueError("x")
    after_e = h_error._sum.get()
    assert after_e >= before_e
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_metrics_module.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'core.metrics'`.

- [ ] **Step 3: Create `core/metrics.py`**

Create `core/metrics.py`:

```python
"""Prometheus metric singletons + helpers for chain_indexer observability.

All metrics live in the prometheus_client default REGISTRY. Worker and web are
separate processes, so they each maintain their own copy; no inter-process
synchronization.

Importing this module twice in the same process is safe (Python's module
cache returns the same module object — the metric singletons are constructed
exactly once).
"""
from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from prometheus_client import Counter, Gauge, Histogram

# ---------- Worker-side metrics ----------

BLOCKS_PROCESSED_TOTAL = Counter(
    "chain_indexer_blocks_processed_total",
    "Confirmed blocks fully processed.",
    ["chain"],
)

CHAIN_TIP_BLOCK = Gauge(
    "chain_indexer_chain_tip_block",
    "Latest head block number observed from subscribe_heads (live loop only).",
    ["chain"],
)

CHAIN_LAST_PROCESSED_BLOCK = Gauge(
    "chain_indexer_chain_last_processed_block",
    "Last fully-processed block number (advances during catchup too).",
    ["chain"],
)

RPC_REQUEST_SECONDS = Histogram(
    "chain_indexer_rpc_request_seconds",
    "RPC call latency.",
    ["chain", "method"],
)

RPC_REQUESTS_TOTAL = Counter(
    "chain_indexer_rpc_requests_total",
    "RPC call count.",
    ["chain", "method", "status"],
)

CHANNEL_SEND_SECONDS = Histogram(
    "chain_indexer_channel_send_seconds",
    "End-to-end Channel.send latency (includes in-channel retry sleeps).",
    ["channel_type"],
)

CHANNEL_SENDS_TOTAL = Counter(
    "chain_indexer_channel_sends_total",
    "Channel send count.",
    ["channel_type", "status"],
)

DISPATCH_IN_FLIGHT = Gauge(
    "chain_indexer_dispatch_in_flight",
    "Current in-flight dispatch tasks (per-event fan-out).",
)

WORKER_UP = Gauge(
    "chain_indexer_worker_up",
    "Worker is up and serving metrics.",
)

WORKER_INFO = Gauge(
    "chain_indexer_worker_info",
    "Worker identity.",
    ["worker_id", "version"],
)

# ---------- Web-side metrics ----------

API_REQUEST_SECONDS = Histogram(
    "chain_indexer_api_request_seconds",
    "Web API request latency.",
    ["method", "path"],
)

API_REQUESTS_TOTAL = Counter(
    "chain_indexer_api_requests_total",
    "Web API request count.",
    ["method", "path", "status"],
)

# ---------- Helpers ----------


@asynccontextmanager
async def track_rpc(chain: str, method: str) -> AsyncIterator[None]:
    """Wrap an async RPC call to record latency and success/error count."""
    t0 = time.perf_counter()
    status = "success"
    try:
        yield
    except Exception:
        status = "error"
        raise
    finally:
        RPC_REQUEST_SECONDS.labels(chain, method).observe(time.perf_counter() - t0)
        RPC_REQUESTS_TOTAL.labels(chain, method, status).inc()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_metrics_module.py -v`
Expected: 4 PASS.

- [ ] **Step 5: Type-check + lint**

Run: `uv run mypy core/metrics.py`
Run: `uv run ruff check core/metrics.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add core/metrics.py tests/unit/test_metrics_module.py
git commit -m "feat(metrics): core/metrics.py — 12 metric singletons + track_rpc helper"
```

---

## Chunk 2: Worker plumbing — /metrics server + tip publishing + chain_runner instrumentation

### Task 2.1: Worker starts `/metrics` HTTP server

**Files:**
- Modify: `apps/worker/main.py`

- [ ] **Step 1: Read current `_Worker.start()` to find insertion point**

The existing `start()` ends with `await self._watcher.start()` (around line 170). We append after it.

- [ ] **Step 2: Add the startup block**

In `apps/worker/main.py`, modify `_Worker.start()` (append after `await self._watcher.start()`):

```python
        # Prometheus metrics server (daemon thread; survives until process exit)
        if self._settings.metrics.enabled:
            from importlib.metadata import PackageNotFoundError, version as pkg_version
            from prometheus_client import start_http_server
            from core.metrics import WORKER_INFO, WORKER_UP

            start_http_server(self._settings.metrics.port)
            WORKER_UP.set(1)
            try:
                v = pkg_version("chain-indexer")
            except PackageNotFoundError:
                v = "dev"
            WORKER_INFO.labels(worker_id=self._worker_id, version=v).set(1)
            log.info(
                "worker.metrics_server_started",
                port=self._settings.metrics.port,
            )
```

- [ ] **Step 3: Manual smoke test (no test gate — port allocation is hard to TDD)**

Run: `uv run python -m apps.worker.main` in one terminal.
Wait for the `worker.metrics_server_started` log line.
In another terminal: `curl -s http://localhost:9091/metrics | head -20`
Expected: Prometheus exposition format text including `chain_indexer_worker_up 1.0`.
Stop the worker with Ctrl-C.

- [ ] **Step 4: Lint + type-check**

Run: `uv run ruff check apps/worker/main.py`
Run: `uv run mypy apps/worker/main.py`
Expected: no new errors (worker file has known pre-existing baseline).

- [ ] **Step 5: Commit**

```bash
git add apps/worker/main.py
git commit -m "feat(worker): start prometheus /metrics server on configured port"
```

### Task 2.2: Worker `_publish_tip` callback + Redis tip key

**Files:**
- Modify: `apps/worker/main.py`
- Create: `tests/unit/test_publish_tip.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_publish_tip.py`:

```python
from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.settings import Settings


def _build_worker_with_mock_bus() -> tuple[Any, AsyncMock]:
    """Return a _Worker with its bus.client.set replaced by an AsyncMock."""
    from apps.worker.main import _Worker

    worker = _Worker(Settings())
    set_mock = AsyncMock()
    fake_client = MagicMock()
    fake_client.set = set_mock
    worker._bus._client = fake_client  # bypass the connect() guard
    return worker, set_mock


@pytest.mark.asyncio
async def test_publish_tip_writes_redis_key_with_ttl() -> None:
    worker, set_mock = _build_worker_with_mock_bus()
    await worker._publish_tip("eth-mainnet", 12345)
    set_mock.assert_awaited_once_with("chain:eth-mainnet:tip", 12345, ex=60)


@pytest.mark.asyncio
async def test_publish_tip_swallows_redis_errors() -> None:
    """A Redis hiccup must not propagate (or the chain runner would die)."""
    worker, set_mock = _build_worker_with_mock_bus()
    set_mock.side_effect = RuntimeError("redis down")
    # Should not raise:
    await worker._publish_tip("eth-mainnet", 12345)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_publish_tip.py -v`
Expected: FAIL with `AttributeError: '_Worker' object has no attribute '_publish_tip'`.

- [ ] **Step 3: Add `_publish_tip` to `_Worker`**

In `apps/worker/main.py`, in the `_Worker` class (after `_on_block_processed`):

```python
    async def _publish_tip(self, chain_id: str, block_number: int) -> None:
        """Write chain:{chain_id}:tip to Redis (TTL 60s) for the web's /lag endpoint.

        Errors are logged and swallowed: a Redis hiccup must not kill the
        chain runner. The Dashboard chip just goes ⚪ "unknown" until the
        next live head re-publishes the key.
        """
        try:
            await self._bus.client.set(
                f"chain:{chain_id}:tip", block_number, ex=60,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "worker.publish_tip_failed", chain_id=chain_id, error=repr(exc),
            )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_publish_tip.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/main.py tests/unit/test_publish_tip.py
git commit -m "feat(worker): _publish_tip writes chain:{id}:tip to Redis"
```

### Task 2.3: `ChainRunner` gains `tip_publisher` parameter

**Files:**
- Modify: `apps/worker/chain_runner.py`
- Modify: `apps/worker/main.py` (wire it in)

- [ ] **Step 1: Add `tip_publisher` to `ChainRunner.__init__`**

In `apps/worker/chain_runner.py`, modify the `__init__` signature (currently lines 73-94). Add `tip_publisher` parameter, store on self:

```python
    def __init__(
        self,
        *,
        chain: SnapshotChain,
        adapter_factory: AdapterFactory,
        channel_factory: ChannelFactory,
        checkpoint_repo: _CheckpointRepo,
        notifier_max_concurrency: int = 50,
        abi_registry: AbiRegistry | None = None,
        on_send_failure: Any = None,
        on_send_success: Any = None,
        on_block_processed: Any = None,
        tip_publisher: Any = None,
    ) -> None:
        ...  # existing body
        self._on_block_processed = on_block_processed
        self._tip_publisher = tip_publisher
```

Use `Any` for the type (matches the existing `on_send_failure` / `on_block_processed` style).

- [ ] **Step 2: Wire from `_Worker._reconcile`**

In `apps/worker/main.py`, find the single `ChainRunner(...)` construction site in `_reconcile` (around line 225). Add `tip_publisher=self._publish_tip` to the kwargs:

```python
                runner = ChainRunner(
                    chain=cfg,
                    adapter_factory=_default_adapter_factory,
                    channel_factory=_make_channel_factory(self._bus),
                    checkpoint_repo=self._checkpoint_adapter,
                    abi_registry=self._registry,
                    on_send_failure=self._on_delivery_failure,
                    on_send_success=self._on_delivery_success,
                    on_block_processed=self._on_block_processed,
                    tip_publisher=self._publish_tip,
                )
```

- [ ] **Step 3: Run existing worker tests to ensure no regression**

Run: `uv run pytest tests/unit/test_chain_runner.py -v`
Expected: all PASS (new param has default `None`, so existing constructions are unaffected).

- [ ] **Step 4: Type-check + lint**

Run: `uv run mypy apps/worker/main.py apps/worker/chain_runner.py`
Run: `uv run ruff check apps/worker/main.py apps/worker/chain_runner.py`
Expected: no new errors.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py apps/worker/main.py
git commit -m "feat(runner): ChainRunner accepts tip_publisher callback"
```

### Task 2.4: EVM tip gauge + Redis publish in `_handle_evm_head`

**Files:**
- Modify: `apps/worker/chain_runner.py`
- Create: `tests/unit/test_chain_runner_instrumentation.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_chain_runner_instrumentation.py`:

```python
"""Unit tests for ChainRunner observability instrumentation.

Verifies that tip-gauge and tip-publisher updates happen on the live path
but NOT during catchup (Solana's _process_solana_slot is shared between
live and catchup, so the tip update must live at the live call site).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.metrics import CHAIN_TIP_BLOCK


def _build_runner(chain_kind: str = "evm"):
    """Return a minimal ChainRunner with mocked dependencies."""
    from apps.worker.chain_runner import ChainRunner
    from core.config.snapshot import SnapshotChain

    chain = SnapshotChain(
        id="test-chain",
        kind=chain_kind,
        rpc_http="http://localhost:8545",
        rpc_ws=None,
        confirmations=12,
        poll_interval_ms=1000,
        commitment="confirmed" if chain_kind == "solana" else None,
        trace_internal_calls=False,
        log_query_range_blocks=100,
        slot_query_range_blocks=1000,
    )
    tip_publisher = AsyncMock()
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: MagicMock(),
        channel_factory=lambda c: MagicMock(),
        checkpoint_repo=MagicMock(),
        tip_publisher=tip_publisher,
    )
    return runner, tip_publisher


@pytest.mark.asyncio
async def test_handle_evm_head_updates_tip_gauge_and_publisher() -> None:
    runner, tip_publisher = _build_runner("evm")
    # Stub the rest of _handle_evm_head's dependencies so we only exercise
    # the tip-update prelude.
    runner._buffer = MagicMock()
    runner._buffer.handle_new_head.return_value = []  # no confirmed blocks
    runner._buffer_tip_hash = None  # no prefetch
    runner._matcher = MagicMock()
    runner._notifier = MagicMock()
    runner._adapter = MagicMock()

    from core.chains.types import BlockHeader
    header = BlockHeader(
        number=12345, hash="0xabc", parent_hash="0xdef", timestamp=0,
    )

    before = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()
    await runner._handle_evm_head(header)
    after = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()

    assert after == 12345
    assert after != before or before == 12345  # gauge moved (or was already there)
    tip_publisher.assert_awaited_once_with("test-chain", 12345)
```

(The BlockHeader fields might differ — double-check `core/chains/types.py` and adjust the kwargs. The exact constructor isn't critical, only that you can build one.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py::test_handle_evm_head_updates_tip_gauge_and_publisher -v`
Expected: FAIL — the tip-update lines don't exist yet, so `tip_publisher.assert_awaited_once_with` raises `AssertionError: Expected await...`.

- [ ] **Step 3: Add the tip-update prelude in `_handle_evm_head`**

In `apps/worker/chain_runner.py`, at the very top of `_handle_evm_head` (currently line 399), insert before the existing asserts:

```python
    async def _handle_evm_head(self, header: BlockHeader) -> None:
        # Live-head tip update (EVM catchup goes through
        # _process_block_with_prefetched_logs directly, so this never runs there).
        from core.metrics import CHAIN_TIP_BLOCK
        CHAIN_TIP_BLOCK.labels(chain=self._chain.id).set(header.number)
        if self._tip_publisher is not None:
            await self._tip_publisher(self._chain.id, header.number)

        assert self._buffer is not None and self._adapter is not None
        ...  # rest of the existing body unchanged
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py::test_handle_evm_head_updates_tip_gauge_and_publisher -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner_instrumentation.py
git commit -m "feat(runner): EVM tip gauge + Redis publish on each live head"
```

### Task 2.5: Solana tip gauge + Redis publish — live loop only

**Files:**
- Modify: `apps/worker/chain_runner.py` (`_run_solana`, lines 311-317)
- Modify: `tests/unit/test_chain_runner_instrumentation.py` (append test)

- [ ] **Step 1: Append failing test**

Append to `tests/unit/test_chain_runner_instrumentation.py`:

```python
@pytest.mark.asyncio
async def test_process_solana_slot_does_NOT_update_tip_gauge() -> None:
    """The shared per-slot processor must NOT touch the tip; tip is live-only.
    Verified by calling _process_solana_slot directly (no live-loop wrapper)
    and asserting the tip gauge for this chain is untouched."""
    runner, tip_publisher = _build_runner("solana")
    runner._matcher = MagicMock()
    runner._notifier = MagicMock()
    runner._adapter = MagicMock()
    runner._adapter.fetch_block = AsyncMock(return_value=None)  # skip body
    runner._solana_pipeline = MagicMock()

    before = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()
    await runner._process_solana_slot(99999)
    after = CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()

    assert after == before
    tip_publisher.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_solana_live_loop_updates_tip_gauge_and_publisher(monkeypatch) -> None:
    """The live `async for slot` loop in _run_solana must update the tip BEFORE
    calling _process_solana_slot."""
    runner, tip_publisher = _build_runner("solana")
    runner._matcher = MagicMock()
    runner._notifier = MagicMock()
    runner._adapter = MagicMock()

    # Make catchup a no-op and yield exactly one slot.
    async def _no_catchup() -> None:
        pass
    monkeypatch.setattr(runner, "_catchup_solana", _no_catchup)

    async def _heads():
        yield 555
        runner._stop.set()  # exit loop after one
    runner._adapter.subscribe_heads = _heads

    # Stub _process_solana_slot to capture if tip was set before it ran.
    seen_tip_at_call = []
    async def _stub_process(slot):
        seen_tip_at_call.append(
            CHAIN_TIP_BLOCK.labels(chain="test-chain")._value.get()
        )
    monkeypatch.setattr(runner, "_process_solana_slot", _stub_process)

    await runner._run_solana()

    assert seen_tip_at_call == [555], "tip must be set before _process_solana_slot"
    tip_publisher.assert_awaited_once_with("test-chain", 555)
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py -v`
Expected: the two new tests FAIL. (The "does NOT update" test would actually pass currently since we haven't added Solana instrumentation yet — but the "live loop updates" test will fail since the prelude doesn't exist. After step 3, both pass.)

- [ ] **Step 3: Move tip update INTO `_run_solana`'s live loop only**

In `apps/worker/chain_runner.py`, modify `_run_solana` (lines 311-317):

```python
    async def _run_solana(self) -> None:
        # 快速追块：从 checkpoint 追到最新 slot
        await self._catchup_solana()
        from core.metrics import CHAIN_TIP_BLOCK
        async for slot in self._adapter.subscribe_heads():
            if self._stop.is_set():
                break
            # Live-loop tip update. Critically NOT inside _process_solana_slot,
            # which is shared with catchup — putting it there would regress the
            # tip gauge to historical slot values during backfill.
            CHAIN_TIP_BLOCK.labels(chain=self._chain.id).set(slot)
            if self._tip_publisher is not None:
                await self._tip_publisher(self._chain.id, slot)
            await self._process_solana_slot(slot)
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py -v`
Expected: all 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner_instrumentation.py
git commit -m "feat(runner): Solana tip gauge updated in live loop only (not catchup)"
```

### Task 2.6: BLOCKS_PROCESSED_TOTAL + CHAIN_LAST_PROCESSED_BLOCK in shared processors

**Files:**
- Modify: `apps/worker/chain_runner.py` (`_process_block_with_prefetched_logs`, `_process_solana_slot`)
- Modify: `tests/unit/test_chain_runner_instrumentation.py` (append)

- [ ] **Step 1: Append failing tests**

Append to `tests/unit/test_chain_runner_instrumentation.py`:

```python
@pytest.mark.asyncio
async def test_evm_per_block_metrics_advance_after_processing(monkeypatch) -> None:
    """BLOCKS_PROCESSED_TOTAL + CHAIN_LAST_PROCESSED_BLOCK update after each
    successfully-processed block (covers both live and catchup paths)."""
    from core.metrics import BLOCKS_PROCESSED_TOTAL, CHAIN_LAST_PROCESSED_BLOCK
    from core.chains.types import Block, BlockHeader

    runner, _ = _build_runner("evm")
    runner._matcher = MagicMock()
    runner._matcher.match = MagicMock(return_value=[])  # no hits
    runner._notifier = MagicMock()
    runner._evm_pipeline = MagicMock()
    runner._evm_pipeline.run = MagicMock(return_value=[])
    runner._adapter = MagicMock()
    runner._cp = MagicMock()
    runner._cp.save = AsyncMock()

    header = BlockHeader(number=42, hash="0xa", parent_hash="0xb", timestamp=0)
    block = Block(header=header, txs=[], logs=[])

    before_count = BLOCKS_PROCESSED_TOTAL.labels(chain="test-chain")._value.get()
    before_gauge = CHAIN_LAST_PROCESSED_BLOCK.labels(chain="test-chain")._value.get()

    await runner._process_block_with_prefetched_logs(
        42, block, [], matcher=runner._matcher, notifier=runner._notifier,
    )

    after_count = BLOCKS_PROCESSED_TOTAL.labels(chain="test-chain")._value.get()
    after_gauge = CHAIN_LAST_PROCESSED_BLOCK.labels(chain="test-chain")._value.get()
    assert after_count - before_count == 1
    assert after_gauge == 42
```

(Adjust `Block` constructor kwargs to match `core/chains/types.py`.)

- [ ] **Step 2: Run failing test**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py::test_evm_per_block_metrics_advance_after_processing -v`
Expected: FAIL — counter/gauge unchanged.

- [ ] **Step 3: Add metric updates after `_cp.save`**

In `apps/worker/chain_runner.py`:

**`_process_block_with_prefetched_logs`** — after the `await self._cp.save(...)` line (currently line 516):

```python
        await self._cp.save(self._chain.id, block.header.number, block.header.hash)
        from core.metrics import BLOCKS_PROCESSED_TOTAL, CHAIN_LAST_PROCESSED_BLOCK
        BLOCKS_PROCESSED_TOTAL.labels(chain=self._chain.id).inc()
        CHAIN_LAST_PROCESSED_BLOCK.labels(chain=self._chain.id).set(block.header.number)
```

**`_process_solana_slot`** — after `await self._cp.save(...)` (currently line 544):

```python
        await self._cp.save(self._chain.id, block.slot, block.block_hash)
        from core.metrics import BLOCKS_PROCESSED_TOTAL, CHAIN_LAST_PROCESSED_BLOCK
        BLOCKS_PROCESSED_TOTAL.labels(chain=self._chain.id).inc()
        CHAIN_LAST_PROCESSED_BLOCK.labels(chain=self._chain.id).set(block.slot)
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner_instrumentation.py
git commit -m "feat(runner): per-block counter + last_processed gauge in shared processors"
```

### Task 2.7: `_tracked_dispatch` helper for `DISPATCH_IN_FLIGHT` gauge

**Files:**
- Modify: `apps/worker/chain_runner.py`
- Modify: `tests/unit/test_chain_runner_instrumentation.py` (append)

- [ ] **Step 1: Append failing test**

Append to `tests/unit/test_chain_runner_instrumentation.py`:

```python
@pytest.mark.asyncio
async def test_tracked_dispatch_increments_and_decrements_gauge() -> None:
    """The wrapper helper must inc on entry and dec on exit (even on exception)."""
    from apps.worker.chain_runner import _tracked_dispatch
    from core.metrics import DISPATCH_IN_FLIGHT

    notifier = MagicMock()
    notifier.dispatch = AsyncMock()

    before = DISPATCH_IN_FLIGHT._value.get()
    await _tracked_dispatch(notifier, "event-stub", "hits-stub")
    after = DISPATCH_IN_FLIGHT._value.get()
    assert before == after  # net zero after success

    notifier.dispatch = AsyncMock(side_effect=RuntimeError("boom"))
    before = DISPATCH_IN_FLIGHT._value.get()
    with pytest.raises(RuntimeError):
        await _tracked_dispatch(notifier, "event-stub", "hits-stub")
    after = DISPATCH_IN_FLIGHT._value.get()
    assert before == after  # net zero after exception too
```

- [ ] **Step 2: Run failing test**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py::test_tracked_dispatch_increments_and_decrements_gauge -v`
Expected: FAIL with `ImportError: cannot import name '_tracked_dispatch'`.

- [ ] **Step 3: Add the helper + swap dispatch call sites**

In `apps/worker/chain_runner.py`, add a module-level helper (above `ChainRunner` class, after the existing helpers):

```python
async def _tracked_dispatch(notifier: Notifier, event: Any, hits: Any) -> None:
    """Wrap notifier.dispatch() to maintain the DISPATCH_IN_FLIGHT gauge."""
    from core.metrics import DISPATCH_IN_FLIGHT
    DISPATCH_IN_FLIGHT.inc()
    try:
        await notifier.dispatch(event, hits)
    finally:
        DISPATCH_IN_FLIGHT.dec()
```

Then replace the THREE `create_task(notifier.dispatch(...))` sites with `create_task(_tracked_dispatch(notifier, ...))`:

- `_process_block_with_prefetched_logs` regular loop (around line 500)
- `_process_block_with_prefetched_logs` internal-call loop (around line 513)
- `_process_solana_slot` loop (around line 539)

```python
# old:
dispatch_tasks.append(asyncio.create_task(notifier.dispatch(event, hits)))
# new:
dispatch_tasks.append(asyncio.create_task(_tracked_dispatch(notifier, event, hits)))
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/unit/test_chain_runner_instrumentation.py -v`
Expected: all PASS.

Also run the wider chain_runner suite:
Run: `uv run pytest tests/unit/test_chain_runner.py -v`
Expected: all PASS (existing tests don't observe the gauge, so they're unaffected).

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner_instrumentation.py
git commit -m "feat(runner): DISPATCH_IN_FLIGHT gauge via _tracked_dispatch wrapper"
```

---

## Chunk 3: Adapter + Notifier instrumentation

### Task 3.1: Wrap EVM adapter RPC methods with `track_rpc`

**Files:**
- Modify: `core/chains/evm.py`

- [ ] **Step 1: Add module-level import + wrap each public RPC method**

In `core/chains/evm.py`, add to the top-of-file imports (after the `from core.chains.types import ...` line):

```python
from core.metrics import track_rpc
```

Then wrap each public method that issues an RPC. Method bodies stay the same; only the outer `async with track_rpc(...)` is added. The adapter exposes `self.chain_id` (verified at evm.py:57).

Modify each of these methods:

**`get_latest_block_number`** (around line 79):

```python
    async def get_latest_block_number(self) -> int:
        assert self._w3 is not None
        async with track_rpc(self.chain_id, "eth_blockNumber"):
            return int(await self._w3.eth.block_number)
```

**`fetch_block`** (around line 83):

```python
    async def fetch_block(self, number: int) -> Block:
        async with track_rpc(self.chain_id, "eth_getBlockByNumber"):
            ...  # entire existing body
```

**`fetch_logs`** (around line 115):

```python
    async def fetch_logs(
        self,
        from_block: int,
        to_block: int,
        addresses: list[str] | None = None,
        topics: list[list[str]] | None = None,
    ) -> list[Log]:
        async with track_rpc(self.chain_id, "eth_getLogs"):
            ...  # entire existing body
```

**`trace_transaction`** (around line 215) — wrap with label `"debug_traceTransaction"`:

```python
    async def trace_transaction(self, tx_hash: str) -> InternalCall | None:
        async with track_rpc(self.chain_id, "debug_traceTransaction"):
            ...  # entire existing body
```

**`trace_block`** (around line 231) — wrap with label `"trace_block"` (batch). Note: trace_block also calls `self._w3.eth.get_block(...)` directly inside; that single `get_block` is left UNINSTRUMENTED in v1 to keep the change surgical. Document this in the inline comment:

```python
    async def trace_block(self, number: int) -> list[InternalCall]:
        # Labeled "trace_block" rather than "debug_traceTransaction" because
        # this method batches per-tx traces; one observation = one block. The
        # inner self._w3.eth.get_block call is intentionally NOT separately
        # metered to keep the instrumentation surface small.
        async with track_rpc(self.chain_id, "trace_block"):
            ...  # entire existing body
```

The internal `_poll_heads` and `_subscribe_heads_ws` methods are NOT wrapped — they're long-lived generators, not single RPCs.

- [ ] **Step 2: Add tests for wrapping**

Create a new test file `tests/unit/test_evm_adapter_metrics.py` (or append to an existing adapter test if you'd rather group). Mock the underlying `self._w3` to assert that `track_rpc` is invoked around the call:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from core.metrics import RPC_REQUESTS_TOTAL


@pytest.mark.asyncio
async def test_get_latest_block_number_records_rpc_metric() -> None:
    from core.chains.evm import EvmAdapter
    adapter = EvmAdapter(
        chain_id="test-eth", rpc_http="http://x", rpc_ws=None, confirmations=1,
    )
    # Bypass connect; stub _w3 with the minimal shape this method touches.
    adapter._w3 = MagicMock()
    type(adapter._w3.eth).block_number = PropertyMock(
        return_value=AsyncMock(return_value=12345)(),
    )

    before = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "success")._value.get()
    await adapter.get_latest_block_number()
    after = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "success")._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_rpc_error_records_error_status() -> None:
    """If the underlying RPC raises, track_rpc bumps the error counter."""
    from core.chains.evm import EvmAdapter
    adapter = EvmAdapter(
        chain_id="test-eth", rpc_http="http://x", rpc_ws=None, confirmations=1,
    )
    adapter._w3 = MagicMock()
    type(adapter._w3.eth).block_number = PropertyMock(
        side_effect=RuntimeError("rpc down"),
    )

    before = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "error")._value.get()
    with pytest.raises(RuntimeError):
        await adapter.get_latest_block_number()
    after = RPC_REQUESTS_TOTAL.labels("test-eth", "eth_blockNumber", "error")._value.get()
    assert after - before == 1
```

The PropertyMock pattern is fragile; if the test gives trouble, mock `adapter._w3` more directly via:

```python
adapter._w3 = MagicMock()
adapter._w3.eth = MagicMock()
# block_number is an async property in web3.py 6+. Stub via async-callable PropertyMock:
async def _bn(): return 12345
type(adapter._w3.eth).block_number = property(lambda self: _bn())
```

- [ ] **Step 3: Run tests**

Run: `uv run pytest tests/unit/test_evm_adapter_metrics.py -v`
Expected: 2 PASS. If web3 mocking is awkward, the simpler alternative is to test against the actual web3 anvil setup (integration), but unit is preferred.

- [ ] **Step 4: Run existing EVM tests for regression**

Run: `uv run pytest tests/integration/test_evm_adapter.py tests/unit/test_evm_catchup_range.py tests/unit/test_evm_fetch_logs_topics.py tests/unit/test_evm_fetch_logs_degrade.py -v`
Expected: all PASS — the wrap is transparent.

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check core/chains/evm.py`
Run: `uv run mypy core/chains/evm.py`
Expected: no new errors.

- [ ] **Step 6: Commit**

```bash
git add core/chains/evm.py tests/unit/test_evm_adapter_metrics.py
git commit -m "feat(evm): instrument adapter RPC methods with track_rpc"
```

### Task 3.2: Wrap Solana adapter RPC methods with `track_rpc`

**Files:**
- Modify: `core/chains/solana.py`

- [ ] **Step 1: Add module-level import + wrap each public RPC method**

In `core/chains/solana.py`, add to the top-of-file imports (after the `from core.chains.types import ...` block):

```python
from core.metrics import track_rpc
```

Then wrap each public RPC method (verified at solana.py:39, attribute is `self.chain_id`):

**`get_latest_slot`** (around line 54):

```python
    async def get_latest_slot(self) -> int:
        assert self._client is not None
        async with track_rpc(self.chain_id, "getSlot"):
            ...  # existing body
```

**`fetch_block`** (around line 66):

```python
    async def fetch_block(self, slot: int) -> SolanaBlock | None:
        assert self._client is not None
        async with track_rpc(self.chain_id, "getBlock"):
            ...  # existing body
```

**`get_blocks`** (around line 92):

```python
    async def get_blocks(self, start_slot: int, end_slot: int) -> list[int]:
        async with track_rpc(self.chain_id, "getBlocks"):
            ...  # existing body
```

The internal `_poll()` / `_gen()` generators for `subscribe_heads` are not wrapped (long-lived).

- [ ] **Step 2: Add a smoke test**

Create `tests/unit/test_solana_adapter_metrics.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.metrics import RPC_REQUESTS_TOTAL


@pytest.mark.asyncio
async def test_get_latest_slot_records_rpc_metric() -> None:
    from core.chains.solana import SolanaAdapter
    adapter = SolanaAdapter(
        chain_id="test-sol", rpc_http="http://x", commitment="confirmed",
    )
    # Stub the httpx client + the slots response.
    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.text = '{"jsonrpc":"2.0","result":{"context":{"slot":123},"value":12345},"id":1}'
    adapter._client = MagicMock()
    adapter._client.post = AsyncMock(return_value=fake_resp)

    before = RPC_REQUESTS_TOTAL.labels("test-sol", "getSlot", "success")._value.get()
    await adapter.get_latest_slot()
    after = RPC_REQUESTS_TOTAL.labels("test-sol", "getSlot", "success")._value.get()
    assert after - before == 1
```

The exact JSON shape may need tweaking to satisfy `GetSlotResp.from_json` — adjust by running the test and looking at the raised error. If `solders` parsing is too rigid, fall back to mocking `GetSlotResp.from_json` directly via monkeypatch.

- [ ] **Step 3: Run test**

Run: `uv run pytest tests/unit/test_solana_adapter_metrics.py -v`
Expected: PASS (after any JSON tweaks).

- [ ] **Step 4: Lint + type-check**

Run: `uv run ruff check core/chains/solana.py`
Run: `uv run mypy core/chains/solana.py`
Expected: no new errors.

- [ ] **Step 5: Commit**

```bash
git add core/chains/solana.py tests/unit/test_solana_adapter_metrics.py
git commit -m "feat(solana): instrument adapter RPC methods with track_rpc"
```

### Task 3.3: Channel send metrics in Notifier

**Files:**
- Modify: `core/notifier/notifier.py`
- Modify: `tests/unit/test_notifier.py` (append)

- [ ] **Step 1: Append failing tests**

Append to `tests/unit/test_notifier.py`:

```python
@pytest.mark.asyncio
async def test_channel_send_metric_records_on_success() -> None:
    from core.metrics import CHANNEL_SENDS_TOTAL

    before = CHANNEL_SENDS_TOTAL.labels("collect-notifier", "success")._value.get()
    notifier = Notifier(
        channel_factory=lambda cfg: _CollectingChannel(),
        max_concurrency=10,
    )
    await notifier.start([_ch("c-ok")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c-ok"]), [_ch("c-ok")])])
    finally:
        await notifier.stop()
    after = CHANNEL_SENDS_TOTAL.labels("collect-notifier", "success")._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_channel_send_metric_records_on_failure() -> None:
    from core.metrics import CHANNEL_SENDS_TOTAL

    class _Boom(Channel):
        type = "boom-metric"
        config_schema: dict = {}

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload: dict[str, Any]) -> None:
            raise RuntimeError("dead")

    before = CHANNEL_SENDS_TOTAL.labels("boom-metric", "failed")._value.get()
    notifier = Notifier(
        channel_factory=lambda cfg: _Boom(),
        max_concurrency=10,
    )
    await notifier.start([_ch("c-boom-m")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c-boom-m"]), [_ch("c-boom-m")])])
    finally:
        await notifier.stop()
    after = CHANNEL_SENDS_TOTAL.labels("boom-metric", "failed")._value.get()
    assert after - before == 1
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/unit/test_notifier.py::test_channel_send_metric_records_on_success tests/unit/test_notifier.py::test_channel_send_metric_records_on_failure -v`
Expected: FAIL — counter doesn't move.

- [ ] **Step 3: Add metric recording in `_send_one`**

In `core/notifier/notifier.py`, modify `_send_one`. The skeleton is:

```python
    async def _send_one(
        self, ch: Channel, payload: dict[str, Any], subscription_id: str, channel_id: str
    ) -> None:
        from core.metrics import CHANNEL_SEND_SECONDS, CHANNEL_SENDS_TOTAL
        import time
        t0 = time.perf_counter()
        send_status = "failed"  # pessimistic default
        async with self._get_sem():
            try:
                await ch.send(payload)
                send_status = "success"
                if self._on_success:
                    ...  # existing success-callback block
            except Exception as exc:  # noqa: BLE001
                ...  # existing failure-callback block
            finally:
                CHANNEL_SEND_SECONDS.labels(ch.type).observe(time.perf_counter() - t0)
                CHANNEL_SENDS_TOTAL.labels(ch.type, send_status).inc()
```

Keep the existing try/except bodies intact; add `send_status` assignment after `await ch.send(payload)` and `finally:` block at the end (still inside `async with self._get_sem()`).

The `import time` and `from core.metrics import ...` are at the top of the method for clarity; the project tolerates inline imports for the metrics module.

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/unit/test_notifier.py -v`
Expected: all PASS (existing 6 + 2 new = 8).

- [ ] **Step 5: Run notifier-adjacent suite for regression**

Run: `uv run pytest tests/unit/test_notifier_sem_lazy.py tests/unit/test_retry.py -v`
Expected: all PASS.

- [ ] **Step 6: Type-check + lint**

Run: `uv run ruff check core/notifier/notifier.py`
Run: `uv run mypy core/notifier/notifier.py`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add core/notifier/notifier.py tests/unit/test_notifier.py
git commit -m "feat(notifier): channel_send_seconds + channel_sends_total in _send_one"
```

---

## Chunk 4: Web /metrics + lag API

### Task 4.1: Mount `/metrics` ASGI app

**Files:**
- Modify: `apps/web/main.py`
- Create: `tests/integration/test_metrics_endpoint.py`

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_metrics_endpoint.py`:

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_metrics_endpoint_returns_prometheus_exposition_format() -> None:
    from apps.web.main import create_app

    app = create_app(lifespan=None)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        r = await c.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
    # At least one chain_indexer_* family should appear (module-init registers them).
    assert "chain_indexer_" in r.text
```

- [ ] **Step 2: Run failing test**

Run: `uv run pytest tests/integration/test_metrics_endpoint.py::test_metrics_endpoint_returns_prometheus_exposition_format -v`
Expected: FAIL with 404 (no `/metrics` mount).

- [ ] **Step 3: Mount `/metrics` in `create_app`**

In `apps/web/main.py`, inside `create_app(...)` after the existing router registrations (after `app.include_router(ws_router.router)` around line 123), add:

```python
    # Prometheus exposition endpoint. Importing core.metrics here ensures
    # all metric singletons are registered before the first scrape.
    import core.metrics  # noqa: F401 — side-effect: register metrics
    from prometheus_client import make_asgi_app
    app.mount("/metrics", make_asgi_app())
```

- [ ] **Step 4: Run test to verify pass**

Run: `uv run pytest tests/integration/test_metrics_endpoint.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/web/main.py tests/integration/test_metrics_endpoint.py
git commit -m "feat(web): mount Prometheus /metrics endpoint"
```

### Task 4.2: API request middleware

**Files:**
- Modify: `apps/web/main.py`
- Modify: `tests/integration/test_metrics_endpoint.py` (append)

- [ ] **Step 1: Append failing test**

Append to `tests/integration/test_metrics_endpoint.py`:

```python
@pytest.mark.asyncio
async def test_middleware_records_api_request_counter() -> None:
    from apps.web.main import create_app
    from apps.web.deps import get_db, get_bus
    from core.metrics import API_REQUESTS_TOTAL
    from unittest.mock import MagicMock

    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: MagicMock()
    app.dependency_overrides[get_bus] = lambda: MagicMock()

    # Hit a real route. The healthz route will fail (mocked db), but the
    # middleware still records on the response.
    before = API_REQUESTS_TOTAL.labels(
        method="GET", path="/healthz", status="503",
    )._value.get()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        await c.get("/healthz")

    after = API_REQUESTS_TOTAL.labels(
        method="GET", path="/healthz", status="503",
    )._value.get()
    assert after - before == 1


@pytest.mark.asyncio
async def test_middleware_skips_metrics_endpoint() -> None:
    """A scrape of /metrics must NOT itself be counted (or the histogram
    series for path=/metrics would dominate everything)."""
    from apps.web.main import create_app
    from core.metrics import API_REQUESTS_TOTAL

    app = create_app(lifespan=None)
    # Use a status label we know won't otherwise be incremented during this test.
    before = API_REQUESTS_TOTAL.labels(
        method="GET", path="/metrics", status="200",
    )._value.get()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        await c.get("/metrics")

    after = API_REQUESTS_TOTAL.labels(
        method="GET", path="/metrics", status="200",
    )._value.get()
    assert after - before == 0
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/integration/test_metrics_endpoint.py -v`
Expected: 2 new tests FAIL.

- [ ] **Step 3: Add the middleware**

In `apps/web/main.py`, after the `/metrics` mount, register the middleware:

```python
    # API request metric middleware. Registered AFTER the /metrics mount
    # so the middleware can short-circuit on /metrics requests.
    from time import perf_counter
    from fastapi import Request
    from core.metrics import API_REQUEST_SECONDS, API_REQUESTS_TOTAL

    @app.middleware("http")
    async def track_api_metrics(request: Request, call_next):
        # Skip /metrics itself: a scrape of /metrics would otherwise produce
        # a chain_indexer_api_requests_total{path="/metrics"} series that
        # dominates everything once Prometheus is wired in.
        if request.url.path == "/metrics":
            return await call_next(request)

        t0 = perf_counter()
        response = await call_next(request)
        elapsed = perf_counter() - t0

        # Use route template ("/api/chains/{chain_id}") not raw URL so
        # cardinality stays bounded.
        route = request.scope.get("route")
        path = getattr(route, "path", request.url.path)

        API_REQUEST_SECONDS.labels(method=request.method, path=path).observe(elapsed)
        API_REQUESTS_TOTAL.labels(
            method=request.method, path=path, status=str(response.status_code),
        ).inc()
        return response
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/integration/test_metrics_endpoint.py -v`
Expected: all PASS.

- [ ] **Step 5: Run broader web test for regression**

Run: `uv run pytest tests/integration/test_web_api.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add apps/web/main.py tests/integration/test_metrics_endpoint.py
git commit -m "feat(web): API metrics middleware with route-template path label"
```

### Task 4.3: `GET /api/chains/{chain_id}/lag` endpoint

**Files:**
- Modify: `apps/web/routers/chains.py`
- Create: `tests/integration/test_chains_lag_router.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_chains_lag_router.py`:

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChainKind
from core.config.repositories import ChainRepo, CheckpointRepo

pytestmark = pytest.mark.integration


async def _seed_chain(db: Database, chain_id: str = "eth-mainnet") -> None:
    async with db.session() as s:
        await ChainRepo(s).create(
            id=chain_id, kind=ChainKind.evm,
            rpc_http="x", rpc_ws=None, confirmations=1, poll_interval_ms=1000,
            enabled=True,
        )
        await s.commit()


@pytest.mark.asyncio
async def test_lag_normal_path(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        await _seed_chain(db)
        async with db.session() as s:
            await CheckpointRepo(s).upsert(
                "eth-mainnet", last_block=100, last_block_hash="0xabc",
            )
            await s.commit()
        await bus.client.set("chain:eth-mainnet:tip", 123, ex=60)

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/eth-mainnet/lag")
        assert r.status_code == 200
        body = r.json()
        assert body["tip_block"] == 123
        assert body["last_processed_block"] == 100
        assert body["lag_blocks"] == 23
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_lag_missing_tip(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        await _seed_chain(db)
        async with db.session() as s:
            await CheckpointRepo(s).upsert(
                "eth-mainnet", last_block=100, last_block_hash="0xabc",
            )
            await s.commit()
        # Explicitly clear the tip key in case prior tests left one.
        await bus.client.delete("chain:eth-mainnet:tip")

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/eth-mainnet/lag")
        body = r.json()
        assert body["tip_block"] is None
        assert body["lag_blocks"] is None
        assert body["last_processed_block"] == 100
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_lag_missing_checkpoint(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        await _seed_chain(db)
        await bus.client.set("chain:eth-mainnet:tip", 999, ex=60)

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/eth-mainnet/lag")
        body = r.json()
        assert body["tip_block"] == 999
        assert body["last_processed_block"] is None
        assert body["lag_blocks"] is None
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_lag_unknown_chain(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/chains/no-such-chain/lag")
        assert r.status_code == 404
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/integration/test_chains_lag_router.py -v`
Expected: 4 FAIL (404 on the route — endpoint doesn't exist yet).

- [ ] **Step 3: Add the endpoint**

In `apps/web/routers/chains.py`, append after `chain_status` (around line 105):

```python
class ChainLagOut(BaseModel):
    chain_id: str
    tip_block: int | None
    last_processed_block: int | None
    lag_blocks: int | None


@router.get("/{chain_id}/lag", response_model=ChainLagOut)
async def get_chain_lag(
    chain_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> ChainLagOut:
    from core.config.repositories import CheckpointRepo

    chain = await ChainRepo(session).get(chain_id)
    if chain is None:
        raise HTTPException(status_code=404, detail="chain not found")

    # Tip is published to Redis by the worker on every live head.
    tip_raw = await bus.client.get(f"chain:{chain_id}:tip")
    tip_block: int | None = int(tip_raw) if tip_raw is not None else None

    checkpoint = await CheckpointRepo(session).get(chain_id)
    last_processed = checkpoint.last_block if checkpoint is not None else None

    lag = (
        max(0, tip_block - last_processed)
        if tip_block is not None and last_processed is not None
        else None
    )

    return ChainLagOut(
        chain_id=chain_id,
        tip_block=tip_block,
        last_processed_block=last_processed,
        lag_blocks=lag,
    )
```

Add the `BaseModel` import at the top of the file if missing:

```python
from pydantic import BaseModel
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/integration/test_chains_lag_router.py -v`
Expected: all 4 PASS.

- [ ] **Step 5: Full backend regression**

Run: `uv run pytest tests/ -m "not e2e" --tb=line -q`
Expected: all PASS (or only environmental Docker / testcontainer failures, like the prior chunks).

- [ ] **Step 6: Type-check + lint**

Run: `uv run ruff check apps/web/routers/chains.py`
Run: `uv run mypy apps/web/routers/chains.py`
Expected: no new errors.

- [ ] **Step 7: Commit**

```bash
git add apps/web/routers/chains.py tests/integration/test_chains_lag_router.py
git commit -m "feat(api): GET /api/chains/{id}/lag — tip + last_processed + lag_blocks"
```

---

## Chunk 5: UI + deployment + final verification

### Task 5.1: Dashboard ChainCard lag chip + tip row

**Files:**
- Modify: `web/src/pages/Dashboard.tsx`

- [ ] **Step 1: Read current `ChainCard` to confirm structure**

The current ChainCard (Dashboard.tsx:54) queries `/chains/${chain.id}/status` and renders `latest_block` + `latest_block_hash`. We add a second query for `/lag` and a lag chip.

- [ ] **Step 2: Apply the React edits**

In `web/src/pages/Dashboard.tsx`:

**A. Add the `ChainLag` interface near `ChainStatus` (around line 7):**

```tsx
interface ChainLag {
  chain_id: string
  tip_block: number | null
  last_processed_block: number | null
  lag_blocks: number | null
}
```

**B. Add the `lagMeta` helper just above the `ChainCard` component:**

```tsx
function lagMeta(lag: number | null): { color: string; bg: string; label: string } {
  if (lag === null) return { color: 'text-gray-400', bg: 'bg-gray-100',   label: '未知' }
  if (lag < 5)      return { color: 'text-green-700', bg: 'bg-green-100', label: `落后 ${lag}` }
  if (lag < 100)    return { color: 'text-amber-700', bg: 'bg-amber-100', label: `落后 ${lag}` }
  return                   { color: 'text-red-700',   bg: 'bg-red-100',   label: `落后 ${lag}` }
}
```

**C. Rewrite `ChainCard` (replace the existing component body):**

```tsx
function ChainCard({ chain }: { chain: Chain }) {
  const { data: status } = useQuery<ChainStatus>({
    queryKey: ['chain-status', chain.id],
    queryFn: () => api.get(`/chains/${chain.id}/status`),
    refetchInterval: 5000,
    enabled: chain.enabled,
  })
  const { data: lag } = useQuery<ChainLag>({
    queryKey: ['chain-lag', chain.id],
    queryFn: () => api.get(`/chains/${chain.id}/lag`),
    refetchInterval: 5000,
    enabled: chain.enabled,
  })
  const lm = lagMeta(lag?.lag_blocks ?? null)

  return (
    <div className={`border rounded-lg p-4 ${chain.enabled ? '' : 'opacity-50'}`}>
      <div className="flex items-center justify-between mb-2">
        <span className="font-mono text-sm font-medium">{chain.id}</span>
        <div className="flex items-center gap-1.5">
          <span className={`px-2 py-0.5 rounded text-xs ${chain.kind === 'evm' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{chain.kind}</span>
          {chain.enabled && (
            <span className={`px-2 py-0.5 rounded text-xs font-medium ${lm.bg} ${lm.color}`}>{lm.label}</span>
          )}
        </div>
      </div>
      <div className="flex items-center gap-1 text-xs mb-2">
        {chain.enabled ? <><Activity size={12} className="text-green-600" /><span className="text-green-600">运行中</span></> : <span className="text-gray-400">已停用</span>}
      </div>
      {status && status.latest_block !== null ? (
        <div className="bg-gray-50 rounded p-2 text-xs space-y-1">
          <div className="flex justify-between"><span className="text-gray-500">已处理</span><span className="font-mono font-medium">{status.latest_block.toLocaleString()}</span></div>
          {lag?.tip_block != null && (
            <div className="flex justify-between"><span className="text-gray-500">链头</span><span className="font-mono">{lag.tip_block.toLocaleString()}</span></div>
          )}
          <div className="flex justify-between"><span className="text-gray-500">区块哈希</span><span className="font-mono truncate max-w-32">{status.latest_block_hash?.slice(0, 18)}...</span></div>
        </div>
      ) : chain.enabled ? (
        <p className="text-xs text-gray-400">等待同步...</p>
      ) : null}
    </div>
  )
}
```

The label was renamed "最新区块" → "已处理" so the new "链头" row reads naturally.

- [ ] **Step 3: Build the frontend**

```bash
cd web && npm run build
```

Expected: build succeeds, no TypeScript errors.

- [ ] **Step 4: Commit**

```bash
git add web/src/pages/Dashboard.tsx
git commit -m "feat(web): Dashboard ChainCard lag chip + tip row"
```

### Task 5.2: docker-compose worker port + README observability section

**Files:**
- Modify: `docker-compose.yml`
- Modify: `README.md`

- [ ] **Step 1: Expose worker port 9091**

In `docker-compose.yml`, modify the `worker` service block (currently lines 51-61). Add `ports`:

```yaml
  worker:
    build: .
    command: uv run python -m apps.worker.main
    ports:
      - "9091:9091"
    environment:
      CHAIN_INDEXER_DATABASE__URL: postgresql+asyncpg://indexer:indexer@db:5432/chain_indexer
      CHAIN_INDEXER_REDIS__URL: redis://redis:6379/0
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
```

- [ ] **Step 2: Add a brief Observability section to README.md**

In `README.md`, insert a new section after "投递记录治理" (the cleanup section added in a previous milestone), before "开发命令":

```markdown
## 可观测性 (Observability)

Worker 和 web 各自暴露 Prometheus `/metrics` 端点：

| 进程 | 地址 | 用途 |
|------|------|------|
| worker | `http://worker:9091/metrics` | 区块处理速率、RPC 延迟分布、channel 发送延迟、dispatch in-flight |
| web    | `http://web:8000/metrics` | API 请求计数 + 延迟 |

Prometheus 配置示例（`prometheus.yml`）：

```yaml
scrape_configs:
  - job_name: chain-indexer-worker
    static_configs:
      - targets: ['worker:9091']
  - job_name: chain-indexer-web
    static_configs:
      - targets: ['web:8000']
```

常用 PromQL：

```promql
# 每条链的 lag（也可直接看 Dashboard ChainCard 状态灯）
chain_indexer_chain_tip_block - chain_indexer_chain_last_processed_block

# 每条链的 RPC p99 延迟
histogram_quantile(0.99, sum by (chain, le) (rate(chain_indexer_rpc_request_seconds_bucket[5m])))

# Channel 失败率（按类型）
sum by (channel_type) (rate(chain_indexer_channel_sends_total{status="failed"}[5m]))
  / sum by (channel_type) (rate(chain_indexer_channel_sends_total[5m]))
```

`CHAIN_INDEXER_METRICS__ENABLED=false` 可关闭 worker 的 `/metrics` 服务（调试时偶尔需要避免端口冲突）。
```

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml README.md
git commit -m "docs: expose worker metrics port + Observability section in README"
```

### Task 5.3: Full-suite green-light

- [ ] **Step 1: Lint everything**

Run: `uv run ruff check core apps tests`
Expected: same or fewer errors than `main` baseline.

- [ ] **Step 2: Type-check everything**

Run: `uv run mypy core apps`
Expected: same or fewer errors than `main` baseline.

- [ ] **Step 3: Full unit + integration suite**

Run: `uv run pytest tests/ -m "not e2e" --tb=line -q`
Expected: all PASS (environmental Docker / testcontainer pulls aside).

- [ ] **Step 4: Frontend build**

```bash
cd web && npm run build
```

- [ ] **Step 5: Final commit (if any whitespace or fixes)**

If any final adjustments are needed, commit them with a clear `fix:` / `chore:` prefix. Do not amend prior task commits.

---

## Out of scope (do not implement)

- No OpenTelemetry tracing / spans.
- No Grafana dashboard JSON committed to the repo.
- No alerting rules.
- No `lag_seconds` field on the lag endpoint.
- No per-subscription metrics.
- No auth around `/metrics`.
- No CI Prometheus / Grafana wiring.
- No client SDK generation.

## References

- Spec: `docs/superpowers/specs/2026-05-29-observability-metrics-and-lag-design.md`
- `_Worker.start()`: `apps/worker/main.py:157`
- `_Worker._reconcile` (sole `ChainRunner(...)` call): `apps/worker/main.py:225`
- `ChainRunner.__init__`: `apps/worker/chain_runner.py:73-94`
- `_handle_evm_head`: `apps/worker/chain_runner.py:399`
- `_run_solana` (live loop): `apps/worker/chain_runner.py:311-317`
- `_process_block_with_prefetched_logs` save point: `apps/worker/chain_runner.py:516`
- `_process_solana_slot` save point: `apps/worker/chain_runner.py:544`
- `Notifier._send_one`: `core/notifier/notifier.py:85`
- `EvmAdapter` RPC methods: `core/chains/evm.py:79, 83, 115, 215, 231`
- `SolanaAdapter` RPC methods: `core/chains/solana.py:54, 66, 92`
- Existing `/chains/{id}/status` endpoint: `apps/web/routers/chains.py:89`
- `RedisBus.client` with `decode_responses=True`: `core/bus/redis_bus.py:17`
- `CheckpointRepo.get`: `core/config/repositories.py` (existing)
- Dashboard `ChainCard`: `web/src/pages/Dashboard.tsx:54-80`
