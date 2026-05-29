# Observability: Prometheus metrics + per-chain lag — Design

**Date**: 2026-05-29
**Status**: Draft
**Scope**: Add Prometheus metrics exposition from worker and web processes, and a per-chain lag REST endpoint surfaced in the Dashboard UI.
**Milestone**: post-m5 follow-up (sub-project B of three; A=RPC layer, C=subscription lifecycle to follow)

## Background

A recent ad-hoc investigation using a `core/timing.py` helper (since reverted) confirmed RPC fetches dominate worker wall-clock (~89%) and that some channel sends have long tails (max 2.4s). Without persistent metrics infrastructure, every future investigation requires re-rolling the instrumentation. Operators cannot answer basic questions like "is the worker keeping up with the chain head?" or "which channel type has the worst p99?" without spelunking through JSON logs.

This design introduces Prometheus-format metrics on both the worker and web processes, plus a single REST endpoint that surfaces the most operationally-significant derived metric — per-chain block lag — in the Dashboard UI for at-a-glance health.

It is intentionally scoped narrowly. It is not a rewrite of the logging story, not a tracing introduction (no OpenTelemetry), and not a Grafana dashboard repository commit. It is *infrastructure for measurement*.

## Goals

- Expose Prometheus `/metrics` on the worker (port 9091) and the web app (port 8000, same as API).
- Cover the four observation surfaces with bounded-cardinality metrics:
  1. Block-processing progress per chain (counter, two gauges).
  2. RPC latency and error rate per chain × method (histogram, counter).
  3. Channel send latency and error rate per channel-type (histogram, counter).
  4. API request latency and error rate per route template × method (histogram, counter).
- Add a single derived REST endpoint `GET /api/chains/{chain_id}/lag` returning `{tip_block, last_processed_block, lag_blocks}`.
- Wire the lag endpoint into the existing Dashboard `ChainCard` as a color-coded chip plus a tip-block row.
- Provide a `track_rpc(chain, method)` async context manager so adapter instrumentation is one line per call site.
- Add a `dispatch_in_flight` gauge so backpressure problems are visible without log spelunking.

## Non-goals

- No OpenTelemetry tracing / spans. The metric set above is sufficient for the optimization work it enables.
- No Grafana dashboard JSON committed to the repo (operators wire their own).
- No alerting rules. Alertmanager configuration belongs to the deployment, not the repo.
- No metric-set tuning helpers (no `linger_ms` / custom buckets for individual call sites). Defaults are fine in v1.
- No `lag_seconds` field (would require a per-chain block-time estimate or storing block timestamps; v1 sticks to `lag_blocks`).
- No per-subscription lag metric (the user explicitly chose per-chain only).
- No `delivery_records` row-count gauge (the cleanup loop already logs deletions).
- No metric for the cleanup loop itself (it is bounded and logged on each iteration).
- No new auth surface around `/metrics`. The endpoint is open for now; if/when API auth lands, `/metrics` will be exempted (separate spec).
- No CI configuration for Prometheus / Grafana. Operators bring their own.
- No client SDK regeneration. The new `/lag` endpoint is small enough to consume directly.

## Architecture

```
┌──────────────────────────┐                  ┌──────────────────────────┐
│   Worker process         │                  │   Web process (FastAPI)  │
│                          │                  │                          │
│ asyncio loop ─┬─ runners │                  │ /healthz, /api/*,  ────► │
│               │          │                  │ /metrics (mounted        │
│               └─ cleanup │                  │  prometheus_client       │
│                          │                  │  ASGI app)               │
│                          │                  │                          │
│ start_http_server(9091)  │                  │ middleware: bump          │
│ │ daemon thread          │                  │  API_REQUESTS_TOTAL,     │
│ └─ /metrics ─────────┐   │                  │  API_REQUEST_SECONDS     │
│                      │   │                  │                          │
└───────────────────┬──┘   │                  └──────────────────────────┘
                    │      │                              │
                    ▼      │                              ▼
            Prometheus scrape   ────────────────► Prometheus scrape
              (worker:9091)                          (web:8000/metrics)

Tip publishing (cross-process):

worker ChainRunner._handle_evm_head(header)
   │
   ├─ CHAIN_TIP_BLOCK.labels(chain).set(header.number)
   │
   └─ await bus.client.set(f"chain:{chain}:tip", header.number, ex=60)
                                  │
                                  ▼
                            Redis (shared)
                                  │
   web GET /api/chains/{id}/lag ──┘
       │
       ├─ tip = redis.get(f"chain:{id}:tip")
       ├─ last_processed = checkpoint.last_block
       └─ lag = tip - last_processed (or null)
```

The two `/metrics` endpoints are independent scrape targets. The worker uses `prometheus_client.start_http_server` (its own daemon thread serving the multiprocess-safe global registry); the web uses `prometheus_client.make_asgi_app()` mounted on the existing FastAPI app.

## Data Model

No SQL migrations. Two new run-time values:

| Key | Where | Lifetime | Purpose |
|-----|-------|----------|---------|
| `chain:{chain_id}:tip` (Redis) | written by worker, read by web | TTL 60s, re-set on every head | feeds the lag REST endpoint |
| In-memory Prometheus registry | per process | process lifetime | metric counters / gauges / histograms |

Worker's `_publish_tip(chain_id, block_number)` writes the Redis key as `int(block_number)` (no JSON, no timestamp; keeps the contract dead simple). TTL is double the longest realistic chain `poll_interval_ms` plus margin. If the worker dies or stops a chain runner, the key expires and the web returns `tip_block=null` (rendered "unknown" in the UI). Web reads with `bus.client.get`.

## Configuration

`core/settings.py` gains:

```python
class MetricsSettings(BaseModel):
    enabled: bool = True
    port: int = 9091  # worker /metrics port; web uses its main port
```

Env vars:
- `CHAIN_INDEXER_METRICS__ENABLED=false` — disables the worker `/metrics` thread (testing / local dev when port 9091 conflicts).
- `CHAIN_INDEXER_METRICS__PORT=9091` — override port.

The web `/metrics` mount is unconditional; FastAPI route registration is free. Web has no separate metrics-disable knob.

## Metric Catalog

Names use the `chain_indexer_` prefix; counters end in `_total`; durations end in `_seconds` per Prometheus convention. Labels chosen for low cardinality (no `subscription_id`, `channel_id`, `block_number`, or raw URL).

### Worker (10 series families)

| Metric | Type | Labels | Notes |
|--------|------|--------|-------|
| `chain_indexer_blocks_processed_total` | Counter | `chain` | Incremented at end of `_process_block_with_prefetched_logs`. |
| `chain_indexer_chain_tip_block` | Gauge | `chain` | Set on every `_handle_evm_head` / `_process_solana_slot`. |
| `chain_indexer_chain_last_processed_block` | Gauge | `chain` | Set after `_cp.save(...)`. |
| `chain_indexer_rpc_request_seconds` | Histogram | `chain`, `method` | Default buckets. Method is the adapter-internal name (`eth_getBlockByNumber`, `eth_getLogs`, `getBlock`, etc.). |
| `chain_indexer_rpc_requests_total` | Counter | `chain`, `method`, `status` | `status ∈ {success, error}`. |
| `chain_indexer_channel_send_seconds` | Histogram | `channel_type` | Per-attempt latency, no retry-backoff inflation. |
| `chain_indexer_channel_sends_total` | Counter | `channel_type`, `status` | `status ∈ {success, failed}`. |
| `chain_indexer_dispatch_in_flight` | Gauge | — | Inc when an event's dispatch task starts, dec on completion. Spot backpressure. |
| `chain_indexer_worker_up` | Gauge | — | Set to 1 after `start_http_server` returns. |
| `chain_indexer_worker_info` | Gauge | `worker_id`, `version` | Constant 1. |

### Web (2 series families)

| Metric | Type | Labels | Notes |
|--------|------|--------|-------|
| `chain_indexer_api_requests_total` | Counter | `method`, `path`, `status` | `path` = FastAPI route template (`/api/chains/{id}`), not raw URL. `status` = HTTP status code as string. |
| `chain_indexer_api_request_seconds` | Histogram | `method`, `path` | Default buckets. |

### Cardinality estimate

- `chain`: ~5 typical, ~20 worst case.
- `method` (RPC): ~6 EVM + ~5 Solana = ~11.
- `channel_type`: 5 (http, mq, kafka, ws, rabbitmq).
- `path`: ~25 (FastAPI routes).
- `status`: 2–4 per metric.

Worst-case Worker: `rpc_requests_total = 20 × 11 × 2 ≈ 440` series. Worst-case Web: `api_requests_total = 7 × 25 × 8 ≈ 1400`. Both well under Prometheus's per-instance comfort zone (~1M).

## Component 1: `core/metrics.py`

New module. Houses all metric singletons and the `track_rpc` async context manager. Imports `prometheus_client.Counter|Gauge|Histogram` and uses the default global registry (no custom registry — both worker and web use the same module, but they are separate processes so no leakage).

```python
# core/metrics.py
from __future__ import annotations
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from prometheus_client import Counter, Gauge, Histogram

BLOCKS_PROCESSED_TOTAL = Counter(
    "chain_indexer_blocks_processed_total",
    "Confirmed blocks fully processed.", ["chain"],
)
CHAIN_TIP_BLOCK = Gauge(
    "chain_indexer_chain_tip_block",
    "Latest head block number from RPC subscribe_heads.", ["chain"],
)
CHAIN_LAST_PROCESSED_BLOCK = Gauge(
    "chain_indexer_chain_last_processed_block",
    "Last fully-processed block number (checkpoint).", ["chain"],
)
RPC_REQUEST_SECONDS = Histogram(
    "chain_indexer_rpc_request_seconds",
    "RPC call latency.", ["chain", "method"],
)
RPC_REQUESTS_TOTAL = Counter(
    "chain_indexer_rpc_requests_total",
    "RPC call count.", ["chain", "method", "status"],
)
CHANNEL_SEND_SECONDS = Histogram(
    "chain_indexer_channel_send_seconds",
    "Channel send latency (single attempt).", ["channel_type"],
)
CHANNEL_SENDS_TOTAL = Counter(
    "chain_indexer_channel_sends_total",
    "Channel send count.", ["channel_type", "status"],
)
DISPATCH_IN_FLIGHT = Gauge(
    "chain_indexer_dispatch_in_flight",
    "Current in-flight dispatch tasks (per-event fan-out).",
)
WORKER_UP = Gauge(
    "chain_indexer_worker_up", "Worker is up and serving metrics.",
)
WORKER_INFO = Gauge(
    "chain_indexer_worker_info", "Worker identity.", ["worker_id", "version"],
)
API_REQUEST_SECONDS = Histogram(
    "chain_indexer_api_request_seconds",
    "Web API request latency.", ["method", "path"],
)
API_REQUESTS_TOTAL = Counter(
    "chain_indexer_api_requests_total",
    "Web API request count.", ["method", "path", "status"],
)


@asynccontextmanager
async def track_rpc(chain: str, method: str) -> AsyncIterator[None]:
    """Wrap an RPC call to record latency and status."""
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

The module is import-side-effect-free except for metric registration. Importing it twice (worker + web in same process during tests) is safe — `prometheus_client` raises on duplicate registration; tests that need fresh state use `prometheus_client.REGISTRY.unregister(...)` or import the module once.

## Component 2: Worker `_metrics` startup

`apps/worker/main.py:_Worker.start()` gets a tail block:

```python
if self._settings.metrics.enabled:
    from prometheus_client import start_http_server
    from core.metrics import WORKER_INFO, WORKER_UP
    start_http_server(self._settings.metrics.port)
    WORKER_UP.set(1)
    WORKER_INFO.labels(worker_id=self._worker_id, version="dev").set(1)
    log.info("worker.metrics_server_started", port=self._settings.metrics.port)
```

`start_http_server` spins a daemon thread; it does not coexist with the asyncio loop. On shutdown, the daemon thread dies with the process — no explicit teardown needed.

## Component 3: Adapter instrumentation

Each method in `core/chains/evm.py` that issues an RPC wraps the call with `track_rpc`:

```python
async def fetch_block(self, number: int) -> Block:
    async with track_rpc(self._chain_id, "eth_getBlockByNumber"):
        return await self._fetch_block_impl(number)

async def fetch_logs(self, from_block, to_block, addresses, topics):
    async with track_rpc(self._chain_id, "eth_getLogs"):
        return await self._fetch_logs_impl(...)

async def get_latest_block_number(self) -> int:
    async with track_rpc(self._chain_id, "eth_blockNumber"):
        return await self._latest_impl()

async def trace_block(self, number):
    async with track_rpc(self._chain_id, "debug_traceTransaction"):
        return await self._trace_impl(number)
```

`subscribe_heads` does not wrap — it's a long-lived generator, not a single RPC. (Per-head WebSocket events are not metered; they're plumbed straight to the runner.)

`core/chains/solana.py` does the analogous wrap: `getBlock`, `getBlocks`, `getSlot`, `getBlockHeight`, `getBlockTime`. The slot-WS subscription is similarly unmetered.

If a method internally does multiple RPC calls (e.g. fetching ancestors), only the outer call is timed. A future iteration could decompose, but v1 measures the call site that matters operationally.

## Component 4: ChainRunner instrumentation + tip publishing

`apps/worker/chain_runner.py` gains:

**Constructor** — new optional `tip_publisher: Callable[[str, int], Awaitable[None]] | None = None` parameter. `_Worker` wires this to its `_publish_tip` method.

**`_handle_evm_head(header)`** — top of function:

```python
CHAIN_TIP_BLOCK.labels(chain=self._chain.id).set(header.number)
if self._tip_publisher is not None:
    await self._tip_publisher(self._chain.id, header.number)
```

**`_process_solana_slot(slot)`** — analogous treatment using `slot` as the tip:

```python
CHAIN_TIP_BLOCK.labels(chain=self._chain.id).set(slot)
if self._tip_publisher is not None:
    await self._tip_publisher(self._chain.id, slot)
```

**`_process_block_with_prefetched_logs`** — after `await self._cp.save(...)`:

```python
BLOCKS_PROCESSED_TOTAL.labels(chain=self._chain.id).inc()
CHAIN_LAST_PROCESSED_BLOCK.labels(chain=self._chain.id).set(block.header.number)
```

**Dispatch wrapper** — replace the `asyncio.create_task(notifier.dispatch(...))` call with a tracked helper:

```python
async def _tracked_dispatch(notifier, event, hits):
    DISPATCH_IN_FLIGHT.inc()
    try:
        await notifier.dispatch(event, hits)
    finally:
        DISPATCH_IN_FLIGHT.dec()
```

Used at both call sites (the regular `for event in events:` loop and the trace-internal-call loop).

## Component 5: `_Worker._publish_tip`

`apps/worker/main.py` adds:

```python
async def _publish_tip(self, chain_id: str, block_number: int) -> None:
    try:
        await self._bus.client.set(
            f"chain:{chain_id}:tip", block_number, ex=60,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("worker.publish_tip_failed", chain_id=chain_id, error=repr(exc))
```

Wired into `ChainRunner(... tip_publisher=self._publish_tip)` at both runner construction sites (initial + reconcile).

A Redis hiccup must not kill the chain runner — exceptions are logged and swallowed. Lag staleness manifests in the UI as a stale chip; the chain runner keeps processing.

## Component 6: Notifier instrumentation

`core/notifier/notifier.py:_send_one` wraps the existing `await ch.send(payload)` call:

```python
async def _send_one(self, ch, payload, subscription_id, channel_id):
    async with self._get_sem():
        t0 = time.perf_counter()
        send_status = "failed"
        try:
            await ch.send(payload)
            send_status = "success"
            if self._on_success:
                ...
        except Exception as exc:
            ...
        finally:
            CHANNEL_SEND_SECONDS.labels(ch.type).observe(time.perf_counter() - t0)
            CHANNEL_SENDS_TOTAL.labels(ch.type, send_status).inc()
```

The `send_status` mutation is necessary because the success-path commit and failure-path commit live in different branches. `finally` runs regardless and records both metrics.

## Component 7: Web `/metrics` mount

`apps/web/main.py:create_app`:

```python
from prometheus_client import make_asgi_app

def create_app(...) -> FastAPI:
    app = FastAPI(...)
    app.include_router(...)
    app.mount("/metrics", make_asgi_app())
    app.middleware("http")(track_api_metrics)  # registered as function below
    return app
```

`make_asgi_app()` returns a multi-process-safe Prometheus exposition app. Mounting it on the FastAPI app uses Starlette's existing mount machinery — no new dependency.

## Component 8: Web API metrics middleware

`apps/web/main.py` (or a new `apps/web/middleware.py` if `main.py` is already crowded):

```python
from time import perf_counter
from fastapi import Request
from core.metrics import API_REQUEST_SECONDS, API_REQUESTS_TOTAL


async def track_api_metrics(request: Request, call_next):
    # Skip /metrics itself to avoid self-observation noise.
    if request.url.path == "/metrics":
        return await call_next(request)

    t0 = perf_counter()
    response = await call_next(request)
    elapsed = perf_counter() - t0

    # Route template (e.g. "/api/chains/{chain_id}") not raw path.
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)

    API_REQUEST_SECONDS.labels(method=request.method, path=path).observe(elapsed)
    API_REQUESTS_TOTAL.labels(
        method=request.method, path=path, status=str(response.status_code),
    ).inc()
    return response
```

Skipping `/metrics` matters: middleware otherwise records every Prometheus scrape, producing a noisy `chain_indexer_api_request_seconds{path="/metrics"}` series.

## Component 9: `GET /api/chains/{chain_id}/lag`

`apps/web/routers/chains.py` gains:

```python
from pydantic import BaseModel
from core.config.repositories import CheckpointRepo


class ChainLagOut(BaseModel):
    chain_id: str
    tip_block: int | None
    last_processed_block: int | None
    lag_blocks: int | None


@router.get("/{chain_id}/lag", response_model=ChainLagOut)
async def get_chain_lag(
    chain_id: str,
    session: AsyncSession = Depends(get_session),
    bus: RedisBus = Depends(get_bus),
) -> ChainLagOut:
    chain = await ChainRepo(session).get(chain_id)
    if chain is None:
        raise HTTPException(status_code=404, detail="chain not found")

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

`bus.client.get` returns `bytes` (when `decode_responses=False`) or `str` (when `True`). The project's `RedisBus` uses `decode_responses=True`, so `tip_raw` is `str` or `None`. `int(str)` parses fine.

`max(0, ...)` guards against a transient state where the worker's checkpoint lags Redis's tip but the tip key has expired and been replaced with an older value (theoretically impossible since TTL covers the gap, but defensive).

### Edge-case table

| Scenario | tip_block | last_processed_block | lag_blocks | UI |
|----------|-----------|---------------------|------------|-----|
| Normal | int | int | int | 🟢 / 🟡 / 🔴 by threshold |
| Worker just started, no head yet | null | null or int | null | ⚪ unknown |
| Worker died >60s | null | int | null | ⚪ unknown |
| Chain added, never processed | int | null | null | ⚪ pending |
| Catchup mode, lag huge | int | int | int (large) | 🔴 |

## Component 10: Dashboard ChainCard

`web/src/pages/Dashboard.tsx` patches `ChainCard`:

- Add `useQuery<ChainLag>` for `/chains/${chain.id}/lag` with `refetchInterval: 5000` and `enabled: chain.enabled`.
- Add `lagMeta(lag)` helper that returns `{color, bg, label}` for the 4 states (unknown / green / amber / red).
- Render the chip in the card header next to the `[evm|solana]` chip.
- Add a "链头" row (tip block) under "已处理" when `lag?.tip_block` is non-null.
- Rename existing "最新区块" label to "已处理" so the distinction with "链头" is clear.

Thresholds (hard-coded in v1):
- 🟢 lag < 5
- 🟡 5 ≤ lag < 100
- 🔴 lag ≥ 100
- ⚪ lag === null

## Settings precedence

`Settings` gains `metrics: MetricsSettings = Field(default_factory=MetricsSettings)`. No precedence changes; env vars override YAML override defaults, as today.

## Error handling

- **Worker /metrics port collision**: `start_http_server` raises `OSError` immediately; this propagates from `_Worker.start()` and crashes the worker (intentional — start-up failures should be visible). Operator can set `CHAIN_INDEXER_METRICS__PORT=...` or `CHAIN_INDEXER_METRICS__ENABLED=false`.
- **Redis hiccup during `_publish_tip`**: caught, logged, runner continues. UI shows ⚪ unknown after TTL expires.
- **Redis hiccup during `get_chain_lag`**: propagates as 500 from the endpoint. The Dashboard query retries every 5s; the UI shows "unknown" via the React Query error path. (No special handling in the endpoint.)
- **`int(tip_raw)` parse error**: would only happen if something else writes a non-integer to the key. Propagates as 500. Won't happen under normal operation.
- **Checkpoint missing**: handled — returns `last_processed_block=null`, `lag_blocks=null`.
- **Cardinality explosion**: cannot happen with the current label set (no per-id labels). The middleware uses route templates explicitly.

## Testing

### Unit tests

- `core/test_metrics.py`:
  - `track_rpc` success path: observes histogram, increments `status="success"` counter.
  - `track_rpc` exception path: still observes histogram, increments `status="error"` counter, re-raises.
  - Module import is idempotent: importing twice does not raise (covers test-order fragility).
- `apps/worker/test_publish_tip.py`:
  - Mock `bus.client.set`, assert called with `f"chain:{id}:tip", N, ex=60`.
  - Mock raising `set`, assert no exception propagates, assert warning logged.
- `core/notifier/test_notifier.py` (extend):
  - Success send increments `chain_indexer_channel_sends_total{status="success"}`.
  - Failed send increments `chain_indexer_channel_sends_total{status="failed"}` and observes histogram.

### Integration tests

- `tests/integration/test_chains_lag_router.py`:
  - Normal path: seed checkpoint, set Redis key, GET returns correct lag.
  - Missing tip: GET returns `tip_block=null`, `lag_blocks=null`.
  - Missing checkpoint: GET returns `last_processed_block=null`, `lag_blocks=null`.
  - Unknown chain: GET returns 404.
- `tests/integration/test_metrics_endpoint.py`:
  - GET `/metrics` returns 200 with `text/plain; version=0.0.4`.
  - After a real API call (e.g. GET `/api/chains`), the `chain_indexer_api_requests_total` metric line is present.
- `tests/integration/test_worker_metrics.py` (optional, env-gated):
  - Boot a `_Worker`, GET `worker:9091/metrics`, assert `chain_indexer_worker_up 1.0`.
  - Skip if `CHAIN_INDEXER_METRICS__ENABLED=false` or port unavailable.

### Frontend smoke (manual)

- `npm run build` succeeds.
- Dashboard renders the lag chip with the right color when worker is running and lag is small.
- Stopping the worker → chip transitions to ⚪ unknown within 60s.

## File-level change summary

| File | Change |
|------|--------|
| `core/metrics.py` | NEW. All metric singletons + `track_rpc`. |
| `core/settings.py` | +`MetricsSettings`, +`Settings.metrics`. |
| `apps/worker/main.py` | +`_publish_tip`. `start()` launches `/metrics` server, sets `WORKER_UP`, `WORKER_INFO`. Pass `tip_publisher=self._publish_tip` to `ChainRunner(...)`. |
| `apps/worker/chain_runner.py` | +`tip_publisher` ctor param. `_handle_evm_head` / `_process_solana_slot` update tip gauge + call publisher. `_process_block_with_prefetched_logs` increments counter + sets last-processed gauge. `_tracked_dispatch` helper wraps `notifier.dispatch`. |
| `core/chains/evm.py` | Each RPC method wraps with `track_rpc`. |
| `core/chains/solana.py` | Each RPC method wraps with `track_rpc`. |
| `core/notifier/notifier.py` | `_send_one` records `CHANNEL_SEND_SECONDS` + `CHANNEL_SENDS_TOTAL`. |
| `apps/web/main.py` | Mount `/metrics`. Register `track_api_metrics` middleware. |
| `apps/web/routers/chains.py` | +`get_chain_lag` endpoint, +`ChainLagOut` pydantic. |
| `web/src/pages/Dashboard.tsx` | `ChainCard` adds `/lag` query + `lagMeta` helper + lag chip + tip-block row. Rename "最新区块" → "已处理". |
| `pyproject.toml` | +`prometheus-client`. |
| `docker-compose.yml` | Expose worker port 9091. |
| `README.md` | +brief operator notes on scraping (1 paragraph). |
| `tests/...` | New tests per the Testing section. |

## Rollout

Single PR. No database migration. No config required to run defaults — `enabled=True` and `port=9091` are sensible. Operators wanting to disable in dev set `CHAIN_INDEXER_METRICS__ENABLED=false`.

Prometheus scrape configuration is documented in the README under a new "Observability" section: two scrape targets, `web:8000/metrics` and `worker:9091/metrics`. The repo does not commit a Prometheus YAML.

## Open questions

None blocking. Possible follow-ups (out of scope here):

- `lag_seconds` field on the lag endpoint once block timestamps are persisted in checkpoints.
- A second metric port for the web app if the project ever wants to gate `/api/*` behind auth while keeping `/metrics` open.
- Custom histogram buckets per metric once we have enough data to know the right bucket boundaries.
- A repo-committed Grafana dashboard JSON once the metric shape stabilizes.
