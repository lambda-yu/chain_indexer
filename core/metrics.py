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
