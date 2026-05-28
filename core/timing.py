"""Lightweight timing helper for ad-hoc bottleneck investigation.

Wrap a block of async code with `timed(label)` to emit a structlog event
with the elapsed milliseconds. The helper short-circuits to a no-op when
the env var ``CHAIN_INDEXER_TIMING=0`` is set, so it's safe to leave
calls in place across deploys.

Example::

    async with timed("block.parse", chain="eth", block=12345):
        events = list(pipeline.run(block))

Aggregate with jq::

    cat worker.log | jq -c 'select(.event=="timing")' \\
      | jq -s 'group_by(.label) | map({
            label: .[0].label,
            count: length,
            p50: (sort_by(.ms)[length/2|floor].ms),
            p99: (sort_by(.ms)[length*99/100|floor].ms),
            max: (max_by(.ms).ms)
        })'
"""
from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import structlog

log = structlog.get_logger("timing")


def _enabled() -> bool:
    return os.environ.get("CHAIN_INDEXER_TIMING", "1") != "0"


@asynccontextmanager
async def timed(label: str, **fields: Any) -> AsyncIterator[None]:
    """Log elapsed wall-clock ms of the wrapped async block under ``label``.

    Disabled when the env var ``CHAIN_INDEXER_TIMING=0``.
    """
    if not _enabled():
        yield
        return
    t0 = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
        log.info("timing", label=label, ms=elapsed_ms, **fields)
