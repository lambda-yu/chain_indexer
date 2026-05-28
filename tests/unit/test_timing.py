from __future__ import annotations

import asyncio

import pytest
import structlog

from core.timing import timed


@pytest.fixture(autouse=True)
def _reset_structlog() -> None:
    """Other tests (test_logging.py) call configure_logging(WARNING) which
    installs a filtering wrapper that swallows our info() calls. Reset to
    defaults so capture_logs() sees everything regardless of test order."""
    structlog.reset_defaults()


@pytest.mark.asyncio
async def test_timed_emits_structured_log_with_ms_and_extra_fields() -> None:
    with structlog.testing.capture_logs() as cap:
        async with timed("block.parse", chain="eth", block=42):
            await asyncio.sleep(0.005)

    matches = [e for e in cap if e.get("event") == "timing"]
    assert len(matches) == 1
    rec = matches[0]
    assert rec["label"] == "block.parse"
    assert rec["chain"] == "eth"
    assert rec["block"] == 42
    assert isinstance(rec["ms"], (int, float))
    assert rec["ms"] >= 0


@pytest.mark.asyncio
async def test_timed_short_circuits_when_env_disabled(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_TIMING", "0")
    with structlog.testing.capture_logs() as cap:
        async with timed("block.parse"):
            pass
    assert not [e for e in cap if e.get("event") == "timing"]


@pytest.mark.asyncio
async def test_timed_logs_even_when_block_raises() -> None:
    with structlog.testing.capture_logs() as cap, pytest.raises(RuntimeError):
        async with timed("block.parse"):
            raise RuntimeError("boom")
    assert any(e.get("event") == "timing" for e in cap)
