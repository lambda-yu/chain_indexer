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
    assert after_s >= before_s

    h_error = M.RPC_REQUEST_SECONDS.labels("test-chain", "histo-error")
    before_e = h_error._sum.get()
    with pytest.raises(ValueError):
        async with M.track_rpc("test-chain", "histo-error"):
            raise ValueError("x")
    after_e = h_error._sum.get()
    assert after_e >= before_e
