from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import pytest

from apps.worker.main import _Worker
from core.settings import Settings

pytestmark = pytest.mark.asyncio


class _SlowRunner:
    """Stand-in for ChainRunner.stop() that sleeps 0.5s."""
    def __init__(self) -> None:
        self.stop_started_at: float | None = None
        self.stop_finished_at: float | None = None

    async def stop(self) -> None:
        self.stop_started_at = time.monotonic()
        await asyncio.sleep(0.5)
        self.stop_finished_at = time.monotonic()

    async def apply_snapshot(self, snap: Any) -> None: ...


async def test_shutdown_stops_runners_in_parallel(tmp_path: Any) -> None:
    """With three slow runners, shutdown must complete in ~0.5s, not ~1.5s."""
    s = Settings(database={"url": f"sqlite+aiosqlite:///{tmp_path / 'x.sqlite'}"})
    w = _Worker(s)
    # Don't actually start db/bus; we only test the parallel-stop path.
    # Inject three fake runners directly.
    runners: dict[str, tuple[Any, asyncio.Task[None]]] = {}
    fake_runners: list[_SlowRunner] = []
    for i in range(3):
        r = _SlowRunner()
        fake_runners.append(r)

        async def _idle() -> None:
            await asyncio.Event().wait()

        t = asyncio.create_task(_idle(), name=f"fake-runner-{i}")
        runners[f"chain-{i}"] = (r, t)
    w._runners = runners  # type: ignore[assignment]

    # Patch out bus/db disconnect so we don't need real connections.
    async def _noop() -> None:
        return None
    w._bus.disconnect = _noop  # type: ignore[method-assign]
    w._db.disconnect = _noop  # type: ignore[method-assign]

    t0 = time.monotonic()
    await w.shutdown()
    elapsed = time.monotonic() - t0

    # Parallel ⇒ <= ~0.7s. Sequential would be ~1.5s.
    assert elapsed < 1.0, f"shutdown took {elapsed:.2f}s; expected <1.0s (parallel)"
    # All runners actually stopped.
    for r in fake_runners:
        assert r.stop_finished_at is not None

    # Tasks were cancelled by _stop_runner.
    for _, t in runners.values():
        assert t.cancelled() or t.done()


async def test_shutdown_disconnects_bus_after_runners_finish(tmp_path: Any) -> None:
    """Bus disconnect ordering must remain strictly AFTER all runner stops."""
    s = Settings(database={"url": f"sqlite+aiosqlite:///{tmp_path / 'x.sqlite'}"})
    w = _Worker(s)
    events: list[str] = []

    class _OrderedRunner:
        async def stop(self) -> None:
            await asyncio.sleep(0.1)
            events.append("runner-stopped")

        async def apply_snapshot(self, snap: Any) -> None: ...

    async def _idle() -> None:
        await asyncio.Event().wait()
    t = asyncio.create_task(_idle(), name="ordered-runner")
    w._runners = {"c1": (_OrderedRunner(), t)}  # type: ignore[assignment]

    async def _bus_disc() -> None:
        events.append("bus-disconnected")
    async def _db_disc() -> None:
        events.append("db-disconnected")
    w._bus.disconnect = _bus_disc  # type: ignore[method-assign]
    w._db.disconnect = _db_disc  # type: ignore[method-assign]

    await w.shutdown()
    with contextlib.suppress(asyncio.CancelledError):
        await t

    assert events == ["runner-stopped", "bus-disconnected", "db-disconnected"], events
