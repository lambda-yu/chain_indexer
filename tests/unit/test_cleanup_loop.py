"""Unit tests for _Worker._run_cleanup_loop's stop-event interaction.

These tests use a minimal _Worker instance and mock its DB/repo dependencies
so the loop can be exercised without a real database.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.settings import DeliveryRecordsSettings, Settings


def _build_worker_with_mock_db(
    monkeypatch,
    *,
    max_success_rows: int = 100, interval_s: int = 30, batch: int = 1000,
    cleanup_return_value: int = 0,
) -> tuple[Any, AsyncMock]:
    """Construct a _Worker with patched _db.session() / DeliveryRecordRepo.

    Returns (worker, cleanup_mock) where cleanup_mock is the AsyncMock backing
    DeliveryRecordRepo.cleanup_success calls. Uses monkeypatch so the global
    repo module is restored after each test.
    """
    from apps.worker.main import _Worker

    settings = Settings(
        delivery_records=DeliveryRecordsSettings(
            max_success_rows=max_success_rows,
            cleanup_interval_seconds=interval_s,
            cleanup_batch_size=batch,
        ),
    )
    worker = _Worker(settings)

    # Replace _db.session() with a context-manager mock.
    fake_session = MagicMock()
    fake_session.commit = AsyncMock()
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=fake_session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    worker._db.session = MagicMock(return_value=session_cm)

    cleanup_mock = AsyncMock(return_value=cleanup_return_value)
    # Patch the repo class via monkeypatch so other tests aren't poisoned.
    import core.config.repositories as repo_mod
    monkey_repo = MagicMock()
    monkey_repo.return_value.cleanup_success = cleanup_mock
    monkeypatch.setattr(repo_mod, "DeliveryRecordRepo", monkey_repo)
    return worker, cleanup_mock


@pytest.mark.asyncio
async def test_cleanup_loop_stops_promptly_on_stop_event(monkeypatch) -> None:
    """Even with a long cleanup_interval_seconds, _stop.set() unblocks the loop
    within milliseconds (not minutes)."""
    worker, cleanup_mock = _build_worker_with_mock_db(monkeypatch, interval_s=3600)
    task = asyncio.create_task(worker._run_cleanup_loop())
    # Let the loop run one iteration so cleanup_success is called once,
    # then enter the sleep-on-stop branch.
    await asyncio.sleep(0.05)
    assert cleanup_mock.await_count >= 1
    worker._stop.set()
    # Loop should exit within a short timeout, not wait the 3600s sleep.
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_cleanup_loop_logs_exception_and_continues(monkeypatch) -> None:
    """A DB error in one iteration must not kill the loop."""
    worker, cleanup_mock = _build_worker_with_mock_db(monkeypatch, interval_s=0)
    cleanup_mock.side_effect = [RuntimeError("db down"), 5, 0]
    task = asyncio.create_task(worker._run_cleanup_loop())
    await asyncio.sleep(0.1)
    worker._stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    # Despite the raised exception in iter 1, iters 2/3 still ran.
    assert cleanup_mock.await_count >= 3


@pytest.mark.asyncio
async def test_cleanup_loop_passes_settings_to_repo(monkeypatch) -> None:
    worker, cleanup_mock = _build_worker_with_mock_db(
        monkeypatch, max_success_rows=42, batch=7, interval_s=3600,
    )
    task = asyncio.create_task(worker._run_cleanup_loop())
    await asyncio.sleep(0.05)
    worker._stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    cleanup_mock.assert_awaited_with(keep=42, batch=7)
