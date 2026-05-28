from __future__ import annotations

import asyncio

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_worker_cleanup_loop_converges(db, redis_url) -> None:
    """Real _Worker against real DB. Insert 20 success rows + 5 failed;
    set keep=5; assert convergence to 5 success rows within a few seconds
    while failed rows remain untouched."""
    from apps.worker.main import _Worker
    from core.config.repositories import DeliveryRecordRepo
    from core.settings import (
        DatabaseSettings,
        DeliveryRecordsSettings,
        RedisSettings,
        Settings,
    )

    # Seed data
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        for i in range(20):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
        for i in range(5):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"f": i}, error="x", status="failed",
            )
        await s.commit()

    # Database URL doesn't matter here — we overwrite worker._db with the
    # test's existing Database instance below. Use any valid scheme.
    settings = Settings(
        database=DatabaseSettings(url="sqlite+aiosqlite:///:memory:"),
        redis=RedisSettings(url=redis_url),
        delivery_records=DeliveryRecordsSettings(
            max_success_rows=5,
            cleanup_interval_seconds=1,
            cleanup_batch_size=100,
        ),
    )
    # Construct worker but ONLY exercise the cleanup loop (skip full start()).
    worker = _Worker(settings)
    worker._db = db  # reuse the test's already-connected Database
    task = asyncio.create_task(worker._run_cleanup_loop())
    try:
        # Wait up to 3s for convergence.
        for _ in range(30):
            async with db.session() as s:
                from core.config.models import DeliveryRecord, DeliveryStatus
                from sqlalchemy import select, func
                r = await s.execute(
                    select(func.count())
                    .select_from(DeliveryRecord)
                    .where(DeliveryRecord.status == DeliveryStatus.success)
                )
                if r.scalar() == 5:
                    break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("cleanup loop did not converge within 3 seconds")
        # Failed rows must still be present.
        async with db.session() as s:
            from core.config.models import DeliveryRecord, DeliveryStatus
            from sqlalchemy import select, func
            r = await s.execute(
                select(func.count())
                .select_from(DeliveryRecord)
                .where(DeliveryRecord.status == DeliveryStatus.failed)
            )
            assert r.scalar() == 5
    finally:
        worker._stop.set()
        await asyncio.wait_for(task, timeout=2.0)
