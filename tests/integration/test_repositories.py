from datetime import UTC

import pytest

from core.config.models import AbiKind, ChainKind, ChannelType, MatchKind
from core.config.repositories import (
    AbiRepo,
    ChainRepo,
    ChannelRepo,
    CheckpointRepo,
    ConfigVersionRepo,
    SubscriptionRepo,
)
from core.config.snapshot import load_snapshot

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_chain_crud(db) -> None:
    async with db.session() as s:
        repo = ChainRepo(s)
        await repo.create(
            id="eth-mainnet",
            kind=ChainKind.evm,
            rpc_http="http://localhost:8545",
            rpc_ws=None,
            confirmations=12,
            poll_interval_ms=3000,
            enabled=True,
        )
        await s.commit()
    async with db.session() as s:
        repo = ChainRepo(s)
        rows = await repo.list_enabled()
        assert len(rows) == 1
        assert rows[0].id == "eth-mainnet"


@pytest.mark.asyncio
async def test_config_version_bump_is_atomic(db) -> None:
    async with db.session() as s:
        repo = ConfigVersionRepo(s)
        v1 = await repo.bump()
        v2 = await repo.bump()
        v3 = await repo.bump()
        await s.commit()
    assert v1 == 1 and v2 == 2 and v3 == 3


@pytest.mark.asyncio
async def test_subscription_with_channels(db) -> None:
    async with db.session() as s:
        c_repo = ChainRepo(s)
        await c_repo.create(
            id="eth-mainnet",
            kind=ChainKind.evm,
            rpc_http="x",
            rpc_ws=None,
            confirmations=12,
            poll_interval_ms=3000,
            enabled=True,
        )
        ch_repo = ChannelRepo(s)
        ch = await ch_repo.create(name="hook", type=ChannelType.http, config={"url": "http://x"})
        sub_repo = SubscriptionRepo(s)
        sub = await sub_repo.create(
            name="wallet1",
            chain_id="eth-mainnet",
            address="0xabc",
            abi_id=None,
            match_kind=MatchKind.native_transfer,
            match_name=None,
            arg_filters={},
            enabled=True,
        )
        await sub_repo.bind_channel(sub.id, ch.id)
        await s.commit()
    async with db.session() as s:
        sub_repo = SubscriptionRepo(s)
        bindings = await sub_repo.list_enabled_with_channels()
        assert len(bindings) == 1
        sub, channels = bindings[0]
        assert sub.address == "0xabc"
        assert len(channels) == 1
        assert channels[0].type == ChannelType.http


@pytest.mark.asyncio
async def test_checkpoint_upsert(db) -> None:
    async with db.session() as s:
        c_repo = ChainRepo(s)
        await c_repo.create(
            id="eth-mainnet", kind=ChainKind.evm, rpc_http="x",
            rpc_ws=None, confirmations=12, poll_interval_ms=3000, enabled=True,
        )
        cp = CheckpointRepo(s)
        await cp.upsert("eth-mainnet", last_block=100, last_block_hash="0xaa")
        await cp.upsert("eth-mainnet", last_block=101, last_block_hash="0xbb")
        await s.commit()
    async with db.session() as s:
        cp = CheckpointRepo(s)
        row = await cp.get("eth-mainnet")
        assert row is not None
        assert row.last_block == 101 and row.last_block_hash == "0xbb"


@pytest.mark.asyncio
async def test_load_snapshot_round_trip(db) -> None:
    async with db.session() as s:
        await ChainRepo(s).create(
            id="eth-mainnet", kind=ChainKind.evm, rpc_http="http://x",
            rpc_ws=None, confirmations=12, poll_interval_ms=3000, enabled=True,
        )
        ch = await ChannelRepo(s).create(name="hook", type=ChannelType.http, config={"url": "http://x"})
        sub = await SubscriptionRepo(s).create(
            name="wallet1", chain_id="eth-mainnet", address="0xabc", abi_id=None,
            match_kind=MatchKind.native_transfer, match_name=None,
            arg_filters={"to": "0xabc"}, enabled=True,
        )
        await SubscriptionRepo(s).bind_channel(sub.id, ch.id)
        await ConfigVersionRepo(s).bump()
        await s.commit()

    async with db.session() as s:
        snap = await load_snapshot(s)
    assert snap.version == 1
    assert len(snap.chains) == 1 and snap.chains[0].id == "eth-mainnet"
    assert len(snap.subscriptions) == 1
    assert snap.subscriptions[0].channel_ids == [ch.id]
    assert snap.subscriptions[0].arg_filters == {"to": "0xabc"}
    assert len(snap.channels) == 1
    assert snap.channels[0].type == "http"


_ERC20_TRANSFER_EVENT = {
    "name": "Transfer", "type": "event", "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}


@pytest.mark.asyncio
async def test_abi_repo_create_get_list(db) -> None:
    """Sanity check that M1's create/get/list_all still work — guards against
    any accidental signature drift introduced by the delete patch."""
    async with db.session() as s:
        row = await AbiRepo(s).create(
            name="erc20", kind=AbiKind.evm_abi, body=[_ERC20_TRANSFER_EVENT],
        )
        await s.commit()
        abi_id = row.id
        assert abi_id

    async with db.session() as s:
        got = await AbiRepo(s).get(abi_id)
        assert got is not None
        assert got.name == "erc20"
        assert got.kind == AbiKind.evm_abi
        rows = await AbiRepo(s).list_all()
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_abi_repo_delete_removes_row(db) -> None:
    async with db.session() as s:
        row = await AbiRepo(s).create(
            name="erc20", kind=AbiKind.evm_abi, body=[_ERC20_TRANSFER_EVENT],
        )
        await s.commit()
        abi_id = row.id

    async with db.session() as s:
        await AbiRepo(s).delete(abi_id)
        await s.commit()

    async with db.session() as s:
        assert await AbiRepo(s).get(abi_id) is None


@pytest.mark.asyncio
async def test_abi_repo_delete_unknown_id_is_noop(db) -> None:
    """delete() on a non-existent id must NOT raise — the router layer is
    responsible for 404 surfacing (Task 2.3 does a get-first guard)."""
    async with db.session() as s:
        await AbiRepo(s).delete("no-such-id")
        await s.commit()


@pytest.mark.asyncio
async def test_cleanup_success_deletes_oldest_excess(db) -> None:
    """Insert 60 success + 10 failed; with keep=50, batch=100,
    expect 10 success deleted (oldest), all 10 failed untouched,
    and the 50 newest success rows remain."""
    from datetime import datetime, timedelta

    from core.config.models import DeliveryRecord, DeliveryStatus
    from core.config.repositories import DeliveryRecordRepo

    base = datetime(2026, 1, 1, tzinfo=UTC)

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        # Insert oldest first so created_at orders monotonically.
        # We set created_at manually because SQLite's CURRENT_TIMESTAMP only has
        # second-level resolution — without explicit timestamps, the "oldest 10"
        # set would be non-deterministic in a tight insert loop.
        success_ids: list[str] = []
        for i in range(60):
            row = await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
            row.created_at = base + timedelta(seconds=i)
            success_ids.append(row.id)
        failed_ids: list[str] = []
        for i in range(10):
            row = await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"f": i}, error="err", status="failed",
            )
            row.created_at = base + timedelta(seconds=100 + i)
            failed_ids.append(row.id)
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        deleted = await repo.cleanup_success(keep=50, batch=100)
        await s.commit()
        assert deleted == 10

    # Verify exactly the 10 oldest success rows are gone; failed untouched
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        from sqlalchemy import select

        from core.config.models import DeliveryRecord, DeliveryStatus
        r = await s.execute(
            select(DeliveryRecord).where(DeliveryRecord.status == DeliveryStatus.success)
        )
        remaining_success = [row.id for row in r.scalars().all()]
        assert len(remaining_success) == 50
        # The 10 deleted should be the oldest = success_ids[0:10]
        assert all(i not in remaining_success for i in success_ids[:10])
        assert all(i in remaining_success for i in success_ids[10:])
        r = await s.execute(
            select(DeliveryRecord).where(DeliveryRecord.status == DeliveryStatus.failed)
        )
        assert len(list(r.scalars().all())) == 10


@pytest.mark.asyncio
async def test_cleanup_success_respects_batch_cap(db) -> None:
    """200 success rows, keep=50, batch=30 → 30 deleted per call.
    Need to delete 150 total; with batch=30 that's 5 full-batch iterations.
    Run 7 iterations (5 needed + slack) and assert final count = 50."""
    from datetime import datetime, timedelta

    from core.config.repositories import DeliveryRecordRepo

    base = datetime(2026, 1, 1, tzinfo=UTC)

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        for i in range(200):
            row = await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
            row.created_at = base + timedelta(seconds=i)
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        deleted = await repo.cleanup_success(keep=50, batch=30)
        await s.commit()
        assert deleted == 30

    # Run additional iterations until converged (150 to delete, 30/call → 5 calls total).
    for _ in range(6):
        async with db.session() as s:
            repo = DeliveryRecordRepo(s)
            await repo.cleanup_success(keep=50, batch=30)
            await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        from sqlalchemy import func, select

        from core.config.models import DeliveryRecord, DeliveryStatus
        r = await s.execute(
            select(func.count())
            .select_from(DeliveryRecord)
            .where(DeliveryRecord.status == DeliveryStatus.success)
        )
        assert r.scalar() == 50


@pytest.mark.asyncio
async def test_cleanup_success_noop_when_under_cap(db) -> None:
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        for i in range(10):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        deleted = await repo.cleanup_success(keep=50, batch=1000)
        await s.commit()
        assert deleted == 0


@pytest.mark.asyncio
async def test_bump_attempt_increments_and_overwrites_error(db) -> None:
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        row = await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={}, error="first failure", attempts=2, status="failed",
        )
        await s.commit()
        delivery_id = row.id

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        await repo.bump_attempt(delivery_id, error="second failure")
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        row = await repo.get(delivery_id)
        assert row is not None
        assert row.attempts == 3
        assert row.error == "second failure"


@pytest.mark.asyncio
async def test_list_all_filters_by_status(db) -> None:
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={"ok": 1}, status="success",
        )
        await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={"bad": 1}, error="err", status="failed",
        )
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        only_failed = await repo.list_all(status="failed")
        assert len(only_failed) == 1
        assert only_failed[0].status == "failed"
        only_success = await repo.list_all(status="success")
        assert len(only_success) == 1
        assert only_success[0].status == "success"
        all_rows = await repo.list_all()
        assert len(all_rows) == 2


@pytest.mark.asyncio
async def test_chain_rpc_pool_fields_round_trip(db) -> None:
    from core.config.repositories import ChainRepo
    async with db.session() as s:
        repo = ChainRepo(s)
        await repo.create(
            id="eth-pool", kind=ChainKind.evm,
            rpc_http="http://a", rpc_ws=None, confirmations=1, poll_interval_ms=1000,
            enabled=True,
            rpc_http_fallbacks=["http://b", "http://c"],
            rpc_timeout_ms=5000,
        )
        await s.commit()
    async with db.session() as s:
        row = await ChainRepo(s).get("eth-pool")
        assert row is not None
        assert row.rpc_http_fallbacks == ["http://b", "http://c"]
        assert row.rpc_timeout_ms == 5000


@pytest.mark.asyncio
async def test_chain_rpc_pool_fields_default_empty(db) -> None:
    from core.config.repositories import ChainRepo
    async with db.session() as s:
        repo = ChainRepo(s)
        await repo.create(
            id="eth-nopool", kind=ChainKind.evm,
            rpc_http="http://a", rpc_ws=None, confirmations=1, poll_interval_ms=1000,
            enabled=True,
        )
        await s.commit()
    async with db.session() as s:
        row = await ChainRepo(s).get("eth-nopool")
        assert row is not None
        assert row.rpc_http_fallbacks == []
        assert row.rpc_timeout_ms == 10000


@pytest.mark.asyncio
async def test_delivery_record_is_replay_round_trip(db) -> None:
    from core.config.repositories import DeliveryRecordRepo
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        r1 = await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={"replay": True}, status="success", is_replay=True,
        )
        r2 = await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={}, status="success",
        )
        await s.commit()
        id1, id2 = r1.id, r2.id
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        assert (await repo.get(id1)).is_replay is True
        assert (await repo.get(id2)).is_replay is False


@pytest.mark.asyncio
async def test_worker_callback_marks_is_replay(db) -> None:
    from apps.worker.main import _Worker
    from core.settings import Settings
    from core.config.repositories import DeliveryRecordRepo

    worker = _Worker(Settings())
    worker._db = db  # reuse the test DB
    await worker._on_delivery_success(
        "sub", "ch", "eth", {"replay": True}, None, 1,
    )
    async with db.session() as s:
        rows = await DeliveryRecordRepo(s).list_all(limit=10)
        assert any(r.is_replay for r in rows)
