import pytest

from core.config.models import ChainKind, ChannelType, MatchKind
from core.config.repositories import (
    ChainRepo,
    ChannelRepo,
    CheckpointRepo,
    ConfigVersionRepo,
    SubscriptionRepo,
)

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
