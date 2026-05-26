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
