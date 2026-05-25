import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from core.config.models import (
    Base,
    ChainKind,
    ChannelType,
    MatchKind,
)


@pytest.mark.asyncio
async def test_can_create_all_tables() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Sanity: the seven tables exist
    async with engine.begin() as conn:
        names = await conn.run_sync(
            lambda sync_conn: list(Base.metadata.tables.keys())
        )
        assert set(names) >= {
            "chains",
            "abis",
            "subscriptions",
            "channels",
            "subscription_channels",
            "checkpoints",
            "config_version",
        }
    await engine.dispose()


def test_enums_have_expected_values() -> None:
    assert {e.value for e in ChainKind} == {"evm", "solana"}
    assert {e.value for e in MatchKind} == {"native_transfer", "token_transfer", "event", "call"}
    assert {e.value for e in ChannelType} == {"mq", "http", "ws"}
    from core.config.models import AbiKind
    assert {e.value for e in AbiKind} == {"evm_abi", "solana_idl"}
