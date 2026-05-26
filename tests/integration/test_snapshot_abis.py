from __future__ import annotations

import pytest

from core.config.db import Database
from core.config.models import AbiKind
from core.config.repositories import AbiRepo
from core.config.snapshot import load_snapshot

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_load_snapshot_includes_abis(db: Database) -> None:
    async with db.session() as s:
        await AbiRepo(s).create(
            name="erc20", kind=AbiKind.evm_abi,
            body=[{"type": "event", "name": "Transfer", "inputs": []}],
        )
        await s.commit()

    async with db.session() as s:
        snap = await load_snapshot(s)

    assert len(snap.abis) == 1
    assert snap.abis[0].name == "erc20"
    assert snap.abis[0].kind == "evm_abi"
    assert snap.abis[0].body[0]["name"] == "Transfer"
