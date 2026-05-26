from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from core.config.db import Database
from core.config.models import Base, MatchKind, Subscription
from scripts.validate_arg_filters import Offender, scan_database


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def _insert_subscription(
    db: Database, *, name: str, arg_filters: dict[str, Any]
) -> str:
    async with db.session() as s:
        row = Subscription(
            name=name,
            chain_id="eth-mainnet",
            address=None,
            abi_id=None,
            match_kind=MatchKind.native_transfer,
            match_name=None,
            arg_filters=arg_filters,
            enabled=True,
        )
        s.add(row)
        await s.commit()
        return row.id


@pytest.mark.asyncio
async def test_scanner_returns_empty_for_clean_database(db: Database) -> None:
    await _insert_subscription(db, name="good1", arg_filters={"from": "0xabc"})
    await _insert_subscription(db, name="good2", arg_filters={"value_gte": 100})
    offenders = await scan_database(db)
    assert offenders == []


@pytest.mark.asyncio
async def test_scanner_flags_nested_dict_in_value(db: Database) -> None:
    bad_id = await _insert_subscription(
        db, name="bad-nested", arg_filters={"from": {"nested": "x"}}
    )
    offenders = await scan_database(db)
    assert len(offenders) == 1
    assert offenders[0].subscription_id == bad_id
    assert offenders[0].name == "bad-nested"


@pytest.mark.asyncio
async def test_scanner_flags_typo_operator(db: Database) -> None:
    bad_id = await _insert_subscription(
        db, name="bad-eq", arg_filters={"value_eq": 100}
    )
    offenders = await scan_database(db)
    assert len(offenders) == 1
    assert offenders[0].subscription_id == bad_id


@pytest.mark.asyncio
async def test_scanner_reports_multiple_offenders(db: Database) -> None:
    await _insert_subscription(db, name="ok", arg_filters={"from": "0x1"})
    await _insert_subscription(db, name="bad1", arg_filters={"x": None})
    await _insert_subscription(db, name="bad2", arg_filters={"y_ne": "z"})
    offenders = await scan_database(db)
    names = {o.name for o in offenders}
    assert names == {"bad1", "bad2"}


def test_main_exits_zero_when_clean(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    from scripts import validate_arg_filters as mod

    async def _empty_scan(_db: Database) -> list[Offender]:
        return []

    async def _fake_connect(self: Database) -> None:
        return None

    async def _fake_disconnect(self: Database) -> None:
        return None

    monkeypatch.setattr(mod, "scan_database", _empty_scan)
    monkeypatch.setattr(Database, "connect", _fake_connect)
    monkeypatch.setattr(Database, "disconnect", _fake_disconnect)

    rc = mod.main(["--database-url", "sqlite+aiosqlite:///:memory:"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "0 offenders" in captured.out.lower()


def test_main_exits_one_when_offenders(monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    from scripts import validate_arg_filters as mod

    async def _scan(_db: Database) -> list[Offender]:
        return [Offender(subscription_id="abc", name="bad-row", reason="bad value shape")]

    async def _fake_connect(self: Database) -> None:
        return None

    async def _fake_disconnect(self: Database) -> None:
        return None

    monkeypatch.setattr(mod, "scan_database", _scan)
    monkeypatch.setattr(Database, "connect", _fake_connect)
    monkeypatch.setattr(Database, "disconnect", _fake_disconnect)

    rc = mod.main(["--database-url", "sqlite+aiosqlite:///:memory:"])
    assert rc == 1
    captured = capsys.readouterr()
    assert "bad-row" in captured.out
    assert "bad value shape" in captured.out
