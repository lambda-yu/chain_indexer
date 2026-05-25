from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio
from eth_account import Account
from httpx import ASGITransport, AsyncClient
from web3 import AsyncHTTPProvider, AsyncWeb3

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import run_worker
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

if TYPE_CHECKING:
    from tests.e2e.conftest import AnvilHandle, WebhookHandle

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


# How many transfers to send and how long to wait for the worker to deliver.
TRANSFER_COUNT = 3
DELIVERY_TIMEOUT_S = 30.0


@pytest_asyncio.fixture
async def db_url(tmp_path: Path) -> str:
    """File-backed SQLite so the worker and API share the same DB."""
    return f"sqlite+aiosqlite:///{tmp_path / 'e2e.sqlite'}"


@pytest_asyncio.fixture
async def initialised_db(db_url: str) -> AsyncIterator[Database]:
    d = Database(db_url)
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def _send_native_transfer(
    w3: AsyncWeb3[Any], *, sender_pk: str, to: str, value_wei: int
) -> str:
    sender = Account.from_key(sender_pk).address
    # Use "pending" so we get a fresh nonce when submitting back-to-back txs
    # within a single block window (Anvil's --block-time is 1s).
    nonce = await w3.eth.get_transaction_count(sender, "pending")
    tx: dict[str, Any] = {
        "from": sender,
        "to": to,
        "value": value_wei,
        "gas": 21_000,
        "gasPrice": await w3.eth.gas_price,
        "nonce": nonce,
        "chainId": 31337,
    }
    signed = Account.sign_transaction(tx, sender_pk)
    h = await w3.eth.send_raw_transaction(signed.raw_transaction)
    return h.hex()


async def test_native_transfer_anvil_to_webhook(
    anvil: AnvilHandle,
    webhook_receiver: WebhookHandle,
    initialised_db: Database,
    db_url: str,
    redis_url: str,
) -> None:
    """Anvil -> Worker -> HttpChannel -> in-process webhook receiver.

    Asserts payload conforms to spec section 8 (kind=native_transfer, contains
    chain_id, tx_hash, block_hash, args.from/to/value, subscription_name).
    """
    # `Settings` uses nested DatabaseSettings/RedisSettings -- pass dicts, not
    # flat kwargs. Pydantic-settings parses these into the nested BaseModels.
    settings = Settings(
        database={"url": db_url},
        redis={"url": redis_url},
    )

    # 1) Seed chain + channel + subscription via the real API stack.
    bus_writer = RedisBus(url=redis_url)
    await bus_writer.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: initialised_db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test",
        ) as c:
            r = await c.post(
                "/api/chains",
                json={
                    "id": "anvil-local",
                    "kind": "evm",
                    "rpc_http": anvil.rpc_url,
                    "rpc_ws": None,
                    "confirmations": 1,
                    "poll_interval_ms": 500,
                    "enabled": True,
                },
            )
            assert r.status_code == 201, r.text

            r = await c.post(
                "/api/channels",
                json={
                    "name": "e2e-hook",
                    "type": "http",
                    "config": {"url": webhook_receiver.url, "method": "POST"},
                },
            )
            assert r.status_code == 201
            channel_id = r.json()["id"]

            r = await c.post(
                "/api/subscriptions",
                json={
                    "name": "all-native-on-anvil",
                    "chain_id": "anvil-local",
                    "address": None,
                    "abi_id": None,
                    "match_kind": "native_transfer",
                    "match_name": None,
                    "arg_filters": {},
                    "enabled": True,
                },
            )
            assert r.status_code == 201
            sub_id = r.json()["id"]

            r = await c.post(
                f"/api/subscriptions/{sub_id}/channels",
                json={"channel_id": channel_id},
            )
            assert r.status_code == 204
    finally:
        await bus_writer.disconnect()

    # 2) Start the worker in-process. It will load the seeded config on boot
    # via the Redis `config_changed` pub/sub path (publishes happened above).
    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(settings, stop_event))

    # Give the worker a moment to load config and connect to Anvil.
    await asyncio.sleep(1.0)

    # 3) Submit N native transfers on Anvil.
    w3 = AsyncWeb3(AsyncHTTPProvider(anvil.rpc_url))
    submitted_hashes: list[str] = []
    timed_out = False
    try:
        sender_pk = anvil.private_keys[0]
        recipient = anvil.accounts[1]
        for i in range(TRANSFER_COUNT):
            h = await _send_native_transfer(
                w3,
                sender_pk=sender_pk,
                to=recipient,
                value_wei=10**16 * (i + 1),  # 0.01, 0.02, 0.03 ETH
            )
            submitted_hashes.append(h.lower().removeprefix("0x"))

        # 4) Wait for the receiver to collect TRANSFER_COUNT payloads.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DELIVERY_TIMEOUT_S
        while len(webhook_receiver.received) < TRANSFER_COUNT:
            if loop.time() > deadline:
                timed_out = True
                break
            await asyncio.sleep(0.5)
    finally:
        await w3.provider.disconnect()

    # 5) Stop the worker before assertions so failures don't dangle a task.
    stop_event.set()
    try:
        await asyncio.wait_for(worker_task, timeout=10.0)
    except TimeoutError:
        worker_task.cancel()
        # `run_worker`'s finally already swallows CancelledError internally, so
        # the awaited task may return normally instead of re-raising. Suppress
        # both possibilities so timeout-recovery never masks the real failure.
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(worker_task, timeout=3.0)

    if timed_out:
        pytest.fail(
            f"only {len(webhook_receiver.received)}/{TRANSFER_COUNT} "
            f"payloads received within {DELIVERY_TIMEOUT_S}s"
        )

    # 6) Assert payload shape per spec section 8.
    received_tx_hashes = {
        p["event"]["tx_hash"].lower().removeprefix("0x")
        for p in webhook_receiver.received
    }
    for submitted in submitted_hashes:
        assert submitted in received_tx_hashes, (
            f"missing tx {submitted} in {received_tx_hashes}"
        )

    sample = webhook_receiver.received[0]
    assert sample["subscription_id"] == sub_id
    assert sample["subscription_name"] == "all-native-on-anvil"
    assert sample["chain_id"] == "anvil-local"
    assert "delivery_id" in sample
    assert "delivered_at" in sample

    ev = sample["event"]
    assert ev["kind"] == "native_transfer"
    assert isinstance(ev["block_number"], int) and ev["block_number"] >= 1
    assert ev["block_hash"].startswith("0x")
    assert ev["tx_hash"].startswith("0x")
    assert "from" in ev["args"] and "to" in ev["args"] and "value" in ev["args"]
    # value is a decimal string (spec section 7.3 arg_filters big-int convention).
    assert isinstance(ev["args"]["value"], str)
    assert int(ev["args"]["value"]) > 0
