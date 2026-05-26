from __future__ import annotations

import asyncio

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams
from solders.system_program import transfer as system_transfer
from solders.transaction import Transaction

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import run_worker
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]

DELIVERY_TIMEOUT_S = 60.0
TRANSFER_LAMPORTS = 500_000_000


async def _airdrop(rpc_url: str, pubkey: Pubkey, lamports: int = 2_000_000_000) -> None:
    body = {
        "jsonrpc": "2.0", "id": 1,
        "method": "requestAirdrop",
        "params": [str(pubkey), lamports],
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(rpc_url, json=body, timeout=10.0)
        sig = r.json()["result"]
    await _wait_sig(rpc_url, sig)


async def _wait_sig(rpc_url: str, sig: str, *, wait_timeout: float = 30.0) -> None:  # noqa: ASYNC109
    deadline = asyncio.get_event_loop().time() + wait_timeout
    async with httpx.AsyncClient() as c:
        while asyncio.get_event_loop().time() < deadline:
            body = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getSignatureStatuses",
                "params": [[sig], {"searchTransactionHistory": True}],
            }
            r = await c.post(rpc_url, json=body, timeout=5.0)
            statuses = r.json().get("result", {}).get("value", [])
            if statuses and statuses[0] is not None:
                return
            await asyncio.sleep(0.5)
    raise TimeoutError(f"signature {sig} not confirmed in {wait_timeout}s")


async def _send_native_transfer(
    rpc_url: str, *, sender: Keypair, to: Pubkey, lamports: int,
) -> str:
    async with httpx.AsyncClient() as c:
        bh_resp = await c.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "getLatestBlockhash",
            "params": [{"commitment": "confirmed"}],
        }, timeout=5.0)
        bh_hex = bh_resp.json()["result"]["value"]["blockhash"]
        blockhash = Hash.from_string(bh_hex)

    ix = system_transfer(TransferParams(
        from_pubkey=sender.pubkey(),
        to_pubkey=to,
        lamports=lamports,
    ))
    msg = Message.new_with_blockhash([ix], sender.pubkey(), blockhash)
    tx = Transaction.new_unsigned(msg)
    tx.sign([sender], blockhash)

    raw = bytes(tx.serialize())
    import base64
    encoded = base64.b64encode(raw).decode()

    async with httpx.AsyncClient() as c:
        r = await c.post(rpc_url, json={
            "jsonrpc": "2.0", "id": 1,
            "method": "sendTransaction",
            "params": [encoded, {"encoding": "base64"}],
        }, timeout=10.0)
        result = r.json()
        if "error" in result:
            raise RuntimeError(f"sendTransaction error: {result['error']}")
        return result["result"]


async def test_solana_native_transfer_to_webhook(
    solana_validator, webhook_receiver, redis_url, tmp_path,
) -> None:
    rpc_url = solana_validator.rpc_url
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'sol_e2e.sqlite'}"

    db = Database(db_url)
    await db.connect()
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    sender = Keypair()
    recipient = Keypair()
    await _airdrop(rpc_url, sender.pubkey())

    settings = Settings(database={"url": db_url}, redis={"url": redis_url})

    bus_writer = RedisBus(url=redis_url)
    await bus_writer.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post("/api/chains", json={
                "id": "sol-local", "kind": "solana",
                "rpc_http": rpc_url, "rpc_ws": None,
                "confirmations": 0, "commitment": "confirmed",
                "poll_interval_ms": 400, "enabled": True,
            })
            assert r.status_code == 201, r.text

            r = await c.post("/api/channels", json={
                "name": "sol-hook", "type": "http",
                "config": {"url": webhook_receiver.url, "method": "POST"},
            })
            assert r.status_code == 201
            channel_id = r.json()["id"]

            r = await c.post("/api/subscriptions", json={
                "name": "sol-native",
                "chain_id": "sol-local",
                "address": None, "abi_id": None,
                "match_kind": "native_transfer",
                "match_name": None, "arg_filters": {},
                "enabled": True,
            })
            assert r.status_code == 201
            sub_id = r.json()["id"]

            r = await c.post(f"/api/subscriptions/{sub_id}/channels",
                             json={"channel_id": channel_id})
            assert r.status_code == 204
    finally:
        await bus_writer.disconnect()

    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(settings, stop_event))
    await asyncio.sleep(2.0)

    sig = await _send_native_transfer(
        rpc_url, sender=sender, to=recipient.pubkey(), lamports=TRANSFER_LAMPORTS,
    )
    await _wait_sig(rpc_url, sig)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + DELIVERY_TIMEOUT_S
    while len(webhook_receiver.received) < 1:
        if loop.time() > deadline:
            break
        await asyncio.sleep(0.5)

    stop_event.set()
    try:
        await asyncio.wait_for(worker_task, timeout=10.0)
    except TimeoutError:
        worker_task.cancel()
        import contextlib
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(worker_task, timeout=3.0)

    await db.disconnect()

    assert len(webhook_receiver.received) >= 1, (
        f"expected >=1 native_transfer payload, got {len(webhook_receiver.received)}"
    )
    ev = webhook_receiver.received[0]["event"]
    assert ev["kind"] == "native_transfer"
    assert "from" in ev["args"]
    assert "to" in ev["args"]
    assert "value" in ev["args"]
    assert int(ev["args"]["value"]) > 0
