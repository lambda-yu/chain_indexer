from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI, Request


@dataclass
class AnvilHandle:
    """Lightweight handle exposed by the `anvil` fixture."""

    rpc_url: str
    chain_id: int = 31337
    # Anvil's default deterministic accounts (mnemonic = test test ... junk).
    # The first three are exposed here; tests only need source/sink pairs.
    accounts: list[str] = field(
        default_factory=lambda: [
            "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
            "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
            "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
        ]
    )
    private_keys: list[str] = field(
        default_factory=lambda: [
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
            "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a",
        ]
    )


@dataclass
class WebhookHandle:
    """Lightweight handle exposed by the `webhook_receiver` fixture."""

    url: str
    received: list[dict[str, Any]] = field(default_factory=list)


def _free_port() -> int:
    """Reserve an ephemeral port. Race-free enough for E2E given we bind
    immediately after release."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = int(s.getsockname()[1])
    s.close()
    return port


def _anvil_available() -> bool:
    return shutil.which("anvil") is not None


@pytest_asyncio.fixture
async def anvil() -> AsyncIterator[AnvilHandle]:
    """Start `anvil` as a subprocess on an ephemeral port. Skip the test if
    Foundry isn't installed."""
    if not _anvil_available():
        pytest.skip(
            "anvil (Foundry) not installed; install via "
            "`curl -L https://foundry.paradigm.xyz | bash && foundryup`"
        )

    port = _free_port()
    proc = subprocess.Popen(  # noqa: ASYNC220 — anvil is a long-running subprocess we keep around
        [
            "anvil",
            "--host", "127.0.0.1",
            "--port", str(port),
            "--chain-id", "31337",
            "--block-time", "1",  # mine a block every second to keep the test snappy
            "--silent",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    rpc_url = f"http://127.0.0.1:{port}"
    # Wait up to 5 s for the JSON-RPC port to answer.
    deadline = time.time() + 5.0
    async with httpx.AsyncClient(timeout=0.5) as client:
        while time.time() < deadline:
            try:
                r = await client.post(
                    rpc_url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_chainId",
                        "params": [],
                    },
                )
                if r.status_code == 200 and r.json().get("result"):
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        else:
            proc.terminate()
            pytest.fail("anvil did not become reachable within 5s")

    try:
        yield AnvilHandle(rpc_url=rpc_url)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest_asyncio.fixture
async def webhook_receiver() -> AsyncIterator[WebhookHandle]:
    """In-process FastAPI app that captures POST bodies."""
    handle = WebhookHandle(url="")  # filled after we know the port

    app = FastAPI()

    @app.post("/hook")
    async def hook(request: Request) -> dict[str, Any]:
        payload = await request.json()
        handle.received.append(payload)
        return {"ok": True}

    port = _free_port()
    handle.url = f"http://127.0.0.1:{port}/hook"

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        lifespan="off",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())

    # Wait for uvicorn to flip its `started` flag.
    deadline = time.time() + 3.0
    while not server.started and time.time() < deadline:  # noqa: ASYNC110 — polling a uvicorn flag
        await asyncio.sleep(0.05)
    if not server.started:
        server.should_exit = True
        await serve_task
        pytest.fail("uvicorn webhook receiver did not start within 3s")

    try:
        yield handle
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(serve_task, timeout=3.0)
        except TimeoutError:
            serve_task.cancel()
