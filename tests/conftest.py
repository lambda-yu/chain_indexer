from __future__ import annotations

import contextlib
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]


@pytest_asyncio.fixture(scope="function")
async def redis_url() -> AsyncIterator[str]:
    with RedisContainer("redis:7-alpine") as rc:
        host = rc.get_container_host_ip()
        port = rc.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@dataclass
class SolanaValidatorHandle:
    rpc_url: str
    process: subprocess.Popen  # type: ignore[type-arg]
    ledger_path: Path


def _free_tcp_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_rpc(url: str, *, timeout_s: float = 25.0) -> None:
    deadline = time.monotonic() + timeout_s
    body = b'{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
    while time.monotonic() < deadline:
        try:
            r = httpx.post(url, content=body, headers={"content-type": "application/json"}, timeout=2.0)
            if r.status_code == 200 and r.json().get("result") == "ok":
                return
        except (httpx.RequestError, ValueError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"solana-test-validator did not become healthy at {url}")


@pytest.fixture(scope="session")
def solana_validator(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SolanaValidatorHandle]:
    if shutil.which("solana-test-validator") is None:
        pytest.skip("solana-test-validator not installed")

    rpc_port = _free_tcp_port()
    faucet_port = _free_tcp_port()
    ledger = tmp_path_factory.mktemp("solana-ledger")
    proc = subprocess.Popen(
        [
            "solana-test-validator",
            "--reset",
            "--quiet",
            "--ledger", str(ledger),
            "--rpc-port", str(rpc_port),
            "--faucet-port", str(faucet_port),
            "--bind-address", "127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    rpc_url = f"http://127.0.0.1:{rpc_port}"
    try:
        _wait_for_rpc(rpc_url)
        yield SolanaValidatorHandle(rpc_url=rpc_url, process=proc, ledger_path=ledger)
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5.0)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)
