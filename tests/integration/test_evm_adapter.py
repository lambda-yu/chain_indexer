import socket

import pytest

from core.chains.evm import EvmAdapter

pytestmark = pytest.mark.integration


def _anvil_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture
def anvil_rpc():
    """Requires `anvil --port 8545 --silent` running (locally) or CI service.

    If port 8545 is not reachable, skip — gives a clear message instead of a
    connection-refused traceback on a fresh checkout.
    """
    if not _anvil_reachable("127.0.0.1", 8545):
        pytest.skip("anvil not running on 127.0.0.1:8545; start with `anvil --port 8545 --silent`")
    return "http://127.0.0.1:8545"


@pytest.mark.asyncio
async def test_get_latest_and_fetch_block(anvil_rpc) -> None:
    a = EvmAdapter(chain_id="anvil", rpc_http=anvil_rpc, rpc_ws=None, confirmations=0)
    await a.connect()
    try:
        n = await a.get_latest_block_number()
        assert n >= 0
        blk = await a.fetch_block(n)
        assert blk.header.number == n
        assert blk.header.hash.startswith("0x")
        # fetch_block does NOT embed logs (listener fetches them separately).
        assert blk.logs == []
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_fetch_logs_empty_range_returns_empty(anvil_rpc) -> None:
    a = EvmAdapter(chain_id="anvil", rpc_http=anvil_rpc, rpc_ws=None, confirmations=0)
    await a.connect()
    try:
        latest = await a.get_latest_block_number()
        logs = await a.fetch_logs(latest, latest)
        assert isinstance(logs, list)
    finally:
        await a.disconnect()
