from __future__ import annotations

import struct

import base58

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTransaction,
)
from core.parser.sol_native import (
    SOLANA_SYSTEM_PROGRAM_ID,
    SolNativeTransferParser,
)


def _transfer_data(lamports: int) -> str:
    payload = b"\x02\x00\x00\x00" + struct.pack("<Q", lamports)
    return base58.b58encode(payload).decode()


def _ix(
    program_id: str = SOLANA_SYSTEM_PROGRAM_ID,
    accounts: list[str] | None = None,
    data_b58: str = "",
    stack_depth: int = 1,
) -> SolanaInstruction:
    return SolanaInstruction(
        program_id=program_id,
        accounts=accounts or ["FROM", "TO"],
        data_b58=data_b58,
        stack_depth=stack_depth,
    )


def _tx(
    signature: str = "SIG",
    success: bool = True,
    instructions: list[SolanaInstruction] | None = None,
) -> SolanaTransaction:
    return SolanaTransaction(
        signature=signature,
        slot=100,
        success=success,
        fee=5000,
        account_keys=["FROM", "TO"],
        pre_balances=[10**9, 0],
        post_balances=[10**9 - 5000 - 1_000, 1_000],
        pre_token_balances=[],
        post_token_balances=[],
        log_messages=[],
        instructions=instructions or [],
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(
        slot=100, block_hash="H100", parent_slot=99,
        block_time=1_700_000_000, transactions=txs,
    )


def test_emits_native_transfer_for_system_program_transfer() -> None:
    ix = _ix(data_b58=_transfer_data(1_000))
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol-mainnet")
    [event] = list(p.parse(block))
    assert event.chain_id == "sol-mainnet"
    assert event.kind == "native_transfer"
    assert event.contract is None
    assert event.args == {"from": "FROM", "to": "TO", "value": "1000"}
    assert event.block_number == 100
    assert event.block_hash == "H100"
    assert event.block_timestamp == 1_700_000_000
    assert event.tx_hash == "SIG"


def test_ignores_non_transfer_system_ops() -> None:
    create_acct_data = base58.b58encode(b"\x00\x00\x00\x00" + b"\x00" * 8).decode()
    ix = _ix(data_b58=create_acct_data)
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_ignores_non_system_programs() -> None:
    ix = _ix(program_id="SomeOtherProgramId1111111111111", data_b58=_transfer_data(500))
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_ignores_inner_cpi_transfers() -> None:
    ix = _ix(data_b58=_transfer_data(1_000), stack_depth=2)
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_skips_failed_transactions() -> None:
    ix = _ix(data_b58=_transfer_data(1_000))
    block = _block([_tx(success=False, instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_emits_multiple_when_one_tx_has_multiple_transfers() -> None:
    ix1 = _ix(data_b58=_transfer_data(100))
    ix2 = _ix(data_b58=_transfer_data(200))
    block = _block([_tx(instructions=[ix1, ix2])])
    p = SolNativeTransferParser(chain_id="sol")
    events = list(p.parse(block))
    assert [e.args["value"] for e in events] == ["100", "200"]


def test_ignores_malformed_transfer_payload() -> None:
    short = base58.b58encode(b"\x02\x00\x00\x00").decode()
    block = _block([_tx(instructions=[_ix(data_b58=short)])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []
