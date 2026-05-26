from __future__ import annotations

import struct

import base58

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)
from core.parser.spl_transfer import (
    SPL_TOKEN_PROGRAM_ID,
    SplTransferParser,
)


def _transfer_data(amount: int) -> str:
    payload = bytes([3]) + struct.pack("<Q", amount)
    return base58.b58encode(payload).decode()


def _transfer_checked_data(amount: int) -> str:
    payload = bytes([12]) + struct.pack("<Q", amount)
    return base58.b58encode(payload).decode()


MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


def _tx(
    instructions: list[SolanaInstruction],
    post_token_balances: list[SolanaTokenBalance] | None = None,
) -> SolanaTransaction:
    return SolanaTransaction(
        signature="SIG",
        slot=100,
        success=True,
        fee=5000,
        account_keys=["SOURCE", MINT, "DEST", SPL_TOKEN_PROGRAM_ID, "OWNER"],
        pre_balances=[10**9, 0, 0, 0, 0],
        post_balances=[10**9 - 5000, 0, 0, 0, 0],
        pre_token_balances=[],
        post_token_balances=post_token_balances or [
            SolanaTokenBalance(account_index=0, mint=MINT, owner="OWNER", amount=900, decimals=6),
        ],
        log_messages=[],
        instructions=instructions,
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(slot=100, block_hash="H100", parent_slot=99, block_time=1_700_000_000, transactions=txs)


def test_transfer_disc3_emits_token_transfer() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", "DEST", "OWNER"],
        data_b58=_transfer_data(1_000_000),
        stack_depth=1,
    )
    p = SplTransferParser(chain_id="sol")
    events = list(p.parse(_block([_tx([ix])])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "token_transfer"
    assert e.name == "Transfer"
    assert e.args["from"] == "SOURCE"
    assert e.args["to"] == "DEST"
    assert e.args["value"] == "1000000"
    assert e.args["mint"] == MINT
    assert e.contract == MINT


def test_transfer_checked_disc12_emits_token_transfer() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", MINT, "DEST", "OWNER"],
        data_b58=_transfer_checked_data(500_000),
        stack_depth=1,
    )
    p = SplTransferParser(chain_id="sol")
    events = list(p.parse(_block([_tx([ix])])))
    assert len(events) == 1
    assert events[0].args["value"] == "500000"
    assert events[0].args["mint"] == MINT


def test_skips_failed_transactions() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", "DEST", "OWNER"],
        data_b58=_transfer_data(100),
        stack_depth=1,
    )
    tx = SolanaTransaction(
        signature="SIG", slot=100, success=False, fee=5000,
        account_keys=["SOURCE", MINT, "DEST"], pre_balances=[], post_balances=[],
        pre_token_balances=[], post_token_balances=[], log_messages=[], instructions=[ix],
    )
    p = SplTransferParser(chain_id="sol")
    assert list(p.parse(_block([tx]))) == []


def test_skips_inner_cpi() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", "DEST", "OWNER"],
        data_b58=_transfer_data(100),
        stack_depth=2,
    )
    p = SplTransferParser(chain_id="sol")
    assert list(p.parse(_block([_tx([ix])]))) == []


def test_skips_non_token_program() -> None:
    ix = SolanaInstruction(
        program_id="11111111111111111111111111111111",
        accounts=["SOURCE", "DEST", "OWNER"],
        data_b58=_transfer_data(100),
        stack_depth=1,
    )
    p = SplTransferParser(chain_id="sol")
    assert list(p.parse(_block([_tx([ix])]))) == []


def _transfer_with_fee_data(amount: int, fee: int) -> str:
    payload = bytes([26, 1]) + struct.pack("<Q", amount) + bytes([6]) + struct.pack("<Q", fee)
    return base58.b58encode(payload).decode()


def test_transfer_checked_with_fee_disc_26_1() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", MINT, "DEST", "OWNER"],
        data_b58=_transfer_with_fee_data(500_000, 1_000),
        stack_depth=1,
    )
    p = SplTransferParser(chain_id="sol")
    events = list(p.parse(_block([_tx([ix])])))
    assert len(events) == 1
    assert events[0].args["value"] == "500000"
    assert events[0].args["fee"] == "1000"
    assert events[0].args["mint"] == MINT


def test_disc_26_sub_0_is_skipped() -> None:
    payload = bytes([26, 0]) + b"\x00" * 17
    data_b58 = base58.b58encode(payload).decode()
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", MINT, "DEST", "OWNER"],
        data_b58=data_b58,
        stack_depth=1,
    )
    p = SplTransferParser(chain_id="sol")
    assert list(p.parse(_block([_tx([ix])]))) == []


def test_transfer_with_fee_truncated_data_skipped() -> None:
    payload = bytes([26, 1]) + b"\x00" * 5
    data_b58 = base58.b58encode(payload).decode()
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", MINT, "DEST", "OWNER"],
        data_b58=data_b58,
        stack_depth=1,
    )
    p = SplTransferParser(chain_id="sol")
    assert list(p.parse(_block([_tx([ix])]))) == []
