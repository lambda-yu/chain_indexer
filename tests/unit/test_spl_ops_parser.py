from __future__ import annotations

import struct

import base58

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)
from core.parser.spl_ops import SplOpsParser
from core.parser.spl_transfer import SPL_TOKEN_PROGRAM_ID

MINT = "TokenMint111111111111111111111111111111111"


def _encode(disc: int, amount: int | None = None) -> str:
    payload = bytes([disc])
    if amount is not None:
        payload += struct.pack("<Q", amount)
    return base58.b58encode(payload).decode()


def _tx(instructions: list[SolanaInstruction]) -> SolanaTransaction:
    return SolanaTransaction(
        signature="SIG", slot=100, success=True, fee=5000,
        account_keys=["SOURCE", MINT, "DEST", "DELEGATE", SPL_TOKEN_PROGRAM_ID, "OWNER"],
        pre_balances=[0] * 6, post_balances=[0] * 6,
        pre_token_balances=[],
        post_token_balances=[
            SolanaTokenBalance(account_index=0, mint=MINT, owner="OWNER", amount=900, decimals=6),
        ],
        log_messages=[], instructions=instructions,
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(slot=100, block_hash="H", parent_slot=99, block_time=1_700_000_000, transactions=txs)


def _ix(disc: int, amount: int | None = None, accounts: list[str] | None = None) -> SolanaInstruction:
    return SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=accounts or ["SOURCE", "DELEGATE", "OWNER"],
        data_b58=_encode(disc, amount),
        stack_depth=1,
    )


def test_approve() -> None:
    p = SplOpsParser(chain_id="sol")
    events = list(p.parse(_block([_tx([_ix(4, 1000, ["SOURCE", "DELEGATE", "OWNER"])])])))
    assert len(events) == 1
    assert events[0].name == "approve"
    assert events[0].kind == "call"
    assert events[0].args["amount"] == "1000"
    assert events[0].args["delegate"] == "DELEGATE"
    assert events[0].contract == MINT


def test_revoke() -> None:
    p = SplOpsParser(chain_id="sol")
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", "OWNER"],
        data_b58=base58.b58encode(bytes([5])).decode(),
        stack_depth=1,
    )
    events = list(p.parse(_block([_tx([ix])])))
    assert len(events) == 1
    assert events[0].name == "revoke"
    assert events[0].args["source"] == "SOURCE"


def test_mint_to() -> None:
    p = SplOpsParser(chain_id="sol")
    events = list(p.parse(_block([_tx([_ix(7, 5000, [MINT, "DEST", "OWNER"])])])))
    assert len(events) == 1
    assert events[0].name == "mint_to"
    assert events[0].args["amount"] == "5000"
    assert events[0].contract == MINT


def test_burn() -> None:
    p = SplOpsParser(chain_id="sol")
    events = list(p.parse(_block([_tx([_ix(8, 2000, ["SOURCE", MINT, "OWNER"])])])))
    assert len(events) == 1
    assert events[0].name == "burn"
    assert events[0].args["amount"] == "2000"
    assert events[0].contract == MINT


def test_skips_failed_tx() -> None:
    p = SplOpsParser(chain_id="sol")
    tx = SolanaTransaction(
        signature="SIG", slot=100, success=False, fee=5000,
        account_keys=["SOURCE", MINT], pre_balances=[0, 0], post_balances=[0, 0],
        pre_token_balances=[], post_token_balances=[], log_messages=[],
        instructions=[_ix(4, 1000)],
    )
    assert list(p.parse(_block([tx]))) == []


def test_skips_unknown_disc() -> None:
    p = SplOpsParser(chain_id="sol")
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SOURCE", "DEST", "OWNER"],
        data_b58=base58.b58encode(bytes([99]) + b"\x00" * 8).decode(),
        stack_depth=1,
    )
    assert list(p.parse(_block([_tx([ix])]))) == []
