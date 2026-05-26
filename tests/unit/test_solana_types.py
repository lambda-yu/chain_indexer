from __future__ import annotations

import pytest

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)


def test_solana_token_balance_round_trip() -> None:
    tb = SolanaTokenBalance(
        account_index=2,
        mint="So11111111111111111111111111111111111111112",
        owner="11111111111111111111111111111111",
        amount=1_000_000,
        decimals=6,
    )
    assert tb.mint.endswith("112")
    assert tb.amount == 1_000_000


def test_solana_instruction_carries_program_and_accounts() -> None:
    ix = SolanaInstruction(
        program_id="11111111111111111111111111111111",
        accounts=["A", "B"],
        data_b58="3Bxs4h",
        stack_depth=1,
    )
    assert ix.program_id == "11111111111111111111111111111111"
    assert ix.accounts == ["A", "B"]


def test_solana_transaction_holds_balances_and_logs() -> None:
    tx = SolanaTransaction(
        signature="5xyz",
        slot=100,
        success=True,
        fee=5000,
        account_keys=["A", "B"],
        pre_balances=[10**9, 0],
        post_balances=[10**9 - 5000, 0],
        pre_token_balances=[],
        post_token_balances=[],
        log_messages=["Program X invoke [1]"],
        instructions=[],
    )
    assert tx.success is True
    assert tx.fee == 5000
    assert tx.log_messages[0].startswith("Program X")


def test_solana_block_top_level_shape() -> None:
    block = SolanaBlock(
        slot=100,
        block_hash="hash100",
        parent_slot=99,
        block_time=1_700_000_000,
        transactions=[],
    )
    assert block.slot == 100
    assert block.parent_slot == 99


def test_solana_block_is_frozen() -> None:
    block = SolanaBlock(slot=1, block_hash="h", parent_slot=0, block_time=None, transactions=[])
    with pytest.raises((AttributeError, TypeError)):
        block.slot = 2  # type: ignore[misc]
