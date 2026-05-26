from __future__ import annotations

from core.chains.types import SolanaBlock, SolanaInstruction, SolanaTransaction
from core.parser.sol_native import SOLANA_SYSTEM_PROGRAM_ID, SolNativeTransferParser


def _tx(
    *,
    sig: str = "5abc",
    success: bool = True,
    fee: int = 5000,
    accounts: list[str] | None = None,
    pre: list[int] | None = None,
    post: list[int] | None = None,
    instructions: list[SolanaInstruction] | None = None,
) -> SolanaTransaction:
    accts = accounts or ["A", "B"]
    return SolanaTransaction(
        signature=sig,
        slot=100,
        success=success,
        fee=fee,
        account_keys=accts,
        pre_balances=pre or [10**9, 0],
        post_balances=post or [10**9 - 100_000 - fee, 100_000],
        pre_token_balances=[],
        post_token_balances=[],
        log_messages=[],
        instructions=instructions or [
            SolanaInstruction(
                program_id=SOLANA_SYSTEM_PROGRAM_ID,
                accounts=["A", "B"],
                data_b58="3Bxs4h",
                stack_depth=1,
            )
        ],
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(
        slot=100,
        block_hash="hash100",
        parent_slot=99,
        block_time=1_700_000_000,
        transactions=txs,
    )


def test_extracts_native_transfer_from_system_program() -> None:
    p = SolNativeTransferParser(chain_id="sol-devnet")
    events = list(p.parse(_block([_tx()])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "native_transfer"
    assert e.chain_id == "sol-devnet"
    assert e.block_number == 100
    assert e.args["from"] == "A"
    assert e.args["to"] == "B"
    assert int(e.args["value"]) > 0


def test_skips_failed_transactions() -> None:
    p = SolNativeTransferParser(chain_id="sol-devnet")
    events = list(p.parse(_block([_tx(success=False)])))
    assert events == []


def test_skips_non_system_program_instructions() -> None:
    p = SolNativeTransferParser(chain_id="sol-devnet")
    tx = _tx(instructions=[
        SolanaInstruction(
            program_id="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            accounts=["A", "B"],
            data_b58="3Bxs4h",
            stack_depth=1,
        )
    ])
    events = list(p.parse(_block([tx])))
    assert events == []


def test_skips_inner_cpi_instructions() -> None:
    p = SolNativeTransferParser(chain_id="sol-devnet")
    tx = _tx(instructions=[
        SolanaInstruction(
            program_id=SOLANA_SYSTEM_PROGRAM_ID,
            accounts=["A", "B"],
            data_b58="3Bxs4h",
            stack_depth=2,
        )
    ])
    events = list(p.parse(_block([tx])))
    assert events == []
