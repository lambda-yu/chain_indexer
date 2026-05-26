from __future__ import annotations

import hashlib
import struct

import base58

from core.abi.registry import AbiRegistry
from core.chains.types import SolanaBlock, SolanaInstruction, SolanaTransaction
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.anchor_call import AnchorIdlCallParser

PROGRAM_ID = "MyCallProg1111111111111111111111111111111"

_IDL = {
    "version": "0.1.0",
    "name": "test_call",
    "metadata": {"address": PROGRAM_ID},
    "instructions": [
        {
            "name": "initialize",
            "args": [
                {"name": "amount", "type": "u64"},
                {"name": "flag", "type": "bool"},
            ],
        }
    ],
    "events": [],
}


def _call_disc(fn_name: str) -> bytes:
    return hashlib.sha256(f"global:{fn_name}".encode()).digest()[:8]


def _encode_call(fn_name: str, amount: int, flag: bool) -> str:
    disc = _call_disc(fn_name)
    body = struct.pack("<Q", amount) + (b"\x01" if flag else b"\x00")
    return base58.b58encode(disc + body).decode()


def _registry() -> AbiRegistry:
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="idl1", name="test", kind="solana_idl", body=_IDL)],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _ix(program_id: str = PROGRAM_ID, data_b58: str = "", stack_depth: int = 1) -> SolanaInstruction:
    return SolanaInstruction(program_id=program_id, accounts=["A", "B"], data_b58=data_b58, stack_depth=stack_depth)


def _tx(instructions: list[SolanaInstruction]) -> SolanaTransaction:
    return SolanaTransaction(
        signature="SIG", slot=300, success=True, fee=5000,
        account_keys=[PROGRAM_ID, "A", "B"], pre_balances=[0, 0, 0], post_balances=[0, 0, 0],
        pre_token_balances=[], post_token_balances=[], log_messages=[], instructions=instructions,
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(slot=300, block_hash="H300", parent_slot=299, block_time=1_700_000_000, transactions=txs)


def test_decodes_anchor_call_instruction() -> None:
    reg = _registry()
    p = AnchorIdlCallParser(chain_id="sol", registry=reg)
    data = _encode_call("initialize", amount=42000, flag=True)
    events = list(p.parse(_block([_tx([_ix(data_b58=data)])])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "call"
    assert e.name == "initialize"
    assert e.contract == PROGRAM_ID
    assert e.args["amount"] == 42000
    assert e.args["flag"] is True


def test_skips_unknown_program() -> None:
    reg = _registry()
    p = AnchorIdlCallParser(chain_id="sol", registry=reg)
    data = _encode_call("initialize", amount=1, flag=False)
    events = list(p.parse(_block([_tx([_ix(program_id="UnknownProg111111111111111111111111111111", data_b58=data)])])))
    assert events == []


def test_skips_unknown_discriminator() -> None:
    reg = _registry()
    p = AnchorIdlCallParser(chain_id="sol", registry=reg)
    fake = base58.b58encode(b"\xff" * 8 + b"\x00" * 9).decode()
    events = list(p.parse(_block([_tx([_ix(data_b58=fake)])])))
    assert events == []


def test_skips_failed_tx() -> None:
    reg = _registry()
    p = AnchorIdlCallParser(chain_id="sol", registry=reg)
    data = _encode_call("initialize", amount=1, flag=False)
    tx = SolanaTransaction(
        signature="SIG", slot=300, success=False, fee=5000,
        account_keys=[], pre_balances=[], post_balances=[],
        pre_token_balances=[], post_token_balances=[], log_messages=[],
        instructions=[_ix(data_b58=data)],
    )
    assert list(p.parse(_block([tx]))) == []


def test_skips_inner_cpi() -> None:
    reg = _registry()
    p = AnchorIdlCallParser(chain_id="sol", registry=reg)
    data = _encode_call("initialize", amount=1, flag=False)
    events = list(p.parse(_block([_tx([_ix(data_b58=data, stack_depth=2)])])))
    assert events == []
