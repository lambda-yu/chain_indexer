from __future__ import annotations

import base64
import hashlib
import struct

from core.abi.registry import AbiRegistry
from core.chains.types import SolanaBlock, SolanaTransaction
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.anchor_event import AnchorIdlEventParser

PROGRAM_ID = "MyProgram111111111111111111111111111111111"

_IDL = {
    "version": "0.1.0",
    "name": "test_program",
    "metadata": {"address": PROGRAM_ID},
    "events": [
        {
            "name": "PriceUpdate",
            "fields": [
                {"name": "price", "type": "u64"},
                {"name": "slot", "type": "u64"},
            ],
        }
    ],
}


def _disc(event_name: str) -> bytes:
    return hashlib.sha256(f"event:{event_name}".encode()).digest()[:8]


def _encode_event(event_name: str, price: int, slot: int) -> str:
    disc = _disc(event_name)
    body = struct.pack("<QQ", price, slot)
    return base64.b64encode(disc + body).decode()


def _registry() -> AbiRegistry:
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="idl1", name="test", kind="solana_idl", body=_IDL)],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _tx(log_messages: list[str]) -> SolanaTransaction:
    return SolanaTransaction(
        signature="SIG1", slot=200, success=True, fee=5000,
        account_keys=[PROGRAM_ID], pre_balances=[0], post_balances=[0],
        pre_token_balances=[], post_token_balances=[],
        log_messages=log_messages, instructions=[],
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(slot=200, block_hash="H200", parent_slot=199, block_time=1_700_000_000, transactions=txs)


def test_decodes_anchor_event_from_program_data_log() -> None:
    reg = _registry()
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)
    b64 = _encode_event("PriceUpdate", price=42000, slot=200)
    logs = [
        f"Program {PROGRAM_ID} invoke [1]",
        f"Program data: {b64}",
        f"Program {PROGRAM_ID} success",
    ]
    events = list(p.parse(_block([_tx(logs)])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "event"
    assert e.name == "PriceUpdate"
    assert e.contract == PROGRAM_ID
    assert e.args["price"] == 42000
    assert e.args["slot"] == 200


def test_skips_unknown_program_id() -> None:
    reg = _registry()
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)
    b64 = _encode_event("PriceUpdate", price=1, slot=1)
    logs = [
        "Program UnknownProgram1111111111111111111111111 invoke [1]",
        f"Program data: {b64}",
        "Program UnknownProgram1111111111111111111111111 success",
    ]
    events = list(p.parse(_block([_tx(logs)])))
    assert events == []


def test_skips_unknown_discriminator() -> None:
    reg = _registry()
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)
    fake = base64.b64encode(b"\xff" * 8 + b"\x00" * 16).decode()
    logs = [
        f"Program {PROGRAM_ID} invoke [1]",
        f"Program data: {fake}",
        f"Program {PROGRAM_ID} success",
    ]
    events = list(p.parse(_block([_tx(logs)])))
    assert events == []


def test_skips_failed_transactions() -> None:
    reg = _registry()
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)
    b64 = _encode_event("PriceUpdate", price=1, slot=1)
    tx = SolanaTransaction(
        signature="SIG", slot=200, success=False, fee=5000,
        account_keys=[PROGRAM_ID], pre_balances=[0], post_balances=[0],
        pre_token_balances=[], post_token_balances=[],
        log_messages=[
            f"Program {PROGRAM_ID} invoke [1]",
            f"Program data: {b64}",
            f"Program {PROGRAM_ID} success",
        ],
        instructions=[],
    )
    events = list(p.parse(_block([tx])))
    assert events == []
