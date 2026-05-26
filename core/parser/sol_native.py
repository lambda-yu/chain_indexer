from __future__ import annotations

import struct
from collections.abc import Iterable

import base58

from core.chains.types import SolanaBlock, SolanaInstruction, SolanaTransaction
from core.parser.event import Event

SOLANA_SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
_TRANSFER_DISC = b"\x02\x00\x00\x00"  # u32 LE = 2
_TRANSFER_DATA_LEN = 12  # 4 (disc) + 8 (u64 LE lamports)


class SolNativeTransferParser:

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            yield from self._extract_transfers(tx, block)

    def _extract_transfers(
        self, tx: SolanaTransaction, block: SolanaBlock,
    ) -> Iterable[Event]:
        for ix in tx.instructions:
            if ix.program_id != SOLANA_SYSTEM_PROGRAM_ID:
                continue
            if ix.stack_depth != 1:
                continue
            lamports = self._decode_transfer(ix)
            if lamports is None:
                continue
            if len(ix.accounts) < 2:
                continue
            yield Event(
                chain_id=self._chain_id,
                block_number=block.slot,
                block_hash=block.block_hash,
                block_timestamp=block.block_time or 0,
                tx_hash=tx.signature,
                tx_index=None,
                log_index=None,
                kind="native_transfer",
                contract=None,
                name=None,
                args={
                    "from": ix.accounts[0],
                    "to": ix.accounts[1],
                    "value": str(lamports),
                },
                raw={"signature": tx.signature},
            )

    @staticmethod
    def _decode_transfer(ix: SolanaInstruction) -> int | None:
        try:
            data = base58.b58decode(ix.data_b58)
        except Exception:  # noqa: BLE001
            return None
        if len(data) != _TRANSFER_DATA_LEN:
            return None
        if data[:4] != _TRANSFER_DISC:
            return None
        result: int = struct.unpack("<Q", data[4:])[0]
        return result
