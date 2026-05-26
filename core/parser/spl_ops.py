from __future__ import annotations

import struct
from collections.abc import Iterable

import base58

from core.chains.types import SolanaBlock, SolanaInstruction, SolanaTransaction
from core.parser.event import Event
from core.parser.spl_transfer import SPL_TOKEN_2022_PROGRAM_ID, SPL_TOKEN_PROGRAM_ID

_TOKEN_PROGRAMS = {SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID}

_APPROVE_DISC = 4
_REVOKE_DISC = 5
_MINT_TO_DISC = 7
_BURN_DISC = 8


class SplOpsParser:

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            yield from self._extract(tx, block)

    def _extract(self, tx: SolanaTransaction, block: SolanaBlock) -> Iterable[Event]:
        for ix in tx.instructions:
            if ix.program_id not in _TOKEN_PROGRAMS:
                continue
            if ix.stack_depth != 1:
                continue
            result = self._decode(ix, tx)
            if result is None:
                continue
            name, mint, args = result
            yield Event(
                chain_id=self._chain_id,
                block_number=block.slot,
                block_hash=block.block_hash,
                block_timestamp=block.block_time or 0,
                tx_hash=tx.signature,
                tx_index=None,
                log_index=None,
                kind="call",
                contract=mint,
                name=name,
                args=args,
                raw={"signature": tx.signature},
            )

    @staticmethod
    def _decode(
        ix: SolanaInstruction, tx: SolanaTransaction,
    ) -> tuple[str, str, dict[str, str]] | None:
        try:
            data = base58.b58decode(ix.data_b58)
        except Exception:  # noqa: BLE001
            return None
        if not data:
            return None

        disc = data[0]

        if disc == _APPROVE_DISC and len(data) >= 9:
            amount: int = struct.unpack("<Q", data[1:9])[0]
            if len(ix.accounts) < 3:
                return None
            source = ix.accounts[0]
            delegate = ix.accounts[1]
            mint = _resolve_mint(source, tx)
            if mint is None:
                return None
            return "approve", mint, {"source": source, "delegate": delegate, "amount": str(amount)}

        if disc == _REVOKE_DISC:
            if len(ix.accounts) < 2:
                return None
            source = ix.accounts[0]
            mint = _resolve_mint(source, tx)
            if mint is None:
                return None
            return "revoke", mint, {"source": source}

        if disc == _MINT_TO_DISC and len(data) >= 9:
            amount = struct.unpack("<Q", data[1:9])[0]
            if len(ix.accounts) < 3:
                return None
            mint = ix.accounts[0]
            dest = ix.accounts[1]
            return "mint_to", mint, {"mint": mint, "dest": dest, "amount": str(amount)}

        if disc == _BURN_DISC and len(data) >= 9:
            amount = struct.unpack("<Q", data[1:9])[0]
            if len(ix.accounts) < 3:
                return None
            source = ix.accounts[0]
            mint = ix.accounts[1]
            return "burn", mint, {"source": source, "mint": mint, "amount": str(amount)}

        return None


def _resolve_mint(account: str, tx: SolanaTransaction) -> str | None:
    for tb in tx.post_token_balances:
        idx = tb.account_index
        if idx < len(tx.account_keys) and tx.account_keys[idx] == account:
            return tb.mint
    return None
