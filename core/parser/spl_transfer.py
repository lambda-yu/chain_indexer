from __future__ import annotations

import struct
from collections.abc import Iterable

import base58

from core.chains.types import SolanaBlock, SolanaInstruction, SolanaTransaction
from core.parser.event import Event

SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_TOKEN_PROGRAMS = {SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID}

_TRANSFER_DISC = 3
_TRANSFER_CHECKED_DISC = 12
_TRANSFER_FEE_EXT_OUTER = 26
_TRANSFER_FEE_EXT_SUB = 1


class SplTransferParser:

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
            from_addr, to_addr, amount, mint, fee = result
            args: dict[str, str] = {
                "from": from_addr,
                "to": to_addr,
                "value": str(amount),
                "mint": mint,
            }
            if fee is not None:
                args["fee"] = str(fee)
            yield Event(
                chain_id=self._chain_id,
                block_number=block.slot,
                block_hash=block.block_hash,
                block_timestamp=block.block_time or 0,
                tx_hash=tx.signature,
                tx_index=None,
                log_index=None,
                kind="token_transfer",
                contract=mint,
                name="Transfer",
                args=args,
                raw={"signature": tx.signature},
            )

    @staticmethod
    def _decode(
        ix: SolanaInstruction, tx: SolanaTransaction,
    ) -> tuple[str, str, int, str, int | None] | None:
        try:
            data = base58.b58decode(ix.data_b58)
        except Exception:  # noqa: BLE001
            return None
        if not data:
            return None

        disc = data[0]
        if disc == _TRANSFER_DISC and len(data) >= 9:
            amount: int = struct.unpack("<Q", data[1:9])[0]
            if len(ix.accounts) < 3:
                return None
            source = ix.accounts[0]
            dest = ix.accounts[1]
            mint = _resolve_mint_from_balances(source, tx)
            if mint is None:
                return None
            return source, dest, amount, mint, None

        if disc == _TRANSFER_CHECKED_DISC and len(data) >= 9:
            amount = struct.unpack("<Q", data[1:9])[0]
            if len(ix.accounts) < 4:
                return None
            source = ix.accounts[0]
            mint = ix.accounts[1]
            dest = ix.accounts[2]
            return source, dest, amount, mint, None

        if disc == _TRANSFER_FEE_EXT_OUTER and len(data) >= 19:
            if data[1] != _TRANSFER_FEE_EXT_SUB:
                return None
            amount = struct.unpack("<Q", data[2:10])[0]
            fee: int = struct.unpack("<Q", data[11:19])[0]
            if len(ix.accounts) < 4:
                return None
            source = ix.accounts[0]
            mint = ix.accounts[1]
            dest = ix.accounts[2]
            return source, dest, amount, mint, fee

        return None


def _resolve_mint_from_balances(
    account: str, tx: SolanaTransaction,
) -> str | None:
    for tb in tx.post_token_balances:
        idx = tb.account_index
        if idx < len(tx.account_keys) and tx.account_keys[idx] == account:
            return tb.mint
    return None
