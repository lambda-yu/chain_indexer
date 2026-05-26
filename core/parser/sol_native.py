from __future__ import annotations

from collections.abc import Iterable

from core.chains.types import SolanaBlock, SolanaTransaction
from core.parser.event import Event

SOLANA_SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"


class SolNativeTransferParser:

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            ev = self._try_extract(tx, block)
            if ev is not None:
                yield ev

    def _try_extract(self, tx: SolanaTransaction, block: SolanaBlock) -> Event | None:
        for ix in tx.instructions:
            if ix.program_id != SOLANA_SYSTEM_PROGRAM_ID:
                continue
            if ix.stack_depth != 1:
                continue
            if len(ix.accounts) < 2:
                continue
            # System Program Transfer: discriminator is 2u32 LE = b'\x02\x00\x00\x00'
            # base58 decode of the data would give us the discriminator + lamports,
            # but we can use balance diffs for simpler extraction.
            from_addr = ix.accounts[0]
            to_addr = ix.accounts[1]

            from_idx = tx.account_keys.index(from_addr) if from_addr in tx.account_keys else None
            to_idx = tx.account_keys.index(to_addr) if to_addr in tx.account_keys else None

            if from_idx is None or to_idx is None:
                continue

            lamports_sent = tx.pre_balances[from_idx] - tx.post_balances[from_idx] - tx.fee
            if lamports_sent <= 0:
                continue

            return Event(
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
                    "from": from_addr,
                    "to": to_addr,
                    "value": str(lamports_sent),
                },
                raw={"signature": tx.signature},
            )
        return None
