from __future__ import annotations

from collections.abc import Iterable

import base58
import structlog

from core.abi.decoder import decode_anchor_borsh
from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import SolanaBlock, SolanaInstruction, SolanaTransaction
from core.parser.event import Event

log = structlog.get_logger(__name__)


class AnchorIdlCallParser:

    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            yield from self._extract(tx, block)

    def _extract(self, tx: SolanaTransaction, block: SolanaBlock) -> Iterable[Event]:
        for ix in tx.instructions:
            if ix.stack_depth != 1:
                continue
            ev = self._try_decode(ix, tx, block)
            if ev is not None:
                yield ev

    def _try_decode(
        self,
        ix: SolanaInstruction,
        tx: SolanaTransaction,
        block: SolanaBlock,
    ) -> Event | None:
        try:
            data = base58.b58decode(ix.data_b58)
        except Exception:  # noqa: BLE001
            return None
        if len(data) < 8:
            return None

        disc_hex = data[:8].hex()
        lookup = self._registry.lookup_idl_call_by_discriminator(ix.program_id, disc_hex)
        if lookup is None:
            return None

        fn_name, struct = lookup
        try:
            args = decode_anchor_borsh(struct, data[8:])
        except DecodeFailed as exc:
            log.warning(
                "anchor_call_parser.decode_failed",
                program_id=ix.program_id,
                discriminator=disc_hex,
                error=str(exc),
            )
            return None

        return Event(
            chain_id=self._chain_id,
            block_number=block.slot,
            block_hash=block.block_hash,
            block_timestamp=block.block_time or 0,
            tx_hash=tx.signature,
            tx_index=None,
            log_index=None,
            kind="call",
            contract=ix.program_id,
            name=fn_name,
            args=args,
            raw={"signature": tx.signature},
        )
