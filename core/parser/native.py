from __future__ import annotations

from collections.abc import Iterable

from core.chains.types import Block
from core.parser.event import Event


class EvmNativeTransferParser:
    """Emit a native_transfer Event for each tx with value > 0 (EVM only).

    Skips contract creations (to_addr is None) and reverted txs (status == 0).

    EVM-specific: this parser consumes the EVM `Block` shape defined in
    `core/chains/types`. Solana has a different block shape (`SolanaBlock`)
    and a dedicated `SolNativeTransferParser` (see `core/parser/sol_native.py`
    in M2 chunk 11). Earlier versions of this docstring claimed
    `SolanaAdapter` normalized Solana txs into the EVM `Tx` shape — that
    claim is retracted; Solana parsers consume `SolanaBlock` directly via
    `SolanaParserPipeline`.
    """

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for tx in block.txs:
            if tx.to_addr is None or tx.value <= 0 or tx.status != 1:
                continue
            yield Event(
                chain_id=self._chain_id,
                block_number=h.number,
                block_hash=h.hash,
                block_timestamp=h.timestamp,
                tx_hash=tx.hash,
                tx_index=tx.index,
                log_index=None,
                kind="native_transfer",
                contract=None,
                name=None,
                args={
                    "from": tx.from_addr,
                    "to": tx.to_addr,
                    "value": str(tx.value),
                },
                raw={"tx_hash": tx.hash},
            )
