from __future__ import annotations

from collections.abc import Iterable

from core.chains.types import Block
from core.parser.event import Event


class NativeTransferParser:
    """Emit a native_transfer Event for each tx with value > 0 (EVM).

    Skips contract creations (to_addr is None) and reverted txs (status == 0).
    On Solana, the SolanaAdapter is responsible for shaping native transfers
    (system program transfer instruction) into Tx entries with status semantics
    matching EVM — this parser is then chain-agnostic.
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
