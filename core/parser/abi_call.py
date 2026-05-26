from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import Block, Tx
from core.parser.event import Event

log = structlog.get_logger(__name__)

_SELECTOR_HEX_LEN = 10  # "0x" + 8 hex chars = 4 bytes


class AbiCallParser:
    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for tx in block.txs:
            ev = self._handle_tx(tx, h.number, h.hash, h.timestamp)
            if ev is not None:
                yield ev

    def _handle_tx(
        self,
        tx: Tx,
        block_number: int,
        block_hash: str,
        block_ts: int,
    ) -> Event | None:
        if tx.status != 1:
            return None
        if tx.to_addr is None:
            return None
        inp = tx.input or ""
        if len(inp) < _SELECTOR_HEX_LEN:
            return None
        selector = inp[:_SELECTOR_HEX_LEN].lower()

        lookup = self._registry.lookup_function_by_selector(selector)
        if lookup is None:
            return None

        fn_name, decoder = lookup
        try:
            args = decoder(inp)
        except DecodeFailed as exc:
            log.warning(
                "abi_call_parser.decode_failed",
                selector=selector,
                function=fn_name,
                tx_hash=tx.hash,
                error=str(exc),
            )
            return None

        return Event(
            chain_id=self._chain_id,
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            tx_hash=tx.hash,
            tx_index=tx.index,
            log_index=None,
            kind="call",
            contract=tx.to_addr.lower(),
            name=fn_name,
            args=args,
            raw={
                "tx_hash": tx.hash,
                "input": inp,
            },
        )
