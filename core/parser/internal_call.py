from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import structlog

from core.abi.decoder import decode_function_call
from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import Block, InternalCall
from core.parser.event import Event

log = structlog.get_logger(__name__)

_MAX_DEPTH = 64


class InternalCallParser:

    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, traces: list[InternalCall], block: Block) -> Iterable[Event]:
        for trace in traces:
            yield from self._walk(trace, block, depth=0)

    def _walk(
        self, call: InternalCall, block: Block, depth: int,
    ) -> Iterable[Event]:
        if depth > _MAX_DEPTH:
            return
        if call.error is not None:
            return

        ev = self._try_decode(call, block)
        if ev is not None:
            yield ev

        for child in call.calls:
            yield from self._walk(child, block, depth + 1)

    def _try_decode(self, call: InternalCall, block: Block) -> Event | None:
        if call.type in ("CREATE", "CREATE2"):
            return Event(
                chain_id=self._chain_id,
                block_number=block.header.number,
                block_hash=block.header.hash,
                block_timestamp=block.header.timestamp,
                tx_hash="",
                tx_index=None,
                log_index=None,
                kind="call",
                contract=call.created_address,
                name="<create>",
                args={"value": str(call.value)},
                raw={"type": call.type},
            )

        inp = call.input or ""
        if len(inp) < 10:
            return None
        selector = inp[:10].lower()

        lookup = self._registry.lookup_function_by_selector(selector)
        if lookup is None:
            return None

        fn_name, decoder = lookup
        try:
            args: dict[str, Any] = decoder(inp)
        except DecodeFailed:
            return None

        return Event(
            chain_id=self._chain_id,
            block_number=block.header.number,
            block_hash=block.header.hash,
            block_timestamp=block.header.timestamp,
            tx_hash="",
            tx_index=None,
            log_index=None,
            kind="call",
            contract=(call.to_addr or "").lower(),
            name=fn_name,
            args=args,
            raw={"type": call.type, "internal": True},
        )
