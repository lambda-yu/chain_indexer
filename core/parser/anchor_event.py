from __future__ import annotations

import base64
import re
from collections.abc import Iterable
from typing import Any

import structlog

from core.abi.decoder import decode_anchor_event
from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import SolanaBlock, SolanaTransaction
from core.parser.event import Event

log = structlog.get_logger(__name__)

_PROGRAM_INVOKE_RE = re.compile(r"^Program (\S+) invoke \[\d+\]$")
_PROGRAM_DATA_PREFIX = "Program data: "


class AnchorIdlEventParser:

    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            yield from self._extract(tx, block)

    def _extract(self, tx: SolanaTransaction, block: SolanaBlock) -> Iterable[Event]:
        program_stack: list[str] = []
        for line in tx.log_messages:
            m = _PROGRAM_INVOKE_RE.match(line)
            if m:
                program_stack.append(m.group(1))
                continue
            if line.startswith("Program ") and line.endswith(" success"):
                if program_stack:
                    program_stack.pop()
                continue
            if line.startswith(_PROGRAM_DATA_PREFIX) and program_stack:
                b64 = line[len(_PROGRAM_DATA_PREFIX):]
                program_id = program_stack[-1]
                ev = self._try_decode(b64, program_id, tx, block)
                if ev is not None:
                    yield ev

    def _try_decode(
        self,
        b64_data: str,
        program_id: str,
        tx: SolanaTransaction,
        block: SolanaBlock,
    ) -> Event | None:
        try:
            raw = base64.b64decode(b64_data)
        except Exception:  # noqa: BLE001
            return None
        if len(raw) < 8:
            return None

        disc_hex = raw[:8].hex()
        lookup = self._registry.lookup_idl_event_by_discriminator(program_id, disc_hex)
        if lookup is None:
            return None

        event_name, struct = lookup
        try:
            args: dict[str, Any] = decode_anchor_event(struct, raw[8:])
        except DecodeFailed as exc:
            log.warning(
                "anchor_event_parser.decode_failed",
                program_id=program_id,
                discriminator=disc_hex,
                error=str(exc),
            )
            return Event(
                chain_id=self._chain_id,
                block_number=block.slot,
                block_hash=block.block_hash,
                block_timestamp=block.block_time or 0,
                tx_hash=tx.signature,
                tx_index=None,
                log_index=None,
                kind="event",
                contract=program_id,
                name=None,
                args={},
                raw={
                    "program_data": b64_data,
                    "program_id": program_id,
                    "discriminator": disc_hex,
                },
            )

        return Event(
            chain_id=self._chain_id,
            block_number=block.slot,
            block_hash=block.block_hash,
            block_timestamp=block.block_time or 0,
            tx_hash=tx.signature,
            tx_index=None,
            log_index=None,
            kind="event",
            contract=program_id,
            name=event_name,
            args=args,
            raw={"signature": tx.signature},
        )
