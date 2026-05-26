from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import Block, Log
from core.parser.event import Event

log = structlog.get_logger(__name__)


class AbiEventParser:
    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for log_entry in block.logs:
            ev = self._handle_log(log_entry, h.number, h.hash, h.timestamp)
            if ev is not None:
                yield ev

    def _handle_log(
        self,
        log_entry: Log,
        block_number: int,
        block_hash: str,
        block_ts: int,
    ) -> Event | None:
        if not log_entry.topics:
            return None
        topic0 = log_entry.topics[0]

        lookup = self._registry.lookup_event_by_topic0(topic0)
        if lookup is None:
            return self._downgraded(log_entry, block_number, block_hash, block_ts)

        event_name, decoder = lookup
        try:
            args = decoder(topics=log_entry.topics, data=log_entry.data)
        except DecodeFailed as exc:
            log.warning(
                "abi_event_parser.decode_failed",
                topic0=topic0,
                event_name=event_name,
                tx_hash=log_entry.tx_hash,
                error=str(exc),
            )
            return self._downgraded(log_entry, block_number, block_hash, block_ts)

        return Event(
            chain_id=self._chain_id,
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            tx_hash=log_entry.tx_hash,
            tx_index=None,
            log_index=log_entry.log_index,
            kind="event",
            contract=log_entry.address.lower(),
            name=event_name,
            args=args,
            raw={
                "tx_hash": log_entry.tx_hash,
                "log_index": log_entry.log_index,
            },
        )

    def _downgraded(
        self,
        log_entry: Log,
        block_number: int,
        block_hash: str,
        block_ts: int,
    ) -> Event:
        return Event(
            chain_id=self._chain_id,
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            tx_hash=log_entry.tx_hash,
            tx_index=None,
            log_index=log_entry.log_index,
            kind="event",
            contract=log_entry.address.lower(),
            name=None,
            args={},
            raw={
                "tx_hash": log_entry.tx_hash,
                "log_index": log_entry.log_index,
                "topics": list(log_entry.topics),
                "data": log_entry.data,
            },
        )
