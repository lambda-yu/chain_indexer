from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.chains.types import Block, Log
from core.parser.event import Event

log = structlog.get_logger(__name__)

ERC20_TRANSFER_TOPIC0 = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


class Erc20TransferParser:

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        tx_index_by_hash: dict[str, int] = {tx.hash: tx.index for tx in block.txs}
        for log_entry in block.logs:
            ev = self._try_decode(
                log_entry,
                header_number=h.number,
                header_hash=h.hash,
                header_ts=h.timestamp,
                tx_index_by_hash=tx_index_by_hash,
            )
            if ev is not None:
                yield ev

    def _try_decode(
        self,
        log_entry: Log,
        *,
        header_number: int,
        header_hash: str,
        header_ts: int,
        tx_index_by_hash: dict[str, int],
    ) -> Event | None:
        if not log_entry.topics:
            return None
        if log_entry.topics[0].lower() != ERC20_TRANSFER_TOPIC0:
            return None
        if len(log_entry.topics) != 3:
            return None

        data_hex = log_entry.data.removeprefix("0x")
        if len(data_hex) < 64:
            log.warning(
                "erc20_parser.malformed_data",
                tx_hash=log_entry.tx_hash,
                log_index=log_entry.log_index,
                data_len=len(data_hex),
            )
            return None

        try:
            value = int(data_hex[:64], 16)
        except ValueError:
            log.warning(
                "erc20_parser.malformed_value_hex",
                tx_hash=log_entry.tx_hash,
                log_index=log_entry.log_index,
            )
            return None

        return Event(
            chain_id=self._chain_id,
            block_number=header_number,
            block_hash=header_hash,
            block_timestamp=header_ts,
            tx_hash=log_entry.tx_hash,
            tx_index=tx_index_by_hash.get(log_entry.tx_hash),
            log_index=log_entry.log_index,
            kind="token_transfer",
            contract=log_entry.address.lower(),
            name="Transfer",
            args={
                "from": _addr_from_topic(log_entry.topics[1]),
                "to": _addr_from_topic(log_entry.topics[2]),
                "value": str(value),
            },
            raw={
                "tx_hash": log_entry.tx_hash,
                "log_index": log_entry.log_index,
            },
        )


def _addr_from_topic(topic_hex: str) -> str:
    body = topic_hex.removeprefix("0x").lower()
    if len(body) < 40:
        return "0x" + body
    return "0x" + body[-40:]
