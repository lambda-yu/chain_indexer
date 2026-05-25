from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class BlockHeader:
    number: int
    hash: str
    parent_hash: str
    timestamp: int  # unix seconds


@dataclass(frozen=True)
class Tx:
    hash: str
    index: int
    from_addr: str
    to_addr: str | None  # None for contract creation
    value: int  # wei / lamports
    input: str  # hex string with 0x
    status: int  # 1 success, 0 fail (EVM); on Solana: 1 success, 0 fail


@dataclass(frozen=True)
class Log:
    tx_hash: str
    log_index: int
    address: str
    topics: list[str]
    data: str  # hex string with 0x


@dataclass(frozen=True)
class Block:
    header: BlockHeader
    txs: list[Tx] = field(default_factory=list)
    logs: list[Log] = field(default_factory=list)
