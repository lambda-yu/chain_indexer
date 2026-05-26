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


@dataclass(frozen=True)
class SolanaTokenBalance:
    account_index: int
    mint: str
    owner: str | None
    amount: int
    decimals: int


@dataclass(frozen=True)
class SolanaInstruction:
    program_id: str
    accounts: list[str]
    data_b58: str
    stack_depth: int


@dataclass(frozen=True)
class SolanaTransaction:
    signature: str
    slot: int
    success: bool
    fee: int
    account_keys: list[str]
    pre_balances: list[int]
    post_balances: list[int]
    pre_token_balances: list[SolanaTokenBalance]
    post_token_balances: list[SolanaTokenBalance]
    log_messages: list[str]
    instructions: list[SolanaInstruction]


@dataclass(frozen=True)
class SolanaBlock:
    slot: int
    block_hash: str
    parent_slot: int
    block_time: int | None
    transactions: list[SolanaTransaction]


@dataclass(frozen=True)
class InternalCall:
    type: str
    from_addr: str
    to_addr: str | None
    value: int
    gas: int
    input: str
    output: str
    error: str | None = None
    calls: list[InternalCall] = field(default_factory=list)
    created_address: str | None = None
