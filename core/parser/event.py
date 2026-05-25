from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

EventKind = Literal["native_transfer", "token_transfer", "log", "call"]


@dataclass(frozen=True)
class Event:
    """Uniform parsed event. See spec §5.2 for full field semantics.

    Big-int fields (e.g. `value`) are decimal strings to round-trip through JSON
    without precision loss on the consumer side.
    """

    chain_id: str
    block_number: int
    block_hash: str
    block_timestamp: int  # unix seconds
    tx_hash: str
    tx_index: int | None
    log_index: int | None
    kind: EventKind
    contract: str | None
    name: str | None
    args: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)
