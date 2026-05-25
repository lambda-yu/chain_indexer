from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from core.chains.types import Block
from core.parser.event import Event


class Parser(Protocol):
    """A parser consumes a confirmed Block and yields Events.

    Implementations should be stateless and side-effect free; the same Block
    may be re-parsed during reorg replay.
    """

    def parse(self, block: Block) -> Iterable[Event]: ...
