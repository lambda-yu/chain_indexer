from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from core.chains.types import Block
from core.parser.event import Event


class EvmParser(Protocol):
    """An EVM parser consumes a confirmed Block and yields Events."""

    def parse(self, block: Block) -> Iterable[Event]: ...
