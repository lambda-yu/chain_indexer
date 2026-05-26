from __future__ import annotations

from collections.abc import Iterable, Sequence

import structlog

from core.chains.types import Block
from core.parser.base import EvmParser
from core.parser.event import Event

log = structlog.get_logger(__name__)


class EvmParserPipeline:
    """Run a sequence of parsers over a block and yield all produced events.

    Any parser that raises is logged and skipped (spec §9 "Matcher exception per
    event" applies equally to parsers — pipeline keeps running).
    """

    def __init__(self, parsers: Sequence[EvmParser]) -> None:
        self._parsers = list(parsers)

    def run(self, block: Block) -> Iterable[Event]:
        for p in self._parsers:
            try:
                yield from p.parse(block)
            except Exception:  # noqa: BLE001 — isolate parser failures
                log.exception(
                    "parser.exception",
                    parser=type(p).__name__,
                    block_number=block.header.number,
                    block_hash=block.header.hash,
                )
