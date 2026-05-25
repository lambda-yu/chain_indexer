from collections.abc import Iterable

from core.chains.types import Block, BlockHeader, Tx
from core.parser.event import Event
from core.parser.native import NativeTransferParser
from core.parser.pipeline import ParserPipeline


class _FakeParser:
    def __init__(self, tag: str) -> None:
        self._tag = tag

    def parse(self, block: Block) -> Iterable[Event]:
        yield Event(
            chain_id="x",
            block_number=block.header.number,
            block_hash=block.header.hash,
            block_timestamp=block.header.timestamp,
            tx_hash=f"{self._tag}-tx",
            tx_index=None,
            log_index=None,
            kind="log",
            contract=None,
            name=self._tag,
            args={},
            raw={},
        )


def test_pipeline_runs_all_parsers_in_order() -> None:
    blk = Block(
        header=BlockHeader(number=1, hash="0xh", parent_hash="0xp", timestamp=1700000000),
        txs=[],
        logs=[],
    )
    pipe = ParserPipeline(parsers=[_FakeParser("a"), _FakeParser("b")])
    out = list(pipe.run(blk))
    assert [e.name for e in out] == ["a", "b"]


def test_pipeline_isolates_parser_exceptions() -> None:
    """A misbehaving parser must not block others — log + skip."""
    blk = Block(
        header=BlockHeader(number=1, hash="0xh", parent_hash="0xp", timestamp=1700000000),
        txs=[
            Tx(hash="0xt", index=0, from_addr="0xa", to_addr="0xb", value=1, input="0x", status=1)
        ],
        logs=[],
    )

    class _Bad:
        def parse(self, block: Block) -> Iterable[Event]:
            raise RuntimeError("boom")
            yield  # pragma: no cover

    pipe = ParserPipeline(parsers=[_Bad(), NativeTransferParser(chain_id="x")])
    out = list(pipe.run(blk))
    assert len(out) == 1
    assert out[0].kind == "native_transfer"
