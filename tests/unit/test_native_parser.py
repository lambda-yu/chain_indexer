from core.chains.types import Block, BlockHeader, Tx
from core.parser.native import NativeTransferParser


def _block_with(txs: list[Tx]) -> Block:
    return Block(
        header=BlockHeader(number=10, hash="0xb", parent_hash="0xa", timestamp=1700000000),
        txs=txs,
        logs=[],
    )


def test_emits_one_event_per_value_carrying_tx() -> None:
    p = NativeTransferParser(chain_id="eth-mainnet")
    blk = _block_with(
        [
            Tx(
                hash="0xt1",
                index=0,
                from_addr="0xfa",
                to_addr="0xfb",
                value=10**18,
                input="0x",
                status=1,
            ),
            Tx(
                hash="0xt2",
                index=1,
                from_addr="0xfc",
                to_addr="0xfd",
                value=0,
                input="0x",
                status=1,
            ),
            Tx(
                hash="0xt3",
                index=2,
                from_addr="0xfe",
                to_addr="0xff",
                value=42,
                input="0x",
                status=1,
            ),
        ]
    )
    events = list(p.parse(blk))
    assert [e.tx_hash for e in events] == ["0xt1", "0xt3"]
    assert events[0].kind == "native_transfer"
    assert events[0].chain_id == "eth-mainnet"
    assert events[0].block_number == 10
    assert events[0].block_hash == "0xb"
    assert events[0].block_timestamp == 1700000000
    assert events[0].tx_index == 0
    assert events[0].log_index is None
    assert events[0].args == {"from": "0xfa", "to": "0xfb", "value": "1000000000000000000"}


def test_skips_contract_creation_txs() -> None:
    """Contract creation has to_addr=None; native transfer requires a recipient."""
    p = NativeTransferParser(chain_id="eth-mainnet")
    blk = _block_with(
        [
            Tx(
                hash="0xtc",
                index=0,
                from_addr="0xfa",
                to_addr=None,
                value=10**18,
                input="0x60...",
                status=1,
            ),
        ]
    )
    assert list(p.parse(blk)) == []


def test_skips_failed_txs() -> None:
    """status=0 means the tx reverted; no value actually moved."""
    p = NativeTransferParser(chain_id="eth-mainnet")
    blk = _block_with(
        [
            Tx(
                hash="0xtf",
                index=0,
                from_addr="0xfa",
                to_addr="0xfb",
                value=10**18,
                input="0x",
                status=0,
            ),
        ]
    )
    assert list(p.parse(blk)) == []
