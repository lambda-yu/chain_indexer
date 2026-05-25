from core.chains.types import Block, BlockHeader, Log, Tx


def test_dataclasses_are_hashable_by_hash_field() -> None:
    h = BlockHeader(number=1, hash="0xaa", parent_hash="0x00", timestamp=1700000000)
    assert h.number == 1
    assert h.hash == "0xaa"


def test_block_carries_txs_and_logs() -> None:
    tx = Tx(hash="0xt", index=0, from_addr="0xa", to_addr="0xb", value=10, input="0x", status=1)
    log = Log(tx_hash="0xt", log_index=0, address="0xc", topics=["0x1"], data="0x")
    blk = Block(
        header=BlockHeader(number=2, hash="0xbb", parent_hash="0xaa", timestamp=1700000001),
        txs=[tx], logs=[log],
    )
    assert blk.header.number == 2
    assert blk.txs[0].hash == "0xt"
    assert blk.logs[0].address == "0xc"
