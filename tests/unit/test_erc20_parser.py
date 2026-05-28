from __future__ import annotations

from core.chains.types import Block, BlockHeader, Log, Tx
from core.parser.erc20 import (
    ERC20_TRANSFER_TOPIC0,
    Erc20TransferParser,
)

_ZERO_ADDR_PAD = "0" * 24   # left-padding for 20-byte address -> 32-byte topic
_FROM = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_TO   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_TOKEN_ADDR = "0xcafe000000000000000000000000000000000000"


def _hdr(n: int = 10) -> BlockHeader:
    return BlockHeader(number=n, hash=f"0xh{n}", parent_hash=f"0xh{n-1}", timestamp=1700000000)


def _erc20_log(
    *,
    tx_hash: str = "0xt1",
    log_index: int = 0,
    address: str = _TOKEN_ADDR,
    topic0: str = ERC20_TRANSFER_TOPIC0,
    from_addr_hex: str = _FROM,
    to_addr_hex: str = _TO,
    value_hex: str | None = "0x" + ("0" * 62) + "7b",  # uint256(123)
) -> Log:
    topics = [
        topic0,
        "0x" + _ZERO_ADDR_PAD + from_addr_hex,
        "0x" + _ZERO_ADDR_PAD + to_addr_hex,
    ]
    return Log(
        tx_hash=tx_hash,
        log_index=log_index,
        address=address,
        topics=topics,
        data=value_hex if value_hex is not None else "0x",
        block_number=10,
    )


def _block(logs: list[Log]) -> Block:
    return Block(
        header=_hdr(10),
        txs=[
            Tx(hash="0xt1", index=0, from_addr="0xf0", to_addr=_TOKEN_ADDR,
               value=0, input="0xa9059cbb", status=1),
        ],
        logs=logs,
    )


def test_decodes_one_erc20_transfer_log() -> None:
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([_erc20_log()])
    events = list(p.parse(blk))
    assert len(events) == 1
    e = events[0]
    assert e.chain_id == "eth-mainnet"
    assert e.kind == "token_transfer"
    assert e.block_number == 10
    assert e.block_hash == "0xh10"
    assert e.tx_hash == "0xt1"
    assert e.tx_index == 0
    assert e.log_index == 0
    assert e.contract == _TOKEN_ADDR
    assert e.name == "Transfer"
    assert e.args == {
        "from":  "0x" + _FROM,
        "to":    "0x" + _TO,
        "value": "123",
    }


def test_emits_one_event_per_matching_log() -> None:
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([
        _erc20_log(log_index=0, value_hex="0x" + "0" * 63 + "1"),
        _erc20_log(log_index=1, value_hex="0x" + "0" * 62 + "0a"),  # 10
    ])
    events = list(p.parse(blk))
    assert [e.log_index for e in events] == [0, 1]
    assert [e.args["value"] for e in events] == ["1", "10"]


def test_skips_logs_with_non_transfer_topic0() -> None:
    other_topic0 = "0x" + "de" * 32
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([_erc20_log(topic0=other_topic0)])
    assert list(p.parse(blk)) == []


def test_skips_erc721_transfers_by_topic_count() -> None:
    log_721 = Log(
        tx_hash="0xt1", log_index=0, address=_TOKEN_ADDR,
        topics=[
            ERC20_TRANSFER_TOPIC0,
            "0x" + _ZERO_ADDR_PAD + _FROM,
            "0x" + _ZERO_ADDR_PAD + _TO,
            "0x" + "0" * 62 + "07",  # tokenId = 7
        ],
        data="0x",
        block_number=10,
    )
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([log_721])
    assert list(p.parse(blk)) == []


def test_skips_logs_with_malformed_data_and_continues() -> None:
    bad = _erc20_log(log_index=0, value_hex="0xdead")  # 2 bytes < 32
    good = _erc20_log(log_index=1)  # standard 123
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([bad, good])
    events = list(p.parse(blk))
    assert len(events) == 1
    assert events[0].log_index == 1
    assert events[0].args["value"] == "123"


def test_normalizes_topic_addresses_to_lowercase_0x() -> None:
    upper_from = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([_erc20_log(from_addr_hex=upper_from)])
    e = next(iter(p.parse(blk)))
    assert e.args["from"] == "0x" + upper_from.lower()
