from __future__ import annotations

from core.abi.decoder import event_topic0
from core.abi.registry import AbiRegistry
from core.chains.types import Block, BlockHeader, Log, Tx
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.abi_event import AbiEventParser

_ERC20_TRANSFER = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}
_FROM = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_TO   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_PAD = "0" * 24
_TOPIC0 = event_topic0(_ERC20_TRANSFER)


def _registry_with_transfer() -> AbiRegistry:
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER])],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _block(logs: list[Log]) -> Block:
    return Block(
        header=BlockHeader(number=42, hash="0xh42", parent_hash="0xh41", timestamp=1700000000),
        txs=[Tx(hash="0xtA", index=0, from_addr="0xf", to_addr="0xt",
                value=0, input="0x", status=1)],
        logs=logs,
    )


def _transfer_log(topic0: str = _TOPIC0, value_hex: str = "0x" + "0" * 62 + "7b") -> Log:
    return Log(
        tx_hash="0xtA", log_index=0, address="0xcafe",
        topics=[topic0, "0x" + _PAD + _FROM, "0x" + _PAD + _TO],
        data=value_hex,
    )


def test_emits_event_kind_for_known_topic0_with_decoded_args() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    events = list(p.parse(_block([_transfer_log()])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "event"
    assert e.name == "Transfer"
    assert e.contract == "0xcafe"
    assert e.args == {
        "from":  "0x" + _FROM,
        "to":    "0x" + _TO,
        "value": "123",
    }
    assert e.chain_id == "eth-mainnet"
    assert e.block_number == 42
    assert e.tx_hash == "0xtA"
    assert e.log_index == 0


def test_downgrades_unknown_topic0_to_event_with_name_none() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    unknown_topic0 = "0xdead" + "00" * 30
    events = list(p.parse(_block([_transfer_log(topic0=unknown_topic0)])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "event"
    assert e.name is None
    assert e.args == {}
    assert e.raw["topics"][0] == unknown_topic0
    assert e.raw["data"].startswith("0x")
    assert e.contract == "0xcafe"


def test_downgrade_when_decode_raises() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    bad = Log(
        tx_hash="0xtA", log_index=1, address="0xcafe",
        topics=[_TOPIC0, "0x" + _PAD + _FROM, "0x" + _PAD + _TO],
        data="0xdead",
    )
    events = list(p.parse(_block([bad])))
    assert len(events) == 1
    assert events[0].kind == "event"
    assert events[0].name is None
    assert events[0].raw["topics"][0] == _TOPIC0


def test_empty_topics_log_is_skipped_not_downgraded() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    anon = Log(tx_hash="0xtA", log_index=0, address="0xcafe", topics=[], data="0xff")
    events = list(p.parse(_block([anon])))
    assert events == []


def test_emits_one_event_per_log() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    logs = [
        _transfer_log(value_hex="0x" + "0" * 63 + "1"),
        Log(tx_hash="0xtA", log_index=1, address="0xcafe",
            topics=["0xfeed" + "00" * 30], data="0x"),
    ]
    events = list(p.parse(_block(logs)))
    assert [e.name for e in events] == ["Transfer", None]
    assert [e.kind for e in events] == ["event", "event"]
