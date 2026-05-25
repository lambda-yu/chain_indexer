import json

import structlog

from core.logging import configure_logging


def test_configure_logging_json_emits_valid_json(capsys) -> None:
    configure_logging(level="INFO", format="json")
    log = structlog.get_logger("test")
    log.info("hello", chain_id="eth-mainnet", block_number=1)
    captured = capsys.readouterr().out.strip()
    record = json.loads(captured)
    assert record["event"] == "hello"
    assert record["chain_id"] == "eth-mainnet"
    assert record["block_number"] == 1
    assert record["level"] == "info"


def test_configure_logging_respects_level(capsys) -> None:
    configure_logging(level="WARNING", format="json")
    log = structlog.get_logger("test")
    log.info("should-not-appear")
    log.warning("should-appear")
    out = capsys.readouterr().out
    assert "should-not-appear" not in out
    assert "should-appear" in out
