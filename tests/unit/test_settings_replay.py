from __future__ import annotations

from core.settings import Settings


def test_replay_defaults() -> None:
    assert Settings().replay.max_replay_blocks == 10000


def test_replay_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_REPLAY__MAX_REPLAY_BLOCKS", "500")
    assert Settings().replay.max_replay_blocks == 500
