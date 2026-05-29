from __future__ import annotations

from core.settings import Settings


def test_rpc_pool_defaults() -> None:
    s = Settings()
    assert s.rpc_pool.failure_threshold == 3
    assert s.rpc_pool.cooldown_s == 30.0


def test_rpc_pool_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_RPC_POOL__FAILURE_THRESHOLD", "5")
    monkeypatch.setenv("CHAIN_INDEXER_RPC_POOL__COOLDOWN_S", "12.5")
    s = Settings()
    assert s.rpc_pool.failure_threshold == 5
    assert s.rpc_pool.cooldown_s == 12.5
