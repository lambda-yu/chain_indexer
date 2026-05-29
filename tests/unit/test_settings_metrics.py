from __future__ import annotations

from core.settings import Settings


def test_metrics_defaults() -> None:
    s = Settings()
    assert s.metrics.enabled is True
    assert s.metrics.port == 9091


def test_metrics_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_METRICS__ENABLED", "false")
    monkeypatch.setenv("CHAIN_INDEXER_METRICS__PORT", "9999")
    s = Settings()
    assert s.metrics.enabled is False
    assert s.metrics.port == 9999
