from __future__ import annotations

from core.settings import Settings


def test_delivery_records_defaults() -> None:
    s = Settings()
    assert s.delivery_records.max_success_rows == 50000
    assert s.delivery_records.cleanup_interval_seconds == 300
    assert s.delivery_records.cleanup_batch_size == 1000


def test_delivery_records_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_DELIVERY_RECORDS__MAX_SUCCESS_ROWS", "123")
    monkeypatch.setenv("CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_INTERVAL_SECONDS", "7")
    monkeypatch.setenv("CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_BATCH_SIZE", "42")
    s = Settings()
    assert s.delivery_records.max_success_rows == 123
    assert s.delivery_records.cleanup_interval_seconds == 7
    assert s.delivery_records.cleanup_batch_size == 42
