from pathlib import Path

from core.settings import Settings, load_settings


def test_load_settings_from_yaml(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
database:
  url: "sqlite+aiosqlite:///:memory:"
redis:
  url: "redis://localhost:6379/1"
worker:
  default_confirmation_blocks: 6
  default_poll_interval_ms: 2000
  notify_concurrency: 10
  config_reload_interval_s: 5
  shutdown_grace_s: 15
web:
  host: 127.0.0.1
  port: 9000
logging:
  level: DEBUG
  format: console
"""
    )
    s = load_settings(cfg)
    assert isinstance(s, Settings)
    assert s.database.url == "sqlite+aiosqlite:///:memory:"
    assert s.worker.default_confirmation_blocks == 6
    assert s.web.port == 9000
    assert s.logging.level == "DEBUG"


def test_env_overrides_yaml(tmp_path: Path, monkeypatch) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
database:
  url: "sqlite+aiosqlite:///./a.db"
redis:
  url: "redis://localhost:6379/0"
worker:
  default_confirmation_blocks: 12
  default_poll_interval_ms: 3000
  notify_concurrency: 50
  config_reload_interval_s: 5
  shutdown_grace_s: 30
web:
  host: 0.0.0.0
  port: 8000
logging:
  level: INFO
  format: json
"""
    )
    monkeypatch.setenv("CHAIN_INDEXER_WEB__PORT", "9999")
    s = load_settings(cfg)
    assert s.web.port == 9999
