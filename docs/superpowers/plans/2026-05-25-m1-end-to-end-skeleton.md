# Chain Indexer — M1 (End-to-End Skeleton) Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a deployable thin end-to-end slice: configure one EVM chain + one wallet-address subscription via REST API, receive native-coin transfers through an HTTP webhook with HMAC signing.

**Architecture:** Web (FastAPI) + Worker (asyncio) split, sharing DB (SQLAlchemy async) and Redis (bus). One asyncio task per chain in worker; confirmation buffer for reorg safety; pluggable Channel interface (only `http` implemented in M1); at-least-once delivery with `delivery_id` idempotency.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic v2, SQLAlchemy 2.x async, Alembic, web3.py v6 async, httpx, structlog, pytest + pytest-asyncio, testcontainers, anvil (foundry) for E2E.

**Spec:** `docs/superpowers/specs/2026-05-25-chain-indexer-design.md`

**Scope explicitly out of M1** (covered by later milestones, do NOT add):
- Token transfer / ABI event / ABI call parsing (M2)
- Solana adapter (M3)
- WebSocket server, MQ channels (M4)
- Web UI static assets, Prometheus metrics (M5)

---

## File Structure (M1)

```
chain_indexer/
├── pyproject.toml                           # deps, tool configs
├── alembic.ini
├── Makefile                                 # dev shortcuts
├── README.md
├── .gitignore
├── config.example.yaml                      # sample settings
│
├── core/
│   ├── __init__.py
│   ├── settings.py                          # pydantic-settings loader
│   ├── logging.py                           # structlog config
│   │
│   ├── config/
│   │   ├── __init__.py
│   │   ├── db.py                            # engine + async session factory
│   │   ├── models.py                        # SQLAlchemy ORM models
│   │   ├── repositories.py                  # CRUD per entity
│   │   └── snapshot.py                      # in-memory subscription index
│   │
│   ├── bus/
│   │   ├── __init__.py
│   │   └── redis_bus.py                     # publish/subscribe wrapper
│   │
│   ├── chains/
│   │   ├── __init__.py
│   │   ├── types.py                         # BlockHeader/Block/Tx/Log dataclasses
│   │   ├── adapter.py                       # ChainAdapter Protocol
│   │   ├── evm.py                           # EvmAdapter (web3.py)
│   │   └── confirmation_buffer.py           # reorg-aware window
│   │
│   ├── parser/
│   │   ├── __init__.py
│   │   ├── event.py                         # Event dataclass
│   │   ├── base.py                          # Parser protocol
│   │   ├── native.py                        # NativeTransferParser
│   │   └── pipeline.py                      # ParserPipeline (composes parsers)
│   │
│   ├── matcher/
│   │   ├── __init__.py
│   │   ├── filters.py                       # arg_filters grammar
│   │   └── matcher.py                       # Matcher (rule index)
│   │
│   └── notifier/
│       ├── __init__.py
│       ├── channel.py                       # Channel ABC + registry
│       ├── payload.py                       # build_payload()
│       ├── retry.py                         # retry policy
│       ├── http.py                          # HttpChannel
│       └── notifier.py                      # Notifier (fanout coordinator)
│
├── apps/
│   ├── __init__.py
│   ├── web/
│   │   ├── __init__.py
│   │   ├── main.py                          # FastAPI app + lifespan
│   │   ├── deps.py                          # DI (session, settings)
│   │   ├── schemas.py                       # Pydantic request/response
│   │   └── routers/
│   │       ├── __init__.py
│   │       ├── chains.py
│   │       ├── subscriptions.py
│   │       └── channels.py
│   └── worker/
│       ├── __init__.py
│       ├── main.py                          # asyncio entrypoint
│       └── chain_runner.py                  # per-chain pipeline orchestrator
│
├── migrations/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── 0001_initial.py
│
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── unit/
    │   ├── test_settings.py
    │   ├── test_confirmation_buffer.py
    │   ├── test_native_parser.py
    │   ├── test_filters.py
    │   ├── test_matcher.py
    │   ├── test_payload.py
    │   ├── test_retry.py
    │   ├── test_http_channel.py
    │   └── test_snapshot.py
    ├── integration/
    │   ├── conftest.py
    │   ├── test_repositories.py
    │   ├── test_bus.py
    │   └── test_web_api.py
    └── e2e/
        ├── conftest.py
        └── test_native_transfer_to_webhook.py
```

**Module responsibilities (one-liners):**

- `core/settings.py` — load `config.yaml` + env, return typed `Settings`
- `core/logging.py` — configure structlog JSON output
- `core/config/db.py` — async engine + session factory
- `core/config/models.py` — ORM models only (no logic)
- `core/config/repositories.py` — DB CRUD; one class per aggregate root
- `core/config/snapshot.py` — read-only in-memory copy of all subscriptions/channels for hot-path matching
- `core/bus/redis_bus.py` — pub/sub primitives over `redis.asyncio`
- `core/chains/types.py` — uniform block/tx/log dataclasses
- `core/chains/adapter.py` — `ChainAdapter` Protocol
- `core/chains/evm.py` — `EvmAdapter` over web3.py
- `core/chains/confirmation_buffer.py` — N-block confirmation window with reorg detection
- `core/parser/event.py` — `Event` dataclass
- `core/parser/base.py` — `Parser` Protocol
- `core/parser/native.py` — emit `native_transfer` Events from tx list
- `core/parser/pipeline.py` — runs all parsers, yields Events
- `core/matcher/filters.py` — `arg_filters` evaluator (closed operator set)
- `core/matcher/matcher.py` — index subscriptions, return hits for an event
- `core/notifier/channel.py` — `Channel` ABC + `CHANNEL_REGISTRY`
- `core/notifier/payload.py` — build the uniform JSON payload
- `core/notifier/retry.py` — retry executor with backoff policy
- `core/notifier/http.py` — `HttpChannel` (POST JSON + HMAC)
- `core/notifier/notifier.py` — `(Event → [Channel.send])` coordinator with bounded concurrency
- `apps/web/main.py` — FastAPI app factory, mounts routers, opens DB pool
- `apps/web/routers/*.py` — REST CRUD per resource
- `apps/worker/main.py` — async entrypoint, signal handling, lifecycle
- `apps/worker/chain_runner.py` — owns one chain: listener → buffer → parser → matcher → notifier

**File-size rule of thumb:** keep each module ≤300 LOC. If approaching the limit, split.

---

## Chunk 1: Project Scaffolding

Establishes the empty-but-runnable shell: dependency declarations, formatter/linter/type-checker/test-runner configs, settings loader, logging, dev workflow scripts. After this chunk, `make lint`, `make typecheck`, and `make test` all pass on an empty test suite.

### Task 1.1: Project metadata + tool configs

**Files:**
- Create: `pyproject.toml`
- Create: `.gitignore`
- Create: `README.md`
- Create: `Makefile`
- Create: `config.example.yaml`

- [ ] **Step 1: Write `pyproject.toml`**

```toml
[project]
name = "chain-indexer"
version = "0.1.0"
description = "Multi-chain block indexer with pluggable notification channels"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.110",
    "uvicorn[standard]>=0.27",
    "pydantic>=2.6",
    "pydantic-settings>=2.2",
    "sqlalchemy[asyncio]>=2.0",
    "alembic>=1.13",
    "aiosqlite>=0.20",          # default driver; users may add asyncpg/asyncmy
    "redis>=5.0",
    "httpx>=0.27",
    "web3>=6.15",
    "structlog>=24.1",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
postgres = ["asyncpg>=0.29"]
mysql = ["asyncmy>=0.2.9"]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-cov>=4.1",
    "ruff>=0.3",
    "mypy>=1.9",
    "testcontainers>=4.0",
    "respx>=0.20",                # httpx mocking
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["core", "apps"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "ASYNC", "SIM"]
ignore = ["E501"]   # line length handled by formatter

[tool.mypy]
python_version = "3.11"
strict = true
plugins = ["pydantic.mypy"]

[[tool.mypy.overrides]]
module = ["tests.*"]
disallow_untyped_defs = false
disallow_incomplete_defs = false
disallow_untyped_decorators = false

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
    "integration: requires testcontainers",
    "e2e: requires anvil",
]
addopts = "-q --strict-markers"
```

- [ ] **Step 2: Write `.gitignore`**

```gitignore
__pycache__/
*.py[cod]
.venv/
.mypy_cache/
.pytest_cache/
.ruff_cache/
.coverage
htmlcov/
dist/
build/
*.egg-info/
.env
*.db
```

- [ ] **Step 3: Write `Makefile`**

```makefile
.PHONY: install lint format typecheck test test-unit test-integration test-e2e migrate web worker

install:
	pip install -e ".[dev,postgres]"

lint:
	ruff check core apps tests

format:
	ruff format core apps tests
	ruff check --fix core apps tests

typecheck:
	mypy core apps

test:
	pytest

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration -m integration

test-e2e:
	pytest tests/e2e -m e2e

migrate:
	alembic upgrade head

web:
	uvicorn apps.web.main:app --reload --port 8000

worker:
	python -m apps.worker.main
```

- [ ] **Step 4: Write `config.example.yaml`**

```yaml
database:
  url: "sqlite+aiosqlite:///./chain_indexer.db"
  echo: false

redis:
  url: "redis://localhost:6379/0"

worker:
  default_confirmation_blocks: 12
  default_poll_interval_ms: 3000
  notify_concurrency: 50
  config_reload_interval_s: 5
  shutdown_grace_s: 30

web:
  host: "0.0.0.0"
  port: 8000

logging:
  level: "INFO"
  format: "json"
```

- [ ] **Step 5: Write minimal `README.md`**

```markdown
# chain-indexer

Multi-chain block indexer with pluggable notification channels.

## Quick start

```bash
make install
cp config.example.yaml config.yaml
make migrate
make web    # in one terminal
make worker # in another
```

See `docs/superpowers/specs/` for design.
```

- [ ] **Step 6: Verify the project installs and tooling runs**

Run: `python -m venv .venv && source .venv/bin/activate && make install`
Expected: dependencies resolve, no errors.

Run: `make lint`
Expected: "All checks passed!" (no files to lint yet, exits 0).

Run: `make typecheck`
Expected: "Success: no issues found in 0 source files" (or exit 0 — mypy may exit 0 with no files).

Run: `make test`
Expected: exit code 5 ("no tests collected"). This is acceptable on a fresh repo; subsequent tasks will add tests and the value will become 0. If pre-commit or CI rejects exit 5, add a placeholder `tests/unit/test_placeholder.py` containing `def test_placeholder(): assert True` and delete it once Task 1.2 adds a real test.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .gitignore README.md Makefile config.example.yaml
git commit -m "chore: project scaffolding and tool configs"
```

### Task 1.2: Settings loader

**Files:**
- Create: `core/__init__.py`
- Create: `core/settings.py`
- Test: `tests/__init__.py`, `tests/unit/__init__.py`, `tests/unit/test_settings.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_settings.py
from pathlib import Path

from core.settings import Settings, load_settings


def test_load_settings_from_yaml(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
database:
  url: sqlite+aiosqlite:///:memory:
redis:
  url: redis://localhost:6379/1
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
  url: sqlite+aiosqlite:///./a.db
redis:
  url: redis://localhost:6379/0
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_settings.py -v`
Expected: FAIL with `ModuleNotFoundError: core.settings`.

- [ ] **Step 3: Write minimal implementation**

```python
# core/__init__.py
```

```python
# core/settings.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class DatabaseSettings(BaseModel):
    url: str
    echo: bool = False


class RedisSettings(BaseModel):
    url: str


class WorkerSettings(BaseModel):
    default_confirmation_blocks: int = 12
    default_poll_interval_ms: int = 3000
    notify_concurrency: int = 50
    config_reload_interval_s: int = 5
    shutdown_grace_s: int = 30


class WebSettings(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


class LoggingSettings(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CHAIN_INDEXER_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    database: DatabaseSettings = Field(
        default_factory=lambda: DatabaseSettings(url="sqlite+aiosqlite:///./chain_indexer.db")
    )
    redis: RedisSettings = Field(default_factory=lambda: RedisSettings(url="redis://localhost:6379/0"))
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    web: WebSettings = Field(default_factory=WebSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Precedence (high → low): env > yaml (passed via init kwargs) > dotenv > secrets > defaults
        return (env_settings, init_settings, dotenv_settings, file_secret_settings)


def load_settings(path: Path | str = Path("config.yaml")) -> Settings:
    """Load YAML config into Settings; env vars take precedence (see settings_customise_sources)."""
    p = Path(path)
    base: dict[str, Any] = {}
    if p.exists():
        with p.open("r") as f:
            base = yaml.safe_load(f) or {}
    return Settings(**base)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_settings.py -v`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add core/__init__.py core/settings.py tests/__init__.py tests/unit/__init__.py tests/unit/test_settings.py
git commit -m "feat(settings): yaml + env-overlay settings loader"
```

### Task 1.3: Structured logging

**Files:**
- Create: `core/logging.py`
- Test: `tests/unit/test_logging.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_logging.py
import json
import logging

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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_logging.py -v`
Expected: FAIL — `core.logging` not found.

- [ ] **Step 3: Write minimal implementation**

```python
# core/logging.py
from __future__ import annotations

import logging
import sys
from typing import Literal

import structlog


def configure_logging(
    *,
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO",
    format: Literal["json", "console"] = "json",
) -> None:
    """Configure stdlib logging + structlog. Idempotent."""
    log_level = getattr(logging, level)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,
    )

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
    ]
    if format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=False))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_logging.py -v`
Expected: PASS, 2 tests.

- [ ] **Step 5: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: both green.

- [ ] **Step 6: Commit**

```bash
git add core/logging.py tests/unit/test_logging.py
git commit -m "feat(logging): structlog JSON/console configuration"
```

### Task 1.4: Sanity-check the scaffolding

- [ ] **Step 1: Run the full check pipeline**

Run: `make lint && make typecheck && make test`
Expected: all three commands exit 0. Two unit tests pass.

- [ ] **Step 2: Verify directory shape**

Run: `find core apps tests -name '*.py' | sort`
Expected output includes at minimum:

```
core/__init__.py
core/logging.py
core/settings.py
tests/__init__.py
tests/unit/__init__.py
tests/unit/test_logging.py
tests/unit/test_settings.py
```

(`apps/` directory may not exist yet; created in Chunk 7.)

- [ ] **Step 3: Tag the milestone checkpoint**

```bash
git tag m1-chunk1-scaffolding
```

---

## Chunk 2: Database Layer

Persistence for chains, subscriptions, channels, subscription↔channel join, checkpoints, and `config_version`. Async SQLAlchemy, Alembic migrations, repository classes, and an in-memory `ConfigSnapshot` used by the worker for hot-path matching.

### Task 2.1: Async engine + session factory

**Files:**
- Create: `core/config/__init__.py`
- Create: `core/config/db.py`
- Test: `tests/unit/test_db.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_db.py
import pytest
from sqlalchemy import text

from core.config.db import Database


@pytest.mark.asyncio
async def test_database_session_executes() -> None:
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.connect()
    async with db.session() as s:
        result = await s.execute(text("SELECT 1"))
        assert result.scalar() == 1
    await db.disconnect()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_db.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement**

```python
# core/config/__init__.py
```

```python
# core/config/db.py
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class Database:
    def __init__(self, url: str, echo: bool = False) -> None:
        self._url = url
        self._echo = echo
        self._engine: AsyncEngine | None = None
        self._sessionmaker: async_sessionmaker[AsyncSession] | None = None

    async def connect(self) -> None:
        self._engine = create_async_engine(self._url, echo=self._echo, future=True)
        self._sessionmaker = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )

    async def disconnect(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None
            self._sessionmaker = None

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a session. Caller is responsible for commit/rollback.

        Sessions auto-close on context exit (uncommitted writes are discarded by
        SQLAlchemy at session close). Repositories never commit on their own —
        callers (routers, worker) decide transaction boundaries.
        """
        assert self._sessionmaker is not None, "Database not connected"
        async with self._sessionmaker() as s:
            yield s

    @property
    def engine(self) -> AsyncEngine:
        assert self._engine is not None, "Database not connected"
        return self._engine
```

- [ ] **Step 4: Run test**

Run: `pytest tests/unit/test_db.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/config/__init__.py core/config/db.py tests/unit/test_db.py
git commit -m "feat(db): async engine + session factory wrapper"
```

### Task 2.2: ORM models

**Files:**
- Create: `core/config/models.py`
- Test: `tests/unit/test_models.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_models.py
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from core.config.models import (
    Base,
    Chain,
    ChainKind,
    Channel,
    ConfigVersion,
    Checkpoint,
    MatchKind,
    Subscription,
    SubscriptionChannel,
    ChannelType,
)


@pytest.mark.asyncio
async def test_can_create_all_tables() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Sanity: the seven tables exist
    async with engine.begin() as conn:
        names = await conn.run_sync(
            lambda sync_conn: list(Base.metadata.tables.keys())
        )
        assert set(names) >= {
            "chains",
            "abis",
            "subscriptions",
            "channels",
            "subscription_channels",
            "checkpoints",
            "config_version",
        }
    await engine.dispose()


def test_enums_have_expected_values() -> None:
    assert {e.value for e in ChainKind} == {"evm", "solana"}
    assert {e.value for e in MatchKind} == {"native_transfer", "token_transfer", "event", "call"}
    assert {e.value for e in ChannelType} == {"mq", "http", "ws"}
    from core.config.models import AbiKind
    assert {e.value for e in AbiKind} == {"evm_abi", "solana_idl"}
```

- [ ] **Step 2: Run test, expect FAIL**

Run: `pytest tests/unit/test_models.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement**

```python
# core/config/models.py
from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _uuid() -> str:
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class ChainKind(str, enum.Enum):
    evm = "evm"
    solana = "solana"


class MatchKind(str, enum.Enum):
    native_transfer = "native_transfer"
    token_transfer = "token_transfer"
    event = "event"
    call = "call"


class AbiKind(str, enum.Enum):
    evm_abi = "evm_abi"
    solana_idl = "solana_idl"


class ChannelType(str, enum.Enum):
    mq = "mq"
    http = "http"
    ws = "ws"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Chain(Base, TimestampMixin):
    __tablename__ = "chains"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[ChainKind] = mapped_column(SAEnum(ChainKind, name="chain_kind"), nullable=False)
    rpc_http: Mapped[str] = mapped_column(Text, nullable=False)
    rpc_ws: Mapped[str | None] = mapped_column(Text, nullable=True)
    confirmations: Mapped[int] = mapped_column(Integer, nullable=False, default=12)
    poll_interval_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=3000)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Abi(Base, TimestampMixin):
    __tablename__ = "abis"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[AbiKind] = mapped_column(SAEnum(AbiKind, name="abi_kind"), nullable=False)
    body: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSON, nullable=False)


class Subscription(Base, TimestampMixin):
    __tablename__ = "subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    chain_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("chains.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    abi_id: Mapped[str | None] = mapped_column(
        ForeignKey("abis.id", ondelete="SET NULL"), nullable=True
    )
    match_kind: Mapped[MatchKind] = mapped_column(SAEnum(MatchKind, name="match_kind"), nullable=False)
    match_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    arg_filters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Channel(Base, TimestampMixin):
    __tablename__ = "channels"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[ChannelType] = mapped_column(SAEnum(ChannelType, name="channel_type"), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class SubscriptionChannel(Base):
    __tablename__ = "subscription_channels"

    subscription_id: Mapped[str] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), primary_key=True
    )
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), primary_key=True
    )


class Checkpoint(Base):
    __tablename__ = "checkpoints"

    chain_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("chains.id", ondelete="CASCADE"), primary_key=True
    )
    last_block: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_block_hash: Mapped[str] = mapped_column(String(80), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class ConfigVersion(Base):
    __tablename__ = "config_version"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
```

- [ ] **Step 4: Run test**

Run: `pytest tests/unit/test_models.py -v`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add core/config/models.py tests/unit/test_models.py
git commit -m "feat(db): ORM models for chains/abis/subscriptions/channels/checkpoints"
```

### Task 2.3: Alembic setup + initial migration

**Files:**
- Create: `alembic.ini`
- Create: `migrations/env.py`
- Create: `migrations/script.py.mako`
- Create: `migrations/versions/0001_initial.py`

- [ ] **Step 1: Write `alembic.ini`**

```ini
[alembic]
script_location = migrations
prepend_sys_path = .
file_template = %%(rev)s_%%(slug)s
sqlalchemy.url = sqlite:///./chain_indexer.db

[loggers]
keys = root,sqlalchemy,alembic

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console
qualname =

[logger_sqlalchemy]
level = WARN
handlers =
qualname = sqlalchemy.engine

[logger_alembic]
level = INFO
handlers =
qualname = alembic

[handler_console]
class = StreamHandler
args = (sys.stderr,)
level = NOTSET
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
datefmt = %H:%M:%S
```

- [ ] **Step 2: Write `migrations/env.py`**

```python
# migrations/env.py
from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from core.config.models import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Allow env override; strip async driver portion if present
url = os.environ.get("ALEMBIC_DATABASE_URL") or config.get_main_option("sqlalchemy.url")
assert url is not None
# Alembic uses sync drivers; map aiosqlite→sqlite, asyncpg→postgresql, asyncmy→mysql+pymysql
sync_url = (
    url.replace("+aiosqlite", "")
    .replace("+asyncpg", "")
    .replace("+asyncmy", "+pymysql")
)
config.set_main_option("sqlalchemy.url", sync_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=sync_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 3: Write `migrations/script.py.mako`**

```mako
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}
"""
from __future__ import annotations

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

revision: str = ${repr(up_revision)}
down_revision: str | None = ${repr(down_revision)}
branch_labels: str | Sequence[str] | None = ${repr(branch_labels)}
depends_on: str | Sequence[str] | None = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    ${downgrades if downgrades else "pass"}
```

- [ ] **Step 4: Sanity-check the models import and generate the migration**

Run: `python -c "from core.config.models import Base; print(sorted(Base.metadata.tables.keys()))"`
Expected: lists 7 tables: `['abis', 'chains', 'channels', 'checkpoints', 'config_version', 'subscription_channels', 'subscriptions']`.

Run: `mkdir -p migrations/versions && alembic revision --autogenerate -m "initial" --rev-id 0001`
Expected: creates `migrations/versions/0001_initial.py` with the deterministic revision id `0001`.

- [ ] **Step 5: Verify migration content**

Open `migrations/versions/0001_initial.py`. It must contain `op.create_table("chains", ...)` plus the other 6 tables. If autogen omitted any (rare with these models), add by hand. Verify `subscriptions.chain_id` and `checkpoints.chain_id` are emitted as `sa.String(length=64)` so the FK widths match `chains.id`.

- [ ] **Step 6: Apply migration to a clean DB**

Run: `rm -f chain_indexer.db && alembic upgrade head`
Expected: "Running upgrade  -> 0001, initial".

Run: `sqlite3 chain_indexer.db ".tables"`
Expected: the output (in any order) is the set:
`{alembic_version, abis, chains, channels, checkpoints, config_version, subscription_channels, subscriptions}`.

- [ ] **Step 7: Commit**

```bash
git add alembic.ini migrations/
git commit -m "feat(db): alembic setup + initial migration"
```

### Task 2.4: Repositories

**Files:**
- Create: `core/config/repositories.py`
- Test: `tests/integration/__init__.py`, `tests/integration/conftest.py`, `tests/integration/test_repositories.py`

- [ ] **Step 1: Write the integration fixtures**

```python
# tests/integration/conftest.py
from collections.abc import AsyncIterator

import pytest_asyncio

from core.config.db import Database
from core.config.models import Base


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()
```

- [ ] **Step 2: Write the failing test**

```python
# tests/integration/test_repositories.py
import pytest

from core.config.models import ChainKind, ChannelType, MatchKind
from core.config.repositories import (
    ChainRepo,
    ChannelRepo,
    CheckpointRepo,
    ConfigVersionRepo,
    SubscriptionRepo,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_chain_crud(db) -> None:
    async with db.session() as s:
        repo = ChainRepo(s)
        await repo.create(
            id="eth-mainnet",
            kind=ChainKind.evm,
            rpc_http="http://localhost:8545",
            rpc_ws=None,
            confirmations=12,
            poll_interval_ms=3000,
            enabled=True,
        )
        await s.commit()
    async with db.session() as s:
        repo = ChainRepo(s)
        rows = await repo.list_enabled()
        assert len(rows) == 1
        assert rows[0].id == "eth-mainnet"


@pytest.mark.asyncio
async def test_config_version_bump_is_atomic(db) -> None:
    async with db.session() as s:
        repo = ConfigVersionRepo(s)
        v1 = await repo.bump()
        v2 = await repo.bump()
        v3 = await repo.bump()
        await s.commit()
    assert v1 == 1 and v2 == 2 and v3 == 3


@pytest.mark.asyncio
async def test_subscription_with_channels(db) -> None:
    async with db.session() as s:
        c_repo = ChainRepo(s)
        await c_repo.create(
            id="eth-mainnet",
            kind=ChainKind.evm,
            rpc_http="x",
            rpc_ws=None,
            confirmations=12,
            poll_interval_ms=3000,
            enabled=True,
        )
        ch_repo = ChannelRepo(s)
        ch = await ch_repo.create(name="hook", type=ChannelType.http, config={"url": "http://x"})
        sub_repo = SubscriptionRepo(s)
        sub = await sub_repo.create(
            name="wallet1",
            chain_id="eth-mainnet",
            address="0xabc",
            abi_id=None,
            match_kind=MatchKind.native_transfer,
            match_name=None,
            arg_filters={},
            enabled=True,
        )
        await sub_repo.bind_channel(sub.id, ch.id)
        await s.commit()
    async with db.session() as s:
        sub_repo = SubscriptionRepo(s)
        bindings = await sub_repo.list_enabled_with_channels()
        assert len(bindings) == 1
        sub, channels = bindings[0]
        assert sub.address == "0xabc"
        assert len(channels) == 1
        assert channels[0].type == ChannelType.http


@pytest.mark.asyncio
async def test_checkpoint_upsert(db) -> None:
    async with db.session() as s:
        c_repo = ChainRepo(s)
        await c_repo.create(
            id="eth-mainnet", kind=ChainKind.evm, rpc_http="x",
            rpc_ws=None, confirmations=12, poll_interval_ms=3000, enabled=True,
        )
        cp = CheckpointRepo(s)
        await cp.upsert("eth-mainnet", last_block=100, last_block_hash="0xaa")
        await cp.upsert("eth-mainnet", last_block=101, last_block_hash="0xbb")
        await s.commit()
    async with db.session() as s:
        cp = CheckpointRepo(s)
        row = await cp.get("eth-mainnet")
        assert row is not None
        assert row.last_block == 101 and row.last_block_hash == "0xbb"
```

- [ ] **Step 3: Run test, expect FAIL**

Run: `pytest tests/integration/test_repositories.py -v -m integration`
Expected: FAIL — module missing.

- [ ] **Step 4: Implement repositories**

```python
# core/config/repositories.py
from __future__ import annotations

from typing import Any

from sqlalchemy import delete as sa_delete, select, update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from core.config.models import (
    Abi,
    AbiKind,
    Chain,
    ChainKind,
    Channel,
    ChannelType,
    Checkpoint,
    ConfigVersion,
    MatchKind,
    Subscription,
    SubscriptionChannel,
)


class ChainRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(
        self,
        *,
        id: str,
        kind: ChainKind,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
        poll_interval_ms: int,
        enabled: bool,
    ) -> Chain:
        c = Chain(
            id=id, kind=kind, rpc_http=rpc_http, rpc_ws=rpc_ws,
            confirmations=confirmations, poll_interval_ms=poll_interval_ms,
            enabled=enabled,
        )
        self.s.add(c)
        await self.s.flush()
        return c

    async def get(self, chain_id: str) -> Chain | None:
        r = await self.s.execute(select(Chain).where(Chain.id == chain_id))
        return r.scalar_one_or_none()

    async def list_enabled(self) -> list[Chain]:
        r = await self.s.execute(select(Chain).where(Chain.enabled.is_(True)))
        return list(r.scalars().all())

    async def update(self, chain_id: str, **fields: Any) -> None:
        await self.s.execute(sa_update(Chain).where(Chain.id == chain_id).values(**fields))

    async def delete(self, chain_id: str) -> None:
        c = await self.get(chain_id)
        if c is not None:
            await self.s.delete(c)


class ChannelRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(self, *, name: str, type: ChannelType, config: dict[str, Any]) -> Channel:
        c = Channel(name=name, type=type, config=config)
        self.s.add(c)
        await self.s.flush()
        return c

    async def get(self, channel_id: str) -> Channel | None:
        r = await self.s.execute(select(Channel).where(Channel.id == channel_id))
        return r.scalar_one_or_none()

    async def list_all(self) -> list[Channel]:
        r = await self.s.execute(select(Channel))
        return list(r.scalars().all())

    async def update(self, channel_id: str, **fields: Any) -> None:
        await self.s.execute(sa_update(Channel).where(Channel.id == channel_id).values(**fields))

    async def delete(self, channel_id: str) -> None:
        c = await self.get(channel_id)
        if c is not None:
            await self.s.delete(c)


class SubscriptionRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(
        self,
        *,
        name: str,
        chain_id: str,
        address: str | None,
        abi_id: str | None,
        match_kind: MatchKind,
        match_name: str | None,
        arg_filters: dict[str, Any],
        enabled: bool,
    ) -> Subscription:
        sub = Subscription(
            name=name, chain_id=chain_id, address=address, abi_id=abi_id,
            match_kind=match_kind, match_name=match_name, arg_filters=arg_filters,
            enabled=enabled,
        )
        self.s.add(sub)
        await self.s.flush()
        return sub

    async def get(self, sub_id: str) -> Subscription | None:
        r = await self.s.execute(select(Subscription).where(Subscription.id == sub_id))
        return r.scalar_one_or_none()

    async def list_all(self) -> list[Subscription]:
        r = await self.s.execute(select(Subscription))
        return list(r.scalars().all())

    async def list_enabled_with_channels(self) -> list[tuple[Subscription, list[Channel]]]:
        subs_res = await self.s.execute(
            select(Subscription).where(Subscription.enabled.is_(True))
        )
        subs = list(subs_res.scalars().all())
        out: list[tuple[Subscription, list[Channel]]] = []
        for sub in subs:
            ch_res = await self.s.execute(
                select(Channel)
                .join(SubscriptionChannel, SubscriptionChannel.channel_id == Channel.id)
                .where(SubscriptionChannel.subscription_id == sub.id)
            )
            out.append((sub, list(ch_res.scalars().all())))
        return out

    async def bind_channel(self, sub_id: str, channel_id: str) -> None:
        self.s.add(SubscriptionChannel(subscription_id=sub_id, channel_id=channel_id))
        await self.s.flush()

    async def unbind_channel(self, sub_id: str, channel_id: str) -> None:
        await self.s.execute(
            sa_delete(SubscriptionChannel).where(
                SubscriptionChannel.subscription_id == sub_id,
                SubscriptionChannel.channel_id == channel_id,
            )
        )

    async def update(self, sub_id: str, **fields: Any) -> None:
        await self.s.execute(sa_update(Subscription).where(Subscription.id == sub_id).values(**fields))

    async def delete(self, sub_id: str) -> None:
        sub = await self.get(sub_id)
        if sub is not None:
            await self.s.delete(sub)


class AbiRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(self, *, name: str, kind: AbiKind, body: Any) -> Abi:
        a = Abi(name=name, kind=kind, body=body)
        self.s.add(a)
        await self.s.flush()
        return a

    async def get(self, abi_id: str) -> Abi | None:
        r = await self.s.execute(select(Abi).where(Abi.id == abi_id))
        return r.scalar_one_or_none()

    async def list_all(self) -> list[Abi]:
        r = await self.s.execute(select(Abi))
        return list(r.scalars().all())


class CheckpointRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def get(self, chain_id: str) -> Checkpoint | None:
        r = await self.s.execute(select(Checkpoint).where(Checkpoint.chain_id == chain_id))
        return r.scalar_one_or_none()

    async def upsert(self, chain_id: str, *, last_block: int, last_block_hash: str) -> None:
        # SELECT-then-INSERT/UPDATE: deliberately portable across SQLite/PG/MySQL
        # instead of dialect-specific ON CONFLICT. Single-writer worker process,
        # so the race window is benign in practice.
        existing = await self.get(chain_id)
        if existing is None:
            self.s.add(
                Checkpoint(
                    chain_id=chain_id, last_block=last_block, last_block_hash=last_block_hash
                )
            )
        else:
            existing.last_block = last_block
            existing.last_block_hash = last_block_hash
        await self.s.flush()


class ConfigVersionRepo:
    """Single-row, monotonic version counter. Bumped on every config write."""

    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def get(self) -> int:
        r = await self.s.execute(select(ConfigVersion).where(ConfigVersion.id == 1))
        row = r.scalar_one_or_none()
        return row.version if row else 0

    async def bump(self) -> int:
        row = await self.s.get(ConfigVersion, 1)
        if row is None:
            row = ConfigVersion(id=1, version=1)
            self.s.add(row)
            await self.s.flush()
            return 1
        row.version += 1
        await self.s.flush()
        return row.version
```

- [ ] **Step 5: Run test**

Run: `pytest tests/integration/test_repositories.py -v -m integration`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
git add core/config/repositories.py tests/integration/__init__.py tests/integration/conftest.py tests/integration/test_repositories.py
git commit -m "feat(db): repositories for chains/abis/subscriptions/channels/checkpoints"
```

### Task 2.5: ConfigSnapshot — in-memory subscription index

**Files:**
- Create: `core/config/snapshot.py`
- Test: `tests/unit/test_snapshot.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_snapshot.py
from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription


def _sub(**overrides):
    base = dict(
        id="s1",
        name="wallet1",
        chain_id="eth-mainnet",
        address="0xabc",
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
        channel_ids=["c1"],
    )
    base.update(overrides)
    return SnapshotSubscription(**base)


def _ch(**overrides):
    base = dict(id="c1", name="hook", type="http", config={"url": "http://x"})
    base.update(overrides)
    return SnapshotChannel(**base)


def test_subscriptions_for_chain_returns_only_matching_chain() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[
            _sub(id="s1", chain_id="eth-mainnet"),
            _sub(id="s2", chain_id="bsc-mainnet"),
        ],
        channels=[_ch()],
    )
    res = s.subscriptions_for_chain("eth-mainnet")
    assert [r.id for r in res] == ["s1"]


def test_disabled_subscriptions_are_skipped() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[
            _sub(id="s1", enabled=False),
            _sub(id="s2", enabled=True),
        ],
        channels=[_ch()],
    )
    res = s.subscriptions_for_chain("eth-mainnet")
    assert [r.id for r in res] == ["s2"]


def test_channels_for_subscription_resolves_ids() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[_sub(channel_ids=["c1", "c2"])],
        channels=[_ch(id="c1"), _ch(id="c2", type="ws")],
    )
    sub = s.subscriptions_for_chain("eth-mainnet")[0]
    chans = s.channels_for_subscription(sub)
    assert {c.id for c in chans} == {"c1", "c2"}


def test_missing_channel_id_is_ignored() -> None:
    s = ConfigSnapshot(
        version=1,
        subscriptions=[_sub(channel_ids=["c1", "c-missing"])],
        channels=[_ch(id="c1")],
    )
    sub = s.subscriptions_for_chain("eth-mainnet")[0]
    chans = s.channels_for_subscription(sub)
    assert [c.id for c in chans] == ["c1"]
```

- [ ] **Step 2: Run test, expect FAIL**

Run: `pytest tests/unit/test_snapshot.py -v`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement**

```python
# core/config/snapshot.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.config.repositories import (
    ChainRepo,
    ConfigVersionRepo,
    SubscriptionRepo,
)


@dataclass(frozen=True)
class SnapshotSubscription:
    id: str
    name: str
    chain_id: str
    address: str | None
    abi_id: str | None
    match_kind: str
    match_name: str | None
    arg_filters: dict[str, Any]
    enabled: bool
    channel_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SnapshotChannel:
    id: str
    name: str
    type: str
    config: dict[str, Any]


@dataclass(frozen=True)
class SnapshotChain:
    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int


@dataclass(frozen=True)
class ConfigSnapshot:
    """Read-only snapshot. The `list[...]` fields are mutable-typed but treat as immutable;
    rebuild a new snapshot rather than mutating in place."""
    version: int
    subscriptions: list[SnapshotSubscription]
    channels: list[SnapshotChannel]
    chains: list[SnapshotChain] = field(default_factory=list)

    def subscriptions_for_chain(self, chain_id: str) -> list[SnapshotSubscription]:
        return [s for s in self.subscriptions if s.chain_id == chain_id and s.enabled]

    def channels_for_subscription(
        self, sub: SnapshotSubscription
    ) -> list[SnapshotChannel]:
        by_id = {c.id: c for c in self.channels}
        return [by_id[cid] for cid in sub.channel_ids if cid in by_id]


async def load_snapshot(session: AsyncSession) -> ConfigSnapshot:
    """Build a ConfigSnapshot from the database in a single transaction."""
    version = await ConfigVersionRepo(session).get()
    chains_rows = await ChainRepo(session).list_enabled()
    sub_bindings = await SubscriptionRepo(session).list_enabled_with_channels()

    snap_chains = [
        SnapshotChain(
            id=c.id,
            kind=c.kind.value,
            rpc_http=c.rpc_http,
            rpc_ws=c.rpc_ws,
            confirmations=c.confirmations,
            poll_interval_ms=c.poll_interval_ms,
        )
        for c in chains_rows
    ]

    snap_channels_by_id: dict[str, SnapshotChannel] = {}
    snap_subs: list[SnapshotSubscription] = []
    for sub, channels in sub_bindings:
        for ch in channels:
            snap_channels_by_id.setdefault(
                ch.id,
                SnapshotChannel(id=ch.id, name=ch.name, type=ch.type.value, config=ch.config),
            )
        snap_subs.append(
            SnapshotSubscription(
                id=sub.id,
                name=sub.name,
                chain_id=sub.chain_id,
                address=sub.address,
                abi_id=sub.abi_id,
                match_kind=sub.match_kind.value,
                match_name=sub.match_name,
                arg_filters=sub.arg_filters or {},
                enabled=sub.enabled,
                channel_ids=[c.id for c in channels],
            )
        )

    return ConfigSnapshot(
        version=version,
        subscriptions=snap_subs,
        channels=list(snap_channels_by_id.values()),
        chains=snap_chains,
    )
```

- [ ] **Step 4: Run test**

Run: `pytest tests/unit/test_snapshot.py -v`
Expected: PASS, 4 tests.

- [ ] **Step 4b: Add an integration test for `load_snapshot()`**

```python
# Append to tests/integration/test_repositories.py
from core.config.snapshot import load_snapshot


@pytest.mark.asyncio
async def test_load_snapshot_round_trip(db) -> None:
    async with db.session() as s:
        await ChainRepo(s).create(
            id="eth-mainnet", kind=ChainKind.evm, rpc_http="http://x",
            rpc_ws=None, confirmations=12, poll_interval_ms=3000, enabled=True,
        )
        ch = await ChannelRepo(s).create(name="hook", type=ChannelType.http, config={"url": "http://x"})
        sub = await SubscriptionRepo(s).create(
            name="wallet1", chain_id="eth-mainnet", address="0xabc", abi_id=None,
            match_kind=MatchKind.native_transfer, match_name=None,
            arg_filters={"to": "0xabc"}, enabled=True,
        )
        await SubscriptionRepo(s).bind_channel(sub.id, ch.id)
        await ConfigVersionRepo(s).bump()
        await s.commit()

    async with db.session() as s:
        snap = await load_snapshot(s)
    assert snap.version == 1
    assert len(snap.chains) == 1 and snap.chains[0].id == "eth-mainnet"
    assert len(snap.subscriptions) == 1
    assert snap.subscriptions[0].channel_ids == [ch.id]
    assert snap.subscriptions[0].arg_filters == {"to": "0xabc"}
    assert len(snap.channels) == 1
    assert snap.channels[0].type == "http"
```

Run: `pytest tests/integration/test_repositories.py::test_load_snapshot_round_trip -v -m integration`
Expected: PASS.

- [ ] **Step 5: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: green.

- [ ] **Step 6: Commit + tag**

```bash
git add core/config/snapshot.py tests/unit/test_snapshot.py
git commit -m "feat(config): ConfigSnapshot + load_snapshot()"
git tag m1-chunk2-database
```

---

## Chunk 3: Redis Bus + Chain Adapter (EVM)

Internal pub/sub primitive (used for `config_changed` and later `ws_fanout`), uniform chain types, `ChainAdapter` Protocol, and the EVM implementation over web3.py.

### Task 3.1: Redis bus wrapper

**Files:**
- Create: `core/bus/__init__.py`, `core/bus/redis_bus.py`
- Test: `tests/integration/test_bus.py`

- [ ] **Step 1: Write the failing integration test**

```python
# tests/integration/test_bus.py
import asyncio
import pytest
from testcontainers.redis import RedisContainer

from core.bus.redis_bus import RedisBus

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_publish_subscribe_round_trip() -> None:
    with RedisContainer("redis:7-alpine") as rc:
        url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}/0"
        pub = RedisBus(url)
        sub = RedisBus(url)
        await pub.connect()
        await sub.connect()
        received: list[dict] = []
        ready = asyncio.Event()

        gen = sub.subscribe("test_channel", ready=ready)

        async def consume() -> None:
            async for msg in gen:
                received.append(msg)
                if len(received) >= 2:
                    return

        task = asyncio.create_task(consume())
        await ready.wait()  # subscriber is attached
        await pub.publish("test_channel", {"k": 1})
        await pub.publish("test_channel", {"k": 2})
        await asyncio.wait_for(task, timeout=2.0)
        await gen.aclose()  # explicit cleanup, runs finally block

        assert received == [{"k": 1}, {"k": 2}]
        await pub.disconnect()
        await sub.disconnect()
```

- [ ] **Step 2: Run test, expect FAIL** (`pytest tests/integration/test_bus.py -v -m integration` — module missing).

- [ ] **Step 3: Implement**

```python
# core/bus/__init__.py
```

```python
# core/bus/redis_bus.py
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis


class RedisBus:
    def __init__(self, url: str) -> None:
        self._url = url
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(self._url, decode_responses=True)
        await self._client.ping()

    async def disconnect(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def ping(self) -> bool:
        """Return True if Redis is reachable. Used by the web `/healthz` route
        (Chunk 9 Task 9.1). Swallows exceptions and returns False on failure
        so callers can render a fail-open health body."""
        if self._client is None:
            return False
        try:
            return bool(await self._client.ping())
        except Exception:
            return False

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        assert self._client is not None
        await self._client.publish(channel, json.dumps(payload))

    async def subscribe(
        self, channel: str, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield JSON-decoded messages from `channel`.

        Pass `ready` to be notified once the subscription is attached (useful
        in tests to avoid sleep-based races). Always call `.aclose()` on the
        returned generator when done to unsubscribe.
        """
        assert self._client is not None
        pubsub = self._client.pubsub()
        await pubsub.subscribe(channel)
        if ready is not None:
            ready.set()
        try:
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                data = msg.get("data")
                if isinstance(data, bytes):
                    data = data.decode()
                yield json.loads(data)
        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
```

- [ ] **Step 4: Run test** → PASS.

- [ ] **Step 5: Commit**

```bash
git add core/bus/ tests/integration/test_bus.py
git commit -m "feat(bus): redis pub/sub wrapper"
```

### Task 3.2: Uniform chain types + ChainAdapter Protocol

**Files:**
- Create: `core/chains/__init__.py`, `core/chains/types.py`, `core/chains/adapter.py`
- Test: `tests/unit/test_chain_types.py`

> Naming note: spec §5.1 lists `get_latest_block`. We rename to `get_latest_block_number` so the call shape (returns `int`) is obvious at the call site. `fetch_block(n)` returns the full `Block`. This naming applies to the Solana adapter too in later milestones.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_chain_types.py
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
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/chains/__init__.py
```

```python
# core/chains/types.py
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class BlockHeader:
    number: int
    hash: str
    parent_hash: str
    timestamp: int  # unix seconds


@dataclass(frozen=True)
class Tx:
    hash: str
    index: int
    from_addr: str
    to_addr: str | None  # None for contract creation
    value: int  # wei / lamports
    input: str  # hex string with 0x
    status: int  # 1 success, 0 fail (EVM); on Solana: 1 success, 0 fail


@dataclass(frozen=True)
class Log:
    tx_hash: str
    log_index: int
    address: str
    topics: list[str]
    data: str  # hex string with 0x


@dataclass(frozen=True)
class Block:
    header: BlockHeader
    txs: list[Tx] = field(default_factory=list)
    logs: list[Log] = field(default_factory=list)
```

```python
# core/chains/adapter.py
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from core.chains.types import Block, BlockHeader, Log


@runtime_checkable
class ChainAdapter(Protocol):
    chain_id: str
    confirmations: int

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_latest_block_number(self) -> int: ...
    async def fetch_block(self, number: int) -> Block: ...
    async def fetch_logs(
        self, from_block: int, to_block: int, addresses: list[str] | None = None
    ) -> list[Log]: ...
    def subscribe_heads(self) -> AsyncIterator[BlockHeader]: ...
    # NOTE: subscribe_heads is a regular (non-async) function returning an
    # AsyncIterator. Callers iterate with `async for`; do NOT `await` the call.
```

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add core/chains/__init__.py core/chains/types.py core/chains/adapter.py tests/unit/test_chain_types.py
git commit -m "feat(chains): uniform types + ChainAdapter Protocol"
```

### Task 3.3: EVM adapter (web3.py async)

**Files:**
- Create: `core/chains/evm.py`
- Test: `tests/integration/test_evm_adapter.py`

- [ ] **Step 1: Write the failing integration test (uses Anvil)**

```python
# tests/integration/test_evm_adapter.py
import asyncio
import socket
import pytest

from core.chains.evm import EvmAdapter

pytestmark = pytest.mark.integration


def _anvil_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture
def anvil_rpc():
    """Requires `anvil --port 8545 --silent` running (locally) or CI service.

    If port 8545 is not reachable, skip — gives a clear message instead of a
    connection-refused traceback on a fresh checkout.
    """
    if not _anvil_reachable("127.0.0.1", 8545):
        pytest.skip("anvil not running on 127.0.0.1:8545; start with `anvil --port 8545 --silent`")
    return "http://127.0.0.1:8545"


@pytest.mark.asyncio
async def test_get_latest_and_fetch_block(anvil_rpc) -> None:
    a = EvmAdapter(chain_id="anvil", rpc_http=anvil_rpc, rpc_ws=None, confirmations=0)
    await a.connect()
    try:
        n = await a.get_latest_block_number()
        assert n >= 0
        blk = await a.fetch_block(n)
        assert blk.header.number == n
        assert blk.header.hash.startswith("0x")
        # fetch_block does NOT embed logs (listener fetches them separately).
        assert blk.logs == []
    finally:
        await a.disconnect()


@pytest.mark.asyncio
async def test_fetch_logs_empty_range_returns_empty(anvil_rpc) -> None:
    a = EvmAdapter(chain_id="anvil", rpc_http=anvil_rpc, rpc_ws=None, confirmations=0)
    await a.connect()
    try:
        latest = await a.get_latest_block_number()
        logs = await a.fetch_logs(latest, latest)
        assert isinstance(logs, list)
    finally:
        await a.disconnect()
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/chains/evm.py
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from web3 import AsyncWeb3
from web3.providers.async_rpc import AsyncHTTPProvider
from web3.providers.persistent import WebSocketProvider

from core.chains.types import Block, BlockHeader, Log, Tx


def _hexify(v: object) -> str:
    """Normalize HexBytes-or-string values to a 0x-prefixed hex string.

    web3.py returns `HexBytes` for hash-like fields on HTTP, but eth_subscribe
    over WS yields plain hex strings. This helper accepts both.
    """
    if isinstance(v, str):
        return v
    if hasattr(v, "hex"):
        s = v.hex()  # type: ignore[union-attr]
        return s if s.startswith("0x") else "0x" + s
    return str(v)


def _intify(v: object) -> int:
    """Accept ints or 0x-hex strings (eth_subscribe yields hex)."""
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return int(v, 16) if v.startswith("0x") else int(v)
    return int(v)  # type: ignore[arg-type]


class EvmAdapter:
    chain_id: str
    confirmations: int

    def __init__(
        self,
        *,
        chain_id: str,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
    ) -> None:
        self.chain_id = chain_id
        self.confirmations = confirmations
        self._rpc_http = rpc_http
        self._rpc_ws = rpc_ws
        self._w3: AsyncWeb3 | None = None

    async def connect(self) -> None:
        self._w3 = AsyncWeb3(AsyncHTTPProvider(self._rpc_http))
        # Sanity ping; raises if the RPC is unreachable.
        await self._w3.eth.block_number

    async def disconnect(self) -> None:
        if self._w3 is not None:
            # In web3.py v6 AsyncHTTPProvider holds an aiohttp session.
            # Closing it explicitly avoids "Unclosed client session" warnings.
            try:
                await self._w3.provider.disconnect()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 — best-effort cleanup
                pass
            self._w3 = None

    async def get_latest_block_number(self) -> int:
        assert self._w3 is not None
        return int(await self._w3.eth.block_number)

    async def fetch_block(self, number: int) -> Block:
        """Fetch block header + transactions. Logs are NOT embedded; callers
        (ChainListener / ConfirmationBuffer) call `fetch_logs` separately to
        avoid double RPC cost when batching."""
        assert self._w3 is not None
        raw = await self._w3.eth.get_block(number, full_transactions=True)
        header = BlockHeader(
            number=int(raw["number"]),
            hash=_hexify(raw["hash"]),
            parent_hash=_hexify(raw["parentHash"]),
            timestamp=int(raw["timestamp"]),
        )
        txs: list[Tx] = []
        for t in raw.get("transactions", []):
            txs.append(
                Tx(
                    hash=_hexify(t["hash"]),
                    index=int(t.get("transactionIndex", 0)),
                    from_addr=str(t["from"]),
                    to_addr=str(t["to"]) if t.get("to") else None,
                    value=int(t.get("value", 0)),
                    input=t.get("input", "0x"),
                    # M1: status is not fetched (would require per-tx receipts).
                    # Downstream parsers in M1 don't depend on status. M2 will
                    # add an optional receipts-fetch pass for failed-tx filtering.
                    status=1,
                )
            )
        return Block(header=header, txs=txs, logs=[])

    async def fetch_logs(
        self, from_block: int, to_block: int, addresses: list[str] | None = None
    ) -> list[Log]:
        assert self._w3 is not None
        params: dict = {"fromBlock": from_block, "toBlock": to_block}
        if addresses:
            params["address"] = addresses
        raw_logs = await self._w3.eth.get_logs(params)
        out: list[Log] = []
        for lg in raw_logs:
            out.append(
                Log(
                    tx_hash=_hexify(lg["transactionHash"]),
                    log_index=int(lg["logIndex"]),
                    address=str(lg["address"]),
                    topics=[_hexify(t) for t in lg["topics"]],
                    data=lg["data"] if isinstance(lg["data"], str) else _hexify(lg["data"]),
                )
            )
        return out

    def subscribe_heads(self) -> AsyncIterator[BlockHeader]:
        """Return an async iterator of new heads. WS if configured, else poll.

        This is a regular (non-async) function so callers can do
        `async for h in adapter.subscribe_heads():` without `await`.
        """
        if self._rpc_ws:
            return self._subscribe_heads_ws()
        return self._poll_heads()

    async def _poll_heads(self) -> AsyncIterator[BlockHeader]:
        assert self._w3 is not None
        last = -1
        while True:
            n = int(await self._w3.eth.block_number)
            if n > last:
                raw = await self._w3.eth.get_block(n)
                yield BlockHeader(
                    number=int(raw["number"]),
                    hash=_hexify(raw["hash"]),
                    parent_hash=_hexify(raw["parentHash"]),
                    timestamp=int(raw["timestamp"]),
                )
                last = n
            await asyncio.sleep(1.0)

    async def _subscribe_heads_ws(self) -> AsyncIterator[BlockHeader]:
        assert self._rpc_ws is not None
        async with AsyncWeb3(WebSocketProvider(self._rpc_ws)) as ws:
            sub_id = await ws.eth.subscribe("newHeads")
            async for raw in ws.socket.process_subscriptions():
                if raw.get("subscription") != sub_id:
                    continue
                head = raw["result"]
                yield BlockHeader(
                    number=_intify(head["number"]),
                    hash=_hexify(head["hash"]),
                    parent_hash=_hexify(head["parentHash"]),
                    timestamp=_intify(head["timestamp"]),
                )
```

> Note on web3.py v6 API: WebSocketProvider's `process_subscriptions` is the canonical async iterator for v6.15+. If web3.py changes the API in a patch release, adjust the iterator call but keep the BlockHeader yield shape stable.

- [ ] **Step 4: Start Anvil locally and run the test**

Run: in another terminal, `anvil --port 8545 --silent` (or `make anvil` if you add a target).
Run: `pytest tests/integration/test_evm_adapter.py -v -m integration`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add core/chains/evm.py tests/integration/test_evm_adapter.py
git commit -m "feat(chains/evm): web3.py adapter (HTTP + optional WS)"
git tag m1-chunk3-chains
```

---

## Chunk 4: ConfirmationBuffer (reorg-aware sliding window)

A small in-memory queue that buffers the last `confirmations` block headers. On each new head:
- If `parent_hash` matches the buffer tip → append.
- If not → walk back to the common ancestor (rewind) and rebuild the branch from there.
- Any block that falls off the tail (depth ≥ `confirmations`) is emitted "confirmed".

For M1 the buffer is a pure in-memory data structure: it does NOT call the chain itself. The ChainListener (Chunk 7) feeds it headers and asks for confirmed blocks. This keeps it trivially unit-testable without RPC.

This buffer is the **only** place reorg detection happens. Downstream stages treat all blocks coming out of it as canonical at the time of emission; consumers reconcile after-the-fact via `block_hash` (see spec §6.2).

### Task 4.1: ConfirmationBuffer data structure

**Files:**
- Create: `core/chains/confirmation_buffer.py`
- Test: `tests/unit/test_confirmation_buffer.py`

- [ ] **Step 1: Write the failing tests (TDD — all cases up front)**

```python
# tests/unit/test_confirmation_buffer.py
from __future__ import annotations

import pytest

from core.chains.confirmation_buffer import ConfirmationBuffer, ReorgEvent
from core.chains.types import BlockHeader


def H(n: int, h: str, parent: str) -> BlockHeader:
    return BlockHeader(number=n, hash=h, parent_hash=parent, timestamp=1700000000 + n)


def test_steady_state_emits_after_confirmations() -> None:
    """With confirmations=2, block N is emitted only when head reaches N+2."""
    buf = ConfirmationBuffer(confirmations=2)
    assert buf.append(H(1, "0x1", "0x0")) == []
    assert buf.append(H(2, "0x2", "0x1")) == []
    confirmed = buf.append(H(3, "0x3", "0x2"))
    assert [c.number for c in confirmed] == [1]
    confirmed = buf.append(H(4, "0x4", "0x3"))
    assert [c.number for c in confirmed] == [2]


def test_confirmations_zero_emits_immediately() -> None:
    """confirmations=0: every appended head is confirmed at once; buffer stays empty."""
    buf = ConfirmationBuffer(confirmations=0)
    confirmed = buf.append(H(1, "0x1", "0x0"))
    assert [c.number for c in confirmed] == [1]
    assert len(buf) == 0


def test_append_rejects_non_linking_head() -> None:
    buf = ConfirmationBuffer(confirmations=2)
    buf.append(H(1, "0x1", "0x0"))
    with pytest.raises(ValueError, match="does not link"):
        buf.append(H(2, "0x2", "0xWRONG"))


def test_handle_new_head_linking_returns_confirmed_list() -> None:
    """If the new head links cleanly, no reorg event; just returns the confirmed list."""
    buf = ConfirmationBuffer(confirmations=1)
    buf.append(H(1, "0x1", "0x0"))
    calls: list[tuple[int, str]] = []

    def resolve(n: int, h: str) -> BlockHeader:
        calls.append((n, h))
        raise AssertionError("resolve_parent must not be called on clean link")

    out = buf.handle_new_head(H(2, "0x2", "0x1"), resolve_parent=resolve)
    assert isinstance(out, list)
    assert [b.number for b in out] == [1]
    assert calls == []


def test_single_block_reorg_rewinds_one() -> None:
    """parent_hash mismatch one block back: rewinds to depth 1, replaces tip."""
    buf = ConfirmationBuffer(confirmations=3)
    buf.append(H(1, "0x1", "0x0"))
    buf.append(H(2, "0x2a", "0x1"))  # branch A at height 2
    # New head at 3, parent 0x2b — must walk back to find ancestor at height 1.
    result = buf.handle_new_head(
        H(3, "0x3b", "0x2b"),
        resolve_parent=lambda n, h: {
            (2, "0x2b"): H(2, "0x2b", "0x1"),
        }[(n, h)],
    )
    assert isinstance(result, ReorgEvent)
    assert result.rewound_to == 1
    assert [b.hash for b in result.new_branch] == ["0x2b", "0x3b"]
    assert [b.hash for b in result.dropped] == ["0x2a"]
    # Buffer tip is now the new branch.
    assert buf.tip() is not None and buf.tip().hash == "0x3b"


def test_multi_block_reorg_walks_back_through_branch() -> None:
    """Reorg spans 3 buffered blocks: walk back through 3 resolve_parent calls."""
    buf = ConfirmationBuffer(confirmations=5)
    buf.append(H(1, "0x1", "0x0"))
    buf.append(H(2, "0x2a", "0x1"))
    buf.append(H(3, "0x3a", "0x2a"))
    buf.append(H(4, "0x4a", "0x3a"))
    # Incoming H(5, "0x5b", "0x4b"). Branch b shares ancestor at height 1.
    branch_b = {
        (4, "0x4b"): H(4, "0x4b", "0x3b"),
        (3, "0x3b"): H(3, "0x3b", "0x2b"),
        (2, "0x2b"): H(2, "0x2b", "0x1"),
    }
    result = buf.handle_new_head(
        H(5, "0x5b", "0x4b"),
        resolve_parent=lambda n, h: branch_b[(n, h)],
    )
    assert isinstance(result, ReorgEvent)
    assert result.deep is False
    assert result.rewound_to == 1
    assert [b.hash for b in result.new_branch] == ["0x2b", "0x3b", "0x4b", "0x5b"]
    assert [b.hash for b in result.dropped] == ["0x2a", "0x3a", "0x4a"]
    assert buf.tip().hash == "0x5b"


def test_reorg_that_advances_tip_returns_newly_confirmed_in_event() -> None:
    """When a reorg's new branch advances the tip past `confirmations`, blocks that
    become confirmed must be reported on the event, not silently dropped."""
    buf = ConfirmationBuffer(confirmations=1)
    buf.append(H(10, "0x10", "0x9"))
    buf.append(H(11, "0x11a", "0x10"))
    # New head at 12 with parent 0x11b. Ancestor is 0x10.
    result = buf.handle_new_head(
        H(12, "0x12b", "0x11b"),
        resolve_parent=lambda n, h: {(11, "0x11b"): H(11, "0x11b", "0x10")}[(n, h)],
    )
    assert isinstance(result, ReorgEvent)
    # After rewind to 10 and appending [11b, 12b], tip=12, confirmations=1
    # → height 10 and 11b should both be drained (depth >= 1).
    assert [b.hash for b in result.confirmed] == ["0x10", "0x11b"]


def test_deep_reorg_returns_actionable_event_and_resets_buffer() -> None:
    """If no common ancestor is found within the buffer, deep=True; buffer is
    reseeded with just the new head so subsequent appends link correctly."""
    buf = ConfirmationBuffer(confirmations=2)
    buf.append(H(10, "0x10", "0x9"))
    buf.append(H(11, "0x11", "0x10"))
    new_head = H(12, "0x12x", "0x11x")
    result = buf.handle_new_head(
        new_head,
        resolve_parent=lambda n, h: H(n, h, f"{h}-no-match"),  # never matches
    )
    assert isinstance(result, ReorgEvent)
    assert result.deep is True
    assert result.divergent_oldest == 10
    assert result.new_head.hash == "0x12x"
    # Buffer reseeded with the new head only.
    assert buf.tip() is not None and buf.tip().hash == "0x12x"
    # Next append from the new branch must link cleanly.
    buf.append(H(13, "0x13x", "0x12x"))


def test_buffer_size_caps_at_confirmations() -> None:
    """Steady-state buffer size equals `confirmations` (oldest drains on each new head)."""
    buf = ConfirmationBuffer(confirmations=3)
    for i in range(1, 11):
        buf.append(H(i, f"0x{i}", f"0x{i-1}"))
    assert len(buf) == 3
```

- [ ] **Step 2: Run, expect FAIL.**

```bash
pytest tests/unit/test_confirmation_buffer.py -v
```

- [ ] **Step 3: Implement**

```python
# core/chains/confirmation_buffer.py
from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from core.chains.types import BlockHeader


@dataclass(frozen=True)
class ReorgEvent:
    """Result of `handle_new_head` when a reorg is detected.

    Fields:
    - `rewound_to`: height of the common ancestor still in the buffer (–1 if deep).
    - `new_branch`: ordered headers from ancestor+1 to the new head (empty if deep).
    - `dropped`: headers popped off the divergent branch (empty if deep).
    - `confirmed`: any headers that became confirmed (depth ≥ confirmations) as
      a result of the reorg's tip advance. Caller MUST consume these.
    - `deep`: True if no common ancestor was found within the buffer's history.
    - `divergent_oldest`: when deep, the oldest height the buffer held at time
      of detection (so the listener can log the affected range).
    - `new_head`: when deep, the header that triggered the reset.
    """
    rewound_to: int
    new_branch: list[BlockHeader] = field(default_factory=list)
    dropped: list[BlockHeader] = field(default_factory=list)
    confirmed: list[BlockHeader] = field(default_factory=list)
    deep: bool = False
    divergent_oldest: int = -1
    new_head: BlockHeader | None = None


class ConfirmationBuffer:
    """Sliding window of the last `confirmations` headers.

    Append a header to advance the tip. Any header whose depth (tip − height)
    reaches `confirmations` is emitted as "confirmed" and removed from the buffer.

    On parent_hash mismatch, call `handle_new_head` which walks back via
    `resolve_parent(n, hash) -> BlockHeader` to find the common ancestor.
    `resolve_parent` MUST return a header whose `.hash == hash` and `.number == n`;
    exceptions raised by it propagate to the caller.

    Deep reorg (no ancestor within buffer) causes the buffer to be reseeded with
    the new head so subsequent appends link cleanly.
    """

    def __init__(self, confirmations: int) -> None:
        if confirmations < 0:
            raise ValueError("confirmations must be >= 0")
        self._confirmations = confirmations
        self._buf: deque[BlockHeader] = deque()

    def __len__(self) -> int:
        return len(self._buf)

    def tip(self) -> BlockHeader | None:
        return self._buf[-1] if self._buf else None

    def append(self, head: BlockHeader) -> list[BlockHeader]:
        """Append a head that links cleanly to the current tip (or is the first).

        Returns the list of headers that just became "confirmed" (depth ≥ confirmations).
        Raises ValueError if the head does not link.
        """
        if self._buf and self._buf[-1].hash != head.parent_hash:
            raise ValueError(
                f"append({head.number}/{head.hash}) does not link to tip "
                f"({self._buf[-1].number}/{self._buf[-1].hash}); call handle_new_head instead"
            )
        self._buf.append(head)
        return self._drain_confirmed()

    def handle_new_head(
        self,
        head: BlockHeader,
        *,
        resolve_parent: Callable[[int, str], BlockHeader],
    ) -> ReorgEvent | list[BlockHeader]:
        """Process a head that may or may not link.

        Clean link → behaves like `append`, returns the confirmed list (no ReorgEvent).
        Mismatch → walks back via `resolve_parent` until a hash matches a buffered
        header, then replaces the divergent suffix and returns a ReorgEvent.
        """
        if not self._buf or self._buf[-1].hash == head.parent_hash:
            return self.append(head)

        # Walk back along the new branch.
        branch: list[BlockHeader] = [head]
        cursor = head
        by_hash = {h.hash: h for h in self._buf}
        max_walk = len(self._buf) + 1
        for _ in range(max_walk):
            if cursor.parent_hash in by_hash:
                ancestor = by_hash[cursor.parent_hash]
                # Pop divergent suffix from buffer; collect what we dropped.
                dropped: list[BlockHeader] = []
                while self._buf and self._buf[-1].hash != ancestor.hash:
                    dropped.append(self._buf.pop())
                dropped.reverse()  # restore oldest-first order
                # Append new branch (currently in reverse order).
                new_branch_ordered = list(reversed(branch))
                for h in new_branch_ordered:
                    self._buf.append(h)
                confirmed = self._drain_confirmed()
                return ReorgEvent(
                    rewound_to=ancestor.number,
                    new_branch=new_branch_ordered,
                    dropped=dropped,
                    confirmed=confirmed,
                )
            # Step one block back along the new branch.
            parent = resolve_parent(cursor.number - 1, cursor.parent_hash)
            branch.append(parent)
            cursor = parent

        # Exhausted the walk: deep reorg. Reset buffer to just the new head so
        # the listener can resume linking.
        divergent_oldest = self._buf[0].number if self._buf else -1
        self._buf.clear()
        self._buf.append(head)
        # Drain in case confirmations=0 (the new head is itself instantly confirmed).
        confirmed = self._drain_confirmed()
        return ReorgEvent(
            rewound_to=-1,
            new_branch=[],
            dropped=[],
            confirmed=confirmed,
            deep=True,
            divergent_oldest=divergent_oldest,
            new_head=head,
        )

    def _drain_confirmed(self) -> list[BlockHeader]:
        """Pop headers whose depth (tip − height) ≥ confirmations and return them."""
        if not self._buf:
            return []
        tip_num = self._buf[-1].number
        confirmed: list[BlockHeader] = []
        while self._buf and (tip_num - self._buf[0].number) >= self._confirmations:
            confirmed.append(self._buf.popleft())
        return confirmed
```

- [ ] **Step 4: Run** → all 9 tests PASS.

```bash
pytest tests/unit/test_confirmation_buffer.py -v
```

- [ ] **Step 5: Commit**

```bash
git add core/chains/confirmation_buffer.py tests/unit/test_confirmation_buffer.py
git commit -m "feat(chains): ConfirmationBuffer with reorg detection"
git tag m1-chunk4-buffer
```

---

## Chunk 5: Event + Parsers + Matcher

The middle of the pipeline: take a confirmed `Block` and produce `Event`s; for each `Event`, find the subscriptions and channels that match.

M1 scope: only `NativeTransferParser` (token / ABI-event / ABI-call parsers ship in M2). The pipeline and matcher are designed to take all four parser kinds, so M2 only adds new `Parser` impls without touching this code.

### Task 5.1: Event dataclass

**Files:**
- Create: `core/parser/__init__.py`, `core/parser/event.py`
- Test: `tests/unit/test_event.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_event.py
from core.parser.event import Event


def test_event_round_trip_dict() -> None:
    e = Event(
        chain_id="eth-mainnet",
        block_number=100,
        block_hash="0xbb",
        block_timestamp=1700000000,
        tx_hash="0xtx",
        tx_index=3,
        log_index=None,
        kind="native_transfer",
        contract=None,
        name=None,
        args={"from": "0xa", "to": "0xb", "value": "1000"},
        raw={},
    )
    assert e.kind == "native_transfer"
    assert e.args["value"] == "1000"
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/parser/__init__.py
```

```python
# core/parser/event.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

EventKind = Literal["native_transfer", "token_transfer", "log", "call"]


@dataclass(frozen=True)
class Event:
    """Uniform parsed event. See spec §5.2 for full field semantics.

    Big-int fields (e.g. `value`) are decimal strings to round-trip through JSON
    without precision loss on the consumer side.
    """
    chain_id: str
    block_number: int
    block_hash: str
    block_timestamp: int  # unix seconds
    tx_hash: str
    tx_index: int | None
    log_index: int | None
    kind: EventKind
    contract: str | None
    name: str | None
    args: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)
```

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**

```bash
git add core/parser/__init__.py core/parser/event.py tests/unit/test_event.py
git commit -m "feat(parser): Event dataclass"
```

### Task 5.2: Parser protocol + NativeTransferParser

**Files:**
- Create: `core/parser/base.py`, `core/parser/native.py`
- Test: `tests/unit/test_native_parser.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_native_parser.py
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
    blk = _block_with([
        Tx(hash="0xt1", index=0, from_addr="0xfa", to_addr="0xfb", value=10**18, input="0x", status=1),
        Tx(hash="0xt2", index=1, from_addr="0xfc", to_addr="0xfd", value=0,      input="0x", status=1),
        Tx(hash="0xt3", index=2, from_addr="0xfe", to_addr="0xff", value=42,     input="0x", status=1),
    ])
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
    blk = _block_with([
        Tx(hash="0xtc", index=0, from_addr="0xfa", to_addr=None, value=10**18, input="0x60...", status=1),
    ])
    assert list(p.parse(blk)) == []


def test_skips_failed_txs() -> None:
    """status=0 means the tx reverted; no value actually moved."""
    p = NativeTransferParser(chain_id="eth-mainnet")
    blk = _block_with([
        Tx(hash="0xtf", index=0, from_addr="0xfa", to_addr="0xfb", value=10**18, input="0x", status=0),
    ])
    assert list(p.parse(blk)) == []
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/parser/base.py
from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from core.chains.types import Block
from core.parser.event import Event


class Parser(Protocol):
    """A parser consumes a confirmed Block and yields Events.

    Implementations should be stateless and side-effect free; the same Block
    may be re-parsed during reorg replay.
    """

    def parse(self, block: Block) -> Iterable[Event]: ...
```

```python
# core/parser/native.py
from __future__ import annotations

from collections.abc import Iterable

from core.chains.types import Block
from core.parser.event import Event


class NativeTransferParser:
    """Emit a native_transfer Event for each tx with value > 0 (EVM).

    Skips contract creations (to_addr is None) and reverted txs (status == 0).
    On Solana, the SolanaAdapter is responsible for shaping native transfers
    (system program transfer instruction) into Tx entries with status semantics
    matching EVM — this parser is then chain-agnostic.
    """

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for tx in block.txs:
            if tx.to_addr is None or tx.value <= 0 or tx.status != 1:
                continue
            yield Event(
                chain_id=self._chain_id,
                block_number=h.number,
                block_hash=h.hash,
                block_timestamp=h.timestamp,
                tx_hash=tx.hash,
                tx_index=tx.index,
                log_index=None,
                kind="native_transfer",
                contract=None,
                name=None,
                args={
                    "from": tx.from_addr,
                    "to": tx.to_addr,
                    "value": str(tx.value),  # decimal string for big int safety
                },
                raw={"tx_hash": tx.hash},
            )
```

- [ ] **Step 4: Run** → 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/parser/base.py core/parser/native.py tests/unit/test_native_parser.py
git commit -m "feat(parser): NativeTransferParser"
```

### Task 5.3: ParserPipeline

**Files:**
- Create: `core/parser/pipeline.py`
- Test: `tests/unit/test_pipeline.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_pipeline.py
from collections.abc import Iterable

from core.chains.types import Block, BlockHeader, Tx
from core.parser.event import Event
from core.parser.native import NativeTransferParser
from core.parser.pipeline import ParserPipeline


class _FakeParser:
    def __init__(self, tag: str) -> None:
        self._tag = tag

    def parse(self, block: Block) -> Iterable[Event]:
        yield Event(
            chain_id="x", block_number=block.header.number, block_hash=block.header.hash,
            block_timestamp=block.header.timestamp, tx_hash=f"{self._tag}-tx",
            tx_index=None, log_index=None, kind="log",
            contract=None, name=self._tag, args={}, raw={},
        )


def test_pipeline_runs_all_parsers_in_order() -> None:
    blk = Block(
        header=BlockHeader(number=1, hash="0xh", parent_hash="0xp", timestamp=1700000000),
        txs=[], logs=[],
    )
    pipe = ParserPipeline(parsers=[_FakeParser("a"), _FakeParser("b")])
    out = list(pipe.run(blk))
    assert [e.name for e in out] == ["a", "b"]


def test_pipeline_isolates_parser_exceptions() -> None:
    """A misbehaving parser must not block others — log + skip."""
    blk = Block(
        header=BlockHeader(number=1, hash="0xh", parent_hash="0xp", timestamp=1700000000),
        txs=[Tx(hash="0xt", index=0, from_addr="0xa", to_addr="0xb", value=1, input="0x", status=1)],
        logs=[],
    )

    class _Bad:
        def parse(self, block: Block) -> Iterable[Event]:
            raise RuntimeError("boom")
            yield  # pragma: no cover

    pipe = ParserPipeline(parsers=[_Bad(), NativeTransferParser(chain_id="x")])
    out = list(pipe.run(blk))
    assert len(out) == 1
    assert out[0].kind == "native_transfer"
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/parser/pipeline.py
from __future__ import annotations

from collections.abc import Iterable, Sequence

import structlog

from core.chains.types import Block
from core.parser.base import Parser
from core.parser.event import Event

log = structlog.get_logger(__name__)


class ParserPipeline:
    """Run a sequence of parsers over a block and yield all produced events.

    Any parser that raises is logged and skipped (spec §9 "Matcher exception per
    event" applies equally to parsers — pipeline keeps running).
    """

    def __init__(self, parsers: Sequence[Parser]) -> None:
        self._parsers = list(parsers)

    def run(self, block: Block) -> Iterable[Event]:
        for p in self._parsers:
            try:
                yield from p.parse(block)
            except Exception:  # noqa: BLE001 — isolate parser failures
                log.exception(
                    "parser.exception",
                    parser=type(p).__name__,
                    block_number=block.header.number,
                    block_hash=block.header.hash,
                )
```

- [ ] **Step 4: Run** → 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/parser/pipeline.py tests/unit/test_pipeline.py
git commit -m "feat(parser): ParserPipeline with per-parser fault isolation"
```

### Task 5.4: arg_filters grammar

Closed operator set from spec §7.3: equality, `_in`, `_gte`, `_lte`.

**Files:**
- Create: `core/matcher/__init__.py`, `core/matcher/filters.py`
- Test: `tests/unit/test_filters.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_filters.py
import pytest

from core.matcher.filters import FilterError, evaluate, validate


def test_validate_rejects_unknown_operator() -> None:
    with pytest.raises(FilterError, match="unknown operator"):
        validate({"value_eq": 1})  # _eq is a forbidden typo for equality (use plain key)


def test_validate_accepts_plain_field_names_with_underscores() -> None:
    """Plain field names like `first_name`, `user_id` must NOT be rejected."""
    validate({"first_name": "alice", "user_id": "u1", "my_long_field_name": 0})


def test_validate_accepts_known_operators() -> None:
    validate({"to": "0xA", "to_in": ["0xa", "0xb"], "value_gte": "1", "value_lte": "10"})


def test_evaluate_equality_case_insensitive_for_hex_addresses() -> None:
    assert evaluate({"to": "0xAbC"}, {"to": "0xabc", "from": "0x1"}) is True
    assert evaluate({"to": "0xZZZ"}, {"to": "0xabc"}) is False


def test_evaluate_in_membership_case_insensitive() -> None:
    assert evaluate({"to_in": ["0xA", "0xB"]}, {"to": "0xb"}) is True
    assert evaluate({"to_in": ["0xA"]}, {"to": "0xc"}) is False


def test_evaluate_gte_lte_for_decimal_strings() -> None:
    f = {"value_gte": "1000000000000000000", "value_lte": "5000000000000000000"}
    assert evaluate(f, {"value": "1000000000000000000"}) is True
    assert evaluate(f, {"value": "5000000000000000001"}) is False
    assert evaluate(f, {"value": "999999999999999999"}) is False


def test_evaluate_missing_field_fails_match() -> None:
    assert evaluate({"to": "0xa"}, {}) is False


def test_evaluate_empty_filter_matches_anything() -> None:
    assert evaluate({}, {"anything": "goes"}) is True


def test_evaluate_combines_all_keys_with_and() -> None:
    f = {"to": "0xa", "value_gte": "10"}
    assert evaluate(f, {"to": "0xa", "value": "11"}) is True
    assert evaluate(f, {"to": "0xa", "value": "9"}) is False
    assert evaluate(f, {"to": "0xb", "value": "11"}) is False
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/matcher/__init__.py
```

```python
# core/matcher/filters.py
from __future__ import annotations

from typing import Any


class FilterError(ValueError):
    """Raised when an arg_filters map contains an unknown operator suffix."""


_OPERATOR_SUFFIXES = ("_in", "_gte", "_lte")
# Common typos that indicate the writer meant an operator but mistyped it.
# Plain field names ending in these suffixes will be rejected so the writer
# doesn't silently get equality semantics when they wanted a range.
_FORBIDDEN_TYPO_SUFFIXES = ("_eq", "_ne", "_gt", "_lt", "_ge", "_le", "_neq", "_like")


def _split(key: str) -> tuple[str, str]:
    """Return (field, op) where op is one of "eq", "in", "gte", "lte"."""
    for s in _OPERATOR_SUFFIXES:
        if key.endswith(s):
            return key[: -len(s)], s[1:]  # strip leading underscore
    return key, "eq"


def _norm(v: Any) -> Any:
    """Hex-address equality is case-insensitive. Lower-case strings for comparison."""
    return v.lower() if isinstance(v, str) else v


def validate(filters: dict[str, Any]) -> None:
    """Reject keys that look like a mistyped operator (e.g. `value_eq`, `to_gt`).

    Plain field names are allowed regardless of length or underscores. Only the
    `_OPERATOR_SUFFIXES` (eq/in/gte/lte) and a small forbidden-typo list are
    enforced; everything else passes through as equality.
    """
    for k in filters:
        if any(k.endswith(s) for s in _OPERATOR_SUFFIXES):
            continue
        if any(k.endswith(s) for s in _FORBIDDEN_TYPO_SUFFIXES):
            raise FilterError(
                f"unknown operator in filter key: {k!r}; allowed: <field>, "
                f"<field>_in, <field>_gte, <field>_lte"
            )
        # Otherwise: plain field name (equality). Accept.


def evaluate(filters: dict[str, Any], args: dict[str, Any]) -> bool:
    """AND-combine all filter entries; missing fields fail the match."""
    for key, expected in filters.items():
        field, op = _split(key)
        if field not in args:
            return False
        actual = args[field]
        if op == "eq":
            if _norm(actual) != _norm(expected):
                return False
        elif op == "in":
            allowed = {_norm(v) for v in expected}
            if _norm(actual) not in allowed:
                return False
        elif op == "gte":
            if int(actual) < int(expected):
                return False
        elif op == "lte":
            if int(actual) > int(expected):
                return False
    return True
```

- [ ] **Step 4: Run** → 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/matcher/__init__.py core/matcher/filters.py tests/unit/test_filters.py
git commit -m "feat(matcher): arg_filters grammar (eq/in/gte/lte)"
```

### Task 5.5: Matcher with subscription index

**Files:**
- Create: `core/matcher/matcher.py`
- Test: `tests/unit/test_matcher.py`

> Note: this task uses `SnapshotSubscription` / `SnapshotChannel` / `ConfigSnapshot` from `core/config/snapshot.py` (Chunk 2). `SnapshotSubscription` carries its bound `channel_ids` inline; there is no separate `subscription_channels` dict.
>
> Latent M2 risk: spec §7.3 lists `match_kind` enum as `native_transfer | token_transfer | event | call`, but `EventKind` (§5.2) is `native_transfer | token_transfer | log | call`. The matcher keys on the literal string equality, so a subscription with `match_kind="event"` will never match (parser would emit `kind="log"` on ABI decode failure or `kind="event"` if we extend EventKind). Reconcile before M2 lands the ABI event parser — either extend EventKind with `"event"` or rename the subscription enum to `log`. M1 only uses `native_transfer`, so this does not block.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_matcher.py
from __future__ import annotations

from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription
from core.matcher.matcher import Matcher
from core.parser.event import Event


def _event(
    *, chain_id: str = "eth", to: str = "0xb", value: str = "100",
    kind: str = "native_transfer",
) -> Event:
    return Event(
        chain_id=chain_id, block_number=1, block_hash="0xh", block_timestamp=0,
        tx_hash="0xt", tx_index=0, log_index=None, kind=kind,  # type: ignore[arg-type]
        contract=None, name=None, args={"from": "0xa", "to": to, "value": value}, raw={},
    )


def _snap(subs: list[SnapshotSubscription], chans: list[SnapshotChannel]) -> ConfigSnapshot:
    return ConfigSnapshot(version=1, subscriptions=subs, channels=chans)


def _sub(**kw) -> SnapshotSubscription:
    defaults = dict(
        id="s1", name="x", chain_id="eth", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None, arg_filters={},
        enabled=True, channel_ids=["c1"],
    )
    defaults.update(kw)
    return SnapshotSubscription(**defaults)


def _ch(**kw) -> SnapshotChannel:
    defaults = dict(id="c1", name="hook", type="http", config={"url": "http://x"})
    defaults.update(kw)
    return SnapshotChannel(**defaults)


def test_matches_by_chain_and_kind() -> None:
    m = Matcher(_snap([_sub()], [_ch()]))
    hits = list(m.match(_event()))
    assert len(hits) == 1
    sub, channels = hits[0]
    assert sub.id == "s1"
    assert [ch.id for ch in channels] == ["c1"]


def test_disabled_subscription_does_not_match() -> None:
    m = Matcher(_snap([_sub(enabled=False)], [_ch()]))
    assert list(m.match(_event())) == []


def test_multiple_subscriptions_all_hit() -> None:
    subs = [
        _sub(id="s1"),
        _sub(id="s2", arg_filters={"to": "0xb"}),
    ]
    m = Matcher(_snap(subs, [_ch()]))
    hits = list(m.match(_event(to="0xb")))
    assert {h[0].id for h in hits} == {"s1", "s2"}


def test_address_match_case_insensitive() -> None:
    sub = _sub(
        match_kind="token_transfer", address="0xAAA", channel_ids=["c1"],
    )
    m = Matcher(_snap([sub], [_ch()]))
    ev = Event(
        chain_id="eth", block_number=1, block_hash="0xh", block_timestamp=0,
        tx_hash="0xt", tx_index=0, log_index=0, kind="token_transfer",
        contract="0xaaa", name="Transfer", args={"to": "0xb", "value": "1"}, raw={},
    )
    assert len(list(m.match(ev))) == 1


def test_chain_id_mismatch_does_not_match() -> None:
    m = Matcher(_snap([_sub(chain_id="bsc")], [_ch()]))
    assert list(m.match(_event(chain_id="eth"))) == []


def test_arg_filter_range_applied() -> None:
    m = Matcher(_snap([_sub(arg_filters={"value_gte": "1000"})], [_ch()]))
    assert list(m.match(_event(value="999"))) == []
    assert len(list(m.match(_event(value="1000")))) == 1


def test_unknown_channel_id_is_skipped_silently() -> None:
    """A subscription bound to a non-existent channel id matches but yields empty channels."""
    m = Matcher(_snap([_sub(channel_ids=["c-missing"])], [_ch(id="c1")]))
    hits = list(m.match(_event()))
    assert len(hits) == 1
    assert hits[0][1] == []  # no resolvable channels
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/matcher/matcher.py
from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription
from core.matcher.filters import evaluate
from core.parser.event import Event

log = structlog.get_logger(__name__)


class Matcher:
    """Index subscriptions by `(chain_id, match_kind)` for O(1) candidate lookup.

    Within the candidate set, address (case-insensitive), match_name, and
    arg_filters are checked sequentially. The number of candidates per
    (chain, kind) is small in practice, so a linear scan is fine.

    The Matcher operates on a `ConfigSnapshot`; rebuild a fresh Matcher on
    hot-reload. It does NOT mutate the snapshot.
    """

    def __init__(self, snapshot: ConfigSnapshot) -> None:
        self._snapshot = snapshot
        self._by_key: dict[tuple[str, str], list[SnapshotSubscription]] = {}
        for s in snapshot.subscriptions:
            if not s.enabled:
                continue
            self._by_key.setdefault((s.chain_id, s.match_kind), []).append(s)
        self._channels: dict[str, SnapshotChannel] = {c.id: c for c in snapshot.channels}

    def match(
        self, event: Event
    ) -> Iterable[tuple[SnapshotSubscription, list[SnapshotChannel]]]:
        candidates = self._by_key.get((event.chain_id, event.kind), [])
        for sub in candidates:
            try:
                if not self._matches(sub, event):
                    continue
                channels = [
                    self._channels[cid] for cid in sub.channel_ids if cid in self._channels
                ]
                yield sub, channels
            except Exception:  # noqa: BLE001 — per-event isolation (spec §9)
                log.exception(
                    "matcher.exception", subscription_id=sub.id, tx_hash=event.tx_hash
                )

    def _matches(self, sub: SnapshotSubscription, event: Event) -> bool:
        # Address (case-insensitive). None = global (e.g. native transfers).
        if sub.address is not None:
            if event.contract is None or sub.address.lower() != event.contract.lower():
                return False
        # match_name: only enforced if both sides specify it (event/call kinds).
        if sub.match_name is not None and event.name != sub.match_name:
            return False
        return evaluate(sub.arg_filters or {}, event.args)
```

- [ ] **Step 4: Run** → 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/matcher/matcher.py tests/unit/test_matcher.py
git commit -m "feat(matcher): subscription index + per-event matching"
git tag m1-chunk5-parser-matcher
```

---

## Chunk 6: Channel ABC + Payload + Retry + HttpChannel + Notifier

The output stage. M1 ships a single channel driver (`HttpChannel`); the `Channel` ABC + `CHANNEL_REGISTRY` make M2 channel additions (MQ, WS) drop-in.

### Task 6.1: Payload builder

Spec §8 defines the uniform JSON payload. Building it is pure data, so test it first.

**Files:**
- Create: `core/notifier/__init__.py`, `core/notifier/payload.py`
- Test: `tests/unit/test_payload.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_payload.py
from __future__ import annotations

import json

from core.config.snapshot import SnapshotSubscription
from core.notifier.payload import build_payload
from core.parser.event import Event


def _sub() -> SnapshotSubscription:
    return SnapshotSubscription(
        id="11111111-1111-1111-1111-111111111111",
        name="USDC big transfers",
        chain_id="eth-mainnet",
        address="0xA0b8...",
        abi_id=None,
        match_kind="token_transfer",
        match_name="Transfer",
        arg_filters={},
        enabled=True,
        channel_ids=["c1"],
    )


def _event() -> Event:
    return Event(
        chain_id="eth-mainnet",
        block_number=19000000,
        block_hash="0xbh",
        block_timestamp=1735689600,
        tx_hash="0xtx",
        tx_index=42,
        log_index=7,
        kind="token_transfer",
        contract="0xA0b8...",
        name="Transfer",
        args={"from": "0xfa", "to": "0xfb", "value": "1000000000"},
        raw={},
    )


def test_payload_shape_matches_spec_section_8(monkeypatch) -> None:
    # Pin clock + delivery_id to assert exact values.
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "delivery-uuid")

    p = build_payload(event=_event(), subscription=_sub())
    assert p == {
        "subscription_id": "11111111-1111-1111-1111-111111111111",
        "subscription_name": "USDC big transfers",
        "chain_id": "eth-mainnet",
        "event": {
            "kind": "token_transfer",
            "name": "Transfer",
            "contract": "0xA0b8...",
            "block_number": 19000000,
            "block_hash": "0xbh",
            "block_timestamp": 1735689600,
            "tx_hash": "0xtx",
            "tx_index": 42,
            "log_index": 7,
            "args": {"from": "0xfa", "to": "0xfb", "value": "1000000000"},
        },
        "delivered_at": 1735689601,
        "delivery_id": "delivery-uuid",
    }
    # Round-trip JSON-serializable.
    json.dumps(p)


def test_delivery_id_is_unique_per_call() -> None:
    a = build_payload(event=_event(), subscription=_sub())
    b = build_payload(event=_event(), subscription=_sub())
    assert a["delivery_id"] != b["delivery_id"]
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/notifier/__init__.py
```

```python
# core/notifier/payload.py
from __future__ import annotations

import time
import uuid
from typing import Any

from core.config.snapshot import SnapshotSubscription
from core.parser.event import Event


def _now_unix() -> int:
    return int(time.time())


def _gen_id() -> str:
    return str(uuid.uuid4())


def build_payload(
    *, event: Event, subscription: SnapshotSubscription
) -> dict[str, Any]:
    """Uniform notification payload (spec §8). Two dedupe keys:
    - logical: (chain_id, tx_hash, log_index, block_hash) — survives reorgs
    - delivery_id: per-attempt idempotency key for at-least-once retry
    """
    return {
        "subscription_id": subscription.id,
        "subscription_name": subscription.name,
        "chain_id": event.chain_id,
        "event": {
            "kind": event.kind,
            "name": event.name,
            "contract": event.contract,
            "block_number": event.block_number,
            "block_hash": event.block_hash,
            "block_timestamp": event.block_timestamp,
            "tx_hash": event.tx_hash,
            "tx_index": event.tx_index,
            "log_index": event.log_index,
            "args": dict(event.args),
        },
        "delivered_at": _now_unix(),
        "delivery_id": _gen_id(),
    }
```

- [ ] **Step 4: Run** → 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/__init__.py core/notifier/payload.py tests/unit/test_payload.py
git commit -m "feat(notifier): uniform payload builder"
```

### Task 6.2: Retry policy

Spec §9: HTTP 5xx → exponential backoff 1/4/16s, max 3 attempts. HTTP 4xx (except 408/429) → no retry. Final failure → log only.

**Files:**
- Create: `core/notifier/retry.py`
- Test: `tests/unit/test_retry.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_retry.py
from __future__ import annotations

import asyncio
import pytest

from core.notifier.retry import RetryAbort, RetryExhausted, retry_with_backoff


@pytest.mark.asyncio
async def test_succeeds_on_first_try() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        return "ok"

    out = await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert out == "ok"
    assert calls == 1


@pytest.mark.asyncio
async def test_retries_on_retryable_error_then_succeeds() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("transient")
        return "ok"

    out = await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert out == "ok"
    assert calls == 3


@pytest.mark.asyncio
async def test_exhausts_after_max_attempts() -> None:
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("always fails")

    with pytest.raises(RetryExhausted) as exc:
        await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert calls == 3
    assert "always fails" in str(exc.value.__cause__)


@pytest.mark.asyncio
async def test_retry_abort_short_circuits() -> None:
    """RetryAbort signals "do not retry" (e.g. HTTP 4xx)."""
    calls = 0

    async def op() -> str:
        nonlocal calls
        calls += 1
        raise RetryAbort("client error 404")

    with pytest.raises(RetryAbort):
        await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert calls == 1


@pytest.mark.asyncio
async def test_backoff_uses_exponential_delays() -> None:
    """With base_delay=0.01, delays are 0.01, 0.04, 0.16. We patch sleep to capture them."""
    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    async def op() -> str:
        raise RuntimeError("nope")

    with pytest.raises(RetryExhausted):
        await retry_with_backoff(
            op, max_attempts=3, base_delay=0.01, factor=4.0, sleep=fake_sleep
        )
    # Two sleeps between three attempts: 0.01, 0.04.
    assert sleeps == [0.01, 0.04]
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/notifier/retry.py
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

import structlog

log = structlog.get_logger(__name__)
T = TypeVar("T")


class RetryAbort(Exception):
    """Raised by an operation to signal "do not retry me" (e.g. HTTP 4xx)."""


class RetryExhausted(Exception):
    """Raised when all retry attempts have been used up. `__cause__` carries the last error."""


async def retry_with_backoff(
    op: Callable[[], Awaitable[T]],
    *,
    max_attempts: int,
    base_delay: float,
    factor: float = 4.0,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Run `op` up to `max_attempts` times with exponential backoff.

    `op` must be a no-arg async callable (typically a `functools.partial` or lambda).
    Raises `RetryAbort` directly without retrying. Other exceptions are retried
    until `max_attempts`, then re-raised as `RetryExhausted` with the last error
    as `__cause__`.

    Default factor=4 with `max_attempts=3` and `base_delay=1.0` yields actual
    sleeps of 1s and 4s between three attempts (the third attempt fires
    immediately and either succeeds or raises `RetryExhausted`). Spec §9's
    "1/4/16s" describes the geometric sequence; with 3 attempts only the first
    two delays are ever realized.
    """
    last_err: BaseException | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await op()
        except RetryAbort:
            raise
        except Exception as e:  # noqa: BLE001 — generic retry surface
            last_err = e
            if attempt >= max_attempts:
                break
            delay = base_delay * (factor ** (attempt - 1))
            log.warning(
                "retry.attempt_failed",
                attempt=attempt,
                max_attempts=max_attempts,
                next_delay=delay,
                error=str(e),
            )
            await sleep(delay)
    out = RetryExhausted(f"giving up after {max_attempts} attempts")
    out.__cause__ = last_err
    raise out
```

- [ ] **Step 4: Run** → 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/retry.py tests/unit/test_retry.py
git commit -m "feat(notifier): retry with exponential backoff"
```

### Task 6.3: Channel ABC + registry

**Files:**
- Create: `core/notifier/channel.py`
- Test: `tests/unit/test_channel_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_channel_registry.py
import pytest

from core.notifier.channel import CHANNEL_REGISTRY, Channel, register_channel


class _FakeChannel(Channel):
    type = "fake"

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, payload: dict) -> None: ...


def test_register_and_lookup() -> None:
    register_channel(_FakeChannel)
    try:
        assert CHANNEL_REGISTRY["fake"] is _FakeChannel
    finally:
        del CHANNEL_REGISTRY["fake"]


def test_register_duplicate_raises() -> None:
    register_channel(_FakeChannel)
    try:
        # A *different* class object claiming the same `type` must collide;
        # re-registering the same class is intentionally idempotent.
        class _FakeChannelDup(Channel):
            type = "fake"

            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict) -> None: ...

        with pytest.raises(ValueError, match="already registered"):
            register_channel(_FakeChannelDup)
    finally:
        del CHANNEL_REGISTRY["fake"]


def test_register_same_class_is_idempotent() -> None:
    register_channel(_FakeChannel)
    try:
        register_channel(_FakeChannel)  # should NOT raise
        assert CHANNEL_REGISTRY["fake"] is _FakeChannel
    finally:
        del CHANNEL_REGISTRY["fake"]
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/notifier/channel.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar


class Channel(ABC):
    """Abstract base for a notification channel driver.

    Lifecycle: `start()` → many `send()` → `stop()`. Implementations should be
    safe to construct from a `SnapshotChannel.config` dict; the worker calls
    `start()` once on first use per chain pipeline.
    """
    type: ClassVar[str]

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def send(self, payload: dict[str, Any]) -> None: ...


CHANNEL_REGISTRY: dict[str, type[Channel]] = {}


def register_channel(cls: type[Channel]) -> type[Channel]:
    """Register a Channel subclass under its `type` attribute. Idempotent only
    if the same class object is re-registered; different classes for the same
    type raise.
    """
    t = cls.type
    if t in CHANNEL_REGISTRY and CHANNEL_REGISTRY[t] is not cls:
        raise ValueError(f"channel type {t!r} already registered to {CHANNEL_REGISTRY[t]!r}")
    CHANNEL_REGISTRY[t] = cls
    return cls
```

- [ ] **Step 4: Run** → 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/channel.py tests/unit/test_channel_registry.py
git commit -m "feat(notifier): Channel ABC + registry"
```

### Task 6.4: HttpChannel

Spec §5.4 + §9: POST JSON, optional HMAC (SHA-256 over body, hex), retry 3× with 1/4/16s backoff on 5xx/timeout, no retry on 4xx except 408/429.

**Files:**
- Create: `core/notifier/http.py`
- Test: `tests/unit/test_http_channel.py`

- [ ] **Step 1: Write the failing tests** (use `respx` to mock httpx)

```python
# tests/unit/test_http_channel.py
from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest
import respx

from core.notifier.http import HttpChannel
from core.notifier.retry import RetryExhausted


@pytest.mark.asyncio
async def test_post_json_success() -> None:
    ch = HttpChannel(config={"url": "https://example.com/hook"})
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                return_value=httpx.Response(200, json={"ok": True})
            )
            await ch.send({"hello": "world"})
        assert route.called
        # body was JSON
        sent = json.loads(route.calls.last.request.content)
        assert sent == {"hello": "world"}
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_hmac_signature_is_byte_correct() -> None:
    secret = "topsecret"
    ch = HttpChannel(config={"url": "https://example.com/hook", "hmac_secret": secret})
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                return_value=httpx.Response(200)
            )
            payload = {"x": 1}
            await ch.send(payload)
        req = route.calls.last.request
        body = req.content
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        assert req.headers["X-Signature"] == f"sha256={expected}"
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_5xx_is_retried_then_succeeds() -> None:
    ch = HttpChannel(
        config={"url": "https://example.com/hook"}, base_delay=0.0
    )
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                side_effect=[
                    httpx.Response(503),
                    httpx.Response(502),
                    httpx.Response(200),
                ]
            )
            await ch.send({})
        assert route.call_count == 3
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_5xx_eventually_gives_up() -> None:
    ch = HttpChannel(
        config={"url": "https://example.com/hook"}, base_delay=0.0
    )
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                return_value=httpx.Response(500)
            )
            with pytest.raises(RetryExhausted):
                await ch.send({})
        assert route.call_count == 3
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_4xx_404_not_retried() -> None:
    ch = HttpChannel(
        config={"url": "https://example.com/hook"}, base_delay=0.0
    )
    await ch.start()
    try:
        async with respx.mock:
            from core.notifier.retry import RetryAbort
            route = respx.post("https://example.com/hook").mock(
                return_value=httpx.Response(404)
            )
            with pytest.raises(RetryAbort):
                await ch.send({})
            assert route.call_count == 1
    finally:
        await ch.stop()


@pytest.mark.asyncio
async def test_4xx_429_is_retried() -> None:
    ch = HttpChannel(
        config={"url": "https://example.com/hook"}, base_delay=0.0
    )
    await ch.start()
    try:
        async with respx.mock:
            route = respx.post("https://example.com/hook").mock(
                side_effect=[httpx.Response(429), httpx.Response(200)]
            )
            await ch.send({})
            assert route.call_count == 2
    finally:
        await ch.stop()
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/notifier/http.py
from __future__ import annotations

import hashlib
import hmac
import json
from functools import partial
from typing import Any

import httpx
import structlog

from core.notifier.channel import Channel, register_channel
from core.notifier.retry import RetryAbort, retry_with_backoff

log = structlog.get_logger(__name__)

_RETRYABLE_STATUS = {408, 429}


class HttpChannel(Channel):
    """POST the payload as JSON. Adds optional `X-Signature: sha256=<hex>` HMAC.

    Retry policy (spec §9):
    - 5xx / network error → retried with exponential backoff (1/4/16s by default)
    - 4xx → not retried, except 408 (request timeout) and 429 (rate limited)
    """

    type = "http"

    def __init__(self, *, config: dict[str, Any], base_delay: float = 1.0) -> None:
        self._url: str = config["url"]
        self._method: str = config.get("method", "POST").upper()
        self._headers: dict[str, str] = dict(config.get("headers", {}))
        self._hmac_secret: str | None = config.get("hmac_secret")
        self._timeout: float = float(config.get("timeout_seconds", 10.0))
        self._base_delay = base_delay
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=self._timeout)

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def send(self, payload: dict[str, Any]) -> None:
        assert self._client is not None, "HttpChannel.start() must be called first"
        body = json.dumps(payload, separators=(",", ":")).encode()
        headers = dict(self._headers)
        headers.setdefault("Content-Type", "application/json")
        if self._hmac_secret:
            sig = hmac.new(self._hmac_secret.encode(), body, hashlib.sha256).hexdigest()
            headers["X-Signature"] = f"sha256={sig}"

        await retry_with_backoff(
            partial(self._post_once, body=body, headers=headers),
            max_attempts=3,
            base_delay=self._base_delay,
        )

    async def _post_once(self, *, body: bytes, headers: dict[str, str]) -> None:
        assert self._client is not None
        resp = await self._client.request(self._method, self._url, content=body, headers=headers)
        if 200 <= resp.status_code < 300:
            return
        if 400 <= resp.status_code < 500 and resp.status_code not in _RETRYABLE_STATUS:
            raise RetryAbort(f"http {resp.status_code} from {self._url}")
        # 5xx or 408/429: raise a normal exception → retry path
        raise RuntimeError(f"http {resp.status_code} from {self._url}")


register_channel(HttpChannel)
```

- [ ] **Step 4: Run** → 6 tests PASS.

```bash
pytest tests/unit/test_http_channel.py -v
```

- [ ] **Step 5: Commit**

```bash
git add core/notifier/http.py tests/unit/test_http_channel.py
git commit -m "feat(notifier): HttpChannel with HMAC + retry"
```

### Task 6.5: Notifier (fanout coordinator)

Glue: take `(event, [(subscription, [channels])])` from the Matcher, render the payload once per subscription, dispatch to each bound channel concurrently with a semaphore-bounded pool (spec §6.1).

**Files:**
- Create: `core/notifier/notifier.py`
- Test: `tests/unit/test_notifier.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_notifier.py
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from core.config.snapshot import SnapshotChannel, SnapshotSubscription
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.event import Event


class _CollectingChannel(Channel):
    type = "collect"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send(self, payload: dict[str, Any]) -> None:
        self.calls.append(payload)


def _sub(channel_ids: list[str]) -> SnapshotSubscription:
    return SnapshotSubscription(
        id="s1", name="sub", chain_id="eth", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None, arg_filters={}, enabled=True,
        channel_ids=channel_ids,
    )


def _ch(id_: str = "c1") -> SnapshotChannel:
    return SnapshotChannel(id=id_, name="x", type="collect", config={})


def _event() -> Event:
    return Event(
        chain_id="eth", block_number=1, block_hash="0xh", block_timestamp=0,
        tx_hash="0xt", tx_index=0, log_index=None, kind="native_transfer",
        contract=None, name=None, args={"from": "0xa", "to": "0xb", "value": "1"}, raw={},
    )


@pytest.mark.asyncio
async def test_dispatch_sends_payload_to_each_channel() -> None:
    notifier = Notifier(
        channel_factory=lambda cfg: _CollectingChannel(),
        max_concurrency=10,
    )
    await notifier.start([_ch("c1"), _ch("c2")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c1", "c2"]), [_ch("c1"), _ch("c2")])])
        # Both channels should have received exactly one payload.
        assert all(len(c.calls) == 1 for c in notifier._channels.values())
        for c in notifier._channels.values():
            assert c.calls[0]["chain_id"] == "eth"
    finally:
        await notifier.stop()


@pytest.mark.asyncio
async def test_one_channel_failure_does_not_block_others() -> None:
    class _Bad(Channel):
        type = "bad"

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload):  # type: ignore[no-untyped-def]
            raise RuntimeError("boom")

    good = _CollectingChannel()
    bad = _Bad()
    mapping = {"c-good": good, "c-bad": bad}

    notifier = Notifier(
        channel_factory=lambda cfg: mapping[cfg.id],
        max_concurrency=10,
    )
    await notifier.start([_ch("c-good"), _ch("c-bad")])
    try:
        await notifier.dispatch(
            _event(),
            [(_sub(["c-good", "c-bad"]), [_ch("c-good"), _ch("c-bad")])],
        )
        assert len(good.calls) == 1  # good received despite bad failing
    finally:
        await notifier.stop()


@pytest.mark.asyncio
async def test_semaphore_caps_concurrent_sends() -> None:
    """With max_concurrency=2 and 5 channels using a slow send, never >2 inflight."""
    in_flight = 0
    peak = 0
    lock = asyncio.Lock()

    class _Slow(Channel):
        type = "slow"

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload):  # type: ignore[no-untyped-def]
            nonlocal in_flight, peak
            async with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            await asyncio.sleep(0.02)
            async with lock:
                in_flight -= 1

    chans = [_ch(f"c{i}") for i in range(5)]
    mapping = {c.id: _Slow() for c in chans}
    notifier = Notifier(channel_factory=lambda cfg: mapping[cfg.id], max_concurrency=2)
    await notifier.start(chans)
    try:
        await notifier.dispatch(_event(), [(_sub([c.id for c in chans]), chans)])
        assert peak <= 2
    finally:
        await notifier.stop()
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```python
# core/notifier/notifier.py
from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence

import structlog

from core.config.snapshot import SnapshotChannel, SnapshotSubscription
from core.notifier.channel import CHANNEL_REGISTRY, Channel
from core.notifier.payload import build_payload
from core.parser.event import Event

log = structlog.get_logger(__name__)


def _default_factory(cfg: SnapshotChannel) -> Channel:
    cls = CHANNEL_REGISTRY[cfg.type]
    return cls(config=cfg.config)  # type: ignore[call-arg]


class Notifier:
    """Owns instantiated channels and dispatches events to them concurrently.

    A bounded `asyncio.Semaphore` (default 50) caps total in-flight sends across
    all channels held by this `Notifier` instance. Spec §6.1 specifies a *per-chain*
    semaphore — the worker (Chunk 7) instantiates one `Notifier` per chain, so the
    per-instance limit here IS the per-chain limit. Do not share a single `Notifier`
    across chains; that would conflate the two budgets.

    Failures in one channel do not block sibling channels — each `send` is wrapped
    to log-and-continue, and `asyncio.gather(..., return_exceptions=True)` is used
    defensively so a bug *outside* the `try` in `_send_one` cannot cancel siblings.
    """

    def __init__(
        self,
        *,
        channel_factory: Callable[[SnapshotChannel], Channel] = _default_factory,
        max_concurrency: int = 50,
    ) -> None:
        self._factory = channel_factory
        self._sem = asyncio.Semaphore(max_concurrency)
        self._channels: dict[str, Channel] = {}

    async def start(self, channels: Sequence[SnapshotChannel]) -> None:
        for cfg in channels:
            inst = self._factory(cfg)
            await inst.start()
            self._channels[cfg.id] = inst

    async def stop(self) -> None:
        for ch in self._channels.values():
            try:
                await ch.stop()
            except Exception:  # noqa: BLE001
                log.exception("notifier.channel_stop_failed", type=ch.type)
        self._channels.clear()

    async def dispatch(
        self,
        event: Event,
        hits: Sequence[tuple[SnapshotSubscription, Sequence[SnapshotChannel]]],
    ) -> None:
        """Build one payload per (sub, channel) pair and send concurrently."""
        tasks: list[asyncio.Task[None]] = []
        for sub, chans in hits:
            payload = build_payload(event=event, subscription=sub)
            for ch_cfg in chans:
                ch = self._channels.get(ch_cfg.id)
                if ch is None:
                    log.warning(
                        "notifier.channel_not_started",
                        channel_id=ch_cfg.id, subscription_id=sub.id,
                    )
                    continue
                tasks.append(asyncio.create_task(self._send_one(ch, payload, sub.id, ch_cfg.id)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_one(
        self, ch: Channel, payload: dict, subscription_id: str, channel_id: str
    ) -> None:
        async with self._sem:
            try:
                await ch.send(payload)
            except Exception:  # noqa: BLE001 — log-only per spec §9
                log.exception(
                    "notifier.send_failed",
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    delivery_id=payload.get("delivery_id"),
                )
```

- [ ] **Step 4: Run** → 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/notifier.py tests/unit/test_notifier.py
git commit -m "feat(notifier): fanout coordinator with bounded concurrency"
git tag m1-chunk6-notifier
```

---

## Chunk 7: Worker — ConfigWatcher + ChainRunner

Builds the two stateful worker components that drive the M1 pipeline. Adds a hot-reload `ConfigWatcher` (Redis `config_changed` subscription + 5-s `config_version` poll fallback per spec §5.5) and a per-chain `ChainRunner` that orchestrates `EvmAdapter → ConfirmationBuffer → ParserPipeline → Matcher → Notifier` plus checkpointing. The asyncio entrypoint that ties them together — plus its integration test — lives in Chunk 8.

**New files this chunk:**
- `apps/worker/config_watcher.py` — drives snapshot refresh
- `apps/worker/chain_runner.py` — per-chain pipeline orchestrator
- `tests/unit/test_config_watcher.py`
- `tests/unit/test_chain_runner.py`

**Out of M1 scope (do NOT add):**
- `worker_heartbeat` row writes (deferred to M5 observability work — spec §10).
- Prometheus metrics emission (M5).
- Solana adapter wiring — `ChainRunner` only constructs `EvmAdapter` for now; the kind dispatch is a TODO comment, not an `if/elif`.

---

### Task 7.1: ConfigWatcher

Drives the worker's view of configuration. Two triggers: (a) a Redis pub/sub message on the `config_changed` channel (low latency); (b) a 5-s poll of `config_version.version` (authoritative fallback if Redis is unreachable). On either trigger it loads a fresh `ConfigSnapshot` and emits it to subscribers via an `asyncio.Queue`. Spec §5.5: "the poll is the authoritative fallback".

**Files:**
- Create: `apps/worker/config_watcher.py`
- Create: `apps/worker/__init__.py` (empty if it doesn't exist)
- Test: `tests/unit/test_config_watcher.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_config_watcher.py
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

import pytest

from apps.worker.config_watcher import ConfigWatcher
from core.config.snapshot import ConfigSnapshot


def _snap(version: int) -> ConfigSnapshot:
    return ConfigSnapshot(version=version, subscriptions=[], channels=[], chains=[])


class _FakeBus:
    """Minimal RedisBus stand-in. `subscribe` yields whatever the test pushes.

    Mirrors the real bus shape: `subscribe()` is NOT awaited by callers — it is
    an async-generator function, so calling it returns the generator directly.
    The real bus yields JSON-decoded dicts (Chunk 3); ConfigWatcher does not
    inspect the payload, so this fake yields strings to keep the test small.
    """

    def __init__(self) -> None:
        self._q: asyncio.Queue[object] = asyncio.Queue()
        self.subscribed_channels: list[str] = []

    async def push(self, msg: object) -> None:
        await self._q.put(msg)

    def subscribe(self, channel: str, *, ready: asyncio.Event | None = None) -> AsyncIterator[object]:
        self.subscribed_channels.append(channel)

        async def _gen() -> AsyncIterator[object]:
            if ready is not None:
                ready.set()
            while True:
                yield await self._q.get()

        return _gen()


class _FakeSessionFactory:
    """Returns the same session-context manager each time. The loader inspects
    a shared `versions` list to decide what version to emit."""

    def __init__(self, versions: list[int]) -> None:
        self.versions = versions
        self.load_calls = 0

    @asynccontextmanager
    async def __call__(self):  # type: ignore[no-untyped-def]
        yield self  # the loader never touches the session in this fake

    async def load_snapshot_fn(self, _session) -> ConfigSnapshot:  # type: ignore[no-untyped-def]
        self.load_calls += 1
        v = self.versions[min(self.load_calls - 1, len(self.versions) - 1)]
        return _snap(v)


@pytest.mark.asyncio
async def test_watcher_emits_initial_snapshot_on_start() -> None:
    bus = _FakeBus()
    factory = _FakeSessionFactory(versions=[1])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus, session_factory=factory, load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=10.0, out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        assert factory.load_calls == 1
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_reloads_on_redis_message() -> None:
    bus = _FakeBus()
    factory = _FakeSessionFactory(versions=[1, 2])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus, session_factory=factory, load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=10.0, out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        # Wait for the subscribe loop to be live before pushing.
        # The fake's `subscribe` always sets `ready` immediately, so a tiny yield is enough.
        await asyncio.sleep(0)
        await bus.push("bump")
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_skips_emit_when_version_unchanged() -> None:
    bus = _FakeBus()
    # Two reloads at v=1 — the watcher must NOT emit the second.
    factory = _FakeSessionFactory(versions=[1, 1, 2])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus, session_factory=factory, load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=10.0, out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1

        await asyncio.sleep(0)
        await bus.push("bump")  # triggers reload → still v=1 → no emit
        await bus.push("bump")  # triggers reload → v=2 → emit
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2
        assert out.qsize() == 0
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_polls_when_bus_is_quiet() -> None:
    bus = _FakeBus()
    factory = _FakeSessionFactory(versions=[1, 2])
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()

    watcher = ConfigWatcher(
        bus=bus, session_factory=factory, load_snapshot=factory.load_snapshot_fn,
        poll_interval_s=0.05, out_queue=out,  # poll fast
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        # No Redis traffic; the 50-ms poll must pick up v=2 within ~200ms.
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2
    finally:
        await watcher.stop()


@pytest.mark.asyncio
async def test_watcher_continues_on_load_error() -> None:
    bus = _FakeBus()

    calls = {"n": 0}

    async def flaky_loader(_session) -> ConfigSnapshot:  # type: ignore[no-untyped-def]
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("db blip")
        # v=1 then (raises) then v=2
        return _snap(1 if calls["n"] == 1 else 2)

    factory = _FakeSessionFactory(versions=[1, 1, 2])  # versions unused; loader overridden
    out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()
    watcher = ConfigWatcher(
        bus=bus, session_factory=factory, load_snapshot=flaky_loader,
        poll_interval_s=0.05, out_queue=out,
    )
    await watcher.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=1.0)
        assert first.version == 1
        second = await asyncio.wait_for(out.get(), timeout=1.0)
        assert second.version == 2  # the flaky middle reload was logged & swallowed
    finally:
        await watcher.stop()
```

- [ ] **Step 2: Run, expect FAIL** (module missing).

```bash
pytest tests/unit/test_config_watcher.py -v
```

- [ ] **Step 3: Implement**

```python
# apps/worker/config_watcher.py
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol

import structlog

from core.config.snapshot import ConfigSnapshot

log = structlog.get_logger(__name__)

_DEFAULT_CHANNEL = "config_changed"


class _Bus(Protocol):
    """Subset of `core.bus.redis_bus.RedisBus` that ConfigWatcher uses."""

    def subscribe(self, channel: str, *, ready: asyncio.Event | None = ...):  # type: ignore[no-untyped-def]
        ...


SessionCM = AbstractAsyncContextManager[object]
SessionFactory = Callable[[], SessionCM]
LoadSnapshotFn = Callable[[object], Awaitable[ConfigSnapshot]]


class ConfigWatcher:
    """Refreshes a `ConfigSnapshot` on Redis `config_changed` events or a periodic
    `config_version.version` poll (whichever fires first).

    Emits the new snapshot onto `out_queue` ONLY when its `version` differs from the
    previously emitted version. This means downstream consumers (the worker main
    loop) see at most one emission per actual configuration change, regardless of
    how many triggers fire.

    Spec §5.5: the 5-s poll is the authoritative fallback if Redis is unreachable.
    """

    def __init__(
        self,
        *,
        bus: _Bus,
        session_factory: SessionFactory,
        load_snapshot: LoadSnapshotFn,
        out_queue: asyncio.Queue[ConfigSnapshot],
        channel: str = _DEFAULT_CHANNEL,
        poll_interval_s: float = 5.0,
    ) -> None:
        self._bus = bus
        self._session_factory = session_factory
        self._load_snapshot = load_snapshot
        self._out = out_queue
        self._channel = channel
        self._poll_interval_s = poll_interval_s
        self._last_version: int | None = None
        self._sub_task: asyncio.Task[None] | None = None
        self._poll_task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._trigger = asyncio.Event()

    async def start(self) -> None:
        # Initial load — must complete before the worker's main loop blocks on `out`.
        await self._reload_and_maybe_emit(reason="initial")
        self._sub_task = asyncio.create_task(self._run_subscriber(), name="config-watcher-sub")
        self._poll_task = asyncio.create_task(self._run_poller(), name="config-watcher-poll")

    async def stop(self) -> None:
        self._stop.set()
        self._trigger.set()  # unblock any pending wait
        for t in (self._sub_task, self._poll_task):
            if t is None:
                continue
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._sub_task = self._poll_task = None

    async def _run_subscriber(self) -> None:
        ready = asyncio.Event()
        gen = self._bus.subscribe(self._channel, ready=ready)
        try:
            await ready.wait()
            async for _msg in gen:
                if self._stop.is_set():
                    break
                await self._reload_and_maybe_emit(reason="redis")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("config_watcher.subscriber_failed")
        finally:
            aclose = getattr(gen, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except Exception:  # noqa: BLE001
                    pass

    async def _run_poller(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval_s)
                    return  # stop fired
                except asyncio.TimeoutError:
                    pass
                await self._reload_and_maybe_emit(reason="poll")
        except asyncio.CancelledError:
            raise

    async def _reload_and_maybe_emit(self, *, reason: str) -> None:
        try:
            async with self._session_factory() as session:
                snap = await self._load_snapshot(session)
        except Exception:  # noqa: BLE001
            log.exception("config_watcher.reload_failed", reason=reason)
            return
        if self._last_version is not None and snap.version == self._last_version:
            return
        self._last_version = snap.version
        await self._out.put(snap)
        log.info("config_watcher.snapshot_emitted", version=snap.version, reason=reason)
```

- [ ] **Step 4: Run** → 5 tests PASS.

```bash
pytest tests/unit/test_config_watcher.py -v
```

- [ ] **Step 5: Commit**

```bash
git add apps/worker/__init__.py apps/worker/config_watcher.py tests/unit/test_config_watcher.py
git commit -m "feat(worker): ConfigWatcher with Redis sub + poll fallback"
```

---

### Task 7.2: ChainRunner — pipeline assembly

Owns one chain's full pipeline. On `start()` it constructs `EvmAdapter → ConfirmationBuffer → ParserPipeline → Matcher → Notifier`, seeds the buffer from the persisted checkpoint, and exposes `run()` as the driver coroutine. On `stop()` it drains in-flight notifications and flushes the checkpoint (spec §9.1).

`apply_snapshot(snap)` is called by the worker main loop whenever the `ConfigWatcher` emits a new snapshot — it rebuilds the `Matcher` index and the `Notifier` channel set in place, without restarting the listener. Chain-level parameters (`rpc_http`/`rpc_ws`/`confirmations`) cannot change without a runner restart; the worker main loop handles that by stopping/starting the runner.

**Files:**
- Create: `apps/worker/chain_runner.py`
- Test: `tests/unit/test_chain_runner.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_chain_runner.py
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from apps.worker.chain_runner import ChainRunner
from core.chains.types import Block, BlockHeader, Tx
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)
from core.notifier.channel import Channel


def _chain() -> SnapshotChain:
    return SnapshotChain(
        id="eth-test", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=2, poll_interval_ms=10,
    )


def _sub(channel_ids: list[str], **overrides: Any) -> SnapshotSubscription:
    base = dict(
        id="s1", name="sub", chain_id="eth-test",
        address=None, abi_id=None,
        match_kind="native_transfer", match_name=None,
        arg_filters={}, enabled=True, channel_ids=channel_ids,
    )
    base.update(overrides)
    return SnapshotSubscription(**base)  # type: ignore[arg-type]


def _ch(id_: str = "c1") -> SnapshotChannel:
    return SnapshotChannel(id=id_, name="hook", type="collect", config={})


class _CollectingChannel(Channel):
    type = "collect"

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def send(self, payload: dict[str, Any]) -> None:
        self.calls.append(payload)


class _FakeAdapter:
    """Stand-in for EvmAdapter. Drives BlockHeaders from a test-controlled queue.

    Matches the EvmAdapter lifecycle from Chunk 3: explicit `connect()` /
    `disconnect()` calls. `fetch_block(n)` is the only I/O ChainRunner needs;
    `subscribe_heads()` returns an unbounded async generator that the test
    cancels via `task.cancel()`.
    """

    chain_id = "eth-test"
    confirmations = 2

    def __init__(self, blocks: list[Block]) -> None:
        self._blocks = {b.header.number: b for b in blocks}
        self._head_q: asyncio.Queue[BlockHeader] = asyncio.Queue()
        self.connected = False

    def add_block(self, block: Block) -> None:
        """Add a block to the fetch-by-number index BEFORE pushing its head."""
        self._blocks[block.header.number] = block

    async def connect(self) -> None:
        self.connected = True

    async def push_head(self, header: BlockHeader) -> None:
        await self._head_q.put(header)

    def subscribe_heads(self) -> AsyncIterator[BlockHeader]:
        async def _gen() -> AsyncIterator[BlockHeader]:
            while True:
                yield await self._head_q.get()
        return _gen()

    async def fetch_block(self, number: int) -> Block:
        return self._blocks[number]

    async def fetch_logs(self, _from: int, _to: int, _addr: list[str] | None = None) -> list[Any]:
        return []

    async def get_latest_block_number(self) -> int:
        return max(self._blocks) if self._blocks else 0

    async def disconnect(self) -> None:
        self.connected = False


def _hdr(n: int, parent: str = "0xp") -> BlockHeader:
    return BlockHeader(number=n, hash=f"0xh{n}", parent_hash=parent, timestamp=n * 10)


def _block_with_native(n: int, value_wei: int, to: str = "0xdead") -> Block:
    return Block(
        header=_hdr(n, parent=f"0xh{n-1}" if n > 0 else "0x0"),
        txs=[Tx(
            hash=f"0xt{n}", index=0, from_addr="0xc0ffee", to_addr=to,
            value=value_wei, input="0x", status=1,
        )],
        logs=[],
    )


class _CheckpointStub:
    """In-memory stand-in for the checkpoint repo."""

    def __init__(self, initial: tuple[int, str] | None = None) -> None:
        self.value = initial
        self.saves: list[tuple[int, str]] = []

    async def get(self, _chain_id: str) -> tuple[int, str] | None:
        return self.value

    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None:
        self.value = (last_block, last_block_hash)
        self.saves.append((last_block, last_block_hash))


@pytest.mark.asyncio
async def test_chain_runner_dispatches_native_transfer_through_pipeline() -> None:
    chain = _chain()
    blocks = [_block_with_native(n, value_wei=10**18) for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1, chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"])],
        channels=[_ch("c1")],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        # block 1 + 2 are inside the confirmation window (depth<2) and not emitted yet.
        # block 3 confirms block 1; block 4 confirms block 2.
        # Expect 2 dispatches.
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        assert {c["event"]["block_number"] for c in coll.calls} == {1, 2}
        assert cp.value == (2, "0xh2")  # last checkpoint = last confirmed block
    finally:
        await runner.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_chain_runner_apply_snapshot_swaps_subscriptions_live() -> None:
    chain = _chain()
    blocks = [_block_with_native(n, value_wei=10**18) for n in (1, 2, 3)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    initial_snap = ConfigSnapshot(
        version=1, chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], enabled=False)],  # disabled at start
        channels=[_ch("c1")],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
    )
    await runner.start(initial_snap)
    task = asyncio.create_task(runner.run())
    try:
        # Push block 1; should NOT dispatch (subscription disabled).
        await adapter.push_head(blocks[0].header)
        await adapter.push_head(blocks[1].header)
        await adapter.push_head(blocks[2].header)
        await asyncio.sleep(0.1)
        assert len(coll.calls) == 0

        # Hot-reload: enable the subscription.
        new_snap = ConfigSnapshot(
            version=2, chains=[chain],
            subscriptions=[_sub(channel_ids=["c1"], enabled=True)],
            channels=[_ch("c1")],
        )
        await runner.apply_snapshot(new_snap)

        # Register block 4 BEFORE pushing its header so fetch_block won't KeyError.
        adapter.add_block(_block_with_native(4, value_wei=10**18))
        await adapter.push_head(_hdr(4, parent="0xh3"))
        # block 2 now sits at depth 2 (tip=4) and will dispatch under the new snapshot.
        # block 1 was already confirmed before the reload and is NOT replayed
        # (apply_snapshot doesn't rewind history — documented contract).
        for _ in range(30):
            if coll.calls:
                break
            await asyncio.sleep(0.02)
        assert any(c["event"]["block_number"] == 2 for c in coll.calls)
    finally:
        await runner.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_chain_runner_seeds_buffer_from_checkpoint() -> None:
    chain = _chain()
    adapter = _FakeAdapter(blocks=[])
    cp = _CheckpointStub(initial=(42, "0xh42"))
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=cp,
    )
    snap = ConfigSnapshot(version=1, chains=[chain], subscriptions=[], channels=[])
    await runner.start(snap)
    try:
        assert runner.resume_from == (42, "0xh42")
    finally:
        await runner.stop()
```

- [ ] **Step 2: Run, expect FAIL** (module missing).

```bash
pytest tests/unit/test_chain_runner.py -v
```

- [ ] **Step 3: Implement**

```python
# apps/worker/chain_runner.py
from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Protocol

import structlog

from core.chains.adapter import ChainAdapter
from core.chains.confirmation_buffer import ConfirmationBuffer, ReorgEvent
from core.chains.types import BlockHeader
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
)
from core.matcher.matcher import Matcher
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.native import NativeTransferParser
from core.parser.pipeline import ParserPipeline

log = structlog.get_logger(__name__)


AdapterFactory = Callable[[SnapshotChain], ChainAdapter]
ChannelFactory = Callable[[SnapshotChannel], Channel]


class _CheckpointRepo(Protocol):
    async def get(self, chain_id: str) -> tuple[int, str] | None: ...
    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None: ...


class ChainRunner:
    """Owns one chain's pipeline.

    Lifecycle:
      1. `start(snap)` — construct adapter (and `await adapter.connect()`),
         confirmation buffer, parser, matcher, notifier; seed `resume_from`
         from the persisted checkpoint.
      2. `run()` — drive `subscribe_heads()` through the buffer, parse + match
         + dispatch each confirmed block, save checkpoint per block.
      3. `apply_snapshot(snap)` — rebuild matcher index + notifier channel set
         in place (no listener restart).
      4. `stop()` — cancel listener, drain in-flight notifications (≤30s),
         disconnect adapter.

    ConfirmationBuffer note (Chunk 4): `handle_new_head` is SYNC and takes a
    SYNC `resolve_parent(n, h) -> BlockHeader`. ChainRunner pre-fetches
    ancestors via async I/O *before* calling the buffer, then passes a cache
    lookup as the sync resolver. Pre-fetch only runs when the new head's
    `parent_hash` doesn't match our mirrored buffer tip.
    """

    DRAIN_TIMEOUT_S = 30.0

    def __init__(
        self,
        *,
        chain: SnapshotChain,
        adapter_factory: AdapterFactory,
        channel_factory: ChannelFactory,
        checkpoint_repo: _CheckpointRepo,
        notifier_max_concurrency: int = 50,
    ) -> None:
        self._chain = chain
        self._adapter_factory = adapter_factory
        self._channel_factory = channel_factory
        self._cp = checkpoint_repo
        self._notifier_max_concurrency = notifier_max_concurrency

        self._adapter: ChainAdapter | None = None
        self._buffer: ConfirmationBuffer | None = None
        self._pipeline = ParserPipeline([NativeTransferParser(chain_id=self._chain.id)])
        self._matcher: Matcher | None = None
        self._notifier: Notifier | None = None
        self._current_snap: ConfigSnapshot | None = None
        self._buffer_tip_hash: str | None = None  # mirrors buffer's rightmost header
        self._stop = asyncio.Event()
        self._snap_lock = asyncio.Lock()
        self.resume_from: tuple[int, str] | None = None

    async def start(self, snap: ConfigSnapshot) -> None:
        self._adapter = self._adapter_factory(self._chain)
        # EvmAdapter (Chunk 3) requires an explicit connect() before any RPC call.
        connect = getattr(self._adapter, "connect", None)
        if callable(connect):
            await connect()
        self._buffer = ConfirmationBuffer(confirmations=self._chain.confirmations)
        self.resume_from = await self._cp.get(self._chain.id)
        if self.resume_from is not None:
            log.info(
                "chain_runner.resuming_from_checkpoint",
                chain_id=self._chain.id,
                last_block=self.resume_from[0],
                last_block_hash=self.resume_from[1],
            )
        self._matcher = Matcher(snap)
        self._notifier = Notifier(
            channel_factory=self._channel_factory,
            max_concurrency=self._notifier_max_concurrency,
        )
        await self._notifier.start(snap.channels)
        self._current_snap = snap

    async def apply_snapshot(self, snap: ConfigSnapshot) -> None:
        async with self._snap_lock:
            assert self._notifier is not None
            self._matcher = Matcher(snap)
            # For M1 the cheap path is stop-then-start; HttpChannel instances
            # are cheap. M4 (MQ) will need a diff to avoid bouncing live AMQP
            # connections.
            await self._notifier.stop()
            self._notifier = Notifier(
                channel_factory=self._channel_factory,
                max_concurrency=self._notifier_max_concurrency,
            )
            await self._notifier.start(snap.channels)
            self._current_snap = snap
            log.info(
                "chain_runner.snapshot_applied",
                chain_id=self._chain.id, version=snap.version,
            )

    async def run(self) -> None:
        assert self._adapter is not None and self._buffer is not None
        try:
            async for header in self._adapter.subscribe_heads():
                if self._stop.is_set():
                    break
                await self._handle_head(header)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("chain_runner.run_failed", chain_id=self._chain.id)
            raise

    async def _handle_head(self, header: BlockHeader) -> None:
        assert self._buffer is not None and self._adapter is not None
        # Pre-fetch ancestors only when the head doesn't link cleanly. The
        # buffer's sync resolver then becomes a pure dict lookup.
        cache: dict[str, BlockHeader] = {}
        if self._buffer_tip_hash is not None and self._buffer_tip_hash != header.parent_hash:
            cache = await self._prefetch_ancestors_for(header)

        def resolve_parent(n: int, h: str) -> BlockHeader:
            try:
                return cache[h]
            except KeyError as e:
                # Buffer treats a missing ancestor as a deep reorg (exhausts walk).
                # We translate to KeyError for clarity in logs.
                raise KeyError(f"ancestor {h} at height {n} not in prefetch cache") from e

        result = self._buffer.handle_new_head(header, resolve_parent=resolve_parent)
        self._buffer_tip_hash = header.hash

        confirmed: list[BlockHeader]
        if isinstance(result, ReorgEvent):
            if result.deep:
                log.error(
                    "chain_runner.deep_reorg",
                    chain_id=self._chain.id,
                    divergent_oldest=result.divergent_oldest,
                    new_head=result.new_head.number if result.new_head else None,
                )
            confirmed = result.confirmed
        else:
            confirmed = result  # list[BlockHeader]

        for h in confirmed:
            await self._process_confirmed_block(h.number)

    async def _prefetch_ancestors_for(self, header: BlockHeader) -> dict[str, BlockHeader]:
        """Fetch up to `confirmations + 1` blocks at the heights below `header`
        and index them by hash. The RPC node has typically already reorged, so
        `fetch_block(n)` returns the new fork's header at height `n`.
        """
        assert self._adapter is not None
        depth = max(1, self._chain.confirmations + 1)
        out: dict[str, BlockHeader] = {}
        for i in range(1, depth + 1):
            n = header.number - i
            if n < 0:
                break
            try:
                blk = await self._adapter.fetch_block(n)
            except Exception:  # noqa: BLE001
                log.warning("chain_runner.prefetch_failed", chain_id=self._chain.id, height=n)
                break
            out[blk.header.hash] = blk.header
        return out

    async def _process_confirmed_block(self, number: int) -> None:
        assert self._adapter is not None and self._matcher is not None
        assert self._notifier is not None
        block = await self._adapter.fetch_block(number)
        events = list(self._pipeline.run(block))
        for event in events:
            hits = [(sub, chans) for sub, chans in self._matcher.match(event) if chans]
            if not hits:
                continue
            await self._notifier.dispatch(event, hits)
        await self._cp.save(self._chain.id, block.header.number, block.header.hash)

    async def stop(self) -> None:
        self._stop.set()
        if self._notifier is not None:
            try:
                await asyncio.wait_for(self._notifier.stop(), timeout=self.DRAIN_TIMEOUT_S)
            except asyncio.TimeoutError:
                log.warning("chain_runner.notifier_drain_timeout", chain_id=self._chain.id)
        if self._adapter is not None:
            try:
                await self._adapter.disconnect()
            except Exception:  # noqa: BLE001
                log.exception("chain_runner.adapter_disconnect_failed", chain_id=self._chain.id)
```

- [ ] **Step 4: Run** → 3 tests PASS.

```bash
pytest tests/unit/test_chain_runner.py -v
```

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(worker): ChainRunner — per-chain pipeline orchestrator"
git tag m1-chunk7-watcher-runner
```

---

## Chunk 8: Worker entrypoint + hot-reload integration test

Wraps the worker process: a single asyncio entrypoint that owns DB, Redis bus, `ConfigWatcher`, and one `ChainRunner` per enabled chain. Handles SIGTERM/SIGINT per spec §9.1 (drain, then exit). The chunk closes with one integration test that boots the watcher against the SQLite-backed `db` fixture + a Redis testcontainer and asserts hot-reload via both the Redis bump path and the poll fallback.

This chunk depends on Chunk 7's `ConfigWatcher` and `ChainRunner`, plus the `Database`, `RedisBus`, and `ConfigVersionRepo` from earlier chunks. After this chunk, the worker process is runnable end-to-end (subject only to having a real RPC endpoint configured), and the M1 worker side is complete.

---

### Task 8.1: Worker entrypoint (asyncio main + signal handling)

The process entry point. Owns the global state: DB engine, Redis bus, `ConfigWatcher`, and one `ChainRunner` per enabled chain. On SIGTERM/SIGINT it triggers graceful shutdown per spec §9.1:

1. Stop accepting new heads (cancel each runner's `run()` task).
2. Drain in-flight notifications (each runner's `stop()` waits up to 30s).
3. Flush checkpoints (already saved per-block; nothing extra needed for M1).
4. Close DB pool + Redis connection.
5. Exit.

The main loop watches the `ConfigWatcher`'s output queue and reconciles `ChainRunner`s against the new snapshot: starts new runners for newly-enabled chains, stops removed/disabled ones, and calls `apply_snapshot` on the survivors.

**Files:**
- Create: `apps/worker/main.py`

- [ ] **Step 1: Implement**

```python
# apps/worker/main.py
from __future__ import annotations

import asyncio
import signal

import structlog

from apps.worker.chain_runner import ChainRunner
from apps.worker.config_watcher import ConfigWatcher
from core.bus.redis_bus import RedisBus
from core.chains.evm import EvmAdapter
from core.config.db import Database
from core.config.repositories import CheckpointRepo
from core.config.snapshot import ConfigSnapshot, SnapshotChain, SnapshotChannel, load_snapshot
from core.logging import configure_logging
from core.notifier.channel import CHANNEL_REGISTRY, Channel
from core.notifier.http import HttpChannel  # noqa: F401 — side-effect: register http
from core.settings import Settings, load_settings

log = structlog.get_logger(__name__)


def _default_adapter_factory(cfg: SnapshotChain) -> EvmAdapter:
    if cfg.kind != "evm":
        # M3 (Solana) will branch on `cfg.kind` here; M1 hard-fails so misconfigs are loud.
        raise NotImplementedError(f"chain kind {cfg.kind!r} not supported in M1")
    # NOTE: `poll_interval_ms` from SnapshotChain is NOT wired through — Chunk 3's
    # EvmAdapter hard-codes a 1s HTTP poll fallback. Plumbing the per-chain value
    # is a follow-up (tracked as an M2 task; not blocking).
    return EvmAdapter(
        chain_id=cfg.id,
        rpc_http=cfg.rpc_http,
        rpc_ws=cfg.rpc_ws,
        confirmations=cfg.confirmations,
    )


def _default_channel_factory(cfg: SnapshotChannel) -> Channel:
    cls = CHANNEL_REGISTRY[cfg.type]
    return cls(config=cfg.config)  # type: ignore[call-arg]


class _CheckpointAdapter:
    """Bridges the ChainRunner's `(get/save) -> tuple[int,str]` contract to
    Chunk 2's `CheckpointRepo` (returns ORM row, uses `upsert`, no commit).
    Opens its own session per call so multiple runners don't share one.
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    async def get(self, chain_id: str) -> tuple[int, str] | None:
        async with self._db.session() as s:
            row = await CheckpointRepo(s).get(chain_id)
            if row is None:
                return None
            return (row.last_block, row.last_block_hash)

    async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None:
        async with self._db.session() as s:
            await CheckpointRepo(s).upsert(
                chain_id, last_block=last_block, last_block_hash=last_block_hash,
            )
            await s.commit()


class _Worker:
    """Holds the shared DB / bus / watcher and a map of chain_id → (runner, task)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._db = Database(settings.database.url)
        self._bus = RedisBus(url=settings.redis.url)
        self._checkpoint_adapter = _CheckpointAdapter(self._db)
        self._snap_queue: asyncio.Queue[ConfigSnapshot] = asyncio.Queue(maxsize=8)
        self._watcher: ConfigWatcher | None = None
        self._runners: dict[str, tuple[ChainRunner, asyncio.Task[None]]] = {}
        self._stop = asyncio.Event()

    async def start(self) -> None:
        await self._db.connect()
        await self._bus.connect()
        self._watcher = ConfigWatcher(
            bus=self._bus,
            session_factory=self._db.session,
            load_snapshot=load_snapshot,
            out_queue=self._snap_queue,
            poll_interval_s=5.0,
        )
        await self._watcher.start()

    async def run(self) -> None:
        """Main loop: dequeue snapshots, reconcile runners, exit on _stop."""
        while not self._stop.is_set():
            snap = await self._dequeue_snapshot_or_stop()
            if snap is None:
                return
            await self._reconcile(snap)

    async def _dequeue_snapshot_or_stop(self) -> ConfigSnapshot | None:
        get_task = asyncio.create_task(self._snap_queue.get())
        stop_task = asyncio.create_task(self._stop.wait())
        try:
            done, _ = await asyncio.wait(
                {get_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in done:
                get_task.cancel()
                return None
            stop_task.cancel()
            return get_task.result()
        finally:
            for t in (get_task, stop_task):
                if not t.done():
                    t.cancel()
    async def _reconcile(self, snap: ConfigSnapshot) -> None:
        enabled = {c.id: c for c in snap.chains}
        for chain_id in list(self._runners):
            if chain_id not in enabled:
                await self._stop_runner(chain_id)
        for chain_id, cfg in enabled.items():
            if chain_id in self._runners:
                runner, _ = self._runners[chain_id]
                await runner.apply_snapshot(snap)
            else:
                runner = ChainRunner(
                    chain=cfg,
                    adapter_factory=_default_adapter_factory,
                    channel_factory=_default_channel_factory,
                    checkpoint_repo=self._checkpoint_adapter,
                )
                await runner.start(snap)
                task = asyncio.create_task(runner.run(), name=f"chain-runner:{chain_id}")
                self._runners[chain_id] = (runner, task)
                log.info("worker.chain_runner_started", chain_id=chain_id)

    async def _stop_runner(self, chain_id: str) -> None:
        runner, task = self._runners.pop(chain_id)
        await runner.stop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        log.info("worker.chain_runner_stopped", chain_id=chain_id)

    async def shutdown(self) -> None:
        """Trigger graceful drain per spec §9.1. Idempotent."""
        if self._stop.is_set():
            return
        self._stop.set()
        log.info("worker.shutdown_starting")
        if self._watcher is not None:
            await self._watcher.stop()
        for chain_id in list(self._runners):
            await self._stop_runner(chain_id)
        await self._bus.disconnect()
        await self._db.disconnect()
        log.info("worker.shutdown_complete")


async def run_worker(settings: Settings, stop_event: asyncio.Event) -> None:
    """Public coroutine that boots a `_Worker` and runs until `stop_event` is set.

    Does NOT install signal handlers — the caller is responsible for triggering
    `stop_event` (E2E tests do this directly; the CLI entry point does it from
    SIGTERM/SIGINT handlers via `_amain` below).
    """
    worker = _Worker(settings)
    await worker.start()
    run_task = asyncio.create_task(worker.run(), name="worker-main-loop")
    stop_task = asyncio.create_task(stop_event.wait(), name="worker-stop-wait")
    try:
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop_task.cancel()
        await worker.shutdown()
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass


async def _amain() -> None:
    settings = load_settings()
    configure_logging(level=settings.logging.level, format=settings.logging.format)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("worker.signal_received", signal=sig.name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _request_shutdown, sig)

    await run_worker(settings, stop_event)


def main() -> None:
    """Console-script entry point (referenced from pyproject.toml `[project.scripts]`)."""
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Register the entry point**

Modify `pyproject.toml` to add (under `[project.scripts]`):

```toml
[project.scripts]
chain-indexer-worker = "apps.worker.main:main"
chain-indexer-web    = "apps.web.main:main"  # populated in Chunk 9
```

- [ ] **Step 3: Smoke-test the import path**

```bash
python -c "from apps.worker.main import main; print('ok')"
```
Expected output: `ok`. Ensures all transitive imports resolve (no circular imports introduced).

- [ ] **Step 4: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/main.py pyproject.toml
git commit -m "feat(worker): asyncio entrypoint with signal handling"
```

---

### Task 8.2: Integration test — worker observes config bump

Wires `ConfigWatcher` end-to-end against the SQLite-backed `db` fixture from Chunk 2 plus a Redis testcontainer. Bumps `config_version` from a separate session and asserts the watcher emits.

**Files:**
- Create: `tests/conftest.py` (top-level conftest — exposes `redis_url` fixture to both `tests/integration/` and `tests/e2e/`; introduced here because this is the first test that needs a real Redis)
- Create: `tests/integration/test_worker_config_reload.py`

- [ ] **Step 1: Add a top-level conftest with a Redis testcontainer fixture**

The existing `tests/integration/conftest.py` from Chunk 2 Task 2.4 only exposes `db` (SQLite in-memory). Create a NEW `tests/conftest.py` at the `tests/` root so the `redis_url` fixture is discoverable by every subtree (`tests/integration/`, `tests/e2e/`). Function-scoped to keep each test isolated (no leaked pub/sub state); M1 has a single-digit count of Redis-using tests, so the per-test container start cost is acceptable.

```python
# tests/conftest.py — NEW FILE
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from testcontainers.redis import RedisContainer


@pytest_asyncio.fixture(scope="function")
async def redis_url() -> AsyncIterator[str]:
    """Redis testcontainer URL.

    Lives at `tests/conftest.py` (not `tests/integration/conftest.py`) so
    `tests/e2e/` can consume it without duplicate fixture definitions.
    """
    with RedisContainer("redis:7-alpine") as rc:
        host = rc.get_container_host_ip()
        port = rc.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"
```

Also ensure `testcontainers[redis]` is in the dev dependency group. Chunk 1 Task 1.2 already pins `testcontainers`; if `from testcontainers.redis import RedisContainer` raises `ImportError`, add the extra to `pyproject.toml`:

```toml
# pyproject.toml — under [project.optional-dependencies].dev
"testcontainers[redis]>=4,<5",
```

- [ ] **Step 2: Write the test**

```python
# tests/integration/test_worker_config_reload.py
from __future__ import annotations

import asyncio

import pytest

from apps.worker.config_watcher import ConfigWatcher
from core.bus.redis_bus import RedisBus
from core.config.repositories import ConfigVersionRepo
from core.config.snapshot import ConfigSnapshot, load_snapshot


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_bump_reloads_snapshot(db, redis_url) -> None:
    """db fixture: connected `Database` from tests/integration/conftest.py.
    redis_url: testcontainer URL fixture from same conftest.
    """
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()
        watcher = ConfigWatcher(
            bus=bus, session_factory=db.session, load_snapshot=load_snapshot,
            out_queue=out, poll_interval_s=30.0,  # rely on redis, not poll
        )
        await watcher.start()
        try:
            first = await asyncio.wait_for(out.get(), timeout=2.0)
            v0 = first.version

            async with db.session() as s:
                await ConfigVersionRepo(s).bump()
                await s.commit()
            await bus.publish("config_changed", {"reason": "bump"})

            second = await asyncio.wait_for(out.get(), timeout=2.0)
            assert second.version == v0 + 1
        finally:
            await watcher.stop()
    finally:
        await bus.disconnect()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poll_fallback_reloads_when_redis_silent(db, redis_url) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        out: asyncio.Queue[ConfigSnapshot] = asyncio.Queue()
        watcher = ConfigWatcher(
            bus=bus, session_factory=db.session, load_snapshot=load_snapshot,
            out_queue=out, poll_interval_s=0.2,  # poll fast
        )
        await watcher.start()
        try:
            first = await asyncio.wait_for(out.get(), timeout=2.0)
            v0 = first.version

            async with db.session() as s:
                await ConfigVersionRepo(s).bump()
                await s.commit()
            # Do NOT publish — poll must pick it up.

            second = await asyncio.wait_for(out.get(), timeout=2.0)
            assert second.version == v0 + 1
        finally:
            await watcher.stop()
    finally:
        await bus.disconnect()
```

- [ ] **Step 3: Run**

```bash
pytest tests/integration/test_worker_config_reload.py -v -m integration
```
Expected: 2 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/conftest.py tests/integration/test_worker_config_reload.py pyproject.toml
git commit -m "test(worker): integration test for hot-reload (Redis + poll fallback)"
git tag m1-chunk8-entrypoint
```

---



## Chunk 9: Web API — App skeleton + chains and channels routers

Adds the management surface scaffolding plus the two simpler resource routers. The FastAPI app, Pydantic schemas, DI helpers, healthz endpoint, the shared `bump_and_publish` helper, and the chains + channels routers all land in this chunk. The subscriptions router and the full DB+Redis integration test follow in Chunk 10 — splitting the API across two chunks keeps each under the 1000-line cap.

Every write router persists through the Chunk 2 repositories and — critically — bumps `config_version` + publishes `config_changed` to Redis after every successful write so the worker's `ConfigWatcher` (Chunk 7) reloads within 5 s (spec §5.5).

**New files this chunk:**
- `apps/web/__init__.py` (empty)
- `apps/web/main.py` — FastAPI app factory + lifespan (opens DB pool + Redis bus)
- `apps/web/deps.py` — DI helpers (db, bus, session)
- `apps/web/schemas.py` — Pydantic v2 request/response models (chains, channels, subscriptions, bind request)
- `apps/web/routers/__init__.py` (empty)
- `apps/web/routers/_common.py` — `bump_and_publish` helper
- `apps/web/routers/chains.py`
- `apps/web/routers/channels.py`
- `apps/web/routers/subscriptions.py` — **placeholder only** in this chunk; the real router lands in Chunk 10 Task 10.1.
- `tests/unit/test_web_app.py`, `tests/unit/test_web_chains.py`, `tests/unit/test_web_channels.py`

**Scope notes:**
- M1 only verifies the `native_transfer` + `http` combination end-to-end, but the API accepts the full spec §7 schema (`match_kind ∈ {native_transfer, token_transfer, event, call}`, `channel.type ∈ {mq, http, ws}`). The worker silently ignores subscriptions/channels it can't yet handle; later milestones add their parsers/drivers without API churn.
- No authentication (spec §2 non-goal). No DELETE endpoints in M1 — operators can drop rows via DB if needed; the E2E test only needs create + read.
- WS server (`/ws`) and static UI are M4/M5; not in this chunk.
- `ChainRepo.list_enabled()` is the only list method on chains in Chunk 2, so `GET /api/chains` only returns enabled chains in M1. Listing disabled chains is an M2 admin task.

**Cross-chunk dependencies (signatures must match exactly):**
- `core.config.db.Database(url)` with `connect()`, `disconnect()`, `session()` async ctx.
- `core.config.repositories.{ChainRepo, ChannelRepo, ConfigVersionRepo}` — see Chunk 2 (Task 2.4) for exact constructor/method signatures.
- `core.config.models.{ChainKind, ChannelType, MatchKind}` enums.
- `core.bus.redis_bus.RedisBus(url=...)` with `connect()`, `disconnect()`, `publish(channel, payload: dict)`.
- `core.settings.Settings` (lazy-loaded via `core.settings.load_settings`).

---

### Task 9.1: Pydantic schemas + FastAPI app factory + deps + healthz

Lays down the Pydantic request/response shapes that the routers will use, the FastAPI `create_app()` factory with a lifespan that opens DB + bus, three injectable dependencies (`get_db`, `get_bus`, `get_session`), and a `GET /healthz` endpoint that pings both DB and Redis. This task is the "scaffolding" — no business routes yet.

**Files:**
- Create: `apps/web/__init__.py` (empty)
- Create: `apps/web/schemas.py`
- Create: `apps/web/deps.py`
- Create: `apps/web/main.py`
- Create: `apps/web/routers/__init__.py` (empty)
- Test: `tests/unit/test_web_app.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_web_app.py
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database
from core.config.models import Base


class _FakeBus:
    """Stand-in for RedisBus exposing the methods deps/healthz touch."""

    def __init__(self, ping_ok: bool = True) -> None:
        self.ping_ok = ping_ok
        self.published: list[tuple[str, dict]] = []

    async def ping(self) -> bool:
        return self.ping_ok

    async def publish(self, channel: str, payload: dict) -> None:
        self.published.append((channel, payload))


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db: Database, bus: _FakeBus) -> TestClient:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def test_healthz_ok(db: Database) -> None:
    bus = _FakeBus(ping_ok=True)
    with _client(db, bus) as c:
        r = c.get("/healthz")
    assert r.status_code == 200
    body = r.json()
    assert body == {"db": "ok", "redis": "ok"}


def test_healthz_reports_redis_failure(db: Database) -> None:
    bus = _FakeBus(ping_ok=False)
    with _client(db, bus) as c:
        r = c.get("/healthz")
    assert r.status_code == 503
    assert r.json()["redis"] == "fail"
```

- [ ] **Step 2: Run, expect FAIL**

```bash
pytest tests/unit/test_web_app.py -v
```
Expected: FAIL — modules missing.

- [ ] **Step 3: Implement schemas**

```python
# apps/web/schemas.py
"""Request/response models for the management API.

Schemas mirror the DB rows but are decoupled from ORM types so we can evolve
the wire format independently. UUID fields are strings (`uuid.uuid4()` is
already stringified in the ORM defaults — Chunk 2 Task 2.2).
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


# ---- Chains ---------------------------------------------------------------


class ChainCreate(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    kind: Literal["evm", "solana"]
    rpc_http: str = Field(min_length=1)
    rpc_ws: str | None = None
    confirmations: int = Field(ge=0, le=10_000)
    poll_interval_ms: int = Field(ge=100, le=60_000)
    enabled: bool = True


class ChainOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    kind: str  # serialized enum value
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int
    enabled: bool


# ---- Channels -------------------------------------------------------------


class ChannelCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    type: Literal["mq", "http", "ws"]
    config: dict[str, Any] = Field(default_factory=dict)


class ChannelOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    type: str
    config: dict[str, Any]


# ---- Subscriptions --------------------------------------------------------


class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    chain_id: str
    address: str | None = None
    abi_id: str | None = None
    match_kind: Literal["native_transfer", "token_transfer", "event", "call"]
    match_name: str | None = None
    arg_filters: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    chain_id: str
    address: str | None
    abi_id: str | None
    match_kind: str
    match_name: str | None
    arg_filters: dict[str, Any]
    enabled: bool


class ChannelBindRequest(BaseModel):
    channel_id: str
```

- [ ] **Step 4: Implement deps**

```python
# apps/web/deps.py
"""FastAPI dependency providers.

These read from `app.state` (populated by `lifespan` in `apps/web/main.py`).
Tests override via `app.dependency_overrides[get_db] = ...`.
"""
from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from core.bus.redis_bus import RedisBus
from core.config.db import Database


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_bus(request: Request) -> RedisBus:
    return request.app.state.bus


async def get_session(
    db: Database = Depends(get_db),
) -> AsyncIterator[AsyncSession]:
    async with db.session() as s:
        yield s
```

- [ ] **Step 5: Implement app factory + lifespan + healthz**

```python
# apps/web/main.py
"""FastAPI app factory.

Lifespan opens the DB pool + Redis bus and attaches them to `app.state`.
Routers (added in Tasks 9.2–9.4 and Chunk 10 Task 10.1) live under
`apps/web/routers/` and are included here.

Tests construct `create_app(lifespan=None)` so the lifespan does NOT attempt
real DB/Redis connections; they override `get_db` / `get_bus` via
`app.dependency_overrides` and use `TestClient(app)` WITHOUT the `with`
context manager (which is what would otherwise trigger the lifespan).
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable

from fastapi import Depends, FastAPI
from fastapi.responses import JSONResponse
from sqlalchemy import text

from apps.web.deps import get_bus, get_db
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.settings import load_settings

log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    db = Database(settings.database.url)
    bus = RedisBus(url=settings.redis.url)
    await db.connect()
    await bus.connect()
    app.state.db = db
    app.state.bus = bus
    try:
        yield
    finally:
        await bus.disconnect()
        await db.disconnect()


_LIFESPAN_SENTINEL = object()


def create_app(*, lifespan: Callable | None | object = _LIFESPAN_SENTINEL) -> FastAPI:
    """Build the FastAPI app.

    `lifespan` defaults to the real lifespan defined above. Pass
    `lifespan=None` from tests to bypass DB/Redis startup; pair that with
    `app.dependency_overrides[get_db|get_bus] = ...` so the routes never read
    `app.state`.
    """
    lifespan_arg = globals()["lifespan"] if lifespan is _LIFESPAN_SENTINEL else lifespan
    app = FastAPI(title="chain-indexer", lifespan=lifespan_arg)

    @app.get("/healthz")
    async def healthz(
        db: Database = Depends(get_db),
        bus: RedisBus = Depends(get_bus),
    ) -> JSONResponse:
        db_ok = True
        try:
            async with db.session() as s:
                await s.execute(text("SELECT 1"))
        except Exception:  # pragma: no cover (covered by integration test)
            db_ok = False

        redis_ok = await bus.ping()  # RedisBus.ping() swallows errors itself (Chunk 3)

        body = {"db": "ok" if db_ok else "fail", "redis": "ok" if redis_ok else "fail"}
        status_code = 200 if db_ok and redis_ok else 503
        return JSONResponse(body, status_code=status_code)

    # Routers (registered in later tasks of this chunk):
    from apps.web.routers import chains as chains_router  # noqa: E402
    from apps.web.routers import channels as channels_router  # noqa: E402
    from apps.web.routers import subscriptions as subs_router  # noqa: E402

    app.include_router(chains_router.router)
    app.include_router(channels_router.router)
    app.include_router(subs_router.router)
    return app


def main() -> None:  # pragma: no cover — used by the `chain-indexer-web` entrypoint
    import uvicorn

    settings = load_settings()
    uvicorn.run("apps.web.main:create_app", factory=True,
                host=settings.web.host, port=settings.web.port)
```

Note on the deferred-router import: `create_app()` references `apps.web.routers.{chains,channels,subscriptions}` inside the function body so the implementation order Task 9.1 → 9.2 → 9.3 → 9.4 can land router files one at a time and each task's tests run by stubbing the modules. Tasks 9.2–9.4 each create the router file and re-run the test suite; the import succeeds once those files exist.

Until those router files exist, this Task 9.1 test imports `create_app` and exercises `/healthz`. Because the routers are imported inside `create_app()`, the test will fail at app construction until 9.2–9.4 are in place. To unblock Task 9.1 specifically, **create empty placeholder router files in Step 6 below** so that `create_app()` can be constructed; 9.2/9.3/9.4 then replace each placeholder with the real router.

- [ ] **Step 6: Create placeholder router modules**

```python
# apps/web/routers/chains.py
from fastapi import APIRouter
router = APIRouter(prefix="/api/chains", tags=["chains"])
```

```python
# apps/web/routers/channels.py
from fastapi import APIRouter
router = APIRouter(prefix="/api/channels", tags=["channels"])
```

```python
# apps/web/routers/subscriptions.py
from fastapi import APIRouter
router = APIRouter(prefix="/api/subscriptions", tags=["subscriptions"])
```

Each placeholder will be REPLACED in its respective Task (9.2 / 9.3 / 9.4).

- [ ] **Step 7: Run the test — expect PASS**

```bash
pytest tests/unit/test_web_app.py -v
```
Expected: 2 PASS (`test_healthz_ok`, `test_healthz_reports_redis_failure`).

- [ ] **Step 8: Commit**

```bash
git add apps/web/__init__.py apps/web/schemas.py apps/web/deps.py apps/web/main.py apps/web/routers/__init__.py apps/web/routers/chains.py apps/web/routers/channels.py apps/web/routers/subscriptions.py tests/unit/test_web_app.py
git commit -m "feat(web): FastAPI app factory + lifespan + healthz + schemas"
```

---

### Task 9.2: Chains router (POST + GET)

`POST /api/chains` creates a chain row, bumps `config_version`, publishes `config_changed` to Redis, and returns 201. `GET /api/chains` returns enabled chains. `GET /api/chains/{id}` returns one (404 on miss). Internal helper `_bump_and_publish(s, bus)` centralizes the "commit + version bump + publish" sequence — reused by Tasks 9.3 and 9.4.

**Files:**
- Create: `apps/web/routers/_common.py` (the `_bump_and_publish` helper)
- Modify: `apps/web/routers/chains.py` (replace placeholder)
- Test: `tests/unit/test_web_chains.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_web_chains.py
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database
from core.config.models import Base


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def ping(self) -> bool:
        return True

    async def publish(self, channel: str, payload: dict) -> None:
        self.published.append((channel, payload))


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db: Database, bus: _FakeBus) -> TestClient:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def test_create_chain_persists_and_publishes(db: Database) -> None:
    bus = _FakeBus()
    payload = {
        "id": "eth-mainnet",
        "kind": "evm",
        "rpc_http": "http://localhost:8545",
        "rpc_ws": None,
        "confirmations": 12,
        "poll_interval_ms": 3000,
        "enabled": True,
    }
    with _client(db, bus) as c:
        r = c.post("/api/chains", json=payload)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["id"] == "eth-mainnet"
    assert body["kind"] == "evm"
    assert len(bus.published) == 1
    ch, msg = bus.published[0]
    assert ch == "config_changed"
    assert msg["entity"] == "chain"
    assert msg["id"] == "eth-mainnet"


def test_list_chains_returns_only_enabled(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        c.post("/api/chains", json={
            "id": "eth-mainnet", "kind": "evm", "rpc_http": "x", "rpc_ws": None,
            "confirmations": 12, "poll_interval_ms": 3000, "enabled": True,
        })
        c.post("/api/chains", json={
            "id": "bsc", "kind": "evm", "rpc_http": "y", "rpc_ws": None,
            "confirmations": 15, "poll_interval_ms": 3000, "enabled": False,
        })
        r = c.get("/api/chains")
    assert r.status_code == 200
    ids = sorted(x["id"] for x in r.json())
    assert ids == ["eth-mainnet"]


def test_get_chain_by_id_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.get("/api/chains/nope")
    assert r.status_code == 404


def test_create_chain_invalid_kind_400(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/chains", json={
            "id": "x", "kind": "doge", "rpc_http": "z", "rpc_ws": None,
            "confirmations": 1, "poll_interval_ms": 1000, "enabled": True,
        })
    assert r.status_code == 422  # pydantic validation
```

- [ ] **Step 2: Run, expect FAIL**

```bash
pytest tests/unit/test_web_chains.py -v
```
Expected: FAIL — endpoints missing.

- [ ] **Step 3: Implement the shared helper**

```python
# apps/web/routers/_common.py
"""Shared helpers used by every write router.

`bump_and_publish` is the single place that increments `config_version` and
fires `config_changed` to Redis. Routers MUST call it after every successful
mutation so the worker's `ConfigWatcher` (Chunk 7) refreshes its snapshot
within 5 s (spec §5.5).
"""
from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from core.bus.redis_bus import RedisBus
from core.config.repositories import ConfigVersionRepo

log = logging.getLogger(__name__)


async def bump_and_publish(
    session: AsyncSession,
    bus: RedisBus,
    *,
    entity: str,
    entity_id: str,
    action: str,
) -> int:
    """Bump config_version (in the same transaction as the caller's write),
    commit, then publish a `config_changed` notification.

    Returns the new version. The publish is best-effort: if Redis is down the
    worker still picks up the change via its 5-s poll (spec §5.5).
    """
    new_version = await ConfigVersionRepo(session).bump()
    await session.commit()
    try:
        await bus.publish(
            "config_changed",
            {"entity": entity, "id": entity_id, "action": action, "version": new_version},
        )
    except Exception as exc:  # noqa: BLE001 — Redis publish is best-effort
        # Poll fallback covers this — see spec §5.5.
        log.warning("config_changed publish failed: %r", exc)
    return new_version
```

- [ ] **Step 4: Implement the chains router**

```python
# apps/web/routers/chains.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import ChainCreate, ChainOut
from core.bus.redis_bus import RedisBus
from core.config.models import ChainKind
from core.config.repositories import ChainRepo

router = APIRouter(prefix="/api/chains", tags=["chains"])


@router.post("", response_model=ChainOut, status_code=status.HTTP_201_CREATED)
async def create_chain(
    payload: ChainCreate,
    session: AsyncSession = Depends(get_session),
    bus: RedisBus = Depends(get_bus),
) -> ChainOut:
    repo = ChainRepo(session)
    if await repo.get(payload.id) is not None:
        raise HTTPException(status_code=409, detail="chain id already exists")
    row = await repo.create(
        id=payload.id,
        kind=ChainKind(payload.kind),
        rpc_http=payload.rpc_http,
        rpc_ws=payload.rpc_ws,
        confirmations=payload.confirmations,
        poll_interval_ms=payload.poll_interval_ms,
        enabled=payload.enabled,
    )
    await bump_and_publish(session, bus, entity="chain", entity_id=row.id, action="create")
    return ChainOut.model_validate(row)


@router.get("", response_model=list[ChainOut])
async def list_chains(
    session: AsyncSession = Depends(get_session),
) -> list[ChainOut]:
    rows = await ChainRepo(session).list_enabled()
    return [ChainOut.model_validate(r) for r in rows]


@router.get("/{chain_id}", response_model=ChainOut)
async def get_chain(
    chain_id: str,
    session: AsyncSession = Depends(get_session),
) -> ChainOut:
    row = await ChainRepo(session).get(chain_id)
    if row is None:
        raise HTTPException(status_code=404, detail="chain not found")
    return ChainOut.model_validate(row)
```

- [ ] **Step 5: Run, expect PASS**

```bash
pytest tests/unit/test_web_chains.py -v
```
Expected: 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add apps/web/routers/_common.py apps/web/routers/chains.py tests/unit/test_web_chains.py
git commit -m "feat(web): chains router (POST/GET) + bump_and_publish helper"
```

---

### Task 9.3: Channels router (POST + GET)

`POST /api/channels` accepts any of `mq`/`http`/`ws` (full spec schema) but, in M1, only `http` channels will actually be exercised by the worker. Validation of the inner `config` payload is **shape-only at the API layer** — the channel driver itself is responsible for stricter validation when constructed (Chunk 6). This keeps the API stable across milestones.

**Files:**
- Modify: `apps/web/routers/channels.py` (replace placeholder)
- Test: `tests/unit/test_web_channels.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_web_channels.py
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database
from core.config.models import Base


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def ping(self) -> bool:
        return True

    async def publish(self, channel: str, payload: dict) -> None:
        self.published.append((channel, payload))


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db, bus):
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def test_create_http_channel(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/channels", json={
            "name": "hook1",
            "type": "http",
            "config": {"url": "https://example.com/webhook", "method": "POST"},
        })
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["type"] == "http"
    assert body["config"]["url"] == "https://example.com/webhook"
    assert len(bus.published) == 1
    assert bus.published[0][0] == "config_changed"


def test_list_channels(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        c.post("/api/channels", json={"name": "a", "type": "http", "config": {"url": "x"}})
        c.post("/api/channels", json={"name": "b", "type": "mq",   "config": {"driver": "rabbitmq", "url": "amqp://"}})
        r = c.get("/api/channels")
    assert r.status_code == 200
    names = sorted(x["name"] for x in r.json())
    assert names == ["a", "b"]


def test_get_channel_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.get("/api/channels/00000000-0000-0000-0000-000000000000")
    assert r.status_code == 404


def test_create_channel_invalid_type_422(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/channels", json={"name": "x", "type": "telegram", "config": {}})
    assert r.status_code == 422
```

- [ ] **Step 2: Run, expect FAIL**

```bash
pytest tests/unit/test_web_channels.py -v
```
Expected: FAIL — endpoints missing.

- [ ] **Step 3: Implement**

```python
# apps/web/routers/channels.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import ChannelCreate, ChannelOut
from core.bus.redis_bus import RedisBus
from core.config.models import ChannelType
from core.config.repositories import ChannelRepo

router = APIRouter(prefix="/api/channels", tags=["channels"])


@router.post("", response_model=ChannelOut, status_code=status.HTTP_201_CREATED)
async def create_channel(
    payload: ChannelCreate,
    session: AsyncSession = Depends(get_session),
    bus: RedisBus = Depends(get_bus),
) -> ChannelOut:
    row = await ChannelRepo(session).create(
        name=payload.name,
        type=ChannelType(payload.type),
        config=payload.config,
    )
    await bump_and_publish(session, bus, entity="channel", entity_id=row.id, action="create")
    return ChannelOut.model_validate(row)


@router.get("", response_model=list[ChannelOut])
async def list_channels(
    session: AsyncSession = Depends(get_session),
) -> list[ChannelOut]:
    rows = await ChannelRepo(session).list_all()
    return [ChannelOut.model_validate(r) for r in rows]


@router.get("/{channel_id}", response_model=ChannelOut)
async def get_channel(
    channel_id: str,
    session: AsyncSession = Depends(get_session),
) -> ChannelOut:
    row = await ChannelRepo(session).get(channel_id)
    if row is None:
        raise HTTPException(status_code=404, detail="channel not found")
    return ChannelOut.model_validate(row)
```

- [ ] **Step 4: Run, expect PASS**

```bash
pytest tests/unit/test_web_channels.py -v
```
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/web/routers/channels.py tests/unit/test_web_channels.py
git commit -m "feat(web): channels router (POST/GET) — accepts mq/http/ws"
git tag m1-chunk9-web-base
```

---

## Chunk 10: Web API — Subscriptions router + integration test

Completes the management surface. Adds the subscriptions router (POST + GET + channel bind) and an end-to-end integration test that drives the full API path through the real `Database` + `RedisBus` stack (SQLite + Redis testcontainer) and verifies that every write bumps `config_version` AND emits on `config_changed`.

The subscriptions router is the most complex of the three (it cross-references chains and channels and exposes a sub-resource for bindings), which is why it gets its own chunk — paired with the integration test that exercises all three routers end-to-end.

**New files this chunk:**
- `apps/web/routers/subscriptions.py` (replaces the placeholder from Task 9.1)
- `tests/unit/test_web_subscriptions.py`
- `tests/integration/test_web_api.py`

**Modified files:**
- `apps/web/schemas.py` — adds `SubscriptionDetail` (subclass of `SubscriptionOut` with bound channel ids)

**Cross-chunk dependencies (same as Chunk 9):**
- `core.config.repositories.{ChainRepo, ChannelRepo, SubscriptionRepo}` from Chunk 2.
- `core.bus.redis_bus.RedisBus.subscribe(channel, *, ready)` — async-generator function from Chunk 3.
- The `bump_and_publish` helper from `apps/web/routers/_common.py` (Chunk 9, Task 9.2).

---

### Task 10.1: Subscriptions router (POST + GET + bind/unbind channel)

`POST /api/subscriptions` creates the subscription, validating that `chain_id` references an existing chain. `POST /api/subscriptions/{id}/channels` binds an existing channel id to the subscription. The bind endpoint also bumps `config_version`. `GET /api/subscriptions/{id}` returns the subscription plus its bound channel ids.

**Files:**
- Modify: `apps/web/routers/subscriptions.py` (replace placeholder)
- Modify: `apps/web/schemas.py` (add `SubscriptionDetail` with `channel_ids` field)
- Test: `tests/unit/test_web_subscriptions.py`

- [ ] **Step 1: Add `SubscriptionDetail` to schemas.py**

Append to `apps/web/schemas.py`:

```python
class SubscriptionDetail(SubscriptionOut):
    channel_ids: list[str] = Field(default_factory=list)
```

- [ ] **Step 2: Write the failing test**

```python
# tests/unit/test_web_subscriptions.py
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database
from core.config.models import Base


class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    async def ping(self) -> bool:
        return True

    async def publish(self, channel: str, payload: dict) -> None:
        self.published.append((channel, payload))


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db, bus):
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def _seed_chain_and_channel(c: TestClient) -> tuple[str, str]:
    c.post("/api/chains", json={
        "id": "eth-mainnet", "kind": "evm", "rpc_http": "x", "rpc_ws": None,
        "confirmations": 12, "poll_interval_ms": 3000, "enabled": True,
    })
    r = c.post("/api/channels", json={"name": "hook", "type": "http", "config": {"url": "http://h"}})
    return "eth-mainnet", r.json()["id"]


def test_create_subscription_and_bind_channel(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, channel_id = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "wallet1",
            "chain_id": chain_id,
            "address": "0xabc",
            "abi_id": None,
            "match_kind": "native_transfer",
            "match_name": None,
            "arg_filters": {},
            "enabled": True,
        })
        assert r.status_code == 201, r.text
        sub_id = r.json()["id"]

        b = c.post(f"/api/subscriptions/{sub_id}/channels",
                   json={"channel_id": channel_id})
        assert b.status_code == 204, b.text

        d = c.get(f"/api/subscriptions/{sub_id}")
        assert d.status_code == 200
        assert d.json()["channel_ids"] == [channel_id]

    # Four writes: chain, channel, subscription, bind — four config_changed pubs.
    assert len(bus.published) == 4
    assert {p[1]["entity"] for p in bus.published} == {"chain", "channel", "subscription"}


def test_create_subscription_unknown_chain_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        r = c.post("/api/subscriptions", json={
            "name": "wallet1", "chain_id": "no-such",
            "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
    assert r.status_code == 404


def test_bind_channel_to_missing_subscription_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, channel_id = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions/00000000-0000-0000-0000-000000000000/channels",
                   json={"channel_id": channel_id})
    assert r.status_code == 404


def test_bind_unknown_channel_404(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        sub = c.post("/api/subscriptions", json={
            "name": "x", "chain_id": chain_id, "address": "0x1",
            "abi_id": None, "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        }).json()
        r = c.post(f"/api/subscriptions/{sub['id']}/channels",
                   json={"channel_id": "00000000-0000-0000-0000-000000000000"})
    assert r.status_code == 404


def test_invalid_match_kind_422(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "x", "chain_id": "eth-mainnet", "address": "0x1",
            "abi_id": None, "match_kind": "telepathy", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
    assert r.status_code == 422
```

- [ ] **Step 3: Run, expect FAIL**

```bash
pytest tests/unit/test_web_subscriptions.py -v
```
Expected: FAIL — endpoints missing.

- [ ] **Step 4: Implement**

```python
# apps/web/routers/subscriptions.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import (
    ChannelBindRequest,
    SubscriptionCreate,
    SubscriptionDetail,
    SubscriptionOut,
)
from core.bus.redis_bus import RedisBus
from core.config.models import MatchKind
from core.config.repositories import (
    ChainRepo,
    ChannelRepo,
    SubscriptionRepo,
)

router = APIRouter(prefix="/api/subscriptions", tags=["subscriptions"])


@router.post("", response_model=SubscriptionOut, status_code=status.HTTP_201_CREATED)
async def create_subscription(
    payload: SubscriptionCreate,
    session: AsyncSession = Depends(get_session),
    bus: RedisBus = Depends(get_bus),
) -> SubscriptionOut:
    if await ChainRepo(session).get(payload.chain_id) is None:
        raise HTTPException(status_code=404, detail="chain_id not found")
    sub = await SubscriptionRepo(session).create(
        name=payload.name,
        chain_id=payload.chain_id,
        address=payload.address,
        abi_id=payload.abi_id,
        match_kind=MatchKind(payload.match_kind),
        match_name=payload.match_name,
        arg_filters=payload.arg_filters,
        enabled=payload.enabled,
    )
    await bump_and_publish(session, bus, entity="subscription", entity_id=sub.id, action="create")
    return SubscriptionOut.model_validate(sub)


@router.get("", response_model=list[SubscriptionOut])
async def list_subscriptions(
    session: AsyncSession = Depends(get_session),
) -> list[SubscriptionOut]:
    rows = await SubscriptionRepo(session).list_all()
    return [SubscriptionOut.model_validate(r) for r in rows]


@router.get("/{sub_id}", response_model=SubscriptionDetail)
async def get_subscription(
    sub_id: str,
    session: AsyncSession = Depends(get_session),
) -> SubscriptionDetail:
    sub = await SubscriptionRepo(session).get(sub_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    # Direct query for bound channel ids — NOT `list_enabled_with_channels`, which
    # filters subs by `enabled=True` and would silently hide bindings on disabled
    # subs (an operator surprise we want to avoid).
    from sqlalchemy import select  # local import to keep header tidy
    from core.config.models import SubscriptionChannel
    res = await session.execute(
        select(SubscriptionChannel.channel_id)
        .where(SubscriptionChannel.subscription_id == sub_id)
    )
    channel_ids = [row[0] for row in res.all()]
    return SubscriptionDetail(
        id=sub.id, name=sub.name, chain_id=sub.chain_id, address=sub.address,
        abi_id=sub.abi_id, match_kind=sub.match_kind.value, match_name=sub.match_name,
        arg_filters=sub.arg_filters, enabled=sub.enabled,
        channel_ids=channel_ids,
    )


@router.post("/{sub_id}/channels", status_code=status.HTTP_204_NO_CONTENT)
async def bind_channel(
    sub_id: str,
    payload: ChannelBindRequest,
    session: AsyncSession = Depends(get_session),
    bus: RedisBus = Depends(get_bus),
) -> Response:
    sub_repo = SubscriptionRepo(session)
    if await sub_repo.get(sub_id) is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    if await ChannelRepo(session).get(payload.channel_id) is None:
        raise HTTPException(status_code=404, detail="channel not found")
    await sub_repo.bind_channel(sub_id, payload.channel_id)
    await bump_and_publish(session, bus, entity="subscription", entity_id=sub_id, action="bind_channel")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
```

- [ ] **Step 5: Run, expect PASS**

```bash
pytest tests/unit/test_web_subscriptions.py -v
```
Expected: 5 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add apps/web/schemas.py apps/web/routers/subscriptions.py tests/unit/test_web_subscriptions.py
git commit -m "feat(web): subscriptions router (POST/GET + channel binding)"
```

---

### Task 10.2: Integration test — full API path against SQLite + Redis

End-to-end API smoke test using the same testcontainer fixtures as Chunk 8's hot-reload integration test. Verifies that POSTs actually land in the DB AND emit on the real Redis bus, and that the version counter monotonically increases.

**Files:**
- Test: `tests/integration/test_web_api.py`

- [ ] **Step 1: Write the test**

`TestClient` cannot be used here. `TestClient` runs the ASGI app on its own (sync) event loop, while `redis.asyncio` clients pin to the loop they were created on. Mixing them inside one test raises `RuntimeError: got Future <...> attached to a different loop`. Use `httpx.AsyncClient(transport=ASGITransport(app=app))` to drive the app on the test's own running loop instead.

```python
# tests/integration/test_web_api.py
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.repositories import ConfigVersionRepo

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_full_create_flow_bumps_version_and_publishes(db, redis_url) -> None:
    """db, redis_url come from tests/integration/conftest.py.

    Subscribes to `config_changed` on a separate RedisBus instance, then drives
    create+bind through the API, and asserts every write produced a publish AND
    bumped the global version counter.
    """
    bus_writer = RedisBus(url=redis_url)
    bus_reader = RedisBus(url=redis_url)
    await bus_writer.connect()
    await bus_reader.connect()
    try:
        received: list[dict] = []
        ready = asyncio.Event()

        async def _drain() -> None:
            async for msg in bus_reader.subscribe("config_changed", ready=ready):
                received.append(msg)
                if len(received) >= 4:
                    return

        drain_task = asyncio.create_task(_drain())
        await asyncio.wait_for(ready.wait(), timeout=2.0)

        async with db.session() as s:
            v0 = await ConfigVersionRepo(s).get()

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r1 = await c.post("/api/chains", json={
                "id": "eth-mainnet", "kind": "evm", "rpc_http": "x", "rpc_ws": None,
                "confirmations": 12, "poll_interval_ms": 3000, "enabled": True,
            })
            assert r1.status_code == 201

            r2 = await c.post("/api/channels", json={
                "name": "hook", "type": "http", "config": {"url": "http://h"},
            })
            assert r2.status_code == 201
            channel_id = r2.json()["id"]

            r3 = await c.post("/api/subscriptions", json={
                "name": "wallet1", "chain_id": "eth-mainnet", "address": "0x1",
                "abi_id": None, "match_kind": "native_transfer", "match_name": None,
                "arg_filters": {}, "enabled": True,
            })
            assert r3.status_code == 201
            sub_id = r3.json()["id"]

            r4 = await c.post(f"/api/subscriptions/{sub_id}/channels",
                              json={"channel_id": channel_id})
            assert r4.status_code == 204

        await asyncio.wait_for(drain_task, timeout=3.0)

        async with db.session() as s:
            v_final = await ConfigVersionRepo(s).get()

        assert v_final == v0 + 4
        assert [m["entity"] for m in received] == ["chain", "channel", "subscription", "subscription"]
        assert received[-1]["action"] == "bind_channel"
    finally:
        await bus_reader.disconnect()
        await bus_writer.disconnect()
```

- [ ] **Step 2: Run**

```bash
pytest tests/integration/test_web_api.py -v -m integration
```
Expected: 1 test PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_web_api.py
git commit -m "test(web): integration — full API flow bumps version + publishes"
git tag m1-chunk10-web-subscriptions
```

---

## Chunk 11: End-to-End — Anvil + Worker + Webhook receiver

The capstone test for M1: prove the full slice works. An Anvil node produces blocks, the worker (in-process asyncio task) ingests them, the `NativeTransferParser` decodes value transfers, the `Matcher` selects the subscription, the `HttpChannel` POSTs to a local FastAPI receiver, and the test asserts the payload shape from spec §8.

This is the ONLY E2E test for M1. It is marked `@pytest.mark.e2e`, gated behind a CI label (spec §11.2), and skips with a clear message when Anvil is not installed locally.

**New files this chunk:**
- `tests/e2e/__init__.py` (empty)
- `tests/e2e/conftest.py` — `anvil` and `webhook_receiver` fixtures
- `tests/e2e/test_native_transfer_e2e.py` — the single end-to-end test

**Modified files:**
- `pyproject.toml` — refine the `e2e` marker description (already registered in Chunk 1 Task 1.2), add `eth-account` to `[project.optional-dependencies].dev`
- `Makefile` — REPLACE the `test:` and `test-e2e:` recipes from Chunk 1 with more explicit forms (add `-v`, exclude e2e from `test`)

**Out of scope this chunk:**
- Solana E2E (M2+).
- ABI event / call parsers (M2+).
- MQ / WS channels (M3+).
- Reorg E2E — covered by unit tests on `ConfirmationBuffer` in Chunk 4; running a real reorg through Anvil's `evm_setNextBlockBaseFeePerGas`-style RPCs is M2.
- Hot-reload E2E — covered by Chunk 8's integration test.
- Crash/restart resumption — covered by `Checkpoint` unit tests in Chunk 4; a full process-restart E2E adds slow surface for no new coverage in M1.

**Cross-chunk dependencies (signatures must match exactly):**
- `apps.worker.main.run_worker(settings: Settings, stop_event: asyncio.Event) -> None` — Chunk 8 Task 8.1. Boots `Database`, `RedisBus`, `ConfigWatcher`, `ChainRunner`s; runs until `stop_event` is set. Does NOT install signal handlers (caller drives shutdown).
- `apps.web.main.create_app(*, lifespan=None) -> FastAPI` — Chunk 9 Task 9.1.
- `apps.web.deps.get_db`, `get_bus` — Chunk 9 Task 9.1.
- `core.config.db.Database`, `core.bus.redis_bus.RedisBus` — Chunks 2 & 3.
- `core.settings.Settings` — Chunk 1 Task 1.4. NOTE: `Settings` has nested `database: DatabaseSettings(url=...)` and `redis: RedisSettings(url=...)` — there is no flat `database_url` / `redis_url`. Construct with `Settings(database={"url": ...}, redis={"url": ...})` (pydantic-settings accepts dict for nested models).
- The `redis_url` fixture lives at `tests/conftest.py` (lifted there by Chunk 8 Task 8.2) so both `tests/integration/` and `tests/e2e/` discover it.

**Approach:**
- Both worker and webhook receiver run in the test's event loop as `asyncio.Task`s. No subprocesses for our own code — simpler teardown, deterministic timing.
- Anvil runs as a subprocess (it's a Foundry binary, not Python). The fixture launches it, waits for the JSON-RPC port to be reachable, yields its URL, then SIGTERMs it.
- The web API is invoked via `httpx.AsyncClient + ASGITransport` to drive create/bind through the real router code (same loop, no `TestClient`).
- The webhook receiver is a tiny FastAPI app run via `uvicorn.Server` on `127.0.0.1:0` (auto-assigned port) inside an `asyncio.Task`.

---

### Task 11.1: Anvil + webhook receiver fixtures

Both fixtures land in `tests/e2e/conftest.py`. The `anvil` fixture spawns the Foundry binary as a subprocess; the `webhook_receiver` fixture runs an in-process FastAPI app via uvicorn. Both yield a small handle object so the test can introspect state (Anvil RPC URL + a list of received payloads).

**Files:**
- Create: `tests/e2e/__init__.py` (empty)
- Create: `tests/e2e/conftest.py`

- [ ] **Step 1: Tighten the `e2e` marker description and add `eth-account`**

Chunk 1 Task 1.2 already registered the marker as `"e2e: requires anvil"`. Replace that line with a more descriptive form and add `eth-account` to dev deps (it's a transitive of `web3>=6` but pinning it directly keeps `mypy --strict` happy):

```toml
# pyproject.toml — REPLACE the existing "e2e: requires anvil" line
[tool.pytest.ini_options]
markers = [
    "integration: requires testcontainers",
    "e2e: end-to-end tests; require Foundry/Anvil and run a webhook receiver in-process",
]

# pyproject.toml — ADD under [project.optional-dependencies].dev
"eth-account>=0.11,<1",
```

(Do NOT duplicate the `e2e` marker; the strict-markers pytest config rejects duplicates. Verify with `grep -n 'e2e' pyproject.toml` after editing — exactly ONE entry.)

- [ ] **Step 2: Write the empty package marker**

```python
# tests/e2e/__init__.py
# (empty)
```

- [ ] **Step 3: Write the conftest**

```python
# tests/e2e/conftest.py
from __future__ import annotations

import asyncio
import shutil
import socket
import subprocess
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import httpx
import pytest
import pytest_asyncio
import uvicorn
from fastapi import FastAPI, Request


@dataclass
class AnvilHandle:
    """Lightweight handle exposed by the `anvil` fixture."""

    rpc_url: str
    chain_id: int = 31337
    # Anvil's default deterministic accounts (mnemonic = test test ... junk).
    # The first three are exposed here; tests only need source/sink pairs.
    accounts: list[str] = field(default_factory=lambda: [
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
        "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
        "0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
    ])
    private_keys: list[str] = field(default_factory=lambda: [
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
        "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a",
    ])


@dataclass
class WebhookHandle:
    """Lightweight handle exposed by the `webhook_receiver` fixture."""

    url: str
    received: list[dict] = field(default_factory=list)


def _free_port() -> int:
    """Reserve an ephemeral port. Race-free enough for E2E given we bind
    immediately after release."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _anvil_available() -> bool:
    return shutil.which("anvil") is not None


@pytest_asyncio.fixture
async def anvil() -> AsyncIterator[AnvilHandle]:
    """Start `anvil` as a subprocess on an ephemeral port. Skip the test if
    Foundry isn't installed."""
    if not _anvil_available():
        pytest.skip("anvil (Foundry) not installed; install via `curl -L https://foundry.paradigm.xyz | bash && foundryup`")

    port = _free_port()
    proc = subprocess.Popen(
        [
            "anvil",
            "--host", "127.0.0.1",
            "--port", str(port),
            "--chain-id", "31337",
            "--block-time", "1",   # mine a block every second to keep the test snappy
            "--silent",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    rpc_url = f"http://127.0.0.1:{port}"
    # Wait up to 5 s for the JSON-RPC port to answer.
    deadline = time.time() + 5.0
    async with httpx.AsyncClient(timeout=0.5) as client:
        while time.time() < deadline:
            try:
                r = await client.post(rpc_url, json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "eth_chainId", "params": [],
                })
                if r.status_code == 200 and r.json().get("result"):
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        else:
            proc.terminate()
            pytest.fail("anvil did not become reachable within 5s")

    try:
        yield AnvilHandle(rpc_url=rpc_url)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3.0)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest_asyncio.fixture
async def webhook_receiver() -> AsyncIterator[WebhookHandle]:
    """In-process FastAPI app that captures POST bodies."""
    handle = WebhookHandle(url="")  # filled after we know the port

    app = FastAPI()

    @app.post("/hook")
    async def hook(request: Request) -> dict:
        payload = await request.json()
        handle.received.append(payload)
        return {"ok": True}

    port = _free_port()
    handle.url = f"http://127.0.0.1:{port}/hook"

    config = uvicorn.Config(
        app, host="127.0.0.1", port=port,
        log_level="warning", lifespan="off", loop="asyncio",
    )
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())

    # Wait for uvicorn to flip its `started` flag.
    deadline = time.time() + 3.0
    while not server.started and time.time() < deadline:
        await asyncio.sleep(0.05)
    if not server.started:
        server.should_exit = True
        await serve_task
        pytest.fail("uvicorn webhook receiver did not start within 3s")

    try:
        yield handle
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(serve_task, timeout=3.0)
        except asyncio.TimeoutError:
            serve_task.cancel()
```

- [ ] **Step 4: Smoke-test the fixtures via a no-op test**

Quickly verify the fixtures load and the skip path works on a machine without Anvil. Don't commit this test — it's just a dry-run.

```bash
python -c "
import asyncio, importlib.util
spec = importlib.util.spec_from_file_location('cf', 'tests/e2e/conftest.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
print('anvil available:', m._anvil_available())
print('free port:', m._free_port())
"
```
Expected: prints `anvil available: True|False` and a port number. No tracebacks.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml tests/e2e/__init__.py tests/e2e/conftest.py
git commit -m "test(e2e): anvil + in-process webhook receiver fixtures"
```

---

### Task 11.2: End-to-end test — native transfer reaches webhook

The single test that ties everything together. Boots the worker as an in-process task pointed at Anvil, drives chain/channel/subscription creation via the API (httpx + ASGI), submits N native transfers on Anvil, waits for the webhook receiver to collect N payloads, then asserts payload shape against spec §8.

**Files:**
- Create: `tests/e2e/test_native_transfer_e2e.py`

- [ ] **Step 1: Write the test**

```python
# tests/e2e/test_native_transfer_e2e.py
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from eth_account import Account
from httpx import ASGITransport, AsyncClient
from web3 import AsyncHTTPProvider, AsyncWeb3

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import run_worker
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


# How many transfers to send and how long to wait for the worker to deliver.
TRANSFER_COUNT = 3
DELIVERY_TIMEOUT_S = 30.0


@pytest_asyncio.fixture
async def db_url(tmp_path) -> str:
    """File-backed SQLite so the worker and API share the same DB."""
    return f"sqlite+aiosqlite:///{tmp_path / 'e2e.sqlite'}"


@pytest_asyncio.fixture
async def initialised_db(db_url: str) -> AsyncIterator[Database]:
    d = Database(db_url)
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def _send_native_transfer(
    w3: AsyncWeb3, *, sender_pk: str, to: str, value_wei: int
) -> str:
    sender = Account.from_key(sender_pk).address
    nonce = await w3.eth.get_transaction_count(sender)
    tx = {
        "from": sender, "to": to, "value": value_wei,
        "gas": 21_000, "gasPrice": await w3.eth.gas_price,
        "nonce": nonce, "chainId": 31337,
    }
    signed = Account.sign_transaction(tx, sender_pk)
    h = await w3.eth.send_raw_transaction(signed.raw_transaction)
    return h.hex()


async def test_native_transfer_anvil_to_webhook(
    anvil, webhook_receiver, initialised_db, db_url, redis_url,
) -> None:
    """Anvil → Worker → HttpChannel → in-process webhook receiver.

    Asserts payload conforms to spec §8 (kind=native_transfer, contains
    chain_id, tx_hash, block_hash, args.from/to/value, subscription_name).
    """
    # `Settings` uses nested DatabaseSettings/RedisSettings — pass dicts, not
    # flat kwargs. Pydantic-settings parses these into the nested BaseModels.
    settings = Settings(
        database={"url": db_url},
        redis={"url": redis_url},
    )

    # 1) Seed chain + channel + subscription via the real API stack.
    bus_writer = RedisBus(url=redis_url)
    await bus_writer.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: initialised_db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test",
        ) as c:
            r = await c.post("/api/chains", json={
                "id": "anvil-local", "kind": "evm",
                "rpc_http": anvil.rpc_url, "rpc_ws": None,
                "confirmations": 1, "poll_interval_ms": 500,
                "enabled": True,
            })
            assert r.status_code == 201, r.text

            r = await c.post("/api/channels", json={
                "name": "e2e-hook", "type": "http",
                "config": {"url": webhook_receiver.url, "method": "POST"},
            })
            assert r.status_code == 201
            channel_id = r.json()["id"]

            r = await c.post("/api/subscriptions", json={
                "name": "all-native-on-anvil",
                "chain_id": "anvil-local",
                "address": None,                    # null = global / native
                "abi_id": None,
                "match_kind": "native_transfer",
                "match_name": None,
                "arg_filters": {},
                "enabled": True,
            })
            assert r.status_code == 201
            sub_id = r.json()["id"]

            r = await c.post(f"/api/subscriptions/{sub_id}/channels",
                             json={"channel_id": channel_id})
            assert r.status_code == 204
    finally:
        await bus_writer.disconnect()

    # 2) Start the worker in-process. It will load the seeded config on boot
    # via the Redis `config_changed` pub/sub path (publishes happened above).
    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(settings, stop_event))

    # Give the worker a moment to load config and connect to Anvil.
    await asyncio.sleep(1.0)

    # 3) Submit N native transfers on Anvil.
    w3 = AsyncWeb3(AsyncHTTPProvider(anvil.rpc_url))
    try:
        sender_pk = anvil.private_keys[0]
        recipient = anvil.accounts[1]
        submitted_hashes: list[str] = []
        for i in range(TRANSFER_COUNT):
            h = await _send_native_transfer(
                w3, sender_pk=sender_pk, to=recipient,
                value_wei=10**16 * (i + 1),  # 0.01, 0.02, 0.03 ETH
            )
            submitted_hashes.append(h.lower().removeprefix("0x"))

        # 4) Wait for the receiver to collect TRANSFER_COUNT payloads.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DELIVERY_TIMEOUT_S
        timed_out = False
        while len(webhook_receiver.received) < TRANSFER_COUNT:
            if loop.time() > deadline:
                timed_out = True
                break
            await asyncio.sleep(0.5)
    finally:
        await w3.provider.disconnect()

    # 5) Stop the worker before assertions so failures don't dangle a task.
    stop_event.set()
    try:
        await asyncio.wait_for(worker_task, timeout=10.0)
    except asyncio.TimeoutError:
        worker_task.cancel()
        # `run_worker`'s finally already swallows CancelledError internally, so
        # the awaited task may return normally instead of re-raising. Suppress
        # both possibilities so timeout-recovery never masks the real failure.
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(worker_task, timeout=3.0)

    if timed_out:
        pytest.fail(
            f"only {len(webhook_receiver.received)}/{TRANSFER_COUNT} "
            f"payloads received within {DELIVERY_TIMEOUT_S}s"
        )

    # 6) Assert payload shape per spec §8.
    received_tx_hashes = {
        p["event"]["tx_hash"].lower().removeprefix("0x")
        for p in webhook_receiver.received
    }
    for h in submitted_hashes:
        assert h in received_tx_hashes, f"missing tx {h} in {received_tx_hashes}"

    sample = webhook_receiver.received[0]
    assert sample["subscription_id"] == sub_id
    assert sample["subscription_name"] == "all-native-on-anvil"
    assert sample["chain_id"] == "anvil-local"
    assert "delivery_id" in sample
    assert "delivered_at" in sample

    ev = sample["event"]
    assert ev["kind"] == "native_transfer"
    assert isinstance(ev["block_number"], int) and ev["block_number"] >= 1
    assert ev["block_hash"].startswith("0x")
    assert ev["tx_hash"].startswith("0x")
    assert "from" in ev["args"] and "to" in ev["args"] and "value" in ev["args"]
    # value is a decimal string (spec §7.3 arg_filters big-int convention).
    assert isinstance(ev["args"]["value"], str)
    assert int(ev["args"]["value"]) > 0
```

- [ ] **Step 2: Run the test against a local Anvil**

```bash
# Ensure Foundry is installed; if not:
#   curl -L https://foundry.paradigm.xyz | bash && foundryup
# Then run:
make test-e2e
```

(See Step 3 for the `test-e2e` Makefile target. If invoking pytest directly: `pytest tests/e2e/test_native_transfer_e2e.py -v -m e2e`.)

Expected: 1 test PASS within ~30s. The webhook receiver collects 3 payloads, all matching submitted tx hashes, and the sample assertions hold.

If the test SKIPs with "anvil not installed", install Foundry and re-run.

- [ ] **Step 3: Replace the `test` and `test-e2e` Makefile recipes**

Chunk 1 Task 1.2 already declares both `test` (`pytest`) and `test-e2e` (`pytest tests/e2e -m e2e`) recipes. REPLACE those two recipe blocks with the explicit forms below (do NOT add a second `.PHONY: test-e2e` directive — `.PHONY` for both is already declared on the consolidated line in Chunk 1). After editing, `grep -nE '^test(-e2e)?:' Makefile` must show exactly one of each.

```makefile
# Makefile — REPLACE the existing test: recipe block from Chunk 1
test:
	pytest tests/unit tests/integration -v -m "not e2e"

# Makefile — REPLACE the existing test-e2e: recipe block from Chunk 1
test-e2e:
	pytest tests/e2e -v -m e2e
```

- [ ] **Step 4: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: green.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/test_native_transfer_e2e.py Makefile
git commit -m "test(e2e): native transfer anvil → worker → webhook end-to-end"
git tag m1-complete
```

The `m1-complete` tag marks the end of Milestone 1: a worker that ingests EVM native transfers from a live chain, parses them, matches against API-configured subscriptions, and delivers via HTTP webhook with the spec §8 payload shape. M2 (token transfers, ABI events, more chains) builds on this skeleton.

---

