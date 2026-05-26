# Chain Indexer

Multi-chain block indexer with pluggable notification channels. Indexes EVM and Solana chains, matches events against subscriptions, and delivers payloads via HTTP webhooks, Redis Streams, or WebSocket fan-out.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Web Framework | FastAPI + Uvicorn |
| ORM | SQLAlchemy 2.0 (async) + Alembic |
| Config | pydantic-settings (YAML + env vars, prefix `CHAIN_INDEXER_`) |
| Message Bus | Redis 5.0+ (pub/sub + streams) |
| EVM RPC | web3.py 6.15+ |
| Solana RPC | solders 0.21+ / httpx |
| ABI Decoding | eth-abi, borsh-construct |
| Logging | structlog (JSON) |
| Frontend | React 19 + Vite 6 + Tailwind 4 + React Query 5 |
| Testing | pytest + pytest-asyncio + testcontainers |
| Linting | ruff (E/F/I/B/UP/ASYNC/SIM) |
| Type Check | mypy (strict) + pydantic plugin |

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  FastAPI API │────▶│  SQLite/PG   │◀────│  Worker Process  │
│  (web)       │     │  (config DB) │     │  (chain runners) │
└──────┬──────┘     └──────────────┘     └────────┬────────┘
       │                                          │
       │            ┌──────────────┐              │
       └───────────▶│  Redis Bus   │◀─────────────┘
                    │  (pub/sub)   │
                    └──────────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         HTTP Hook   Redis Stream   WebSocket
```

**Data flow:** Chain adapter polls blocks → Parser pipeline extracts Events → Matcher filters by subscription → Notifier dispatches to channels.

## Key Patterns

- **Protocol-based adapters:** `ChainAdapter` (EVM) and `SolanaChainAdapter` (Solana) — divergent return types, separate Protocols
- **Auto-registering channels:** `Channel.__init_subclass__` populates `CHANNEL_REGISTRY` at class-definition time. Subclasses must declare `type` and `config_schema` ClassVars
- **Parser pipelines:** `EvmParserPipeline` and `SolanaParserPipeline` — per-parser exception isolation
- **Config hot-reload:** `ConfigWatcher` listens to Redis pub/sub `config_changed` events, triggers snapshot rebuild → runner reconciliation

## Directory Structure

```
core/                          # Domain logic (no web/worker deps)
├── abi/                       # ABI decoder + registry (EVM + Anchor IDL)
├── bus/                       # Redis pub/sub bus
├── chains/                    # Chain adapters + block types
│   ├── evm.py                # EvmAdapter (web3.py + debug_traceTransaction)
│   └── solana.py             # SolanaAdapter (solders RPC + WS slot subscribe)
├── config/                    # DB models, repositories, snapshot
├── matcher/                   # Subscription matching + arg_filters
├── notifier/                  # Channel dispatch (http/mq/ws) + retry
└── parser/                    # EVM + Solana parsers
    ├── native.py, erc20.py, abi_event.py, abi_call.py, internal_call.py  (EVM)
    └── sol_native.py, spl_transfer.py, spl_ops.py, anchor_event.py, anchor_call.py  (Solana)

apps/
├── web/                       # FastAPI API + SPA static serve
│   ├── routers/              # chains, channels, subscriptions, abis, ws
│   └── schemas.py            # Pydantic request/response models
└── worker/                    # Background indexer
    ├── main.py               # Worker entrypoint + _Worker class
    └── chain_runner.py       # Per-chain EVM/Solana pipeline runner

web/                           # React SPA (Vite + Tailwind)
├── src/pages/                # Dashboard, Chains, Channels, Subscriptions, Abis, EventStream
└── src/api/client.ts         # Fetch wrapper (proxied /api → backend)

scripts/                       # Operator tools
└── validate_arg_filters.py   # Scan DB for M2-incompatible arg_filters
```

## Development

```bash
# Install
uv sync --extra dev

# Run checks
uv run ruff check core apps tests        # lint
uv run mypy core apps                     # type check (strict)

# Run tests
uv run pytest tests/ -m "not e2e"         # unit + integration (253 tests)
uv run pytest tests/e2e -m e2e            # e2e (needs Anvil + solana-test-validator)

# Run services
uv run uvicorn apps.web.main:create_app --factory --reload --port 8000
uv run python -m apps.worker.main

# Frontend
cd web && npm install && npm run dev      # dev server on :5173 (proxies /api → :8000)
cd web && npm run build                   # production build → web/dist/
```

## Entrypoints

| Command | Module |
|---------|--------|
| `chain-indexer-worker` | `apps.worker.main:main` |
| `chain-indexer-web` | `apps.web.main:main` |

## Configuration

Env prefix: `CHAIN_INDEXER_`. Nested: `__`. Example: `CHAIN_INDEXER_DATABASE__URL=postgresql+asyncpg://...`

Also reads `config.yaml` if present (lower priority than env vars).

## Documentation Map

> Load docs on demand — only when the current task matches the trigger.

| Doc | Load When |
|-----|-----------|
| `docs/superpowers/specs/` | Understanding design decisions |
| `docs/superpowers/plans/` | Understanding implementation history |
| `core/settings.py` | Configuration questions |
| `apps/web/schemas.py` | API request/response shape questions |
| `core/parser/event.py` | Event dataclass shape |
| `core/notifier/channel.py` | Adding new channel types |
| `core/chains/adapter.py` | Adding new chain types |

## Milestones

| Tag | Scope |
|-----|-------|
| `m1-complete` | EVM skeleton: native transfer → confirmation buffer → matcher → HTTP webhook |
| `m2-complete` | ABI parsers, Solana adapter + 3 parsers, MQ/WS channels, arg_filters |
| `m3-complete` | borsh nested types, Anchor call parser, SPL ops, config schema, retry, WS heads |
| `m4-complete` | EVM internal call tracing (debug_traceTransaction + callTracer) |
| `m5-complete` | Web UI (React + Vite + Tailwind dashboard) |
