# Chain Indexer

Multi-chain block indexer with pluggable notification channels. Indexes **EVM** (Ethereum, Polygon, Arbitrum, etc.) and **Solana** chains in real-time, matches on-chain events against user-defined subscriptions, and delivers structured payloads via HTTP webhooks, Redis Streams, or WebSocket fan-out.

## Features

**Parsing**
- EVM: native transfers, ERC-20 transfers, ABI-driven event/call decoding, internal call tracing (`debug_traceTransaction`)
- Solana: SOL transfers, SPL token transfers (incl. Token-2022 `transferWithFee`), SPL ops (Approve/Revoke/MintTo/Burn), Anchor IDL event + call decoding with nested type support

**Notification Channels**
- HTTP webhooks (HMAC signature, configurable retry)
- Redis Streams (`XADD` with `MAXLEN ~` trim)
- WebSocket (Redis pub/sub fan-out with back-pressure)
- Per-channel retry policy (max_attempts, base_delay, backoff_factor)
- Config JSON-schema validation at API creation time

**Management**
- REST API for chains, channels, subscriptions, and ABIs (CRUD)
- Hot-reload: config changes propagate via Redis pub/sub without worker restart
- Confirmation buffer for EVM reorg safety; Solana uses commitment-level finality
- Web UI dashboard (React + Tailwind) with live event stream

## Architecture

```
                    ┌───────────────────────────────────────────┐
                    │              Management API               │
                    │         (FastAPI + React SPA)              │
                    └────────────────┬──────────────────────────┘
                                     │ config CRUD
                    ┌────────────────▼──────────────────────────┐
                    │          Config Database                   │
                    │     (SQLite / PostgreSQL / MySQL)          │
                    └────────────────┬──────────────────────────┘
                                     │ snapshot load
┌──────────┐    ┌────────────────────▼──────────────────────────┐
│  EVM RPC │◄───│              Worker Process                   │
│  (web3)  │    │  ┌─────────┐  ┌─────────┐  ┌──────────────┐  │
└──────────┘    │  │ Chain   │  │ Parser  │  │   Matcher    │  │
                │  │ Adapter │─▶│Pipeline │─▶│ (subscriptions)│ │
┌──────────┐    │  └─────────┘  └─────────┘  └──────┬───────┘  │
│Solana RPC│◄───│                                    │          │
│(solders) │    │                              ┌─────▼────────┐ │
└──────────┘    │                              │   Notifier   │ │
                │                              └──────┬───────┘ │
                └─────────────────────────────────────┼─────────┘
                              ┌────────────────┬──────┴────────┐
                              ▼                ▼               ▼
                        HTTP Webhook    Redis Stream     WebSocket
```

## Quick Start

### Prerequisites

- Python 3.11+
- Redis 5.0+
- Node.js 20+ (for Web UI)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Install & Run

```bash
# Clone and install
git clone https://github.com/lambda-yu/chain_indexer.git
cd chain_indexer
uv sync --extra dev

# Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your database URL and Redis URL

# Run database migrations
uv run alembic upgrade head

# Start the API server (terminal 1)
uv run uvicorn apps.web.main:create_app --factory --reload --port 8000

# Start the worker (terminal 2)
uv run python -m apps.worker.main

# (Optional) Start the Web UI dev server (terminal 3)
cd web && npm install && npm run dev
```

### Create Your First Subscription

```bash
# 1. Register an EVM chain
curl -X POST http://localhost:8000/api/chains -H 'Content-Type: application/json' -d '{
  "id": "eth-mainnet",
  "kind": "evm",
  "rpc_http": "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
  "confirmations": 12,
  "poll_interval_ms": 3000
}'

# 2. Create a webhook channel
curl -X POST http://localhost:8000/api/channels -H 'Content-Type: application/json' -d '{
  "name": "my-webhook",
  "type": "http",
  "config": {"url": "https://your-server.com/webhook", "method": "POST"}
}'

# 3. Subscribe to native transfers
curl -X POST http://localhost:8000/api/subscriptions -H 'Content-Type: application/json' -d '{
  "name": "eth-transfers",
  "chain_id": "eth-mainnet",
  "match_kind": "native_transfer",
  "enabled": true
}'

# 4. Bind the channel to the subscription
curl -X POST http://localhost:8000/api/subscriptions/{sub_id}/channels \
  -H 'Content-Type: application/json' -d '{"channel_id": "{channel_id}"}'
```

### Solana Example

```bash
curl -X POST http://localhost:8000/api/chains -H 'Content-Type: application/json' -d '{
  "id": "sol-mainnet",
  "kind": "solana",
  "rpc_http": "https://api.mainnet-beta.solana.com",
  "rpc_ws": "wss://api.mainnet-beta.solana.com",
  "commitment": "confirmed",
  "poll_interval_ms": 400
}'
```

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/healthz` | Health check (DB + Redis) |
| `POST` | `/api/chains` | Create chain |
| `GET` | `/api/chains` | List enabled chains |
| `POST` | `/api/channels` | Create notification channel |
| `GET` | `/api/channels` | List channels |
| `POST` | `/api/subscriptions` | Create subscription |
| `GET` | `/api/subscriptions` | List subscriptions |
| `POST` | `/api/subscriptions/{id}/channels` | Bind channel to subscription |
| `POST` | `/api/abis` | Upload ABI/IDL |
| `GET` | `/api/abis` | List ABIs |
| `WS` | `/ws?channel_id={id}` | Live event stream |

## Event Kinds

| Kind | Description | Chains |
|------|-------------|--------|
| `native_transfer` | ETH/SOL value transfers | EVM, Solana |
| `token_transfer` | ERC-20 / SPL token transfers | EVM, Solana |
| `event` | ABI-decoded log events / Anchor IDL events | EVM, Solana |
| `call` | ABI-decoded function calls / Anchor IDL calls | EVM, Solana |

## Channel Types

| Type | Transport | Config |
|------|-----------|--------|
| `http` | HTTP POST (webhook) | `url`, `method`, `headers`, `hmac_secret` |
| `mq` | Redis Streams (XADD) | `stream`, `maxlen` |
| `ws` | Redis pub/sub → WebSocket | `ws_fanout_channel` |

All channels support optional `retry` config: `{"max_attempts": 5, "base_delay": 2.0, "backoff_factor": 3.0}`

## Development

```bash
uv run ruff check core apps tests          # lint
uv run mypy core apps                       # type check (strict)
uv run pytest tests/ -m "not e2e"           # unit + integration (253 tests)
uv run pytest tests/e2e -m e2e             # e2e (needs Anvil + solana-test-validator)
cd web && npm run build                     # frontend production build
```

## Configuration

Environment variables with `CHAIN_INDEXER_` prefix, or `config.yaml`:

```yaml
database:
  url: "postgresql+asyncpg://user:pass@localhost:5432/indexer"
redis:
  url: "redis://localhost:6379/0"
worker:
  default_poll_interval_ms: 3000
  notify_concurrency: 50
logging:
  level: INFO
  format: json
```

## License

MIT
