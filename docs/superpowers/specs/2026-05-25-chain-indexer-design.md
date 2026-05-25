# Chain Indexer — Design Spec

**Date:** 2026-05-25
**Status:** Approved (pending review)

## 1. Purpose

A Python service that watches blocks on configured chains (EVM family and Solana), parses native-token transfers, standard token transfers, contract events, and contract calls (using user-supplied ABIs), and dispatches matched events to user-configured notification channels (MQ, HTTP webhook, WebSocket).

Target scale: small/mid-size production. Single deployment, single worker process, horizontally scalable later by splitting chains across worker processes.

## 2. Non-Goals

- Storing parsed event data for query (only metadata + checkpoints are persisted).
- Multi-tenant isolation, RBAC, or authentication on the management UI.
- Historical backfill UI beyond a configurable start block per chain.
- Built-in analytics, alerting rules beyond simple equality filters.

## 3. Functional Requirements

- Support multiple EVM chains (Ethereum, BSC, common L2s, and any EVM-compatible chain via dynamic RPC config) and Solana mainnet/devnet.
- Block ingestion via RPC + WebSocket subscription; HTTP polling as fallback when WS is unavailable.
- Parsing layers (all four enabled simultaneously):
  - Native coin transfers
  - Standard token transfers (ERC20 / SPL)
  - Custom contract events parsed by user-uploaded ABI / Anchor IDL
  - Contract function calls parsed by function selector
- Reorg handling via N-block confirmation buffer (configurable per chain). Solana uses commitment levels (`confirmed` / `finalized`).
- Notification channels:
  - MQ (pluggable: RabbitMQ, Kafka, Redis Streams, NATS)
  - HTTP webhook (with optional HMAC signature, retry policy)
  - WebSocket server (this service accepts client connections; clients subscribe by channel id)
- Built-in Web UI (compiled static assets served by FastAPI) for configuring chains, ABIs, subscriptions, and channels. No authentication.
- Configuration hot-reload: changes via the API take effect in the worker within 5 seconds.

## 4. Architecture

### 4.1 Process Topology

| Process | Role | External deps |
|---|---|---|
| `web` (FastAPI + Uvicorn) | REST API, WS server, static UI | DB, Redis |
| `worker` (asyncio entrypoint) | Chain listening, parsing, matching, notification | DB, Redis, chain RPC endpoints |
| `db` | SQLAlchemy-compatible (MySQL / PostgreSQL / SQLite) | — |
| `redis` | Internal pub/sub: WS fanout + config change events | — |

External (per user config): MQ brokers, chain RPC/WS endpoints.

### 4.2 Component Map

```
[Web process]                              [Worker process]
  ├─ REST API                                ├─ ChainListener (one asyncio task per chain)
  ├─ WS server (/ws?channel_id=)             │     ├─ WS subscribe + HTTP poll fallback
  ├─ Static UI                               │     └─ ConfirmationBuffer
  └─ Redis subscriber on "ws_fanout"         ├─ Parser pipeline (native / token / event / call)
                                             ├─ Matcher (rule index)
                                             └─ Notifier pool
                                                   ├─ MQ Channel (pluggable driver)
                                                   ├─ HTTP Channel
                                                   └─ WS Channel → Redis publish "ws_fanout"

[DB]   chains, abis, subscriptions, channels, subscription_channels,
       checkpoints, config_version
[Redis] pub/sub: ws_fanout, config_changed
```

### 4.3 Directory Layout

```
chain_indexer/
├── apps/
│   ├── web/         FastAPI app: routers, ws hub, static UI
│   └── worker/      asyncio entrypoint, scheduler
├── core/
│   ├── chains/      EVM / Solana adapters (uniform interface)
│   ├── parser/      native / ERC20 / SPL / ABI event / ABI call
│   ├── matcher/     subscription rule matching
│   ├── notifier/    Channel interface + MQ/HTTP/WS implementations
│   ├── config/      DB models, repositories, hot reload
│   └── bus/         Redis pub/sub abstraction
├── migrations/      Alembic
├── tests/
└── pyproject.toml
```

### 4.4 Key Technology

- FastAPI + Uvicorn + Pydantic v2
- asyncio, `httpx` / `aiohttp`
- EVM: `web3.py` v6 async API
- Solana: `solana-py` + `solders`
- DB: SQLAlchemy 2.x async, Alembic
- MQ libs (lazy-imported): `aio-pika`, `aiokafka`, `redis.asyncio`, `nats-py`
- Config: `pydantic-settings` (single `config.yaml` + env overrides)
- Logging: `structlog` (JSON)
- Metrics: `prometheus_client` (web process exposes `/metrics`)

## 5. Core Modules

### 5.1 `core/chains/` — Chain Adapters

```python
class ChainAdapter(Protocol):
    chain_id: str
    confirmations: int

    async def subscribe_heads(self) -> AsyncIterator[BlockHeader]: ...
    async def fetch_block(self, number: int) -> Block: ...
    async def fetch_logs(self, from_block: int, to_block: int,
                        addresses: list[str]) -> list[Log]: ...
    async def get_latest_block(self) -> int: ...
```

Implementations: `EvmAdapter` (web3.py wrapper), `SolanaAdapter` (solana-py wrapper that normalizes instructions into the uniform `Log`/`Tx` shape).

### 5.2 `core/parser/` — Parsers

Output: a uniform `Event`:

```python
@dataclass
class Event:
    chain_id: str
    block_number: int
    block_hash: str
    block_timestamp: int            # unix seconds
    tx_hash: str
    tx_index: int | None
    log_index: int | None
    kind: Literal["native_transfer", "token_transfer", "log", "call"]
    contract: str | None
    name: str | None                # event name or function name
    args: dict                      # ABI-decoded
    raw: dict                       # original payload for fallback
```

Built-in parsers:
- `NativeTransferParser` — scans `tx.value > 0`
- `Erc20TransferParser`, `SplTransferParser` — standard ABI / SPL token program
- `AbiEventParser` — user ABI; matched via `topic0` (EVM) / discriminator (Solana)
- `AbiCallParser` — user ABI; matched via 4-byte selector (EVM) / discriminator (Solana)

If decoding fails, the event is downgraded to `kind="log"` with raw topics/data preserved.

### 5.3 `core/matcher/` — Subscription Matcher

Input: `Event`. Output: list of `(Subscription, list[Channel])` tuples. Match keys: `chain_id`, `address`, `match_kind`, `match_name`, plus simple equality / range filters from `subscriptions.arg_filters`.

Address comparisons are case-insensitive.

### 5.4 `core/notifier/` — Channels

```python
class Channel(ABC):
    type: ClassVar[str]

    @abstractmethod
    async def start(self) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
    @abstractmethod
    async def send(self, payload: dict) -> None: ...
```

Channel registry: `CHANNEL_REGISTRY: dict[str, type[Channel]]`. Only enabled channel drivers are imported (avoids loading unused SDKs).

MQ uses a sub-driver pattern: `MqChannel` holds a `MqDriver` instance.

| Driver | Lib | Key config |
|---|---|---|
| `rabbitmq` | aio-pika | exchange, routing_key (templated: `{chain_id}`, `{name}`) |
| `kafka` | aiokafka | topic, partition_key (default `chain_id`) |
| `redis_streams` | redis.asyncio | stream name, maxlen |
| `nats` | nats-py | subject, optional jetstream |

### 5.5 `core/config/` — Configuration Store

- Repository pattern over SQLAlchemy async.
- On worker startup: full snapshot into in-memory subscription index.
- Hot reload: on `config_changed` Redis event OR periodic 5s poll of `config_version.version`, the worker refreshes its index.

### 5.6 `core/bus/` — Internal Bus

Two channels on Redis pub/sub:
- `ws_fanout` (worker → web)
- `config_changed` (web → worker)

Thin wrapper `Bus.publish` / `Bus.subscribe` so future swap to NATS/Kafka doesn't touch business code.

## 6. Data Flow

```
RPC/WS ─► ChainListener ─► ConfirmationBuffer ─► Parser ─► Matcher ─► Notifier
                                                                          │
                                              ┌───────────────────────────┼─────────┐
                                              ▼                           ▼         ▼
                                            MQ                      HTTP webhook  Redis ws_fanout
                                                                                     │
                                                                                     ▼
                                                                              Web WS Hub → clients
```

Step-by-step:

1. **Listener** subscribes to chain WS heads; maintains an HTTP head-poll fallback that takes over on WS disconnect.
2. **ConfirmationBuffer** maintains a `[head - confirmations, head]` window. New block in:
   - If `parentHash` of new block does not match buffer tip, walk back to the common ancestor (reorg), refetch the new branch.
   - Blocks reaching depth ≥ `confirmations` are emitted downstream as "confirmed".
3. **fetch_block + fetch_logs** for confirmed block. Solana: `getBlock(commitment=...)`; the chain `confirmations` value maps to commitment level.
4. **Parser** runs all enabled parsers on the block's transactions and logs, producing `Event` objects.
5. **Matcher** indexes subscriptions by `(chain_id, address, match_kind)`; emits `(Event, Subscription, [Channel,...])` triples.
6. **Notifier** dispatches each triple to its bound channels concurrently. Each `send` wraps retry (exponential backoff, max 3). Final failures: log only (no `delivery_failures` table).
7. **Checkpoint** updated after every block fully processed.

### 6.1 Backpressure

- One asyncio task per chain; chains do not block each other.
- Each chain pipeline uses bounded `asyncio.Queue`s between stages so a slow notifier cannot OOM the listener.
- A per-chain `asyncio.Semaphore(N)` limits concurrent in-flight notifications (default 50, configurable).

### 6.2 Reorg Consistency Contract

Documented downstream contract: a notified event with `(chain_id, tx_hash, log_index)` is only authoritative when combined with `block_hash`. If a reorg occurs after dispatch, the same logical event may be re-emitted under a different `block_hash`. Downstream MUST treat `(chain_id, tx_hash, log_index, block_hash)` as the dedupe key, or use the top-level `delivery_id` for at-least-once dedupe at the channel level.

## 7. Configuration Schema

### 7.1 `chains`

| Column | Type | Notes |
|---|---|---|
| `id` | str PK | logical name, e.g. `eth-mainnet` |
| `kind` | enum | `evm` / `solana` |
| `rpc_http` | text | HTTP RPC URL |
| `rpc_ws` | text \| null | WS URL (null → polling only) |
| `confirmations` | int | EVM: block count; Solana: 0=processed, 1=confirmed, 2=finalized |
| `poll_interval_ms` | int | Fallback poll interval |
| `enabled` | bool | |
| `created_at`, `updated_at` | ts | |

### 7.2 `abis`

| Column | Type | Notes |
|---|---|---|
| `id` | uuid PK | |
| `name` | str | |
| `kind` | enum | `evm_abi` / `solana_idl` |
| `body` | json | ABI JSON / Anchor IDL |

### 7.3 `subscriptions`

| Column | Type | Notes |
|---|---|---|
| `id` | uuid PK | |
| `name` | str | |
| `chain_id` | fk → `chains` | |
| `address` | str \| null | null = global (used for native transfers) |
| `abi_id` | fk → `abis` \| null | required for `event` / `call` |
| `match_kind` | enum | `native_transfer` / `token_transfer` / `event` / `call` |
| `match_name` | str \| null | required for `event` / `call` |
| `arg_filters` | json | `{ "to": "0xabc", "value_gte": "1000000000000000000" }` |
| `enabled` | bool | |

### 7.4 `channels`

| Column | Type | Notes |
|---|---|---|
| `id` | uuid PK | |
| `name` | str | |
| `type` | enum | `mq` / `http` / `ws` |
| `config` | json | driver-specific |

Example `config` payloads:

```jsonc
// type=mq
{ "driver": "rabbitmq", "url": "amqp://...", "exchange": "events", "routing_key": "{chain_id}.{name}" }
{ "driver": "kafka", "bootstrap_servers": "...", "topic": "chain_events" }
{ "driver": "redis_streams", "url": "redis://...", "stream": "events" }
{ "driver": "nats", "url": "nats://...", "subject": "events" }

// type=http
{ "url": "https://...", "method": "POST", "headers": {...}, "hmac_secret": "..." }

// type=ws
{}   // channel_id becomes the WS subscription key
```

### 7.5 `subscription_channels`

```
(subscription_id, channel_id)  PRIMARY KEY
```

### 7.6 `checkpoints`

| Column | Type | Notes |
|---|---|---|
| `chain_id` | fk PK | |
| `last_block` | bigint | last fully processed |
| `last_block_hash` | str | reorg guard on resume |
| `updated_at` | ts | |

### 7.7 `config_version`

Single row, `version int`. Incremented on every config write. Worker uses it to detect changes for hot-reload polling.

## 8. Notification Payload (uniform across channels)

```json
{
  "subscription_id": "uuid",
  "subscription_name": "USDC big transfers",
  "chain_id": "eth-mainnet",
  "event": {
    "kind": "token_transfer",
    "name": "Transfer",
    "contract": "0xA0b8...",
    "block_number": 19000000,
    "block_hash": "0x...",
    "block_timestamp": 1735689600,
    "tx_hash": "0x...",
    "tx_index": 42,
    "log_index": 7,
    "args": { "from": "0x...", "to": "0x...", "value": "1000000000" }
  },
  "delivered_at": 1735689601,
  "delivery_id": "uuid"
}
```

`delivery_id` is the idempotency key for downstream dedupe.

## 9. Error Handling

| Fault | Handling |
|---|---|
| RPC HTTP 5xx / timeout | Exponential backoff retry (1/2/4/8s, max 5); mark chain `degraded`, keep retrying |
| RPC WS disconnect | Reconnect every ≤5s; HTTP poll bridges the gap (no block loss) |
| Malformed block from RPC | Skip block, log WARN; do not block subsequent blocks |
| Reorg | ConfirmationBuffer rewinds to common ancestor; already-emitted events are NOT retracted (consumers reconcile via `block_hash`) |
| ABI decode failure | Downgrade event to `kind="log"` with raw payload; still subject to matching |
| Matcher exception per event | Skip event, increment error counter, log ERROR |
| `Channel.send` exception | Retry 3× with exponential backoff (1/4/16s); final failure → log only |
| HTTP 4xx (except 408/429) | No retry |
| DB unreachable | Worker continues with last in-memory config snapshot; retry every 30s |
| Redis unreachable | WS fanout drops silently; MQ/HTTP unaffected; auto-reconnect |

### 9.1 Graceful Shutdown

On SIGTERM:
1. Listener stops accepting new heads.
2. Drain pipeline (max wait 30s).
3. Notifier flushes in-flight tasks.
4. Checkpoint flushed to DB.
5. Exit.

Anything not drained will be re-fetched and re-notified from the checkpoint on next start (at-least-once semantics; downstream dedup via `delivery_id`).

## 10. Observability

- **Logging**: structlog JSON; standard fields `chain_id`, `block_number`, `subscription_id`, `channel_id`, `event_id`.
- **Metrics** (`/metrics` on web process):
  - `chain_head_lag{chain_id}` — local head vs RPC head
  - `block_processing_seconds{chain_id}` histogram
  - `events_emitted_total{chain_id,kind}` counter
  - `notify_send_total{channel_type,status}` counter
  - `notify_retry_total{channel_type}` counter
  - `worker_last_heartbeat_seconds` gauge
- **Health**:
  - `web`: `GET /healthz` — DB + Redis ping
  - `worker`: writes `worker_heartbeat` row every 5s; web `/healthz/worker` checks freshness

## 11. Testing Strategy

| Layer | ~Share | Tools | Scope |
|---|---|---|---|
| Unit | 70% | pytest + pytest-asyncio | parsers, matcher, channel drivers, reorg buffer |
| Integration | 25% | pytest + testcontainers | real DB (SQLite/Postgres), Redis, RabbitMQ/Kafka |
| End-to-end | 5% | pytest + Anvil + solana-test-validator | full pipeline: chain → worker → channel |

### 11.1 Required Test Cases

**Parser**
- ERC20 Transfer decode
- Anchor IDL instruction decode
- ABI missing / decode failure → downgrade to raw log
- Multi-topic non-Transfer event arg ordering correct

**ConfirmationBuffer / Reorg**
- Steady-state advance
- Single-block reorg: parentHash mismatch → rewind 1
- Deep reorg beyond `confirmations`: error log + skip (rare)
- WS disconnect → HTTP poll takeover → WS reconnect → no duplicate blocks

**Matcher**
- Case-insensitive address match
- `arg_filters` equality and range
- Multiple subscriptions hit by one event → all returned
- Disabled subscription → no match

**Channel**
- HTTP 5xx retried 3 times then abandoned
- HTTP 4xx not retried (except 408/429)
- HMAC signature byte-correct
- Each MQ driver: independent consumer receives the message (testcontainers)
- WS: worker publish → Redis → web hub → connected client receives

**Hot Reload**
- New subscription takes effect within 5s
- Disabling a subscription stops matches within 5s
- Channel changes do not disturb running chain listeners

**Checkpoint / Restart**
- Worker restart resumes from checkpoint
- At-least-once: same `(tx_hash, block_hash, log_index)` may be re-sent; downstream dedupes via `delivery_id`

### 11.2 CI

- Lint: ruff
- Type check: mypy strict on `core/`
- Unit + integration on every PR
- E2E on a separate workflow (slow; PR-label triggered)

## 12. Out of Scope / Future Work

- Multi-tenant config isolation and RBAC
- Storing parsed event history for query / replay
- Backfill UI beyond start-block selection
- Pluggable internal bus (currently Redis-only)
- Sharding chains across multiple worker processes (current design assumes a single worker)

## 13. Approval

Approved by user on 2026-05-25 over interactive brainstorming session. See conversation transcript.
