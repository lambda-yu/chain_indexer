# Chain Indexer

多链区块索引器，带可插拔的通知通道。实时索引 **EVM**（Ethereum、Polygon、Arbitrum 等）和 **Solana** 链上数据，根据用户订阅规则匹配链上事件，通过 HTTP Webhook、Redis Streams 或 WebSocket 扇出投递结构化负载。

## 功能特性

**解析能力**
- EVM：原生转账、ERC-20 转账、ABI 驱动的事件/调用解码、内部调用追踪（`debug_traceTransaction`）
- Solana：SOL 转账、SPL 代币转账（含 Token-2022 的 `transferWithFee`）、SPL 操作（Approve/Revoke/MintTo/Burn）、Anchor IDL 事件 + 调用解码，支持嵌套类型

**通知通道**
- HTTP Webhook（HMAC 签名、可配置重试）
- Redis Streams（`XADD` + `MAXLEN ~` 裁剪）
- WebSocket（Redis pub/sub 扇出，带背压）
- 每个通道独立的重试策略（max_attempts、base_delay、backoff_factor）
- API 创建时进行 config JSON-schema 校验

**运营管理**
- REST API 管理链、通道、订阅、ABI（增删改查）
- 热加载：配置变更通过 Redis pub/sub 推送，无需重启 worker
- EVM 确认缓冲区防止重组；Solana 使用 commitment 级别 finality
- Web UI 控制台（React + Tailwind），含实时事件流
- 投递记录：成功/失败的投递历史可查询、可重推、可清理（自动按上限滚动删除成功记录）

## 架构

```
                    ┌───────────────────────────────────────────┐
                    │              管理 API                      │
                    │         (FastAPI + React SPA)             │
                    └────────────────┬──────────────────────────┘
                                     │ 配置增删改查
                    ┌────────────────▼──────────────────────────┐
                    │            配置数据库                       │
                    │     (SQLite / PostgreSQL / MySQL)          │
                    └────────────────┬──────────────────────────┘
                                     │ snapshot 加载
┌──────────┐    ┌────────────────────▼──────────────────────────┐
│  EVM RPC │◄───│              Worker 进程                       │
│  (web3)  │    │  ┌─────────┐  ┌─────────┐  ┌──────────────┐  │
└──────────┘    │  │ 链适配器 │  │ 解析器  │  │   匹配器     │  │
                │  │         │─▶│ 流水线  │─▶│  (订阅匹配)   │  │
┌──────────┐    │  └─────────┘  └─────────┘  └──────┬───────┘  │
│Solana RPC│◄───│                                    │          │
│(solders) │    │                              ┌─────▼────────┐ │
└──────────┘    │                              │  通知器      │ │
                │                              └──────┬───────┘ │
                └─────────────────────────────────────┼─────────┘
                              ┌────────────────┬──────┴────────┐
                              ▼                ▼               ▼
                         HTTP Webhook    Redis Stream     WebSocket
```

数据流：链适配器轮询区块 → 解析流水线提取 Event → 匹配器按订阅过滤 → 通知器投递到各通道。

## 快速开始

### Docker 一键启动（推荐）

```bash
git clone https://github.com/lambda-yu/chain_indexer.git
cd chain_indexer
make up
```

会拉起 5 个服务：PostgreSQL + Redis + 数据库迁移 + API 服务 + Worker 进程。

- **Web UI**: http://localhost:8000
- **API**: http://localhost:8000/api
- **健康检查**: http://localhost:8000/healthz

```bash
make logs    # 查看 web + worker 日志
make down    # 停止所有服务

# 水平扩展 worker
docker compose up -d --scale worker=3

# 强制重建（代码更新后）
docker compose build --no-cache && docker compose up -d
```

### 本地开发（不用 Docker）

#### 前置依赖

- Python 3.11+
- Redis 5.0+
- Node.js 20+（仅 Web UI 需要）
- [uv](https://docs.astral.sh/uv/)（推荐）或 pip

#### 安装与运行

```bash
# 克隆并安装
git clone https://github.com/lambda-yu/chain_indexer.git
cd chain_indexer
uv sync --extra dev

# 配置
cp config.example.yaml config.yaml
# 修改 config.yaml 中的数据库地址和 Redis 地址

# 跑数据库迁移
uv run alembic upgrade head

# 启动 API 服务（终端 1）
uv run uvicorn apps.web.main:create_app --factory --reload --port 8000

# 启动 worker（终端 2）
uv run python -m apps.worker.main

# （可选）启动 Web UI 开发服务器（终端 3）
cd web && npm install && npm run dev
```

### 创建第一个订阅

```bash
# 1. 注册一条 EVM 链
curl -X POST http://localhost:8000/api/chains -H 'Content-Type: application/json' -d '{
  "id": "eth-mainnet",
  "kind": "evm",
  "rpc_http": "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY",
  "confirmations": 12,
  "poll_interval_ms": 3000
}'

# 2. 创建一个 webhook 通道
curl -X POST http://localhost:8000/api/channels -H 'Content-Type: application/json' -d '{
  "name": "my-webhook",
  "type": "http",
  "config": {"url": "https://your-server.com/webhook", "method": "POST"}
}'

# 3. 订阅原生转账
curl -X POST http://localhost:8000/api/subscriptions -H 'Content-Type: application/json' -d '{
  "name": "eth-transfers",
  "chain_id": "eth-mainnet",
  "match_kind": "native_transfer",
  "enabled": true
}'

# 4. 把通道绑到订阅上
curl -X POST http://localhost:8000/api/subscriptions/{sub_id}/channels \
  -H 'Content-Type: application/json' -d '{"channel_id": "{channel_id}"}'
```

### Solana 示例

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

## API 参考

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/healthz` | 健康检查（DB + Redis） |
| `POST` | `/api/chains` | 创建链 |
| `GET` | `/api/chains` | 列出启用的链 |
| `POST` | `/api/channels` | 创建通知通道 |
| `GET` | `/api/channels` | 列出通道 |
| `POST` | `/api/subscriptions` | 创建订阅 |
| `GET` | `/api/subscriptions` | 列出订阅 |
| `POST` | `/api/subscriptions/{id}/channels` | 绑定通道到订阅 |
| `POST` | `/api/abis` | 上传 ABI/IDL |
| `GET` | `/api/abis` | 列出 ABI |
| `GET` | `/api/delivery-records` | 查询投递记录（支持 `?status=success\|failed\|retrying\|resolved` 服务端过滤） |
| `POST` | `/api/delivery-records/{id}/retry` | 手动重推失败的投递（失败时 attempts +1 + error 更新） |
| `POST` | `/api/delivery-records/{id}/resolve` | 标记投递为已解决 |
| `DELETE` | `/api/delivery-records/{id}` | 删除单条投递记录 |
| `WS` | `/ws?channel_id={id}` | 实时事件流 |

## 事件类型

| 类型 | 说明 | 支持链 |
|------|------|--------|
| `native_transfer` | ETH/SOL 原生转账 | EVM、Solana |
| `token_transfer` | ERC-20 / SPL 代币转账 | EVM、Solana |
| `event` | ABI 解码的日志事件 / Anchor IDL 事件 | EVM、Solana |
| `call` | ABI 解码的函数调用 / Anchor IDL 调用 | EVM、Solana |

## 通道类型

| 类型 | 传输方式 | 配置项 |
|------|----------|--------|
| `http` | HTTP POST（webhook） | `url`、`method`、`headers`、`hmac_secret` |
| `mq` | Redis Streams（XADD） | `stream`、`maxlen` |
| `ws` | Redis pub/sub → WebSocket | `ws_fanout_channel` |

所有通道支持可选的 `retry` 配置：`{"max_attempts": 5, "base_delay": 2.0, "backoff_factor": 3.0}`

## 投递记录治理

worker 内置后台清理任务，自动按上限滚动删除最旧的 **成功** 投递记录（`status='success'`），失败 / 重试中 / 已解决记录永不自动清理。通过 env 调参：

| 配置项 | 默认 | 说明 |
|--------|------|------|
| `CHAIN_INDEXER_DELIVERY_RECORDS__MAX_SUCCESS_ROWS` | `50000` | 成功记录上限 |
| `CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_INTERVAL_SECONDS` | `300` | 检查间隔（秒） |
| `CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_BATCH_SIZE` | `1000` | 单轮删除上限（控制事务大小） |

UI 上的投递记录页面支持按状态服务端过滤，并对 `attempts > 1` 的记录用琥珀色高亮，方便排查反复失败的投递。

## 开发命令

```bash
uv run ruff check core apps tests          # lint
uv run mypy core apps                       # 类型检查（strict）
uv run pytest tests/ -m "not e2e"           # 单元 + 集成测试（299 个）
uv run pytest tests/e2e -m e2e             # 端到端（需要 Anvil + solana-test-validator）
cd web && npm run build                     # 前端生产构建
```

## 配置

环境变量前缀 `CHAIN_INDEXER_`，嵌套用 `__` 分隔；也可写在 `config.yaml`：

```yaml
database:
  url: "postgresql+asyncpg://user:pass@localhost:5432/indexer"
redis:
  url: "redis://localhost:6379/0"
worker:
  default_poll_interval_ms: 3000
  notify_concurrency: 50
delivery_records:
  max_success_rows: 50000
  cleanup_interval_seconds: 300
  cleanup_batch_size: 1000
logging:
  level: INFO
  format: json
```

## License

MIT
