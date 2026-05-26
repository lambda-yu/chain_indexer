# M5 Design Spec: Web UI — React + Vite + shadcn/ui

## 1. Goal

Provide a browser-based management UI for the chain indexer: CRUD configuration (chains, channels, subscriptions, ABIs), real-time monitoring dashboard, and live event stream via WebSocket.

## 2. Scope

Six chunks:

| # | Deliverable | Key files |
|---|-------------|-----------|
| 1 | Project scaffold (Vite + React + Tailwind + shadcn/ui + layout) | `web/` directory |
| 2 | Chains CRUD page | `web/src/pages/Chains.tsx` |
| 3 | Channels + Subscriptions + ABIs CRUD pages | `web/src/pages/` |
| 4 | Real-time monitoring dashboard | `web/src/pages/Dashboard.tsx` |
| 5 | Live event stream (WebSocket) | `web/src/pages/EventStream.tsx` |
| 6 | FastAPI static serve + build integration + close-out | `apps/web/main.py` |

## 3. Out of scope

- Auth (M6+)
- Events persistence / historical query
- Mobile-responsive design (desktop-first, basic responsiveness via Tailwind)
- i18n / l10n
- E2E browser tests (Playwright) — unit tests for API client + component smoke tests only

## 4. Detailed design

### 4.1 Chunk 1: Project scaffold

**Directory structure:**
```
web/
├── index.html
├── package.json
├── tsconfig.json
├── vite.config.ts
├── tailwind.config.ts
├── postcss.config.js
├── components.json          # shadcn/ui config
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   ├── api/
│   │   └── client.ts        # fetch wrapper
│   ├── components/
│   │   ├── ui/              # shadcn/ui components
│   │   └── Layout.tsx        # sidebar + topbar
│   ├── pages/
│   │   ├── Dashboard.tsx
│   │   ├── Chains.tsx
│   │   ├── Channels.tsx
│   │   ├── Subscriptions.tsx
│   │   ├── Abis.tsx
│   │   └── EventStream.tsx
│   └── lib/
│       └── utils.ts
```

**API client** (`web/src/api/client.ts`): Thin wrapper around `fetch`. Base URL from `VITE_API_BASE` env var (defaults to `/api` for production; Vite dev proxy maps `/api` → `http://localhost:8000/api`).

**Layout:** Sidebar with navigation links (Dashboard, Chains, Channels, Subscriptions, ABIs, Event Stream). Top bar with project title. Content area uses React Router.

**Vite dev proxy:** `vite.config.ts` proxies `/api` and `/ws` to the backend dev server.

### 4.2 Chunk 2: Chains CRUD

**Table columns:** ID, Kind (EVM/Solana badge), RPC HTTP, Confirmations/Commitment, Poll Interval, Status (enabled toggle), Actions (edit/delete).

**Create/Edit dialog:** Form with kind selector. When kind=evm: show confirmations field, hide commitment. When kind=solana: show commitment dropdown, hide confirmations. Optional: trace_internal_calls checkbox (EVM only).

**API calls:** `GET /api/chains`, `POST /api/chains`, `DELETE` (if endpoint exists, otherwise disable delete button).

### 4.3 Chunk 3: Channels + Subscriptions + ABIs

**Channels page:**
- Table: ID, Name, Type badge (http/mq/ws), Config summary, Actions.
- Create dialog: type selector → dynamic config form (HTTP: url + method + headers; MQ: stream + maxlen; WS: ws_fanout_channel).

**Subscriptions page:**
- Table: ID, Name, Chain (link), Match Kind, Match Name, Enabled, Channels count, Actions.
- Create dialog: chain dropdown, match_kind selector, arg_filters JSON editor (monaco-editor or simple textarea with validation).
- Channel binding: multi-select dropdown of available channels.

**ABIs page:**
- Table: ID, Name, Kind (evm_abi/solana_idl), Actions.
- Create dialog: name + kind selector + JSON file upload or paste.
- Preview panel: parsed function/event list from the uploaded ABI JSON (client-side parse, no backend needed).

### 4.4 Chunk 4: Real-time monitoring dashboard

**Health card:** Polls `GET /api/healthz` every 10s. Shows DB and Redis status with green/red indicators.

**Chain status cards:** One card per chain. Shows chain ID, kind, latest known block/slot (from a new lightweight `GET /api/chains/:id/status` endpoint or polling existing data).

**Stats cards:** Total chains, total subscriptions, total channels — simple count from list endpoints.

Note: Real-time block number tracking requires either a new backend endpoint or WebSocket push. For M5, polling the health endpoint + chain list is sufficient. A dedicated `/api/chains/:id/status` with `latest_block` from the checkpoint table is a small backend addition (chunk 4 adds this).

### 4.5 Chunk 5: Live event stream

**WebSocket connection:** Connect to backend `ws://<host>/ws?channel_id=<selected>`.

**UI:**
- Channel selector dropdown (populated from `GET /api/channels` filtered to type=ws).
- Connect/Disconnect button.
- Event card list (newest first): each card shows timestamp, kind badge, name, contract, args (collapsible JSON viewer).
- Pause/Resume toggle (stops appending to the list without disconnecting).
- Max 200 events in the UI buffer (oldest dropped).

### 4.6 Chunk 6: FastAPI static serve + integration

**Production serve:** FastAPI mounts `web/dist/` as static files. SPA fallback: any non-`/api` non-`/ws` GET returns `index.html`.

```python
# apps/web/main.py addition
from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="web/dist", html=True), name="spa")
```

**Build script:** `cd web && npm run build` produces `web/dist/`. The Python package does NOT bundle the frontend — operators build separately or use a Docker multi-stage build.

## 5. Backend additions needed

| Addition | Chunk | Files |
|----------|-------|-------|
| `GET /api/chains/:id/status` → `{latest_block, latest_block_hash}` from checkpoint table | 4 | `apps/web/routers/chains.py` |
| SPA fallback route (catch-all → index.html) | 6 | `apps/web/main.py` |

## 6. Dependencies (frontend)

```json
{
  "react": "^19",
  "react-dom": "^19",
  "react-router-dom": "^7",
  "@tanstack/react-query": "^5",
  "tailwindcss": "^4",
  "lucide-react": "latest",
  "class-variance-authority": "latest",
  "clsx": "latest",
  "tailwind-merge": "latest"
}
```

shadcn/ui components are copied into `web/src/components/ui/` at init time (not a runtime dependency).

## 7. Risks

- **CORS in development:** Vite dev server runs on port 5173; backend on 8000. The Vite proxy config handles this. Production has no CORS issue (same origin).
- **Bundle size:** shadcn/ui is tree-shakeable; only imported components are bundled. Target: <200KB gzipped for the full SPA.
- **WebSocket reconnect:** The event stream page should auto-reconnect on disconnect with exponential backoff (1s → 4s → 16s → 60s cap), matching the backend's own reconnect pattern.
