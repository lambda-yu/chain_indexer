# M3 Implementation Plan: Solana Parsing Completeness + Ops Maturity

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete Solana parsing surface to near-EVM parity and harden notification channels with config validation, per-channel retry, and WS-based head subscription.

**Architecture:** 7 chunks in 2 segments. Segment A (chunks 1-4) extends Solana parsers: borsh nested types, Anchor call parser, SPL transferWithFee, SPL non-Transfer ops. Segment B (chunks 5-7) hardens operations: channel config JSON-schema validation, per-channel retry policy, Solana WS head subscription. Each chunk follows TDD red→green.

**Tech Stack:** borsh-construct (existing), construct (existing), jsonschema (new dep), websockets (transitive via web3), base58 (existing), solders (existing).

---

## Chunk 1: borsh nested/composite type support

Extends `build_anchor_event_struct` in `core/abi/decoder.py` to handle `{defined: ...}`, `{vec: ...}`, `{option: ...}`, `{array: [T, N]}` IDL types recursively. Renames `decode_anchor_event` → `decode_anchor_borsh` for shared use by event and call parsers.

### Task 1.1: Recursive type resolver + rename

**Files:**
- Modify: `core/abi/decoder.py:168-220`
- Modify: `core/parser/anchor_event.py:10` (import rename)
- Test: `tests/unit/test_anchor_event_decoder.py` (new)

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_anchor_event_decoder.py` with tests for:
1. Nested `{defined: "Price"}` struct with inner fields
2. `{vec: "pubkey"}` field
3. `{option: "u64"}` field (present + absent)
4. `{array: ["u8", 32]}` field
5. Circular `{defined: "Self"}` returns `None`
6. Depth > 8 returns `None`

- [ ] **Step 2: Run, expect FAIL** (import errors — `_resolve_type` doesn't exist)

- [ ] **Step 3: Implement `_resolve_type` + update `build_anchor_event_struct`**

In `core/abi/decoder.py`:
- Add `_resolve_type(type_spec, types_section, seen=frozenset(), depth=0)` recursive function
- Handle: str scalars → `_ANCHOR_SCALAR_MAP`, `{"defined": name}` → lookup in types_section + recurse, `{"vec": inner}` → `borsh_construct.Vec(...)`, `{"option": inner}` → `borsh_construct.Option(...)`, `{"array": [inner, size]}` → `construct.Array(size, ...)`
- Guard: `depth >= 8` → return None, `name in seen` → return None
- Update `build_anchor_event_struct(idl_event, types_section=None)` to call `_resolve_type` per field
- Rename `decode_anchor_event` → `decode_anchor_borsh` with recursive Container→dict normalization
- Update `core/parser/anchor_event.py` import

- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Lint + typecheck**: `uv run ruff check core && uv run mypy core apps`
- [ ] **Step 6: Commit**

### Task 1.2: Chunk 1 close-out

- [ ] **Step 1: Full test suite**: `uv run pytest tests/ -m "not e2e"`
- [ ] **Step 2: Lint + typecheck**

---

## Chunk 2: Anchor IDL call parser

New `AnchorIdlCallParser` that walks `block.transactions[].instructions[]`, decodes Anchor program calls via `sha256("global:<fn_name>")[:8]` discriminator.

### Task 2.1: Registry call-discriminator index

**Files:**
- Modify: `core/abi/decoder.py` (add `anchor_call_discriminator`)
- Modify: `core/abi/registry.py` (add `_anchor_call_index`, `_rebuild_anchor_call_index`, `lookup_idl_call_by_discriminator`)
- Test: extend `tests/unit/test_abi_registry.py`

- [ ] **Step 1: Write failing tests** — `lookup_idl_call_by_discriminator` for known/unknown/collision cases
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement**
  - `anchor_call_discriminator(fn_name) -> bytes` = `sha256(f"global:{fn_name}".encode()).digest()[:8]`
  - `_anchor_call_index: dict[tuple[str, str], tuple[str, str, Any]]`
  - `_anchor_call_cache: dict[tuple[str, str], tuple[str, Any] | None]`
  - `_rebuild_anchor_call_index()` reads `idl["instructions"]`, computes disc, builds index
  - `lookup_idl_call_by_discriminator(program_id, disc_hex) -> tuple[str, Any] | None`
  - Wire `_rebuild_anchor_call_index()` and `_anchor_call_cache.clear()` into `refresh()`
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 2.2: AnchorIdlCallParser implementation

**Files:**
- Create: `core/parser/anchor_call.py`
- Test: `tests/unit/test_anchor_call_parser.py` (new)

- [ ] **Step 1: Write failing tests** — decode known instruction, skip unknown program, skip unknown disc, skip failed tx, skip stack_depth > 1
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement** `AnchorIdlCallParser(chain_id, registry)` mirroring `AnchorIdlEventParser` structure but walking `tx.instructions` instead of `tx.log_messages`
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 2.3: Wire into ChainRunner

**Files:**
- Modify: `apps/worker/chain_runner.py:81-89`

- [ ] **Step 1: Write failing test** — assert `AnchorIdlCallParser` in Solana pipeline when registry present
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Import and append** `AnchorIdlCallParser` after `AnchorIdlEventParser` in `_build_pipeline`
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 2.4: Chunk 2 close-out
- [ ] Full suite + lint + typecheck

---

## Chunk 3: SPL `transferWithFee` (Token-2022)

Extends `SplTransferParser._decode` with two-byte discriminator `[26, 1]` for `TransferCheckedWithFee`.

### Task 3.1: transferWithFee decode

**Files:**
- Modify: `core/parser/spl_transfer.py:15-16,60-90`
- Test: extend `tests/unit/test_spl_transfer_parser.py`

- [ ] **Step 1: Write failing tests**
  - disc `[26, 1]` with valid fee field → emits token_transfer with `args["fee"]`
  - disc `[26, 0]` (InitializeTransferFeeConfig) → skip
  - disc `[26, 1]` with truncated data → skip
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement** — add `_TRANSFER_CHECKED_WITH_FEE_OUTER = 26`, `_TRANSFER_CHECKED_WITH_FEE_SUB = 1`, handle in `_decode`: check `data[0] == 26 and data[1] == 1`, parse amount at `data[2:10]`, decimals at `data[10]`, fee at `data[11:19]`
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 3.2: Chunk 3 close-out
- [ ] Full suite + lint + typecheck

---

## Chunk 4: SPL non-Transfer operations

New `SplOpsParser` for Approve (disc 4), Revoke (disc 5), MintTo (disc 7), Burn (disc 8).

### Task 4.1: SplOpsParser implementation

**Files:**
- Create: `core/parser/spl_ops.py`
- Test: `tests/unit/test_spl_ops_parser.py` (new)

- [ ] **Step 1: Write failing tests** — one test per op (approve, revoke, mint_to, burn) + skip failed tx + skip unknown disc + both program IDs
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement** `SplOpsParser(chain_id)` — each op: base58-decode data, check disc byte, unpack amount where applicable, resolve mint from accounts/post_token_balances, emit `Event(kind="call", contract=mint, name=<op>)`
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 4.2: Wire into ChainRunner

**Files:**
- Modify: `apps/worker/chain_runner.py:81-89`

- [ ] **Step 1: Write failing test** — assert `SplOpsParser` in Solana pipeline
- [ ] **Step 2: Implement** — import and append after `SplTransferParser`
- [ ] **Step 3: Run, expect PASS**
- [ ] **Step 4: Commit**

### Task 4.3: Chunk 4 close-out
- [ ] Full suite + lint + typecheck

---

## Chunk 5: Channel config JSON-schema validation

Each Channel subclass declares `config_schema: ClassVar[dict]`. API validates at creation time.

### Task 5.1: Add jsonschema dependency

- [ ] **Step 1: Add** `"jsonschema>=4.20,<5"` to `pyproject.toml` dependencies
- [ ] **Step 2: `uv sync --extra dev`**
- [ ] **Step 3: Commit**

### Task 5.2: Extend `__init_subclass__` + add schemas to channels

**Files:**
- Modify: `core/notifier/channel.py:20-28` — check `config_schema` in `__init_subclass__`
- Modify: `core/notifier/http.py` — add `config_schema` ClassVar
- Modify: `core/notifier/redis_streams.py` — add `config_schema` ClassVar
- Modify: `core/notifier/websocket.py` — add `config_schema` ClassVar
- Test: extend `tests/unit/test_channel_registry.py`

- [ ] **Step 1: Write failing tests** — subclass without config_schema raises TypeError, each channel has config_schema attribute
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement** — extend hook, add schemas to each channel
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 5.3: API-layer validation

**Files:**
- Modify: `apps/web/routers/channels.py` — validate config against schema before create
- Test: extend `tests/unit/test_web_channels.py`

- [ ] **Step 1: Write failing tests** — POST with invalid config → 422, POST with valid config → 201
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement** — `jsonschema.validate(payload.config, CHANNEL_REGISTRY[payload.type].config_schema)` in the create endpoint, catch `ValidationError` → 422
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 5.4: Chunk 5 close-out
- [ ] Full suite + lint + typecheck

---

## Chunk 6: Per-channel retry policy

Channel config gains optional `retry: {max_attempts, base_delay, backoff_factor}`.

### Task 6.1: Channel base `_retry_config` property + channel updates

**Files:**
- Modify: `core/notifier/channel.py` — add `_retry_config` property
- Modify: `core/notifier/http.py` — store config, use `_retry_config` in `send()`
- Modify: `core/notifier/redis_streams.py` — same
- Modify: `core/notifier/websocket.py` — same
- Test: `tests/unit/test_channel_retry_config.py` (new)

- [ ] **Step 1: Write failing tests** — default retry config (3, 1.0, 4.0), custom override from config, partial override
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement**
  - `Channel._retry_config` property returns `tuple[int, float, float]` from `self._config.get("retry", {})`
  - Each channel stores `self._config = config` and unpacks `_retry_config` in `send()`
  - Update JSON schemas from chunk 5 with optional `retry` property
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 6.2: Chunk 6 close-out
- [ ] Full suite + lint + typecheck

---

## Chunk 7: Solana WS head subscription

`SolanaAdapter` uses `slotSubscribe` WS when `rpc_ws` is set, with fallback to HTTP polling.

### Task 7.1: Add `rpc_ws` to SolanaAdapter + WS subscribe_heads

**Files:**
- Modify: `core/chains/solana.py:30-42,163-178` — add `rpc_ws` param, WS mode in `subscribe_heads`
- Modify: `apps/worker/main.py:38-44` — pass `rpc_ws` to SolanaAdapter
- Test: `tests/unit/test_solana_adapter_ws.py` (new)

- [ ] **Step 1: Write failing tests**
  - `rpc_ws=None` → subscribe_heads uses HTTP polling (existing behavior)
  - `rpc_ws` set → subscribe_heads attempts WS, yields slots
  - WS disconnect → falls back to HTTP polling, logs warning
- [ ] **Step 2: Run, expect FAIL**
- [ ] **Step 3: Implement**
  - Constructor: `rpc_ws: str | None = None`
  - `subscribe_heads()`: if `self._rpc_ws` → `_ws_heads()`, else `_poll_heads()` (existing)
  - `_ws_heads()`: connect via `websockets.connect`, send `slotSubscribe`, yield slots. On disconnect: log, switch to `_poll_heads` with exponential backoff reconnect (1s→4s→16s→60s cap)
  - Worker factory: pass `rpc_ws=cfg.rpc_ws`
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Commit**

### Task 7.2: Chunk 7 close-out + tag `m3-complete`
- [ ] Full suite + lint + typecheck
- [ ] `git tag m3-complete`
