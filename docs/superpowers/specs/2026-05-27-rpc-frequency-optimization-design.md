# RPC Frequency Optimization (Package A) — Design

**Date**: 2026-05-27
**Status**: Draft
**Scope**: Reduce blockchain RPC call count and data transfer for EVM and Solana adapters
**Milestone**: post-m5 follow-up

## Background

The indexer's hottest cost center is RPC traffic to upstream chain nodes (Alchemy / Infura / self-hosted Geth / Solana validators). Profiling the current pipeline reveals three avoidable inefficiencies:

1. **EVM `eth_getLogs` is called per-block with no filters.** `core/chains/evm.py` issues `fetch_logs(n, n)` for every confirmed block and omits `addresses` / `topics`, so the node returns *every* log in the block. The indexer then filters client-side in `Matcher`.
2. **Catchup is fully serial.** `ChainRunner._catchup_evm` and `_catchup_solana` loop block-by-block; a 10k-block gap = 10k RTTs even though `eth_getLogs` natively supports `fromBlock`/`toBlock` ranges and Solana supports `getBlocks(start, end)`.
3. **Solana hits empty/skipped slots.** Every slot is fetched via `getBlock(slot)`; on mainnet 5–20% of slots are skipped and return `result: null`, wasting an RPC round trip each.

Hidden bonus: subscriptions with `match_kind ∈ {native_transfer, call}` do not need `eth_getLogs` at all (internal calls also surface with `match_kind="call"` since `InternalCallParser` emits `Event.kind="call"`). The current pipeline calls it unconditionally. The actual `MatchKind` enum is `{native_transfer, token_transfer, event, call}` — only `event` and `token_transfer` consume logs.

## Goals

- Reduce `eth_getLogs` data volume by 50–95% via RPC-side filtering (`addresses` + `topics`).
- Reduce `eth_getLogs` call count by 100–1000× on catchup via range queries.
- Eliminate `fetch_logs` calls entirely when the chain has no log-consuming subscriptions.
- Reduce Solana `getBlock` calls by ~5–20% on catchup by skipping empty slots via `getBlocks`.
- Catchup speed improvement is a *side effect*, not a primary goal of this design (handled by Package B in a future iteration).

## Non-goals

- Concurrent block fetching during catchup (Package B).
- `debug_traceBlockByNumber` consolidation (Package C).
- JSON-RPC batch requests (#5, low ROI).
- `_prefetch_ancestors_for` concurrency (#6, reorg-only, low frequency).
- Range merging during head-following (one block at a time, low ROI).
- Adaptive window sizing.
- Caching of topic0 keccak computations (snapshot rebuild is rare; volume is small).

## Architecture

```
┌─────────────────────────────┐
│ ConfigSnapshot              │
│   subscriptions, abis       │
└──────────────┬──────────────┘
               │ build on snapshot apply
               ▼
┌─────────────────────────────┐    used by    ┌────────────────────────┐
│ EvmLogFilterSet (per chain) │──────────────▶│ ChainRunner            │
│   addresses                 │               │   _catchup_evm         │
│   topic0s                   │               │   _process_confirmed.. │
│   skip_logs                 │               └─────────┬──────────────┘
└─────────────────────────────┘                         │
                                                        ▼
                                              ┌────────────────────────┐
                                              │ EvmAdapter.fetch_logs  │
                                              │   from, to, addresses, │
                                              │   topics               │
                                              └────────────────────────┘

┌─────────────────────────────┐
│ ChainRunner._catchup_solana │
│   for window in slot_range: │
│     valid = get_blocks()    │
│     for s in valid:         │
│       getBlock(s)           │
└─────────────────────────────┘
```

## Data Model Changes

Two new fields on `chains` table (Alembic migration `0006_rpc_range_config.py`):

| Column | Type | Default | Applies To | Purpose |
|--------|------|---------|------------|---------|
| `log_query_range_blocks` | `Integer NOT NULL` | `100` | EVM | Max block span per `eth_getLogs` call during catchup |
| `slot_query_range_blocks` | `Integer NOT NULL` | `1000` | Solana | Max slot span per `getBlocks` call during catchup (name uses "blocks" for symmetry with the EVM field and the `getBlocks` RPC method name; the unit is slots) |

Both fields are present on every row regardless of chain `kind` (no kind-conditional schema), but only consumed by the matching adapter type. This avoids a sparse schema or a JSON config blob.

Affected files:
- `core/config/models.py` — `Chain` model
- `core/config/snapshot.py` — `SnapshotChain`
- `core/config/repositories.py` — read/write through
- `apps/web/schemas.py` — request/response models for chains router
- `migrations/versions/0006_rpc_range_config.py` — new

## Component 1: `EvmLogFilterSet`

**Location**: `core/matcher/filter_set.py` (new file)

**Shape**:

```python
@dataclass(frozen=True)
class EvmLogFilterSet:
    addresses: list[str] | None      # lowercased hex; None = no RPC-side address filter
    topic0s: list[str] | None        # lowercased hex; None = no RPC-side topic filter
    skip_logs: bool                  # True when no subscription needs logs at all

    @property
    def topics_param(self) -> list[list[str]] | None:
        """Shape for eth_getLogs `topics` field: [[t0a, t0b, ...]] or None.

        Outer position 0 matches log topic 0 (event signature). The inner
        list is OR-of-topic0-candidates — RPC returns any log whose first
        topic is in the set.
        """
        return [self.topic0s] if self.topic0s else None


def build_evm_log_filter(
    snapshot: ConfigSnapshot,
    chain_id: str,
    abi_registry: AbiRegistry | None,
) -> EvmLogFilterSet:
    ...
```

**Builder logic**:

1. Collect `relevant = [s for s in snapshot.subscriptions_for_chain(chain_id) if s.match_kind in {"event", "token_transfer"}]`.
2. If `relevant == []` → return `EvmLogFilterSet(None, None, skip_logs=True)`.
3. **Address set**:
   - If any `s.address is None` in `relevant` → `addresses = None`.
   - Else → `addresses = sorted({s.address.lower() for s in relevant})`.
4. **Topic0 set**:
   - Start `topics = set()`.
   - For each `s in relevant`:
     - `match_kind == "token_transfer"` → add `ERC20_TRANSFER_TOPIC0`.
     - `match_kind == "event"`:
       - If `s.abi_id is None` or `s.match_name is None` → return early with `topic0s = None` (cannot compute; fall back to full topic scan).
       - Look up event signature in `abi_registry`; if not found → `topic0s = None` (consistent with user's "any miss → drop topic filter" decision).
       - Compute the topic0 via the existing `event_topic0(...)` helper in `core/abi/decoder.py` (keccak256 of the canonical event signature, already 32 bytes — no slicing).
   - `topic0s = sorted(topics)` if not bailed.
5. Return `EvmLogFilterSet(addresses, topic0s, skip_logs=False)`.

**Behavior table** (already approved in brainstorm):

| Subscription mix | addresses | topic0s | skip_logs |
|------------------|-----------|---------|-----------|
| No event/token_transfer subs | — | — | **True** |
| All relevant subs have `address` | concrete list | derived | False |
| Any relevant sub has `address=None` | **None** | derived | False |
| All events have abi_id + match_name (computable) | concrete | computed + ERC20 | False |
| Any event missing abi_id / match_name / signature lookup | concrete | **None** | False |
| Both missing | None | None | False (range query still used in catchup, saves per-block RTTs) |

**Helpers needed in `AbiRegistry`**: existing `lookup_event_by_topic0` is reverse direction. We need forward: signature → topic0 hash. If not exposed, add `event_signature_to_topic0(abi_id, event_name) -> str | None`.

## Component 2: `EvmAdapter.fetch_logs` topics parameter

**Location**: `core/chains/evm.py`

**Current signature**:
```python
async def fetch_logs(self, from_block, to_block, addresses=None) -> list[Log]
```

**New signature**:
```python
async def fetch_logs(
    self,
    from_block: int,
    to_block: int,
    addresses: list[str] | None = None,
    topics: list[list[str]] | None = None,   # eth_getLogs topics shape
) -> list[Log]
```

Threads `topics` into `params` dict before the `get_logs` call. Existing call sites without `topics` are unaffected.

**Backward compatibility**: None needed — internal API, single caller (`ChainRunner`).

## Component 3: `ChainRunner` filter integration

**Location**: `apps/worker/chain_runner.py`

**State additions**:
```python
self._evm_filter: EvmLogFilterSet | None = None  # EVM chains only
```

**Build/rebuild points**:
- `start(snap)`: build filter after `_current_snap = snap`.
- `apply_snapshot(snap)`: rebuild filter under `self._snap_lock`.

For Solana chains the filter stays `None` and is never read.

**Head-following path** (`_process_confirmed_block`):

```python
filter = self._evm_filter
assert filter is not None

block_coro = self._adapter.fetch_block(number)
if filter.skip_logs:
    block = await block_coro
    logs = []
else:
    logs_coro = self._adapter.fetch_logs(
        number, number,
        addresses=filter.addresses,
        topics=filter.topics_param,
    )
    block, logs = await asyncio.gather(block_coro, logs_coro)

# Delegate to the inner helper (see Refactoring contract below)
await self._process_block_with_prefetched_logs(
    number, block, logs, matcher=matcher, notifier=notifier,
)
```

**Catchup path** (`_catchup_evm`) — replaces the per-block loop:

```python
range_blocks = self._chain.log_query_range_blocks
filter = self._evm_filter
assert filter is not None

start_n = last_block + 1
processed = 0
while start_n <= safe_tip:
    end_n = min(start_n + range_blocks - 1, safe_tip)
    try:
        if filter.skip_logs:
            logs_by_block: dict[int, list[Log]] = {}
        else:
            logs_by_block = await self._fetch_logs_with_degrade(
                start_n, end_n, filter,
            )
        for n in range(start_n, end_n + 1):
            if self._stop.is_set():
                return
            block = await self._adapter.fetch_block(n)
            await self._process_block_with_prefetched_logs(
                n, block, logs_by_block.get(n, []),
                matcher=matcher, notifier=notifier,
            )
            processed += 1
            if processed % 100 == 0:
                log.info("chain_runner.catchup_progress",
                         chain_id=self._chain.id, block=n, remaining=safe_tip - n)
    except Exception:  # noqa: BLE001 — preserves existing _catchup_evm break-on-error
        log.error("chain_runner.catchup_window_failed",
                  chain_id=self._chain.id, start=start_n, end=end_n)
        break
    start_n = end_n + 1
```

The inner helper signature: `_process_block_with_prefetched_logs(number: int, block: Block, prefetched_logs: list[Log], *, matcher, notifier) -> None`. Both head-following and catchup paths fetch the block themselves and pass it in; the helper owns parsing, trace handling, matcher, notifier, and checkpoint save.

**New helper**: `_process_block_with_prefetched_logs(n, prefetched_logs, ...)` is a refactor of `_process_confirmed_block` that accepts `logs` directly instead of calling `fetch_logs(n, n)`.

**Refactoring contract**:
- `_process_confirmed_block` stays as the **head-following entrypoint** — it owns the `asyncio.gather(block_coro, logs_coro)` concurrency (or single fetch when `skip_logs`), then calls `_process_block_with_prefetched_logs(n, block, logs, ...)` with both resolved.
- Catchup calls the same inner helper, passing the per-block `fetch_block` result plus the bucketed log slice (no per-block `fetch_logs`).
- The inner helper owns parser dispatch, trace handling, matcher, notifier, checkpoint save — only the `block`+`logs` sourcing differs between callers.

**Degradation helper** `_fetch_logs_with_degrade`:

```python
DEGRADE_ERR_HINTS = ("too large", "result too big", "query timeout",
                     "limit exceeded", "Returned more than")

async def _fetch_logs_with_degrade(
    self, start: int, end: int, filter: EvmLogFilterSet,
) -> dict[int, list[Log]]:
    try:
        logs = await self._adapter.fetch_logs(
            start, end,
            addresses=filter.addresses, topics=filter.topics_param,
        )
        return _bucket_by_block(logs)
    except Exception as exc:
        msg = str(exc).lower()
        if any(h in msg for h in DEGRADE_ERR_HINTS):
            if end > start:
                mid = (start + end) // 2
                left = await self._fetch_logs_with_degrade(start, mid, filter)
                right = await self._fetch_logs_with_degrade(mid + 1, end, filter)
                return {**left, **right}
            # Single-block floor still failing — log and re-raise. This can
            # happen on extremely active contracts (e.g. Polygon archive nodes
            # have been observed returning "too large" for a single block).
            log.error("chain_runner.fetch_logs_single_block_too_large",
                      chain_id=self._chain.id, block=start)
        raise
```

`_bucket_by_block` derives `block_number` from each `Log`:

```python
def _bucket_by_block(logs: list[Log]) -> dict[int, list[Log]]:
    out: dict[int, list[Log]] = {}
    for lg in logs:
        out.setdefault(lg.block_number, []).append(lg)
    return out
```

**Schema dependency**: `Log` does not currently carry `block_number` — we must add it. Audit shows `Log` (in `core/chains/types.py`) has `tx_hash, log_index, address, topics, data`. Need to:

- Extend `Log` with `block_number: int`.
- Update `EvmAdapter.fetch_logs` parsing to populate `block_number` from `lg["blockNumber"]`.
- Audit callers of `Log` to ensure additive field is safe (it is — `frozen=True` dataclass with required positional arg; we use keyword everywhere in adapter, but check the wider codebase before commit).

## Component 4: Solana `get_blocks` + catchup

**Location**: `core/chains/solana.py`

**New method**:

```python
async def get_blocks(self, start_slot: int, end_slot: int) -> list[int]:
    """Return slot numbers in [start_slot, end_slot] that contain confirmed blocks.

    Skipped/empty slots are excluded. Uses RPC `getBlocks(start, end)`.
    Range size must respect server limit (`slot_query_range_blocks` controls
    the caller-side window; this method does not internally chunk).
    """
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "getBlocks",
        "params": [start_slot, end_slot, {"commitment": "finalized"}],
    }
    resp = await self._client.post(self._rpc_url, json=payload, ...)
    result = resp.json().get("result")
    return list(result or [])
```

**Commitment caveat**: `getBlocks` is officially supported for `finalized` commitment. `confirmed` works on most providers but may return empty on stricter ones. We pass `finalized` regardless of the chain's configured commitment, *only* for the slot-discovery step. The subsequent `getBlock` calls still use the chain's configured commitment.

**Catchup change** (`_catchup_solana`):

```python
SOL_RANGE_TOO_LARGE_HINTS = ("exceeds maximum", "too large", "limit exceeded")

async def _get_blocks_classified(self, start: int, end: int) -> list[int]:
    """Wrap adapter.get_blocks, classify errors:
       - size-limit class (hint match) → raise (config error)
       - other transient errors → log + return dense range as fallback
    """
    try:
        return await self._adapter.get_blocks(start, end)
    except Exception as exc:
        msg = str(exc).lower()
        if any(h in msg for h in SOL_RANGE_TOO_LARGE_HINTS):
            log.error("chain_runner.get_blocks_range_too_large",
                      chain_id=self._chain.id, start=start, end=end)
            raise  # config error: operator must lower slot_query_range_blocks
        log.warning("chain_runner.get_blocks_failed",
                    chain_id=self._chain.id, start=start, end=end, error=str(exc))
        return list(range(start, end + 1))  # dense fallback

range_slots = self._chain.slot_query_range_blocks  # default 1000
start_s = last_slot + 1
while start_s <= tip:
    end_s = min(start_s + range_slots - 1, tip)
    valid = await self._get_blocks_classified(start_s, end_s)
    for s in valid:
        if self._stop.is_set():
            return
        try:
            await self._process_solana_slot(s)
        except Exception:  # match existing per-slot error tolerance
            log.error("chain_runner.catchup_slot_failed",
                      chain_id=self._chain.id, slot=s)
            continue
    start_s = end_s + 1
```

Head-following Solana path is unchanged (one slot at a time).

## Error Handling

| Failure | Behavior |
|---------|----------|
| `eth_getLogs` returns "too large"-class error, range > 1 | Binary-split the window |
| `eth_getLogs` returns "too large"-class error, single block | Log error + propagate (no degrade path remaining) |
| `eth_getLogs` returns network error | Propagate; `ChainRunner` catches and aborts current catchup iteration (existing behavior) |
| `getBlocks` returns "range too large" / size-limit error | Treat as config error: log + raise (this is a misconfigured `slot_query_range_blocks`, not a transient failure) |
| `getBlocks` returns network / transient error | Log warning + degrade to dense iteration over `[start, end]` for that window only |
| Topic0 keccak computation fails | Builder drops topic filter for entire chain (consistent with approved policy) |
| `Log.block_number` mismatch with bucket key | Skip the log with warning (defensive; should never happen with conformant RPCs) |

## Testing Strategy

**Unit tests**:

- `tests/unit/test_filter_set.py` — 7 cases (one per behavior-table row + ERC20 topic0 + abi registry hit/miss).
- `tests/unit/test_evm_fetch_logs_topics.py` — mock httpx provider, verify `topics` and `address` keys appear/absent in JSON-RPC body for each filter shape.
- `tests/unit/test_evm_catchup_range.py` — mock adapter, count calls; verify N-block gap → ceil(N/range_blocks) `fetch_logs` calls and N `fetch_block` calls.
- `tests/unit/test_evm_fetch_logs_degrade.py` — adapter raising "too large" once, verify bisection and eventual success.
- `tests/unit/test_evm_skip_logs.py` — chain with only native_transfer subs, verify `fetch_logs` never called.
- `tests/unit/test_solana_get_blocks.py` — mock client, verify only valid slots get `fetch_block`.
- `tests/unit/test_solana_catchup_range.py` — full catchup with mixed valid/skipped slots.

**Integration tests** (in `tests/e2e`, gated by `@pytest.mark.e2e`):

- `test_evm_catchup_anvil_logs_filtered.py` — anvil with multiple contracts; only one subscribed; verify catchup correctness + reduced `eth_getLogs` calls via anvil request log.
- `test_solana_catchup_skips_empty.py` — solana-test-validator with engineered skipped slots (or mock).

**Regression**: existing `tests/e2e/test_anvil_e2e.py` and `tests/e2e/test_solana_e2e.py` must still pass without modification.

## Implementation Order

Each step is independently committable, testable, and adds no broken state in between.

1. **Schema migration + model**
   - Alembic `0006_rpc_range_config.py`
   - `Chain` model: `log_query_range_blocks` (Integer NOT NULL DEFAULT 100), `slot_query_range_blocks` (Integer NOT NULL DEFAULT 1000)
   - **Migration MUST use `server_default="100"` / `server_default="1000"` on `op.add_column(...)`** (mirroring the pattern in `0003_failed_deliveries.py`). Without `server_default`, adding a NOT NULL column to an existing populated table fails on both SQLite and Postgres because SQLAlchemy `default=` is Python-side only and does not produce DDL. The Python-side `default=` is also kept on the model so new in-Python instantiations work.
   - Default values applied to existing rows on migration via `server_default`
2. **Snapshot + repos + web schema wiring**
   - `SnapshotChain` propagates both fields
   - `ChainRepo` reads/writes
   - `apps/web/schemas.py` request/response models
   - `apps/web/routers/chains.py` accepts on POST/PATCH
   - `web/src/pages/Chains.tsx` form input (optional; API-only is enough for first cut)
3. **`Log.block_number` field**
   - Add to dataclass as a **required field** (no default — we want a hard invariant that any constructed `Log` knows its origin block).
   - Populate in `EvmAdapter.fetch_logs` from `lg["blockNumber"]` (web3.py returns it as int).
   - **Audit and update ALL `Log(...)` call sites** (both production and test). Known affected test files (grep for `Log(` in `tests/`):
     - `tests/unit/test_chain_runner.py`
     - `tests/unit/test_erc20_parser.py`
     - `tests/unit/test_abi_event_parser.py`
     - `tests/unit/test_chain_types.py`
   - Each test fixture must supply a sensible `block_number` (typically matches the block header's number). This is mechanical work but cannot be skipped.
4. **`EvmLogFilterSet` + builder**
   - New file + unit tests
5. **`EvmAdapter.fetch_logs` topics parameter**
   - Signature change + unit test for parameter passthrough
6. **`ChainRunner` head-following filter integration**
   - Build filter in `start` / `apply_snapshot`; use in `_process_confirmed_block`
   - Verify existing tests still pass
7. **`ChainRunner._catchup_evm` range query + degradation**
   - `_bucket_by_block` helper + `_fetch_logs_with_degrade` helper + main catchup loop refactor
   - **Also restructures `_process_confirmed_block` per the refactoring contract**: splits out `_process_block_with_prefetched_logs(n, prefetched_logs, ...)` as the inner helper; head-following entrypoint owns the fetch concurrency
   - New unit tests
8. **`SolanaAdapter.get_blocks`**
   - Method + unit test
9. **`ChainRunner._catchup_solana` range + skip integration**
   - Main loop refactor + degrade fallback
   - New unit tests
10. **E2E coverage**
    - anvil + solana validator scenarios
11. **Docs**: update `CLAUDE.md` quick reference if any new config knob mentioned

**Trace path note**: `_process_confirmed_block` also conditionally invokes `trace_block(number)` when `chain.trace_internal_calls=True`. This path is independent of logs and remains unchanged — trace events are produced from EVM call traces, not from `eth_getLogs` responses.

## Rollout / Safety

- All changes are behind existing config keys with sensible defaults — no opt-in flag needed.
- Per-chain `log_query_range_blocks=1` reverts EVM behavior to per-block (escape hatch for misbehaving RPC providers).
- Per-chain `slot_query_range_blocks=1` similarly degrades Solana to current behavior.
- No data migration of historical events; checkpoint resume continues to work unchanged.

## Open Questions

None at design time. Implementation may surface RPC-provider-specific quirks (e.g., Infura/Alchemy topic param shape strictness); those become bug fixes, not design changes.

## File-by-file Change Summary

| File | Change | Approx LOC |
|------|--------|-----------|
| `migrations/versions/0006_rpc_range_config.py` | new | ~30 |
| `core/config/models.py` | +2 fields | +4 |
| `core/config/snapshot.py` | +2 fields, propagation | +6 |
| `core/config/repositories.py` | read/write fields | +6 |
| `apps/web/schemas.py` | request/response fields | +4 |
| `apps/web/routers/chains.py` | accept new fields on POST/PATCH | +6 |
| `web/src/pages/Chains.tsx` | form inputs (optional, can lag) | +20 |
| `core/chains/types.py` | `Log.block_number` | +1 |
| `core/chains/evm.py` | `fetch_logs(topics)` + `block_number` population | +10 |
| `core/chains/solana.py` | `get_blocks()` method | +30 |
| `core/matcher/filter_set.py` | new | ~80 |
| `apps/worker/chain_runner.py` | filter wiring + catchup refactor | +120 |
| `tests/unit/test_filter_set.py` | new | ~120 |
| `tests/unit/test_evm_fetch_logs_topics.py` | new | ~60 |
| `tests/unit/test_evm_catchup_range.py` | new | ~80 |
| `tests/unit/test_evm_fetch_logs_degrade.py` | new | ~50 |
| `tests/unit/test_evm_skip_logs.py` | new | ~40 |
| `tests/unit/test_solana_get_blocks.py` | new | ~40 |
| `tests/unit/test_solana_catchup_range.py` | new | ~60 |
| `tests/e2e/test_evm_catchup_anvil_logs_filtered.py` | new | ~100 |
| `tests/e2e/test_solana_catchup_skips_empty.py` | new | ~80 |

**Total**: ~950 LOC including tests; ~340 LOC of production code.
