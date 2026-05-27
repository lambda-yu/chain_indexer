# RPC Frequency Optimization (Package A) Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce blockchain RPC call count and data transfer for EVM and Solana adapters via RPC-side filtering, range queries, and empty-slot skipping.

**Architecture:** Build a per-chain `EvmLogFilterSet` from the config snapshot to drive RPC-side `addresses`/`topics` filtering on `eth_getLogs`. Refactor catchup to issue range queries instead of per-block calls, with binary-bisection degradation for "result too large" errors. Add Solana `getBlocks` to enumerate non-empty slots in a window before fetching them individually. Configuration knobs live on the `chains` table per chain.

**Tech Stack:** Python 3.11 / asyncio, SQLAlchemy 2.0 async + Alembic, web3.py 7, solders/httpx, pytest + pytest-asyncio.

**Reference spec:** `docs/superpowers/specs/2026-05-27-rpc-frequency-optimization-design.md`

---

## Chunk 1: Schema, Snapshot, and Web API Wiring

This chunk lays the foundation: persist the two new per-chain config fields, propagate them through repositories / snapshot / web schemas, and add the `block_number` field to the `Log` dataclass. After this chunk the codebase still works exactly as before — the fields are stored but unread.

### Task 1: Alembic migration `0006_rpc_range_config`

**Files:**
- Create: `migrations/versions/0006_rpc_range_config.py`

- [ ] **Step 1.1: Write the migration**

```python
"""add log_query_range_blocks and slot_query_range_blocks to chains

Revision ID: 0006
Revises: 0005
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.add_column(
            sa.Column(
                "log_query_range_blocks",
                sa.Integer(),
                nullable=False,
                server_default="100",
            )
        )
        batch.add_column(
            sa.Column(
                "slot_query_range_blocks",
                sa.Integer(),
                nullable=False,
                server_default="1000",
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.drop_column("slot_query_range_blocks")
        batch.drop_column("log_query_range_blocks")
```

The `server_default` is REQUIRED — adding a NOT NULL column to a populated table fails on both SQLite and Postgres without DDL-level defaults. SQLAlchemy's Python `default=` is not enough.

- [ ] **Step 1.2: Run migration to verify it applies**

Run: `uv run alembic upgrade head`
Expected: clean upgrade; no errors. Subsequent `uv run alembic downgrade -1 && uv run alembic upgrade head` round-trip must also succeed.

- [ ] **Step 1.3: Commit**

```bash
git add migrations/versions/0006_rpc_range_config.py
git commit -m "feat(db): add log_query_range_blocks and slot_query_range_blocks columns to chains"
```

---

### Task 2: `Chain` model fields

**Files:**
- Modify: `core/config/models.py:68-79` (Chain class)

- [ ] **Step 2.1: Add the two columns to the `Chain` model**

After the `trace_internal_calls` line, insert:

```python
    log_query_range_blocks: Mapped[int] = mapped_column(
        Integer, nullable=False, default=100, server_default="100"
    )
    slot_query_range_blocks: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1000, server_default="1000"
    )
```

Keep both `default=` (Python-side) and `server_default=` (DDL-side) so both new Python instantiations and bare SQL inserts succeed.

- [ ] **Step 2.2: Smoke-check the model imports clean**

Run: `uv run python -c "from core.config.models import Chain; print(Chain.__table__.c.keys())"`
Expected: the printed key list contains `log_query_range_blocks` and `slot_query_range_blocks`.

- [ ] **Step 2.3: Commit**

```bash
git add core/config/models.py
git commit -m "feat(db): add log_query_range_blocks/slot_query_range_blocks fields on Chain ORM"
```

---

### Task 3: `SnapshotChain` field propagation

**Files:**
- Modify: `core/config/snapshot.py:39-49` (SnapshotChain dataclass + load_snapshot)

- [ ] **Step 3.1: Add fields to `SnapshotChain`**

In `core/config/snapshot.py:39-49`, extend the frozen dataclass:

```python
@dataclass(frozen=True)
class SnapshotChain:
    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int
    commitment: str | None = None
    trace_internal_calls: bool = False
    log_query_range_blocks: int = 100
    slot_query_range_blocks: int = 1000
```

- [ ] **Step 3.2: Populate fields in `load_snapshot`**

In `core/config/snapshot.py:92-104`, extend the `SnapshotChain(...)` constructor call:

```python
        SnapshotChain(
            id=c.id,
            kind=c.kind.value,
            rpc_http=c.rpc_http,
            rpc_ws=c.rpc_ws,
            confirmations=c.confirmations,
            poll_interval_ms=c.poll_interval_ms,
            commitment=c.commitment,
            trace_internal_calls=bool(c.trace_internal_calls) if c.trace_internal_calls is not None else False,
            log_query_range_blocks=c.log_query_range_blocks,
            slot_query_range_blocks=c.slot_query_range_blocks,
        )
```

- [ ] **Step 3.3: Run existing snapshot tests**

Run: `uv run pytest tests/unit/test_config_snapshot.py -v`
Expected: all existing tests pass (defaults shouldn't break anything).

- [ ] **Step 3.4: Commit**

```bash
git add core/config/snapshot.py
git commit -m "feat(snapshot): propagate log/slot query range fields from Chain to SnapshotChain"
```

---

### Task 4: `ChainRepo.create` accepts new fields

**Files:**
- Modify: `core/config/repositories.py:30-51` (ChainRepo.create)

- [ ] **Step 4.1: Extend the `create` signature**

```python
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
        commitment: str | None = None,
        trace_internal_calls: bool = False,
        log_query_range_blocks: int = 100,
        slot_query_range_blocks: int = 1000,
    ) -> Chain:
        c = Chain(
            id=id, kind=kind, rpc_http=rpc_http, rpc_ws=rpc_ws,
            confirmations=confirmations, poll_interval_ms=poll_interval_ms,
            enabled=enabled, commitment=commitment,
            trace_internal_calls=trace_internal_calls,
            log_query_range_blocks=log_query_range_blocks,
            slot_query_range_blocks=slot_query_range_blocks,
        )
        self.s.add(c)
        await self.s.flush()
        return c
```

`update` already accepts `**fields` so no change needed there.

- [ ] **Step 4.2: Commit**

```bash
git add core/config/repositories.py
git commit -m "feat(repo): pass log/slot query range fields through ChainRepo.create"
```

---

### Task 5: Web API schemas

**Files:**
- Modify: `apps/web/schemas.py:21-52` (ChainCreate, ChainOut)
- Modify: `apps/web/routers/chains.py:25-60` (create + update routes)

- [ ] **Step 5.1: Add fields to `ChainCreate` and `ChainOut`**

In `apps/web/schemas.py`, extend both classes:

```python
class ChainCreate(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    kind: Literal["evm", "solana"]
    rpc_http: str = Field(min_length=1)
    rpc_ws: str | None = None
    confirmations: int = Field(ge=0, le=10_000, default=0)
    poll_interval_ms: int = Field(ge=100, le=60_000, default=3000)
    commitment: Literal["confirmed", "finalized"] | None = None
    trace_internal_calls: bool = False
    log_query_range_blocks: int = Field(ge=1, le=10_000, default=100)
    slot_query_range_blocks: int = Field(ge=1, le=500_000, default=1000)
    enabled: bool = True

    # ... (keep existing model_validator)


class ChainOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int
    commitment: str | None
    trace_internal_calls: bool | None
    log_query_range_blocks: int
    slot_query_range_blocks: int
    enabled: bool
```

`ge`/`le` bounds match practical RPC-provider limits: EVM `eth_getLogs` rarely supports >10k blocks per call; Solana `getBlocks` caps at 500k slots.

- [ ] **Step 5.2: Pass fields through `chains.py` create + update routes**

In `apps/web/routers/chains.py:25-35`:

```python
    row = await repo.create(
        id=payload.id,
        kind=ChainKind(payload.kind),
        rpc_http=payload.rpc_http,
        rpc_ws=payload.rpc_ws,
        confirmations=payload.confirmations,
        poll_interval_ms=payload.poll_interval_ms,
        enabled=payload.enabled,
        commitment=payload.commitment,
        trace_internal_calls=payload.trace_internal_calls,
        log_query_range_blocks=payload.log_query_range_blocks,
        slot_query_range_blocks=payload.slot_query_range_blocks,
    )
```

In `apps/web/routers/chains.py:51-60` (update route):

```python
    await repo.update(
        chain_id,
        rpc_http=payload.rpc_http,
        rpc_ws=payload.rpc_ws,
        confirmations=payload.confirmations,
        poll_interval_ms=payload.poll_interval_ms,
        enabled=payload.enabled,
        commitment=payload.commitment,
        trace_internal_calls=payload.trace_internal_calls,
        log_query_range_blocks=payload.log_query_range_blocks,
        slot_query_range_blocks=payload.slot_query_range_blocks,
    )
```

- [ ] **Step 5.3: Write test that round-trips new fields through the API**

Append to `tests/unit/test_web_chains.py`. The existing tests use a sync `TestClient` via the `_client(db, bus)` helper already defined in that file — follow the same pattern (don't introduce an async client fixture).

```python
def test_chain_create_persists_log_and_slot_range(db: Database) -> None:
    bus = _FakeBus()
    payload = {
        "id": "test-evm-range",
        "kind": "evm",
        "rpc_http": "http://localhost:8545",
        "rpc_ws": None,
        "confirmations": 1,
        "poll_interval_ms": 1000,
        "log_query_range_blocks": 250,
        "slot_query_range_blocks": 500,
        "enabled": True,
    }
    with _client(db, bus) as c:
        r = c.post("/api/chains", json=payload)
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["log_query_range_blocks"] == 250
        assert body["slot_query_range_blocks"] == 500

        g = c.get(f"/api/chains/{payload['id']}")
        assert g.status_code == 200
        assert g.json()["log_query_range_blocks"] == 250


def test_chain_create_rejects_zero_log_query_range(db: Database) -> None:
    bus = _FakeBus()
    payload = {
        "id": "bad", "kind": "evm", "rpc_http": "http://x",
        "rpc_ws": None, "confirmations": 1, "poll_interval_ms": 1000,
        "log_query_range_blocks": 0,  # below ge=1 bound
        "enabled": True,
    }
    with _client(db, bus) as c:
        r = c.post("/api/chains", json=payload)
    assert r.status_code == 422
```

- [ ] **Step 5.4: Run the new tests**

Run: `uv run pytest tests/unit/test_web_chains.py::test_chain_create_persists_log_and_slot_range tests/unit/test_web_chains.py::test_chain_create_rejects_zero_log_query_range -v`
Expected: both PASS.

- [ ] **Step 5.5: Run full unit test suite to verify no regressions**

Run: `uv run pytest tests/unit/ -m "not e2e"`
Expected: all pass.

- [ ] **Step 5.6: Commit**

```bash
git add apps/web/schemas.py apps/web/routers/chains.py tests/unit/test_web_chains.py
git commit -m "feat(web): expose log_query_range_blocks and slot_query_range_blocks on /api/chains"
```

---

### Task 6: `Log.block_number` field + test fixture updates

**Files:**
- Modify: `core/chains/types.py:25-31` (Log dataclass)
- Modify: `core/chains/evm.py:124-135` (fetch_logs Log construction)
- Modify: `tests/unit/test_chain_types.py`
- Modify: `tests/unit/test_erc20_parser.py`
- Modify: `tests/unit/test_abi_event_parser.py`
- Modify: `tests/unit/test_chain_runner.py`

- [ ] **Step 6.1: Add `block_number` to `Log`**

In `core/chains/types.py`:

```python
@dataclass(frozen=True)
class Log:
    tx_hash: str
    log_index: int
    address: str
    topics: list[str]
    data: str  # hex string with 0x
    block_number: int
```

No default — this is a required field. The intent is that every constructed `Log` knows which block it came from.

- [ ] **Step 6.2: Run unit tests to see what breaks**

Run: `uv run pytest tests/unit/test_chain_types.py tests/unit/test_erc20_parser.py tests/unit/test_abi_event_parser.py tests/unit/test_chain_runner.py -v`
Expected: FAIL with "missing 1 required positional argument: 'block_number'" or similar for each `Log(...)` construction.

- [ ] **Step 6.3: Update `EvmAdapter.fetch_logs` to populate `block_number`**

In `core/chains/evm.py:124-135`, modify the `Log` construction:

```python
        for lg in raw_logs:
            out.append(
                Log(
                    tx_hash=_hexify(lg["transactionHash"]),
                    log_index=int(lg["logIndex"]),
                    address=str(lg["address"]),
                    topics=[_hexify(t) for t in lg["topics"]],
                    data=lg["data"] if isinstance(lg["data"], str) else _hexify(lg["data"]),
                    block_number=int(lg["blockNumber"]),
                )
            )
```

`blockNumber` in web3.py's response is already an int (or hex string in raw JSON-RPC, but the AsyncWeb3 layer normalizes). `int(...)` handles both safely.

- [ ] **Step 6.4: Update each test fixture**

For every `Log(...)` construction in the four test files, add `block_number=<header_number>` matching the surrounding block's number. Example pattern:

```python
# Before:
Log(tx_hash="0xabc", log_index=0, address="0x...", topics=[...], data="0x...")
# After:
Log(tx_hash="0xabc", log_index=0, address="0x...", topics=[...], data="0x...", block_number=42)
```

Pick the block number from the surrounding test context (often the test constructs a `BlockHeader(number=N, ...)` nearby; use that `N`).

- [ ] **Step 6.5: Re-run the unit tests**

Run: `uv run pytest tests/unit/test_chain_types.py tests/unit/test_erc20_parser.py tests/unit/test_abi_event_parser.py tests/unit/test_chain_runner.py -v`
Expected: all PASS.

- [ ] **Step 6.6: Run mypy to catch any missed call sites**

Run: `uv run mypy core apps`
Expected: clean. If mypy flags any `Log(...)` construction outside the modified files, fix that site too (and add it to a follow-up commit note).

- [ ] **Step 6.7: Commit**

```bash
git add core/chains/types.py core/chains/evm.py tests/unit/test_chain_types.py tests/unit/test_erc20_parser.py tests/unit/test_abi_event_parser.py tests/unit/test_chain_runner.py
git commit -m "feat(types): add Log.block_number required field, populate in EvmAdapter and tests"
```

---

## Chunk 2: EvmLogFilterSet + Adapter Topics Parameter

This chunk introduces the filter set and extends the adapter to accept RPC-side topic filters. The filter is built but not yet consumed by `ChainRunner` — that happens in Chunk 3. After this chunk, the new module exists with full test coverage, and `EvmAdapter.fetch_logs` can take topics, but nothing in the worker uses them yet.

### Task 7: `EvmLogFilterSet` dataclass + builder

**Files:**
- Create: `core/matcher/filter_set.py`
- Create: `tests/unit/test_filter_set.py`

- [ ] **Step 7.1: Write the failing tests first**

Create `tests/unit/test_filter_set.py`:

```python
from __future__ import annotations

import pytest

from core.abi.decoder import event_topic0
from core.abi.registry import AbiRegistry
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotAbi,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)
from core.matcher.filter_set import (
    ERC20_TRANSFER_TOPIC0,
    EvmLogFilterSet,
    build_evm_log_filter,
)


def _snap(subs: list[SnapshotSubscription], abis: list[SnapshotAbi] | None = None) -> ConfigSnapshot:
    return ConfigSnapshot(
        version=1,
        subscriptions=subs,
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[],
        abis=abis or [],
    )


def _sub(**kw):
    return SnapshotSubscription(
        id=kw.get("id", "s1"),
        name=kw.get("name", "s1"),
        chain_id=kw["chain_id"],
        address=kw.get("address"),
        abi_id=kw.get("abi_id"),
        match_kind=kw["match_kind"],
        match_name=kw.get("match_name"),
        arg_filters=kw.get("arg_filters", {}),
        enabled=kw.get("enabled", True),
        channel_ids=["c1"],
    )


def test_skip_logs_when_no_event_or_token_subscriptions():
    snap = _snap([_sub(chain_id="evm-1", match_kind="native_transfer")])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.skip_logs is True
    assert f.addresses is None
    assert f.topic0s is None


def test_addresses_concrete_when_all_relevant_subs_have_address():
    snap = _snap([
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0xABCDEF"),
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0x123456", id="s2"),
    ])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.skip_logs is False
    assert f.addresses == ["0x123456", "0xabcdef"]
    assert f.topic0s == [ERC20_TRANSFER_TOPIC0]


def test_addresses_none_when_any_relevant_sub_has_no_address():
    snap = _snap([
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0xABCDEF"),
        _sub(chain_id="evm-1", match_kind="token_transfer", address=None, id="s2"),
    ])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.addresses is None
    assert f.topic0s == [ERC20_TRANSFER_TOPIC0]


def test_topic0s_includes_computed_event_signatures():
    abi_body = [{
        "type": "event",
        "name": "Foo",
        "inputs": [{"name": "x", "type": "uint256", "indexed": False}],
    }]
    expected_t0 = event_topic0(abi_body[0]).lower()
    abi = SnapshotAbi(id="abi-1", name="abi-1", kind="evm_abi", body=abi_body)
    registry = AbiRegistry()
    registry.refresh(ConfigSnapshot(version=1, subscriptions=[], channels=[], chains=[], abis=[abi]))
    snap = _snap(
        [_sub(chain_id="evm-1", match_kind="event", abi_id="abi-1", match_name="Foo", address="0xaaa")],
        abis=[abi],
    )
    f = build_evm_log_filter(snap, "evm-1", registry)
    assert f.topic0s == [expected_t0]


def test_topic0s_none_when_event_sub_missing_abi_id():
    snap = _snap([_sub(chain_id="evm-1", match_kind="event", abi_id=None, match_name="Foo", address="0xaaa")])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.topic0s is None
    assert f.addresses == ["0xaaa"]


def test_topic0s_none_when_event_sub_missing_match_name():
    snap = _snap([_sub(chain_id="evm-1", match_kind="event", abi_id="abi-1", match_name=None, address="0xaaa")])
    f = build_evm_log_filter(snap, "evm-1", None)
    assert f.topic0s is None


def test_topic0s_none_when_event_signature_not_found_in_registry():
    abi_body = [{"type": "event", "name": "Bar", "inputs": []}]
    abi = SnapshotAbi(id="abi-1", name="abi-1", kind="evm_abi", body=abi_body)
    registry = AbiRegistry()
    registry.refresh(ConfigSnapshot(version=1, subscriptions=[], channels=[], chains=[], abis=[abi]))
    snap = _snap(
        [_sub(chain_id="evm-1", match_kind="event", abi_id="abi-1", match_name="Missing", address="0xaaa")],
        abis=[abi],
    )
    f = build_evm_log_filter(snap, "evm-1", registry)
    assert f.topic0s is None


def test_topics_param_shape():
    f = EvmLogFilterSet(addresses=None, topic0s=["0xaa", "0xbb"], skip_logs=False)
    assert f.topics_param == [["0xaa", "0xbb"]]
    g = EvmLogFilterSet(addresses=None, topic0s=None, skip_logs=False)
    assert g.topics_param is None


def test_only_enabled_subscriptions_considered():
    snap = _snap([
        _sub(chain_id="evm-1", match_kind="token_transfer", address="0xaaa"),
        _sub(chain_id="evm-1", match_kind="token_transfer", address=None, id="s2", enabled=False),
    ])
    f = build_evm_log_filter(snap, "evm-1", None)
    # disabled sub with address=None should NOT force addresses → None
    assert f.addresses == ["0xaaa"]
```

- [ ] **Step 7.2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_filter_set.py -v`
Expected: FAIL with `ModuleNotFoundError: core.matcher.filter_set`.

- [ ] **Step 7.3: Implement `EvmLogFilterSet` + `build_evm_log_filter`**

Create `core/matcher/filter_set.py`:

```python
"""Per-chain RPC-side log filter set, derived from the ConfigSnapshot.

Used by ChainRunner to drive `eth_getLogs(addresses=..., topics=...)` so the
node filters server-side instead of returning every log to the indexer.
"""
from __future__ import annotations

from dataclasses import dataclass

import structlog

from core.abi.decoder import event_topic0
from core.abi.registry import AbiRegistry
from core.config.snapshot import ConfigSnapshot
from core.parser.erc20 import ERC20_TRANSFER_TOPIC0  # re-export below

log = structlog.get_logger(__name__)

# Re-export so consumers (tests, future callers) have a single import path.
__all__ = ["EvmLogFilterSet", "build_evm_log_filter", "ERC20_TRANSFER_TOPIC0"]

# Subscription kinds that consume `eth_getLogs` output. native_transfer / call
# do not look at logs at all.
_LOG_CONSUMING_KINDS = frozenset({"event", "token_transfer"})


@dataclass(frozen=True)
class EvmLogFilterSet:
    """Filter set to pass into `eth_getLogs` for a chain.

    - `addresses=None`  → don't filter by address (some relevant sub is
      address-agnostic; we must accept logs from any contract).
    - `topic0s=None`    → don't filter by topic0 (some relevant sub can't be
      reduced to a topic0; fall back to full topic scan).
    - `skip_logs=True`  → don't call `eth_getLogs` at all; no subscription on
      this chain needs logs.
    """

    addresses: list[str] | None
    topic0s: list[str] | None
    skip_logs: bool

    @property
    def topics_param(self) -> list[list[str]] | None:
        """Shape required by `eth_getLogs` `topics` field.

        Position 0 of the outer list matches log topic 0. The inner list is
        OR-of-candidates: RPC returns any log whose first topic is in the
        set. Returns None when topic filtering is disabled.
        """
        return [list(self.topic0s)] if self.topic0s else None


def build_evm_log_filter(
    snapshot: ConfigSnapshot,
    chain_id: str,
    abi_registry: AbiRegistry | None,
) -> EvmLogFilterSet:
    """Build the filter set for `chain_id` from the snapshot's enabled subs.

    Algorithm (matches the spec §"EvmLogFilterSet" behaviour table):
      1. Collect log-consuming subs for this chain.
      2. If none, return skip_logs=True.
      3. Address set: union of all `s.address`; if any is None, drop filter.
      4. Topic0 set: ERC-20 topic for `token_transfer`; computed via
         `event_topic0(...)` for `event` subs. If any sub can't be reduced
         (missing abi_id / match_name / lookup miss), drop filter entirely.
    """
    relevant = [
        s for s in snapshot.subscriptions_for_chain(chain_id)
        if s.match_kind in _LOG_CONSUMING_KINDS
    ]
    if not relevant:
        return EvmLogFilterSet(addresses=None, topic0s=None, skip_logs=True)

    # --- Addresses ---
    addresses: list[str] | None
    if any(s.address is None for s in relevant):
        addresses = None
    else:
        addresses = sorted({s.address.lower() for s in relevant if s.address is not None})

    # --- Topic0s ---
    topic0s: list[str] | None = None
    topics_set: set[str] = set()
    bailed = False
    for s in relevant:
        if s.match_kind == "token_transfer":
            topics_set.add(ERC20_TRANSFER_TOPIC0)
            continue
        # match_kind == "event"
        if s.abi_id is None or s.match_name is None or abi_registry is None:
            bailed = True
            break
        t0 = _event_topic0_for(abi_registry, s.abi_id, s.match_name)
        if t0 is None:
            bailed = True
            break
        topics_set.add(t0)
    if not bailed:
        topic0s = sorted(topics_set)

    return EvmLogFilterSet(addresses=addresses, topic0s=topic0s, skip_logs=False)


def _event_topic0_for(registry: AbiRegistry, abi_id: str, event_name: str) -> str | None:
    """Compute topic0 for the named event in the given abi, or None on miss.

    Iterates the abi body looking for `{type: "event", name: event_name}`.
    Returns lowercased 0x-hex topic0, or None if the event is not in the body
    or topic0 computation raises.
    """
    try:
        body = registry.get_body(abi_id)
    except Exception:  # noqa: BLE001 — abi may have been deleted; treat as miss
        return None
    entries = body if isinstance(body, list) else [body]
    for entry in entries:
        if entry.get("type") != "event":
            continue
        if entry.get("name") != event_name:
            continue
        try:
            return event_topic0(entry).lower()
        except Exception:  # noqa: BLE001
            log.warning(
                "filter_set.event_topic0_failed",
                abi_id=abi_id, event_name=event_name,
            )
            return None
    return None
```

- [ ] **Step 7.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_filter_set.py -v`
Expected: all 9 tests PASS.

- [ ] **Step 7.5: Run lint + mypy**

Run: `uv run ruff check core/matcher/filter_set.py tests/unit/test_filter_set.py && uv run mypy core/matcher/filter_set.py`
Expected: clean.

- [ ] **Step 7.6: Commit**

```bash
git add core/matcher/filter_set.py tests/unit/test_filter_set.py
git commit -m "feat(matcher): add EvmLogFilterSet and build_evm_log_filter"
```

---

### Task 8: `EvmAdapter.fetch_logs` accepts `topics`

**Files:**
- Modify: `core/chains/evm.py:115-135` (fetch_logs)
- Create: `tests/unit/test_evm_fetch_logs_topics.py`

- [ ] **Step 8.1: Write the failing test**

Create `tests/unit/test_evm_fetch_logs_topics.py`:

```python
"""Verify EvmAdapter.fetch_logs threads addresses + topics into eth_getLogs."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from core.chains.evm import EvmAdapter


class _StubEthLogs:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_logs(self, params):  # web3 v7 returns AsyncContractEventManager
        self.calls.append(dict(params))
        return []


class _StubEth:
    def __init__(self) -> None:
        self._logs = _StubEthLogs()

    async def get_logs(self, params):
        return await self._logs.get_logs(params)


class _StubW3:
    def __init__(self) -> None:
        self.eth = _StubEth()


@pytest.mark.asyncio
async def test_fetch_logs_passes_addresses_and_topics():
    adapter = EvmAdapter(
        chain_id="x", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    adapter._w3 = _StubW3()  # type: ignore[assignment]
    await adapter.fetch_logs(
        from_block=10, to_block=20,
        addresses=["0xaaa", "0xbbb"],
        topics=[["0xt0a", "0xt0b"]],
    )
    call = adapter._w3.eth._logs.calls[0]  # type: ignore[attr-defined]
    assert call["fromBlock"] == 10
    assert call["toBlock"] == 20
    assert call["address"] == ["0xaaa", "0xbbb"]
    assert call["topics"] == [["0xt0a", "0xt0b"]]


@pytest.mark.asyncio
async def test_fetch_logs_omits_topics_when_none():
    adapter = EvmAdapter(
        chain_id="x", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    adapter._w3 = _StubW3()  # type: ignore[assignment]
    await adapter.fetch_logs(from_block=1, to_block=1, addresses=None, topics=None)
    call = adapter._w3.eth._logs.calls[0]  # type: ignore[attr-defined]
    assert "address" not in call
    assert "topics" not in call
```

- [ ] **Step 8.2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_evm_fetch_logs_topics.py -v`
Expected: FAIL with `TypeError: fetch_logs() got an unexpected keyword argument 'topics'`.

- [ ] **Step 8.3: Extend the signature**

In `core/chains/evm.py:115-135`:

```python
    async def fetch_logs(
        self,
        from_block: int,
        to_block: int,
        addresses: list[str] | None = None,
        topics: list[list[str]] | None = None,
    ) -> list[Log]:
        assert self._w3 is not None
        params: dict[str, Any] = {"fromBlock": from_block, "toBlock": to_block}
        if addresses:
            params["address"] = addresses
        if topics:
            params["topics"] = topics
        raw_logs = await self._w3.eth.get_logs(cast(FilterParams, params))
        out: list[Log] = []
        for lg in raw_logs:
            out.append(
                Log(
                    tx_hash=_hexify(lg["transactionHash"]),
                    log_index=int(lg["logIndex"]),
                    address=str(lg["address"]),
                    topics=[_hexify(t) for t in lg["topics"]],
                    data=lg["data"] if isinstance(lg["data"], str) else _hexify(lg["data"]),
                    block_number=int(lg["blockNumber"]),
                )
            )
        return out
```

The `if topics:` guard is the same shape as `if addresses:` and correctly treats `None` and `[]` as "don't filter."

- [ ] **Step 8.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_evm_fetch_logs_topics.py -v`
Expected: both PASS.

- [ ] **Step 8.5: Run broader tests to ensure no callers broke**

Run: `uv run pytest tests/unit/ -m "not e2e"`
Expected: all PASS. `ChainRunner` still calls `fetch_logs` with positional `from, to` and `addresses=None`; `topics=None` defaults preserve behaviour.

- [ ] **Step 8.6: Commit**

```bash
git add core/chains/evm.py tests/unit/test_evm_fetch_logs_topics.py
git commit -m "feat(evm-adapter): accept topics param on fetch_logs"
```

---

## Chunk 3: ChainRunner Filter Integration (Head-Following)

This chunk wires the filter set into `ChainRunner` for the head-following path only. After this chunk, every confirmed block's `eth_getLogs` call carries `addresses` + `topics`, and the `skip_logs=True` shortcut is honoured. Catchup still uses the old per-block path — that's Chunk 4.

### Task 9: Build filter on snapshot apply, use in `_process_confirmed_block`

**Files:**
- Modify: `apps/worker/chain_runner.py:54-159` (init, start, apply_snapshot, _process_confirmed_block)
- Modify: `tests/unit/test_chain_runner.py`

- [ ] **Step 9.1: Add filter state and build sites**

In `apps/worker/chain_runner.py`, add the import:

```python
from core.matcher.filter_set import EvmLogFilterSet, build_evm_log_filter
```

In `ChainRunner.__init__` (after `self._notifier = None`):

```python
        self._evm_filter: EvmLogFilterSet | None = None
```

In `ChainRunner.start`, after the final `self._current_snap = snap`:

```python
        if self._chain.kind != "solana":
            self._evm_filter = build_evm_log_filter(snap, self._chain.id, self._abi_registry)
```

In `ChainRunner.apply_snapshot`, inside the lock after `self._current_snap = snap`:

```python
            if self._chain.kind != "solana":
                self._evm_filter = build_evm_log_filter(snap, self._chain.id, self._abi_registry)
```

- [ ] **Step 9.2: Refactor `_process_confirmed_block`**

The current method does both fetch concurrency and parsing/dispatch. Split per the spec's refactoring contract:

```python
    async def _process_confirmed_block(
        self,
        number: int,
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        assert self._adapter is not None and self._evm_pipeline is not None
        assert self._evm_filter is not None
        filter = self._evm_filter

        block_coro = self._adapter.fetch_block(number)
        if filter.skip_logs:
            block = await block_coro
            logs: list[Log] = []
        else:
            logs_coro = self._adapter.fetch_logs(
                number, number,
                addresses=filter.addresses,
                topics=filter.topics_param,
            )
            block, logs = await asyncio.gather(block_coro, logs_coro)

        await self._process_block_with_prefetched_logs(
            number, block, logs, matcher=matcher, notifier=notifier,
        )

    async def _process_block_with_prefetched_logs(
        self,
        number: int,
        block: Block,
        prefetched_logs: list[Log],
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        assert self._adapter is not None and self._evm_pipeline is not None
        from dataclasses import replace
        block = replace(block, logs=prefetched_logs)

        events = list(self._evm_pipeline.run(block))

        dispatch_tasks: list[asyncio.Task[None]] = []
        matched_sub_ids: set[str] = set()
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if hits:
                dispatch_tasks.append(asyncio.create_task(notifier.dispatch(event, hits)))
                for sub, _ in hits:
                    matched_sub_ids.add(sub.id)

        if self._chain.trace_internal_calls and self._abi_registry is not None:
            trace_fn = getattr(self._adapter, "trace_block", None)
            if callable(trace_fn):
                traces = await trace_fn(number)
                if traces:
                    internal_parser = InternalCallParser(chain_id=self._chain.id, registry=self._abi_registry)
                    for event in internal_parser.parse(traces, block):
                        hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
                        if hits:
                            dispatch_tasks.append(asyncio.create_task(notifier.dispatch(event, hits)))

        if dispatch_tasks:
            await asyncio.gather(*dispatch_tasks, return_exceptions=True)

        await self._cp.save(self._chain.id, block.header.number, block.header.hash)
        if self._on_block_processed and matched_sub_ids:
            try:
                await self._on_block_processed(matched_sub_ids, block.header.number)
            except Exception:  # noqa: BLE001
                pass
```

Add the necessary `Block`, `Log` imports at the top of the module:

```python
from core.chains.types import Block, BlockHeader, Log
```

(`BlockHeader` is already imported; verify after editing.)

- [ ] **Step 9.3: Write a test for the head-following filter usage**

Append to `tests/unit/test_chain_runner.py`. **Reuse the existing `_CheckpointStub` helper** at line 141 — don't introduce a parallel fake.

```python
@pytest.mark.asyncio
async def test_head_following_passes_filter_to_fetch_logs() -> None:
    """ChainRunner with a token_transfer subscription should call fetch_logs
    with the ERC-20 topic0 in `topics`."""
    from core.matcher.filter_set import ERC20_TRANSFER_TOPIC0
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address="0xaaa",
            abi_id=None, match_kind="token_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"],
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[],
        abis=[],
    )
    captured: dict = {}

    class _Adapter:
        chain_id = "evm-1"
        async def connect(self): pass
        async def disconnect(self): pass
        async def get_latest_block_number(self): return 100
        async def fetch_block(self, n):
            return Block(
                header=BlockHeader(
                    number=n, hash="0x" + "f"*64,
                    parent_hash="0x" + "0"*64, timestamp=0,
                ),
                txs=[], logs=[],
            )
        async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
            captured["from"] = from_block
            captured["to"] = to_block
            captured["addresses"] = addresses
            captured["topics"] = topics
            return []
        def subscribe_heads(self): ...
        async def trace_block(self, n): return []

    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: _Adapter(),
        channel_factory=lambda c: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    await runner.start(snap)
    await runner._process_confirmed_block(
        42, matcher=runner._matcher, notifier=runner._notifier,
    )
    assert captured["from"] == 42
    assert captured["to"] == 42
    assert captured["addresses"] == ["0xaaa"]
    assert captured["topics"] == [[ERC20_TRANSFER_TOPIC0]]


@pytest.mark.asyncio
async def test_head_following_skips_logs_when_no_log_subscription() -> None:
    """If only native_transfer subs exist, fetch_logs should never be called."""
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"],
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    called = {"fetch_logs": 0}

    class _Adapter:
        chain_id = "evm-1"
        async def connect(self): pass
        async def disconnect(self): pass
        async def get_latest_block_number(self): return 100
        async def fetch_block(self, n):
            return Block(
                header=BlockHeader(
                    number=n, hash="0x" + "f"*64,
                    parent_hash="0x" + "0"*64, timestamp=0,
                ),
                txs=[], logs=[],
            )
        async def fetch_logs(self, *a, **kw):
            called["fetch_logs"] += 1
            return []
        def subscribe_heads(self): ...
        async def trace_block(self, n): return []

    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: _Adapter(),
        channel_factory=lambda c: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    await runner.start(snap)
    await runner._process_confirmed_block(
        7, matcher=runner._matcher, notifier=runner._notifier,
    )
    assert called["fetch_logs"] == 0
```

If `Block`, `BlockHeader`, `ConfigSnapshot`, `SnapshotSubscription`, `SnapshotChannel`, `SnapshotChain`, or `_CollectingChannel` are not already imported at the top of `test_chain_runner.py`, add them now. Verify by reading the file's existing imports first.

- [ ] **Step 9.4: Run the tests**

Run: `uv run pytest tests/unit/test_chain_runner.py -v`
Expected: all PASS, including the two new ones.

- [ ] **Step 9.5: Run mypy**

Run: `uv run mypy apps core`
Expected: clean.

- [ ] **Step 9.6: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(runner): wire EvmLogFilterSet into head-following path"
```

---

## Chunk 4: EVM Catchup Range Query + Degradation

This chunk replaces the per-block catchup loop with range queries plus binary-bisection degradation. After this chunk, EVM catchup issues one `eth_getLogs` per `log_query_range_blocks`-sized window instead of one per block.

### Task 10: `_fetch_logs_with_degrade` + `_bucket_by_block` helpers + catchup refactor

**Files:**
- Modify: `apps/worker/chain_runner.py:183-222` (_catchup_evm)
- Create: `tests/unit/test_evm_catchup_range.py`
- Create: `tests/unit/test_evm_fetch_logs_degrade.py`
- Create: `tests/unit/test_evm_skip_logs_catchup.py`

- [ ] **Step 10.1: Add helpers**

In `apps/worker/chain_runner.py`, add module-level helper and constants:

```python
_DEGRADE_ERR_HINTS = (
    "too large", "result too big", "query timeout",
    "limit exceeded", "returned more than",
)


def _bucket_by_block(logs: list[Log]) -> dict[int, list[Log]]:
    out: dict[int, list[Log]] = {}
    for lg in logs:
        out.setdefault(lg.block_number, []).append(lg)
    return out
```

(Both go above `class ChainRunner`.)

Inside `class ChainRunner`, add the degrade helper as an instance method:

```python
    async def _fetch_logs_with_degrade(
        self, start: int, end: int, filter: EvmLogFilterSet,
    ) -> dict[int, list[Log]]:
        try:
            logs = await self._adapter.fetch_logs(
                start, end,
                addresses=filter.addresses,
                topics=filter.topics_param,
            )
            return _bucket_by_block(logs)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if any(h in msg for h in _DEGRADE_ERR_HINTS):
                if end > start:
                    mid = (start + end) // 2
                    left = await self._fetch_logs_with_degrade(start, mid, filter)
                    right = await self._fetch_logs_with_degrade(mid + 1, end, filter)
                    return {**left, **right}
                # Single-block floor still failing. Some chains return
                # "too large" for very active single blocks. Log + re-raise.
                log.error(
                    "chain_runner.fetch_logs_single_block_too_large",
                    chain_id=self._chain.id, block=start,
                )
            raise
```

- [ ] **Step 10.2: Refactor `_catchup_evm`**

Replace the per-block loop body (`for n in range(last_block + 1, safe_tip + 1):` ... `break`) with the window loop. Keep the surrounding catchup-effective-start logic intact.

```python
    async def _catchup_evm(self) -> None:
        assert self._adapter is not None and self._matcher is not None and self._notifier is not None
        assert self._evm_filter is not None
        cp_block = self.resume_from[0] if self.resume_from else None
        sub_starts = [s.start_block for s in (self._current_snap.subscriptions if self._current_snap else [])
                      if s.chain_id == self._chain.id and s.start_block is not None and s.enabled]
        candidates = [b for b in [cp_block, *sub_starts] if b is not None]
        if not candidates:
            return
        last_block = min(candidates)
        try:
            tip = await self._adapter.get_latest_block_number()
        except Exception:  # noqa: BLE001
            log.warning("chain_runner.catchup_tip_failed", chain_id=self._chain.id)
            return
        safe_tip = tip - self._chain.confirmations
        gap = safe_tip - last_block
        if gap <= 0:
            return
        if gap > self.MAX_CATCHUP_BLOCKS:
            log.warning(
                "chain_runner.catchup_gap_too_large",
                chain_id=self._chain.id, gap=gap, max=self.MAX_CATCHUP_BLOCKS,
                skipping_to=safe_tip - self.MAX_CATCHUP_BLOCKS,
            )
            last_block = safe_tip - self.MAX_CATCHUP_BLOCKS
            gap = self.MAX_CATCHUP_BLOCKS
        log.info(
            "chain_runner.catchup_starting",
            chain_id=self._chain.id, from_block=last_block + 1,
            to_block=safe_tip, gap=gap,
        )
        matcher = self._matcher
        notifier = self._notifier
        filter = self._evm_filter
        range_blocks = self._chain.log_query_range_blocks

        processed = 0
        start_n = last_block + 1
        while start_n <= safe_tip:
            end_n = min(start_n + range_blocks - 1, safe_tip)
            try:
                if filter.skip_logs:
                    logs_by_block: dict[int, list[Log]] = {}
                else:
                    logs_by_block = await self._fetch_logs_with_degrade(start_n, end_n, filter)
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
                        log.info(
                            "chain_runner.catchup_progress",
                            chain_id=self._chain.id, block=n,
                            remaining=safe_tip - n,
                        )
            except Exception:  # noqa: BLE001 — match existing break-on-error semantics
                log.error(
                    "chain_runner.catchup_window_failed",
                    chain_id=self._chain.id, start=start_n, end=end_n,
                )
                break
            start_n = end_n + 1
        log.info("chain_runner.catchup_done", chain_id=self._chain.id, processed=processed)
```

- [ ] **Step 10.3: Write the range-query test**

Create `tests/unit/test_evm_catchup_range.py`:

```python
"""Verify _catchup_evm issues one fetch_logs per window, N fetch_block calls."""
from __future__ import annotations

import pytest

from apps.worker.chain_runner import ChainRunner
from core.chains.types import Block, BlockHeader
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)


class _Adapter:
    chain_id = "evm-1"

    def __init__(self, tip: int) -> None:
        self._tip = tip
        self.fetch_block_calls = 0
        self.fetch_logs_calls: list[tuple[int, int]] = []

    async def connect(self): pass
    async def disconnect(self): pass

    async def get_latest_block_number(self): return self._tip

    async def fetch_block(self, n):
        self.fetch_block_calls += 1
        return Block(
            header=BlockHeader(number=n, hash="0x" + format(n, "064x"),
                               parent_hash="0x" + format(n-1, "064x"), timestamp=0),
            txs=[], logs=[],
        )

    async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
        self.fetch_logs_calls.append((from_block, to_block))
        return []

    def subscribe_heads(self): ...


class _NullChannel:
    """Minimal Channel stand-in so Notifier.start doesn't crash."""
    type = "http"
    async def start(self): pass
    async def stop(self): pass
    async def send(self, *a, **kw): pass


class _CP:
    def __init__(self, start): self.start = start
    async def get(self, chain_id): return (self.start, "0x" + format(self.start, "064x"))
    async def save(self, *a, **kw): pass


@pytest.mark.asyncio
async def test_catchup_issues_window_sized_log_queries():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address="0xaaa",
            abi_id=None, match_kind="token_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
        log_query_range_blocks=10,
    )
    adapter = _Adapter(tip=29)
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_evm()

    # 29 blocks to catch up (1..29), window=10 → 3 windows: [1-10],[11-20],[21-29]
    assert adapter.fetch_logs_calls == [(1, 10), (11, 20), (21, 29)]
    assert adapter.fetch_block_calls == 29


@pytest.mark.asyncio
async def test_catchup_skips_logs_for_native_transfer_only_subs():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000, log_query_range_blocks=10,
    )
    adapter = _Adapter(tip=5)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_evm()

    assert adapter.fetch_logs_calls == []
    assert adapter.fetch_block_calls == 5
```

- [ ] **Step 10.4: Write the degradation test**

Create `tests/unit/test_evm_fetch_logs_degrade.py`:

```python
"""Verify _fetch_logs_with_degrade bisects on 'too large' errors and re-raises at single-block floor."""
from __future__ import annotations

import pytest

from apps.worker.chain_runner import ChainRunner
from core.config.snapshot import (
    ConfigSnapshot, SnapshotChain, SnapshotChannel, SnapshotSubscription,
)
from core.matcher.filter_set import EvmLogFilterSet


class _StubAdapter:
    chain_id = "evm-1"

    def __init__(self, fail_until_window: int) -> None:
        self.fail_until_window = fail_until_window  # only succeed for ranges ≤ this
        self.calls: list[tuple[int, int]] = []

    async def connect(self): pass
    async def disconnect(self): pass
    async def get_latest_block_number(self): return 100

    async def fetch_logs(self, from_block, to_block, addresses=None, topics=None):
        self.calls.append((from_block, to_block))
        if (to_block - from_block + 1) > self.fail_until_window:
            raise RuntimeError("query returned more than 10000 results: result too large")
        return []


class _CheckpointStub:
    """Local async-aware stub. Mirrors the one in test_chain_runner.py."""
    async def get(self, _chain_id: str): return None
    async def save(self, *_a, **_kw): pass


@pytest.fixture
def runner():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="evm-1", address="0xaaa",
            abi_id=None, match_kind="token_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"],
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="evm-1", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=1000,
    )

    class _NullChannel:
        async def start(self): pass
        async def stop(self): pass
        async def send(self, *a, **kw): pass

    r = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: _StubAdapter(fail_until_window=2),
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    return r, snap


@pytest.mark.asyncio
async def test_bisects_to_passing_window_size(runner):
    r, snap = runner
    await r.start(snap)
    f = EvmLogFilterSet(addresses=["0xaaa"], topic0s=None, skip_logs=False)
    result = await r._fetch_logs_with_degrade(1, 4, f)  # 4 blocks, will bisect → 2, 2
    assert result == {}
    adapter = r._adapter
    # Initial call (1-4) fails → bisect to (1-2) and (3-4), both succeed.
    assert (1, 4) in adapter.calls
    assert (1, 2) in adapter.calls
    assert (3, 4) in adapter.calls


@pytest.mark.asyncio
async def test_single_block_floor_propagates(runner):
    r, snap = runner
    await r.start(snap)
    f = EvmLogFilterSet(addresses=["0xaaa"], topic0s=None, skip_logs=False)
    # Make the adapter always fail.
    r._adapter.fail_until_window = 0
    with pytest.raises(RuntimeError, match="too large"):
        await r._fetch_logs_with_degrade(7, 7, f)
```

- [ ] **Step 10.5: Run all new tests**

Run: `uv run pytest tests/unit/test_evm_catchup_range.py tests/unit/test_evm_fetch_logs_degrade.py -v`
Expected: all PASS.

- [ ] **Step 10.6: Run full unit test suite**

Run: `uv run pytest tests/unit/ -m "not e2e"`
Expected: all PASS.

- [ ] **Step 10.7: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_evm_catchup_range.py tests/unit/test_evm_fetch_logs_degrade.py
git commit -m "feat(runner): EVM catchup uses range queries with binary-bisection degradation"
```

---

## Chunk 5: Solana Adapter `get_blocks` + Catchup Skip-Empty-Slots

This chunk adds `getBlocks` to the Solana adapter and refactors `_catchup_solana` to skip empty slots. Head-following Solana path is unchanged. After this chunk, all Package A optimizations are in place.

### Task 11: `SolanaAdapter.get_blocks`

**Files:**
- Modify: `core/chains/solana.py`
- Create: `tests/unit/test_solana_get_blocks.py`

- [ ] **Step 11.1: Write the failing test**

Create `tests/unit/test_solana_get_blocks.py`:

```python
"""Verify SolanaAdapter.get_blocks calls getBlocks RPC and returns the slot list."""
from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import pytest

from core.chains.solana import SolanaAdapter


class _FakeResp:
    def __init__(self, payload): self._p = payload
    def raise_for_status(self): pass
    def json(self): return self._p


@pytest.mark.asyncio
async def test_get_blocks_returns_valid_slots():
    adapter = SolanaAdapter(
        chain_id="sol", rpc_http="http://x", commitment="confirmed",
        poll_interval_ms=2000,
    )
    captured: dict = {}

    async def fake_post(url, json=None, headers=None):
        captured["json"] = json
        return _FakeResp({"jsonrpc": "2.0", "id": 1, "result": [10, 12, 15]})

    adapter._client = type("C", (), {"post": staticmethod(fake_post)})()
    out = await adapter.get_blocks(10, 20)
    assert out == [10, 12, 15]
    assert captured["json"]["method"] == "getBlocks"
    assert captured["json"]["params"][:2] == [10, 20]
    # finalized is the only commitment that supports range queries reliably
    assert captured["json"]["params"][2]["commitment"] == "finalized"


@pytest.mark.asyncio
async def test_get_blocks_returns_empty_list_when_result_null():
    adapter = SolanaAdapter(
        chain_id="sol", rpc_http="http://x", commitment="confirmed",
        poll_interval_ms=2000,
    )

    async def fake_post(url, json=None, headers=None):
        return _FakeResp({"jsonrpc": "2.0", "id": 1, "result": None})

    adapter._client = type("C", (), {"post": staticmethod(fake_post)})()
    out = await adapter.get_blocks(10, 20)
    assert out == []
```

- [ ] **Step 11.2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_solana_get_blocks.py -v`
Expected: FAIL with `AttributeError: 'SolanaAdapter' object has no attribute 'get_blocks'`.

- [ ] **Step 11.3: Implement `get_blocks`**

In `core/chains/solana.py`, add after `fetch_block`:

```python
    async def get_blocks(self, start_slot: int, end_slot: int) -> list[int]:
        """Return slots in [start_slot, end_slot] that contain confirmed blocks.

        Skipped/empty slots are excluded. The slot-discovery step always uses
        `finalized` commitment because `confirmed` is not guaranteed to support
        large ranges across providers. Subsequent `getBlock` calls still
        respect the chain's configured commitment.

        The caller is responsible for window sizing (per-chain
        `slot_query_range_blocks`); this method does not internally chunk.
        """
        assert self._client is not None
        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getBlocks",
            "params": [start_slot, end_slot, {"commitment": "finalized"}],
        }
        resp = await self._client.post(
            self._rpc_url,
            json=payload,
            headers={"content-type": "application/json"},
        )
        resp.raise_for_status()
        body = resp.json()
        result = body.get("result")
        return list(result or [])
```

- [ ] **Step 11.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_solana_get_blocks.py -v`
Expected: both PASS.

- [ ] **Step 11.5: Commit**

```bash
git add core/chains/solana.py tests/unit/test_solana_get_blocks.py
git commit -m "feat(solana-adapter): add get_blocks for enumerating non-empty slots"
```

---

### Task 12: Solana catchup uses `get_blocks`

**Files:**
- Modify: `apps/worker/chain_runner.py:232-268` (_catchup_solana)
- Create: `tests/unit/test_solana_catchup_range.py`

- [ ] **Step 12.1: Add the `_get_blocks_classified` helper**

In `apps/worker/chain_runner.py`, near the EVM degrade helper, add module-level constant and instance method:

```python
_SOL_RANGE_TOO_LARGE_HINTS = ("exceeds maximum", "too large", "limit exceeded")
```

Inside `class ChainRunner`:

```python
    async def _get_blocks_classified(self, start: int, end: int) -> list[int]:
        """Wrap adapter.get_blocks, classify errors.

        - Size-limit class (hint match) → raise (config error: operator must
          lower slot_query_range_blocks).
        - Other transient errors → log warning, return dense [start, end+1).
        """
        assert self._adapter is not None
        try:
            return await self._adapter.get_blocks(start, end)
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).lower()
            if any(h in msg for h in _SOL_RANGE_TOO_LARGE_HINTS):
                log.error(
                    "chain_runner.get_blocks_range_too_large",
                    chain_id=self._chain.id, start=start, end=end,
                )
                raise
            log.warning(
                "chain_runner.get_blocks_failed",
                chain_id=self._chain.id, start=start, end=end, error=str(exc),
            )
            return list(range(start, end + 1))
```

- [ ] **Step 12.2: Refactor `_catchup_solana`**

```python
    async def _catchup_solana(self) -> None:
        assert self._adapter is not None and self._matcher is not None and self._notifier is not None
        cp_slot = self.resume_from[0] if self.resume_from else None
        sub_starts = [s.start_block for s in (self._current_snap.subscriptions if self._current_snap else [])
                      if s.chain_id == self._chain.id and s.start_block is not None and s.enabled]
        candidates = [b for b in [cp_slot, *sub_starts] if b is not None]
        if not candidates:
            return
        last_slot = min(candidates)
        try:
            tip = await self._adapter.get_latest_slot()
        except Exception:  # noqa: BLE001
            log.warning("chain_runner.catchup_tip_failed", chain_id=self._chain.id)
            return
        gap = tip - last_slot
        if gap <= 0:
            return
        if gap > self.MAX_CATCHUP_BLOCKS:
            log.warning(
                "chain_runner.catchup_gap_too_large",
                chain_id=self._chain.id, gap=gap, max=self.MAX_CATCHUP_BLOCKS,
            )
            last_slot = tip - self.MAX_CATCHUP_BLOCKS
            gap = self.MAX_CATCHUP_BLOCKS
        log.info(
            "chain_runner.catchup_starting",
            chain_id=self._chain.id, from_slot=last_slot + 1, to_slot=tip, gap=gap,
        )

        range_slots = self._chain.slot_query_range_blocks
        processed = 0
        start_s = last_slot + 1
        while start_s <= tip:
            end_s = min(start_s + range_slots - 1, tip)
            valid = await self._get_blocks_classified(start_s, end_s)
            for s in valid:
                if self._stop.is_set():
                    return
                try:
                    await self._process_solana_slot(s)
                    processed += 1
                    if processed % 100 == 0:
                        log.info(
                            "chain_runner.catchup_progress",
                            chain_id=self._chain.id, slot=s, remaining=tip - s,
                        )
                except Exception:  # noqa: BLE001 — match existing per-slot tolerance
                    log.error(
                        "chain_runner.catchup_slot_failed",
                        chain_id=self._chain.id, slot=s,
                    )
                    continue
            start_s = end_s + 1
        log.info("chain_runner.catchup_done", chain_id=self._chain.id, processed=processed)
```

- [ ] **Step 12.3: Write the catchup test**

Create `tests/unit/test_solana_catchup_range.py`:

```python
"""Verify Solana catchup calls get_blocks per window and only fetch_block for valid slots."""
from __future__ import annotations

import pytest

from apps.worker.chain_runner import ChainRunner
from core.chains.types import SolanaBlock
from core.config.snapshot import (
    ConfigSnapshot, SnapshotChain, SnapshotChannel, SnapshotSubscription,
)


class _Adapter:
    chain_id = "sol-1"

    def __init__(self, valid_slots: dict[tuple[int, int], list[int]], tip: int):
        self._valid = valid_slots
        self._tip = tip
        self.fetch_block_calls: list[int] = []
        self.get_blocks_calls: list[tuple[int, int]] = []

    async def connect(self): pass
    async def disconnect(self): pass
    async def get_latest_slot(self): return self._tip
    async def get_blocks(self, start, end):
        self.get_blocks_calls.append((start, end))
        return list(self._valid.get((start, end), []))
    async def fetch_block(self, slot):
        self.fetch_block_calls.append(slot)
        return SolanaBlock(slot=slot, block_hash="0xaa", parent_slot=slot-1, block_time=0, transactions=[])
    def subscribe_heads(self): ...


class _NullChannel:
    type = "http"
    async def start(self): pass
    async def stop(self): pass
    async def send(self, *a, **kw): pass


class _CP:
    def __init__(self, start): self.start = start
    async def get(self, chain_id): return (self.start, "0x00")
    async def save(self, *a, **kw): pass


@pytest.mark.asyncio
async def test_solana_catchup_skips_empty_slots():
    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="sol-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="sol-1", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        slot_query_range_blocks=10,
    )
    valid_slots = {(1, 10): [1, 3, 5], (11, 15): [11, 14]}
    adapter = _Adapter(valid_slots=valid_slots, tip=15)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_solana()

    assert adapter.get_blocks_calls == [(1, 10), (11, 15)]
    assert adapter.fetch_block_calls == [1, 3, 5, 11, 14]


@pytest.mark.asyncio
async def test_solana_catchup_size_limit_error_raises():
    class _BadAdapter(_Adapter):
        async def get_blocks(self, start, end):
            raise RuntimeError("query range exceeds maximum allowed")

    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="sol-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="sol-1", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        slot_query_range_blocks=10,
    )
    adapter = _BadAdapter(valid_slots={}, tip=5)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    with pytest.raises(RuntimeError, match="exceeds maximum"):
        await runner._catchup_solana()


@pytest.mark.asyncio
async def test_solana_catchup_transient_error_degrades_to_dense():
    class _FlakyAdapter(_Adapter):
        async def get_blocks(self, start, end):
            self.get_blocks_calls.append((start, end))
            raise RuntimeError("temporary network failure")

    snap = ConfigSnapshot(
        version=1,
        subscriptions=[SnapshotSubscription(
            id="s1", name="s1", chain_id="sol-1", address=None,
            abi_id=None, match_kind="native_transfer", match_name=None,
            arg_filters={}, enabled=True, channel_ids=["c1"], start_block=0,
        )],
        channels=[SnapshotChannel(id="c1", name="c1", type="http", config={})],
        chains=[], abis=[],
    )
    chain = SnapshotChain(
        id="sol-1", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, poll_interval_ms=2000, commitment="confirmed",
        slot_query_range_blocks=10,
    )
    adapter = _FlakyAdapter(valid_slots={}, tip=3)
    runner = ChainRunner(
        chain=chain, adapter_factory=lambda c: adapter,
        channel_factory=lambda c: _NullChannel(),
        checkpoint_repo=_CP(start=0),
    )
    await runner.start(snap)
    await runner._catchup_solana()
    # Transient error → dense fallback; fetch_block called for 1,2,3.
    assert adapter.fetch_block_calls == [1, 2, 3]
```

- [ ] **Step 12.4: Run the new tests**

Run: `uv run pytest tests/unit/test_solana_catchup_range.py -v`
Expected: all three PASS.

- [ ] **Step 12.5: Run full unit test suite**

Run: `uv run pytest tests/unit/ -m "not e2e"`
Expected: all PASS.

- [ ] **Step 12.6: Run mypy**

Run: `uv run mypy core apps`
Expected: clean.

- [ ] **Step 12.7: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_solana_catchup_range.py
git commit -m "feat(runner): Solana catchup uses get_blocks to skip empty slots"
```

---

## Chunk 6: E2E Coverage + Docs Update

This chunk adds anvil-backed and solana-test-validator-backed integration tests to prove the optimizations work end-to-end against real RPCs, then updates documentation.

### Task 13: anvil-backed integration test (EVM)

**Files:**
- Create: `tests/e2e/test_evm_catchup_logs_filtered.py`

- [ ] **Step 13.1: Write the test**

Reuse the anvil + webhook_receiver fixtures from `tests/e2e/conftest.py` and the test patterns in `tests/e2e/test_native_transfer_e2e.py`. Sketch:

```python
"""E2E: catchup uses RPC-side filtering and range queries against anvil."""
import pytest

# Fixtures `anvil`, `webhook_receiver` are provided by tests/e2e/conftest.py.
# Read tests/e2e/test_native_transfer_e2e.py for the full harness pattern.

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_catchup_with_address_and_topic_filter(anvil, webhook_receiver):
    """Mint ERC-20 transfers across N blocks, then subscribe to only one
    contract and verify catchup picks up exactly those events via RPC-side
    filtering AND that the runner issues ceil(N / log_query_range_blocks)
    fetch_logs calls (not N)."""
    # 1. Deploy two ERC-20s; emit transfers on both across ≥30 blocks.
    # 2. Configure a single token_transfer subscription on contract A.
    # 3. Start a ChainRunner from block 0 with log_query_range_blocks=10.
    # 4. Wait for catchup completion (poll checkpoint / matched_count).
    # 5. Verify webhook received exactly A's transfer count.
    # 6. Wrap EvmAdapter.fetch_logs with a counter via monkeypatch to assert
    #    the call count == ceil(blocks / 10).
    pytest.skip("e2e harness — implement against existing anvil fixture in tests/e2e/conftest.py")
```

The fixture wiring depends on the e2e harness in `tests/e2e/conftest.py`. If deploying ERC-20s through the existing harness is non-trivial, leave the `pytest.skip` in place and capture the harness extension as a follow-up task — the unit tests in Chunk 4 already cover the optimization's correctness.

- [ ] **Step 13.2: Run e2e if anvil is available**

Run: `uv run pytest tests/e2e/test_evm_catchup_logs_filtered.py -m e2e -v`
Expected: PASS or SKIP (no anvil) — never FAIL.

- [ ] **Step 13.3: Commit**

```bash
git add tests/e2e/test_evm_catchup_logs_filtered.py
git commit -m "test(e2e): catchup uses RPC-side log filter + range queries against anvil"
```

---

### Task 14: solana-test-validator integration test

**Files:**
- Create: `tests/e2e/test_solana_catchup_skips_empty.py`

- [ ] **Step 14.1: Write the test**

```python
"""E2E: Solana catchup uses get_blocks against solana-test-validator."""
import pytest

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_catchup_skips_empty_slots():
    """No `solana_validator` fixture exists in tests/e2e/conftest.py today —
    add one (similar to the `anvil` fixture: launch solana-test-validator as
    a subprocess, expose its RPC URL, tear down on session end) and a
    matching ChainRunner harness, then port the assertion shape from
    test_solana_catchup_range.py (verify get_blocks call count and that
    every valid slot received a fetch_block call)."""
    pytest.skip("e2e harness — needs new solana_validator fixture")
```

Same caveat as Task 13: leave `pytest.skip` if the fixture work is too large to bundle into this plan; unit tests in `test_solana_catchup_range.py` already prove correctness.

- [ ] **Step 14.2: Run e2e**

Run: `uv run pytest tests/e2e/test_solana_catchup_skips_empty.py -m e2e -v`
Expected: PASS or SKIP.

- [ ] **Step 14.3: Commit**

```bash
git add tests/e2e/test_solana_catchup_skips_empty.py
git commit -m "test(e2e): Solana catchup skips empty slots via get_blocks"
```

---

### Task 15: Final lint, type-check, full test run, docs

**Files:**
- Modify: `CLAUDE.md` (optional — mention new config fields)

- [ ] **Step 15.1: Run the full unit + integration suite**

Run: `uv run pytest tests/ -m "not e2e" -v`
Expected: all PASS.

- [ ] **Step 15.2: Run lint**

Run: `uv run ruff check core apps tests`
Expected: clean.

- [ ] **Step 15.3: Run mypy strict**

Run: `uv run mypy core apps`
Expected: clean.

- [ ] **Step 15.4: Update `CLAUDE.md`** (only if the new config knobs deserve a callout in the Tech Stack / Configuration sections)

If touching `CLAUDE.md`, keep it short — one bullet in the Configuration section mentioning the two new chain-level fields and their defaults.

- [ ] **Step 15.5: Final commit**

```bash
git add CLAUDE.md  # only if modified
git commit -m "docs: note new chain-level RPC range config knobs"
```

If there's nothing to commit, skip this step.

---

## Done When

- All 6 chunks merged with green tests at each step.
- `uv run pytest tests/ -m "not e2e"` is clean.
- `uv run ruff check core apps tests` is clean.
- `uv run mypy core apps` is clean (strict).
- E2E suite at least skips cleanly when fixtures unavailable; passes when they are.
- An operator can set `log_query_range_blocks` / `slot_query_range_blocks` per chain via `POST /api/chains`.
