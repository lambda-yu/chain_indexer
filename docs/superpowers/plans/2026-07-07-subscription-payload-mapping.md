# Subscription Payload Mapping Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let operators configure a per-subscription mapping (rename fields, nest targets, inject constants) that transforms the default payload into a downstream-specific shape before delivery, with a web UI that lets them fetch a source sample, edit the mapping, preview the result, and save.

**Architecture:** A new `core/notifier/payload_mapper.py` module owns the pure `apply_mapping` transform and a `build_source_payload` helper. `build_payload` is refactored to return a `(source, delivery_payload)` tuple; the notifier sends `delivery_payload` to channels and hands `source` to the on_success/on_failure callbacks so `delivery_records.event_payload` stores the un-mapped structure (needed for the "learn from real deliveries" sample endpoint and for downstream preview). A nullable JSON column on `subscriptions` holds the mapping; NULL means "use the default shape" (fully backwards compatible). Three new API endpoints (`GET /subscriptions/{id}/payload_sample`, `POST /subscriptions/{id}/payload_preview`, `GET /delivery-records/{id}/downstream_preview`) power the UI. The mapping editor is embedded in the existing `SubForm` in `Subscriptions.tsx`.

**Tech Stack:** Python 3.11 + FastAPI + SQLAlchemy 2.0 async + Alembic + Pydantic v2; React 19 + Vite + Tailwind 4 + React Query 5; pytest + pytest-asyncio.

**Spec:** [docs/superpowers/specs/2026-07-07-subscription-payload-mapping-design.md](../specs/2026-07-07-subscription-payload-mapping-design.md)

**Development principles:**
- TDD: write the failing test first, then the minimum code to pass.
- Frequent commits: one focused commit per task.
- Do NOT skip the "run the test and confirm it fails" step — it catches typos in the test that silently pass.
- All Python code must pass `uv run ruff check core apps tests` and `uv run mypy core apps` before commit.

---

## Chunk 1: Domain layer — mapper + source builder + snapshot plumbing

### Task 1.1: Alembic migration for `payload_mapping` column

**Files:**
- Create: `migrations/versions/0011_subscription_payload_mapping.py`

- [ ] **Step 1: Create the migration**

```python
"""add payload_mapping to subscriptions

Revision ID: 0011
Revises: 0010
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.add_column(sa.Column("payload_mapping", sa.JSON(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.drop_column("payload_mapping")
```

- [ ] **Step 2: Add the ORM column**

Modify `core/config/models.py`, in `class Subscription`, right after `business_name`:

```python
    payload_mapping: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
```

- [ ] **Step 3: Run the migration and verify the column exists**

```bash
uv run alembic upgrade head
uv run python -c "
from sqlalchemy import create_engine, inspect
e = create_engine('sqlite:///chain_indexer.db')
cols = {c['name'] for c in inspect(e).get_columns('subscriptions')}
assert 'payload_mapping' in cols, cols
print('OK')
"
```

Expected: `OK`.

- [ ] **Step 4: Commit**

```bash
git add migrations/versions/0011_subscription_payload_mapping.py core/config/models.py
git commit -m "feat(db): add payload_mapping column to subscriptions"
```

---

### Task 1.2: `payload_mapper.apply_mapping` — plain rename

**Files:**
- Create: `core/notifier/payload_mapper.py`
- Test: `tests/unit/test_payload_mapper.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_payload_mapper.py
from __future__ import annotations

from core.notifier.payload_mapper import apply_mapping


def test_apply_mapping_plain_rename() -> None:
    source = {"event": {"tx_hash": "0xabc", "block_number": 100}}
    mapping = {"fields": [
        {"target": "txHash",  "source": "event.tx_hash"},
        {"target": "blockNo", "source": "event.block_number"},
    ]}
    out, warnings = apply_mapping(source, mapping)
    assert out == {"txHash": "0xabc", "blockNo": 100}
    assert warnings == []
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest tests/unit/test_payload_mapper.py::test_apply_mapping_plain_rename -v
```

Expected: `ModuleNotFoundError: No module named 'core.notifier.payload_mapper'`.

- [ ] **Step 3: Create the module with the minimal implementation**

```python
# core/notifier/payload_mapper.py
from __future__ import annotations

from typing import Any


def _resolve_source(source: dict[str, Any], path: str) -> tuple[Any, bool]:
    """Return (value, found). value is None when found is False."""
    cur: Any = source
    for seg in path.split("."):
        if not isinstance(cur, dict) or seg not in cur:
            return None, False
        cur = cur[seg]
    return cur, True


def apply_mapping(
    source: dict[str, Any], mapping: dict[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    """Apply the field mapping to `source`. Never raises.

    Returns (output, warnings). See spec §2 for semantics.
    """
    out: dict[str, Any] = {}
    warnings: list[str] = []
    for field in mapping.get("fields", []):
        target: str = field["target"]
        if "const" in field and field.get("const") is not None:
            value: Any = field["const"]
        else:
            src = field.get("source", "")
            value, found = _resolve_source(source, src)
            if not found:
                warnings.append(f"source path not found: {src}")

        segments = target.split(".")
        node = out
        for seg in segments[:-1]:
            nxt = node.get(seg)
            if not isinstance(nxt, dict):
                if nxt is not None:
                    warnings.append(
                        f"target conflict: {'.'.join(segments)} overwrote {seg}"
                    )
                nxt = {}
                node[seg] = nxt
            node = nxt
        last = segments[-1]
        if last in node:
            warnings.append(f"target overwritten: {target}")
        node[last] = value
    return out, warnings
```

- [ ] **Step 4: Run to verify PASS**

```bash
uv run pytest tests/unit/test_payload_mapper.py -v
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/payload_mapper.py tests/unit/test_payload_mapper.py
git commit -m "feat(notifier): apply_mapping — plain field rename"
```

---

### Task 1.3: `apply_mapping` — nested target + nested source

**Files:**
- Modify: `tests/unit/test_payload_mapper.py`

- [ ] **Step 1: Add the failing tests**

Append:

```python
def test_apply_mapping_nested_target() -> None:
    source = {"event": {"args": {"value": "1000"}}}
    mapping = {"fields": [
        {"target": "data.amount", "source": "event.args.value"},
        {"target": "data.token",  "source": "event.args.value"},
    ]}
    out, warnings = apply_mapping(source, mapping)
    assert out == {"data": {"amount": "1000", "token": "1000"}}
    assert warnings == []


def test_apply_mapping_deep_nested_source() -> None:
    source = {"event": {"args": {"nested": {"deep": "hit"}}}}
    mapping = {"fields": [{"target": "d", "source": "event.args.nested.deep"}]}
    out, _ = apply_mapping(source, mapping)
    assert out == {"d": "hit"}
```

- [ ] **Step 2: Run — should PASS immediately**

```bash
uv run pytest tests/unit/test_payload_mapper.py -v
```

Expected: 3 passed. (The mapper already handles this — the tests exist to lock behavior in.)

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_payload_mapper.py
git commit -m "test(notifier): pin nested target + source semantics"
```

---

### Task 1.4: `apply_mapping` — constant injection

**Files:**
- Modify: `tests/unit/test_payload_mapper.py`, `core/notifier/payload_mapper.py`

- [ ] **Step 1: Add the failing tests**

```python
def test_apply_mapping_const_string() -> None:
    out, _ = apply_mapping({}, {"fields": [
        {"target": "source", "const": "chain-indexer"},
    ]})
    assert out == {"source": "chain-indexer"}


def test_apply_mapping_const_number() -> None:
    out, _ = apply_mapping({}, {"fields": [
        {"target": "version", "const": 1},
    ]})
    assert out == {"version": 1}


def test_apply_mapping_const_zero_is_kept() -> None:
    # Regression: 0 is falsy but a valid const.
    out, _ = apply_mapping({}, {"fields": [
        {"target": "count", "const": 0},
    ]})
    assert out == {"count": 0}


def test_apply_mapping_const_false_is_kept() -> None:
    out, _ = apply_mapping({}, {"fields": [
        {"target": "flag", "const": False},
    ]})
    assert out == {"flag": False}


def test_apply_mapping_const_none_is_kept() -> None:
    # Schema will reject None-only fields at API layer;
    # runtime must still handle it.
    out, _ = apply_mapping({}, {"fields": [
        {"target": "n", "const": None, "source": None},
    ]})
    # With both None, prefer source resolution (returns None, not found).
    assert out == {"n": None}


def test_apply_mapping_const_dict() -> None:
    out, _ = apply_mapping({}, {"fields": [
        {"target": "meta", "const": {"a": 1, "b": [2, 3]}},
    ]})
    assert out == {"meta": {"a": 1, "b": [2, 3]}}
```

- [ ] **Step 2: Run to see FAIL**

```bash
uv run pytest tests/unit/test_payload_mapper.py -v
```

Expected: `test_apply_mapping_const_zero_is_kept`, `test_apply_mapping_const_false_is_kept`, and `test_apply_mapping_const_dict` **pass** already (the initial `is not None` check handles them). The one that actually FAILs is `test_apply_mapping_const_none_is_kept`: with `const=None` the code falls into the else branch and hits `_resolve_source(source, "")` (empty path), returning `(source, False)` — so the value is the whole source dict, not `None`. That's the bug we want to lock down.

- [ ] **Step 3: Fix the branch to use key presence, and handle explicit `None` source**

Replace the `if "const" in field ...` block in `core/notifier/payload_mapper.py`:

```python
        has_const = "const" in field
        has_source = "source" in field and field.get("source") is not None
        if has_const:
            value: Any = field["const"]
        elif has_source:
            src = field["source"]
            value, found = _resolve_source(source, src)
            if not found:
                warnings.append(f"source path not found: {src}")
        else:
            # Both source and const absent. Schema layer rejects this at
            # API boundary; runtime falls back to None to stay non-fatal.
            value = None
```

Note: `has_const` uses key presence (not truthiness), so `const=None`, `const=0`,
`const=False` all take the const branch — that's the whole point.

- [ ] **Step 4: Run to verify PASS**

```bash
uv run pytest tests/unit/test_payload_mapper.py -v
```

Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/payload_mapper.py tests/unit/test_payload_mapper.py
git commit -m "feat(notifier): const injection in apply_mapping"
```

---

### Task 1.5: `apply_mapping` — missing source + target conflict warnings

**Files:**
- Modify: `tests/unit/test_payload_mapper.py`

- [ ] **Step 1: Add the failing tests**

```python
def test_apply_mapping_missing_source_warns_and_nulls() -> None:
    out, warnings = apply_mapping(
        {"event": {"args": {}}},
        {"fields": [{"target": "amount", "source": "event.args.value"}]},
    )
    assert out == {"amount": None}
    assert warnings == ["source path not found: event.args.value"]


def test_apply_mapping_non_dict_intermediate_warns() -> None:
    # `event` is a string, so `event.tx_hash` cannot descend.
    out, warnings = apply_mapping(
        {"event": "not-a-dict"},
        {"fields": [{"target": "h", "source": "event.tx_hash"}]},
    )
    assert out == {"h": None}
    assert warnings == ["source path not found: event.tx_hash"]


def test_apply_mapping_scalar_then_nested_conflict() -> None:
    out, warnings = apply_mapping(
        {},
        {"fields": [
            {"target": "data",     "const": "scalar"},
            {"target": "data.foo", "const": "nested"},
        ]},
    )
    assert out == {"data": {"foo": "nested"}}
    assert any("target conflict" in w for w in warnings)


def test_apply_mapping_duplicate_final_segment_last_wins() -> None:
    out, warnings = apply_mapping(
        {},
        {"fields": [
            {"target": "x", "const": "a"},
            {"target": "x", "const": "b"},
        ]},
    )
    assert out == {"x": "b"}
    assert warnings == ["target overwritten: x"]
```

- [ ] **Step 2: Run**

```bash
uv run pytest tests/unit/test_payload_mapper.py -v
```

Expected: all passed (the implementation already covers these; tests lock behavior in).

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_payload_mapper.py
git commit -m "test(notifier): pin mapper warning semantics"
```

---

### Task 1.6: Split `build_payload` to return `(source, delivery_payload)`

**Files:**
- Modify: `core/notifier/payload.py`
- Modify: `core/config/snapshot.py`
- Modify: `tests/unit/test_payload.py`
- Modify: `core/notifier/notifier.py` (call sites)

Rationale: the spec (§2 and Deliverables checklist) requires `build_payload` to
return a two-tuple so `delivery_id` / `delivered_at` are **identical** in the
wire payload and in `delivery_records.event_payload` (operators need this
correlation for downstream matching). Extract the un-mapped structure once,
then optionally apply the mapping — never invoke `_now_unix()` / `_gen_id()`
twice per event.

- [ ] **Step 1: Add `payload_mapping` to `SnapshotSubscription`**

In `core/config/snapshot.py`, in `@dataclass(frozen=True) class SnapshotSubscription`, add after `business_name`:

```python
    payload_mapping: dict[str, Any] | None = None
```

In `load_snapshot`, pass it through:

```python
        snap_subs.append(
            SnapshotSubscription(
                id=sub.id,
                # ... existing fields ...
                business_name=sub.business_name,
                payload_mapping=sub.payload_mapping,
            )
        )
```

- [ ] **Step 2: Write failing tests**

Replace the existing `test_payload_shape_matches_spec_section_8`, `test_delivery_id_is_unique_per_call`, `test_payload_includes_business_name_when_set`, `test_payload_omits_business_name_when_none`, `test_payload_business_name_key_position` bodies to consume the new tuple shape — they already assert on a single dict `p`; change every `p = build_payload(...)` to `_, p = build_payload(...)` (source discarded, delivery kept). Then append:

```python
def test_build_payload_returns_source_and_delivery_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "did")

    source, delivery = build_payload(event=_event(), subscription=_sub())
    assert source == delivery  # No mapping → the two dicts are equal (same object OK too).
    assert source["delivery_id"] == "did"
    assert delivery["delivery_id"] == "did"


def test_build_payload_applies_mapping_when_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "did")

    sub = SnapshotSubscription(
        id=_sub().id, name=_sub().name, chain_id=_sub().chain_id,
        address=_sub().address, abi_id=_sub().abi_id,
        match_kind=_sub().match_kind, match_name=_sub().match_name,
        arg_filters=_sub().arg_filters, enabled=True,
        channel_ids=_sub().channel_ids,
        payload_mapping={"fields": [
            {"target": "txHash", "source": "event.tx_hash"},
            {"target": "amt",    "source": "event.args.value"},
            {"target": "src",    "const":  "chain-indexer"},
        ]},
    )
    source, delivery = build_payload(event=_event(), subscription=sub)

    # Source: full un-mapped shape.
    assert source["event"]["tx_hash"] == "0xtx"
    assert source["delivery_id"] == "did"

    # Delivery: only mapped keys + auto metadata.
    assert delivery == {
        "txHash": "0xtx",
        "amt": "1000000000",
        "src": "chain-indexer",
        "delivery_id": "did",       # SAME as source["delivery_id"]
        "delivered_at": 1735689601, # SAME as source["delivered_at"]
    }
    # Identity assertion — this is the whole reason for the tuple.
    assert source["delivery_id"] == delivery["delivery_id"]
    assert source["delivered_at"] == delivery["delivered_at"]


def test_build_payload_replay_flag_appended_when_mapping_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "did")

    sub = SnapshotSubscription(
        id=_sub().id, name=_sub().name, chain_id=_sub().chain_id,
        address=_sub().address, abi_id=_sub().abi_id,
        match_kind=_sub().match_kind, match_name=_sub().match_name,
        arg_filters=_sub().arg_filters, enabled=True,
        channel_ids=_sub().channel_ids,
        payload_mapping={"fields": [
            {"target": "txHash", "source": "event.tx_hash"},
        ]},
    )
    source, delivery = build_payload(event=_event(), subscription=sub, replay=True)
    assert source["replay"] is True
    assert delivery["replay"] is True


def test_build_payload_user_can_override_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "did")

    sub = SnapshotSubscription(
        id=_sub().id, name=_sub().name, chain_id=_sub().chain_id,
        address=_sub().address, abi_id=_sub().abi_id,
        match_kind=_sub().match_kind, match_name=_sub().match_name,
        arg_filters=_sub().arg_filters, enabled=True,
        channel_ids=_sub().channel_ids,
        payload_mapping={"fields": [
            {"target": "delivery_id", "const": "custom-id"},
        ]},
    )
    _source, delivery = build_payload(event=_event(), subscription=sub)
    # User target wins (setdefault only adds when missing).
    assert delivery["delivery_id"] == "custom-id"
    # But delivered_at still auto-appended.
    assert delivery["delivered_at"] == 1735689601


def test_build_source_payload_returns_full_default_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from core.notifier.payload import build_source_payload

    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "delivery-uuid")

    p = build_source_payload(event=_event(), subscription=_sub())
    assert p["subscription_id"] == _sub().id
    assert p["event"]["tx_hash"] == "0xtx"
    assert p["delivery_id"] == "delivery-uuid"
    assert p["delivered_at"] == 1735689601
```

- [ ] **Step 3: Run — expect FAIL**

```bash
uv run pytest tests/unit/test_payload.py -v
```

Expected: existing tests still call `build_payload` returning a dict (they'll fail with tuple unpacking errors once you edit them per Step 2; if they pass the edit but new tests fail with ImportError on `build_source_payload`, that's the right state).

- [ ] **Step 4: Rewrite `core/notifier/payload.py`**

```python
from __future__ import annotations

import time
import uuid
from typing import Any

import structlog

from core.config.snapshot import SnapshotSubscription
from core.notifier.payload_mapper import apply_mapping
from core.parser.event import Event

log = structlog.get_logger(__name__)


def _now_unix() -> int:
    return int(time.time())


def _gen_id() -> str:
    return str(uuid.uuid4())


def _safe(obj: Any) -> Any:
    if isinstance(obj, bytes):
        return "0x" + obj.hex()
    if isinstance(obj, dict):
        return {str(k): _safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe(v) for v in obj]
    if isinstance(obj, (int, float, bool, str)) or obj is None:
        return obj
    return str(obj)


def build_source_payload(
    *, event: Event, subscription: SnapshotSubscription, replay: bool = False
) -> dict[str, Any]:
    """The un-mapped payload structure. Stored in delivery_records and used as
    the input to `apply_mapping` when the subscription has a mapping.
    """
    payload: dict[str, Any] = {
        "subscription_id": subscription.id,
        "subscription_name": subscription.name,
    }
    if subscription.business_name:
        payload["business_name"] = subscription.business_name
    payload["chain_id"] = event.chain_id
    payload["event"] = {
        "kind": event.kind,
        "name": event.name,
        "contract": _safe(event.contract),
        "block_number": event.block_number,
        "block_hash": _safe(event.block_hash),
        "block_timestamp": event.block_timestamp,
        "tx_hash": _safe(event.tx_hash),
        "tx_index": event.tx_index,
        "log_index": event.log_index,
        "args": _safe(dict(event.args)),
    }
    payload["delivered_at"] = _now_unix()
    payload["delivery_id"] = _gen_id()
    if replay:
        payload["replay"] = True
    return payload


def build_payload(
    *, event: Event, subscription: SnapshotSubscription, replay: bool = False
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (source, delivery_payload).

    - `source` is always the un-mapped shape and is what gets persisted to
      delivery_records.event_payload.
    - `delivery_payload` is what channels send. When the subscription has no
      mapping, it IS `source` (same object; safe because callers don't mutate).
      When mapping is set, it is the mapped result with delivery_id /
      delivered_at / replay carried over from `source` (setdefault semantics).
    """
    source = build_source_payload(event=event, subscription=subscription, replay=replay)
    if not subscription.payload_mapping:
        return source, source

    delivery, warnings = apply_mapping(source, subscription.payload_mapping)
    for w in warnings:
        log.warning(
            "payload_mapper.warning",
            subscription_id=subscription.id, msg=w,
        )

    # Metadata fallback: user's mapping may drop these, but the delivery system
    # needs them for idempotency + observability. setdefault → user's value
    # wins if they explicitly targeted the key. Because both dicts come from
    # the same `source` values, `delivery_id` and `delivered_at` are identical.
    delivery.setdefault("delivery_id", source["delivery_id"])
    delivery.setdefault("delivered_at", source["delivered_at"])
    if replay:
        delivery.setdefault("replay", True)
    return source, delivery
```

- [ ] **Step 5: Fix every existing call site**

Grep for `build_payload(` across the codebase:

```bash
grep -rn "build_payload(" core apps tests --include="*.py"
```

Expected hits (production paths):
- `core/notifier/notifier.py` — the notifier itself (updated in Task 1.7).

Test hits: the ones you already edited in Step 2. If any other production call
appears (e.g. a replay path in `core/parser` or `apps/worker`), unpack the tuple
there too — the receiver should always want either the source (for persistence)
or the delivery (for sending), never the ambiguous single dict.

- [ ] **Step 6: Run all payload + mapper tests to verify PASS**

```bash
uv run pytest tests/unit/test_payload.py tests/unit/test_payload_mapper.py -v
```

Expected: all passed.

- [ ] **Step 7: Commit**

```bash
git add core/notifier/payload.py core/config/snapshot.py tests/unit/test_payload.py
git commit -m "feat(notifier): build_payload returns (source, delivery) tuple

Ensures delivery_id / delivered_at are identical between the payload
sent to channels and the source persisted to delivery_records — needed
for downstream idempotency correlation. Splits the mapping application
out of the metadata generation."
```

---

### Task 1.7: Notifier — thread source + delivery through dispatch

**Files:**
- Modify: `core/notifier/notifier.py`
- Modify: `tests/unit/test_notifier.py`

The callback signature stays `(sub_id, ch_id, chain_id, payload, error, attempts)`.
The `payload` handed to callbacks becomes **source** (pre-mapping). The value handed
to `ch.send(...)` becomes the **delivery** payload (post-mapping). Nothing else in
the callback surface changes — `is_replay=bool(payload.get("replay"))` still
resolves correctly from `source` because `replay` also lives on `source`.

- [ ] **Step 1: Write a failing behavioral test**

Append to `tests/unit/test_notifier.py`:

```python
async def test_notifier_persists_source_but_delivers_mapped() -> None:
    from core.config.snapshot import SnapshotChannel, SnapshotSubscription
    from core.notifier.channel import Channel
    from core.notifier.notifier import Notifier
    from core.parser.event import Event

    delivered: list[dict] = []
    persisted: list[dict] = []

    class Recorder(Channel):
        type = "recorder"
        config_schema = {}
        async def start(self) -> None: pass
        async def stop(self) -> None: pass
        async def send(self, payload):
            delivered.append(payload)

    async def on_success(sub_id, ch_id, chain_id, payload, err, attempts):
        persisted.append(payload)

    ch_cfg = SnapshotChannel(id="c1", name="rec", type="recorder", config={})
    sub = SnapshotSubscription(
        id="s1", name="s", chain_id="eth", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None, arg_filters={},
        enabled=True, channel_ids=["c1"],
        payload_mapping={"fields": [
            {"target": "h", "source": "event.tx_hash"},
        ]},
    )
    ev = Event(
        chain_id="eth", block_number=1, block_hash="0xb", block_timestamp=0,
        tx_hash="0xTX", tx_index=0, log_index=0,
        kind="native_transfer", contract=None, name=None, args={}, raw={},
    )
    n = Notifier(channel_factory=lambda cfg: Recorder(config={}), on_success=on_success)
    await n.start([ch_cfg])
    await n.dispatch(ev, [(sub, [ch_cfg])])
    await n.stop()

    assert len(delivered) == 1 and delivered[0]["h"] == "0xTX"
    assert "event" not in delivered[0]  # strict mode

    assert len(persisted) == 1 and persisted[0]["event"]["tx_hash"] == "0xTX"
    assert "h" not in persisted[0]  # source, not mapped

    # Identity: delivery_id in both dicts is the same value.
    assert delivered[0]["delivery_id"] == persisted[0]["delivery_id"]
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest tests/unit/test_notifier.py::test_notifier_persists_source_but_delivers_mapped -v
```

Expected: FAIL — dispatch still calls `build_payload` expecting a single dict.

- [ ] **Step 3: Update `Notifier.dispatch` + `_send_one`**

In `core/notifier/notifier.py`:

```python
    async def dispatch(
        self,
        event: Event,
        hits: Sequence[tuple[SnapshotSubscription, Sequence[SnapshotChannel]]],
        *,
        replay: bool = False,
    ) -> None:
        from core.notifier.payload import build_payload

        tasks: list[asyncio.Task[None]] = []
        for sub, chans in hits:
            source, delivery_payload = build_payload(
                event=event, subscription=sub, replay=replay,
            )
            for ch_cfg in chans:
                ch = self._channels.get(ch_cfg.id)
                if ch is None:
                    log.warning(
                        "notifier.channel_not_started",
                        channel_id=ch_cfg.id, subscription_id=sub.id,
                    )
                    continue
                tasks.append(asyncio.create_task(
                    self._send_one(ch, source, delivery_payload, sub.id, ch_cfg.id)
                ))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_one(
        self,
        ch: Channel,
        source: dict[str, Any],
        delivery_payload: dict[str, Any],
        subscription_id: str,
        channel_id: str,
    ) -> None:
        import time
        from core.metrics import CHANNEL_SEND_SECONDS, CHANNEL_SENDS_TOTAL

        t0 = time.perf_counter()
        send_status = "failed"
        async with self._get_sem():
            try:
                await ch.send(delivery_payload)
                send_status = "success"
                if self._on_success:
                    try:
                        await self._on_success(
                            subscription_id, channel_id,
                            source.get("chain_id", ""),
                            source, None, 1,
                        )
                    except Exception:  # noqa: BLE001
                        log.error("notifier.on_success_callback_error")
            except Exception as exc:  # noqa: BLE001
                attempts = getattr(exc, "attempts", 1)
                log.error(
                    "notifier.send_failed",
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    delivery_id=delivery_payload.get("delivery_id"),
                    attempts=attempts, error=repr(exc),
                )
                if self._on_failure:
                    try:
                        await self._on_failure(
                            subscription_id, channel_id,
                            source.get("chain_id", ""),
                            source, repr(exc), attempts,
                        )
                    except Exception:  # noqa: BLE001
                        log.error("notifier.on_failure_callback_error")
            finally:
                CHANNEL_SEND_SECONDS.labels(ch.type).observe(time.perf_counter() - t0)
                CHANNEL_SENDS_TOTAL.labels(ch.type, send_status).inc()
```

- [ ] **Step 4: Confirm `apps/worker/main.py` still works**

`_on_delivery_success` and `_on_delivery_failure` read `payload.get("replay", False)` — `source` includes `replay` when `replay=True`, so this stays correct. No worker changes needed.

- [ ] **Step 5: Run all notifier + worker tests**

```bash
uv run pytest tests/unit/test_notifier.py tests/unit/test_notifier_sem_lazy.py -v
uv run pytest tests/integration/ -v -k "delivery"
```

Expected: all passed.

- [ ] **Step 6: Commit**

```bash
git add core/notifier/notifier.py tests/unit/test_notifier.py
git commit -m "feat(notifier): dispatch persists source; sends mapped payload

Callbacks now receive the pre-mapping source structure so
delivery_records.event_payload preserves the raw event view — used by
the /payload_sample endpoint (learn from real deliveries) and the
delivery record downstream preview. Channels still receive the mapped
payload."
```

---

### Task 1.8: Renumbered — this task is now merged into Task 1.7 above.

(Retained slot number for legacy references; skip to Chunk 1 review.)

---

### Chunk 1 review

- [ ] **Dispatch spec-document-reviewer subagent** with the current plan file + spec path. Fix any issues. Loop until approved (max 5).

---

## Chunk 2: Web API — CRUD + sample + preview

### Task 2.1: Pydantic mapping schemas

**Files:**
- Modify: `apps/web/schemas.py`
- Create: `tests/unit/test_payload_mapping_schema.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_payload_mapping_schema.py
from __future__ import annotations

import pytest
from pydantic import ValidationError

from apps.web.schemas import MappingField, PayloadMapping


def test_mapping_field_source_only_ok() -> None:
    f = MappingField(target="tx", source="event.tx_hash")
    assert f.source == "event.tx_hash" and f.const is None


def test_mapping_field_const_only_ok() -> None:
    f = MappingField(target="v", const="chain-indexer")
    assert f.const == "chain-indexer" and f.source is None


def test_mapping_field_both_source_and_const_rejected() -> None:
    with pytest.raises(ValidationError):
        MappingField(target="v", source="x", const="y")


def test_mapping_field_neither_source_nor_const_rejected() -> None:
    with pytest.raises(ValidationError):
        MappingField(target="v")


def test_mapping_field_empty_target_rejected() -> None:
    with pytest.raises(ValidationError):
        MappingField(target="", source="x")


def test_mapping_field_target_max_length() -> None:
    with pytest.raises(ValidationError):
        MappingField(target="a" * 201, source="x")


def test_payload_mapping_duplicate_target_rejected() -> None:
    with pytest.raises(ValidationError):
        PayloadMapping(fields=[
            MappingField(target="x", source="a"),
            MappingField(target="x", source="b"),
        ])


def test_payload_mapping_empty_segment_rejected() -> None:
    for bad in ("a..b", ".a", "a.", "."):
        with pytest.raises(ValidationError):
            PayloadMapping(fields=[MappingField(target=bad, source="a")])


def test_payload_mapping_empty_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        PayloadMapping(fields=[])


def test_payload_mapping_too_many_fields_rejected() -> None:
    fields = [MappingField(target=f"t{i}", source="a") for i in range(101)]
    with pytest.raises(ValidationError):
        PayloadMapping(fields=fields)


def test_mapping_field_explicit_null_const_allowed() -> None:
    # `{"target":"x","const":null}` — user intent is "emit null literal".
    # The schema layer accepts this; the mapper writes null.
    f = MappingField.model_validate({"target": "x", "const": None})
    assert f.const is None
    assert "const" in f.model_fields_set


def test_mapping_field_explicit_null_source_rejected() -> None:
    # `source: null` is meaningless (no path to follow) — reject at schema layer.
    with pytest.raises(ValidationError):
        MappingField.model_validate({"target": "x", "source": None})
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest tests/unit/test_payload_mapping_schema.py -v
```

Expected: `ImportError` — `MappingField` doesn't exist.

- [ ] **Step 3: Add the schemas**

In `apps/web/schemas.py`, before `class SubscriptionCreate`, add:

```python
class MappingField(BaseModel):
    target: str = Field(min_length=1, max_length=200)
    source: str | None = None
    const:  Any | None = None

    @model_validator(mode="after")
    def _xor(self) -> MappingField:
        # Symmetric key-presence check: distinguishes "user omitted the key"
        # from "user explicitly passed null". `const: null` is a legitimate
        # constant value; `source: null` is not (a source path must be a
        # non-empty string). So `has_source` also requires the string form,
        # while `has_const` just needs the key to be set.
        has_source = self.source is not None
        has_const = "const" in self.model_fields_set
        if has_source == has_const:
            raise ValueError("field 必须且只能设置 source 或 const 之一")
        return self


class PayloadMapping(BaseModel):
    fields: list[MappingField] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def _validate(self) -> PayloadMapping:
        seen: set[str] = set()
        for f in self.fields:
            if f.target in seen:
                raise ValueError(f"target 重复: {f.target}")
            seen.add(f.target)
            if any(seg == "" for seg in f.target.split(".")):
                raise ValueError(f"target 路径非法: {f.target}")
        return self
```

Notes:
- Pydantic v2 populates `model_fields_set` from JSON key presence, so a
  request body `{"target":"x","const":null}` correctly registers `const` as set.
- The `_xor` check does allow `{"target":"x","const":null}` through as a
  const-only field — the mapper then writes `null` to the target. That's
  intentional and covered by `test_mapping_field_explicit_null_const_allowed`
  in the schema test file (add it in the same task).

- [ ] **Step 4: Run to verify PASS**

```bash
uv run pytest tests/unit/test_payload_mapping_schema.py -v
```

Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add apps/web/schemas.py tests/unit/test_payload_mapping_schema.py
git commit -m "feat(api): PayloadMapping + MappingField pydantic schemas"
```

---

### Task 2.2: Wire `payload_mapping` into subscription CRUD

**Files:**
- Modify: `apps/web/schemas.py`
- Modify: `apps/web/routers/subscriptions.py`
- Modify: `core/config/repositories.py`
- Create: `tests/integration/test_subscription_payload_mapping_api.py`

**Testing pattern.** Existing integration tests (see
`tests/integration/test_delivery_records_router.py` and `test_web_api.py`)
follow this shape:

```python
import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.repositories import ChainRepo
from core.config.models import ChainKind

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_something(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        # Seed a chain first — subscriptions need chain_id to exist.
        async with db.session() as s:
            await ChainRepo(s).create(
                id="eth", kind=ChainKind.evm, rpc_http="http://x",
                rpc_ws=None, confirmations=0, poll_interval_ms=3000,
                enabled=True,
            )
            await s.commit()

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            # ... your assertions
            pass
    finally:
        await bus.disconnect()
```

The `db` fixture comes from `tests/integration/conftest.py` (in-memory
SQLite + all tables created via `Base.metadata.create_all`). The `redis_url`
fixture comes from `tests/conftest.py`. **Do not** invent `client` / `seed_chain` /
`session` fixtures — mirror the pattern above verbatim in each test.

- [ ] **Step 1: Write a failing round-trip test**

Create `tests/integration/test_subscription_payload_mapping_api.py`:

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChainKind
from core.config.repositories import ChainRepo

pytestmark = pytest.mark.integration


async def _seed_chain(db: Database, chain_id: str = "eth") -> str:
    async with db.session() as s:
        await ChainRepo(s).create(
            id=chain_id, kind=ChainKind.evm, rpc_http="http://x",
            rpc_ws=None, confirmations=0, poll_interval_ms=3000,
            enabled=True,
        )
        await s.commit()
    return chain_id


def _app_client(db: Database, bus: RedisBus):
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


@pytest.mark.asyncio
async def test_create_read_update_clear_payload_mapping(
    db: Database, redis_url: str,
) -> None:
    bus = RedisBus(url=redis_url); await bus.connect()
    try:
        chain_id = await _seed_chain(db)
        async with _app_client(db, bus) as c:
            body = {
                "name": "s1", "chain_id": chain_id,
                "match_kind": "native_transfer",
                "payload_mapping": {"fields": [
                    {"target": "txHash", "source": "event.tx_hash"},
                    {"target": "src",    "const":  "chain-indexer"},
                ]},
            }
            r = await c.post("/api/subscriptions", json=body)
            assert r.status_code == 201, r.text
            sid = r.json()["id"]

            r = await c.get(f"/api/subscriptions/{sid}")
            assert r.status_code == 200
            got = r.json()
            assert got["payload_mapping"]["fields"][0]["target"] == "txHash"

            body["payload_mapping"]["fields"].append({"target": "v", "const": 1})
            r = await c.put(f"/api/subscriptions/{sid}", json=body)
            assert r.status_code == 200
            assert len(r.json()["payload_mapping"]["fields"]) == 3

            body["payload_mapping"] = None
            r = await c.put(f"/api/subscriptions/{sid}", json=body)
            assert r.status_code == 200
            assert r.json()["payload_mapping"] is None
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run to verify FAIL**

```bash
uv run pytest tests/integration/test_subscription_payload_mapping_api.py -v
```

Expected: FAIL — router doesn't accept the field yet.

- [ ] **Step 3: Extend schemas + repo + router**

`apps/web/schemas.py` — add to `SubscriptionCreate`:

```python
    payload_mapping: PayloadMapping | None = None
```

Add to `SubscriptionOut`:

```python
    payload_mapping: dict[str, Any] | None = None
```

`core/config/repositories.py` — extend `SubscriptionRepo.create` signature to accept `payload_mapping: dict[str, Any] | None = None` and pass it into the `Subscription(...)` constructor:

```python
    async def create(
        self, *,
        name: str, chain_id: str, address: str | None, abi_id: str | None,
        match_kind: MatchKind, match_name: str | None,
        arg_filters: dict[str, Any], enabled: bool,
        start_block: int | None = None,
        business_name: str | None = None,
        payload_mapping: dict[str, Any] | None = None,
    ) -> Subscription:
        sub = Subscription(
            name=name, chain_id=chain_id, address=address, abi_id=abi_id,
            match_kind=match_kind, match_name=match_name, arg_filters=arg_filters,
            enabled=enabled, start_block=start_block,
            business_name=business_name, payload_mapping=payload_mapping,
        )
        self.s.add(sub); await self.s.flush(); return sub
```

`apps/web/routers/subscriptions.py` — in `create_subscription` and `update_subscription`, plumb the field through. **Important:** in `update_subscription`, `PUT` with `payload_mapping: null` must actively set the column to NULL (not "leave unchanged"). Convert the pydantic model back to plain dict for storage:

```python
    # In create_subscription:
    sub = await SubscriptionRepo(session).create(
        # ... existing fields ...
        business_name=payload.business_name,
        payload_mapping=(
            payload.payload_mapping.model_dump() if payload.payload_mapping else None
        ),
    )

    # In update_subscription:
    await repo.update(
        sub_id,
        # ... existing fields ...
        business_name=payload.business_name,
        payload_mapping=(
            payload.payload_mapping.model_dump() if payload.payload_mapping else None
        ),
    )
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/integration/test_subscription_payload_mapping_api.py tests/integration/test_web_api.py -v
```

Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add apps/web/schemas.py apps/web/routers/subscriptions.py core/config/repositories.py tests/integration/test_subscription_payload_mapping_api.py
git commit -m "feat(api): create/update/read subscription payload_mapping"
```

---

### Task 2.3: `GET /subscriptions/{id}/payload_sample`

**Files:**
- Create: `core/notifier/sample.py` (synthetic sample builder)
- Modify: `apps/web/routers/subscriptions.py`
- Test: extend `tests/integration/test_subscription_payload_mapping_api.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/integration/test_subscription_payload_mapping_api.py`:

```python
@pytest.mark.asyncio
async def test_payload_sample_synthetic_when_no_delivery(
    db: Database, redis_url: str,
) -> None:
    bus = RedisBus(url=redis_url); await bus.connect()
    try:
        chain_id = await _seed_chain(db, "eth-a")
        async with _app_client(db, bus) as c:
            r = await c.post("/api/subscriptions", json={
                "name": "s", "chain_id": chain_id, "match_kind": "native_transfer",
            })
            sid = r.json()["id"]
            r = await c.get(f"/api/subscriptions/{sid}/payload_sample")
            assert r.status_code == 200
            body = r.json()
            assert body["origin"] == "synthetic"
            assert body["sample"]["event"]["kind"] == "native_transfer"
            assert "tx_hash" in body["sample"]["event"]
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_payload_sample_prefers_real_delivery(
    db: Database, redis_url: str,
) -> None:
    from core.config.repositories import DeliveryRecordRepo

    bus = RedisBus(url=redis_url); await bus.connect()
    try:
        chain_id = await _seed_chain(db, "eth-b")
        async with _app_client(db, bus) as c:
            r = await c.post("/api/subscriptions", json={
                "name": "s", "chain_id": chain_id, "match_kind": "native_transfer",
            })
            sid = r.json()["id"]

            async with db.session() as s:
                await DeliveryRecordRepo(s).create(
                    subscription_id=sid, channel_id="ch",
                    chain_id=chain_id,
                    event_payload={
                        "marker": "real-delivery",
                        "event": {"tx_hash": "0xabc"},
                    },
                    status="success",
                )
                await s.commit()

            r = await c.get(f"/api/subscriptions/{sid}/payload_sample")
            body = r.json()
            assert body["origin"] == "delivery"
            assert body["sample"]["marker"] == "real-delivery"
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run — FAIL**

```bash
uv run pytest tests/integration/test_subscription_payload_mapping_api.py -v -k sample
```

Expected: 404 (endpoint doesn't exist).

- [ ] **Step 3: Implement the synthetic builder**

```python
# core/notifier/sample.py
from __future__ import annotations

from typing import Any

from core.config.snapshot import SnapshotSubscription
from core.notifier.payload import build_source_payload
from core.parser.event import Event

_SYNTH_EVENT = Event(
    chain_id="synthetic",
    block_number=0,
    block_hash="0x0",
    block_timestamp=0,
    tx_hash="0x0",
    tx_index=0,
    log_index=0,
    kind="native_transfer",
    contract="0x0",
    name=None,
    args={},
    raw={},
)


def _args_placeholder_from_evm_abi(
    body: list[dict[str, Any]], match_kind: str, match_name: str | None,
) -> dict[str, str]:
    entry_type = "event" if match_kind == "event" else "function"
    for entry in body:
        if entry.get("type") == entry_type and entry.get("name") == match_name:
            return {
                inp.get("name") or f"arg{i}": f"<{inp.get('type', 'any')}>"
                for i, inp in enumerate(entry.get("inputs", []))
            }
    return {}


def _args_placeholder_from_idl(
    body: dict[str, Any], match_kind: str, match_name: str | None,
) -> dict[str, str]:
    section = "events" if match_kind == "event" else "instructions"
    entries = body.get(section, []) if isinstance(body, dict) else []
    for entry in entries:
        if entry.get("name") == match_name:
            fields = entry.get("fields") or entry.get("args") or []
            return {
                (f.get("name") or f"arg{i}"): f"<{str(f.get('type', 'any'))}>"
                for i, f in enumerate(fields)
            }
    return {}


def build_synthetic_sample(
    subscription: SnapshotSubscription, abi_body: Any | None,
) -> dict[str, Any]:
    """Produce a placeholder payload that has the right shape but obviously fake values."""
    kind = subscription.match_kind
    args: dict[str, Any] = {}
    if kind in ("event", "call") and abi_body is not None:
        if isinstance(abi_body, list):
            args = _args_placeholder_from_evm_abi(
                abi_body, kind, subscription.match_name
            )
        elif isinstance(abi_body, dict):
            args = _args_placeholder_from_idl(
                abi_body, kind, subscription.match_name
            )

    # Narrow to the Literal expected by Event without silencing mypy.
    from typing import cast
    from core.parser.event import EventKind
    valid_kinds: tuple[EventKind, ...] = (
        "native_transfer", "token_transfer", "event", "call",
    )
    event_kind: EventKind = cast(EventKind, kind) if kind in valid_kinds else "native_transfer"

    ev = Event(
        chain_id=subscription.chain_id or "synthetic",
        block_number=0, block_hash="0x0", block_timestamp=0,
        tx_hash="0x0", tx_index=0, log_index=0,
        kind=event_kind,
        contract=subscription.address or "0x0",
        name=subscription.match_name,
        args=args,
        raw={},
    )
    return build_source_payload(event=ev, subscription=subscription)
```

- [ ] **Step 4: Add the endpoint**

In `apps/web/routers/subscriptions.py`:

```python
from apps.web.schemas import PayloadSampleOut
from core.notifier.sample import build_synthetic_sample
from core.config.repositories import AbiRepo, DeliveryRecordRepo


@router.get("/{sub_id}/payload_sample", response_model=PayloadSampleOut)
async def get_payload_sample(
    sub_id: str,
    refresh: int = 0,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> PayloadSampleOut:
    _ = refresh  # cache-buster only; behavior is idempotent
    sub_row = await SubscriptionRepo(session).get(sub_id)
    if sub_row is None:
        raise HTTPException(status_code=404, detail="subscription not found")

    # 1) Prefer a real delivery.
    rows = await DeliveryRecordRepo(session).list_all(
        limit=1, subscription_id=sub_id, status="success",
    )
    if rows:
        return PayloadSampleOut(sample=rows[0].event_payload, origin="delivery")

    # 2) Synthetic fallback.
    abi_body: Any | None = None
    if sub_row.abi_id:
        abi_row = await AbiRepo(session).get(sub_row.abi_id)
        if abi_row is not None:
            abi_body = abi_row.body

    from core.config.snapshot import SnapshotSubscription
    snap = SnapshotSubscription(
        id=sub_row.id, name=sub_row.name, chain_id=sub_row.chain_id,
        address=sub_row.address, abi_id=sub_row.abi_id,
        match_kind=sub_row.match_kind.value, match_name=sub_row.match_name,
        arg_filters=sub_row.arg_filters or {}, enabled=True,
        business_name=sub_row.business_name,
    )
    return PayloadSampleOut(sample=build_synthetic_sample(snap, abi_body), origin="synthetic")
```

Add to `apps/web/schemas.py`:

```python
class PayloadSampleOut(BaseModel):
    sample: dict[str, Any]
    origin: Literal["delivery", "synthetic"]
```

- [ ] **Step 5: Run**

```bash
uv run pytest tests/integration/test_subscription_payload_mapping_api.py -v -k sample
```

Expected: passed.

- [ ] **Step 6: Commit**

```bash
git add core/notifier/sample.py apps/web/schemas.py apps/web/routers/subscriptions.py tests/integration/test_subscription_payload_mapping_api.py
git commit -m "feat(api): GET /subscriptions/{id}/payload_sample"
```

---

### Task 2.4: `POST /subscriptions/{id}/payload_preview`

**Files:**
- Modify: `apps/web/routers/subscriptions.py`, `apps/web/schemas.py`
- Test: extend `tests/integration/test_subscription_payload_mapping_api.py`

- [ ] **Step 1: Write failing test**

Append to `tests/integration/test_subscription_payload_mapping_api.py`:

```python
@pytest.mark.asyncio
async def test_payload_preview_applies_mapping(
    db: Database, redis_url: str,
) -> None:
    bus = RedisBus(url=redis_url); await bus.connect()
    try:
        chain_id = await _seed_chain(db, "eth-c")
        async with _app_client(db, bus) as c:
            r = await c.post("/api/subscriptions", json={
                "name": "s", "chain_id": chain_id,
                "match_kind": "native_transfer",
            })
            sid = r.json()["id"]
            r = await c.post(f"/api/subscriptions/{sid}/payload_preview", json={
                "mapping": {"fields": [
                    {"target": "txHash",        "source": "event.tx_hash"},
                    {"target": "missing_field", "source": "event.does_not_exist"},
                    {"target": "brand",         "const":  "chain-indexer"},
                ]},
                "sample": {"event": {"tx_hash": "0xdead"}},
            })
            assert r.status_code == 200
            body = r.json()
            assert body["output"]["txHash"] == "0xdead"
            assert body["output"]["brand"] == "chain-indexer"
            assert body["output"]["missing_field"] is None
            assert any("event.does_not_exist" in w for w in body["warnings"])
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_payload_preview_uses_sample_endpoint_when_omitted(
    db: Database, redis_url: str,
) -> None:
    bus = RedisBus(url=redis_url); await bus.connect()
    try:
        chain_id = await _seed_chain(db, "eth-d")
        async with _app_client(db, bus) as c:
            r = await c.post("/api/subscriptions", json={
                "name": "s", "chain_id": chain_id,
                "match_kind": "native_transfer",
            })
            sid = r.json()["id"]
            r = await c.post(f"/api/subscriptions/{sid}/payload_preview", json={
                "mapping": {"fields": [{"target": "kind", "source": "event.kind"}]},
            })
            assert r.status_code == 200
            assert r.json()["output"]["kind"] == "native_transfer"
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run — FAIL**

```bash
uv run pytest tests/integration/test_subscription_payload_mapping_api.py -v -k preview
```

- [ ] **Step 3: Implement**

```python
# apps/web/schemas.py
class PayloadPreviewIn(BaseModel):
    mapping: PayloadMapping
    sample: dict[str, Any] | None = None


class PayloadPreviewOut(BaseModel):
    output: dict[str, Any]
    warnings: list[str]
```

```python
# apps/web/routers/subscriptions.py
from apps.web.schemas import PayloadPreviewIn, PayloadPreviewOut
from core.notifier.payload_mapper import apply_mapping


@router.post("/{sub_id}/payload_preview", response_model=PayloadPreviewOut)
async def post_payload_preview(
    sub_id: str,
    payload: PayloadPreviewIn,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> PayloadPreviewOut:
    if payload.sample is not None:
        source = payload.sample
    else:
        # Reuse the sample-resolution logic.
        sample_response = await get_payload_sample(sub_id, refresh=0, session=session)
        source = sample_response.sample
    output, warnings = apply_mapping(source, payload.mapping.model_dump())
    return PayloadPreviewOut(output=output, warnings=warnings)
```

- [ ] **Step 4: Run**

```bash
uv run pytest tests/integration/test_subscription_payload_mapping_api.py -v -k preview
```

- [ ] **Step 5: Commit**

```bash
git add apps/web/schemas.py apps/web/routers/subscriptions.py tests/integration/test_subscription_payload_mapping_api.py
git commit -m "feat(api): POST /subscriptions/{id}/payload_preview"
```

---

### Task 2.5: `GET /delivery-records/{id}/downstream_preview`

**Files:**
- Modify: `apps/web/routers/delivery_records.py`
- Test: extend `tests/integration/test_delivery_records_router.py`

- [ ] **Step 1: Failing test**

Add to `tests/integration/test_delivery_records_router.py` (or create a new
`test_downstream_preview_router.py` alongside it; both are fine — the existing
file already covers this router):

```python
@pytest.mark.asyncio
async def test_downstream_preview_uses_current_mapping(
    db: Database, redis_url: str,
) -> None:
    from core.config.repositories import DeliveryRecordRepo, ChainRepo
    from core.config.models import ChainKind

    bus = RedisBus(url=redis_url); await bus.connect()
    try:
        async with db.session() as s:
            await ChainRepo(s).create(
                id="eth-e", kind=ChainKind.evm, rpc_http="http://x",
                rpc_ws=None, confirmations=0, poll_interval_ms=3000,
                enabled=True,
            )
            await s.commit()

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test",
        ) as c:
            r = await c.post("/api/subscriptions", json={
                "name": "s", "chain_id": "eth-e",
                "match_kind": "native_transfer",
                "payload_mapping": {"fields": [
                    {"target": "h", "source": "event.tx_hash"},
                ]},
            })
            assert r.status_code == 201, r.text
            sid = r.json()["id"]

            async with db.session() as s:
                row = await DeliveryRecordRepo(s).create(
                    subscription_id=sid, channel_id="ch", chain_id="eth-e",
                    event_payload={"event": {"tx_hash": "0xabc"}},
                    status="success",
                )
                await s.commit()
                did = row.id

            r = await c.get(f"/api/delivery-records/{did}/downstream_preview")
            assert r.status_code == 200
            body = r.json()
            assert body["output"] == {"h": "0xabc"}
            assert body["mapping_source"] == "current"
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run — FAIL (404)**

- [ ] **Step 3: Implement**

```python
# apps/web/routers/delivery_records.py
from apps.web.schemas import DownstreamPreviewOut
from core.config.repositories import SubscriptionRepo
from core.notifier.payload_mapper import apply_mapping


@router.get("/{delivery_id}/downstream_preview", response_model=DownstreamPreviewOut)
async def downstream_preview(
    delivery_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> DownstreamPreviewOut:
    repo = DeliveryRecordRepo(session)
    row = await repo.get(delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail="delivery not found")

    sub_row = await SubscriptionRepo(session).get(row.subscription_id)
    if sub_row is None or not sub_row.payload_mapping:
        # No mapping — downstream would receive source as-is.
        return DownstreamPreviewOut(
            output=row.event_payload, warnings=[], mapping_source="current",
        )
    output, warnings = apply_mapping(row.event_payload, sub_row.payload_mapping)
    return DownstreamPreviewOut(
        output=output, warnings=warnings, mapping_source="current",
    )
```

```python
# apps/web/schemas.py
class DownstreamPreviewOut(BaseModel):
    output: dict[str, Any]
    warnings: list[str]
    mapping_source: Literal["current"]
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/integration/test_delivery_records_router.py -v
```

- [ ] **Step 5: Commit**

```bash
git add apps/web/routers/delivery_records.py apps/web/schemas.py tests/integration/test_delivery_records_router.py
git commit -m "feat(api): GET /delivery-records/{id}/downstream_preview"
```

---

### Chunk 2 review

- [ ] **Dispatch spec-document-reviewer subagent** with the current plan section + spec. Fix issues and loop.

---

## Chunk 3: End-to-end integration test + regression snapshot

### Task 3.1: E2E dispatch + delivery_records semantics

**Files:**
- Create: `tests/integration/test_payload_mapping_e2e.py`

- [ ] **Step 1: Write the failing E2E test**

```python
# tests/integration/test_payload_mapping_e2e.py
from __future__ import annotations

import pytest

from core.config.snapshot import SnapshotChannel, SnapshotSubscription
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.event import Event


class _Recorder(Channel):
    type = "e2e_recorder"
    config_schema: dict = {}
    _sent: list[dict] = []

    async def start(self) -> None: pass
    async def stop(self) -> None: pass
    async def send(self, payload: dict) -> None:
        type(self)._sent.append(payload)


@pytest.mark.asyncio
async def test_end_to_end_mapping_and_persistence() -> None:
    _Recorder._sent.clear()
    persisted: list[dict] = []

    async def on_success(sub_id, ch_id, chain_id, payload, err, attempts):
        persisted.append(payload)

    n = Notifier(
        channel_factory=lambda cfg: _Recorder(config={}),
        on_success=on_success,
    )
    ch_cfg = SnapshotChannel(id="c", name="rec", type="e2e_recorder", config={})
    sub = SnapshotSubscription(
        id="s", name="s", chain_id="eth", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None, arg_filters={},
        enabled=True, channel_ids=["c"],
        payload_mapping={"fields": [
            {"target": "hash",   "source": "event.tx_hash"},
            {"target": "amount", "source": "event.args.value"},
            {"target": "brand",  "const":  "chain-indexer"},
        ]},
    )
    ev = Event(
        chain_id="eth", block_number=5, block_hash="0xb", block_timestamp=1,
        tx_hash="0xdeadbeef", tx_index=0, log_index=0,
        kind="native_transfer", contract="0xc", name=None,
        args={"value": "42"}, raw={},
    )
    await n.start([ch_cfg])
    await n.dispatch(ev, [(sub, [ch_cfg])])
    await n.stop()

    # Downstream saw the mapped shape only.
    assert len(_Recorder._sent) == 1
    sent = _Recorder._sent[0]
    assert sent["hash"] == "0xdeadbeef"
    assert sent["amount"] == "42"
    assert sent["brand"] == "chain-indexer"
    assert "event" not in sent  # strict mode
    # Metadata auto-appended.
    assert "delivery_id" in sent and "delivered_at" in sent

    # Persistence saw the source shape (not mapped).
    assert len(persisted) == 1
    assert persisted[0]["event"]["tx_hash"] == "0xdeadbeef"
    assert "hash" not in persisted[0]


@pytest.mark.asyncio
async def test_no_mapping_unchanged_from_pre_change_behavior() -> None:
    _Recorder._sent.clear()
    sub = SnapshotSubscription(
        id="s", name="s", chain_id="eth", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None, arg_filters={},
        enabled=True, channel_ids=["c"],
    )
    ch_cfg = SnapshotChannel(id="c", name="rec", type="e2e_recorder", config={})
    ev = Event(
        chain_id="eth", block_number=5, block_hash="0xb", block_timestamp=1,
        tx_hash="0xTX", tx_index=0, log_index=0,
        kind="native_transfer", contract="0xc", name=None,
        args={}, raw={},
    )
    n = Notifier(channel_factory=lambda cfg: _Recorder(config={}))
    await n.start([ch_cfg])
    await n.dispatch(ev, [(sub, [ch_cfg])])
    await n.stop()

    sent = _Recorder._sent[0]
    # Pre-change wire snapshot: full nested `event` block.
    assert sent["subscription_id"] == "s"
    assert sent["chain_id"] == "eth"
    assert sent["event"]["tx_hash"] == "0xTX"
```

- [ ] **Step 2: Run**

```bash
uv run pytest tests/integration/test_payload_mapping_e2e.py -v
```

Expected: passed. If not, fix regressions before continuing — do not skip.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_payload_mapping_e2e.py
git commit -m "test: e2e mapping — mapped delivery + source persistence + regression"
```

---

### Task 3.2: Full test + lint + typecheck sweep

- [ ] **Step 1: Run everything**

```bash
uv run ruff check core apps tests
uv run mypy core apps
uv run pytest tests/ -m "not e2e"
```

- [ ] **Step 2: If anything fails, fix and re-run before moving on.**

- [ ] **Step 3: Commit any fixes**

```bash
git add -A
git commit -m "chore: lint + type fixes for payload mapping"
```

---

## Chunk 4: Frontend — mapping editor in `SubForm`

### Task 4.1: Types + API client hooks

**Files:**
- Modify: `web/src/pages/Subscriptions.tsx`

- [ ] **Step 1: Extend the `Sub` interface**

Add `payload_mapping: PayloadMapping | null` to the top-of-file `Sub` interface. Add:

```ts
interface MappingField { target: string; source?: string | null; const?: unknown }
interface PayloadMapping { fields: MappingField[] }
interface PayloadSample { sample: Record<string, unknown>; origin: 'delivery' | 'synthetic' }
interface PreviewOut { output: Record<string, unknown>; warnings: string[] }
```

- [ ] **Step 2: Commit**

```bash
git add web/src/pages/Subscriptions.tsx
git commit -m "feat(web): mapping editor types"
```

---

### Task 4.2: Mapping editor component

**Files:**
- Modify: `web/src/pages/Subscriptions.tsx`

Add a new component `PayloadMappingEditor` inside the same file (kept co-located because it's tightly coupled to `SubForm` — matches the existing pattern where `TestSubModal` lives in the same file).

- [ ] **Step 1: Add the component skeleton**

Add `useRef` to the existing `import { useEffect, useMemo, useState } from 'react'` at the top of `web/src/pages/Subscriptions.tsx`:

```tsx
import { useEffect, useMemo, useRef, useState } from 'react'
```

Then define the component:

```tsx
function PayloadMappingEditor({
  subId, value, onChange,
}: {
  subId: string | null   // null = new subscription (no sample endpoint yet)
  value: PayloadMapping | null
  onChange: (v: PayloadMapping | null) => void
}) {
  const [open, setOpen] = useState<boolean>(value !== null)
  const [rows, setRows] = useState<MappingField[]>(value?.fields ?? [])
  const [sample, setSample] = useState<PayloadSample | null>(null)
  const [preview, setPreview] = useState<PreviewOut | null>(null)

  // Fetch sample (edit mode only; new subs get synthetic on server side once created).
  useEffect(() => {
    if (!open || !subId) return
    api.get<PayloadSample>(`/subscriptions/${subId}/payload_sample`)
      .then(setSample)
      .catch(() => setSample(null))
  }, [open, subId])

  const refresh = () => {
    if (!subId) return
    api.get<PayloadSample>(`/subscriptions/${subId}/payload_sample?refresh=1`)
      .then(setSample)
  }

  // Debounced preview.
  useEffect(() => {
    if (!open || !subId || rows.length === 0) { setPreview(null); return }
    const t = setTimeout(() => {
      api.post<PreviewOut>(`/subscriptions/${subId}/payload_preview`, {
        mapping: { fields: rows },
        sample: sample?.sample ?? null,
      }).then(setPreview).catch(() => setPreview(null))
    }, 300)
    return () => clearTimeout(t)
  }, [rows, sample, open, subId])

  // Propagate up — only when the mapping actually differs, so a mount with an
  // existing `value` doesn't mark the parent form dirty.
  const lastEmittedRef = useRef<PayloadMapping | null>(value ?? null)
  useEffect(() => {
    const next: PayloadMapping | null = rows.length === 0 ? null : { fields: rows }
    if (JSON.stringify(next) === JSON.stringify(lastEmittedRef.current)) return
    lastEmittedRef.current = next
    onChange(next)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rows])

  const addRow = (r: MappingField) => setRows(prev => [...prev, r])
  const updateRow = (i: number, patch: Partial<MappingField>) =>
    setRows(prev => prev.map((r, idx) => idx === i ? { ...r, ...patch } : r))
  const removeRow = (i: number) => setRows(prev => prev.filter((_, idx) => idx !== i))

  const flat = flattenLeaves(sample?.sample ?? {})

  const generateTemplate = () => {
    const seen = new Set<string>()
    const built: MappingField[] = []
    for (const { path } of flat) {
      let base = path.split('.').at(-1) ?? path
      let target = base
      let n = 2
      while (seen.has(target)) target = `${base}_${n++}`
      seen.add(target)
      built.push({ target, source: path })
    }
    setRows(built)
  }

  if (!open) {
    return (
      <div className="border rounded p-2 bg-gray-50">
        <button type="button" onClick={() => setOpen(true)}
          className="text-xs text-blue-600 hover:text-blue-800">
          + 配置投递数据映射
        </button>
        <span className="text-xs text-gray-400 ml-2">未配置时使用默认 payload 结构</span>
      </div>
    )
  }

  return (
    <div className="border rounded p-3 space-y-3 bg-slate-50">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium">投递数据映射</span>
        <div className="flex gap-2">
          <button type="button" onClick={generateTemplate}
            className="text-xs px-2 py-1 border rounded" disabled={flat.length === 0}>
            从源字段一键生成
          </button>
          <button type="button" onClick={refresh}
            className="text-xs px-2 py-1 border rounded" disabled={!subId}>刷新样本</button>
          <button type="button" onClick={() => setRows([])}
            className="text-xs px-2 py-1 border rounded text-red-600">清空</button>
          <button type="button" onClick={() => { setRows([]); setOpen(false) }}
            className="text-xs px-2 py-1 border rounded text-gray-500">折叠</button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="border rounded bg-white p-2 min-h-32 max-h-56 overflow-auto">
          <p className="text-[10px] text-gray-500 mb-1">
            源 payload {sample ? `(${sample.origin === 'delivery' ? '来自最近一次真实推送' : '合成占位样本'})` : ''}
          </p>
          {flat.length === 0 && <p className="text-[10px] text-gray-400">
            {subId ? '样本加载中...' : '保存订阅后可查看源样本'}
          </p>}
          {flat.map(({ path, value: v }) => (
            <div key={path} className="text-[11px] font-mono flex items-start gap-1 py-0.5">
              <button type="button" onClick={() => addRow({ target: path.split('.').at(-1) ?? path, source: path })}
                className="text-blue-500 hover:text-blue-700 shrink-0" title="→ 添加到映射">→</button>
              <span className="text-gray-700">{path}</span>
              <span className="text-gray-400 truncate">{JSON.stringify(v)}</span>
            </div>
          ))}
        </div>

        <div className="border rounded bg-white p-2 min-h-32 max-h-56 overflow-auto">
          <p className="text-[10px] text-gray-500 mb-1">目标 payload (预览)</p>
          {preview?.warnings.length ? (
            <ul className="text-[10px] text-amber-600 mb-1">
              {preview.warnings.map((w, i) => <li key={i}>⚠ {w}</li>)}
            </ul>
          ) : null}
          <pre className="text-[11px] font-mono whitespace-pre-wrap">
            {preview ? JSON.stringify(preview.output, null, 2) : '(添加映射后显示预览)'}
          </pre>
        </div>
      </div>

      <div className="space-y-1">
        {rows.map((r, i) => {
          const mode: 'source' | 'const' = r.source != null ? 'source' : 'const'
          return (
            <div key={i} className="flex gap-1 items-center">
              <input value={r.target}
                onChange={e => updateRow(i, { target: e.target.value })}
                placeholder="target"
                className="border rounded px-2 py-1 text-xs font-mono w-40" />
              <select value={mode}
                onChange={e => updateRow(i, e.target.value === 'source'
                  ? { source: '', const: undefined }
                  : { source: undefined, const: '' })}
                className="border rounded px-1 py-1 text-xs">
                <option value="source">源</option>
                <option value="const">常量</option>
              </select>
              {mode === 'source' ? (
                <input value={r.source ?? ''}
                  onChange={e => updateRow(i, { source: e.target.value })}
                  placeholder="event.tx_hash"
                  className="border rounded px-2 py-1 text-xs font-mono flex-1" />
              ) : (
                <input value={typeof r.const === 'string' ? r.const : JSON.stringify(r.const)}
                  onChange={e => {
                    const raw = e.target.value
                    try { updateRow(i, { const: JSON.parse(raw) }) }
                    catch { updateRow(i, { const: raw }) }
                  }}
                  placeholder='"value" or 42 or {"k":"v"}'
                  className="border rounded px-2 py-1 text-xs font-mono flex-1" />
              )}
              <button type="button" onClick={() => removeRow(i)}
                className="text-red-500 text-xs px-1">×</button>
            </div>
          )
        })}
        <button type="button" onClick={() => addRow({ target: '', source: '' })}
          className="text-xs text-blue-600">+ 添加一行</button>
      </div>
    </div>
  )
}

function flattenLeaves(
  obj: unknown, prefix = '',
): { path: string; value: unknown }[] {
  if (obj === null || typeof obj !== 'object' || Array.isArray(obj)) {
    return prefix ? [{ path: prefix, value: obj }] : []
  }
  const out: { path: string; value: unknown }[] = []
  for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
    const p = prefix ? `${prefix}.${k}` : k
    out.push(...flattenLeaves(v, p))
  }
  return out
}
```

- [ ] **Step 2: Wire it into `SubForm`**

Inside `SubForm`, add state:

```tsx
const [payloadMapping, setPayloadMapping] = useState<PayloadMapping | null>(
  initial?.payload_mapping ?? null
)
```

In the JSX, right after the channels binding block, add:

```tsx
<PayloadMappingEditor
  subId={initial?.id ?? null}
  value={payloadMapping}
  onChange={setPayloadMapping}
/>
```

In `submit`, include the mapping in the base body:

```tsx
    const base = {
      // ... existing fields ...
      business_name: (fd.get('business_name') as string | null) || null,
      payload_mapping: payloadMapping,  // ← add
    }
```

- [ ] **Step 3: Type-check + run the frontend**

```bash
cd web && npm run build
```

Expected: no TypeScript errors, build succeeds.

- [ ] **Step 4: Manual smoke test (optional but recommended)**

```bash
uv run uvicorn apps.web.main:create_app --factory --reload --port 8000 &
cd web && npm run dev
```

Open http://localhost:5173, edit a subscription:
- Expand "投递数据映射" — verify sample panel populates.
- Click "从源字段一键生成" — verify the rows populate.
- Click "→" next to a source row — verify a new row is added.
- Toggle a row to "常量" and type `"my-source"` — verify preview updates.
- Save — verify the value round-trips (reload, re-open).

- [ ] **Step 5: Commit**

```bash
git add web/src/pages/Subscriptions.tsx
git commit -m "feat(web): payload mapping editor in SubForm"
```

---

### Task 4.3: Delivery record downstream preview

**Files:**
- Modify: `web/src/pages/DeliveryRecords.tsx`

- [ ] **Step 1: Add the collapsible "downstream" block**

Inside the expanded record body, next to the "事件负载" block, add:

```tsx
<DownstreamPreview id={item.id} />
```

At the bottom of the file, add:

```tsx
function DownstreamPreview({ id }: { id: string }) {
  const [open, setOpen] = useState(false)
  const { data, isLoading } = useQuery<{
    output: Record<string, unknown>; warnings: string[]; mapping_source: string
  }>({
    queryKey: ['downstream-preview', id],
    queryFn: () => api.get(`/delivery-records/${id}/downstream_preview`),
    enabled: open,
  })
  return (
    <div className="bg-white rounded p-2 text-xs">
      <button type="button" onClick={() => setOpen(o => !o)}
        className="text-gray-600 hover:text-gray-800">
        {open ? '▾' : '▸'} 预览下游实际 payload
      </button>
      {open && (
        <div className="mt-2">
          {isLoading && <p className="text-gray-400 text-[11px]">加载中...</p>}
          {data?.warnings.length ? (
            <ul className="text-[10px] text-amber-600 mb-1">
              {data.warnings.map((w, i) => <li key={i}>⚠ {w}</li>)}
            </ul>
          ) : null}
          {data && (
            <pre className="overflow-auto max-h-40 text-[11px] font-mono">
              {JSON.stringify(data.output, null, 2)}
            </pre>
          )}
        </div>
      )}
    </div>
  )
}
```

- [ ] **Step 2: Build**

```bash
cd web && npm run build
```

- [ ] **Step 3: Commit**

```bash
git add web/src/pages/DeliveryRecords.tsx
git commit -m "feat(web): delivery record downstream payload preview"
```

---

## Chunk 5: Docs + release note

### Task 5.1: Update README + spec deliverables checklist

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-07-07-subscription-payload-mapping-design.md`

- [ ] **Step 1: Add a "Payload mapping" section to README**

Under a suitable section (feature list), add a short paragraph:

```markdown
### Payload mapping (subscription)

Configure per-subscription field mapping to reshape the delivery payload
(rename fields, nest targets, inject constants). Leave a subscription's mapping
empty to keep the default full payload. See the mapping editor in the
subscription form.

Delivery records store the **source** (un-mapped) payload; use the "预览下游实际
payload" button on a delivery row to see what the downstream actually received.
```

- [ ] **Step 2: Tick off deliverables in the spec file** (checkboxes in §"Deliverables checklist")

- [ ] **Step 3: Commit**

```bash
git add README.md docs/superpowers/specs/2026-07-07-subscription-payload-mapping-design.md
git commit -m "docs: subscription payload mapping — README + spec checklist"
```

---

### Task 5.2: Final full test run

- [ ] **Step 1: Run everything**

```bash
uv run ruff check core apps tests
uv run mypy core apps
uv run pytest tests/ -m "not e2e"
cd web && npm run build
```

- [ ] **Step 2: If green, we're done.**

- [ ] **Step 3: Verify no untracked files**

```bash
git status
```

Expected: clean working tree.

---

## Notes for the executing agent

- **Every task ends with a commit.** Do not batch commits across tasks — one focused commit per task keeps `git log` legible and makes bisecting future regressions cheap.
- **When a test unexpectedly passes on Step 2, don't skip Step 3.** Investigate why. Common cause: the test asserts something the code already guarantees for an unrelated reason. Adjust the test to actually pin the new behavior, or delete it as redundant. Do not proceed with a "green from step 1" test — it's blind.
- **Do not silently widen scope.** If a task exposes a gap the spec didn't cover, note it and ask; don't implement drive-by.
- **Ruff / mypy are gates.** Every commit must pass both. `mypy` is strict; use `# type: ignore[...]` sparingly and only with a specific error code.

Follow the file order above — later chunks depend on earlier ones (mapper → API → frontend → docs).