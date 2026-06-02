# Subscription `business_name` Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `business_name` string to each subscription and surface it as a top-level field on the notification envelope, so downstream consumers can identify the owning business per delivery.

**Architecture:** Plumb a single nullable string through the existing layered path: ORM column → snapshot dataclass → envelope builder → API schema/routers → React form & list. No new modules, no separate entity, no filtering. Empty/whitespace strings are normalized to NULL at the API boundary; the envelope omits the key when the value is falsy (same precedent as the existing `replay` flag).

**Tech Stack:** SQLAlchemy 2.0 (async) + Alembic, FastAPI + Pydantic v2, structlog, React 19 + Vite + Tailwind, pytest + pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-06-02-subscription-business-name-design.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `core/config/models.py` | Modify | Add `business_name` column on `Subscription` |
| `migrations/versions/0010_subscription_business_name.py` | Create | Alembic upgrade/downgrade via `op.batch_alter_table` |
| `core/config/repositories.py` | Modify | Add `business_name` kwarg to `SubscriptionRepo.create` |
| `core/config/snapshot.py` | Modify | Add field to `SnapshotSubscription`; populate in `load_snapshot` |
| `core/notifier/payload.py` | Modify | Insert `business_name` into envelope between `subscription_name` and `chain_id` when set |
| `apps/web/schemas.py` | Modify | Add field on `SubscriptionCreate`/`SubscriptionOut` + normalizer validator |
| `apps/web/routers/subscriptions.py` | Modify | Thread the kwarg through create/update; include in replay-request inline subscription dict |
| `tests/unit/test_payload.py` | Modify | Cover present / absent / position cases |
| `tests/unit/test_snapshot.py` | Modify | Cover default value on `SnapshotSubscription` |
| `tests/unit/test_web_subscriptions.py` | Modify | Cover POST/GET roundtrip, whitespace normalization, length, PUT-to-null |
| `tests/integration/test_subscription_replay_api.py` | Modify | Assert `business_name` appears in the published `replay_request.subscription` dict |
| `web/src/pages/Subscriptions.tsx` | Modify | Extend `interface Sub`; list column; form input |

Each task ends in a green test run and a commit. Tasks are ordered so that earlier tasks don't break later tests — schema/snapshot first, payload + API next, UI last.

---

## Chunk 1: Backend plumbing (DB → snapshot → payload)

### Task 1: DB column + Alembic migration

**Files:**
- Modify: `core/config/models.py:103-120` (Subscription class)
- Create: `migrations/versions/0010_subscription_business_name.py`

- [ ] **Step 1: Add the column to the ORM model**

Edit `core/config/models.py` — append one line at the end of the `Subscription` class body (after the existing `last_processed_block` line near line 120):

```python
class Subscription(Base, TimestampMixin):
    __tablename__ = "subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    # ...existing columns unchanged...
    last_processed_block: Mapped[int | None] = mapped_column(BigInteger, nullable=True, default=None)
    business_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
```

No index, no `server_default`. Existing rows become NULL on upgrade.

- [ ] **Step 2: Create the Alembic migration**

Write `migrations/versions/0010_subscription_business_name.py`:

```python
"""add business_name to subscriptions

Revision ID: 0010
Revises: 0009
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.add_column(sa.Column("business_name", sa.String(length=255), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.drop_column("business_name")
```

Pattern matches `migrations/versions/0004_subscription_start_block.py`. Bare `op.add_column` / `op.drop_column` are NOT SQLite-safe — `batch_alter_table` is mandatory.

- [ ] **Step 3: Verify migration applies cleanly against a fresh SQLite DB**

Run:

```bash
rm -f /tmp/ci_chain_indexer.db
CHAIN_INDEXER_DATABASE__URL=sqlite+aiosqlite:////tmp/ci_chain_indexer.db \
  uv run alembic upgrade head
```

Expected: ends with `Running upgrade 0009 -> 0010, add business_name to subscriptions`.

Verify the column exists:

```bash
sqlite3 /tmp/ci_chain_indexer.db "PRAGMA table_info(subscriptions);" | grep business_name
```

Expected: a line containing `business_name|VARCHAR(255)|0||0` (column present, nullable, no default).

- [ ] **Step 4: Verify downgrade is symmetric**

```bash
CHAIN_INDEXER_DATABASE__URL=sqlite+aiosqlite:////tmp/ci_chain_indexer.db \
  uv run alembic downgrade -1
sqlite3 /tmp/ci_chain_indexer.db "PRAGMA table_info(subscriptions);" | grep business_name || echo "OK: column removed"
CHAIN_INDEXER_DATABASE__URL=sqlite+aiosqlite:////tmp/ci_chain_indexer.db \
  uv run alembic upgrade head
```

Expected: `OK: column removed` after downgrade, then upgrade re-adds it cleanly.

- [ ] **Step 5: Run mypy + ruff on the model file**

```bash
uv run mypy core/config/models.py
uv run ruff check core/config/models.py migrations/versions/0010_subscription_business_name.py
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add core/config/models.py migrations/versions/0010_subscription_business_name.py
git commit -m "feat(db): add business_name column to subscriptions"
```

---

### Task 2: Repository `create` kwarg

**Files:**
- Modify: `core/config/repositories.py:113-133` (`SubscriptionRepo.create`)

`SubscriptionRepo.update` already accepts `**fields`, so no change is needed there — the router will pass `business_name=...` in. Only `create` needs a new parameter.

- [ ] **Step 1: Add the kwarg**

Edit `SubscriptionRepo.create` to take a new optional parameter and pass it to the model constructor:

```python
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
    start_block: int | None = None,
    business_name: str | None = None,
) -> Subscription:
    sub = Subscription(
        name=name, chain_id=chain_id, address=address, abi_id=abi_id,
        match_kind=match_kind, match_name=match_name, arg_filters=arg_filters,
        enabled=enabled, start_block=start_block,
        business_name=business_name,
    )
    self.s.add(sub)
    await self.s.flush()
    return sub
```

The default `None` keeps every existing call site working unchanged.

- [ ] **Step 2: Type-check**

```bash
uv run mypy core/config/repositories.py
```

Expected: no errors.

- [ ] **Step 3: Run the integration tests that exercise `SubscriptionRepo.create`**

```bash
uv run pytest tests/integration/test_repositories.py tests/integration/test_subscription_replay_api.py -v
```

Expected: same pass count as before this change (the new kwarg is defaulted, no behavior change).

- [ ] **Step 4: Commit**

```bash
git add core/config/repositories.py
git commit -m "feat(db): SubscriptionRepo.create accepts business_name kwarg"
```

---

### Task 3: Snapshot dataclass

**Files:**
- Modify: `core/config/snapshot.py:16-28` (`SnapshotSubscription`), `:130-144` (`load_snapshot`)
- Test: `tests/unit/test_snapshot.py`

- [ ] **Step 1: Write failing test for the dataclass default**

Append to `tests/unit/test_snapshot.py`:

```python
def test_subscription_snapshot_business_name_defaults_to_none() -> None:
    s = _sub()
    assert s.business_name is None


def test_subscription_snapshot_business_name_carried_through() -> None:
    s = _sub(business_name="trading-team")
    assert s.business_name == "trading-team"
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
uv run pytest tests/unit/test_snapshot.py::test_subscription_snapshot_business_name_defaults_to_none -v
```

Expected: FAIL — `TypeError: SnapshotSubscription.__init__() got an unexpected keyword argument 'business_name'` or `AttributeError`.

- [ ] **Step 3: Add the field to `SnapshotSubscription`**

Edit `core/config/snapshot.py`. Append `business_name` AFTER `start_block` so all defaulted fields stay contiguous:

```python
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
    start_block: int | None = None
    business_name: str | None = None
```

- [ ] **Step 4: Wire `load_snapshot` to populate it**

In `core/config/snapshot.py::load_snapshot`, find the `SnapshotSubscription(...)` constructor call near the bottom of the for-loop (around line 130) and add the new kwarg:

```python
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
        start_block=sub.start_block,
        business_name=sub.business_name,
    )
)
```

- [ ] **Step 5: Run snapshot tests, verify they pass**

```bash
uv run pytest tests/unit/test_snapshot.py -v
```

Expected: all snapshot tests PASS, including the two new ones.

- [ ] **Step 6: Type-check**

```bash
uv run mypy core/config/snapshot.py
```

Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add core/config/snapshot.py tests/unit/test_snapshot.py
git commit -m "feat(snapshot): carry business_name on SnapshotSubscription"
```

---

### Task 4: Envelope emission in `build_payload`

**Files:**
- Modify: `core/notifier/payload.py:31-55`
- Test: `tests/unit/test_payload.py`

The current test in `tests/unit/test_payload.py::test_payload_shape_matches_spec_section_8` asserts an exact dict equality and uses a `_sub()` helper that does not set `business_name`. After Task 3 the field defaults to `None`, so the helper unchanged will produce `business_name=None`, and we want the envelope to omit the key — the existing assertion stays valid.

- [ ] **Step 1: Write failing tests for present / absent / position**

Append to `tests/unit/test_payload.py`:

```python
def test_payload_includes_business_name_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("core.notifier.payload._now_unix", lambda: 1735689601)
    monkeypatch.setattr("core.notifier.payload._gen_id", lambda: "delivery-uuid")

    sub = SnapshotSubscription(
        id=_sub().id,
        name=_sub().name,
        chain_id=_sub().chain_id,
        address=_sub().address,
        abi_id=_sub().abi_id,
        match_kind=_sub().match_kind,
        match_name=_sub().match_name,
        arg_filters=_sub().arg_filters,
        enabled=True,
        channel_ids=_sub().channel_ids,
        business_name="trading-team",
    )
    p = build_payload(event=_event(), subscription=sub)
    assert p["business_name"] == "trading-team"


def test_payload_omits_business_name_when_none() -> None:
    p = build_payload(event=_event(), subscription=_sub())
    assert "business_name" not in p


def test_payload_business_name_key_position() -> None:
    # CPython dicts preserve insertion order; assert key sequence is
    # subscription_id, subscription_name, business_name, chain_id, ...
    sub = SnapshotSubscription(
        id=_sub().id, name=_sub().name, chain_id=_sub().chain_id,
        address=_sub().address, abi_id=_sub().abi_id,
        match_kind=_sub().match_kind, match_name=_sub().match_name,
        arg_filters=_sub().arg_filters, enabled=True,
        channel_ids=_sub().channel_ids,
        business_name="ops",
    )
    keys = list(build_payload(event=_event(), subscription=sub).keys())
    assert keys[:4] == ["subscription_id", "subscription_name", "business_name", "chain_id"]
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
uv run pytest tests/unit/test_payload.py -v
```

Expected: the three new tests FAIL with `KeyError`/`AssertionError` (`business_name` not in payload). Existing tests still pass.

- [ ] **Step 3: Rewrite `build_payload` to build the dict incrementally**

Replace `core/notifier/payload.py::build_payload` with:

```python
def build_payload(
    *, event: Event, subscription: SnapshotSubscription, replay: bool = False
) -> dict[str, Any]:
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
```

Why incremental construction rather than a single dict literal: inserting `business_name` conditionally inside a literal would either require `**({"business_name": ...} if x else {})` (ugly) or accept the key at the end of the dict (wrong position). Incremental is the cleanest way to keep both the conditional and the key order correct.

- [ ] **Step 4: Run tests, verify they pass**

```bash
uv run pytest tests/unit/test_payload.py -v
```

Expected: ALL tests PASS, including the original `test_payload_shape_matches_spec_section_8` (since the default `business_name=None` keeps it omitted).

- [ ] **Step 5: Sanity-check the broader notifier surface**

```bash
uv run pytest tests/unit/test_notifier.py -v
```

Expected: no regression — `_sub()` fixtures in those files either set `business_name=None` (via default) or are unaffected.

- [ ] **Step 6: Type-check**

```bash
uv run mypy core/notifier/payload.py
```

Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add core/notifier/payload.py tests/unit/test_payload.py
git commit -m "feat(notifier): emit business_name in envelope when set"
```

---

## Chunk 2: API surface

### Task 5: Pydantic schemas + normalizer

**Files:**
- Modify: `apps/web/schemas.py:96-133` (`SubscriptionCreate`, `SubscriptionOut`)
- Test: `tests/unit/test_web_subscriptions.py`

The normalizer ensures whitespace-only input never reaches the DB / envelope as a present-but-blank label.

- [ ] **Step 1: Add the field + validator on `SubscriptionCreate` and the field on `SubscriptionOut`**

Edit `apps/web/schemas.py`. In `SubscriptionCreate`, add `business_name` right after `name`, and add a normalizer validator alongside the existing `_check_operator_grammar`:

```python
class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    business_name: str | None = Field(default=None, max_length=255)
    chain_id: str
    address: str | None = None
    abi_id: str | None = None
    match_kind: Literal["native_transfer", "token_transfer", "event", "call"]
    match_name: str | None = None
    arg_filters: dict[str, ArgFilterValue] = Field(default_factory=dict)
    start_block: int | None = None
    enabled: bool = True

    @field_validator("business_name", mode="before")
    @classmethod
    def _normalize_business_name(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        return v or None

    @field_validator("arg_filters")
    @classmethod
    def _check_operator_grammar(
        cls, v: dict[str, ArgFilterValue],
    ) -> dict[str, ArgFilterValue]:
        try:
            _validate_filter_keys(v)
        except FilterError as exc:
            raise ValueError(str(exc)) from exc
        return v
```

In `SubscriptionOut`, add `business_name` right after `name`:

```python
class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    business_name: str | None
    chain_id: str
    # ...rest unchanged...
```

`SubscriptionDetail` inherits from `SubscriptionOut` and picks the field up automatically.

- [ ] **Step 2: Verify schema unit-level sanity**

```bash
uv run python -c "
from apps.web.schemas import SubscriptionCreate
print(SubscriptionCreate(name='x', chain_id='eth', match_kind='native_transfer').business_name)
print(SubscriptionCreate(name='x', chain_id='eth', match_kind='native_transfer', business_name='').business_name)
print(SubscriptionCreate(name='x', chain_id='eth', match_kind='native_transfer', business_name='   ').business_name)
print(SubscriptionCreate(name='x', chain_id='eth', match_kind='native_transfer', business_name='  ops  ').business_name)
"
```

Expected output (one per line): `None`, `None`, `None`, `ops`.

- [ ] **Step 3: Type-check**

```bash
uv run mypy apps/web/schemas.py
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add apps/web/schemas.py
git commit -m "feat(api): add business_name to subscription schemas with normalizer"
```

---

### Task 6: Wire create + update endpoints

**Files:**
- Modify: `apps/web/routers/subscriptions.py:32-79` (`create_subscription`, `update_subscription`)
- Test: `tests/unit/test_web_subscriptions.py`

- [ ] **Step 1: Write failing tests for the CRUD roundtrip + normalization + update + length**

Append to `tests/unit/test_web_subscriptions.py`:

```python
def test_subscription_business_name_roundtrip(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "trading-team",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 201, r.text
        sub_id = r.json()["id"]
        assert r.json()["business_name"] == "trading-team"

        d = c.get(f"/api/subscriptions/{sub_id}").json()
        assert d["business_name"] == "trading-team"


def test_subscription_business_name_defaults_to_null(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 201
        assert r.json()["business_name"] is None


def test_subscription_business_name_whitespace_normalized_to_null(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "   ",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 201
        assert r.json()["business_name"] is None


def test_subscription_business_name_put_updates_and_clears(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        sub_id = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "old",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        }).json()["id"]

        # Update to a new value
        r = c.put(f"/api/subscriptions/{sub_id}", json={
            "name": "w", "business_name": "new",
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 200
        assert r.json()["business_name"] == "new"

        # Update to null (clear)
        r = c.put(f"/api/subscriptions/{sub_id}", json={
            "name": "w", "business_name": None,
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 200
        assert r.json()["business_name"] is None


def test_subscription_business_name_over_length_422(db: Database) -> None:
    bus = _FakeBus()
    with _client(db, bus) as c:
        chain_id, _ = _seed_chain_and_channel(c)
        r = c.post("/api/subscriptions", json={
            "name": "w", "business_name": "x" * 256,
            "chain_id": chain_id, "address": None, "abi_id": None,
            "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True,
        })
        assert r.status_code == 422
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
uv run pytest tests/unit/test_web_subscriptions.py -v -k business_name
```

Expected: all five new tests FAIL — the router does not pass `business_name` to the repo yet, so the column is never written and `SubscriptionOut` returns `None`/422 paths are wrong.

- [ ] **Step 3: Thread the kwarg through `create_subscription` and `update_subscription`**

Edit `apps/web/routers/subscriptions.py`. In `create_subscription` (around line 40), add `business_name=payload.business_name` to the `SubscriptionRepo.create` call:

```python
sub = await SubscriptionRepo(session).create(
    name=payload.name,
    chain_id=payload.chain_id,
    address=payload.address,
    abi_id=payload.abi_id,
    match_kind=MatchKind(payload.match_kind),
    match_name=payload.match_name,
    arg_filters=payload.arg_filters,
    enabled=payload.enabled,
    start_block=payload.start_block,
    business_name=payload.business_name,
)
```

In `update_subscription` (around line 66), add `business_name=payload.business_name` to the `.update(...)` call. Since `repo.update` is `**fields`, passing `business_name=None` will explicitly set the column to NULL — exactly what we want for "clear the label":

```python
await repo.update(
    sub_id,
    name=payload.name,
    address=payload.address,
    abi_id=payload.abi_id,
    match_kind=MatchKind(payload.match_kind),
    match_name=payload.match_name,
    arg_filters=payload.arg_filters,
    enabled=payload.enabled,
    start_block=payload.start_block,
    business_name=payload.business_name,
)
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
uv run pytest tests/unit/test_web_subscriptions.py -v
```

Expected: all subscription tests PASS, including the five new `business_name` tests and the original ones (which don't supply `business_name`, so it defaults to `None` and roundtrips cleanly).

- [ ] **Step 5: Type-check**

```bash
uv run mypy apps/web/routers/subscriptions.py apps/web/schemas.py
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add tests/unit/test_web_subscriptions.py apps/web/routers/subscriptions.py
git commit -m "feat(api): create/update subscription with business_name"
```

---

### Task 7: Wire replay endpoint

**Files:**
- Modify: `apps/web/routers/subscriptions.py:161-208` (`replay_subscription`)
- Test: `tests/integration/test_subscription_replay_api.py`

`replay_subscription` publishes a self-contained `replay_request` bus message whose `subscription` sub-dict is later used by the worker to reconstruct a `SnapshotSubscription`. If `business_name` is missing from that dict, replayed deliveries silently lose the label even though the envelope shape supports it.

- [ ] **Step 1: Write failing integration test extending the existing replay assertions**

Edit `tests/integration/test_subscription_replay_api.py`. In `_seed`, pass `business_name="trading-team"` when creating the subscription:

```python
async def _seed(db: Database) -> str:
    async with db.session() as s:
        await ChainRepo(s).create(
            id="eth", kind=ChainKind.evm, rpc_http="x", rpc_ws=None,
            confirmations=1, poll_interval_ms=1000, enabled=True,
        )
        sub = await SubscriptionRepo(s).create(
            name="t", chain_id="eth", address=None, abi_id=None,
            match_kind=MatchKind.native_transfer, match_name=None,
            arg_filters={}, enabled=True, start_block=None,
            business_name="trading-team",
        )
        await s.commit()
        return sub.id
```

In `test_replay_publishes_request`, add one assertion at the end of the existing `assert msg["from_block"] == 100 and msg["to_block"] == 200` block:

```python
assert msg["subscription"]["business_name"] == "trading-team"
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
uv run pytest tests/integration/test_subscription_replay_api.py::test_replay_publishes_request -v
```

Expected: FAIL — `KeyError: 'business_name'` because the inline dict in `replay_subscription` doesn't include it yet.

- [ ] **Step 3: Add `business_name` to the inline `subscription` dict**

Edit `apps/web/routers/subscriptions.py::replay_subscription` (around line 196). Add the new key inside the inline `"subscription"` dict:

```python
await bus.publish("replay_request", {
    "request_id": request_id,
    "chain_id": sub.chain_id,
    "subscription": {
        "id": sub.id, "name": sub.name, "chain_id": sub.chain_id,
        "address": sub.address, "abi_id": sub.abi_id,
        "match_kind": sub.match_kind.value, "match_name": sub.match_name,
        "arg_filters": sub.arg_filters, "enabled": True,
        "channel_ids": channel_ids, "start_block": None,
        "business_name": sub.business_name,
    },
    "channels": channels,
    "from_block": payload.from_block,
    "to_block": payload.to_block,
})
```

- [ ] **Step 4: Verify the worker side reconstructs the SnapshotSubscription correctly**

The worker's `ReplayWatcher` calls `SnapshotSubscription(**msg["subscription"])` somewhere — confirm by grep:

```bash
grep -rn "SnapshotSubscription(" apps/worker core/ | head
```

If a call site reconstructs from `msg["subscription"]` (likely in `apps/worker/main.py` or `apps/worker/chain_runner.py`), confirm it splats `**` so the new key is accepted automatically. If it picks fields explicitly, add `business_name=msg["subscription"].get("business_name")` to that call too. **Note:** if the call site already uses `**msg["subscription"]`, no change is needed — `SnapshotSubscription` accepts the new field after Task 3.

- [ ] **Step 5: Run the integration test, verify it passes**

```bash
uv run pytest tests/integration/test_subscription_replay_api.py -v
```

Expected: all replay tests PASS, including the augmented `test_replay_publishes_request`.

Also run the chain-runner replay unit test in case the worker reconstruction path is exercised there:

```bash
uv run pytest tests/unit/test_chain_runner_replay.py -v
```

Expected: PASS, possibly with no code changes if the test passes the dict via `**`.

- [ ] **Step 6: Type-check**

```bash
uv run mypy apps/web/routers/subscriptions.py apps/worker
```

Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add apps/web/routers/subscriptions.py tests/integration/test_subscription_replay_api.py
# Plus any worker file touched in Step 4
git commit -m "feat(api): include business_name in replay_request subscription"
```

---

## Chunk 3: UI

### Task 8: Subscriptions page — type, list column, form input

**Files:**
- Modify: `web/src/pages/Subscriptions.tsx` (line 7 — `interface Sub`; lines 39-77 — table; lines 200-220 — form)

Note: there are no frontend tests in this project (verified via `Glob`/`Grep`), so this task is verified by build + browser smoke. Don't introduce a new test layer.

- [ ] **Step 1: Extend the inline `interface Sub` type**

Edit `web/src/pages/Subscriptions.tsx` line 7. Add `business_name: string | null` between `name` and `chain_id` to mirror the API ordering:

```ts
interface Sub { id: string; name: string; business_name: string | null; chain_id: string; match_kind: string; match_name: string | null; address: string | null; abi_id: string | null; enabled: boolean; arg_filters: Record<string, unknown>; start_block: number | null; last_processed_block: number | null }
```

- [ ] **Step 2: Add a list column for "业务名称"**

In the `<thead>` (line ~41), insert a new `<th>` between "名称" and "链":

```tsx
<th className="py-2 px-2">名称</th>
<th className="py-2 px-2">业务</th>
<th className="py-2 px-2">链</th>
```

In the `<tbody>` row (line ~55), insert the matching `<td>` between the name `<td>` and the chain `<td>`:

```tsx
<td className="py-2 px-2 text-xs text-gray-700">{s.business_name ?? '—'}</td>
```

- [ ] **Step 3: Add a form input for `business_name`**

The form lives in the `SubForm` component further down the file. Locate it by searching for `function SubForm` (the `submit` handler inside it is anchored by `const base = {`). Right after the `name` input near the top of the form's JSX, add:

```tsx
<input name="business_name" defaultValue={initial?.business_name ?? ''} maxLength={255} placeholder="业务名称（可选，用于下游识别业务）" className="w-full border rounded px-3 py-1.5 text-sm" />
```

In the `submit` handler (find via `const base = {`), add `business_name` to the `base` payload:

```ts
const base = {
  chain_id: fd.get('chain_id'),
  address: fd.get('address') || null,
  abi_id: abiId || null,
  match_kind: matchKind,
  arg_filters: af,
  start_block: fd.get('start_block') ? Number(fd.get('start_block')) : null,
  enabled: fd.get('enabled') === 'on',
  business_name: (fd.get('business_name') as string | null) || null,
}
```

Treating empty string as `null` here matches the backend normalizer; either would work.

- [ ] **Step 4: Build the frontend, verify no TypeScript errors**

```bash
cd web && npm run build
```

Expected: build succeeds. No "Property 'business_name' does not exist on type 'Sub'" errors.

- [ ] **Step 5: Smoke test in browser**

Start backend + frontend:

```bash
# Terminal 1 — worker (optional for this smoke)
uv run python -m apps.worker.main &

# Terminal 2 — API
uv run uvicorn apps.web.main:create_app --factory --reload --port 8000 &

# Terminal 3 — frontend dev server
cd web && npm run dev
```

In the browser (http://localhost:5173):

1. Open the "订阅规则" page.
2. Create a subscription with `业务名称 = trading-team`. Save.
3. Confirm the new subscription appears in the list with `trading-team` in the "业务" column.
4. Create a second subscription leaving `业务名称` blank. Confirm `—` is rendered in the "业务" column.
5. Edit the first subscription, clear the business name, save. Confirm the list now shows `—`.

If any step doesn't work as expected, debug before committing.

- [ ] **Step 6: Commit**

```bash
git add web/src/pages/Subscriptions.tsx
git commit -m "feat(ui): show + edit business_name on subscriptions"
```

---

## Final verification

After all tasks, run the full test gate and lint:

- [ ] **Step 1: Run the unit + integration suites**

```bash
uv run pytest tests/ -m "not e2e" -v
```

Expected: 100% pass. No regressions in existing tests.

- [ ] **Step 2: Lint + type-check**

```bash
uv run ruff check core apps tests
uv run mypy core apps
```

Expected: no errors.

- [ ] **Step 3: Verify the envelope shape end-to-end via a fixture-driven payload**

```bash
uv run python -c "
from core.config.snapshot import SnapshotSubscription
from core.notifier.payload import build_payload
from core.parser.event import Event

sub = SnapshotSubscription(
    id='s1', name='wallet1', chain_id='eth-mainnet',
    address='0xabc', abi_id=None, match_kind='native_transfer',
    match_name=None, arg_filters={}, enabled=True, channel_ids=['c1'],
    business_name='trading-team',
)
event = Event(
    chain_id='eth-mainnet', block_number=1, block_hash='0x', block_timestamp=0,
    tx_hash='0x', tx_index=0, log_index=0, kind='native_transfer',
    contract=None, name=None, args={'from': '0x', 'to': '0x', 'value': '0'}, raw={},
)
import json
print(json.dumps(build_payload(event=event, subscription=sub), indent=2))
"
```

Expected: the printed envelope has `business_name` between `subscription_name` and `chain_id`.

- [ ] **Step 4: Done — the spec's six goals are met**

1. ✅ DB column added.
2. ✅ Snapshot carries it.
3. ✅ Envelope emits it when set.
4. ✅ CRUD persists + returns it.
5. ✅ UI shows + edits it.
6. ✅ No existing test or downstream consumer broken.