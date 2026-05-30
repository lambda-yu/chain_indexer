# Subscription Lifecycle: Pause/Resume + Replay Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dedicated pause/resume controls for subscriptions and on-demand single-subscription historical replay over a block range, with replayed deliveries marked.

**Architecture:** Pause/resume flip `Subscription.enabled` via lightweight endpoints + existing hot-reload. Replay: a web endpoint publishes a self-contained `replay_request` on the Redis bus; a worker `ReplayWatcher` routes it to the live `ChainRunner`, whose new `replay()` reuses the pooled adapter + parser pipeline but builds a throwaway single-sub `Matcher` + one-shot `Notifier`, re-fetches the clamped range, and dispatches with a `replay=True` payload flag. Marking flows through a new `delivery_records.is_replay` column read from `payload["replay"]`.

**Tech Stack:** Python 3.11+ async, FastAPI, SQLAlchemy + Alembic, Redis pub/sub, React 19, pytest + pytest-asyncio.

**Reference spec:** `docs/superpowers/specs/2026-05-29-subscription-lifecycle-design.md`

---

## File Structure

**New files:**
- `apps/worker/replay_watcher.py` — `ReplayWatcher` (Redis `replay_request` consumer)
- `migrations/versions/0009_delivery_is_replay.py` — additive `is_replay` column

**New test files:**
- `tests/unit/test_replay_watcher.py`
- `tests/unit/test_chain_runner_replay.py`
- `tests/integration/test_subscription_pause_resume.py`
- `tests/integration/test_subscription_replay_api.py`

**Modified files:**
- `core/notifier/payload.py` — `build_payload` +`replay`
- `core/notifier/notifier.py` — `dispatch` +`replay`
- `core/config/models.py` — `DeliveryRecord` +`is_replay`
- `core/config/repositories.py` — `DeliveryRecordRepo.create` +`is_replay`
- `core/settings.py` — `ReplaySettings`
- `apps/worker/main.py` — `_on_replay_request`; start/stop `ReplayWatcher`; callbacks read `payload["replay"]`
- `apps/worker/chain_runner.py` — `replay`, `_replay_evm`, `_replay_solana`
- `apps/web/routers/subscriptions.py` — pause/resume/replay endpoints
- `apps/web/routers/delivery_records.py` — `DeliveryRecordOut` +`is_replay`
- `apps/web/schemas.py` — `ReplayRequest`
- `web/src/pages/Subscriptions.tsx` — pause/resume buttons + replay modal
- `web/src/pages/DeliveryRecords.tsx` — `is_replay` badge

---

## Chunk 1: Pause/Resume

Smallest, independent slice — reuses the existing `enabled` flag + hot-reload.

### Task 1.1: Pause/resume endpoints

**Files:**
- Modify: `apps/web/routers/subscriptions.py`
- Create: `tests/integration/test_subscription_pause_resume.py`

- [ ] **Step 1: Write the failing test**

Create `tests/integration/test_subscription_pause_resume.py`:

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChainKind, MatchKind
from core.config.repositories import ChainRepo, SubscriptionRepo

pytestmark = pytest.mark.integration


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
        )
        await s.commit()
        return sub.id


@pytest.mark.asyncio
async def test_pause_then_resume_flips_enabled(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        sub_id = await _seed(db)
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/subscriptions/{sub_id}/pause")
            assert r.status_code == 200
            assert r.json()["status"] == "paused"
        async with db.session() as s:
            row = await SubscriptionRepo(s).get(sub_id)
            assert row is not None and row.enabled is False

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/subscriptions/{sub_id}/resume")
            assert r.status_code == 200
            assert r.json()["status"] == "resumed"
        async with db.session() as s:
            row = await SubscriptionRepo(s).get(sub_id)
            assert row is not None and row.enabled is True
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_pause_unknown_sub_404(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post("/api/subscriptions/nope/pause")
        assert r.status_code == 404
    finally:
        await bus.disconnect()
```

> Confirm `SubscriptionRepo.create` kwargs by reading `core/config/repositories.py` — adjust the seed call if the signature differs (e.g. arg order). The `db` + `redis_url` fixtures exist.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_subscription_pause_resume.py -v`
Expected: FAIL (404 — endpoints don't exist).

- [ ] **Step 3: Add the endpoints**

In `apps/web/routers/subscriptions.py`, append (after the existing routes):

```python
@router.post("/{sub_id}/pause", status_code=status.HTTP_200_OK)
async def pause_subscription(
    sub_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> dict[str, str]:
    repo = SubscriptionRepo(session)
    if await repo.get(sub_id) is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    await repo.update(sub_id, enabled=False)
    await bump_and_publish(session, bus, entity="subscription", entity_id=sub_id, action="pause")
    return {"status": "paused", "id": sub_id}


@router.post("/{sub_id}/resume", status_code=status.HTTP_200_OK)
async def resume_subscription(
    sub_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> dict[str, str]:
    repo = SubscriptionRepo(session)
    if await repo.get(sub_id) is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    await repo.update(sub_id, enabled=True)
    await bump_and_publish(session, bus, entity="subscription", entity_id=sub_id, action="resume")
    return {"status": "resumed", "id": sub_id}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/integration/test_subscription_pause_resume.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check apps/web/routers/subscriptions.py`
Run: `uv run mypy apps/web/routers/subscriptions.py`
Expected: no new errors.

- [ ] **Step 6: Commit**

```bash
git add apps/web/routers/subscriptions.py tests/integration/test_subscription_pause_resume.py
git commit -m "feat(api): subscription pause/resume endpoints"
```

### Task 1.2: Pause/resume UI buttons

**Files:**
- Modify: `web/src/pages/Subscriptions.tsx`

- [ ] **Step 1: Read the current list row** to find where action buttons (edit/delete) live (around line 55-58).

- [ ] **Step 2: Add the toggle button + mutation**

In `web/src/pages/Subscriptions.tsx`, add `Pause` and `Play` to the lucide-react import. Add a mutation in the `Subscriptions` component:

```tsx
const pauseMut = useMutation({
  mutationFn: ({ id, action }: { id: string; action: 'pause' | 'resume' }) =>
    api.post(`/subscriptions/${id}/${action}`, {}),
  onSuccess: () => qc.invalidateQueries({ queryKey: ['subscriptions'] }),
})
```

In the action-buttons cell of each row, before the delete button, add:

```tsx
{s.enabled
  ? <button onClick={() => pauseMut.mutate({ id: s.id, action: 'pause' })} className="text-amber-600 hover:text-amber-800" title="暂停"><Pause size={14} /></button>
  : <button onClick={() => pauseMut.mutate({ id: s.id, action: 'resume' })} className="text-green-600 hover:text-green-800" title="恢复"><Play size={14} /></button>}
```

- [ ] **Step 3: Build the frontend**

Run: `cd web && npm run build`
Expected: build succeeds.

- [ ] **Step 4: Commit**

```bash
git add web/src/pages/Subscriptions.tsx
git commit -m "feat(web): inline pause/resume buttons in Subscriptions list"
```

---

## Chunk 2: Replay marking foundation

Payload flag + DB column + repo + callbacks + API out. No replay execution yet, but the marking machinery is testable in isolation.

### Task 2.1: `build_payload` + `Notifier.dispatch` replay flag

**Files:**
- Modify: `core/notifier/payload.py`
- Modify: `core/notifier/notifier.py`
- Modify: `tests/unit/test_notifier.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_notifier.py`:

```python
def test_build_payload_replay_flag() -> None:
    from core.notifier.payload import build_payload
    sub = _sub([])
    ev = _event()
    assert "replay" not in build_payload(event=ev, subscription=sub)
    assert build_payload(event=ev, subscription=sub, replay=True)["replay"] is True


@pytest.mark.asyncio
async def test_dispatch_threads_replay_to_payload() -> None:
    ch = _CollectingChannel()
    notifier = Notifier(channel_factory=lambda cfg: ch, max_concurrency=10)
    await notifier.start([_ch("c1")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c1"]), [_ch("c1")])], replay=True)
    finally:
        await notifier.stop()
    assert ch.calls[0]["replay"] is True
```

(Helpers `_sub`, `_event`, `_ch`, `_CollectingChannel` already exist at the top of `tests/unit/test_notifier.py`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_notifier.py::test_build_payload_replay_flag tests/unit/test_notifier.py::test_dispatch_threads_replay_to_payload -v`
Expected: FAIL (`build_payload` has no `replay` param / `dispatch` has no `replay` param).

- [ ] **Step 3: Add the `replay` params**

In `core/notifier/payload.py`, change `build_payload`:

```python
def build_payload(
    *, event: Event, subscription: SnapshotSubscription, replay: bool = False
) -> dict[str, Any]:
    payload = {
        ...  # existing fields unchanged
    }
    if replay:
        payload["replay"] = True
    return payload
```

In `core/notifier/notifier.py`, change `dispatch`:

```python
    async def dispatch(
        self,
        event: Event,
        hits: Sequence[tuple[SnapshotSubscription, Sequence[SnapshotChannel]]],
        *,
        replay: bool = False,
    ) -> None:
        tasks: list[asyncio.Task[None]] = []
        for sub, chans in hits:
            payload = build_payload(event=event, subscription=sub, replay=replay)
            ...  # rest unchanged
```

- [ ] **Step 4: Run tests to verify pass**

Run: `uv run pytest tests/unit/test_notifier.py -v`
Expected: all PASS.

- [ ] **Step 5: Lint + type-check**

Run: `uv run ruff check core/notifier/payload.py core/notifier/notifier.py`
Run: `uv run mypy core/notifier/payload.py core/notifier/notifier.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add core/notifier/payload.py core/notifier/notifier.py tests/unit/test_notifier.py
git commit -m "feat(notifier): replay flag in build_payload + dispatch"
```

### Task 2.2: `delivery_records.is_replay` column + migration + repo

**Files:**
- Modify: `core/config/models.py`
- Create: `migrations/versions/0009_delivery_is_replay.py`
- Modify: `core/config/repositories.py`
- Modify: `tests/integration/test_repositories.py`

- [ ] **Step 1: Write the failing round-trip test**

Append to `tests/integration/test_repositories.py`:

```python
@pytest.mark.asyncio
async def test_delivery_record_is_replay_round_trip(db) -> None:
    from core.config.repositories import DeliveryRecordRepo
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        r1 = await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={"replay": True}, status="success", is_replay=True,
        )
        r2 = await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={}, status="success",
        )
        await s.commit()
        id1, id2 = r1.id, r2.id
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        assert (await repo.get(id1)).is_replay is True
        assert (await repo.get(id2)).is_replay is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_repositories.py -k is_replay -v`
Expected: FAIL (no `is_replay` column / param).

- [ ] **Step 3: Add the model column**

In `core/config/models.py`, in `DeliveryRecord` after `resolved_at` (confirm `Boolean` is imported — it is):

```python
    is_replay: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=sa.false()
    )
```

Confirm `import sqlalchemy as sa` is present in this file; if not, add it (the migration file imports it separately).

> If `sa` is not imported in `models.py`, use `from sqlalchemy import false as sa_false` and `server_default=sa_false()`, OR use a plain string `server_default="0"` (SQLite/PG both accept `"0"` for a boolean default). Pick whichever keeps the file's existing import style — read the file first.

- [ ] **Step 4: Create the migration**

Create `migrations/versions/0009_delivery_is_replay.py`:

```python
"""add is_replay to delivery_records

Revision ID: 0009
Revises: 0008
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("delivery_records") as batch:
        batch.add_column(
            sa.Column("is_replay", sa.Boolean(), nullable=False, server_default=sa.false())
        )


def downgrade() -> None:
    with op.batch_alter_table("delivery_records") as batch:
        batch.drop_column("is_replay")
```

(Bare revision strings `"0009"` / `"0008"`, matching the convention of `0008_rpc_node_pool.py`.)

- [ ] **Step 5: Add the repo param**

In `core/config/repositories.py`, `DeliveryRecordRepo.create`:

```python
    async def create(
        self, *, subscription_id: str, channel_id: str, chain_id: str,
        event_payload: dict[str, Any], error: str | None = None, attempts: int = 1,
        status: str = "success", is_replay: bool = False,
    ) -> DeliveryRecord:
        from core.config.models import DeliveryStatus
        row = DeliveryRecord(
            subscription_id=subscription_id, channel_id=channel_id, chain_id=chain_id,
            event_payload=event_payload, error=error, attempts=attempts,
            status=DeliveryStatus(status), is_replay=is_replay,
        )
        ...
```

- [ ] **Step 6: Run test to verify pass**

Run: `uv run pytest tests/integration/test_repositories.py -k is_replay -v`
Expected: PASS.

- [ ] **Step 7: Type-check**

Run: `uv run mypy core/config/models.py core/config/repositories.py`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add core/config/models.py migrations/versions/0009_delivery_is_replay.py core/config/repositories.py tests/integration/test_repositories.py
git commit -m "feat(db): delivery_records.is_replay (migration 0009)"
```

### Task 2.3: Worker callbacks read `payload["replay"]` + `DeliveryRecordOut`

**Files:**
- Modify: `apps/worker/main.py`
- Modify: `apps/web/routers/delivery_records.py`
- Modify: `tests/integration/test_repositories.py` (or a worker test)

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_repositories.py` a focused test that exercises the worker callback path. Since the callbacks are methods on `_Worker`, build a minimal worker with a real DB and call the success callback with a replay payload:

```python
@pytest.mark.asyncio
async def test_worker_callback_marks_is_replay(db) -> None:
    from apps.worker.main import _Worker
    from core.settings import Settings
    from core.config.repositories import DeliveryRecordRepo

    worker = _Worker(Settings())
    worker._db = db  # reuse the test DB
    await worker._on_delivery_success(
        "sub", "ch", "eth", {"replay": True}, None, 1,
    )
    async with db.session() as s:
        rows = await DeliveryRecordRepo(s).list_all(limit=10)
        assert any(r.is_replay for r in rows)
```

(Confirm `_on_delivery_success` arity is `(self, subscription_id, channel_id, chain_id, payload, _error, _attempts)` by reading `apps/worker/main.py` — it is 6-arity from sub-project B.)

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/integration/test_repositories.py::test_worker_callback_marks_is_replay -v`
Expected: FAIL (`is_replay` not set → no replay rows).

- [ ] **Step 3: Read the flag in both callbacks**

In `apps/worker/main.py`, in `_on_delivery_success` and `_on_delivery_failure`, add `is_replay=bool(payload.get("replay", False))` to the `DeliveryRecordRepo(s).create(...)` call:

```python
                await DeliveryRecordRepo(s).create(
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    chain_id=chain_id,
                    event_payload=payload,
                    status="success",
                    is_replay=bool(payload.get("replay", False)),
                )
```

(and the analogous `status="failed"` call in `_on_delivery_failure`, with `error=error, attempts=attempts` preserved.)

- [ ] **Step 4: Add `is_replay` to `DeliveryRecordOut`**

In `apps/web/routers/delivery_records.py`, add to the `DeliveryRecordOut` pydantic model:

```python
    is_replay: bool
```

(`from_attributes=True` reads it from the ORM row automatically.)

- [ ] **Step 5: Run test to verify pass**

Run: `uv run pytest tests/integration/test_repositories.py::test_worker_callback_marks_is_replay -v`
Run: `uv run pytest tests/integration -k delivery -v`
Expected: PASS (no regressions in delivery-records router tests).

- [ ] **Step 6: Lint + type-check**

Run: `uv run ruff check apps/worker/main.py apps/web/routers/delivery_records.py`
Run: `uv run mypy apps/web/routers/delivery_records.py`
Expected: no new errors.

- [ ] **Step 7: Commit**

```bash
git add apps/worker/main.py apps/web/routers/delivery_records.py tests/integration/test_repositories.py
git commit -m "feat: worker callbacks mark is_replay from payload; expose in DeliveryRecordOut"
```

---

## Chunk 3: Replay trigger chain

ReplaySettings → ReplayRequest schema → replay endpoint → ReplayWatcher → worker routing. End of this chunk: a replay request flows from API to a (stubbed) runner handoff, without the actual replay execution (Chunk 4).

### Task 3.1: `ReplaySettings`

**Files:**
- Modify: `core/settings.py`
- Create: `tests/unit/test_settings_replay.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_settings_replay.py`:

```python
from __future__ import annotations

from core.settings import Settings


def test_replay_defaults() -> None:
    assert Settings().replay.max_replay_blocks == 10000


def test_replay_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_REPLAY__MAX_REPLAY_BLOCKS", "500")
    assert Settings().replay.max_replay_blocks == 500
```

- [ ] **Step 2: Run** `uv run pytest tests/unit/test_settings_replay.py -v` — expect FAIL.

- [ ] **Step 3: Add the settings model**

In `core/settings.py`, after the other nested models:

```python
class ReplaySettings(BaseModel):
    max_replay_blocks: int = 10000
```

Add to `Settings`:

```python
    replay: ReplaySettings = Field(default_factory=ReplaySettings)
```

- [ ] **Step 4: Run** `uv run pytest tests/unit/test_settings_replay.py -v` — expect 2 PASS.
- [ ] **Step 5:** `uv run mypy core/settings.py` — clean.
- [ ] **Step 6: Commit**

```bash
git add core/settings.py tests/unit/test_settings_replay.py
git commit -m "feat(settings): ReplaySettings (max_replay_blocks)"
```

### Task 3.2: Replay endpoint + `ReplayRequest`

**Files:**
- Modify: `apps/web/schemas.py`
- Modify: `apps/web/routers/subscriptions.py`
- Create: `tests/integration/test_subscription_replay_api.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/integration/test_subscription_replay_api.py`:

```python
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChainKind, MatchKind
from core.config.repositories import ChainRepo, SubscriptionRepo

pytestmark = pytest.mark.integration


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
        )
        await s.commit()
        return sub.id


@pytest.mark.asyncio
async def test_replay_publishes_request(db: Database, redis_url: str) -> None:
    bus_writer = RedisBus(url=redis_url)
    bus_reader = RedisBus(url=redis_url)
    await bus_writer.connect()
    await bus_reader.connect()
    drain: asyncio.Task | None = None
    try:
        sub_id = await _seed(db)
        received: list[dict] = []
        ready = asyncio.Event()

        async def _drain() -> None:
            async for msg in bus_reader.subscribe("replay_request", ready=ready):
                received.append(msg)
                return
        drain = asyncio.create_task(_drain())
        await asyncio.wait_for(ready.wait(), timeout=5.0)

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus_writer
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(f"/api/subscriptions/{sub_id}/replay",
                             json={"from_block": 100, "to_block": 200})
            assert r.status_code == 202
            body = r.json()
            assert body["status"] == "accepted"
            assert "request_id" in body and body["chain_id"] == "eth"

        await asyncio.wait_for(drain, timeout=5.0)
        assert len(received) == 1
        msg = received[0]
        assert msg["chain_id"] == "eth"
        assert msg["subscription"]["id"] == sub_id
        assert msg["subscription"]["start_block"] is None  # forced None for replay
        assert msg["subscription"]["enabled"] is True
        assert msg["from_block"] == 100 and msg["to_block"] == 200
    finally:
        if drain and not drain.done():
            drain.cancel()
        await bus_writer.disconnect()
        await bus_reader.disconnect()


@pytest.mark.asyncio
async def test_replay_validation(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        sub_id = await _seed(db)
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            assert (await c.post("/api/subscriptions/nope/replay",
                                 json={"from_block": 1, "to_block": 2})).status_code == 404
            assert (await c.post(f"/api/subscriptions/{sub_id}/replay",
                                 json={"from_block": 200, "to_block": 100})).status_code == 422
            assert (await c.post(f"/api/subscriptions/{sub_id}/replay",
                                 json={"from_block": 0, "to_block": 100000})).status_code == 422
    finally:
        await bus.disconnect()
```

- [ ] **Step 2: Run** `uv run pytest tests/integration/test_subscription_replay_api.py -v` — expect FAIL (no endpoint).

- [ ] **Step 3: Add `ReplayRequest` schema**

In `apps/web/schemas.py`, add:

```python
class ReplayRequest(BaseModel):
    from_block: int = Field(ge=0)
    to_block: int = Field(ge=0)
```

- [ ] **Step 4: Add the replay endpoint**

In `apps/web/routers/subscriptions.py`:
- Add imports: `from uuid import uuid4`, `from core.config.repositories import ChannelRepo` (already imported), `from core.settings import load_settings`, `from apps.web.schemas import ReplayRequest`, and ensure `Any` is importable (`from typing import Any`).

```python
@router.post("/{sub_id}/replay", status_code=status.HTTP_202_ACCEPTED)
async def replay_subscription(
    sub_id: str,
    payload: ReplayRequest,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> dict[str, Any]:
    repo = SubscriptionRepo(session)
    sub = await repo.get(sub_id)
    if sub is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    if payload.to_block < payload.from_block:
        raise HTTPException(status_code=422, detail="to_block < from_block")
    span = payload.to_block - payload.from_block + 1
    max_span = load_settings().replay.max_replay_blocks
    if span > max_span:
        raise HTTPException(status_code=422, detail=f"replay span {span} exceeds max {max_span}")

    res = await session.execute(
        select(SubscriptionChannel.channel_id).where(
            SubscriptionChannel.subscription_id == sub_id
        )
    )
    channel_ids: list[str] = list(res.scalars().all())
    channels: list[dict[str, Any]] = []
    ch_repo = ChannelRepo(session)
    for cid in channel_ids:
        ch = await ch_repo.get(cid)
        if ch is not None:
            channels.append({"id": ch.id, "name": ch.name, "type": ch.type.value, "config": ch.config})

    request_id = str(uuid4())
    await bus.publish("replay_request", {
        "request_id": request_id,
        "chain_id": sub.chain_id,
        "subscription": {
            "id": sub.id, "name": sub.name, "chain_id": sub.chain_id,
            "address": sub.address, "abi_id": sub.abi_id,
            "match_kind": sub.match_kind.value, "match_name": sub.match_name,
            "arg_filters": sub.arg_filters, "enabled": True,
            "channel_ids": channel_ids, "start_block": None,
        },
        "channels": channels,
        "from_block": payload.from_block,
        "to_block": payload.to_block,
    })
    return {"status": "accepted", "request_id": request_id,
            "chain_id": sub.chain_id, "span": span}
```

- [ ] **Step 5: Run** `uv run pytest tests/integration/test_subscription_replay_api.py -v` — expect all PASS.

- [ ] **Step 6: Lint + type-check**

Run: `uv run ruff check apps/web/routers/subscriptions.py apps/web/schemas.py`
Run: `uv run mypy apps/web/routers/subscriptions.py apps/web/schemas.py`
Expected: no new errors.

- [ ] **Step 7: Commit**

```bash
git add apps/web/routers/subscriptions.py apps/web/schemas.py tests/integration/test_subscription_replay_api.py
git commit -m "feat(api): subscription replay endpoint publishes replay_request"
```

### Task 3.3: `ReplayWatcher` + worker routing

**Files:**
- Create: `apps/worker/replay_watcher.py`
- Modify: `apps/worker/main.py`
- Create: `tests/unit/test_replay_watcher.py`

- [ ] **Step 1: Write the failing test for `ReplayWatcher`**

Create `tests/unit/test_replay_watcher.py`:

```python
from __future__ import annotations

import asyncio

import pytest


class _FakeBus:
    def __init__(self, messages):
        self._messages = messages

    async def subscribe(self, channel, *, ready=None):
        if ready is not None:
            ready.set()
        for m in self._messages:
            yield m


@pytest.mark.asyncio
async def test_replay_watcher_forwards_messages() -> None:
    from apps.worker.replay_watcher import ReplayWatcher
    seen: list[dict] = []

    async def on_replay(msg):
        seen.append(msg)

    w = ReplayWatcher(bus=_FakeBus([{"a": 1}, {"a": 2}]), on_replay=on_replay)
    await w.start()
    await asyncio.sleep(0.05)
    await w.stop()
    assert seen == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_replay_watcher_survives_callback_error() -> None:
    from apps.worker.replay_watcher import ReplayWatcher
    seen: list[dict] = []

    async def on_replay(msg):
        if msg["a"] == 1:
            raise RuntimeError("boom")
        seen.append(msg)

    w = ReplayWatcher(bus=_FakeBus([{"a": 1}, {"a": 2}]), on_replay=on_replay)
    await w.start()
    await asyncio.sleep(0.05)
    await w.stop()
    assert seen == [{"a": 2}]  # second message still processed despite first raising
```

- [ ] **Step 2: Run** `uv run pytest tests/unit/test_replay_watcher.py -v` — expect FAIL (no module).

- [ ] **Step 3: Create `apps/worker/replay_watcher.py`**

```python
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import structlog

log = structlog.get_logger(__name__)

_DEFAULT_CHANNEL = "replay_request"


class _Bus(Protocol):
    def subscribe(self, channel: str, *, ready: asyncio.Event | None = ...):  # type: ignore[no-untyped-def]
        ...


class ReplayWatcher:
    """Subscribes to the Redis `replay_request` channel and hands each message
    to `on_replay`. A raising callback is logged and does not stop the loop."""

    def __init__(
        self,
        *,
        bus: _Bus,
        on_replay: Callable[[dict[str, Any]], Awaitable[None]],
        channel: str = _DEFAULT_CHANNEL,
    ) -> None:
        self._bus = bus
        self._on_replay = on_replay
        self._channel = channel
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="replay_watcher")

    async def _run(self) -> None:
        async for msg in self._bus.subscribe(self._channel, ready=self._ready):
            try:
                await self._on_replay(msg)
            except Exception:  # noqa: BLE001
                log.exception("replay_watcher.dispatch_failed")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
```

- [ ] **Step 4: Run** `uv run pytest tests/unit/test_replay_watcher.py -v` — expect 2 PASS.

- [ ] **Step 5: Wire into `_Worker` + add routing test**

In `apps/worker/main.py`:
- `__init__`: add `self._replay_watcher: ReplayWatcher | None = None`.
- Add the `_on_replay_request` method:

```python
    async def _on_replay_request(self, msg: dict[str, Any]) -> None:
        chain_id = msg.get("chain_id")
        entry = self._runners.get(chain_id) if chain_id is not None else None
        if entry is None:
            log.warning(
                "worker.replay_no_runner",
                chain_id=chain_id, request_id=msg.get("request_id"),
            )
            return
        runner, _ = entry
        asyncio.create_task(
            runner.replay(msg), name=f"replay:{msg.get('request_id')}",
        )
```

- In `start()`, after the config watcher is started, add:

```python
        from apps.worker.replay_watcher import ReplayWatcher
        self._replay_watcher = ReplayWatcher(bus=self._bus, on_replay=self._on_replay_request)
        await self._replay_watcher.start()
```

- In `shutdown()`, after `self._stop.set()` and near the watcher stop, add:

```python
        if self._replay_watcher is not None:
            await self._replay_watcher.stop()
```

Append a routing unit test to `tests/unit/test_replay_watcher.py` (or a new `tests/unit/test_worker_replay_routing.py`):

```python
@pytest.mark.asyncio
async def test_on_replay_request_routes_to_runner() -> None:
    from unittest.mock import AsyncMock, MagicMock
    from apps.worker.main import _Worker
    from core.settings import Settings

    worker = _Worker(Settings())
    runner = MagicMock()
    runner.replay = AsyncMock()
    worker._runners["eth"] = (runner, MagicMock())
    await worker._on_replay_request({"chain_id": "eth", "request_id": "r1"})
    await asyncio.sleep(0.01)  # let the created task run
    runner.replay.assert_awaited_once()


@pytest.mark.asyncio
async def test_on_replay_request_no_runner_is_noop() -> None:
    from apps.worker.main import _Worker
    from core.settings import Settings

    worker = _Worker(Settings())
    # No runner registered for "eth" — must not raise.
    await worker._on_replay_request({"chain_id": "eth", "request_id": "r1"})
```

- [ ] **Step 6: Run** `uv run pytest tests/unit/test_replay_watcher.py -v` (and the routing tests' file) — expect all PASS. Also run `uv run pytest tests/ -m "not e2e" -k worker -v` for regressions.

> Note: `runner.replay` doesn't exist yet (Chunk 4) but the routing test uses a `MagicMock` runner, so it passes. The real `replay` lands next chunk.

- [ ] **Step 7: Lint + type-check**

Run: `uv run ruff check apps/worker/replay_watcher.py apps/worker/main.py`
Run: `uv run mypy apps/worker/replay_watcher.py apps/worker/main.py`
Expected: no new errors.

- [ ] **Step 8: Commit**

```bash
git add apps/worker/replay_watcher.py apps/worker/main.py tests/unit/test_replay_watcher.py
git commit -m "feat(worker): ReplayWatcher consumes replay_request, routes to runner"
```

---

## Chunk 4: ChainRunner.replay

The execution core. Reuses the runner's pooled adapter + pipeline + ABI registry, builds a throwaway single-sub matcher + one-shot notifier, never touches checkpoints.

### Task 4.1: `ChainRunner.replay` (EVM + Solana)

**Files:**
- Modify: `apps/worker/chain_runner.py`
- Create: `tests/unit/test_chain_runner_replay.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_chain_runner_replay.py`:

```python
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from core.chains.types import Block, BlockHeader


def _build_evm_runner():
    from apps.worker.chain_runner import ChainRunner
    from core.config.snapshot import SnapshotChain

    chain = SnapshotChain(
        id="eth", kind="evm", rpc_http="http://x", rpc_ws=None,
        confirmations=1, poll_interval_ms=1000, commitment=None,
        trace_internal_calls=False, log_query_range_blocks=100,
        slot_query_range_blocks=1000,
    )
    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda c: MagicMock(),
        channel_factory=lambda c: MagicMock(),
        checkpoint_repo=MagicMock(),
    )
    return runner


def _replay_msg(from_block=10, to_block=11):
    return {
        "request_id": "r1", "chain_id": "eth",
        "subscription": {
            "id": "s1", "name": "t", "chain_id": "eth", "address": None,
            "abi_id": None, "match_kind": "native_transfer", "match_name": None,
            "arg_filters": {}, "enabled": True, "channel_ids": [], "start_block": None,
        },
        "channels": [],
        "from_block": from_block, "to_block": to_block,
    }


@pytest.mark.asyncio
async def test_replay_dispatches_with_replay_flag_and_no_checkpoint(monkeypatch) -> None:
    runner = _build_evm_runner()
    # Stub adapter: tip high enough; fetch_block returns an empty block; no logs.
    adapter = MagicMock()
    adapter.get_latest_block_number = AsyncMock(return_value=1000)
    hdr = lambda n: BlockHeader(number=n, hash=f"0x{n}", parent_hash="0x0", timestamp=0)
    adapter.fetch_block = AsyncMock(side_effect=lambda n: Block(header=hdr(n), txs=[], logs=[]))
    adapter.fetch_logs = AsyncMock(return_value=[])
    runner._adapter = adapter
    runner._abi_registry = MagicMock()
    runner._cp = MagicMock()
    runner._cp.save = AsyncMock()
    runner._channel_factory = lambda c: MagicMock()

    # Stub the parser pipeline to yield zero events (we only assert no-checkpoint + completion).
    runner._evm_pipeline = MagicMock()
    runner._evm_pipeline.run = MagicMock(return_value=[])

    # Stub build_evm_log_filter so we don't need real ABI/filter machinery.
    import apps.worker.chain_runner as mod
    fake_filter = MagicMock()
    fake_filter.skip_logs = True
    monkeypatch.setattr(mod, "build_evm_log_filter", lambda *a, **k: fake_filter)

    await runner.replay(_replay_msg(10, 11))

    # Replay must fetch the 2 blocks and never write a checkpoint.
    assert adapter.fetch_block.await_count == 2
    runner._cp.save.assert_not_called()


@pytest.mark.asyncio
async def test_replay_clamps_to_safe_tip(monkeypatch) -> None:
    runner = _build_evm_runner()
    adapter = MagicMock()
    adapter.get_latest_block_number = AsyncMock(return_value=15)  # safe_tip = 15 - 1 = 14
    hdr = lambda n: BlockHeader(number=n, hash=f"0x{n}", parent_hash="0x0", timestamp=0)
    adapter.fetch_block = AsyncMock(side_effect=lambda n: Block(header=hdr(n), txs=[], logs=[]))
    adapter.fetch_logs = AsyncMock(return_value=[])
    runner._adapter = adapter
    runner._abi_registry = MagicMock()
    runner._cp = MagicMock(); runner._cp.save = AsyncMock()
    runner._channel_factory = lambda c: MagicMock()
    runner._evm_pipeline = MagicMock(); runner._evm_pipeline.run = MagicMock(return_value=[])

    import apps.worker.chain_runner as mod
    fake_filter = MagicMock(); fake_filter.skip_logs = True
    monkeypatch.setattr(mod, "build_evm_log_filter", lambda *a, **k: fake_filter)

    # Request to_block=100 but safe_tip=14 → only blocks 10..14 (5 blocks).
    await runner.replay(_replay_msg(10, 100))
    assert adapter.fetch_block.await_count == 5
```

> Confirm `ChainRunner.__init__` accepts the kwargs used in `_build_evm_runner` (chain, adapter_factory, channel_factory, checkpoint_repo) — read the constructor. Adjust if the optional callback params differ.

- [ ] **Step 2: Run** `uv run pytest tests/unit/test_chain_runner_replay.py -v` — expect FAIL (`replay` not defined).

- [ ] **Step 3: Implement `replay` + `_replay_evm` + `_replay_solana`**

In `apps/worker/chain_runner.py`, add to the `ChainRunner` class:

```python
    async def replay(self, msg: dict[str, Any]) -> None:
        from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription
        sub = SnapshotSubscription(**msg["subscription"])
        channels = [SnapshotChannel(**c) for c in msg["channels"]]
        one = ConfigSnapshot(
            version=-1, chains=[], subscriptions=[sub], channels=channels, abis=[],
        )
        matcher = Matcher(one)
        notifier = Notifier(
            channel_factory=self._channel_factory,
            max_concurrency=self._notifier_max_concurrency,
            on_failure=self._on_send_failure,
            on_success=self._on_send_success,
        )
        await notifier.start(channels)
        try:
            if self._chain.kind == "solana":
                await self._replay_solana(msg, sub, matcher, notifier)
            else:
                await self._replay_evm(msg, sub, matcher, notifier)
        except Exception:  # noqa: BLE001
            log.exception(
                "chain_runner.replay_failed",
                chain_id=self._chain.id, request_id=msg.get("request_id"),
            )
        finally:
            await notifier.stop()

    async def _replay_evm(self, msg, sub, matcher, notifier) -> None:
        from dataclasses import replace

        from core.config.snapshot import ConfigSnapshot
        one = ConfigSnapshot(version=-1, chains=[], subscriptions=[sub], channels=[], abis=[])
        log_filter = build_evm_log_filter(one, self._chain.id, self._abi_registry)
        from_block = msg["from_block"]
        tip = await self._adapter.get_latest_block_number()
        to_block = min(msg["to_block"], tip - self._chain.confirmations)
        if to_block < from_block:
            log.warning("chain_runner.replay_nothing_to_do",
                        request_id=msg.get("request_id"),
                        from_block=from_block, to_block=to_block)
            return
        log.info("chain_runner.replay_starting", request_id=msg.get("request_id"),
                 sub=sub.id, from_block=from_block, to_block=to_block)
        n = from_block
        while n <= to_block:
            if self._stop.is_set():
                break
            block = await self._adapter.fetch_block(n)
            logs = [] if log_filter.skip_logs else await self._adapter.fetch_logs(
                n, n, addresses=log_filter.addresses, topics=log_filter.topics_param)
            block = replace(block, logs=logs)
            assert self._evm_pipeline is not None
            for event in self._evm_pipeline.run(block):
                hits = [(s, ch) for s, ch in matcher.match(event) if ch]
                if hits:
                    await notifier.dispatch(event, hits, replay=True)
            n += 1
        log.info("chain_runner.replay_done", request_id=msg.get("request_id"),
                 blocks=to_block - from_block + 1)

    async def _replay_solana(self, msg, sub, matcher, notifier) -> None:
        from_slot = msg["from_block"]
        tip = await self._adapter.get_latest_slot()
        to_slot = min(msg["to_block"], tip)
        if to_slot < from_slot:
            log.warning("chain_runner.replay_nothing_to_do",
                        request_id=msg.get("request_id"),
                        from_block=from_slot, to_block=to_slot)
            return
        log.info("chain_runner.replay_starting", request_id=msg.get("request_id"),
                 sub=sub.id, from_block=from_slot, to_block=to_slot)
        s = from_slot
        while s <= to_slot:
            if self._stop.is_set():
                break
            block = await self._adapter.fetch_block(s)
            if block is not None:
                assert self._solana_pipeline is not None
                for event in self._solana_pipeline.run(block):
                    hits = [(sub_, ch) for sub_, ch in matcher.match(event) if ch]
                    if hits:
                        await notifier.dispatch(event, hits, replay=True)
            s += 1
        log.info("chain_runner.replay_done", request_id=msg.get("request_id"),
                 blocks=to_slot - from_slot + 1)
```

> The Solana path iterates slot-by-slot (no `_get_blocks_classified` windowing — simpler and avoids the size-limit raise; the span is already capped at `max_replay_blocks`). `fetch_block` returning `None` for a skipped slot is handled.

> `Any` must be imported in `chain_runner.py` (it already is, used by callback params). `build_evm_log_filter`, `Matcher`, `Notifier` are already imported at module top.

- [ ] **Step 4: Run** `uv run pytest tests/unit/test_chain_runner_replay.py -v` — expect 2 PASS.

- [ ] **Step 5: Run the broader chain_runner suite** for regressions:

Run: `uv run pytest tests/unit/test_chain_runner.py tests/unit/test_chain_runner_instrumentation.py -v`
Expected: all PASS (replay is additive).

- [ ] **Step 6: Lint + type-check**

Run: `uv run ruff check apps/worker/chain_runner.py`
Run: `uv run mypy apps/worker/chain_runner.py`
Expected: no new errors. (If mypy complains about the untyped `replay`/`_replay_*` params, add minimal annotations: `msg: dict[str, Any]`, `sub: SnapshotSubscription`, `matcher: Matcher`, `notifier: Notifier`.)

- [ ] **Step 7: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner_replay.py
git commit -m "feat(runner): ChainRunner.replay — single-sub ranged re-delivery"
```

---

## Chunk 5: UI + final verification

### Task 5.1: Replay modal + is_replay badge

**Files:**
- Modify: `web/src/pages/Subscriptions.tsx`
- Modify: `web/src/pages/DeliveryRecords.tsx`

- [ ] **Step 1: Subscriptions replay modal**

In `web/src/pages/Subscriptions.tsx`:
- Add `History` (or `RotateCcw`) to the lucide-react import for the replay button.
- Add state at the top of the `Subscriptions` component: `const [replayFor, setReplayFor] = useState<Sub | null>(null)`.
- Add a replay mutation:

```tsx
const replayMut = useMutation({
  mutationFn: ({ id, from_block, to_block }: { id: string; from_block: number; to_block: number }) =>
    api.post(`/subscriptions/${id}/replay`, { from_block, to_block }),
})
```

- Add an inline replay button per row (next to pause/resume):

```tsx
<button onClick={() => setReplayFor(s)} className="text-blue-500 hover:text-blue-700" title="重放"><History size={14} /></button>
```

- Render a small modal when `replayFor` is set (mirror the existing form modal style):

```tsx
{replayFor && (
  <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50">
    <form onSubmit={(e) => {
      e.preventDefault(); const fd = new FormData(e.currentTarget)
      replayMut.mutate({
        id: replayFor.id,
        from_block: Number(fd.get('from_block')),
        to_block: Number(fd.get('to_block')),
      }, { onSuccess: () => setReplayFor(null) })
    }} className="bg-white rounded-lg p-6 w-80 space-y-3">
      <h3 className="text-lg font-bold">重放订阅「{replayFor.name}」</h3>
      <input name="from_block" type="number" placeholder="起始区块" required className="w-full border rounded px-3 py-1.5 text-sm" />
      <input name="to_block" type="number" placeholder="结束区块" required className="w-full border rounded px-3 py-1.5 text-sm" />
      <div className="flex gap-2 pt-2">
        <button type="button" onClick={() => setReplayFor(null)} className="flex-1 border rounded py-1.5 text-sm">取消</button>
        <button type="submit" className="flex-1 bg-black text-white rounded py-1.5 text-sm">提交重放</button>
      </div>
      {replayMut.isError && <p className="text-red-500 text-xs">{String(replayMut.error)}</p>}
      {replayMut.isSuccess && <p className="text-green-600 text-xs">已接受，后台处理中</p>}
    </form>
  </div>
)}
```

- [ ] **Step 2: DeliveryRecords replay badge**

In `web/src/pages/DeliveryRecords.tsx`:
- Add `is_replay: boolean` to the `DeliveryRecord` interface.
- In the row header, next to the status chip, add:

```tsx
{item.is_replay && <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-purple-100 text-purple-700">重放</span>}
```

- [ ] **Step 3: Build the frontend**

Run: `cd web && npm run build`
Expected: build succeeds, no TypeScript errors.

- [ ] **Step 4: Commit**

```bash
git add web/src/pages/Subscriptions.tsx web/src/pages/DeliveryRecords.tsx
git commit -m "feat(web): subscription replay modal + delivery replay badge"
```

### Task 5.2: Full-suite green-light

- [ ] **Step 1: Lint** — `uv run ruff check core apps tests` — same or fewer errors than the `main` baseline (no NEW).
- [ ] **Step 2: Type-check** — `uv run mypy core apps` — no NEW errors vs baseline.
- [ ] **Step 3: Full suite** — `uv run pytest tests/ -m "not e2e" --tb=line -q` — all PASS (environmental Docker/testcontainer aside).
- [ ] **Step 4: Frontend build** — `cd web && npm run build`.
- [ ] **Step 5: Git log** — `git log --oneline main..HEAD` — one clean commit per task.
- [ ] **Step 6: Final fix-up commit** if any baseline drift, with a `chore:` prefix. Do not amend prior task commits.

---

## Out of scope (do not implement)

- Gap backfill on resume.
- Chain-wide replay.
- Replay for chains without an active runner.
- Replay progress bar / status push.
- Replay serialization/queue.
- "Replay-only" filter in Delivery Records.
- Checkpoint / last_processed mutation during replay.

## References

- Spec: `docs/superpowers/specs/2026-05-29-subscription-lifecycle-design.md`
- `Subscription` / `DeliveryRecord` models: `core/config/models.py:103`, `:167`
- `SubscriptionRepo` / `DeliveryRecordRepo` / `ChannelRepo`: `core/config/repositories.py`
- `bump_and_publish`: `apps/web/routers/_common.py`
- `get_subscription` channel-id query pattern: `apps/web/routers/subscriptions.py:85`
- `build_payload`: `core/notifier/payload.py:31`
- `Notifier.dispatch`: `core/notifier/notifier.py:63`
- `ConfigWatcher` (mirror for ReplayWatcher): `apps/worker/config_watcher.py`
- `_Worker` start/shutdown/_runners/_on_delivery_*: `apps/worker/main.py:110`, `:133`
- `ChainRunner` adapter/pipeline/registry/callbacks: `apps/worker/chain_runner.py:73`
- `build_evm_log_filter`: `core/matcher/filter_set.py`
- `RedisBus.publish/subscribe`: `core/bus/redis_bus.py:41`, `:45`
- Latest migration: `migrations/versions/0008_rpc_node_pool.py` (revision "0008")
