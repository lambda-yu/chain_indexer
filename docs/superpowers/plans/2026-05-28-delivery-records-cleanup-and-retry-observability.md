# Delivery Records: Cleanup & Retry Observability — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound the growth of `delivery_records` via a worker-side cleanup loop, propagate the real retry attempt count from `retry_with_backoff` to the persisted row, and surface accurate status/attempts info in the UI.

**Architecture:** Five additions and one bug fix, no schema migration. Settings get a new nested section. `RetryExhausted` carries the attempts count. `Notifier`'s success/failure callbacks gain an `attempts` argument. `_Worker` runs a periodic `_run_cleanup_loop` calling a new `DeliveryRecordRepo.cleanup_success`. The router's `/retry` failure branch bumps the record's attempts via a new `bump_attempt` repo method. The list endpoint and UI move from client-side to server-side status filtering.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.0 async, FastAPI, pydantic-settings, React 19 + React Query 5, pytest + pytest-asyncio.

**Reference spec:** `docs/superpowers/specs/2026-05-28-delivery-records-cleanup-and-retry-observability-design.md`

---

## File Structure

**Modified Python files (no new files):**
- `core/settings.py` — add `DeliveryRecordsSettings` model
- `core/notifier/retry.py` — `RetryExhausted.attempts` attribute
- `core/notifier/notifier.py` — 6-arity callback signatures; `_send_one` reads `attempts` from exception
- `core/config/repositories.py` — three additions: `cleanup_success`, `bump_attempt`, `status` param on `list_all`; remove dead `list_failed`
- `apps/worker/main.py` — callback bodies updated; cleanup loop + start/stop wiring
- `apps/worker/chain_runner.py` — callback type annotations updated (bodies untouched; they pass through)
- `apps/web/routers/delivery_records.py` — `Literal` status param; `/retry` failure bumps attempts

**Modified TypeScript files:**
- `web/src/pages/DeliveryRecords.tsx` — server-side status filter; remove count chips; amber attempts highlight; `onSettled` invalidation

**New test files:**
- `tests/unit/test_cleanup_loop.py` — unit test for `_Worker._run_cleanup_loop` with mocked repo (stop-event interrupts sleep)
- `tests/integration/test_delivery_records_router.py` — list endpoint status filter; retry failure path bumps attempts

**Modified test files:**
- `tests/unit/test_retry.py` — assert `RetryExhausted.attempts == max_attempts`
- `tests/unit/test_notifier.py` — assert `on_failure`/`on_success` receive `attempts` argument
- `tests/integration/test_repositories.py` — `cleanup_success`, `bump_attempt`, `list_all` status filter

---

## Chunk 1: Foundation — settings and retry exception

Two small, self-contained changes that downstream chunks depend on. No call-site changes yet.

### Task 1: Add `DeliveryRecordsSettings` to settings

**Files:**
- Modify: `core/settings.py`

- [ ] **Step 1: Write the failing test**

Add to a new file `tests/unit/test_settings_delivery_records.py`:

```python
from __future__ import annotations

import os

from core.settings import Settings


def test_delivery_records_defaults() -> None:
    s = Settings()
    assert s.delivery_records.max_success_rows == 50000
    assert s.delivery_records.cleanup_interval_seconds == 300
    assert s.delivery_records.cleanup_batch_size == 1000


def test_delivery_records_env_override(monkeypatch) -> None:
    monkeypatch.setenv("CHAIN_INDEXER_DELIVERY_RECORDS__MAX_SUCCESS_ROWS", "123")
    monkeypatch.setenv("CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_INTERVAL_SECONDS", "7")
    monkeypatch.setenv("CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_BATCH_SIZE", "42")
    s = Settings()
    assert s.delivery_records.max_success_rows == 123
    assert s.delivery_records.cleanup_interval_seconds == 7
    assert s.delivery_records.cleanup_batch_size == 42
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_settings_delivery_records.py -v`
Expected: FAIL with `AttributeError: ... has no attribute 'delivery_records'`

- [ ] **Step 3: Add `DeliveryRecordsSettings` and wire into `Settings`**

In `core/settings.py`, add the model class after `LoggingSettings` (around line 40):

```python
class DeliveryRecordsSettings(BaseModel):
    max_success_rows: int = 50000
    cleanup_interval_seconds: int = 300
    cleanup_batch_size: int = 1000
```

Add to `Settings` (after the `logging` field, around line 55):

```python
    delivery_records: DeliveryRecordsSettings = Field(default_factory=DeliveryRecordsSettings)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_settings_delivery_records.py -v`
Expected: PASS (both tests)

- [ ] **Step 5: Type-check**

Run: `uv run mypy core/settings.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add core/settings.py tests/unit/test_settings_delivery_records.py
git commit -m "feat(settings): add DeliveryRecordsSettings nested config"
```

### Task 2: `RetryExhausted` carries attempts count

**Files:**
- Modify: `core/notifier/retry.py`
- Modify: `tests/unit/test_retry.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_retry.py`:

```python
@pytest.mark.asyncio
async def test_exhausted_exposes_attempts_count() -> None:
    async def op() -> str:
        raise RuntimeError("always fails")

    with pytest.raises(RetryExhausted) as exc:
        await retry_with_backoff(op, max_attempts=3, base_delay=0.0)
    assert exc.value.attempts == 3


@pytest.mark.asyncio
async def test_exhausted_attempts_matches_max_attempts_when_one() -> None:
    async def op() -> str:
        raise RuntimeError("nope")

    with pytest.raises(RetryExhausted) as exc:
        await retry_with_backoff(op, max_attempts=1, base_delay=0.0)
    assert exc.value.attempts == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_retry.py -v`
Expected: 2 FAILs with `AttributeError: 'RetryExhausted' object has no attribute 'attempts'`

- [ ] **Step 3: Add `attempts` to `RetryExhausted` and pass it from `retry_with_backoff`**

In `core/notifier/retry.py`, replace:

```python
class RetryExhausted(Exception):
    """Raised when all retry attempts have been used up. `__cause__` carries the last error."""
```

With:

```python
class RetryExhausted(Exception):
    """Raised when all retry attempts have been used up. `__cause__` carries the last error."""

    def __init__(self, message: str, *, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts
```

And at the bottom of `retry_with_backoff`, replace:

```python
    out = RetryExhausted(f"giving up after {max_attempts} attempts")
    out.__cause__ = last_err
    raise out
```

With:

```python
    out = RetryExhausted(
        f"giving up after {max_attempts} attempts", attempts=max_attempts
    )
    out.__cause__ = last_err
    raise out
```

- [ ] **Step 4: Run all retry tests to verify pass and no regressions**

Run: `uv run pytest tests/unit/test_retry.py -v`
Expected: all PASS (7 tests including the original 5)

- [ ] **Step 5: Type-check**

Run: `uv run mypy core/notifier/retry.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add core/notifier/retry.py tests/unit/test_retry.py
git commit -m "feat(retry): RetryExhausted carries attempts count"
```

---

## Chunk 2: Notifier callback signatures + worker callback bodies

Plumbs the new `attempts` argument through `Notifier._send_one` into the worker's success/failure callbacks, where it lands in the database. This is the chunk that fixes the "attempts always 1" bug.

### Task 3: Notifier callback aliases gain `attempts`

**Files:**
- Modify: `core/notifier/notifier.py`
- Modify: `tests/unit/test_notifier.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_notifier.py`:

```python
@pytest.mark.asyncio
async def test_on_failure_receives_attempts_from_retry_exhausted() -> None:
    """When a channel raises RetryExhausted, on_failure sees the real attempts."""
    from core.notifier.retry import RetryExhausted

    class _Boom(Channel):
        type = "boom"
        config_schema: dict = {}

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload: dict[str, Any]) -> None:
            raise RetryExhausted("dead", attempts=5)

    captured: list[tuple[str, int]] = []

    async def on_failure(
        sub_id: str, ch_id: str, chain_id: str,
        payload: dict[str, Any], error: str, attempts: int,
    ) -> None:
        captured.append((error, attempts))

    notifier = Notifier(
        channel_factory=lambda cfg: _Boom(),
        max_concurrency=10,
        on_failure=on_failure,
    )
    await notifier.start([_ch("c-boom")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c-boom"]), [_ch("c-boom")])])
    finally:
        await notifier.stop()
    assert len(captured) == 1
    assert captured[0][1] == 5  # attempts


@pytest.mark.asyncio
async def test_on_failure_defaults_attempts_to_one_for_plain_exception() -> None:
    """Non-RetryExhausted exceptions don't carry attempts; default to 1."""

    class _Boom(Channel):
        type = "boom-plain"
        config_schema: dict = {}

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload: dict[str, Any]) -> None:
            raise RuntimeError("plain")

    captured: list[int] = []

    async def on_failure(
        sub_id: str, ch_id: str, chain_id: str,
        payload: dict[str, Any], error: str, attempts: int,
    ) -> None:
        captured.append(attempts)

    notifier = Notifier(
        channel_factory=lambda cfg: _Boom(),
        max_concurrency=10,
        on_failure=on_failure,
    )
    await notifier.start([_ch("c-plain")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c-plain"]), [_ch("c-plain")])])
    finally:
        await notifier.stop()
    assert captured == [1]


@pytest.mark.asyncio
async def test_on_success_receives_attempts_one() -> None:
    """Success path always passes attempts=1."""
    captured: list[int] = []

    async def on_success(
        sub_id: str, ch_id: str, chain_id: str,
        payload: dict[str, Any], _err: None, attempts: int,
    ) -> None:
        captured.append(attempts)

    notifier = Notifier(
        channel_factory=lambda cfg: _CollectingChannel(),
        max_concurrency=10,
        on_success=on_success,
    )
    await notifier.start([_ch("c-ok")])
    try:
        await notifier.dispatch(_event(), [(_sub(["c-ok"]), [_ch("c-ok")])])
    finally:
        await notifier.stop()
    assert captured == [1]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_notifier.py -v`
Expected: 3 new tests FAIL — `on_failure` / `on_success` invoked with 5 args, callback expects 6 → `TypeError: missing 1 required positional argument: 'attempts'`

- [ ] **Step 3: Update `Notifier` callback aliases + `_send_one`**

In `core/notifier/notifier.py`:

Replace the type alias section (around line 22):

```python
FailureCallback = Callable[[str, str, str, dict[str, Any], str], Any] | None
```

With:

```python
FailureCallback = Callable[[str, str, str, dict[str, Any], str, int], Any] | None
SuccessCallback = Callable[[str, str, str, dict[str, Any], None, int], Any] | None
```

Update the `Notifier.__init__` signature so `on_success` uses `SuccessCallback`:

```python
    def __init__(
        self,
        *,
        channel_factory: Callable[[SnapshotChannel], Channel] = _default_factory,
        max_concurrency: int = 50,
        on_failure: FailureCallback = None,
        on_success: SuccessCallback = None,
    ) -> None:
```

And update the field annotation for `_on_success` if mypy complains (it's currently typed via the alias). The init body already stores it as-is; no body change needed beyond the type annotation.

Update `_send_one` to read `attempts` from the exception and pass it both ways. Replace the existing body of `_send_one` with:

```python
    async def _send_one(
        self, ch: Channel, payload: dict[str, Any], subscription_id: str, channel_id: str
    ) -> None:
        async with self._get_sem():
            try:
                await ch.send(payload)
                if self._on_success:
                    try:
                        await self._on_success(
                            subscription_id, channel_id,
                            payload.get("chain_id", ""),
                            payload, None, 1,
                        )
                    except Exception:  # noqa: BLE001
                        pass
            except Exception as exc:  # noqa: BLE001
                attempts = getattr(exc, "attempts", 1)
                log.error(
                    "notifier.send_failed",
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    delivery_id=payload.get("delivery_id"),
                    attempts=attempts,
                    error=repr(exc),
                )
                if self._on_failure:
                    try:
                        await self._on_failure(
                            subscription_id, channel_id,
                            payload.get("chain_id", ""),
                            payload, repr(exc), attempts,
                        )
                    except Exception:  # noqa: BLE001
                        log.error("notifier.on_failure_callback_error")
```

(Only changes vs. current code: success branch passes a trailing `1` for attempts; failure branch reads `attempts = getattr(exc, "attempts", 1)`, logs it, and passes it as the 6th argument to `on_failure`.)

- [ ] **Step 4: Run notifier tests to verify pass**

Run: `uv run pytest tests/unit/test_notifier.py tests/unit/test_notifier_sem_lazy.py -v`
Expected: all PASS

- [ ] **Step 5: Type-check**

Run: `uv run mypy core/notifier/notifier.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add core/notifier/notifier.py tests/unit/test_notifier.py
git commit -m "feat(notifier): callbacks receive attempts count

FailureCallback and SuccessCallback gain a trailing attempts: int
argument. _send_one reads attempts from RetryExhausted.attempts
(defaulting to 1 for plain exceptions) and forwards to both callbacks."
```

### Task 4: Worker callback bodies accept and persist real attempts

**Files:**
- Modify: `apps/worker/main.py` (`_on_delivery_failure`, `_on_delivery_success`)
- Modify: `apps/worker/chain_runner.py` (type annotations on `__init__` only)

- [ ] **Step 1: Read current callback bodies**

Confirm `apps/worker/main.py` `_on_delivery_failure` is at line 109 and `_on_delivery_success` at line 128, both currently 5-arity.

- [ ] **Step 2: Update callback signatures and persistence**

Replace `_on_delivery_failure` (currently lines 109–126):

```python
    async def _on_delivery_failure(
        self, subscription_id: str, channel_id: str, chain_id: str,
        payload: dict, error: str, attempts: int,
    ) -> None:
        from core.config.repositories import DeliveryRecordRepo
        try:
            async with self._db.session() as s:
                await DeliveryRecordRepo(s).create(
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    chain_id=chain_id,
                    event_payload=payload,
                    error=error,
                    attempts=attempts,
                    status="failed",
                )
                await s.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("worker.failed_delivery_save_error", error=repr(exc))
```

Replace `_on_delivery_success` (currently lines 128–144). Note: today's body passes `_error: str | None`; new signature uses `None` and adds `_attempts: int`:

```python
    async def _on_delivery_success(
        self, subscription_id: str, channel_id: str, chain_id: str,
        payload: dict, _error: None, _attempts: int,
    ) -> None:
        from core.config.repositories import DeliveryRecordRepo
        try:
            async with self._db.session() as s:
                await DeliveryRecordRepo(s).create(
                    subscription_id=subscription_id,
                    channel_id=channel_id,
                    chain_id=chain_id,
                    event_payload=payload,
                    status="success",
                )
                await s.commit()
        except Exception as exc:  # noqa: BLE001
            log.error("worker.delivery_success_save_error", error=repr(exc))
```

- [ ] **Step 3: Update `ChainRunner.__init__` parameter annotations (optional but improves type safety)**

In `apps/worker/chain_runner.py`, the `on_send_failure: Any = None` / `on_send_success: Any = None` parameters work as-is because they're typed `Any`. Leave the bodies untouched; the callbacks are simply forwarded to `Notifier`.

If desired for clarity (this is advisory), tighten the annotations:

```python
from core.notifier.notifier import FailureCallback, SuccessCallback
...
        on_send_failure: FailureCallback = None,
        on_send_success: SuccessCallback = None,
```

For this plan we **leave them as `Any`** to minimize blast radius. The `Notifier` constructor accepts the values at runtime regardless of the static type.

- [ ] **Step 4: Run unit + integration test suite**

Run: `uv run pytest tests/ -m "not e2e" -v`
Expected: all PASS. The previously-failing notifier tests (added in Task 3) pass, and worker tests do not regress because the worker callbacks now match the 6-arity contract.

- [ ] **Step 5: Type-check**

Run: `uv run mypy core apps`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add apps/worker/main.py
git commit -m "fix(worker): persist real attempts count on failed delivery

Previously attempts was always 1 because the failure callback didn't
receive the count. Now it's plumbed through from Notifier._send_one
(reading RetryExhausted.attempts). Success callback signature also
updated to 6-arity for symmetry."
```

---

## Chunk 3: Repository methods + worker cleanup loop

The data-access layer changes (`cleanup_success`, `bump_attempt`, `list_all` status filter) and the worker-side periodic loop that calls them. Includes one unit test for the loop's stop-event responsiveness, an integration test for end-to-end convergence, and removal of one dead method.

### Task 5: Repository `cleanup_success`, `bump_attempt`, `list_all` status filter

**Files:**
- Modify: `core/config/repositories.py`
- Modify: `tests/integration/test_repositories.py`

- [ ] **Step 1: Write failing integration tests**

Append to `tests/integration/test_repositories.py`:

```python
@pytest.mark.asyncio
async def test_cleanup_success_deletes_oldest_excess(db) -> None:
    """Insert 60 success + 10 failed; with keep=50, batch=100,
    expect 10 success deleted (oldest), all 10 failed untouched,
    and the 50 newest success rows remain."""
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        # Insert oldest first so created_at orders monotonically
        success_ids: list[str] = []
        for i in range(60):
            row = await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
            success_ids.append(row.id)
        failed_ids: list[str] = []
        for i in range(10):
            row = await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"f": i}, error="err", status="failed",
            )
            failed_ids.append(row.id)
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        deleted = await repo.cleanup_success(keep=50, batch=100)
        await s.commit()
        assert deleted == 10

    # Verify exactly the 10 oldest success rows are gone; failed untouched
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        from core.config.models import DeliveryRecord, DeliveryStatus
        from sqlalchemy import select
        r = await s.execute(
            select(DeliveryRecord).where(DeliveryRecord.status == DeliveryStatus.success)
        )
        remaining_success = [row.id for row in r.scalars().all()]
        assert len(remaining_success) == 50
        # The 10 deleted should be the oldest = success_ids[0:10]
        assert all(i not in remaining_success for i in success_ids[:10])
        assert all(i in remaining_success for i in success_ids[10:])
        r = await s.execute(
            select(DeliveryRecord).where(DeliveryRecord.status == DeliveryStatus.failed)
        )
        assert len(list(r.scalars().all())) == 10


@pytest.mark.asyncio
async def test_cleanup_success_respects_batch_cap(db) -> None:
    """200 success rows, keep=50, batch=30 → 30 deleted per call.
    Need to delete 150 total; with batch=30 that's 5 full-batch iterations.
    Run 7 iterations (5 needed + slack) and assert final count = 50."""
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        for i in range(200):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        deleted = await repo.cleanup_success(keep=50, batch=30)
        await s.commit()
        assert deleted == 30

    # Run additional iterations until converged (150 to delete, 30/call → 5 calls total).
    for _ in range(6):
        async with db.session() as s:
            repo = DeliveryRecordRepo(s)
            await repo.cleanup_success(keep=50, batch=30)
            await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        from core.config.models import DeliveryRecord, DeliveryStatus
        from sqlalchemy import select, func
        r = await s.execute(
            select(func.count())
            .select_from(DeliveryRecord)
            .where(DeliveryRecord.status == DeliveryStatus.success)
        )
        assert r.scalar() == 50


@pytest.mark.asyncio
async def test_cleanup_success_noop_when_under_cap(db) -> None:
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        for i in range(10):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        deleted = await repo.cleanup_success(keep=50, batch=1000)
        await s.commit()
        assert deleted == 0


@pytest.mark.asyncio
async def test_bump_attempt_increments_and_overwrites_error(db) -> None:
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        row = await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={}, error="first failure", attempts=2, status="failed",
        )
        await s.commit()
        delivery_id = row.id

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        await repo.bump_attempt(delivery_id, error="second failure")
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        row = await repo.get(delivery_id)
        assert row is not None
        assert row.attempts == 3
        assert row.error == "second failure"


@pytest.mark.asyncio
async def test_list_all_filters_by_status(db) -> None:
    from core.config.repositories import DeliveryRecordRepo

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={"ok": 1}, status="success",
        )
        await repo.create(
            subscription_id="sub", channel_id="ch", chain_id="eth",
            event_payload={"bad": 1}, error="err", status="failed",
        )
        await s.commit()

    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        only_failed = await repo.list_all(status="failed")
        assert len(only_failed) == 1
        assert only_failed[0].status.value == "failed"
        only_success = await repo.list_all(status="success")
        assert len(only_success) == 1
        assert only_success[0].status.value == "success"
        all_rows = await repo.list_all()
        assert len(all_rows) == 2
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/integration/test_repositories.py -v -k "cleanup_success or bump_attempt or list_all_filters_by_status"`
Expected: 5 FAILs (`AttributeError: ... has no attribute 'cleanup_success'`, etc.)

- [ ] **Step 3: Implement `cleanup_success`, `bump_attempt`, extend `list_all`; delete dead `list_failed`**

In `core/config/repositories.py`, in the `DeliveryRecordRepo` class:

**Replace** the existing `list_all` (currently around lines 280–286):

```python
    async def list_all(
        self,
        limit: int = 100,
        subscription_id: str | None = None,
        status: str | None = None,
    ) -> list[DeliveryRecord]:
        from core.config.models import DeliveryStatus
        stmt = select(DeliveryRecord)
        if subscription_id is not None:
            stmt = stmt.where(DeliveryRecord.subscription_id == subscription_id)
        if status is not None:
            stmt = stmt.where(DeliveryRecord.status == DeliveryStatus(status))
        stmt = stmt.order_by(DeliveryRecord.created_at.desc()).limit(limit)
        r = await self.s.execute(stmt)
        return list(r.scalars().all())
```

**Delete** the dead `list_failed` method (currently lines 270–278).

**Add** `cleanup_success` and `bump_attempt` after `mark_resolved`:

```python
    async def cleanup_success(self, *, keep: int, batch: int) -> int:
        """Delete oldest status='success' rows so at most `keep` remain.

        Returns the number of rows actually deleted (≤ batch). Only touches
        status='success'; failed/retrying/resolved rows are never affected.
        """
        from core.config.models import DeliveryStatus
        inner = (
            select(DeliveryRecord.id)
            .where(DeliveryRecord.status == DeliveryStatus.success)
            .order_by(DeliveryRecord.created_at.desc())
            .offset(keep)
            .limit(batch)
        )
        result = await self.s.execute(
            sa_delete(DeliveryRecord).where(DeliveryRecord.id.in_(inner))
        )
        return result.rowcount or 0

    async def bump_attempt(self, delivery_id: str, *, error: str) -> None:
        """Increment attempts and overwrite error. Used by manual retry on failure."""
        await self.s.execute(
            sa_update(DeliveryRecord)
            .where(DeliveryRecord.id == delivery_id)
            .values(
                attempts=DeliveryRecord.attempts + 1,
                error=error,
            )
        )
```

- [ ] **Step 4: Verify tests pass**

Run: `uv run pytest tests/integration/test_repositories.py -v`
Expected: all PASS

- [ ] **Step 5: Type-check**

Run: `uv run mypy core/config/repositories.py`
Expected: no errors

- [ ] **Step 6: Commit**

```bash
git add core/config/repositories.py tests/integration/test_repositories.py
git commit -m "feat(repo): add cleanup_success, bump_attempt; list_all status filter

- cleanup_success deletes oldest status='success' rows beyond keep.
  Bounded by batch per call for predictable transaction size.
- bump_attempt increments attempts and overwrites error (used by
  manual retry on failure).
- list_all now accepts optional status filter (server-side).
- Removed unused list_failed."
```

### Task 6: Worker `_run_cleanup_loop`

**Files:**
- Modify: `apps/worker/main.py`
- Create: `tests/unit/test_cleanup_loop.py`

- [ ] **Step 1: Write the failing unit test for stop-event responsiveness**

Create `tests/unit/test_cleanup_loop.py`:

```python
"""Unit tests for _Worker._run_cleanup_loop's stop-event interaction.

These tests use a minimal _Worker instance and mock its DB/repo dependencies
so the loop can be exercised without a real database.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.settings import DeliveryRecordsSettings, Settings


def _build_worker_with_mock_db(
    monkeypatch,
    *,
    max_success_rows: int = 100, interval_s: int = 30, batch: int = 1000,
    cleanup_return_value: int = 0,
) -> tuple[Any, AsyncMock]:
    """Construct a _Worker with patched _db.session() / DeliveryRecordRepo.

    Returns (worker, cleanup_mock) where cleanup_mock is the AsyncMock backing
    DeliveryRecordRepo.cleanup_success calls. Uses monkeypatch so the global
    repo module is restored after each test.
    """
    from apps.worker.main import _Worker

    settings = Settings(
        delivery_records=DeliveryRecordsSettings(
            max_success_rows=max_success_rows,
            cleanup_interval_seconds=interval_s,
            cleanup_batch_size=batch,
        ),
    )
    worker = _Worker(settings)

    # Replace _db.session() with a context-manager mock.
    fake_session = MagicMock()
    fake_session.commit = AsyncMock()
    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=fake_session)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    worker._db.session = MagicMock(return_value=session_cm)

    cleanup_mock = AsyncMock(return_value=cleanup_return_value)
    # Patch the repo class via monkeypatch so other tests aren't poisoned.
    import core.config.repositories as repo_mod
    monkey_repo = MagicMock()
    monkey_repo.return_value.cleanup_success = cleanup_mock
    monkeypatch.setattr(repo_mod, "DeliveryRecordRepo", monkey_repo)
    return worker, cleanup_mock


@pytest.mark.asyncio
async def test_cleanup_loop_stops_promptly_on_stop_event(monkeypatch) -> None:
    """Even with a long cleanup_interval_seconds, _stop.set() unblocks the loop
    within milliseconds (not minutes)."""
    worker, cleanup_mock = _build_worker_with_mock_db(monkeypatch, interval_s=3600)
    task = asyncio.create_task(worker._run_cleanup_loop())
    # Let the loop run one iteration so cleanup_success is called once,
    # then enter the sleep-on-stop branch.
    await asyncio.sleep(0.05)
    assert cleanup_mock.await_count >= 1
    worker._stop.set()
    # Loop should exit within a short timeout, not wait the 3600s sleep.
    await asyncio.wait_for(task, timeout=1.0)


@pytest.mark.asyncio
async def test_cleanup_loop_logs_exception_and_continues(monkeypatch) -> None:
    """A DB error in one iteration must not kill the loop."""
    worker, cleanup_mock = _build_worker_with_mock_db(monkeypatch, interval_s=0)
    cleanup_mock.side_effect = [RuntimeError("db down"), 5, 0]
    task = asyncio.create_task(worker._run_cleanup_loop())
    await asyncio.sleep(0.1)
    worker._stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    # Despite the raised exception in iter 1, iters 2/3 still ran.
    assert cleanup_mock.await_count >= 3


@pytest.mark.asyncio
async def test_cleanup_loop_passes_settings_to_repo(monkeypatch) -> None:
    worker, cleanup_mock = _build_worker_with_mock_db(
        monkeypatch, max_success_rows=42, batch=7, interval_s=3600,
    )
    task = asyncio.create_task(worker._run_cleanup_loop())
    await asyncio.sleep(0.05)
    worker._stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    cleanup_mock.assert_awaited_with(keep=42, batch=7)
```

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/unit/test_cleanup_loop.py -v`
Expected: FAIL with `AttributeError: '_Worker' object has no attribute '_run_cleanup_loop'`

- [ ] **Step 3: Add `_run_cleanup_loop` and wire into `start`/`shutdown`**

In `apps/worker/main.py`:

Add to `_Worker.__init__` (after `self._stop = asyncio.Event()` at line 106), a new instance field:

```python
        self._cleanup_task: asyncio.Task[None] | None = None
```

Add the loop method after `_on_block_processed` (around line 155):

```python
    async def _run_cleanup_loop(self) -> None:
        """Periodically delete oldest status='success' delivery_records rows
        so the table stays under settings.delivery_records.max_success_rows.

        Failed/retrying/resolved rows are never touched.
        """
        from core.config.repositories import DeliveryRecordRepo
        cfg = self._settings.delivery_records
        while not self._stop.is_set():
            try:
                async with self._db.session() as s:
                    deleted = await DeliveryRecordRepo(s).cleanup_success(
                        keep=cfg.max_success_rows,
                        batch=cfg.cleanup_batch_size,
                    )
                    await s.commit()
                if deleted > 0:
                    log.info(
                        "delivery_records.cleanup_done",
                        deleted=deleted,
                        keep=cfg.max_success_rows,
                    )
            except Exception:  # noqa: BLE001
                log.exception("delivery_records.cleanup_error")
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=cfg.cleanup_interval_seconds,
                )
            except TimeoutError:
                pass  # interval elapsed; loop again
```

Update `_Worker.start()` (around line 157). After `await self._watcher.start()` (currently the last line of `start`), append:

```python
        self._cleanup_task = asyncio.create_task(
            self._run_cleanup_loop(), name="delivery_records_cleanup",
        )
```

Update `_Worker.shutdown()` (around line 257). After `self._stop.set()` and before `if self._watcher is not None:`, add the cleanup-task cancellation:

```python
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
```

(`contextlib` is already imported at line 4.)

- [ ] **Step 4: Run cleanup loop unit tests**

Run: `uv run pytest tests/unit/test_cleanup_loop.py -v`
Expected: all 3 tests PASS

- [ ] **Step 5: Add integration test for end-to-end convergence**

Append to `tests/integration/test_repositories.py` (or a new file `tests/integration/test_worker_cleanup.py`):

```python
@pytest.mark.asyncio
async def test_worker_cleanup_loop_converges(db, redis_url) -> None:
    """Real _Worker against real DB. Insert 20 success rows + 5 failed;
    set keep=5; assert convergence to 5 success rows within a few seconds
    while failed rows remain untouched."""
    import asyncio
    from apps.worker.main import _Worker
    from core.config.repositories import DeliveryRecordRepo
    from core.settings import DeliveryRecordsSettings, Settings, DatabaseSettings, RedisSettings

    # Seed data
    async with db.session() as s:
        repo = DeliveryRecordRepo(s)
        for i in range(20):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"i": i}, status="success",
            )
        for i in range(5):
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"f": i}, error="x", status="failed",
            )
        await s.commit()

    # Database URL doesn't matter here — we overwrite worker._db with the
    # test's existing Database instance below. Use any valid scheme.
    settings = Settings(
        database=DatabaseSettings(url="sqlite+aiosqlite:///:memory:"),
        redis=RedisSettings(url=redis_url),
        delivery_records=DeliveryRecordsSettings(
            max_success_rows=5,
            cleanup_interval_seconds=1,
            cleanup_batch_size=100,
        ),
    )
    # Construct worker but ONLY exercise the cleanup loop (skip full start()).
    worker = _Worker(settings)
    worker._db = db  # reuse the test's already-connected Database
    task = asyncio.create_task(worker._run_cleanup_loop())
    try:
        # Wait up to 3s for convergence.
        for _ in range(30):
            async with db.session() as s:
                from core.config.models import DeliveryRecord, DeliveryStatus
                from sqlalchemy import select, func
                r = await s.execute(
                    select(func.count())
                    .select_from(DeliveryRecord)
                    .where(DeliveryRecord.status == DeliveryStatus.success)
                )
                if r.scalar() == 5:
                    break
            await asyncio.sleep(0.1)
        else:
            pytest.fail("cleanup loop did not converge within 3 seconds")
        # Failed rows must still be present.
        async with db.session() as s:
            from core.config.models import DeliveryRecord, DeliveryStatus
            from sqlalchemy import select, func
            r = await s.execute(
                select(func.count())
                .select_from(DeliveryRecord)
                .where(DeliveryRecord.status == DeliveryStatus.failed)
            )
            assert r.scalar() == 5
    finally:
        worker._stop.set()
        await asyncio.wait_for(task, timeout=2.0)
```

- [ ] **Step 6: Verify integration test passes**

Run: `uv run pytest tests/integration/test_worker_cleanup.py -v` (or whichever file you placed it in)
Expected: PASS

- [ ] **Step 7: Type-check**

Run: `uv run mypy apps/worker/main.py`
Expected: no errors

- [ ] **Step 8: Commit**

```bash
git add apps/worker/main.py tests/unit/test_cleanup_loop.py tests/integration/test_worker_cleanup.py
git commit -m "feat(worker): periodic cleanup loop for status=success records

A background asyncio task runs every cleanup_interval_seconds and calls
DeliveryRecordRepo.cleanup_success with the configured cap and batch.
Failed/retrying/resolved rows are never touched.

Uses asyncio.wait_for(_stop.wait(), timeout=N) so shutdown isn't
delayed by an in-flight sleep."
```

---

## Chunk 4: Router + UI changes

### Task 7: Router `/retry` failure bumps attempts; list endpoint accepts status

**Files:**
- Modify: `apps/web/routers/delivery_records.py`
- Create: `tests/integration/test_delivery_records_router.py`

- [ ] **Step 1: Write the failing integration tests**

Create `tests/integration/test_delivery_records_router.py`:

```python
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db, get_session
from apps.web.main import create_app
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.repositories import DeliveryRecordRepo

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_list_filters_by_status_server_side(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        async with db.session() as s:
            repo = DeliveryRecordRepo(s)
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"ok": 1}, status="success",
            )
            await repo.create(
                subscription_id="sub", channel_id="ch", chain_id="eth",
                event_payload={"bad": 1}, error="err", status="failed",
            )
            await s.commit()

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/delivery-records?status=failed")
            assert r.status_code == 200
            rows = r.json()
            assert len(rows) == 1
            assert rows[0]["status"] == "failed"

            r = await c.get("/api/delivery-records?status=success")
            assert r.status_code == 200
            assert len(r.json()) == 1

            r = await c.get("/api/delivery-records")
            assert len(r.json()) == 2
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_list_rejects_invalid_status(db: Database, redis_url: str) -> None:
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.get("/api/delivery-records?status=garbage")
            assert r.status_code == 422
    finally:
        await bus.disconnect()


@pytest.mark.asyncio
async def test_retry_failure_bumps_attempts(db: Database, redis_url: str) -> None:
    """A manual retry that itself fails increments attempts and updates error."""
    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        from core.config.repositories import ChannelRepo
        from core.config.models import ChannelType

        async with db.session() as s:
            # ChannelRepo.create auto-generates the id (uuid). Capture it from the
            # returned row and use it for the delivery record's channel_id.
            ch = await ChannelRepo(s).create(
                name="bad-webhook",
                type=ChannelType.http,
                config={"url": "http://127.0.0.1:1"},  # connection refused
            )
            row = await DeliveryRecordRepo(s).create(
                subscription_id="sub", channel_id=ch.id, chain_id="eth",
                event_payload={"x": 1}, error="initial", attempts=2, status="failed",
            )
            await s.commit()
            delivery_id = row.id

        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as c:
            r = await c.post(f"/api/delivery-records/{delivery_id}/retry", json={})
            assert r.status_code == 502

        async with db.session() as s:
            row = await DeliveryRecordRepo(s).get(delivery_id)
            assert row is not None
            assert row.attempts == 3
            assert row.error != "initial"  # overwritten by new error
            assert row.status.value == "failed"  # still failed, not resolved
    finally:
        await bus.disconnect()
```

> The exact `ChannelType` enum values live at `core/config/models.py:48` — confirm `ChannelType.http` matches whatever the codebase uses (likely `http`). If the value differs, substitute it.

- [ ] **Step 2: Run failing tests**

Run: `uv run pytest tests/integration/test_delivery_records_router.py -v`
Expected: FAILs:
- `test_list_filters_by_status_server_side`: 200 returns all rows ignoring status (current behavior)
- `test_list_rejects_invalid_status`: 200 (no validation today)
- `test_retry_failure_bumps_attempts`: 502 OK but `attempts == 2` (not bumped) and `error == "initial"` (not overwritten)

- [ ] **Step 3: Update router**

In `apps/web/routers/delivery_records.py`:

**Replace the imports block (top of file)**:

```python
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from core.bus.redis_bus import RedisBus
from core.config.repositories import DeliveryRecordRepo
from core.notifier.channel import CHANNEL_REGISTRY
```

(Additions: `Literal` from typing, `Query` from fastapi.)

**Add the status type alias** just after the imports, before `router = ...`:

```python
StatusFilter = Literal["success", "failed", "retrying", "resolved"]
```

**Replace** `list_delivery_records` (currently around lines 32–38). Use `Query(alias="status")` to avoid shadowing the `fastapi.status` module name inside the function scope:

```python
@router.get("", response_model=list[DeliveryRecordOut])
async def list_delivery_records(
    subscription_id: str | None = None,
    status_filter: StatusFilter | None = Query(None, alias="status"),
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[DeliveryRecordOut]:
    rows = await DeliveryRecordRepo(session).list_all(
        limit=200, subscription_id=subscription_id, status=status_filter,
    )
    return [DeliveryRecordOut.model_validate(r) for r in rows]
```

The query string still uses `?status=failed` (alias), but the Python parameter is `status_filter` so `fastapi.status` remains usable inside the function body if ever needed.

**Replace** the existing `retry_delivery` function (currently around lines 41–73) with the version that bumps attempts on failure:

```python
@router.post("/{delivery_id}/retry", status_code=status.HTTP_200_OK)
async def retry_delivery(
    delivery_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> dict[str, str]:
    repo = DeliveryRecordRepo(session)
    row = await repo.get(delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail="delivery not found")

    from core.config.repositories import ChannelRepo
    ch_row = await ChannelRepo(session).get(row.channel_id)
    if ch_row is None:
        raise HTTPException(status_code=404, detail="channel no longer exists")

    cls = CHANNEL_REGISTRY.get(ch_row.type.value)
    if cls is None:
        raise HTTPException(status_code=400, detail=f"unknown channel type: {ch_row.type}")

    try:
        ch = cls(config=ch_row.config, bus=bus)
        await ch.start()
        try:
            await ch.send(row.event_payload)
        finally:
            await ch.stop()
    except Exception as exc:
        await session.rollback()
        # Persist the failed retry so attempts and error reflect reality.
        await repo.bump_attempt(delivery_id, error=repr(exc))
        await session.commit()
        raise HTTPException(status_code=502, detail=f"重推失败: {exc!r}") from exc

    await repo.mark_resolved(delivery_id)
    await session.commit()
    return {"status": "resolved", "delivery_id": delivery_id}
```

**Important shape change vs. the old function:** The `try` block now wraps *only* the channel start/send/stop. Resolve + commit happens **after** the try on the success path. Failure path: rollback (drops any side-effect of the partially-completed send transaction), then `bump_attempt` + commit in a fresh implicit transaction, then raise 502. The `session` is reusable after `rollback()` — SQLAlchemy starts a new implicit transaction on the next `execute()`.

- [ ] **Step 4: Verify tests pass**

Run: `uv run pytest tests/integration/test_delivery_records_router.py -v`
Expected: all 3 PASS

- [ ] **Step 5: Run full backend test suite to check no regressions**

Run: `uv run pytest tests/ -m "not e2e"`
Expected: all PASS

- [ ] **Step 6: Lint + type-check**

Run: `uv run ruff check core apps tests`
Run: `uv run mypy core apps`
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add apps/web/routers/delivery_records.py tests/integration/test_delivery_records_router.py
git commit -m "feat(api): server-side status filter; /retry failure bumps attempts

- GET /api/delivery-records accepts status query param (Literal validated,
  422 on invalid). Replaces client-side filter that missed failed records
  buried beyond the 200-row LIMIT.
- POST /retry failure path now bump_attempts the row (attempts++, error
  overwritten) before raising 502, so repeat manual retries are visible."
```

### Task 8: Frontend — server-side status filter, attempts highlight, settled invalidation

**Files:**
- Modify: `web/src/pages/DeliveryRecords.tsx`

- [ ] **Step 1: Read the current file to confirm line ranges**

Confirm the file structure matches `web/src/pages/DeliveryRecords.tsx` as captured during spec writing. The mutation at lines 47–49 and the query at lines 36–40 are the primary targets.

- [ ] **Step 2: Update the React component**

The targeted edits:

**A. Query: include status in queryKey + URL**

Replace lines 36–40 (the `useQuery` for `delivery-records`):

```tsx
  const { data: items = [], isLoading } = useQuery<DeliveryRecord[]>({
    queryKey: ['delivery-records', subFilter, statusFilter],
    queryFn: () => {
      const q = new URLSearchParams()
      if (subFilter) q.set('subscription_id', subFilter)
      if (statusFilter !== 'all') q.set('status', statusFilter)
      const qs = q.toString()
      return api.get(`/delivery-records${qs ? `?${qs}` : ''}`)
    },
    refetchInterval: 10000,
  })
```

**B. Remove client-side filter**

Delete the `counts` and `filtered` derivations (around lines 53–57) and the count-chip block (around lines 73–80). Render `items` directly:

```tsx
  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h2 className="text-2xl font-bold">投递记录</h2>
      </div>
```

Then later in the JSX, replace `filtered.map(...)` with `items.map(...)` and update the empty state:

```tsx
      {isLoading ? <p className="text-gray-500">加载中...</p> : items.length === 0 ? (
        <div className="text-center py-12 text-gray-400">
          <Inbox size={32} className="mx-auto mb-2 opacity-50" />
          <p>暂无投递记录</p>
        </div>
      ) : (
        <div className="space-y-2">
          {items.map(item => {
```

**C. Amber attempts when >1**

Replace the existing line for attempts (around line 124):

```tsx
                  <span className="text-xs text-gray-400 ml-auto">{item.attempts} 次</span>
```

with:

```tsx
                  <span className={`text-xs ml-auto ${item.attempts > 1 ? 'text-amber-600 font-medium' : 'text-gray-400'}`}>{item.attempts} 次</span>
```

**D. Retry mutation: invalidate on settled**

Replace the existing `retryMut` line (around line 47):

```tsx
  const retryMut = useMutation({ mutationFn: (id: string) => api.post(`/delivery-records/${id}/retry`, {}), onSuccess: () => qc.invalidateQueries({ queryKey: ['delivery-records'] }) })
```

with:

```tsx
  const retryMut = useMutation({
    mutationFn: (id: string) => api.post(`/delivery-records/${id}/retry`, {}),
    onSettled: () => qc.invalidateQueries({ queryKey: ['delivery-records'] }),
  })
```

**E. Remove the unused imports**

After deletes, ensure imports don't include anything dead. The `lucide-react` icons all remain in use. No prune needed for icons.

- [ ] **Step 3: Build the frontend to catch type errors**

Run:
```bash
cd web && npm run build
```
Expected: build succeeds. If TypeScript complains about unused `counts` or `filtered`, ensure they're fully removed from the file.

- [ ] **Step 4: Manual UI smoke test (optional but recommended)**

Start dev server:
```bash
cd web && npm run dev          # in one terminal, proxies /api → :8000
uv run uvicorn apps.web.main:create_app --factory --reload --port 8000  # in another
```

Verify in a browser at http://localhost:5173/delivery-records:
- Clicking the "成功" / "失败" / "已解决" / "重试中" filter buttons updates the URL with `?status=...` and only shows rows of that status.
- Records with `attempts > 1` show the amber count.
- Clicking 重推 on a failed record that genuinely fails (channel unreachable) updates the count visibly without a manual refresh.

If you have no failure scenario to trigger easily, this step is optional — the unit/integration tests cover the contract.

- [ ] **Step 5: Commit**

```bash
git add web/src/pages/DeliveryRecords.tsx
git commit -m "feat(web): server-side status filter, amber attempts, settled invalidation

- DeliveryRecords filter button hits API with ?status=, so failed rows
  buried beyond the 200-row LIMIT are no longer hidden.
- Records with attempts > 1 highlight amber to flag retried deliveries.
- Retry mutation invalidates on settled (not just success), so a failed
  manual retry's bumped attempts count refreshes without manual reload.
- Removed local count chips (misleading after server-side filtering)."
```

---

## Final verification

### Task 9: Full-suite green-light

- [ ] **Step 1: Lint everything**

```bash
uv run ruff check core apps tests
```

- [ ] **Step 2: Type-check everything (strict)**

```bash
uv run mypy core apps
```

- [ ] **Step 3: Run full unit + integration suite**

```bash
uv run pytest tests/ -m "not e2e"
```
Expected: all tests pass; the existing test count (253) plus the new tests added in this plan all green.

- [ ] **Step 4: Frontend build**

```bash
cd web && npm run build
```

- [ ] **Step 5: Inspect git log**

```bash
git log --oneline main..HEAD
```
Expected ~8 commits, one per task, with clean messages.

- [ ] **Step 6: Final commit if any whitespace or fixes**

If any final adjustments are needed, commit them with a clear `fix:` or `chore:` prefix. Do not amend prior task commits.

---

## Out of scope (do not implement)

- No `next_retry_at` column, no persistent retry queue, no scheduled retry worker. (Future M-something.)
- No UI surface for cleanup configuration. Env vars only.
- No pagination, search, or date-range filtering on the UI list.
- No alembic migration. The existing schema is sufficient.
- No metrics/counter for "rows deleted last cycle". Worker structured logs only.

## References

- Spec: `docs/superpowers/specs/2026-05-28-delivery-records-cleanup-and-retry-observability-design.md`
- DeliveryRecord model: `core/config/models.py:167`
- Current callback wiring: `apps/worker/main.py:109` (failure), `apps/worker/main.py:128` (success)
- Notifier dispatch path: `core/notifier/notifier.py:84` (`_send_one`)
- Retry helper: `core/notifier/retry.py:21` (`retry_with_backoff`)
- Web list route: `apps/web/routers/delivery_records.py:32`
- UI page: `web/src/pages/DeliveryRecords.tsx:36` (query), `:124` (attempts display)
