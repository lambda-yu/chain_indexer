# Delivery Records: Cleanup & Retry Observability — Design

**Date**: 2026-05-28
**Status**: Draft
**Scope**: Bound growth of `delivery_records` table and surface accurate retry information in the UI
**Milestone**: post-m5 follow-up

## Background

The `delivery_records` table (renamed from `failed_deliveries` in commit `1169ad1`) records the outcome of every notification dispatch, *including successful sends*. With a busy chain and many subscriptions, this table grows linearly with delivered events — there is no retention policy, no automated cleanup, and only a single-row delete in the UI.

A second, smaller defect compounds the operability gap: the `attempts` column is **hardcoded to the SQLAlchemy default of `1`** when the worker's `_on_delivery_failure` callback writes a failed row. The `retry_with_backoff` helper does perform multiple in-process attempts, but the actual count is lost between `RetryExhausted` and the database row. As a result the UI's `{item.attempts} 次` indicator is always `1` for failures, defeating its purpose.

These two issues — unbounded growth and inaccurate attempts — are independent in mechanism but share enough of the data model, API, and UI surface that addressing them together avoids duplicate work on the React page and on `DeliveryRecordRepo`.

## Goals

- Keep `status='success'` rows in `delivery_records` under a configurable cap (default 50000). Older success rows are auto-deleted by a worker-side background task.
- Never auto-delete `failed`, `retrying`, or `resolved` rows; those have audit value and remain until an operator deletes them through the existing per-row delete control.
- Propagate the real attempts count from `retry_with_backoff` through the failure callback into the persisted row, so the UI's "N 次" indicator reflects truth.
- Update `attempts` and `error` when a manual retry from the UI fails, so repeat-poking is visible.
- Make status filtering server-side so a user looking at "失败" sees every failed record, not just those in the most-recent 200.

## Non-goals

- No persistent retry queue / automatic re-attempt after `RetryExhausted`. Manual retry from the UI is the only post-exhaustion path. (Persistent retry is option B2 from brainstorming, deferred.)
- No `next_retry_at` column. The current retry model is fully synchronous within one dispatch; a scheduled-later retry would need a redesign, not a column.
- No pagination, search, or date-range filtering on the UI list. 200-row LIMIT remains in place.
- No UI surface for cleanup configuration. Operators tune via `CHAIN_INDEXER_DELIVERY_RECORDS__*` env vars and restart.
- No metric/counter for "rows deleted last cycle". Worker structured log is the observation surface.
- No schema migration. All changes work with the existing `0007_rename_failed_deliveries.py` table shape.

## Architecture

```
┌──────────────────────────────────┐
│ apps/worker/main.py: _Worker     │
│   start():                       │
│     _runner_loops (existing)     │
│     _run_cleanup_loop (NEW)──┐   │
│   stop():                    │   │
│     cancel all               │   │
└──────────────────────────────│───┘
                               │
                               │ every cleanup_interval_seconds
                               ▼
                  ┌────────────────────────────┐
                  │ DeliveryRecordRepo         │
                  │   cleanup_success(keep,    │
                  │                   batch)   │
                  │   → DELETE oldest N rows   │
                  │     WHERE status='success' │
                  └────────────────────────────┘

┌────────────────────────────┐   raises    ┌────────────────────────────┐
│ retry_with_backoff         │────────────▶│ RetryExhausted             │
│   tracks attempt count     │             │   .attempts: int           │
└────────────────────────────┘             └─────────────┬──────────────┘
                                                         │ caught by
                                                         ▼
┌────────────────────────────┐         ┌────────────────────────────────┐
│ Notifier._send_one         │────────▶│ FailureCallback(..., attempts) │
│   reads exc.attempts       │         │   apps/worker/main.py          │
│   default 1 for non-Retry  │         │   _on_delivery_failure         │
│   exceptions               │         │     writes row with real count │
└────────────────────────────┘         └────────────────────────────────┘

┌────────────────────────────┐    GET /api/delivery-records?status=failed
│ React: DeliveryRecords.tsx │────────────────────────────────────────────▶
│   server-side status query │
└────────────────────────────┘
```

## Data Model

**No schema changes.** The existing `delivery_records` columns are sufficient:

- `attempts: Integer` — already exists; today populated incorrectly. This design wires correct values into it.
- `error: Text | NULL` — already exists; manual retry failure path updates it.
- `status: String(16)` — used as the cleanup discriminator.
- `created_at: DateTime` — used as the cleanup ordering key (delete oldest first).

## Configuration

New nested section in `core/settings.py`:

```python
class DeliveryRecordsSettings(BaseModel):
    max_success_rows: int = 50000
    cleanup_interval_seconds: int = 300
    cleanup_batch_size: int = 1000
```

Wired into `Settings`:

```python
delivery_records: DeliveryRecordsSettings = Field(default_factory=DeliveryRecordsSettings)
```

Env var examples:
- `CHAIN_INDEXER_DELIVERY_RECORDS__MAX_SUCCESS_ROWS=100000`
- `CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_INTERVAL_SECONDS=60`
- `CHAIN_INDEXER_DELIVERY_RECORDS__CLEANUP_BATCH_SIZE=2000`

Setting `max_success_rows=0` would delete all success rows on every cycle. We do not document this as a feature but the SQL semantics make it work safely.

## Component 1: Cleanup Loop

**Location**: `apps/worker/main.py`

The `_Worker` class gains:

```python
async def _run_cleanup_loop(self) -> None:
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
                self._stop.wait(), timeout=cfg.cleanup_interval_seconds,
            )
        except TimeoutError:
            pass  # interval elapsed; loop again
```

The `asyncio.wait_for(_stop.wait(), timeout=N)` pattern (instead of `asyncio.sleep(N)`) lets a `stop()` call interrupt the sleep immediately, so worker shutdown isn't blocked for up to 5 minutes.

Started inside `_Worker.start()` alongside the existing runner-launch logic:

```python
self._cleanup_task = asyncio.create_task(
    self._run_cleanup_loop(), name="delivery_records_cleanup",
)
```

Cancelled in `_Worker.stop()`:

```python
if self._cleanup_task is not None:
    self._cleanup_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await self._cleanup_task
```

The cleanup loop runs whether or not chain runners are active; this is intentional — a stopped chain can still need its historical success rows trimmed.

## Component 2: `DeliveryRecordRepo` additions

**Location**: `core/config/repositories.py`

```python
async def cleanup_success(self, *, keep: int, batch: int) -> int:
    """Delete oldest success rows so at most `keep` remain.

    Returns the number of rows actually deleted (≤ batch).
    Only touches status='success'; other statuses are left untouched.
    Single DELETE statement; no application-side count step.
    """
    from core.config.models import DeliveryStatus
    inner = (
        select(DeliveryRecord.id)
        .where(DeliveryRecord.status == DeliveryStatus.success)
        .order_by(DeliveryRecord.created_at.asc())
        .offset(keep)
        .limit(batch)
    )
    result = await self.s.execute(
        sa_delete(DeliveryRecord).where(DeliveryRecord.id.in_(inner))
    )
    return result.rowcount or 0
```

**Note on SQL approach**: The original brainstorm proposed a `LIMIT (COUNT-keep)` subquery. The OFFSET form above is equivalent in result and simpler to reason about — "skip the newest `keep` success rows, then delete up to `batch` of the oldest remainder". SQLite and PostgreSQL both support `IN (SELECT ... ORDER BY ... OFFSET ... LIMIT ...)` reliably.

```python
async def bump_attempt(self, delivery_id: str, *, error: str) -> None:
    """Increment attempts and overwrite error after a failed manual retry."""
    await self.s.execute(
        sa_update(DeliveryRecord)
        .where(DeliveryRecord.id == delivery_id)
        .values(
            attempts=DeliveryRecord.attempts + 1,
            error=error,
        )
    )
```

```python
async def list_all(
    self,
    limit: int = 100,
    subscription_id: str | None = None,
    status: str | None = None,           # NEW
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

The router validates the status string via `Literal`, so an invalid value never reaches `DeliveryStatus(status)` and there is no `ValueError` to translate.

## Component 3: Retry attempt propagation

**Location**: `core/notifier/retry.py`

```python
class RetryExhausted(Exception):
    def __init__(self, message: str, *, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts
```

`retry_with_backoff` change (single line):

```python
out = RetryExhausted(f"giving up after {max_attempts} attempts", attempts=max_attempts)
out.__cause__ = last_err
raise out
```

**Location**: `core/notifier/notifier.py`

The callback signature gains a trailing `attempts: int` parameter:

```python
FailureCallback = Callable[[str, str, str, dict[str, Any], str, int], Any] | None
SuccessCallback = Callable[[str, str, str, dict[str, Any], None, int], Any] | None
```

(`SuccessCallback` is added for symmetry; today `_on_success` shares the `FailureCallback` alias. Splitting them clarifies intent. Both callbacks become 6-arity.)

`_send_one`:

```python
try:
    await ch.send(payload)
    if self._on_success:
        await self._on_success(
            subscription_id, channel_id, payload.get("chain_id", ""),
            payload, None, 1,
        )
except Exception as exc:  # noqa: BLE001
    attempts = getattr(exc, "attempts", 1)
    log.error(..., attempts=attempts, ...)
    if self._on_failure:
        await self._on_failure(
            subscription_id, channel_id, payload.get("chain_id", ""),
            payload, repr(exc), attempts,
        )
```

`getattr(exc, "attempts", 1)` covers both `RetryExhausted` (has the attribute) and any other exception that bubbles through (defaults to 1, matching today's behavior).

**Location**: `apps/worker/main.py` — callback signatures and persistence:

```python
async def _on_delivery_failure(
    self, subscription_id: str, channel_id: str, chain_id: str,
    payload: dict, error: str, attempts: int,
) -> None:
    async with self._db.session() as s:
        await DeliveryRecordRepo(s).create(
            subscription_id=subscription_id, channel_id=channel_id,
            chain_id=chain_id, event_payload=payload,
            error=error, attempts=attempts, status="failed",
        )
        await s.commit()

async def _on_delivery_success(
    self, subscription_id: str, channel_id: str, chain_id: str,
    payload: dict, _error: None, _attempts: int,
) -> None:
    # unchanged body; attempts on success is always 1
```

**Location**: `apps/worker/chain_runner.py` — wherever these callbacks are wired into `Notifier`, no body change needed since the worker passes bound methods.

## Component 4: Manual retry attempt bump

**Location**: `apps/web/routers/delivery_records.py`

The `POST /api/delivery-records/{id}/retry` endpoint today:
- On success: `mark_resolved` → 200
- On failure: HTTP 502, record unchanged

Updated failure branch:

```python
try:
    await ch.send(row.event_payload)
    await repo.mark_resolved(delivery_id)
    await session.commit()
    return {"status": "resolved", "delivery_id": delivery_id}
except Exception as exc:
    await session.rollback()
    # New: persist the failed retry so attempts/error reflect reality
    async with session.begin():
        await repo.bump_attempt(delivery_id, error=repr(exc))
    raise HTTPException(status_code=502, detail=f"重推失败: {exc!r}") from exc
```

The `bump_attempt` write runs in a separate transaction after the rollback so the original send failure doesn't poison it. The HTTP response is still 502 to keep the existing UI's red-error path working.

## Component 5: API + UI changes

**Router** (`apps/web/routers/delivery_records.py`):

```python
from typing import Literal

StatusFilter = Literal["success", "failed", "retrying", "resolved"]

@router.get("", response_model=list[DeliveryRecordOut])
async def list_delivery_records(
    subscription_id: str | None = None,
    status: StatusFilter | None = None,
    session: AsyncSession = Depends(get_session),
) -> list[DeliveryRecordOut]:
    rows = await DeliveryRecordRepo(session).list_all(
        limit=200,
        subscription_id=subscription_id,
        status=status,
    )
    return [DeliveryRecordOut.model_validate(r) for r in rows]
```

FastAPI's `Literal` validation produces a clean 422 for unknown statuses.

**Frontend** (`web/src/pages/DeliveryRecords.tsx`):

1. Status filter goes server-side:
   ```ts
   const { data: items = [] } = useQuery<DeliveryRecord[]>({
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
   Drop the local `filtered = ...` line; render `items` directly.

2. Remove the per-status count chips from the header. The counts are computed from `items` (which after the server-side filter only contains the current status), making the chips either redundant ("失败 12" while looking at the 失败 tab) or misleading ("there are 200 success and 0 failed" when in reality there are 1000 failed beyond LIMIT).

3. `attempts` highlight: when `item.attempts > 1`, the chip turns amber to flag retried deliveries:
   ```tsx
   <span className={`text-xs ${item.attempts > 1 ? 'text-amber-600 font-medium' : 'text-gray-400'}`}>
     {item.attempts} 次
   </span>
   ```

4. Retry mutation invalidates on settled, not just success:
   ```ts
   const retryMut = useMutation({
     mutationFn: (id: string) => api.post(`/delivery-records/${id}/retry`, {}),
     onSettled: () => qc.invalidateQueries({ queryKey: ['delivery-records'] }),
   })
   ```
   This makes a failed retry visible (`attempts` and `error` refresh) without the user having to manually reload.

## Error handling & edge cases

- **Cleanup runs concurrently with inserts**: new success rows being inserted during the DELETE are excluded by the OFFSET (they sort by `created_at DESC` in the inner query — newest first — so they fall within the "keep" window). No locking needed.
- **DB transient errors in cleanup**: caught and logged, next interval retries.
- **Stop event during cleanup**: the loop checks `_stop.is_set()` at the top; an in-flight `cleanup_success` call completes (its DELETE is bounded to `batch`) and the loop exits on the next iteration.
- **`RetryExhausted` raised outside `retry_with_backoff`**: the `getattr(exc, "attempts", 1)` default handles any path where attempts isn't known.
- **Manual retry on a row whose channel was deleted**: existing behavior preserved (404 from `ChannelRepo.get`).
- **`bump_attempt` racing with a parallel `mark_resolved`**: not possible — manual retry serializes on the row's existence check, and `mark_resolved` only runs on the success branch.
- **`max_success_rows=0` setting**: deletes all success rows on each cycle. Documented as undefined but not blocked.
- **Cleanup batch smaller than overflow**: loop converges over multiple cycles. With default interval 300s and batch 1000, max sustained insert rate before cleanup falls behind is ~3.3 rows/sec — far above realistic worker throughput. If the rate is higher, raise `cleanup_batch_size` or shorten `cleanup_interval_seconds`.

## Testing

**Unit tests** (`tests/unit/`):

- `core/notifier/test_retry.py`: assert `RetryExhausted.attempts == max_attempts` after exhaustion; assert `RetryAbort` and other early-exits don't set attempts (or default to 1 in callers).
- `core/notifier/test_notifier.py`: mock a channel that raises `RetryExhausted(attempts=3)`; assert `on_failure` called with `attempts=3`; mock a channel that raises plain `RuntimeError`; assert `on_failure` called with `attempts=1`.
- `core/config/test_repositories.py`:
  - `cleanup_success`: insert 60 success + 10 failed rows; call with `keep=50, batch=100`; assert 10 success rows deleted, all 10 failed rows untouched, oldest survivors are exactly rows 11–60 by `created_at`.
  - `cleanup_success` batch cap: insert 200 success rows; `keep=50, batch=30`; assert 30 deleted in one call, six iterations needed to converge.
  - `bump_attempt`: assert `attempts` increments and `error` overwrites.
  - `list_all` with status filter: assert only matching rows returned.

**Integration tests** (`tests/integration/`):

- `test_worker_cleanup.py`: spin up a real `_Worker` with a tiny `cleanup_interval_seconds` (1) and small `max_success_rows` (5); seed 20 rows; assert convergence to 5 success rows within a few seconds; assert failed rows remain.
- `tests/integration/test_delivery_records_router.py`:
  - GET with `status=failed` returns only failed.
  - GET with invalid status returns 422.
  - POST `/retry` failure path: verify row's `attempts` and `error` updated, response is 502.

**Frontend smoke** (manual): start dev server, click each status tab, verify URL updates, verify amber attempts chip appears for retried rows.

## File-level change summary

| File | Change |
|------|--------|
| `core/settings.py` | +`DeliveryRecordsSettings` model; +field on `Settings`. |
| `core/notifier/retry.py` | `RetryExhausted` gains `attempts` attribute; one-line constructor change. |
| `core/notifier/notifier.py` | `FailureCallback` / `SuccessCallback` 6-arity; `_send_one` reads `attempts` from exception. |
| `core/config/repositories.py` | +`cleanup_success`, +`bump_attempt`; `list_all` adds `status` parameter. |
| `apps/worker/main.py` | Callback bodies accept `attempts`; persist real value; +`_run_cleanup_loop` and start/stop wiring. |
| `apps/worker/chain_runner.py` | No body change; if any test mocks the callback, update arity. |
| `apps/web/routers/delivery_records.py` | List endpoint accepts `status` (Literal); `/retry` failure path calls `bump_attempt`. |
| `web/src/pages/DeliveryRecords.tsx` | Server-side status filter; remove count chips; amber attempts >1; `onSettled` invalidation. |
| `tests/unit/...` | New tests per the Testing section. |
| `tests/integration/...` | New cleanup loop integration; expanded router tests. |

## Rollout

Single PR. No migration. No config required to run defaults. Operators wishing to tune the cap set the env vars before restarting the worker.

## Open questions

None blocking. Possible follow-ups (out of scope here):

- A `next_retry_at` column + scheduled retry queue if the project later wants automatic post-`RetryExhausted` retries.
- Pagination + date-range filtering on the UI if the table grows enough that 200-row LIMIT becomes limiting even after status filtering.
