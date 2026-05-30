# Subscription Lifecycle: Pause/Resume + Replay — Design

**Date**: 2026-05-29
**Status**: Draft
**Scope**: Lightweight pause/resume controls for subscriptions, plus on-demand single-subscription historical replay over a block range. Surfaced in API + UI; replayed deliveries are marked.
**Milestone**: post-m5 follow-up (sub-project C of three; A=RPC pool done, B=observability done)

## Background

Operators occasionally need to (1) temporarily stop a subscription from delivering without deleting it, and (2) re-deliver historical events for a subscription after fixing a downstream outage. The data model already supports the first case partially: `Subscription.enabled` gates whether the matcher indexes a subscription, and the worker's hot-reload (`config_changed` → snapshot rebuild → `apply_snapshot` → new `Matcher`) makes a toggle take effect without a restart. But the only way to flip `enabled` today is through the full edit form (`PUT /subscriptions/{id}` with the entire payload), which is clumsy for a quick pause.

For replay, the catchup mechanism re-processes from `min(start_block)` chain-wide, but it is coarse (re-delivers for every subscription on the chain), requires a worker restart, and is not targeted at one subscription or range. There is no clean per-subscription, ranged, on-demand replay.

This sub-project adds: dedicated pause/resume endpoints + inline UI controls (reusing `enabled` semantics — resume rejoins at the live tip, no gap backfill), and a targeted single-subscription replay that re-fetches a block range, re-matches only the target subscription, and re-dispatches to its channels — running inside the live chain runner and marking replayed deliveries so downstream consumers and the audit trail can distinguish them.

## Goals

- `POST /subscriptions/{id}/pause` and `/resume` flip `enabled` (false/true) and publish `config_changed`, taking effect via hot-reload with no restart.
- Inline pause/resume buttons in the Subscriptions UI list.
- `POST /subscriptions/{id}/replay {from_block, to_block}` validates the range and publishes a self-contained `replay_request` on the Redis bus; returns `202 Accepted` with a `request_id`.
- A worker `ReplayWatcher` (mirroring `ConfigWatcher`) consumes `replay_request` and routes it to the live `ChainRunner` for that chain.
- `ChainRunner.replay(msg)` reuses the runner's pooled adapter + parser pipeline + ABI registry, but builds a throwaway single-subscription `Matcher` + one-shot `Notifier`, re-fetches the (clamped) range, and dispatches matched events with a `replay: true` payload flag. It never touches checkpoints or `last_processed_block`.
- Replayed deliveries are marked: `replay: true` in the payload, and a new `delivery_records.is_replay` column; the worker callbacks read `payload["replay"]` (no callback signature change).
- The Delivery Records UI shows a "replay" badge; the Subscriptions UI exposes a replay modal.
- Replay span is capped by `settings.replay.max_replay_blocks` (default 10000); the API rejects over-cap requests with 422.

## Non-goals

- No gap backfill on resume. Re-enabling rejoins at the live tip; events missed during pause are not re-delivered (use replay for that). This matches existing `enabled` behavior.
- No chain-wide replay. Replay is single-subscription only.
- No replay for a chain that has no active runner (the chosen "reuse live runner" approach requires the chain to be enabled/running). The worker logs a warning and drops the request.
- No replay progress bar / live status push. A 202 + structured logs suffice.
- No replay serialization/queue. Concurrent replays each run as their own async task.
- No "replay-only" filter toggle in the Delivery Records UI (the badge is enough for v1).
- No checkpoint / `last_processed_block` mutation during replay (it is a re-delivery, not progress).
- No confirmation-buffer reorg handling during replay (it fetches already-confirmed blocks directly).
- No deduplication of replayed deliveries on the server — marking lets downstream dedup if it wants.

## Architecture

```
Pause/Resume:
  POST /subscriptions/{id}/pause|resume
    └─ SubscriptionRepo.update(enabled=…) + bump_and_publish("config_changed")
         └─ worker ConfigWatcher → snapshot rebuild → runner.apply_snapshot → new Matcher
              (paused sub dropped from / resumed sub re-added to the live matcher)

Replay:
  POST /subscriptions/{id}/replay {from_block,to_block}
    ├─ validate (404 / 422 range / 422 over-cap)
    ├─ fetch sub + bound channels from DB
    └─ bus.publish("replay_request", {request_id, chain_id, subscription{…},
                                      channels[…], from_block, to_block})
         └─ Redis pub/sub
              └─ worker ReplayWatcher → _Worker._on_replay_request
                   └─ find live runner for chain_id (else warn+drop)
                        └─ asyncio.create_task(runner.replay(msg))   ← parallel to live head-following
                             ├─ build single-sub Matcher + one-shot Notifier (start target channels)
                             ├─ clamp to_block to safe tip
                             ├─ for n in [from..to]: fetch block(+logs via 1-sub filter),
                             │     pipeline.run, match (1 sub), notifier.dispatch(..., replay=True)
                             └─ notifier.stop()    (NO checkpoint / last_processed writes)
```

The replay re-uses the runner's `self._adapter` (with the RPC endpoint pool from sub-project A), `self._evm_pipeline`/`self._solana_pipeline`, and `self._abi_registry`. It builds an isolated `Matcher` and `Notifier` from the self-contained replay message, so replay works regardless of the target subscription's live enabled-state or the live matcher's contents.

## Component 1: Pause/Resume endpoints

**Location**: `apps/web/routers/subscriptions.py`

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

`bump_and_publish` (existing helper used by other mutating routes) increments the config version and publishes `config_changed`. The worker's `ConfigWatcher` picks it up and reconciles, so a paused sub is dropped from the live `Matcher` and a resumed sub re-added — no restart.

## Component 2: Replay endpoint

**Location**: `apps/web/routers/subscriptions.py` (+ `ReplayRequest` schema in `apps/web/schemas.py`)

```python
class ReplayRequest(BaseModel):
    from_block: int = Field(ge=0)
    to_block: int = Field(ge=0)


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
        raise HTTPException(
            status_code=422, detail=f"replay span {span} exceeds max {max_span}"
        )

    # Bound channel ids (direct query — same pattern as get_subscription, which
    # deliberately avoids list_enabled_with_channels so disabled subs still resolve).
    res = await session.execute(
        select(SubscriptionChannel.channel_id).where(
            SubscriptionChannel.subscription_id == sub_id
        )
    )
    channel_ids: list[str] = list(res.scalars().all())
    channels = []
    for cid in channel_ids:
        ch = await ChannelRepo(session).get(cid)
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
            "arg_filters": sub.arg_filters, "enabled": True,  # force-true for replay matcher
            "channel_ids": channel_ids, "start_block": sub.start_block,
        },
        "channels": channels,
        "from_block": payload.from_block,
        "to_block": payload.to_block,
    })
    return {
        "status": "accepted", "request_id": request_id,
        "chain_id": sub.chain_id, "span": span,
    }
```

The `subscription` dict mirrors `SnapshotSubscription` fields (including `channel_ids` and a force-`enabled=True` so the replay matcher always indexes it regardless of the live pause state). The `channels` list mirrors `SnapshotChannel`. The message is self-contained — the runner rebuilds config from it, not from the live snapshot.

`to_block ≤ tip` is NOT validated here (the web process has no chain adapter); the worker clamps it (Component 4).

`load_settings()` is the existing settings loader; if the router already has access to settings via app state, use that instead.

## Component 3: ReplayWatcher + worker routing

**Location**: `apps/worker/replay_watcher.py` (new), `apps/worker/main.py`

`ReplayWatcher` mirrors `ConfigWatcher`'s structure (subscribe to a Redis channel, hand each message to a callback, log-and-continue on error):

```python
class ReplayWatcher:
    def __init__(
        self, *, bus: _Bus,
        on_replay: Callable[[dict[str, Any]], Awaitable[None]],
        channel: str = "replay_request",
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

`_Worker` gains:

```python
    async def _on_replay_request(self, msg: dict[str, Any]) -> None:
        chain_id = msg.get("chain_id")
        entry = self._runners.get(chain_id)
        if entry is None:
            log.warning(
                "worker.replay_no_runner",
                chain_id=chain_id, request_id=msg.get("request_id"),
            )
            return
        runner, _ = entry
        asyncio.create_task(
            runner.replay(msg),
            name=f"replay:{msg.get('request_id')}",
        )
```

`_Worker.start()` constructs + starts a `ReplayWatcher(bus=self._bus, on_replay=self._on_replay_request)` (store on `self._replay_watcher`); `shutdown()` stops it (before stopping runners, mirroring `_watcher`).

## Component 4: ChainRunner.replay

**Location**: `apps/worker/chain_runner.py`

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
```

`_replay_evm`:

```python
    async def _replay_evm(self, msg, sub, matcher, notifier) -> None:
        from dataclasses import replace
        one = ConfigSnapshot(version=-1, chains=[], subscriptions=[sub],
                             channels=[], abis=[])
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
            for event in self._evm_pipeline.run(block):
                hits = [(s, ch) for s, ch in matcher.match(event) if ch]
                if hits:
                    await notifier.dispatch(event, hits, replay=True)
            n += 1
        log.info("chain_runner.replay_done", request_id=msg.get("request_id"),
                 blocks=to_block - from_block + 1)
```

`_replay_solana` is analogous: `tip = await get_latest_slot()`, `to_block = min(msg["to_block"], tip)`, iterate slots (optionally via `_get_blocks_classified` to skip empty slots), `fetch_block(slot)` may return `None` (skip), `self._solana_pipeline.run(block)`, dispatch with `replay=True`.

**Invariants:** replay never calls `self._cp.save`, never updates `last_processed_block`, never uses the `ConfirmationBuffer`. It shares `self._adapter` with live head-following (async-safe; the RPC pool handles concurrency). `self._stop` aborts replay during shutdown.

## Component 5: Replay payload + delivery marking

### `build_payload` (`core/notifier/payload.py`)

```python
def build_payload(
    *, event: Event, subscription: SnapshotSubscription, replay: bool = False
) -> dict[str, Any]:
    payload = { ... existing fields ... }
    if replay:
        payload["replay"] = True
    return payload
```

### `Notifier.dispatch` (`core/notifier/notifier.py`)

```python
    async def dispatch(self, event, hits, *, replay: bool = False) -> None:
        ...
        payload = build_payload(event=event, subscription=sub, replay=replay)
        ...
```

Live callers pass nothing (default `False`) — zero behavior change.

### `delivery_records.is_replay` column (`core/config/models.py`)

```python
    is_replay: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=sa.false()
    )
```

Migration `0009_delivery_is_replay.py` (down_revision `0008`): add the column with `server_default=sa.false()`.

### `DeliveryRecordRepo.create` (`core/config/repositories.py`)

Add `is_replay: bool = False` param, pass into `DeliveryRecord(...)`.

### Worker callbacks (`apps/worker/main.py`)

`_on_delivery_success` / `_on_delivery_failure` signatures UNCHANGED; read the flag from the payload:

```python
        await DeliveryRecordRepo(s).create(
            ..., is_replay=bool(payload.get("replay", False)),
        )
```

### API out (`apps/web/routers/delivery_records.py`)

`DeliveryRecordOut` gains `is_replay: bool` (read automatically via `from_attributes=True`).

## Component 6: Settings

`core/settings.py`:

```python
class ReplaySettings(BaseModel):
    max_replay_blocks: int = 10000
```

Wired into `Settings` as `replay: ReplaySettings = Field(default_factory=ReplaySettings)`. Env: `CHAIN_INDEXER_REPLAY__MAX_REPLAY_BLOCKS`.

## Component 7: UI

### Subscriptions (`web/src/pages/Subscriptions.tsx`)

- Inline **pause/resume** button per row: shows a Pause icon when `enabled`, a Play icon when not; clicking calls `POST /subscriptions/{id}/pause|resume` and invalidates the `subscriptions` query.
- Inline **replay** button per row → opens a small modal with `from_block` / `to_block` number inputs → `POST /subscriptions/{id}/replay`; on success shows a toast with the `request_id` ("已接受，后台处理中").

### Delivery Records (`web/src/pages/DeliveryRecords.tsx`)

- `DeliveryRecord` interface gains `is_replay: boolean`.
- Rows with `is_replay` show a purple "重放" badge next to the status chip.

## Error handling

- **Pause/resume on unknown sub**: 404.
- **Replay validation**: 404 unknown sub; 422 `to_block < from_block`; 422 span > `max_replay_blocks`.
- **Replay, chain not running**: worker logs `worker.replay_no_runner` and drops the request (known limitation of reusing the live runner). The API already returned 202 — the operator sees no error there; they'd notice no replay deliveries. (Acceptable for v1; a future status channel could surface this.)
- **Replay `to_block > tip`**: worker clamps to safe tip; if nothing remains, logs `replay_nothing_to_do`.
- **Replay block fetch failure**: a transient RPC error now fails over via the endpoint pool (sub-project A); a hard failure raises out of `_replay_evm`, caught by `replay()`'s try/except → logged `chain_runner.replay_failed`; the one-shot notifier is still stopped in `finally`.
- **Replay during shutdown**: `self._stop` aborts the loop; the notifier is stopped in `finally`.
- **Solana `fetch_block` returns None** (skipped slot): skip, continue.
- **Replayed delivery failure**: recorded as a `DeliveryRecord` with `status="failed"` AND `is_replay=True`; the existing retry/resolve controls apply.

## Testing

### Unit

- `core/notifier/test_payload.py` (or existing): `build_payload(replay=True)` adds `"replay": true`; default omits it.
- `core/notifier/test_notifier.py`: `dispatch(..., replay=True)` makes the channel receive a payload with `replay: true`; default does not.
- `apps/worker/test_replay_watcher.py`: `ReplayWatcher` subscribes and forwards messages to the callback; a raising callback does not kill the watcher loop.
- `apps/worker/test_replay_routing.py` (or extend worker tests): `_on_replay_request` routes to the matching runner's `replay`; with no runner for the chain it logs a warning and does not raise.
- `apps/worker/test_chain_runner_replay.py`: with a mocked adapter returning blocks over a range and a single-sub matcher, `replay` dispatches matched events with `replay=True` and **never calls `_cp.save`**; `to_block > safe_tip` clamps to safe_tip.

### Integration

- `tests/integration/test_subscription_pause_resume.py`: `POST /pause` then `/resume` flip `enabled` and publish `config_changed` (assert via a bus subscriber, mirroring `test_web_api.py`).
- `tests/integration/test_subscription_replay_api.py`: 404 unknown sub; 422 reversed range; 422 over-cap; 202 + `request_id` on valid; assert a `replay_request` message is published with the self-contained sub + channels.
- `tests/integration/test_repositories.py`: `DeliveryRecordRepo.create(is_replay=True)` round-trips.
- Migration: fresh-DB `create_all` (the `db` fixture) shows `delivery_records.is_replay`.

### Regression

- Existing notifier / payload / delivery_records / subscriptions / worker tests pass (the new `dispatch` and `build_payload` `replay` params default to `False`; callback signatures unchanged).
- Full `pytest -m "not e2e"` green (environmental Docker/testcontainer aside).

### Frontend

- `npm run build` succeeds with the new buttons + modal + badge.

## File-level change summary

| File | Change |
|------|--------|
| `core/notifier/payload.py` | `build_payload` +`replay` param. |
| `core/notifier/notifier.py` | `dispatch` +`replay` param → `build_payload`. |
| `core/config/models.py` | `DeliveryRecord` +`is_replay` column. |
| `core/config/repositories.py` | `DeliveryRecordRepo.create` +`is_replay`. |
| `core/settings.py` | +`ReplaySettings`. |
| `migrations/versions/0009_delivery_is_replay.py` | NEW. |
| `apps/worker/replay_watcher.py` | NEW. `ReplayWatcher`. |
| `apps/worker/main.py` | `_on_replay_request` routing; start/stop `ReplayWatcher`; callbacks read `payload["replay"]`. |
| `apps/worker/chain_runner.py` | +`replay`, `_replay_evm`, `_replay_solana`. |
| `apps/web/routers/subscriptions.py` | +pause/resume/replay endpoints. |
| `apps/web/routers/delivery_records.py` | `DeliveryRecordOut` +`is_replay`. |
| `apps/web/schemas.py` | +`ReplayRequest`. |
| `web/src/pages/Subscriptions.tsx` | pause/resume inline buttons + replay modal. |
| `web/src/pages/DeliveryRecords.tsx` | `is_replay` badge. |
| `tests/...` | per the Testing section. |

## Rollout

Single PR. One additive migration (`0009`, `is_replay` with server default `false` — safe for existing rows). No config required (10000-block cap default). The `replay_request` bus channel is new; old workers ignore it, new workers consume it. Replay only functions for chains with a running runner.

## Open questions

None blocking. Possible follow-ups (out of scope):

- A replay status/result channel so the API/UI can confirm completion or surface "no runner" failures.
- Replay for disabled chains via an ephemeral adapter job.
- A "replay-only" filter in the Delivery Records UI.
- Resume-with-gap-backfill as an explicit option.
