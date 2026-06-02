# Subscription `business_name` — Design

**Date**: 2026-06-02
**Status**: Draft
**Scope**: Add an optional, free-text `business_name` field to subscriptions so the notification envelope can carry a downstream business identifier. CRUD + UI form/list exposure; no filtering, no separate entity.
**Milestone**: post-m5 follow-up (small enhancement)

## Background

The notification envelope built by `core/notifier/payload.py::build_payload` currently exposes who the delivery is for via `subscription_id` and `subscription_name`. Downstream consumers that receive events for several distinct business lines from the same Chain Indexer instance have no stable, human-meaningful key to route on — they have to maintain their own `subscription_id → business` lookup table out-of-band.

Adding a free-text business label on each subscription closes that gap with a one-column schema change and a single envelope field. The label travels with every delivery (and is persisted as part of the frozen `event_payload` in `delivery_records`), so it survives replays and is auditable historically.

## Goals

- New nullable string column `subscriptions.business_name` (VARCHAR(255)).
- `SnapshotSubscription` carries `business_name`; `load_snapshot` populates it.
- `build_payload()` emits a top-level `business_name` field on the envelope — **only when non-null** (same precedent as `replay`).
- CRUD API: `SubscriptionCreate` accepts it, `SubscriptionOut`/`SubscriptionDetail` return it; create and update endpoints persist it.
- UI: Subscriptions list shows a "业务名称" column (empty rendered as `—`); create/edit form exposes an optional input.
- Existing subscriptions and existing downstream consumers continue to work unchanged.

## Non-goals

- **No separate `businesses` table / `business_id` FK.** A free-text string is enough today; promoting it to an entity is a future migration if real lifecycle (rename, statistics, permissions) emerges.
- **No filter by business_name** in any list endpoint or UI page. Listed as an explicitly-considered extension that did not make this scope.
- **No display in Delivery Records / Event Stream / Dashboard.** The label is in `event_payload` JSON if a user needs it via record detail; no separate column yet.
- **No backfill.** Existing rows stay NULL. Migration is one `add_column` / one `drop_column`.
- **No validation beyond max length.** Whitespace, casing, uniqueness, and character set are caller's concern.
- **No nesting / labels map.** A flat top-level field is chosen for parity with `subscription_name`.

## Architecture

```
DB:           subscriptions.business_name (VARCHAR(255) NULL)
              ─────────────────────────────────────────────
Snapshot:     SnapshotSubscription.business_name: str | None
              (populated by load_snapshot from sub.business_name)
              ─────────────────────────────────────────────
Payload:      build_payload(event, subscription, replay):
                  envelope = {subscription_id, subscription_name,
                              business_name (if set), chain_id, ...}
              ─────────────────────────────────────────────
API:          SubscriptionCreate.business_name: str | None
              SubscriptionOut.business_name:    str | None
              routers/subscriptions.py:
                - create_subscription: add business_name kwarg to repo.create
                - update_subscription: add business_name kwarg to repo.update
                - replay_subscription: include business_name in the inline
                  `subscription` dict published on the replay_request bus msg
              ─────────────────────────────────────────────
UI:           Subscriptions.tsx: form input + list column
              types: extend the inline `interface Sub` with
                     `business_name: string | null`
```

The field is plumbed through the existing layered path with no new modules. No new endpoints, no new background jobs, no schema relationships.

## Data flow

1. Operator creates or edits a subscription via API/UI, optionally setting `business_name`.
2. `SubscriptionRepo` writes the value (Pydantic strips whitespace and coerces empty string to `None` at the API layer — see Error handling); `bump_and_publish("config_changed")` fires (existing path).
3. Worker's `ConfigWatcher` rebuilds the snapshot; `SnapshotSubscription.business_name` is populated.
4. When the matcher hits an event for that subscription, `build_payload` reads `subscription.business_name` from the snapshot copy and includes it in the envelope iff non-null.
5. Each bound channel sends the same envelope; `delivery_records.event_payload` persists the envelope JSON verbatim (so replays from a record reproduce the same `business_name`).
6. For ranged replay (`POST /subscriptions/{id}/replay`), the router publishes a self-contained `replay_request` bus message that embeds the subscription inline. `business_name` must be present in that inline dict so the worker's reconstructed `SnapshotSubscription` carries the label into `build_payload` for replayed deliveries.

## Components

### `core/config/models.py` — `Subscription`

Add column at the end of the existing column list (no reorder):

```python
business_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
```

No index, no server_default. Existing rows become NULL on upgrade.

### `migrations/versions/0010_subscription_business_name.py`

Follow the project's existing SQLite-compatible pattern (`op.batch_alter_table` for every DDL, as in `0004_subscription_start_block.py`):

```python
"""add business_name to subscriptions

Revision ID: 0010
Revises: 0009
"""
import sqlalchemy as sa
from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.add_column(sa.Column("business_name", sa.String(length=255), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("subscriptions") as batch:
        batch.drop_column("business_name")
```

Bare `op.add_column` / `op.drop_column` would not be SQLite-safe under Alembic's migration model — `batch_alter_table` is mandatory here.

### `core/config/snapshot.py`

`SnapshotSubscription` gains `business_name: str | None = None` (placed after `start_block` to keep defaulted fields contiguous and avoid breaking positional construction in tests). `load_snapshot()` adds `business_name=sub.business_name` to the constructor call.

### `core/notifier/payload.py`

Insert `business_name` **inside the dict literal** (between `subscription_name` and `chain_id`, conditional via a dict expansion or — simpler — insert into the dict literal and rely on Python dropping `None`-valued keys via a small helper). Concrete approach:

```python
def build_payload(*, event, subscription, replay=False):
    payload = {
        "subscription_id": subscription.id,
        "subscription_name": subscription.name,
    }
    if subscription.business_name:
        payload["business_name"] = subscription.business_name
    payload["chain_id"] = event.chain_id
    payload["event"] = { ... }   # existing event sub-dict, unchanged
    payload["delivered_at"] = _now_unix()
    payload["delivery_id"] = _gen_id()
    if replay:
        payload["replay"] = True
    return payload
```

This places `business_name` after `subscription_name` and before `chain_id` in serialized output (CPython dict preserves insertion order). Falsy values (None, empty string) are not emitted — empty strings are also coerced to `None` at the schema layer, but the conditional here is defensive.

### `apps/web/schemas.py`

```python
class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    business_name: str | None = Field(default=None, max_length=255)
    chain_id: str
    ...

    @field_validator("business_name", mode="before")
    @classmethod
    def _normalize_business_name(cls, v: str | None) -> str | None:
        # Strip whitespace and coerce empty string to None so the envelope's
        # "omit if falsy" logic does not see "" as a present-but-blank label.
        if v is None:
            return None
        v = v.strip()
        return v or None

class SubscriptionOut(BaseModel):
    ...
    name: str
    business_name: str | None
    ...
```

`SubscriptionDetail` inherits from `SubscriptionOut` and gets it automatically — both `GET /subscriptions` (list) and `GET /subscriptions/{id}` (detail) serialize the field with no extra change.

### `apps/web/routers/subscriptions.py`

Three call sites need an explicit `business_name=payload.business_name` (the create/update kwargs are enumerated, not splatted):

- `create_subscription` → `SubscriptionRepo.create(..., business_name=payload.business_name)`
- `update_subscription` → `SubscriptionRepo.update(sub_id, ..., business_name=payload.business_name)` (so setting the field to `None` clears it)
- `replay_subscription` → the inline `"subscription": {...}` dict embedded in the published `replay_request` bus message must include `"business_name": sub.business_name`. Without this the replayed deliveries silently lose the label even though the envelope shape supports it.

`SubscriptionRepo.update` already accepts arbitrary `**fields`, so no repo change is needed for update. `SubscriptionRepo.create` enumerates its kwargs and needs `business_name: str | None = None` added to its signature alongside the existing fields.

### `web/src/pages/Subscriptions.tsx`

The TS `Subscription` type is not centralized — it lives as the inline `interface Sub` near the top of `Subscriptions.tsx`. Updates:

- Extend `interface Sub` with `business_name: string | null`.
- Add a "业务名称" column to the list table (between "名称" and "链 ID" feels natural); render `s.business_name ?? '—'`.
- Add an optional text input to the create/edit form (label "业务名称"; placeholder "用于下游识别业务（可选）"; `maxLength={255}`); include it in the form submission body as `business_name: fd.get('business_name') || null`.

Other consumers (`BlockTest.tsx` defines its own local `Sub` type for the rule-test panel) do not need the new field; they exercise matching, not the envelope.

## Error handling

- Pydantic enforces `max_length=255`. Over-length input → standard 422.
- `null`, missing field, empty string, and whitespace-only string are all normalized to `None` by the `_normalize_business_name` validator. Downstream code (snapshot, payload, persistence) only ever sees `None` or a non-empty trimmed string.
- DB constraint mirrors max length (`VARCHAR(255)`). Direct SQL bypasses would truncate at insert time on most engines, accepted as a non-concern.
- No new failure modes in the notifier path: a missing or `None` `business_name` simply omits the key from the envelope.

## Backward compatibility

- **Existing subscribers** continue to receive envelopes without a `business_name` field as long as no subscription sets one. This matches the current envelope.
- **Downstream consumers** that already key on `subscription_id` are unaffected. Adding `business_name` is a strictly additive change.
- **Existing tests** that assert exact envelope key sets must be reviewed — if any assert "envelope == {...exact dict...}" they'll need to handle the optional key (in practice tests will set business_name=None on their test subscriptions, so the field stays absent).
- **DB downgrade** is a pure `drop_column` — safe as long as the application code does not still reference `business_name` (i.e., downgrade migration + code rollback are paired).
- **Persisted `event_payload` JSON in `delivery_records`** is forward-compatible only: records written before this change will not contain `business_name`, and re-delivering them via the record-detail replay path produces an envelope without the label. New deliveries (and ranged replays driven from the live subscription) populate the field as expected.

## Testing

Test layout uses `tests/unit/` and `tests/integration/`; the existing files cover the layers we touch:

| Layer | File | Test |
|---|---|---|
| Payload | `tests/unit/test_payload.py` (extend) | (a) `build_payload` with `business_name="x"` → envelope contains `"business_name": "x"`. (b) With `business_name=None` → envelope has no `business_name` key. (c) Key ordering: assert `business_name` appears between `subscription_name` and `chain_id` in the serialized dict. |
| Snapshot | `tests/unit/test_snapshot.py` (extend) | `load_snapshot` populates `SnapshotSubscription.business_name` from the DB row (both null and non-null cases). |
| API | `tests/unit/test_web_subscriptions.py` (extend) | (a) POST with `business_name` → GET returns same value. (b) POST without it → GET returns `null`. (c) POST with `"   "` (whitespace) → GET returns `null` (normalization). (d) PUT updates the value, including setting it back to `null`. (e) Over-length input → 422. |
| Replay | `tests/integration/` (extend the existing replay test, whichever exercises `POST /subscriptions/{id}/replay`) | The published `replay_request` payload's inline `subscription` dict carries `business_name`; an end-to-end replay produces deliveries whose payload contains the label. |
| Migration | Same CI path as today (`alembic upgrade head` against fresh DB) — no extra round-trip step unless one is already standard. |

No e2e tests. The change does not affect chain ingestion, matching semantics, or delivery wiring, so the marginal value of an e2e run does not justify the cost.

No frontend unit tests (project has none for React today; do not introduce a new test layer for this).

## Open questions

None at design time — all four axes (data model shape, required/optional, exposure scope, envelope shape) settled to the minimal-A option during brainstorming.

## Out-of-scope follow-ups (deferred)

If real demand surfaces, any of these can land as separate tickets without re-doing this work:

- Promote `business_name` to a foreign key into a `businesses` table (gives rename, lifecycle, statistics).
- Add a `?business_name=` filter on `GET /subscriptions` and the Subscriptions UI list.
- Surface `business_name` as a column in Delivery Records / Event Stream pages.
- Group Dashboard counters by business.