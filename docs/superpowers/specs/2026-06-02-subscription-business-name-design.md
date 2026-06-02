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
                  envelope = {subscription_id, subscription_name, ...}
                  if subscription.business_name:
                      envelope["business_name"] = subscription.business_name
              ─────────────────────────────────────────────
API:          SubscriptionCreate.business_name: str | None
              SubscriptionOut.business_name:    str | None
              routers/subscriptions.py: passthrough on create + update
              ─────────────────────────────────────────────
UI:           Subscriptions.tsx: form input + list column
              types: Subscription.business_name?: string | null
```

The field is plumbed through the existing layered path with no new modules. No new endpoints, no new background jobs, no schema relationships.

## Data flow

1. Operator creates or edits a subscription via API/UI, optionally setting `business_name`.
2. `SubscriptionRepo` writes the value; `bump_and_publish("config_changed")` fires (existing path).
3. Worker's `ConfigWatcher` rebuilds the snapshot; `SnapshotSubscription.business_name` is populated.
4. When the matcher hits an event for that subscription, `build_payload` reads `subscription.business_name` from the snapshot copy and includes it in the envelope iff non-null.
5. Each bound channel sends the same envelope; `delivery_records.event_payload` persists the envelope JSON verbatim (so replays from a record reproduce the same `business_name`).

## Components

### `core/config/models.py` — `Subscription`

Add column at the end of the existing column list (no reorder):

```python
business_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
```

No index, no server_default. Existing rows become NULL on upgrade.

### `migrations/versions/<rev>_add_business_name_to_subscriptions.py`

Single migration:

```python
def upgrade() -> None:
    op.add_column(
        "subscriptions",
        sa.Column("business_name", sa.String(length=255), nullable=True),
    )

def downgrade() -> None:
    op.drop_column("subscriptions", "business_name")
```

SQLite-compatible (op.add_column with nullable column works without batch_alter_table).

### `core/config/snapshot.py`

`SnapshotSubscription` gains `business_name: str | None = None` (placed after `start_block` to keep defaulted fields contiguous and avoid breaking positional construction in tests). `load_snapshot()` adds `business_name=sub.business_name` to the constructor call.

### `core/notifier/payload.py`

After the base payload dict, before the `replay` check:

```python
if subscription.business_name:
    payload["business_name"] = subscription.business_name
```

Position in the envelope: between `subscription_name` and `chain_id` if emitted (CPython dict preserves insertion order). Falsy values (empty string, None) are not emitted — empty strings should not occur because the API/UI never accept them, but coercing to omit is defensive.

### `apps/web/schemas.py`

```python
class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    business_name: str | None = Field(default=None, max_length=255)
    ...

class SubscriptionOut(BaseModel):
    ...
    name: str
    business_name: str | None
    ...
```

`SubscriptionDetail` inherits from `SubscriptionOut` and gets it automatically.

### `apps/web/routers/subscriptions.py`

`create_subscription` and `update_subscription` thread `business_name` into the repo write (along with the other passthrough fields). No router signature change beyond what flows through the schema.

### `web/src/pages/Subscriptions.tsx`

- Add a "业务名称" column to the list table; render `—` when value is null/empty.
- Add an optional text input to the create/edit form (label "业务名称"; placeholder "用于下游识别业务（可选）"; maxLength 255).
- Update the TypeScript `Subscription` type (`web/src/api/types.ts` or wherever it lives) to include `business_name?: string | null`.

## Error handling

- Pydantic enforces `max_length=255`. Over-length input → standard 422.
- `null` and missing field are equivalent on the API surface (Pydantic default).
- DB constraint mirrors max length (`VARCHAR(255)`). Bypasses (e.g., direct SQL) would truncate at insert time on most engines, accepted as a non-concern.
- No new failure modes in the notifier path: a missing `business_name` simply omits the key.

## Backward compatibility

- **Existing subscribers** continue to receive envelopes without a `business_name` field as long as no subscription sets one. This matches the current envelope.
- **Downstream consumers** that already key on `subscription_id` are unaffected. Adding `business_name` is a strictly additive change.
- **Existing tests** that assert exact envelope key sets must be reviewed — if any assert "envelope == {...exact dict...}" they'll need to handle the optional key (in practice tests will set business_name=None on their test subscriptions, so the field stays absent).
- **DB downgrade** is a pure `drop_column` — safe as long as the application code does not still reference `business_name` (i.e., downgrade migration + code rollback are paired).

## Testing

| Layer | Test |
|---|---|
| `tests/notifier/test_payload.py` | (a) `build_payload` with `business_name="x"` → envelope contains `"business_name": "x"`. (b) With `business_name=None` → envelope has no `business_name` key. (c) With `business_name=""` → no key (defensive). |
| `tests/web/test_subscriptions_api.py` (or nearest equivalent) | (a) POST with `business_name` → GET returns same value. (b) POST without it → GET returns `null`. (c) PUT updates the value, including setting it back to `null`. (d) Over-length input → 422. |
| `tests/config/test_snapshot.py` | `load_snapshot` populates `SnapshotSubscription.business_name` from the DB row (both null and non-null cases). |
| Migration smoke | `alembic upgrade head` → `downgrade -1` → `upgrade head` round-trip if the project already has this pattern; otherwise just run `upgrade head` against a fresh DB in CI as today. |

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