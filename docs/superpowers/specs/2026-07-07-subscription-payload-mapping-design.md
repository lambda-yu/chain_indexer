# Subscription Payload Mapping — Design

**Date:** 2026-07-07
**Status:** Draft
**Related:** `core/notifier/payload.py`, `core/config/models.py`, `apps/web/schemas.py`, `web/src/pages/Subscriptions.tsx`

## Problem

Downstream consumers (webhook receivers, MQ consumers, WS clients) currently receive a
fixed payload shape hard-coded in `core/notifier/payload.py:build_payload`:

```json
{
  "subscription_id": "...", "subscription_name": "...", "business_name": "...",
  "chain_id": "...",
  "event": {
    "kind": "...", "name": "...", "contract": "...",
    "block_number": 0, "block_hash": "0x...", "block_timestamp": 0,
    "tx_hash": "0x...", "tx_index": 0, "log_index": 0,
    "args": { "...": "..." }
  },
  "delivered_at": 0, "delivery_id": "..."
}
```

Different business systems need different field names and shapes. Today the only way to
adapt is to write an adapter on the consumer side. We want operators to configure the
payload shape per subscription from the web UI: **fetch a sample of the source payload,
edit the mapping, save.**

## Scope

- Per-subscription payload mapping (not per-channel, not per-pair).
- Field renaming with dotted paths, nested target structure, and constant injection.
- Web UI editor with source sample viewer, mapping table, and live preview of the
  mapped output.
- Backwards compatibility: subscriptions with no mapping continue to use the current
  default payload structure.

Out of scope:

- Type conversion / formatting (wei→decimal, bytes→hex, etc.). Deferred; the raw
  values already have sensible defaults from `_safe`.
- Per-channel overrides.
- Expression languages (JMESPath / JSONata / Jinja).
- `include_unmapped` passthrough — strict mode only.

## Design

### 1. Data model

New nullable column on `subscriptions`:

```python
payload_mapping: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
```

`NULL` means "use the default payload shape" — zero migration risk for existing rows.

Mapping shape:

```json
{
  "fields": [
    { "target": "txHash",       "source": "event.tx_hash" },
    { "target": "blockNo",      "source": "event.block_number" },
    { "target": "data.amount",  "source": "event.args.value" },
    { "target": "data.from",    "source": "event.args.from" },
    { "target": "source",       "const":  "chain-indexer" },
    { "target": "version",      "const":  1 }
  ]
}
```

**Rules:**

- Each `field` has exactly one of `source` (dotted path into source payload) or
  `const` (any JSON scalar / object / array). Enforced by pydantic.
- `target` supports dotted paths for nested output structure.
- `fields` is an **ordered** array; output key order follows the array (Python dicts
  preserve insertion order — matters for downstream debug tooling).
- Fields not listed are **not** emitted (strict mode).
- If a `source` path cannot be resolved, the target value is `null` and a warning is
  logged (delivery still proceeds).
- If two entries write to the same target (or one writes `data` as scalar and another
  writes `data.foo`), **last write wins** and a warning is logged.
- `delivery_id`, `delivered_at`, and (when replay) `replay` are **always** appended to
  the top level of the output regardless of the mapping — they are the delivery
  system's own metadata used by downstream idempotency and observability. If the
  mapping explicitly writes these targets, the user's value wins (respect operator
  intent).

**Alembic migration:** `migrations/versions/0011_subscription_payload_mapping.py`
adds one nullable JSON column. No backfill. Reversible.

### 2. Backend rendering

New module `core/notifier/payload_mapper.py`:

```python
def apply_mapping(source: dict, mapping: dict) -> tuple[dict, list[str]]:
    """Return (output, warnings). Never raises — all errors become warnings."""

def build_source_payload(*, event, subscription, replay=False) -> dict:
    """The current build_payload logic, extracted. Returns the un-mapped structure."""
```

Refactor `core/notifier/payload.py:build_payload`:

```python
def build_payload(*, event, subscription, replay=False) -> tuple[dict, dict]:
    """Return (source, delivery_payload). delivery_payload is what channels send."""
    source = build_source_payload(event=event, subscription=subscription, replay=replay)
    if subscription.payload_mapping:
        out, warnings = apply_mapping(source, subscription.payload_mapping)
        for w in warnings:
            log.warning("payload_mapper.warning", subscription_id=subscription.id, msg=w)
    else:
        out = dict(source)
    # Metadata fallback: only set if user didn't already claim these targets.
    out.setdefault("delivery_id", source["delivery_id"])
    out.setdefault("delivered_at", source["delivered_at"])
    if replay:
        out.setdefault("replay", True)
    return source, out
```

`SnapshotSubscription` gains `payload_mapping: dict | None = None`; `snapshot.py` passes
it through. `Notifier.dispatch` uses the two-tuple: source goes into `delivery_records`,
`out` goes to channels (see §6).

**`Notifier._send_one` signature change.** Today the method takes
`(ch, payload, sub_id, ch_id)` and uses the single `payload` for both `ch.send(...)`
and the `on_success` / `on_failure` callbacks. New signature:
`(ch, source, delivery_payload, sub_id, ch_id)`. `ch.send` receives `delivery_payload`;
callbacks receive `source` as the payload argument (so `event_payload` written to
`delivery_records` is the pre-mapping structure — this is what §6 relies on).
`payload.get("chain_id", "")` inside callbacks still works because `source` retains
`chain_id`.

**`apply_mapping` implementation:**

- Iterate `mapping["fields"]` in order.
- For each entry:
  - `const` → assign the value directly.
  - `source` → split path on `.` and walk `source_payload` segment by segment. Missing
    key or non-dict intermediate → resolved value is `None`, append warning.
- Split `target` on `.` and walk/create nested dicts via `setdefault({})`; the final
  segment gets the value.
- Conflict (a segment on the path is not a dict, or the final segment already exists) →
  overwrite and append warning.
- No exceptions escape. Schema validation lives in the API layer (§4.1).

### 3. Source sample endpoint

```
GET /api/subscriptions/{id}/payload_sample?refresh=0|1
→ 200 { "sample": {...}, "origin": "delivery" | "synthetic" }
```

Resolution order:

1. **Prefer real:** query `delivery_records` for the most recent `status = success` row
   for this subscription. Return its `event_payload` (after §6 this is the source
   structure). `origin = "delivery"`.
2. **Fallback synthetic:** call `build_source_payload` with a synthetic `Event`:
   - `native_transfer` / `token_transfer` — fixed field names, placeholder values
     (`"0x…"` for hex, `0` for numeric, `""` for str).
   - `event` / `call` — read the subscription's bound ABI (EVM ABI JSON or Solana IDL)
     for the specific `match_name`, produce `args` where each field is `"<type>"`
     (e.g. `"<uint256>"`, `"<address>"`, `"<pubkey>"`).
   - ABI missing / not bound → degrade to `args: {}`. All other envelope fields
     (`kind`, `name`, `contract`, `block_number`, `block_hash`, `block_timestamp`,
     `tx_hash`, `tx_index`, `log_index`) still receive the same
     placeholder values as the native/token path.

   Return `origin = "synthetic"`.

`?refresh=1` — semantically same query; primary purpose is a cache-buster for the
browser's GET cache when the user clicks "refresh sample" after a recent delivery
lands.

### 4. Web API

#### 4.1 CRUD extension

`apps/web/schemas.py`:

```python
class MappingField(BaseModel):
    target: str = Field(min_length=1, max_length=200)
    source: str | None = None
    const:  Any | None = None

    @model_validator(mode="after")
    def _xor(self):
        if (self.source is None) == (self.const is None):
            raise ValueError("field 必须且只能设置 source 或 const 之一")
        return self

class PayloadMapping(BaseModel):
    fields: list[MappingField] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def _validate(self):
        seen: set[str] = set()
        for f in self.fields:
            if f.target in seen:
                raise ValueError(f"target 重复: {f.target}")
            seen.add(f.target)
            if any(seg == "" for seg in f.target.split(".")):
                raise ValueError(f"target 路径非法: {f.target}")
        return self
```

`SubscriptionCreate` / `SubscriptionUpdate` / `SubscriptionRead` each gain:

```python
payload_mapping: PayloadMapping | None = None
```

Semantics of `null` on `PUT`: **clear the mapping** (set the column back to NULL).
Absence of the key on `PUT` (Pydantic exclude-unset): leave unchanged. This matches how
other nullable columns are handled by the existing repo/schemas.

#### 4.2 Sample endpoint

`GET /api/subscriptions/{id}/payload_sample?refresh=0|1` — as in §3. Placed in
`apps/web/routers/subscriptions.py` (path grouping).

#### 4.3 Preview endpoint

```
POST /api/subscriptions/{id}/payload_preview
  body: { "mapping": {...},        // PayloadMapping shape
          "sample":  {...} | null } // optional; falls back to /payload_sample logic
  →     { "output": {...},
          "warnings": ["..."] }
```

Pure function: no DB writes, no channel side effects. Runs the same `apply_mapping`
that production dispatch uses. When `sample` is omitted / `null`, the endpoint runs
the same resolution as `GET /payload_sample` (real delivery first, synthetic
fallback) so the preview reflects a realistic downstream view even before the user
has ever received a delivery.

### 5. Frontend UI

Extend `web/src/pages/Subscriptions.tsx:SubForm`. No new route/page.

Below the "绑定通知渠道" section, add a collapsible **投递数据映射（可选）** panel.
Default collapsed with a hint: "未配置时使用默认 payload 结构".

Expanded layout:

```
┌─ 投递数据映射 ────────────────────────────────────┐
│ [ 从源字段一键生成模板 ] [ 刷新样本 ] [ 清空 ]     │
│                                                    │
│ 源 payload (只读)          目标 payload (预览)     │
│ ┌────────────────────┐   ┌────────────────────┐   │
│ │ event.tx_hash      │   │ txHash             │   │
│ │   "0x…"           │   │   "0x…"           │   │
│ │ event.args.value   │   │ data.amount        │   │
│ │   "1000"          │   │   "1000"          │   │
│ └────────────────────┘   └────────────────────┘   │
│                                                    │
│ 映射编辑（每行一条）                               │
│  target             mode      source / const       │
│  [txHash        ]  [源▼]    [event.tx_hash    ][×]│
│  [data.amount   ]  [源▼]    [event.args.value ][×]│
│  [source        ]  [常量▼]  ["chain-indexer"  ][×]│
│  [+ 添加一行]                                      │
│                                                    │
│ origin: delivery (来自最近一次真实推送)             │
└────────────────────────────────────────────────────┘
```

Key interactions:

- **源 payload 面板** — pull once from `GET /payload_sample`; render as a flat list of
  `dotted-path : value` rows; each row has a "→ 复制到映射" button that appends a
  mapping row `target=<last-segment>, source=<full-path>`.
- **目标 payload 预览面板** — on every mapping change, debounce 300 ms then call
  `POST /payload_preview`; render `output` flattened, with warnings badged in.
- **从源字段一键生成模板** — walk all leaf paths of the sample and create a mapping
  row per leaf (`target = last segment`, `source = full path`). If two paths share
  the same last segment, suffix `_2`, `_3`, etc. on collisions.
- **刷新样本** — hits `GET /payload_sample?refresh=1`; only the source panel updates.
- **模式切换 (source ↔ const)** — per-row dropdown. In const mode the value input
  tries `JSON.parse` first (so `1`, `true`, `[1,2]` become typed); if parsing fails,
  the raw string is used.
- **Local validation** — empty target / duplicate target / both-or-neither-of
  source/const → row turns red; submit disabled while any row is red.
- **Submit** — includes `payload_mapping` in the existing `POST /subscriptions` or
  `PUT /subscriptions/{id}` body. Empty (zero rows or explicitly cleared) → send
  `null` to clear.

TypeScript types live alongside the existing `Sub` interface at the top of the file.

### 6. `delivery_records` storage change

`delivery_records.event_payload` today stores the payload sent to channels. With
mapping enabled, that could be lossy (strict mode drops unmapped fields), which
breaks:

- The `payload_sample` endpoint's "prefer real" path.
- Debugging: operators need to see the raw event that triggered a delivery.

**Change:** in `Notifier.dispatch`, keep two values per event/sub:

```python
source, delivery_payload = build_payload(event=event, subscription=sub, replay=replay)
# Send:
await ch.send(delivery_payload)
# Persist (via existing on_success / on_failure callbacks):
event_payload_for_records = source
```

- Channels receive the mapped payload (unchanged behavior for consumers).
- `delivery_records.event_payload` stores the **source** (pre-mapping) structure.
- No historical migration: existing rows were written under the old logic where
  mapping did not exist, so they are already source-shaped by construction.

**UI: preview downstream payload from a record.** In `DeliveryRecords.tsx`, add a
collapsible "预览下游实际 payload" block on each row that pulls the subscription's
current mapping and runs it against the stored source via a new endpoint:

```
GET /api/delivery_records/{id}/downstream_preview
→ 200 { "output": {...}, "warnings": [...], "mapping_source": "current" }
```

`mapping_source: "current"` clarifies that this reflects the subscription's mapping
as it exists now, not necessarily the mapping in effect at delivery time. (We do not
version mappings — YAGNI; if a downstream disputes an old payload, use the source.)

**Replay:** `notifier.dispatch` is shared, so replay automatically picks up the
subscription's current mapping. This is the expected behavior — replays test the
current pipeline, not archaeology.

**Preview response field naming.** The response uses `mapping_source: "current"`
(not `mapping_version`) to avoid implying we version mappings.

### 7. Testing

**Unit (`tests/unit/`):**

- `test_payload_mapper.py`
  - Plain rename.
  - Nested target (`data.amount` → nested dict).
  - Nested source (`event.args.value`).
  - Constant injection: string / number / dict / list.
  - Missing source path → `None` + warning.
  - Target conflict (scalar-then-nested; duplicate final) → overwrite + warning.
  - Metadata fallback: `delivery_id` / `delivered_at` added when not in mapping;
    user's value kept when in mapping.

- `test_payload_schema.py`
  - source + const both set → 422.
  - Duplicate target → 422.
  - Empty-segment target (`"a..b"` / `".a"` / `"a."`) → 422.
  - `fields = []` → 422.

**Integration (`tests/integration/`):**

- `test_subscription_payload_mapping.py`
  - CRUD round-trip: create with mapping → read back → PUT to modify → PUT `null` to
    clear.
  - `GET /payload_sample`: no delivery → synthetic + correct `origin`; after a
    delivery is written → real + correct `origin`.
  - `POST /payload_preview`: given mapping + sample → correct output + warnings.
  - End-to-end via `notifier.dispatch` (with a fake HTTP channel):
    - Subscription with mapping → HTTP body is mapped.
    - `delivery_records.event_payload` is source structure, not mapped.
    - `delivery_id` / `delivered_at` present in HTTP body.
  - Regression: subscription with `payload_mapping = NULL` behaves identically to
    pre-change (byte-for-byte snapshot of HTTP body).

**E2E:** deferred. Integration coverage of the API is enough; UI is thin over it.

### 8. Migration and rollout

- Alembic `0011_subscription_payload_mapping.py`: add nullable JSON column. SQLite and
  Postgres both handle `ADD COLUMN … NULL` with zero downtime; reversible.
- No data backfill.
- No feature flag: `NULL` mapping is the off state.
- No breaking API change: old clients that don't send `payload_mapping` behave as
  before.

## Alternatives considered

- **Per-channel mapping.** Rejected: user chose subscription-level. Channels stay
  "dumb pipes" — simpler mental model.
- **Per-(subscription, channel) mapping.** Rejected: too much config surface for
  marginal use; can be added later without breaking this design (payload_mapping
  moves to the `subscription_channels` bridge, subscription-level becomes the
  default fallback).
- **JMESPath / JSONata.** Rejected: user chose the field-map approach for UI-first
  editing. Can be reintroduced later as a `expression` field on `MappingField` if
  power users demand it.
- **`include_unmapped: true` passthrough.** Deferred (user did not select). Adding
  it later is a strictly additive change to `PayloadMapping`.
- **Store mapped payload in `delivery_records`.** Rejected because it loses fields
  in strict mode and breaks the "learn from real deliveries" loop for the sample
  endpoint. Downstream preview via `mapping_version: current` mitigates the
  "what did they actually receive?" question.

## Risks

- **`delivery_records` semantic drift.** Historical rows: pre-change behavior
  effectively equals post-change behavior (no mapping existed), so no drift there.
  Rows written between deploy and first mapping config on a given subscription: same.
  Only concern: operators who scripted against `event_payload` assuming it's what
  went out on the wire. Mitigation: release note calling this out; the downstream
  preview endpoint reproduces the wire payload on demand.

- **Mapping breaks a downstream silently.** A user editing a mapping could drop
  a field a downstream depends on. Mitigation: the preview panel makes the change
  visible before save, and warnings surface missing source paths. We do not
  attempt server-side schema validation of "what the downstream needs" — that's
  their contract.

- **Pydantic-side validation gap.** We validate structure but not that `source`
  paths resolve for the subscription's specific `match_kind`+ABI. A typo silently
  produces `null` at runtime. Mitigation: warnings in logs + preview panel;
  acceptable because the source shape is not stable enough to validate up front
  (differs per event/ABI).

## Deliverables checklist

- [ ] `migrations/versions/0011_subscription_payload_mapping.py`
- [ ] `core/notifier/payload_mapper.py` (new)
- [ ] `core/notifier/payload.py` refactor: extract `build_source_payload`, change
  `build_payload` return signature to `(source, delivery)`
- [ ] `core/notifier/notifier.py`: use the two-tuple, persist source; update
  `_send_one` to `(ch, source, delivery_payload, sub_id, ch_id)` and pass `source`
  into the `on_success` / `on_failure` callbacks
- [ ] `core/config/models.py`: `payload_mapping` column
- [ ] `core/config/snapshot.py`: `SnapshotSubscription.payload_mapping`
- [ ] `apps/web/schemas.py`: `MappingField`, `PayloadMapping`, `SubscriptionCreate/Update/Read`
- [ ] `apps/web/routers/subscriptions.py`: `GET /payload_sample`, `POST /payload_preview`
- [ ] `apps/web/routers/delivery_records.py`: `GET /{id}/downstream_preview`
- [ ] `web/src/pages/Subscriptions.tsx`: mapping editor in `SubForm`
- [ ] `web/src/pages/DeliveryRecords.tsx`: "预览下游实际 payload" block
- [ ] Unit + integration tests as in §7
- [ ] Release note re: `delivery_records` semantic change