# Chain Indexer — M2 Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the spec §3 parsing and channel gaps left by M1: ship all four parser kinds (native / standard token / ABI event / ABI call) on EVM, the same four on Solana via a new `SolanaAdapter`, plus two additional notification channels (Redis Streams MQ, WebSocket fanout), and clear the M1 follow-ups list.

**Architecture:** Additive on the M1 pipeline backbone — `adapter → confirmation/commitment → parser pipeline → matcher → notifier` is unchanged. New cross-cutting modules: `core/abi/` (shared decoder cache for ABI/IDL parsers) and `core/chains/solana.py` (RPC + commitment-level adapter, no `ConfirmationBuffer`). `ParserPipeline` splits into `EvmParserPipeline` and `SolanaParserPipeline`, both yielding the same uniform `Event`. `Channel` ABC gains `__init_subclass__` strictness.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic v2, SQLAlchemy 2.x async, Alembic, web3.py v6 async, **eth-abi** (transitive of web3), **solders + httpx** for Solana RPC, **borsh-construct** for Anchor IDL, structlog, pytest + pytest-asyncio, testcontainers, anvil (foundry), `solana-test-validator` for Solana E2E.

**Spec:** `docs/superpowers/specs/2026-05-26-m2-design.md`

**Builds on:** `m1-complete` tag (commit on `main`). M1's pipeline is intact and additive only — every existing M1 test must remain green after each chunk.

**Scope explicitly out of M2** (covered by later milestones, do NOT add):
- Reorg E2E driving real Anvil snapshot rollbacks (M3+; `ConfirmationBuffer` unit tests stay authoritative).
- Anchor IDL **call** parsing (deferred; event parser handles 80% of observability use cases).
- Additional MQ brokers — RabbitMQ, Kafka, NATS (extend `Channel` ABC in M3 when a user needs them).
- Auth on management API and WS server (no-auth design is a parent-spec §2 non-goal).
- WebSocket replay / backfill (`/ws` is live-only).
- Web UI static assets (separate milestone).
- Per-chain horizontal sharding.

**M2 segment tags:**
- After chunk 9 (EVM segment complete): `git tag m2-evm-complete`
- After chunk 14 (Solana segment complete): `git tag m2-complete`

If Solana slips, the EVM segment ships as M2.5 with `m2-evm-complete` as the release point.

---

## File Structure (M2 delta on top of M1)

M2 adds modules; it does NOT restructure M1. New files are marked `[+]`; modified files are marked `[~]`; the rest is M1 inheritance.

```
chain_indexer/
├── pyproject.toml                           [~] add: solders, borsh-construct, eth-abi (pinned)
├── alembic.ini
├── Makefile                                 [~] add: test-solana target (E2E with solana-test-validator)
│
├── core/
│   ├── settings.py
│   ├── logging.py
│   │
│   ├── config/
│   │   ├── db.py
│   │   ├── models.py                        [~] add: chains.commitment (nullable str)
│   │   ├── repositories.py                  [~] add: AbiRepo CRUD
│   │   └── snapshot.py                      [~] add: SnapshotAbi; load_snapshot reads abis table
│   │
│   ├── bus/
│   │   └── redis_bus.py
│   │
│   ├── abi/                                 [+] new package
│   │   ├── __init__.py                      [+]
│   │   ├── registry.py                      [+] AbiRegistry (LRU cache, refresh on config_changed)
│   │   ├── decoder.py                       [+] eth-abi event/call + borsh-construct IDL decoders
│   │   └── errors.py                        [+] AbiNotFound, DecodeFailed
│   │
│   ├── chains/
│   │   ├── types.py                         [~] add: SolanaBlock, SolanaTransaction, SolanaInstruction
│   │   ├── adapter.py                       [~] add: SolanaChainAdapter Protocol (distinct from ChainAdapter)
│   │   ├── evm.py                           [~] wire poll_interval_ms ctor arg
│   │   ├── confirmation_buffer.py
│   │   └── solana.py                        [+] SolanaAdapter (solders + httpx, commitment-bound poll)
│   │
│   ├── parser/
│   │   ├── event.py                         [~] EventKind.log → event rename
│   │   ├── base.py                          [~] add: SolanaParser Protocol (vs EvmParser)
│   │   ├── native.py                        [~] docstring fix (retract Solana claim)
│   │   ├── pipeline.py                      [~] rename ParserPipeline → EvmParserPipeline; add SolanaParserPipeline
│   │   ├── erc20.py                         [+] Erc20TransferParser
│   │   ├── abi_event.py                     [+] AbiEventParser (topic0 lookup)
│   │   ├── abi_call.py                      [+] AbiCallParser (4-byte selector)
│   │   ├── sol_native.py                    [+] SolNativeTransferParser
│   │   ├── spl_transfer.py                  [+] SplTransferParser (legacy + 2022)
│   │   └── anchor_event.py                  [+] AnchorIdlEventParser
│   │
│   ├── matcher/
│   │   ├── filters.py                       (unchanged — M1 grammar stays)
│   │   └── matcher.py                       (unchanged)
│   │
│   └── notifier/
│       ├── channel.py                       [~] add __init_subclass__ enforcement
│       ├── payload.py                       (unchanged)
│       ├── retry.py                         (unchanged — reused by new channels)
│       ├── http.py
│       ├── notifier.py                      [~] _sem lazy event-loop binding
│       ├── redis_streams.py                 [+] RedisStreamsChannel
│       └── websocket.py                     [+] WebSocketChannel (publishes to Redis pub/sub)
│
├── apps/
│   ├── web/
│   │   ├── main.py                          [~] mount /api/abis, /ws routers
│   │   ├── deps.py
│   │   ├── schemas.py                       [~] add Abi schemas, tighten arg_filters, add commitment
│   │   └── routers/
│   │       ├── chains.py                    [~] accept commitment for solana kind
│   │       ├── subscriptions.py
│   │       ├── channels.py                  [~] register redis_streams + websocket configs
│   │       ├── abis.py                      [+] /api/abis CRUD
│   │       └── ws.py                        [+] /ws?channel_id= server
│   └── worker/
│       ├── main.py                          [~] solana factory branch; parallel shutdown; signal docstring
│       └── chain_runner.py                  [~] pipeline_kind selection (evm vs solana)
│
├── migrations/
│   └── versions/
│       ├── 0001_initial.py
│       └── 0002_solana_commitment.py        [+] add chains.commitment nullable column
│
├── scripts/
│   └── validate_arg_filters.py              [+] one-shot scanner (chunk 9)
│
└── tests/
    ├── conftest.py
    ├── unit/
    │   ├── (M1 tests unchanged)
    │   ├── test_abi_registry.py             [+]
    │   ├── test_abi_decoder.py              [+]
    │   ├── test_erc20_parser.py             [+]
    │   ├── test_abi_event_parser.py         [+]
    │   ├── test_abi_call_parser.py          [+]
    │   ├── test_channel_init_subclass.py    [+]
    │   ├── test_redis_streams_channel.py    [+] unit-level (mocked redis)
    │   ├── test_websocket_channel.py        [+] unit-level
    │   ├── test_sol_native_parser.py        [+]
    │   ├── test_spl_transfer_parser.py      [+]
    │   ├── test_anchor_event_parser.py      [+]
    │   ├── test_notifier_sem_lazy.py        [+] chunk-1 followup
    │   └── test_pipeline_split.py           [+] EvmParserPipeline + SolanaParserPipeline
    ├── integration/
    │   ├── test_abi_api.py                  [+] CRUD + reload
    │   ├── test_redis_streams_channel.py    [+] testcontainers Redis round-trip
    │   ├── test_ws_fanout.py                [+] in-proc FastAPI + WS client
    │   └── test_solana_adapter.py           [+] solana-test-validator
    └── e2e/
        ├── conftest.py                      [~] add session-scoped solana_validator fixture; deterministic worker-ready signal
        ├── test_native_transfer_e2e.py      [~] replace asyncio.sleep(1.0) with ready barrier
        ├── test_evm_erc20_e2e.py            [+] deploy ERC-20 on Anvil, transfer → webhook
        └── test_solana_e2e.py               [+] native SOL + SPL → webhook
```

**Module responsibilities (one-liners, new modules only):**

- `core/abi/registry.py` — `AbiRegistry` loads `abis.body` on demand, caches compiled decoders by `(abi_id, key)`, refreshes when `config_changed` fires.
- `core/abi/decoder.py` — Build & cache `eth-abi` event/call codecs (EVM) and `borsh-construct` schemas (Anchor IDL).
- `core/abi/errors.py` — `AbiNotFound`, `DecodeFailed` exceptions.
- `core/chains/solana.py` — `SolanaAdapter` (RPC + commitment level), `SolanaBlock` builder, missed-slot vs RPC-error disambiguation.
- `core/parser/erc20.py` — Detect `Transfer(address,address,uint256)` topic, decode `{from,to,value}`.
- `core/parser/abi_event.py` — Topic0-indexed event decoder via `AbiRegistry`.
- `core/parser/abi_call.py` — 4-byte-selector-indexed call decoder via `AbiRegistry`.
- `core/parser/sol_native.py` — System program transfer extractor.
- `core/parser/spl_transfer.py` — Legacy + 2022 Token Program transfer extractor.
- `core/parser/anchor_event.py` — Discriminator-indexed Anchor IDL event decoder, program-id-scoped.
- `core/notifier/redis_streams.py` — `RedisStreamsChannel` (XADD MAXLEN, reuses `retry.py`).
- `core/notifier/websocket.py` — `WebSocketChannel` (publishes to Redis pub/sub; `/ws` server subscribes and fans out).
- `apps/web/routers/abis.py` — `/api/abis` CRUD; publishes `config_changed` after write.
- `apps/web/routers/ws.py` — `/ws?channel_id=` handler; resolves channel → pubsub name, streams via bounded `asyncio.Queue`.

**File-size rule of thumb:** keep each module ≤300 LOC. If approaching the limit, split.

---

## Chunk 1: M1 Follow-ups Cleanup

Closes the five M1 follow-ups that are self-contained and unblock the rest of M2. Three follow-ups (`Channel` ABC contract, `Channel.type` enforcement, `arg_filters` value-shape validation) graduate into chunks 6 and 9 because they share files with new work; one (session-scope Redis fixture) is tracked-only per spec §9.1.

**Items closed this chunk:**
1. **`_Worker.shutdown` parallel runner stops** — replace the sequential `for chain_id in ... await self._stop_runner(...)` loop in `apps/worker/main.py:163-164` with `asyncio.gather(...)`. The bus/db disconnect ordering **must remain strictly after** gather returns.
2. **`Notifier._sem` event-loop binding** — build `asyncio.Semaphore` on first `send()`, not in `__init__`, so the bound loop matches the running loop (`core/notifier/notifier.py:43`).
3. **`poll_interval_ms` plumbing into `EvmAdapter`** — read `chain.poll_interval_ms` from snapshot, pass through `EvmAdapter(poll_interval_ms=...)`, use it inside `_poll_heads` instead of the hard-coded `await asyncio.sleep(1.0)` (`core/chains/evm.py:156` and `apps/worker/main.py:31-36`).
4. **Windows signal-handler docstring polish** — clarify in `apps/worker/main.py:_amain` that `loop.add_signal_handler` raises `NotImplementedError` on Windows and document the contract (the CLI entry point is POSIX-targeted; Windows runs `run_worker(...)` directly with caller-driven `stop_event`).
5. **Deterministic worker-ready signal in E2E** — replace `await asyncio.sleep(1.0)` in `tests/e2e/test_native_transfer_e2e.py:156` with an `asyncio.Event` the worker flips after its first reconcile completes. Eliminates the only timing-based wait in the E2E test.

**Constraint:** Every existing M1 test must still pass after this chunk. The followups are mechanical refactors; no behavioral change is intended.

**New files this chunk:**
- `tests/unit/test_notifier_sem_lazy.py`
- `tests/unit/test_worker_shutdown_parallel.py`

**Modified files this chunk:**
- `apps/worker/main.py` — parallel shutdown, signal docstring, `_default_adapter_factory` passes `poll_interval_ms`, ready-signal hook for tests.
- `core/notifier/notifier.py` — `_sem` lazy build.
- `core/chains/evm.py` — accept `poll_interval_ms`, use in `_poll_heads`.
- `core/parser/native.py` — docstring fix (retract Solana claim).
- `tests/e2e/test_native_transfer_e2e.py` — wait on `worker_ready` event instead of sleep.

**Out of scope this chunk:**
- Anything ABI / channel / Solana related (chunks 2-14).
- Changes to `_stop_runner` semantics (still cancels task; gather only collapses outer loop).
- Removing `NotImplementedError` from `_default_adapter_factory` (chunk 10 owns the Solana branch).

### Task 1.1: Worker shutdown parallel runner stops

`_Worker.shutdown` currently loops sequentially over `self._runners`, awaiting each `_stop_runner` in series. Each `_stop_runner` waits up to `DRAIN_TIMEOUT_S` (30s); with N chains, total worst-case shutdown is N×30s. Parallel `gather` makes it max(30s) regardless of N.

**Files:**
- Modify: `apps/worker/main.py:153-167`
- Create: `tests/unit/test_worker_shutdown_parallel.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_worker_shutdown_parallel.py
from __future__ import annotations

import asyncio
import contextlib
import time
from typing import Any

import pytest

from apps.worker.main import _Worker
from core.settings import Settings

pytestmark = pytest.mark.asyncio


class _SlowRunner:
    """Stand-in for ChainRunner.stop() that sleeps 0.5s."""
    def __init__(self) -> None:
        self.stop_started_at: float | None = None
        self.stop_finished_at: float | None = None

    async def stop(self) -> None:
        self.stop_started_at = time.monotonic()
        await asyncio.sleep(0.5)
        self.stop_finished_at = time.monotonic()

    async def apply_snapshot(self, snap: Any) -> None: ...


async def test_shutdown_stops_runners_in_parallel(tmp_path: Any) -> None:
    """With three slow runners, shutdown must complete in ~0.5s, not ~1.5s."""
    s = Settings(database={"url": f"sqlite+aiosqlite:///{tmp_path / 'x.sqlite'}"})
    w = _Worker(s)
    # Don't actually start db/bus; we only test the parallel-stop path.
    # Inject three fake runners directly.
    runners: dict[str, tuple[Any, asyncio.Task[None]]] = {}
    fake_runners: list[_SlowRunner] = []
    for i in range(3):
        r = _SlowRunner()
        fake_runners.append(r)

        async def _idle() -> None:
            await asyncio.Event().wait()

        t = asyncio.create_task(_idle(), name=f"fake-runner-{i}")
        runners[f"chain-{i}"] = (r, t)
    w._runners = runners  # type: ignore[assignment]

    # Patch out bus/db disconnect so we don't need real connections.
    async def _noop() -> None:
        return None
    w._bus.disconnect = _noop  # type: ignore[method-assign]
    w._db.disconnect = _noop  # type: ignore[method-assign]

    t0 = time.monotonic()
    await w.shutdown()
    elapsed = time.monotonic() - t0

    # Parallel ⇒ <= ~0.7s. Sequential would be ~1.5s.
    assert elapsed < 1.0, f"shutdown took {elapsed:.2f}s; expected <1.0s (parallel)"
    # All runners actually stopped.
    for r in fake_runners:
        assert r.stop_finished_at is not None

    # Tasks were cancelled by _stop_runner.
    for _, t in runners.values():
        assert t.cancelled() or t.done()


async def test_shutdown_disconnects_bus_after_runners_finish(tmp_path: Any) -> None:
    """Bus disconnect ordering must remain strictly AFTER all runner stops."""
    s = Settings(database={"url": f"sqlite+aiosqlite:///{tmp_path / 'x.sqlite'}"})
    w = _Worker(s)
    events: list[str] = []

    class _OrderedRunner:
        async def stop(self) -> None:
            await asyncio.sleep(0.1)
            events.append("runner-stopped")

        async def apply_snapshot(self, snap: Any) -> None: ...

    async def _idle() -> None:
        await asyncio.Event().wait()
    t = asyncio.create_task(_idle(), name="ordered-runner")
    w._runners = {"c1": (_OrderedRunner(), t)}  # type: ignore[assignment]

    async def _bus_disc() -> None:
        events.append("bus-disconnected")
    async def _db_disc() -> None:
        events.append("db-disconnected")
    w._bus.disconnect = _bus_disc  # type: ignore[method-assign]
    w._db.disconnect = _db_disc  # type: ignore[method-assign]

    await w.shutdown()
    with contextlib.suppress(asyncio.CancelledError):
        await t

    assert events == ["runner-stopped", "bus-disconnected", "db-disconnected"], events
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_worker_shutdown_parallel.py -v`
Expected: FAIL (parallel-elapsed test fails because current shutdown is sequential).

- [ ] **Step 3: Refactor `_Worker.shutdown` to use `asyncio.gather`**

Replace `apps/worker/main.py:163-164` (the `for chain_id in list(self._runners): await self._stop_runner(chain_id)` lines) with:

```python
    async def shutdown(self) -> None:
        """Trigger graceful drain per spec §9.1. Idempotent and safe to call
        after a partially-failed `start()` — `RedisBus.disconnect()` and
        `Database.disconnect()` both guard on connection state.

        Runner stops are issued in parallel (each runner has its own ~30s drain
        timeout; sequential shutdown would compound to N×30s). Bus/DB
        disconnect must happen strictly AFTER all runners stop so a runner
        cannot publish during teardown.
        """
        if self._stop.is_set():
            return
        self._stop.set()
        log.info("worker.shutdown_starting")
        if self._watcher is not None:
            await self._watcher.stop()
        if self._runners:
            await asyncio.gather(
                *(self._stop_runner(cid) for cid in list(self._runners)),
                return_exceptions=False,
            )
        await self._bus.disconnect()
        await self._db.disconnect()
        log.info("worker.shutdown_complete")
```

Notes:
- `return_exceptions=False` is deliberate: if a runner stop raises, surface it (M1 already isolates per-channel send failures inside `_stop_runner` → `runner.stop()`; a raise here means an infrastructure bug we want loud).
- `list(self._runners)` snapshots keys before gathering because `_stop_runner` mutates `self._runners` (each finishes with `self._runners.pop(...)`).

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_worker_shutdown_parallel.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Run M1 worker tests, expect no regressions.**

Run: `pytest tests/ -k worker -v`
Expected: every existing test still passes.

- [ ] **Step 6: Commit**

```bash
git add apps/worker/main.py tests/unit/test_worker_shutdown_parallel.py
git commit -m "fix(worker): parallel runner shutdown via asyncio.gather

Sequential drains would compound to N×30s on N chains. gather keeps the
strict bus/DB-after-runner ordering required by spec §9.1."
```

### Task 1.2: Notifier._sem lazy event-loop binding

`Notifier.__init__` builds `asyncio.Semaphore(max_concurrency)`. The semaphore binds to whichever loop is current at construction time. If a `Notifier` is constructed before the worker's event loop is running (e.g., in a top-level fixture), the semaphore is bound to the wrong loop and `acquire()` raises `RuntimeError: ... bound to a different event loop` at first `send`. Lazy build defers binding to the running loop at first use.

**Files:**
- Modify: `core/notifier/notifier.py:43`
- Create: `tests/unit/test_notifier_sem_lazy.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_notifier_sem_lazy.py
from __future__ import annotations

import asyncio
from typing import Any

import pytest

from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.event import Event

pytestmark = pytest.mark.asyncio


class _StubChannel(Channel):
    type = "stub"

    def __init__(self, config: dict[str, Any]) -> None:
        self.sent: list[dict[str, Any]] = []

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, payload: dict[str, Any]) -> None:
        self.sent.append(payload)


def test_constructing_notifier_outside_a_running_loop_does_not_bind() -> None:
    """Constructing Notifier with no running loop must not raise nor pre-bind
    a semaphore to a now-defunct loop."""
    n = Notifier(max_concurrency=5)
    # _sem is None until the first send call inside a running loop.
    assert n._sem is None


async def test_first_send_binds_sem_to_running_loop(tmp_path: Any) -> None:
    """Build Notifier in one place, call dispatch in the test's loop. Must work."""
    from core.config.snapshot import SnapshotChannel, SnapshotSubscription

    def _factory(cfg: SnapshotChannel) -> Channel:
        return _StubChannel(cfg.config)

    n = Notifier(channel_factory=_factory, max_concurrency=3)
    await n.start([SnapshotChannel(id="c1", name="c1", type="stub", config={})])

    ev = Event(
        chain_id="x", block_number=1, block_hash="0xb", block_timestamp=0,
        tx_hash="0xt", tx_index=0, log_index=None, kind="native_transfer",
        contract=None, name=None, args={}, raw={},
    )
    sub = SnapshotSubscription(
        id="s1", name="s", chain_id="x", address=None, abi_id=None,
        match_kind="native_transfer", match_name=None,
        arg_filters={}, enabled=True, channel_ids=["c1"],
    )
    chan = SnapshotChannel(id="c1", name="c1", type="stub", config={})

    await n.dispatch(ev, [(sub, [chan])])

    # After first dispatch the semaphore exists and is bound to *this* loop.
    sem = getattr(n, "_sem")
    assert sem is not None
    assert sem._value == 3  # default value, fully released

    await n.stop()
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_notifier_sem_lazy.py -v`
Expected: FAIL (first test fails — current `__init__` always builds the semaphore eagerly).

- [ ] **Step 3: Implement lazy binding**

Edit `core/notifier/notifier.py`:

```python
class Notifier:
    """Owns instantiated channels and dispatches events to them concurrently.

    A bounded `asyncio.Semaphore` (default 50) caps total in-flight sends across
    all channels held by this `Notifier` instance. Spec §6.1 specifies a *per-chain*
    semaphore — the worker (Chunk 7) instantiates one `Notifier` per chain, so the
    per-instance limit here IS the per-chain limit. Do not share a single `Notifier`
    across chains; that would conflate the two budgets.

    The semaphore is built lazily on the first `_send_one` call so it binds to
    the running loop. Constructing a `Notifier` outside a running loop (e.g.
    inside a sync fixture body) used to crash at first `send` with
    `RuntimeError: ... bound to a different event loop`.

    Failures in one channel do not block sibling channels — each `send` is wrapped
    to log-and-continue, and `asyncio.gather(..., return_exceptions=True)` is used
    defensively so a bug *outside* the `try` in `_send_one` cannot cancel siblings.
    """

    def __init__(
        self,
        *,
        channel_factory: Callable[[SnapshotChannel], Channel] = _default_factory,
        max_concurrency: int = 50,
    ) -> None:
        self._factory = channel_factory
        self._max_concurrency = max_concurrency
        self._sem: asyncio.Semaphore | None = None
        self._channels: dict[str, Channel] = {}

    def _get_sem(self) -> asyncio.Semaphore:
        if self._sem is None:
            self._sem = asyncio.Semaphore(self._max_concurrency)
        return self._sem
```

And in `_send_one`, replace `async with self._sem:` with `async with self._get_sem():`.

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_notifier_sem_lazy.py -v`
Expected: 2 PASS.

- [ ] **Step 5: Run M1 notifier tests for regression.**

Run: `pytest tests/ -k notifier -v`
Expected: every existing test still passes.

- [ ] **Step 6: Commit**

```bash
git add core/notifier/notifier.py tests/unit/test_notifier_sem_lazy.py
git commit -m "fix(notifier): lazy-build _sem so it binds to the running loop

Constructing Notifier in a sync context (top-level fixture) used to bind
asyncio.Semaphore to a now-defunct loop and crash at first send. Build on
first dispatch instead."
```

### Task 1.3: poll_interval_ms plumbing into EvmAdapter

`EvmAdapter._poll_heads` (`core/chains/evm.py:156`) hard-codes `await asyncio.sleep(1.0)`. The chain row already carries `poll_interval_ms` and `SnapshotChain` already exposes it; `_default_adapter_factory` in `apps/worker/main.py:31` just doesn't pass it through.

**Files:**
- Modify: `core/chains/evm.py` (constructor + `_poll_heads`)
- Modify: `apps/worker/main.py:_default_adapter_factory`
- Create: `tests/unit/test_evm_poll_interval.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_evm_poll_interval.py
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from core.chains.evm import EvmAdapter

pytestmark = pytest.mark.asyncio


async def test_poll_interval_ms_is_respected() -> None:
    """With poll_interval_ms=100, the unconditional end-of-loop sleep in
    `_poll_heads` should be 0.1s, not 1.0s."""
    a = EvmAdapter(
        chain_id="x", rpc_http="http://stub", rpc_ws=None,
        confirmations=0, poll_interval_ms=100,
    )

    # Inject a fake AsyncWeb3-like that always reports block 1.
    fake_eth = AsyncMock()
    fake_eth.block_number = 1
    fake_eth.get_block = AsyncMock(return_value={
        "number": 1, "hash": "0xaa", "parentHash": "0xbb", "timestamp": 1700000000,
    })
    class _FakeW3:
        eth = fake_eth
    a._w3 = _FakeW3()  # type: ignore[assignment]

    # Patch asyncio.sleep at the evm module's namespace so the cadence inside
    # `_poll_heads` is observable without actually sleeping.
    sleeps: list[float] = []
    import core.chains.evm as evm_mod
    orig_sleep = evm_mod.asyncio.sleep

    async def _record_sleep(d: float) -> None:
        sleeps.append(d)
        await orig_sleep(0)  # don't actually wait

    evm_mod.asyncio.sleep = _record_sleep  # type: ignore[assignment]
    try:
        gen = a._poll_heads()
        # Drive once to flush the first head + first sleep.
        await asyncio.wait_for(anext(gen), timeout=1.0)
        await asyncio.wait_for(orig_sleep(0), timeout=0.1)
    finally:
        evm_mod.asyncio.sleep = orig_sleep  # type: ignore[assignment]

    assert sleeps, "expected at least one sleep call"
    assert sleeps[0] == pytest.approx(0.1, abs=1e-6), \
        f"expected sleep ~0.1s for poll_interval_ms=100, got {sleeps[0]}"
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_evm_poll_interval.py -v`
Expected: FAIL — current `EvmAdapter.__init__` doesn't accept `poll_interval_ms`.

- [ ] **Step 3: Add `poll_interval_ms` to EvmAdapter**

Modify `core/chains/evm.py`:

```python
class EvmAdapter:
    chain_id: str
    confirmations: int

    def __init__(
        self,
        *,
        chain_id: str,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
        poll_interval_ms: int = 1000,
    ) -> None:
        self.chain_id = chain_id
        self.confirmations = confirmations
        self._rpc_http = rpc_http
        self._rpc_ws = rpc_ws
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._w3: AsyncWeb3[Any] | None = None
```

And in `_poll_heads`:

```python
    async def _poll_heads(self) -> AsyncIterator[BlockHeader]:
        assert self._w3 is not None
        last = -1
        while True:
            n = int(await self._w3.eth.block_number)
            if n > last:
                raw = await self._w3.eth.get_block(n)
                yield BlockHeader(
                    number=int(raw["number"]),
                    hash=_hexify(raw["hash"]),
                    parent_hash=_hexify(raw["parentHash"]),
                    timestamp=int(raw["timestamp"]),
                )
                last = n
            await asyncio.sleep(self._poll_interval_s)
```

- [ ] **Step 4: Pass `poll_interval_ms` from worker factory**

Edit `apps/worker/main.py:_default_adapter_factory`:

```python
def _default_adapter_factory(cfg: SnapshotChain) -> EvmAdapter:
    if cfg.kind != "evm":
        # Chunk 10 (Solana) extends this branch. M1 hard-fails so misconfigs are loud.
        raise NotImplementedError(f"chain kind {cfg.kind!r} not supported yet")
    return EvmAdapter(
        chain_id=cfg.id,
        rpc_http=cfg.rpc_http,
        rpc_ws=cfg.rpc_ws,
        confirmations=cfg.confirmations,
        poll_interval_ms=cfg.poll_interval_ms,
    )
```

(The `M3 (Solana)` comment becomes `Chunk 10` since the Solana branch is no longer the next milestone — it's the next chunk in this very plan.)

- [ ] **Step 5: Run, expect PASS.**

Run: `pytest tests/unit/test_evm_poll_interval.py -v`
Expected: PASS.

- [ ] **Step 6: Run all evm + worker tests for regression.**

Run: `pytest tests/ -k 'evm or worker' -v`
Expected: every existing test still passes. If any M1 test constructed `EvmAdapter` without `poll_interval_ms`, the new arg has a default so the test still compiles.

- [ ] **Step 7: Commit**

```bash
git add core/chains/evm.py apps/worker/main.py tests/unit/test_evm_poll_interval.py
git commit -m "feat(evm): plumb per-chain poll_interval_ms into EvmAdapter

Closes M1 follow-up #6. EvmAdapter takes an optional poll_interval_ms
(default 1000); the worker factory reads chain.poll_interval_ms from the
snapshot and passes it through."
```

### Task 1.4: Windows signal-handler docstring polish

`loop.add_signal_handler` raises `NotImplementedError` on Windows (the Proactor event loop does not implement signal handlers). The CLI entry point `apps/worker/main.py:_amain` is documented in the design spec as POSIX-targeted; this task adds an explicit comment so the contract is clear at the call site, and adds a doctest-style note documenting that Windows callers should drive `run_worker` directly with their own `stop_event`.

**Files:**
- Modify: `apps/worker/main.py:198-212` (`_amain`)

- [ ] **Step 1: Edit `_amain`**

Replace the body of `_amain` with:

```python
async def _amain() -> None:
    """POSIX-targeted CLI entry. Installs SIGTERM/SIGINT handlers and runs
    until either signal sets `stop_event`.

    Windows note: `loop.add_signal_handler` raises `NotImplementedError` on
    the Proactor event loop. Windows users should embed `run_worker(settings,
    stop_event)` directly into their own process supervisor and drive
    `stop_event` from whatever signal mechanism their OS provides (e.g. a
    SetConsoleCtrlHandler shim). Embedding callers don't need this CLI.
    """
    settings = load_settings()
    configure_logging(level=settings.logging.level, format=settings.logging.format)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_shutdown(sig: signal.Signals) -> None:
        log.info("worker.signal_received", signal=sig.name)
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig)
        except NotImplementedError:
            # Windows Proactor loop: signal handlers unsupported. Surface the
            # advice and bail; embedding callers should use run_worker() directly.
            log.error(
                "worker.signal_handler_unsupported",
                signal=sig.name,
                hint="run_worker(settings, stop_event) directly on Windows",
            )
            raise

    await run_worker(settings, stop_event)
```

The `try/except NotImplementedError` is **not** a silent swallow — it logs the actionable hint and re-raises so the user sees the error and the platform mismatch is loud.

- [ ] **Step 2: No new test needed.**

Rationale: a Windows-conditional unit test would require platform-mocking that's noisier than the one-line `except + raise`. The change is documentation + a single guard; the existing POSIX tests continue to exercise the happy path.

- [ ] **Step 3: Run the M1 worker tests for regression.**

Run: `pytest tests/ -k worker -v`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add apps/worker/main.py
git commit -m "docs(worker): clarify _amain is POSIX-only; surface Windows hint

loop.add_signal_handler raises NotImplementedError on Proactor. Catch,
log a hint to use run_worker() directly, and re-raise."
```

### Task 1.5: Deterministic worker-ready signal in E2E

`tests/e2e/test_native_transfer_e2e.py:156` has `await asyncio.sleep(1.0)` after starting the worker, before submitting transactions. This is the only timing-based wait in the entire M1 E2E test and an obvious flake source on slow CI. Replace with an `asyncio.Event` the worker flips after the first reconcile completes (i.e. once `ChainRunner` is running and the adapter is connected).

The hook is added at the `run_worker` layer so the E2E test can pass an event in; production callers (`_amain`) construct a private event they ignore.

**Files:**
- Modify: `apps/worker/main.py` (`run_worker`, `_Worker.run`)
- Modify: `tests/e2e/test_native_transfer_e2e.py:156` (remove sleep, wait on event)
- Modify: `tests/e2e/conftest.py` (no fixture change needed — event is created by the test directly)

- [ ] **Step 1: Extend `run_worker` to accept an optional ready event**

In `apps/worker/main.py`:

```python
async def run_worker(
    settings: Settings,
    stop_event: asyncio.Event,
    *,
    ready_event: asyncio.Event | None = None,
) -> None:
    """Public coroutine that boots a `_Worker` and runs until `stop_event` is set.

    `ready_event` (optional) is set after `_Worker.start()` succeeds AND the
    first reconcile completes (i.e. all enabled chains have started). Tests
    use this to avoid timing-based sleeps; production callers (`_amain`)
    ignore it.

    Does NOT install signal handlers — the caller is responsible for triggering
    `stop_event` (E2E tests do this directly; the CLI entry point does it from
    SIGTERM/SIGINT handlers via `_amain` below).
    """
    worker = _Worker(settings, ready_event=ready_event)
    try:
        await worker.start()
    except BaseException:
        await worker.shutdown()
        raise
    run_task = asyncio.create_task(worker.run(), name="worker-main-loop")
    stop_task = asyncio.create_task(stop_event.wait(), name="worker-stop-wait")
    try:
        await asyncio.wait({run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        stop_task.cancel()
        await worker.shutdown()
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
```

- [ ] **Step 2: Wire `ready_event` into `_Worker`**

This step **only adds** the `ready_event` kwarg, the `self._ready` attribute, and the `first_reconcile_done` book-keeping inside `run()`. Every other method on `_Worker` (`start`, `_dequeue_snapshot_or_stop`, `_reconcile`, `_stop_runner`, `shutdown`) must stay byte-for-byte identical to the post-Task-1.1 file. Apply with targeted edits, not a class-level replace.

```python
class _Worker:
    def __init__(
        self,
        settings: Settings,
        *,
        ready_event: asyncio.Event | None = None,
    ) -> None:
        self._settings = settings
        self._db = Database(settings.database.url, echo=settings.database.echo)
        self._bus = RedisBus(url=settings.redis.url)
        self._checkpoint_adapter = _CheckpointAdapter(self._db)
        self._snap_queue: asyncio.Queue[ConfigSnapshot] = asyncio.Queue(maxsize=8)
        self._watcher: ConfigWatcher | None = None
        self._runners: dict[str, tuple[ChainRunner, asyncio.Task[None]]] = {}
        self._stop = asyncio.Event()
        self._ready = ready_event

    async def run(self) -> None:
        """Main loop: dequeue snapshots, reconcile runners, exit on _stop.
        Sets `_ready` after the first reconcile completes (i.e. all enabled
        chains have started)."""
        first_reconcile_done = False
        while not self._stop.is_set():
            snap = await self._dequeue_snapshot_or_stop()
            if snap is None:
                return
            await self._reconcile(snap)
            if not first_reconcile_done:
                first_reconcile_done = True
                if self._ready is not None:
                    self._ready.set()
```

- [ ] **Step 3: Update the E2E test to wait on the ready event**

In `tests/e2e/test_native_transfer_e2e.py`, replace the worker-startup block:

```python
    # 2) Start the worker in-process. It will load the seeded config on boot
    # via the Redis `config_changed` pub/sub path (publishes happened above).
    stop_event = asyncio.Event()
    ready_event = asyncio.Event()
    worker_task = asyncio.create_task(
        run_worker(settings, stop_event, ready_event=ready_event)
    )

    # Wait for the worker to load config and start the chain runner.
    # Deterministic replacement for the legacy `await asyncio.sleep(1.0)`.
    try:
        await asyncio.wait_for(ready_event.wait(), timeout=10.0)
    except TimeoutError:
        stop_event.set()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(worker_task, timeout=5.0)
        pytest.fail("worker did not signal ready within 10s")
```

- [ ] **Step 4: Run the E2E test, expect PASS**

Run: `pytest tests/e2e/test_native_transfer_e2e.py -v -m e2e`
Expected: PASS (and noticeably faster than M1 because there's no fixed 1s wait).

- [ ] **Step 5: Run the full M1 test suite for regression.**

Run: `pytest tests/ -v`
Expected: green across unit + integration; e2e green if Anvil is installed locally (skipped otherwise).

- [ ] **Step 6: Commit**

```bash
git add apps/worker/main.py tests/e2e/test_native_transfer_e2e.py
git commit -m "test(e2e): deterministic worker-ready signal; remove sleep(1.0)

Adds an optional ready_event arg to run_worker; the worker flips it after
the first reconcile completes. Eliminates the only timing-based wait in
the M1 E2E test."
```

### Task 1.6: NativeTransferParser docstring fix

The current docstring on `NativeTransferParser` claims the Solana adapter normalizes Solana txs into the EVM `Tx` shape so this parser is "chain-agnostic". The M2 design spec explicitly retracts that claim (§4.7): Solana parsers consume `SolanaBlock` directly via the new `SolanaParserPipeline` (chunk 11). This task corrects the docstring so future readers don't chase a non-existent abstraction.

**Files:**
- Modify: `core/parser/native.py:9-16`

- [ ] **Step 1: Edit the docstring**

```python
class NativeTransferParser:
    """Emit a native_transfer Event for each tx with value > 0 (EVM only).

    Skips contract creations (to_addr is None) and reverted txs (status == 0).

    EVM-specific: this parser consumes the EVM `Block` shape defined in
    `core/chains/types`. Solana has a different block shape (`SolanaBlock`)
    and a dedicated `SolNativeTransferParser` (see `core/parser/sol_native.py`
    in M2 chunk 11). Earlier versions of this docstring claimed
    `SolanaAdapter` normalized Solana txs into the EVM `Tx` shape — that
    claim is retracted; Solana parsers consume `SolanaBlock` directly via
    `SolanaParserPipeline`.
    """
```

- [ ] **Step 2: Run native-parser tests for regression.**

Run: `pytest tests/unit/test_native_parser.py -v`
Expected: PASS (docstring only; behavior unchanged).

- [ ] **Step 3: Commit**

```bash
git add core/parser/native.py
git commit -m "docs(parser): retract Solana 'chain-agnostic' claim from NativeTransferParser

M2 ships a separate SolNativeTransferParser (chunk 11) that consumes
SolanaBlock directly. The previous docstring promised a normalization
that was never implemented."
```

### Task 1.7: Chunk 1 close-out

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: every M1 test plus the new chunk-1 tests pass.

- [ ] **Step 2: Lint / type check**

Run: `make lint typecheck`
Expected: clean.

- [ ] **Step 3: Note in the plan that chunk 1 is done.**

(No file change — the next subagent picks up chunk 2 from this plan.)

---

## Chunk 2: ABI Entity + Registry + Decoder Cache

Introduces the cross-cutting `core/abi/` package that backs the three new ABI-driven parsers in chunks 3-5 (ERC-20, AbiEvent, AbiCall) and chunk 13 (AnchorIdlEvent). The `Abi` ORM table is already in M1 (`core/config/models.py:78`) — this chunk adds **only** the API surface (`/api/abis` CRUD), the snapshot integration, and the runtime registry that compiles and caches decoders.

**Spec §4.1 scope:**
- `AbiRegistry`: constructed once per worker, takes a `Database` handle, refreshes on `config_changed`.
- `Decoder` cache: keyed by `(abi_id, key)` where `key` is `topic0` (event) / 4-byte selector (call) / 8-byte discriminator (Anchor IDL).
- Decoders compiled lazily on first request, then memoized.
- API: `POST /api/abis`, `GET /api/abis`, `GET /api/abis/{id}`, `DELETE /api/abis/{id}` (no PATCH — ABIs are immutable; update = delete + create with a new id).
- Side effect: every write bumps `config_version` via `bump_and_publish`.

**New files this chunk:**
- `core/abi/__init__.py`
- `core/abi/errors.py`
- `core/abi/registry.py`
- `core/abi/decoder.py` (EVM event/call paths only; Anchor IDL path lands in chunk 13)
- `apps/web/routers/abis.py`
- `tests/unit/test_abi_registry.py`
- `tests/unit/test_abi_decoder.py`
- `tests/integration/test_abi_api.py`

**Modified files this chunk:**
- `apps/web/main.py` — include `abis` router.
- `apps/web/schemas.py` — `AbiCreate`, `AbiOut` schemas.
- `core/config/repositories.py` — `AbiRepo` CRUD.
- `core/config/snapshot.py` — `SnapshotAbi`, `load_snapshot` reads `abis`.

**Out of scope this chunk:**
- Wiring `AbiRegistry` into any parser (chunks 3-5, 13 do that).
- Anchor IDL discriminator path in `decoder.py` (chunk 13).
- Refresh-on-`config_changed` hook in the worker (deferred to chunk 4 where the first ABI parser ships and the wiring becomes load-bearing).

**Dependency / version pin:**
- `eth-abi` ships as a transitive of `web3>=6` (already in `pyproject.toml`). Verify with `pip show eth-abi` — no new top-level dep needed.

### Task 2.1: AbiRepo — add `delete` method

`AbiRepo` already exists in M1 at `core/config/repositories.py:162` with `create`, `get`, and `list_all` already implemented (M1 needed `Abi` rows for FK targets even though no parser consumed them yet). The only gap is `delete` — needed by the `DELETE /api/abis/{id}` route in Task 2.3.

**Files:**
- Modify: `core/config/repositories.py` (extend existing `AbiRepo` with `delete`)
- Test: `tests/integration/test_repositories.py` (append AbiRepo cases)

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/test_repositories.py`:

```python
# tests/integration/test_repositories.py — append
from core.config.models import AbiKind
from core.config.repositories import AbiRepo


_ERC20_TRANSFER_EVENT = {
    "name": "Transfer", "type": "event", "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}


@pytest.mark.asyncio
async def test_abi_repo_create_get_list(db) -> None:
    """Sanity check that M1's create/get/list_all still work — guards against
    any accidental signature drift introduced by the delete patch."""
    async with db.session() as s:
        row = await AbiRepo(s).create(
            name="erc20", kind=AbiKind.evm_abi, body=[_ERC20_TRANSFER_EVENT],
        )
        await s.commit()
        abi_id = row.id
        assert abi_id

    async with db.session() as s:
        got = await AbiRepo(s).get(abi_id)
        assert got is not None
        assert got.name == "erc20"
        assert got.kind == AbiKind.evm_abi
        rows = await AbiRepo(s).list_all()
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_abi_repo_delete_removes_row(db) -> None:
    async with db.session() as s:
        row = await AbiRepo(s).create(
            name="erc20", kind=AbiKind.evm_abi, body=[_ERC20_TRANSFER_EVENT],
        )
        await s.commit()
        abi_id = row.id

    async with db.session() as s:
        await AbiRepo(s).delete(abi_id)
        await s.commit()

    async with db.session() as s:
        assert await AbiRepo(s).get(abi_id) is None


@pytest.mark.asyncio
async def test_abi_repo_delete_unknown_id_is_noop(db) -> None:
    """delete() on a non-existent id must NOT raise — the router layer is
    responsible for 404 surfacing (Task 2.3 does a get-first guard)."""
    async with db.session() as s:
        await AbiRepo(s).delete("no-such-id")
        await s.commit()
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/integration/test_repositories.py -k abi_repo -v`
Expected: `test_abi_repo_delete_removes_row` and `test_abi_repo_delete_unknown_id_is_noop` fail with `AttributeError: 'AbiRepo' object has no attribute 'delete'`; `test_abi_repo_create_get_list` may pass already.

- [ ] **Step 3: Add `delete` to the existing `AbiRepo`**

Edit `core/config/repositories.py`: locate the existing `AbiRepo` class (around line 162, immediately above `CheckpointRepo`). Append the `delete` method, mirroring `SubscriptionRepo.delete` at line 156:

```python
    async def delete(self, abi_id: str) -> None:
        a = await self.get(abi_id)
        if a is not None:
            await self.s.delete(a)
```

Do NOT touch the existing `create`/`get`/`list_all` methods. Do NOT change the `body: Any` signature on `create` (changing it would invalidate M1 callers).

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/integration/test_repositories.py -k abi_repo -v`
Expected: all 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/config/repositories.py tests/integration/test_repositories.py
git commit -m "feat(config): AbiRepo.delete"
```

### Task 2.2: AbiCreate / AbiOut Pydantic schemas

**Files:**
- Modify: `apps/web/schemas.py` (append)

- [ ] **Step 1: Append schemas**

```python
# apps/web/schemas.py — append

# ---- ABIs -----------------------------------------------------------------


class AbiCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    kind: Literal["evm_abi", "solana_idl"]
    body: dict[str, Any] | list[Any]


class AbiOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    kind: str
    body: dict[str, Any] | list[Any]
```

- [ ] **Step 2: Run schema imports for sanity**

Run: `python -c "from apps.web.schemas import AbiCreate, AbiOut; print('ok')"`
Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add apps/web/schemas.py
git commit -m "feat(web): AbiCreate/AbiOut schemas"
```

### Task 2.3: /api/abis router

**Files:**
- Create: `apps/web/routers/abis.py`
- Modify: `apps/web/main.py` (include router)
- Test: `tests/integration/test_abi_api.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_abi_api.py
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from core.config.db import Database
from core.config.models import Base

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# Mirrors `_FakeBus` from tests/unit/test_web_chains.py — captures
# (channel, payload) tuples so the test can assert publish side effects.
class _FakeBus:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    async def ping(self) -> bool:
        return True

    async def publish(self, channel: str, payload: dict[str, Any]) -> None:
        self.published.append((channel, payload))


# `db` is provided by tests/integration/conftest.py (M1) — file-backed memory SQLite
# with Base.metadata.create_all already run.


@pytest_asyncio.fixture
async def fake_bus() -> _FakeBus:
    return _FakeBus()


@pytest.fixture
def erc20_body() -> list[dict[str, Any]]:
    return [
        {
            "name": "Transfer", "type": "event",
            "inputs": [
                {"name": "from", "type": "address", "indexed": True},
                {"name": "to",   "type": "address", "indexed": True},
                {"name": "value","type": "uint256", "indexed": False},
            ],
        }
    ]


async def test_abi_crud_round_trip(db: Database, fake_bus: _FakeBus, erc20_body) -> None:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: fake_bus

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        # CREATE
        r = await c.post("/api/abis", json={
            "name": "erc20", "kind": "evm_abi", "body": erc20_body,
        })
        assert r.status_code == 201, r.text
        abi_id = r.json()["id"]
        assert r.json()["name"] == "erc20"
        assert r.json()["kind"] == "evm_abi"

        # GET single
        r = await c.get(f"/api/abis/{abi_id}")
        assert r.status_code == 200
        assert r.json()["body"] == erc20_body

        # GET 404 for unknown
        r = await c.get("/api/abis/no-such-id")
        assert r.status_code == 404

        # LIST
        r = await c.get("/api/abis")
        assert r.status_code == 200
        ids = [x["id"] for x in r.json()]
        assert abi_id in ids

        # DELETE
        r = await c.delete(f"/api/abis/{abi_id}")
        assert r.status_code == 204

        r = await c.get(f"/api/abis/{abi_id}")
        assert r.status_code == 404

    # Verify config_version bumped via fake_bus's recorded publishes.
    # `bump_and_publish` posts to the "config_changed" channel with an
    # {"entity": "abi", "id": ..., "action": "create|delete"} payload.
    actions = [payload["action"] for _, payload in fake_bus.published]
    assert "create" in actions
    assert "delete" in actions


async def test_abi_create_accepts_empty_body(db: Database, fake_bus: _FakeBus) -> None:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: fake_bus

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/abis", json={"name": "x", "kind": "evm_abi", "body": []})
        # Empty body is allowed at API level; downstream decoder cache will be a no-op.
        # This test just ensures we don't fail-fast on empty arrays.
        assert r.status_code == 201


async def test_abi_create_rejects_invalid_kind(db: Database, fake_bus: _FakeBus) -> None:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: fake_bus

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        r = await c.post("/api/abis", json={"name": "x", "kind": "yaml", "body": {}})
        assert r.status_code == 422
```

Note: `AbiRepo.delete` is intentionally permissive (no-op on unknown id — see Task 2.1). The router layer adds a `get`-then-404 guard before calling `delete`, so the test's 404 expectation passes through the router check, not the repo.

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/integration/test_abi_api.py -v`
Expected: all three FAIL with 404 (no `/api/abis` route mounted yet).

- [ ] **Step 3: Implement the router**

```python
# apps/web/routers/abis.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from apps.web.deps import get_bus, get_session
from apps.web.routers._common import bump_and_publish
from apps.web.schemas import AbiCreate, AbiOut
from core.bus.redis_bus import RedisBus
from core.config.models import AbiKind
from core.config.repositories import AbiRepo

router = APIRouter(prefix="/api/abis", tags=["abis"])


@router.post("", response_model=AbiOut, status_code=status.HTTP_201_CREATED)
async def create_abi(
    payload: AbiCreate,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> AbiOut:
    row = await AbiRepo(session).create(
        name=payload.name,
        kind=AbiKind(payload.kind),
        body=payload.body,
    )
    await bump_and_publish(session, bus, entity="abi", entity_id=row.id, action="create")
    return AbiOut.model_validate(row)


@router.get("", response_model=list[AbiOut])
async def list_abis(
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> list[AbiOut]:
    rows = await AbiRepo(session).list_all()
    return [AbiOut.model_validate(r) for r in rows]


@router.get("/{abi_id}", response_model=AbiOut)
async def get_abi(
    abi_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
) -> AbiOut:
    row = await AbiRepo(session).get(abi_id)
    if row is None:
        raise HTTPException(status_code=404, detail="abi not found")
    return AbiOut.model_validate(row)


@router.delete("/{abi_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_abi(
    abi_id: str,
    session: AsyncSession = Depends(get_session),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> None:
    repo = AbiRepo(session)
    row = await repo.get(abi_id)
    if row is None:
        raise HTTPException(status_code=404, detail="abi not found")
    await repo.delete(abi_id)
    await bump_and_publish(session, bus, entity="abi", entity_id=abi_id, action="delete")
```

- [ ] **Step 4: Wire the router into the app**

Edit `apps/web/main.py`'s `create_app` function. The existing router-import / include block already imports `chains`, `channels`, `subscriptions`. Add **exactly two new lines** alongside them — do not delete or reorder the existing entries:

1. After the existing `from apps.web.routers import chains as chains_router` import line, add:

```python
    from apps.web.routers import abis as abis_router  # noqa: E402
```

2. After the existing `app.include_router(chains_router.router)` call, add:

```python
    app.include_router(abis_router.router)
```

Keep the existing chains / channels / subscriptions imports and includes unchanged.

- [ ] **Step 5: Run, expect PASS.**

Run: `pytest tests/integration/test_abi_api.py -v`
Expected: all 3 PASS.

- [ ] **Step 6: Run full integration suite**

Run: `pytest tests/integration -v`
Expected: no regressions in M1 routers.

- [ ] **Step 7: Commit**

```bash
git add apps/web/routers/abis.py apps/web/main.py tests/integration/test_abi_api.py
git commit -m "feat(web): /api/abis CRUD with config_version bump"
```

### Task 2.4: SnapshotAbi + load_snapshot integration

The worker boots a `ConfigSnapshot` once per `config_changed`. ABIs need to be in that snapshot so the registry can refresh on reload without an extra DB round-trip.

**Files:**
- Modify: `core/config/snapshot.py` (add `SnapshotAbi`, extend `load_snapshot`, add `ConfigSnapshot.abis`)
- Test: `tests/integration/test_snapshot_abis.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_snapshot_abis.py
from __future__ import annotations

import pytest

from core.config.db import Database
from core.config.models import AbiKind
from core.config.repositories import AbiRepo
from core.config.snapshot import load_snapshot

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_load_snapshot_includes_abis(db: Database) -> None:
    async with db.session() as s:
        await AbiRepo(s).create(
            name="erc20", kind=AbiKind.evm_abi,
            body=[{"type": "event", "name": "Transfer", "inputs": []}],
        )
        await s.commit()

    async with db.session() as s:
        snap = await load_snapshot(s)

    assert len(snap.abis) == 1
    assert snap.abis[0].name == "erc20"
    assert snap.abis[0].kind == "evm_abi"
    assert snap.abis[0].body[0]["name"] == "Transfer"
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/integration/test_snapshot_abis.py -v`
Expected: `AttributeError: 'ConfigSnapshot' object has no attribute 'abis'`.

- [ ] **Step 3: Extend snapshot**

Edit `core/config/snapshot.py`. Add the `SnapshotAbi` dataclass and the `abis: list[SnapshotAbi]` field on `ConfigSnapshot`; update `load_snapshot` to fetch ABIs.

```python
@dataclass(frozen=True)
class SnapshotAbi:
    id: str
    name: str
    kind: str
    body: dict[str, Any] | list[Any]


@dataclass(frozen=True)
class ConfigSnapshot:
    """Read-only snapshot. The `list[...]` fields are mutable-typed but treat as immutable;
    rebuild a new snapshot rather than mutating in place."""
    version: int
    subscriptions: list[SnapshotSubscription]
    channels: list[SnapshotChannel]
    chains: list[SnapshotChain] = field(default_factory=list)
    abis: list[SnapshotAbi] = field(default_factory=list)

    # ... existing methods unchanged ...

    def abi_by_id(self, abi_id: str) -> SnapshotAbi | None:
        for a in self.abis:
            if a.id == abi_id:
                return a
        return None
```

In `load_snapshot`, add ABI loading. The `AbiRepo` import goes alongside the existing repo imports at the top of the file:

```python
from core.config.repositories import (
    AbiRepo,
    ChainRepo,
    ConfigVersionRepo,
    SubscriptionRepo,
)

async def load_snapshot(session: AsyncSession) -> ConfigSnapshot:
    """Build a ConfigSnapshot from the database in a single transaction."""
    version = await ConfigVersionRepo(session).get()
    chains_rows = await ChainRepo(session).list_enabled()
    abi_rows = await AbiRepo(session).list_all()
    sub_bindings = await SubscriptionRepo(session).list_enabled_with_channels()

    # ... existing snap_chains, snap_subs, snap_channels build ...

    snap_abis = [
        SnapshotAbi(id=a.id, name=a.name, kind=a.kind.value, body=a.body)
        for a in abi_rows
    ]
    return ConfigSnapshot(
        version=version,
        chains=snap_chains,
        subscriptions=snap_subs,
        channels=snap_channels,
        abis=snap_abis,
    )
```

(The exact placement of `snap_abis` and the existing snapshot build calls depends on M1's load_snapshot body; the patch is additive — do not delete existing logic.)

- [ ] **Step 4: Run, expect PASS**

Run: `pytest tests/integration/test_snapshot_abis.py -v`
Expected: PASS.

- [ ] **Step 5: Run all snapshot tests for regression.**

Run: `pytest tests/ -k snapshot -v`
Expected: M1 snapshot tests still green; the new ABI test passes.

- [ ] **Step 6: Commit**

```bash
git add core/config/snapshot.py tests/integration/test_snapshot_abis.py
git commit -m "feat(snapshot): include abis in ConfigSnapshot"
```

### Task 2.5: AbiRegistry — load + cache by abi_id

`AbiRegistry` is the single source of truth for "given an `abi_id`, what does its body decode?" It owns the LRU cache, refreshes from a `ConfigSnapshot`, and exposes typed lookup methods that the parsers in chunks 3-5 will call. The cache invalidation strategy: hash the body bytes of each ABI, and rebuild any compiled decoder whose hash changed.

**Files:**
- Create: `core/abi/__init__.py`, `core/abi/errors.py`, `core/abi/registry.py`
- Test: `tests/unit/test_abi_registry.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_abi_registry.py
from __future__ import annotations

import pytest

from core.abi.errors import AbiNotFound
from core.abi.registry import AbiRegistry
from core.config.snapshot import ConfigSnapshot, SnapshotAbi


_ERC20_TRANSFER = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}


def _snap_with(*abis: SnapshotAbi) -> ConfigSnapshot:
    return ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[], abis=list(abis),
    )


def test_registry_returns_body_by_abi_id() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    body = r.get_body("a1")
    assert body == [_ERC20_TRANSFER]


def test_registry_raises_for_unknown_abi() -> None:
    r = AbiRegistry()
    r.refresh(_snap_with())
    with pytest.raises(AbiNotFound):
        r.get_body("does-not-exist")


def test_refresh_evicts_deleted_abis() -> None:
    snap1 = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_ERC20_TRANSFER]))
    snap2 = _snap_with()  # a1 deleted
    r = AbiRegistry()
    r.refresh(snap1)
    assert r.get_body("a1") == [_ERC20_TRANSFER]
    r.refresh(snap2)
    with pytest.raises(AbiNotFound):
        r.get_body("a1")


def test_refresh_replaces_body_when_hash_changes() -> None:
    body_v1 = [_ERC20_TRANSFER]
    body_v2 = [{**_ERC20_TRANSFER, "name": "TransferV2"}]
    snap_v1 = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=body_v1))
    snap_v2 = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=body_v2))
    r = AbiRegistry()
    r.refresh(snap_v1)
    assert r.get_body("a1") == body_v1
    r.refresh(snap_v2)
    assert r.get_body("a1") == body_v2


def test_registry_decoder_cache_persists_across_refresh_if_body_unchanged() -> None:
    """When an ABI's body hash is unchanged across two refreshes, _evict
    is not called for that abi_id, so any prior decoder entries in
    `_decoders[(abi_id, *)]` should survive. We prove this by poking a
    sentinel into `_decoders` and confirming refresh preserves it; the
    real-decoder identity check lives in Task 2.7
    (test_decoder_cache_is_reused_for_same_abi_id_and_key)."""
    body = [_ERC20_TRANSFER]
    snap = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=body))
    r = AbiRegistry()
    r.refresh(snap)
    sentinel = object()
    # Prime the internal decoder dict with a sentinel under a fake key.
    r._decoders[("a1", "0xdead")] = sentinel  # type: ignore[index]
    r.refresh(snap)  # body hash unchanged → preserve cache
    assert r._decoders.get(("a1", "0xdead")) is sentinel
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_registry.py -v`
Expected: `ImportError`s (no `core.abi` package yet).

- [ ] **Step 3: Implement `errors.py`**

```python
# core/abi/__init__.py
```

```python
# core/abi/errors.py
from __future__ import annotations


class AbiError(Exception):
    """Base for all AbiRegistry / decoder errors."""


class AbiNotFound(AbiError):
    """No ABI is registered under the given id."""


class DecodeFailed(AbiError):
    """An ABI was found but decoding the input failed (malformed log/calldata)."""
```

- [ ] **Step 4: Implement `registry.py`**

```python
# core/abi/registry.py
from __future__ import annotations

import hashlib
import json
from typing import Any

import structlog

from core.abi.errors import AbiNotFound
from core.config.snapshot import ConfigSnapshot, SnapshotAbi

log = structlog.get_logger(__name__)


def _hash_body(body: Any) -> str:
    """Stable content hash for a body. Used to decide whether to drop a
    cached decoder when an ABI is republished with the same id."""
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class AbiRegistry:
    """In-memory registry of ABIs by id.

    Responsibilities:
      - `refresh(snapshot)`: replace internal `{abi_id → SnapshotAbi}` map.
      - `get_body(abi_id)`: return the raw ABI body for downstream decode.
      - decoder cache: `_decoders[(abi_id, key)] → compiled decoder`. Caller
        (parsers in chunks 3-5) populate this via `get_event_decoder` /
        `get_call_decoder` (added in Task 2.6).
      - On refresh, evict decoders for any abi whose body hash changed; keep
        decoders for unchanged abis to avoid recompiling on every snapshot
        bump.
    """

    def __init__(self) -> None:
        self._abis: dict[str, SnapshotAbi] = {}
        self._hashes: dict[str, str] = {}
        self._decoders: dict[tuple[str, str], Any] = {}

    def refresh(self, snap: ConfigSnapshot) -> None:
        new_abis: dict[str, SnapshotAbi] = {a.id: a for a in snap.abis}
        # Drop decoders for deleted or changed abis.
        for abi_id in list(self._hashes.keys()):
            if abi_id not in new_abis:
                self._evict(abi_id)
                continue
            new_hash = _hash_body(new_abis[abi_id].body)
            if new_hash != self._hashes[abi_id]:
                self._evict(abi_id)
        # Record fresh state.
        self._abis = new_abis
        self._hashes = {aid: _hash_body(a.body) for aid, a in new_abis.items()}
        log.info("abi_registry.refreshed", count=len(new_abis))

    def _evict(self, abi_id: str) -> None:
        for key in list(self._decoders.keys()):
            if key[0] == abi_id:
                self._decoders.pop(key, None)
        self._hashes.pop(abi_id, None)

    def get_body(self, abi_id: str) -> dict[str, Any] | list[Any]:
        a = self._abis.get(abi_id)
        if a is None:
            raise AbiNotFound(abi_id)
        return a.body

    def get(self, abi_id: str) -> SnapshotAbi:
        a = self._abis.get(abi_id)
        if a is None:
            raise AbiNotFound(abi_id)
        return a
```

- [ ] **Step 5: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_registry.py -v`
Expected: 5 PASS.

- [ ] **Step 6: Commit**

```bash
git add core/abi/__init__.py core/abi/errors.py core/abi/registry.py tests/unit/test_abi_registry.py
git commit -m "feat(abi): AbiRegistry with content-hash-aware decoder cache"
```

### Task 2.6: Decoder cache — EVM event + call

Builds the compiled-decoder cache on top of `AbiRegistry`. Uses `eth-abi` directly (transitive dep of web3). Decoders are keyed by `(abi_id, key)` where `key` is the topic0 hash for events or the 4-byte selector for calls.

Both `topic0` and selector are derived from `keccak256(<canonical signature>)` (e.g. `keccak256("Transfer(address,address,uint256)")` for the ERC-20 event). `eth_utils` provides `function_signature_to_4byte_selector` and `event_signature_to_log_topic`; both are transitive deps of web3.

**Files:**
- Create: `core/abi/decoder.py`
- Test: `tests/unit/test_abi_decoder.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_abi_decoder.py
from __future__ import annotations

import pytest

from core.abi.decoder import (
    canonical_event_signature,
    canonical_function_signature,
    decode_event,
    decode_function_call,
    event_topic0,
    function_selector,
)
from core.abi.errors import DecodeFailed


_ERC20_TRANSFER_EVENT = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}

_TRANSFER_FN = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to",    "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
    "stateMutability": "nonpayable",
}


def test_canonical_signatures() -> None:
    assert canonical_event_signature(_ERC20_TRANSFER_EVENT) == "Transfer(address,address,uint256)"
    assert canonical_function_signature(_TRANSFER_FN) == "transfer(address,uint256)"


def test_event_topic0_is_keccak() -> None:
    t = event_topic0(_ERC20_TRANSFER_EVENT)
    # ERC-20 Transfer canonical topic0:
    assert t == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def test_function_selector_is_first_4_bytes_of_keccak() -> None:
    s = function_selector(_TRANSFER_FN)
    # `transfer(address,uint256)` 4-byte selector:
    assert s == "0xa9059cbb"


def test_decode_event_extracts_indexed_and_data() -> None:
    """Decoded args dict contains all fields with addresses lowercased and
    big ints as decimal strings (matches Event.args convention)."""
    topics = [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",  # topic0
        "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",  # from
        "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",  # to
    ]
    # uint256(123) encoded:
    data = "0x000000000000000000000000000000000000000000000000000000000000007b"
    args = decode_event(_ERC20_TRANSFER_EVENT, topics, data)
    assert args == {
        "from":  "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "to":    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "value": "123",
    }


def test_decode_event_fails_on_topic_count_mismatch() -> None:
    with pytest.raises(DecodeFailed, match="topic count"):
        decode_event(_ERC20_TRANSFER_EVENT, ["0xddf2..."], "0x")


def test_decode_function_call_extracts_args() -> None:
    # transfer(0xbbbb...bbb, 999)
    calldata = (
        "0xa9059cbb"
        "000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "00000000000000000000000000000000000000000000000000000000000003e7"
    )
    args = decode_function_call(_TRANSFER_FN, calldata)
    assert args == {
        "to":    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "value": "999",
    }


def test_decode_function_call_fails_on_wrong_selector() -> None:
    bad = "0xdeadbeef" + ("00" * 32)
    with pytest.raises(DecodeFailed, match="selector"):
        decode_function_call(_TRANSFER_FN, bad)
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_decoder.py -v`
Expected: ImportError (no `decoder.py` yet).

- [ ] **Step 3: Implement `decoder.py`**

```python
# core/abi/decoder.py
"""EVM ABI decoders backed by eth-abi + eth-utils. Solana Anchor IDL decoders
land in chunk 13 via `borsh-construct` and live in this same module."""
from __future__ import annotations

from typing import Any

from eth_abi import decode as eth_abi_decode
from eth_utils import (
    event_signature_to_log_topic,
    function_signature_to_4byte_selector,
)

from core.abi.errors import DecodeFailed


def canonical_event_signature(event_abi: dict[str, Any]) -> str:
    """Build `Name(type,type,...)` from a JSON ABI event entry."""
    inputs = event_abi.get("inputs", [])
    types = ",".join(_canonical_type(i) for i in inputs)
    return f"{event_abi['name']}({types})"


def canonical_function_signature(fn_abi: dict[str, Any]) -> str:
    """Build `name(type,type,...)` from a JSON ABI function entry."""
    inputs = fn_abi.get("inputs", [])
    types = ",".join(_canonical_type(i) for i in inputs)
    return f"{fn_abi['name']}({types})"


def _canonical_type(input_entry: dict[str, Any]) -> str:
    """Render a single ABI input's canonical type, including tuples.

    Recursive on `components` for tuple types: a `tuple` with components
    `[uint256, address]` renders as `(uint256,address)`. Array suffixes
    (`[]`, `[3]`) are preserved.
    """
    t = input_entry["type"]
    if t.startswith("tuple"):
        comps = input_entry.get("components", [])
        inner = ",".join(_canonical_type(c) for c in comps)
        suffix = t[len("tuple"):]
        return f"({inner}){suffix}"
    return t


def event_topic0(event_abi: dict[str, Any]) -> str:
    sig = canonical_event_signature(event_abi)
    return "0x" + event_signature_to_log_topic(sig).hex()


def function_selector(fn_abi: dict[str, Any]) -> str:
    sig = canonical_function_signature(fn_abi)
    return "0x" + function_signature_to_4byte_selector(sig).hex()


def _split_indexed(event_abi: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    indexed: list[dict[str, Any]] = []
    not_indexed: list[dict[str, Any]] = []
    for inp in event_abi.get("inputs", []):
        if inp.get("indexed"):
            indexed.append(inp)
        else:
            not_indexed.append(inp)
    return indexed, not_indexed


def _normalize_value(t: str, v: Any) -> Any:
    """Match the `Event.args` convention: addresses are 0x-lowercase strings;
    big ints (uint*/int*) are decimal strings; bytes are 0x-hex strings.

    Array / tuple element-wise normalisation is deferred: ABI decode for
    `uint256[]` returns a tuple of Python ints. JSON serialisation in the
    delivery payload (Task 9.x) will widen the contract: parser-side fields
    must already be `Event.args`-compatible (scalar address/int/bytes). Array
    element coercion for ABI-event parsers lands with the AbiEventParser in
    chunk 4 where it actually matters.
    """
    if t == "address":
        # eth-abi returns address as a checksum-cased str.
        return v.lower() if isinstance(v, str) else "0x" + v.hex().lower()
    if t.startswith(("uint", "int")) and not t.endswith("]"):
        return str(int(v))
    if t.startswith("bytes") and not t.endswith("]"):
        return "0x" + v.hex() if isinstance(v, (bytes, bytearray)) else v
    return v


def decode_event(
    event_abi: dict[str, Any], topics: list[str], data: str
) -> dict[str, Any]:
    """Decode an event log per spec §5.2. Returns a `{name: value}` dict
    aligned with `Event.args` conventions."""
    indexed, not_indexed = _split_indexed(event_abi)
    expected_topic_count = 1 + len(indexed)  # topic0 + indexed inputs
    if len(topics) != expected_topic_count:
        raise DecodeFailed(
            f"topic count {len(topics)} != expected {expected_topic_count} "
            f"for {event_abi.get('name')}"
        )

    args: dict[str, Any] = {}

    # Indexed topics: each topic is the 32-byte abi-encoded value (or hash for
    # dynamic types per Solidity ABI rules). For value types we re-decode.
    for inp, topic_hex in zip(indexed, topics[1:], strict=True):
        t = _canonical_type(inp)
        if t in ("string", "bytes") or t.endswith("]") or t.startswith("("):
            # Reference types: Solidity hashes the value into the topic, so we
            # can't recover the plaintext. Surface as the raw hash hex.
            args[inp["name"]] = topic_hex
            continue
        raw = bytes.fromhex(topic_hex.removeprefix("0x"))
        decoded = eth_abi_decode([t], raw)[0]
        args[inp["name"]] = _normalize_value(t, decoded)

    # Non-indexed: concatenated abi-encoded in `data`.
    if not_indexed:
        types = [_canonical_type(i) for i in not_indexed]
        raw = bytes.fromhex(data.removeprefix("0x"))
        try:
            decoded_tuple = eth_abi_decode(types, raw)
        except Exception as exc:
            raise DecodeFailed(f"data decode failed: {exc}") from exc
        for inp, val in zip(not_indexed, decoded_tuple, strict=True):
            args[inp["name"]] = _normalize_value(_canonical_type(inp), val)

    return args


def decode_function_call(fn_abi: dict[str, Any], calldata: str) -> dict[str, Any]:
    """Decode a function-call `input` per spec §5.2 (kind=call)."""
    expected = function_selector(fn_abi)
    raw = bytes.fromhex(calldata.removeprefix("0x"))
    if len(raw) < 4:
        raise DecodeFailed("calldata shorter than selector")
    sel = "0x" + raw[:4].hex()
    if sel != expected:
        raise DecodeFailed(
            f"selector {sel} != expected {expected} for {fn_abi.get('name')}"
        )

    types = [_canonical_type(i) for i in fn_abi.get("inputs", [])]
    try:
        decoded_tuple = eth_abi_decode(types, raw[4:])
    except Exception as exc:
        raise DecodeFailed(f"calldata decode failed: {exc}") from exc

    out: dict[str, Any] = {}
    for inp, val in zip(fn_abi.get("inputs", []), decoded_tuple, strict=True):
        out[inp["name"]] = _normalize_value(_canonical_type(inp), val)
    return out
```

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_decoder.py -v`
Expected: 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/abi/decoder.py tests/unit/test_abi_decoder.py
git commit -m "feat(abi): EVM event + call decoders (eth-abi backed)"
```

### Task 2.7: Wire registry-level event/call decoder lookup

The parsers in chunks 3-5 will call `registry.get_event_decoder(abi_id, topic0)` and `registry.get_call_decoder(abi_id, selector)`. The registry builds a topic0-keyed event map and a selector-keyed call map on demand, caches the entries, and invalidates them in `_evict`.

**Files:**
- Modify: `core/abi/registry.py`
- Test: extend `tests/unit/test_abi_registry.py`

- [ ] **Step 1: Append failing tests**

```python
# tests/unit/test_abi_registry.py — append
from core.abi.decoder import event_topic0, function_selector

_FN_TRANSFER = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to", "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}


def test_get_event_decoder_returns_decoder_for_known_topic0() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    decoder = r.get_event_decoder("a1", t0)
    args = decoder(
        topics=[
            t0,
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ],
        data="0x000000000000000000000000000000000000000000000000000000000000007b",
    )
    assert args["from"] == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert args["value"] == "123"


def test_get_event_decoder_raises_for_unknown_topic0() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    with pytest.raises(KeyError):
        r.get_event_decoder("a1", "0xdeadbeef" + "00" * 28)


def test_get_call_decoder_returns_decoder_for_known_selector() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_FN_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    sel = function_selector(_FN_TRANSFER)
    decoder = r.get_call_decoder("a1", sel)
    args = decoder(
        "0xa9059cbb"
        "000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "00000000000000000000000000000000000000000000000000000000000003e7"
    )
    assert args["value"] == "999"


def test_decoder_cache_is_reused_for_same_abi_id_and_key() -> None:
    """Calling get_event_decoder twice for the same (abi_id, topic0) should
    return the same callable instance (cached)."""
    snap = _snap_with(SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    d1 = r.get_event_decoder("a1", t0)
    d2 = r.get_event_decoder("a1", t0)
    assert d1 is d2
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_registry.py -v`
Expected: 4 new failures.

- [ ] **Step 3: Extend `AbiRegistry` with decoder lookup**

Add to `core/abi/registry.py`:

```python
from collections.abc import Callable
from typing import Any as _Any

from core.abi.decoder import (
    decode_event,
    decode_function_call,
    event_topic0,
    function_selector,
)


EventDecoder = Callable[..., dict[str, _Any]]
CallDecoder = Callable[[str], dict[str, _Any]]


class AbiRegistry:
    # ... existing __init__, refresh, _evict, get_body unchanged ...

    def get_event_decoder(self, abi_id: str, topic0: str) -> EventDecoder:
        key = (abi_id, topic0.lower())
        cached = self._decoders.get(key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        a = self._abis.get(abi_id)
        if a is None:
            raise AbiNotFound(abi_id)

        body = a.body if isinstance(a.body, list) else [a.body]
        for entry in body:
            if entry.get("type") != "event":
                continue
            if event_topic0(entry).lower() == topic0.lower():
                event_abi = entry
                def _decoder(*, topics: list[str], data: str, _ev=event_abi) -> dict[str, _Any]:
                    return decode_event(_ev, topics, data)
                self._decoders[key] = _decoder
                return _decoder
        raise KeyError(f"no event with topic0 {topic0} in abi {abi_id}")

    def get_call_decoder(self, abi_id: str, selector: str) -> CallDecoder:
        key = (abi_id, selector.lower())
        cached = self._decoders.get(key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        a = self._abis.get(abi_id)
        if a is None:
            raise AbiNotFound(abi_id)

        body = a.body if isinstance(a.body, list) else [a.body]
        for entry in body:
            if entry.get("type") != "function":
                continue
            if function_selector(entry).lower() == selector.lower():
                fn_abi = entry
                def _decoder(calldata: str, _fn=fn_abi) -> dict[str, _Any]:
                    return decode_function_call(_fn, calldata)
                self._decoders[key] = _decoder
                return _decoder
        raise KeyError(f"no function with selector {selector} in abi {abi_id}")
```

The default-argument `_ev=event_abi` / `_fn=fn_abi` pattern is the standard Python idiom for "capture the loop variable at closure creation time" — without it the closure would always see the last entry of `body`.

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_registry.py -v`
Expected: all 9 PASS (5 from Task 2.5 + 4 new).

- [ ] **Step 5: Commit**

```bash
git add core/abi/registry.py tests/unit/test_abi_registry.py
git commit -m "feat(abi): registry-level event/call decoder lookup with cache"
```

### Task 2.8: Chunk 2 close-out

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: all M1 tests + chunk-1 tests + chunk-2 tests pass.

- [ ] **Step 2: Lint / type check**

Run: `make lint typecheck`
Expected: clean.

---

## Chunk 3: `Erc20TransferParser`

Decodes standard ERC-20 `Transfer(address,address,uint256)` logs into `kind="token_transfer"` events and wires the parser into every EVM `ChainRunner` pipeline.

**Spec §4.2 / §8 chunk 3 scope:**
- New parser `core/parser/erc20.py` that walks `block.logs`, matches the canonical ERC-20 Transfer topic0, decodes the two indexed address topics and the uint256 data, and emits a `token_transfer` `Event`.
- The signature is fixed — **no `AbiRegistry` dependency** (unlike `AbiEventParser` in chunk 4). This keeps the parser cheap and lets every EVM chain consume it unconditionally.
- ERC-721 reuses the same event name and types (`Transfer(address,address,uint256)`) but with **all three args indexed**, so the topic count differs: ERC-20 = 3 topics (topic0 + 2 indexed), ERC-721 = 4 topics (topic0 + 3 indexed). The parser uses topic count as the discriminator and skips ERC-721 silently.
- Add it to the `ChainRunner._pipeline` constructor alongside the existing `NativeTransferParser`.

**New files this chunk:**
- `core/parser/erc20.py`
- `tests/unit/test_erc20_parser.py`

**Modified files this chunk:**
- `apps/worker/chain_runner.py` — extend `_pipeline` list with `Erc20TransferParser`.
- `tests/unit/test_chain_runner.py` — extend with one test asserting ERC-20 logs in a block produce a dispatched payload.

**Out of scope this chunk:**
- AbiEventParser / EventKind rename — chunk 4.
- The `value` decimal-string convention follows `NativeTransferParser` (Event.args §5.2). No change to `Event.args` shape.
- `arg_filters` validation tightening — chunk 9.

**Constants:**
- ERC-20 Transfer topic0 = keccak256("Transfer(address,address,uint256)") = `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`. The plan hard-codes this hex value rather than computing it at import time so import-side failures (no eth-utils, etc.) can't break the parser.

### Task 3.1: `Erc20TransferParser` core decode

**Files:**
- Create: `core/parser/erc20.py`
- Test: `tests/unit/test_erc20_parser.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_erc20_parser.py
from __future__ import annotations

from core.chains.types import Block, BlockHeader, Log, Tx
from core.parser.erc20 import (
    ERC20_TRANSFER_TOPIC0,
    Erc20TransferParser,
)


_ZERO_ADDR_PAD = "0" * 24   # left-padding for 20-byte address → 32-byte topic
_FROM = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_TO   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_TOKEN_ADDR = "0xcafe000000000000000000000000000000000000"


def _hdr(n: int = 10) -> BlockHeader:
    return BlockHeader(number=n, hash=f"0xh{n}", parent_hash=f"0xh{n-1}", timestamp=1700000000)


def _erc20_log(
    *,
    tx_hash: str = "0xt1",
    log_index: int = 0,
    address: str = _TOKEN_ADDR,
    topic0: str = ERC20_TRANSFER_TOPIC0,
    from_addr_hex: str = _FROM,
    to_addr_hex: str = _TO,
    value_hex: str | None = "0x" + ("0" * 62) + "7b",  # uint256(123)
) -> Log:
    """Build a synthetic ERC-20 Transfer log. `value_hex=None` → empty data."""
    topics = [
        topic0,
        "0x" + _ZERO_ADDR_PAD + from_addr_hex,
        "0x" + _ZERO_ADDR_PAD + to_addr_hex,
    ]
    return Log(
        tx_hash=tx_hash,
        log_index=log_index,
        address=address,
        topics=topics,
        data=value_hex if value_hex is not None else "0x",
    )


def _block(logs: list[Log]) -> Block:
    return Block(
        header=_hdr(10),
        txs=[
            Tx(hash="0xt1", index=0, from_addr="0xf0", to_addr=_TOKEN_ADDR,
               value=0, input="0xa9059cbb", status=1),
        ],
        logs=logs,
    )


def test_decodes_one_erc20_transfer_log() -> None:
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([_erc20_log()])
    events = list(p.parse(blk))
    assert len(events) == 1
    e = events[0]
    assert e.chain_id == "eth-mainnet"
    assert e.kind == "token_transfer"
    assert e.block_number == 10
    assert e.block_hash == "0xh10"
    assert e.tx_hash == "0xt1"
    assert e.tx_index == 0
    assert e.log_index == 0
    assert e.contract == _TOKEN_ADDR
    assert e.name == "Transfer"
    assert e.args == {
        "from":  "0x" + _FROM,
        "to":    "0x" + _TO,
        "value": "123",
    }


def test_emits_one_event_per_matching_log() -> None:
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([
        _erc20_log(log_index=0, value_hex="0x" + "0" * 63 + "1"),
        _erc20_log(log_index=1, value_hex="0x" + "0" * 62 + "0a"),  # 10
    ])
    events = list(p.parse(blk))
    assert [e.log_index for e in events] == [0, 1]
    assert [e.args["value"] for e in events] == ["1", "10"]


def test_skips_logs_with_non_transfer_topic0() -> None:
    other_topic0 = "0x" + "de" * 32
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([_erc20_log(topic0=other_topic0)])
    assert list(p.parse(blk)) == []


def test_skips_erc721_transfers_by_topic_count() -> None:
    """ERC-721 uses the same name+types but indexes ALL THREE args (incl. tokenId),
    producing 4 topics instead of ERC-20's 3. The parser must skip them — emitting
    a `token_transfer` with `value` actually being a tokenId would be semantically
    wrong and would corrupt downstream consumers.
    """
    log_721 = Log(
        tx_hash="0xt1", log_index=0, address=_TOKEN_ADDR,
        topics=[
            ERC20_TRANSFER_TOPIC0,
            "0x" + _ZERO_ADDR_PAD + _FROM,
            "0x" + _ZERO_ADDR_PAD + _TO,
            "0x" + "0" * 62 + "07",  # tokenId = 7
        ],
        data="0x",
    )
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([log_721])
    assert list(p.parse(blk)) == []


def test_skips_logs_with_malformed_data_and_continues() -> None:
    """A log whose `data` is shorter than 32 bytes can't carry a uint256 value.
    The parser must skip it (with a structured log emission) and process the
    remaining valid logs in the same block.
    """
    bad = _erc20_log(log_index=0, value_hex="0xdead")  # 2 bytes < 32
    good = _erc20_log(log_index=1)  # standard 123
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([bad, good])
    events = list(p.parse(blk))
    assert len(events) == 1
    assert events[0].log_index == 1
    assert events[0].args["value"] == "123"


def test_normalizes_topic_addresses_to_lowercase_0x() -> None:
    """Even if a test fixture leaves topic addresses upper-case, the emitted
    args.from / args.to must be 0x-lowercase to match the Event.args convention."""
    upper_from = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    p = Erc20TransferParser(chain_id="eth-mainnet")
    blk = _block([_erc20_log(from_addr_hex=upper_from)])
    e = next(iter(p.parse(blk)))
    assert e.args["from"] == "0x" + upper_from.lower()
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_erc20_parser.py -v`
Expected: `ImportError: cannot import name 'Erc20TransferParser'`.

- [ ] **Step 3: Implement `core/parser/erc20.py`**

```python
# core/parser/erc20.py
"""ERC-20 Transfer log parser.

Decodes the canonical `Transfer(address,address,uint256)` event into a
`token_transfer` Event. No ABI lookup — the signature is fixed.

ERC-721 reuses the same name/types but indexes the tokenId, giving 4 topics
instead of 3; topic-count is the discriminator.

Reverted transactions: this parser does NOT inspect tx.status, because the
EVM only emits logs for successful txs in the first place (failed txs revert
and their logs disappear). Defensive logic kept minimal.
"""
from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.chains.types import Block, Log
from core.parser.event import Event

log = structlog.get_logger(__name__)


# keccak256("Transfer(address,address,uint256)") — well-known constant.
# Hard-coded to keep import-time dependencies minimal (no eth-utils import).
ERC20_TRANSFER_TOPIC0 = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


class Erc20TransferParser:
    """Emit a token_transfer Event for each ERC-20 Transfer log in a block."""

    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for log_entry in block.logs:
            ev = self._try_decode(log_entry, header_number=h.number,
                                  header_hash=h.hash, header_ts=h.timestamp)
            if ev is not None:
                yield ev

    def _try_decode(
        self,
        log_entry: Log,
        *,
        header_number: int,
        header_hash: str,
        header_ts: int,
    ) -> Event | None:
        if not log_entry.topics:
            return None
        if log_entry.topics[0].lower() != ERC20_TRANSFER_TOPIC0:
            return None
        # ERC-20 has exactly 3 topics (topic0 + 2 indexed); ERC-721 has 4.
        if len(log_entry.topics) != 3:
            return None

        data_hex = log_entry.data.removeprefix("0x")
        if len(data_hex) < 64:  # 32 bytes for the uint256 value
            log.warning(
                "erc20_parser.malformed_data",
                tx_hash=log_entry.tx_hash,
                log_index=log_entry.log_index,
                data_len=len(data_hex),
            )
            return None

        try:
            value = int(data_hex[:64], 16)
        except ValueError:
            log.warning(
                "erc20_parser.malformed_value_hex",
                tx_hash=log_entry.tx_hash,
                log_index=log_entry.log_index,
            )
            return None

        return Event(
            chain_id=self._chain_id,
            block_number=header_number,
            block_hash=header_hash,
            block_timestamp=header_ts,
            tx_hash=log_entry.tx_hash,
            tx_index=None,  # we don't have direct tx index from Log alone
            log_index=log_entry.log_index,
            kind="token_transfer",
            contract=log_entry.address.lower(),
            name="Transfer",
            args={
                "from":  _addr_from_topic(log_entry.topics[1]),
                "to":    _addr_from_topic(log_entry.topics[2]),
                "value": str(value),
            },
            raw={
                "tx_hash": log_entry.tx_hash,
                "log_index": log_entry.log_index,
            },
        )


def _addr_from_topic(topic_hex: str) -> str:
    """A 32-byte topic encodes a 20-byte address left-padded with zeros.
    Take the last 40 hex chars and re-add the 0x prefix; lowercase normalised."""
    body = topic_hex.removeprefix("0x").lower()
    if len(body) < 40:
        return "0x" + body  # last-resort safety; let the test fixture be wrong rather than crash
    return "0x" + body[-40:]
```

Note on `tx_index`: `Log` doesn't carry `tx_index` in M1's `core/chains/types.py:14-22`. M1's `NativeTransferParser` reads `tx.index` from `Tx`. Cross-walking `tx_index` from logs would require either (a) extending `Log` with a `tx_index` field or (b) building a `{tx_hash → tx_index}` lookup in the parser. Deferring (b) until any subscription needs `tx_index` for token transfers; the spec §5.2 marks `tx_index` as `int | None`, so `None` is contractually OK for log-derived events.

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_erc20_parser.py -v`
Expected: 6 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/parser/erc20.py tests/unit/test_erc20_parser.py
git commit -m "feat(parser): Erc20TransferParser for canonical ERC-20 Transfer logs"
```

### Task 3.2: Wire `Erc20TransferParser` into `ChainRunner`

Every EVM `ChainRunner` should run both `NativeTransferParser` and `Erc20TransferParser` unconditionally. The Solana split (chunk 11) will introduce a separate Solana pipeline so this list is EVM-only by then; for now the runner is single-chain-kind so the wire is straightforward.

**Files:**
- Modify: `apps/worker/chain_runner.py:75` (extend `_pipeline` list)
- Test: extend `tests/unit/test_chain_runner.py` with an ERC-20 dispatch case.

- [ ] **Step 1: Append the failing test**

```python
# tests/unit/test_chain_runner.py — append (alongside existing tests)
from core.chains.types import Log
from core.parser.erc20 import ERC20_TRANSFER_TOPIC0


def _block_with_erc20_log(n: int, *, value: int = 1000) -> Block:
    """Build a block whose single log is an ERC-20 Transfer for `value` units."""
    pad = "0" * 24
    _from = "aaaa" + "00" * 18
    _to = "bbbb" + "00" * 18
    return Block(
        header=_hdr(n, parent=f"0xh{n-1}" if n > 0 else "0x0"),
        txs=[
            Tx(hash=f"0xt{n}", index=0, from_addr="0xf0", to_addr="0xtoken",
               value=0, input="0xa9059cbb", status=1),
        ],
        logs=[
            Log(
                tx_hash=f"0xt{n}",
                log_index=0,
                address="0xtoken",
                topics=[
                    ERC20_TRANSFER_TOPIC0,
                    "0x" + pad + _from,
                    "0x" + pad + _to,
                ],
                data="0x" + format(value, "064x"),
            ),
        ],
    )


@pytest.mark.asyncio
async def test_chain_runner_dispatches_erc20_token_transfer() -> None:
    chain = _chain()
    blocks = [_block_with_erc20_log(n, value=n * 1000) for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], match_kind="token_transfer")],
        channels=[_ch("c1")],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        # Same confirmation arithmetic as the native-transfer test above.
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        kinds = {c["event"]["kind"] for c in coll.calls}
        assert kinds == {"token_transfer"}
        values = {c["event"]["args"]["value"] for c in coll.calls}
        # block 1 → 1000, block 2 → 2000
        assert values == {"1000", "2000"}
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_dispatches_erc20_token_transfer -v`
Expected: FAIL — `len(coll.calls) == 0` (the existing `_pipeline` only runs `NativeTransferParser`, which yields nothing because the test block carries no value-transferring tx).

- [ ] **Step 3: Add `Erc20TransferParser` to `ChainRunner._pipeline`**

Edit `apps/worker/chain_runner.py`. At the top, import:

```python
from core.parser.erc20 import Erc20TransferParser
```

Then change the `_pipeline` construction at line 75 from:

```python
        self._pipeline = ParserPipeline([NativeTransferParser(chain_id=self._chain.id)])
```

to:

```python
        self._pipeline = ParserPipeline([
            NativeTransferParser(chain_id=self._chain.id),
            Erc20TransferParser(chain_id=self._chain.id),
        ])
```

Order matters for stable test output but the matcher is keyed on `(chain_id, kind)`, so the order doesn't affect routing correctness.

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_chain_runner.py -v`
Expected: all existing native-transfer tests PASS + new ERC-20 test PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(runner): wire Erc20TransferParser into the EVM pipeline"
```

### Task 3.3: Chunk 3 close-out

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: all M1 + chunk-1 + chunk-2 + chunk-3 tests pass.

- [ ] **Step 2: Lint / type check**

Run: `make lint typecheck`
Expected: clean.

---

