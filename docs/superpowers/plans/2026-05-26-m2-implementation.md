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

## Chunk 4: `EventKind` rename + `AbiEventParser`

Closes spec §4.9's `EventKind ↔ MatchKind` drift and introduces the generic ABI-driven event parser. M1 left `EventKind = Literal["native_transfer", "token_transfer", "log", "call"]` (core/parser/event.py:6) while `MatchKind` (core/config/models.py:36) declared `event` instead of `log`. The matcher keys on string equality, so any future `AbiEventParser` emitting `kind="log"` would have silently never matched a subscription with `match_kind="event"`. This chunk fixes that and lights up the matcher's `event`-kind path.

**Spec §4.9, §4.2, §4.1, §6 scope:**
- Rename `EventKind.log → event`. Update the only known internal call site (`tests/unit/test_pipeline.py:22`). No DB / API schema change.
- Implement `AbiEventParser` (`core/parser/abi_event.py`) that walks `block.logs`, looks up each topic0 in the `AbiRegistry`, and emits:
  - **Match path:** `kind="event"`, `name=<event_name>`, `args=<decoded>`, `contract=log.address` for known topic0s.
  - **Downgrade path:** `kind="event"`, `name=None`, `args={}`, `contract=log.address`, raw log payload preserved in `event.raw` for unknown topic0s (spec §4.1, §6).
- Add an `AbiRegistry.lookup_event_by_topic0` method backed by a `topic0 → (abi_id, event_name)` index built at refresh time.
- Wire `AbiRegistry` into `_Worker` as a singleton: build once in `__init__`, refresh on each `_reconcile(snap)` **before** dispatching to runners (spec §4.1 deferred-from-chunk-2 hook).
- Thread the registry into `ChainRunner` via an optional kwarg; when set, the runner instantiates `AbiEventParser` alongside `NativeTransferParser` and `Erc20TransferParser`.

**Interaction with chunk 3 (`Erc20TransferParser`):**
`Erc20TransferParser` emits `kind="token_transfer"` and `AbiEventParser` emits `kind="event"` for the same ERC-20 Transfer log when an ABI references it. Both events flow downstream; subscriptions route by `match_kind`. There is no de-dup — that's by design (a sub keyed on `token_transfer` and another keyed on `event` both fire). Coverage of this overlap lives in the chunk-4 ChainRunner integration test (Task 4.5).

**New files this chunk:**
- `core/parser/abi_event.py`
- `tests/unit/test_abi_event_parser.py`

**Modified files this chunk:**
- `core/parser/event.py` — `EventKind` literal rename.
- `tests/unit/test_pipeline.py` — single `kind="log" → "event"` fix.
- `core/abi/registry.py` — `_topic0_index` build + `lookup_event_by_topic0`.
- `tests/unit/test_abi_registry.py` — `lookup_event_by_topic0` tests.
- `apps/worker/chain_runner.py` — optional `abi_registry` constructor kwarg + parser-list extension.
- `apps/worker/main.py` — `_Worker._registry` field, instantiation, and refresh call from `_reconcile`.
- `tests/unit/test_chain_runner.py` — extend with one ABI-driven dispatch test.

**Out of scope this chunk:**
- `AbiCallParser` — chunk 5.
- Address-scoped topic0 lookup (subscription with `address` set) — handled by the matcher's existing address filter; the parser does not pre-filter.
- Collision handling for two ABIs declaring the same topic0 — the registry's `_topic0_index` is first-write-wins with a warning log; subscribers needing multi-ABI fan-out are M3+ territory.

### Task 4.1: Rename `EventKind.log → event`

**Files:**
- Modify: `core/parser/event.py:6`
- Modify: `tests/unit/test_pipeline.py:22` (only call site)

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_event.py` (the existing event-dataclass smoke-test file):

```python
# tests/unit/test_event.py — append
from typing import get_args

from core.parser.event import EventKind


def test_event_kind_literal_contains_event_not_log() -> None:
    """Spec §4.9 rename: matcher uses `match_kind="event"`, so the parser
    enum must expose `event` (NOT `log`) to align."""
    kinds = set(get_args(EventKind))
    assert "event" in kinds
    assert "log" not in kinds
    # Other M1 kinds must still be present.
    assert {"native_transfer", "token_transfer", "call"}.issubset(kinds)
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_event.py::test_event_kind_literal_contains_event_not_log -v`
Expected: FAIL — `"log" in kinds` and `"event" not in kinds`.

- [ ] **Step 3: Rename in `core/parser/event.py:6`**

Change:

```python
EventKind = Literal["native_transfer", "token_transfer", "log", "call"]
```

to:

```python
EventKind = Literal["native_transfer", "token_transfer", "event", "call"]
```

- [ ] **Step 4: Fix the one M1 call site**

Edit `tests/unit/test_pipeline.py:22`. Change:

```python
            kind="log",
```

to:

```python
            kind="event",
```

(There are no other `kind="log"` references in the codebase — confirm with `grep -rn 'kind="log"' core/ apps/ tests/` before proceeding; expected: zero hits.)

- [ ] **Step 5: Run, expect PASS.**

Run: `pytest tests/unit/test_event.py tests/unit/test_pipeline.py -v`
Expected: both files PASS.

- [ ] **Step 6: Full suite regression sweep**

Run: `pytest tests/ -v`
Expected: no failures introduced by the rename.

- [ ] **Step 7: Commit**

```bash
git add core/parser/event.py tests/unit/test_event.py tests/unit/test_pipeline.py
git commit -m "refactor(event): rename EventKind.log → event to match MatchKind"
```

### Task 4.2: `AbiRegistry.lookup_event_by_topic0`

The parser needs an O(1) "given a topic0, which ABI declares this event?" lookup. Build it at refresh time as `_topic0_index: dict[str, tuple[abi_id, event_name]]`; on collisions, first-write-wins with a warning log.

**Files:**
- Modify: `core/abi/registry.py`
- Test: extend `tests/unit/test_abi_registry.py`

- [ ] **Step 1: Append the failing test**

```python
# tests/unit/test_abi_registry.py — append

_SECOND_EVENT = {
    "type": "event", "name": "Approval",
    "inputs": [
        {"name": "owner",   "type": "address", "indexed": True},
        {"name": "spender", "type": "address", "indexed": True},
        {"name": "value",   "type": "uint256", "indexed": False},
    ],
}


def test_lookup_event_by_topic0_returns_decoder_for_known_topic() -> None:
    snap = _snap_with(SnapshotAbi(
        id="a1", name="erc20", kind="evm_abi",
        body=[_ERC20_TRANSFER, _SECOND_EVENT],
    ))
    r = AbiRegistry()
    r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    result = r.lookup_event_by_topic0(t0)
    assert result is not None
    name, decoder = result
    assert name == "Transfer"
    args = decoder(
        topics=[
            t0,
            "0x000000000000000000000000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0x000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ],
        data="0x000000000000000000000000000000000000000000000000000000000000007b",
    )
    assert args["value"] == "123"


def test_lookup_event_by_topic0_returns_none_for_unknown() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_ERC20_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    assert r.lookup_event_by_topic0("0xdead" + "00" * 30) is None


def test_lookup_event_picks_first_abi_on_topic0_collision(caplog) -> None:
    """Two ABIs both declare Transfer with the same canonical signature →
    same topic0. The registry keeps the first-encountered abi_id and logs
    the collision; lookups consistently use the first."""
    snap = _snap_with(
        SnapshotAbi(id="a1", name="erc20a", kind="evm_abi", body=[_ERC20_TRANSFER]),
        SnapshotAbi(id="a2", name="erc20b", kind="evm_abi", body=[_ERC20_TRANSFER]),
    )
    r = AbiRegistry()
    with caplog.at_level("WARNING"):
        r.refresh(snap)
    t0 = event_topic0(_ERC20_TRANSFER)
    result = r.lookup_event_by_topic0(t0)
    assert result is not None
    # Stability: looking up twice returns the same decoder (cache hit).
    assert r.lookup_event_by_topic0(t0) is result


def test_topic0_index_rebuilt_on_abi_removal() -> None:
    """After an abi is removed, its events should no longer be looked-up-able."""
    snap_with = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_ERC20_TRANSFER]))
    snap_without = _snap_with()
    r = AbiRegistry()
    r.refresh(snap_with)
    t0 = event_topic0(_ERC20_TRANSFER)
    assert r.lookup_event_by_topic0(t0) is not None
    r.refresh(snap_without)
    assert r.lookup_event_by_topic0(t0) is None
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_registry.py -k lookup_event_by_topic0 -v`
Expected: `AttributeError: 'AbiRegistry' object has no attribute 'lookup_event_by_topic0'`.

- [ ] **Step 3: Add `_topic0_index` build + `lookup_event_by_topic0`**

Edit `core/abi/registry.py`. In `__init__`, add the new instance attribute:

```python
        self._topic0_index: dict[str, tuple[str, str]] = {}  # topic0 → (abi_id, event_name)
```

In `refresh()`, after the existing eviction loop and the `self._abis = new_abis` assignment, rebuild the index. The full updated `refresh` body is:

```python
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
        self._rebuild_topic0_index()
        log.info("abi_registry.refreshed", count=len(new_abis))

    def _rebuild_topic0_index(self) -> None:
        idx: dict[str, tuple[str, str]] = {}
        for abi_id, abi in self._abis.items():
            body = abi.body if isinstance(abi.body, list) else [abi.body]
            for entry in body:
                if entry.get("type") != "event":
                    continue
                try:
                    t0 = event_topic0(entry).lower()
                except Exception:  # noqa: BLE001 — malformed ABI entry, skip
                    log.warning(
                        "abi_registry.topic0_compute_failed",
                        abi_id=abi_id,
                        event=entry.get("name"),
                    )
                    continue
                if t0 in idx:
                    log.warning(
                        "abi_registry.topic0_collision",
                        topic0=t0,
                        first=idx[t0],
                        second=(abi_id, entry.get("name")),
                    )
                    continue  # first-write-wins
                idx[t0] = (abi_id, entry.get("name", ""))
        self._topic0_index = idx
```

Then add the `lookup_event_by_topic0` method (place it next to `get_event_decoder` for cohesion):

```python
    def lookup_event_by_topic0(
        self, topic0: str
    ) -> tuple[str, EventDecoder] | None:
        """Resolve a log's `topics[0]` to a `(event_name, decoder)` pair.

        Returns None if no known ABI declares an event with this topic0.
        The decoder return value reuses the per-`(abi_id, topic0)` cache
        from `get_event_decoder`, so repeated calls for the same topic
        return the same callable identity.
        """
        entry = self._topic0_index.get(topic0.lower())
        if entry is None:
            return None
        abi_id, event_name = entry
        decoder = self.get_event_decoder(abi_id, topic0)
        return event_name, decoder
```

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_registry.py -v`
Expected: all existing tests + 4 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add core/abi/registry.py tests/unit/test_abi_registry.py
git commit -m "feat(abi): topic0 index + lookup_event_by_topic0 with collision handling"
```

### Task 4.3: `AbiEventParser` (match + downgrade)

The parser walks `block.logs` and emits one `kind="event"` Event per log: with `name=<event_name>` + decoded args when topic0 is known, and with `name=None` + empty args + raw payload preserved when topic0 is unknown.

**Files:**
- Create: `core/parser/abi_event.py`
- Test: `tests/unit/test_abi_event_parser.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_abi_event_parser.py
from __future__ import annotations

from core.abi.decoder import event_topic0
from core.abi.registry import AbiRegistry
from core.chains.types import Block, BlockHeader, Log, Tx
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.abi_event import AbiEventParser


_ERC20_TRANSFER = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}
_FROM = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_TO   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
_PAD = "0" * 24
_TOPIC0 = event_topic0(_ERC20_TRANSFER)


def _registry_with_transfer() -> AbiRegistry:
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_ERC20_TRANSFER])],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _block(logs: list[Log]) -> Block:
    return Block(
        header=BlockHeader(number=42, hash="0xh42", parent_hash="0xh41", timestamp=1700000000),
        txs=[Tx(hash="0xtA", index=0, from_addr="0xf", to_addr="0xt",
                value=0, input="0x", status=1)],
        logs=logs,
    )


def _transfer_log(topic0: str = _TOPIC0, value_hex: str = "0x" + "0" * 62 + "7b") -> Log:
    return Log(
        tx_hash="0xtA", log_index=0, address="0xcafe",
        topics=[topic0, "0x" + _PAD + _FROM, "0x" + _PAD + _TO],
        data=value_hex,
    )


def test_emits_event_kind_for_known_topic0_with_decoded_args() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    events = list(p.parse(_block([_transfer_log()])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "event"
    assert e.name == "Transfer"
    assert e.contract == "0xcafe"
    assert e.args == {
        "from":  "0x" + _FROM,
        "to":    "0x" + _TO,
        "value": "123",
    }
    assert e.chain_id == "eth-mainnet"
    assert e.block_number == 42
    assert e.tx_hash == "0xtA"
    assert e.log_index == 0


def test_downgrades_unknown_topic0_to_event_with_name_none() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    unknown_topic0 = "0xdead" + "00" * 30
    events = list(p.parse(_block([_transfer_log(topic0=unknown_topic0)])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "event"
    assert e.name is None
    assert e.args == {}
    # Raw payload preserved per spec §4.1.
    assert e.raw["topics"][0] == unknown_topic0
    assert e.raw["data"].startswith("0x")
    assert e.contract == "0xcafe"


def test_downgrade_when_decode_raises() -> None:
    """A known topic0 whose data fails to decode (e.g. malformed data length)
    also downgrades — the matcher still gets a kind=event row with name=None
    and the raw payload, per spec §6 'ABI decode failure'."""
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    bad = Log(
        tx_hash="0xtA", log_index=1, address="0xcafe",
        topics=[_TOPIC0, "0x" + _PAD + _FROM, "0x" + _PAD + _TO],
        data="0xdead",  # < 32 bytes for the uint256 → eth-abi raises
    )
    events = list(p.parse(_block([bad])))
    assert len(events) == 1
    assert events[0].kind == "event"
    assert events[0].name is None
    assert events[0].raw["topics"][0] == _TOPIC0


def test_empty_topics_log_is_skipped_not_downgraded() -> None:
    """An anonymous-event log (topics=[]) carries no topic0 to scan against.
    We skip these entirely rather than downgrade — they can't realistically
    represent a subscribable event and would just noise up the pipeline.
    """
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    anon = Log(tx_hash="0xtA", log_index=0, address="0xcafe", topics=[], data="0xff")
    events = list(p.parse(_block([anon])))
    assert events == []


def test_emits_one_event_per_log() -> None:
    reg = _registry_with_transfer()
    p = AbiEventParser(chain_id="eth-mainnet", registry=reg)
    logs = [
        _transfer_log(value_hex="0x" + "0" * 63 + "1"),
        Log(tx_hash="0xtA", log_index=1, address="0xcafe",
            topics=["0xfeed" + "00" * 30], data="0x"),
    ]
    events = list(p.parse(_block(logs)))
    assert [e.name for e in events] == ["Transfer", None]
    assert [e.kind for e in events] == ["event", "event"]
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_event_parser.py -v`
Expected: `ImportError: cannot import name 'AbiEventParser'`.

- [ ] **Step 3: Implement `core/parser/abi_event.py`**

```python
# core/parser/abi_event.py
"""Generic ABI-driven event log parser.

Per spec §4.1 / §4.2 / §6:
- Match: emit `kind="event"`, `name=<event_name>`, decoded args.
- Downgrade (unknown topic0 OR decode failure): emit `kind="event"`,
  `name=None`, `args={}`, raw log payload preserved in `event.raw`.

The parser does NOT pre-bind to specific ABI IDs — it consults the
shared `AbiRegistry` on every log, so newly-added ABIs become observable
on the very next block without rebuilding the pipeline.
"""
from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import Block, Log
from core.parser.event import Event

log = structlog.get_logger(__name__)


class AbiEventParser:
    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for log_entry in block.logs:
            ev = self._handle_log(log_entry, h.number, h.hash, h.timestamp)
            if ev is not None:
                yield ev

    def _handle_log(
        self,
        log_entry: Log,
        block_number: int,
        block_hash: str,
        block_ts: int,
    ) -> Event | None:
        if not log_entry.topics:
            return None
        topic0 = log_entry.topics[0]

        lookup = self._registry.lookup_event_by_topic0(topic0)
        if lookup is None:
            return self._downgraded(log_entry, block_number, block_hash, block_ts)

        event_name, decoder = lookup
        try:
            args = decoder(topics=log_entry.topics, data=log_entry.data)
        except DecodeFailed as exc:
            log.warning(
                "abi_event_parser.decode_failed",
                topic0=topic0,
                event=event_name,
                tx_hash=log_entry.tx_hash,
                error=str(exc),
            )
            return self._downgraded(log_entry, block_number, block_hash, block_ts)

        return Event(
            chain_id=self._chain_id,
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            tx_hash=log_entry.tx_hash,
            tx_index=None,
            log_index=log_entry.log_index,
            kind="event",
            contract=log_entry.address.lower(),
            name=event_name,
            args=args,
            raw={
                "tx_hash": log_entry.tx_hash,
                "log_index": log_entry.log_index,
            },
        )

    def _downgraded(
        self,
        log_entry: Log,
        block_number: int,
        block_hash: str,
        block_ts: int,
    ) -> Event:
        return Event(
            chain_id=self._chain_id,
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            tx_hash=log_entry.tx_hash,
            tx_index=None,
            log_index=log_entry.log_index,
            kind="event",
            contract=log_entry.address.lower(),
            name=None,
            args={},
            raw={
                "tx_hash": log_entry.tx_hash,
                "log_index": log_entry.log_index,
                "topics": list(log_entry.topics),
                "data": log_entry.data,
            },
        )
```

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_event_parser.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/parser/abi_event.py tests/unit/test_abi_event_parser.py
git commit -m "feat(parser): AbiEventParser with topic0 lookup + decode-failure downgrade"
```

### Task 4.4: Thread `AbiRegistry` through `ChainRunner`

`ChainRunner` accepts an optional `abi_registry` constructor kwarg. When provided, the pipeline appends `AbiEventParser`. When omitted (legacy tests, no-ABI scenarios), the pipeline is unchanged.

**Files:**
- Modify: `apps/worker/chain_runner.py`

- [ ] **Step 1: Append a failing test that asserts the parser is wired**

Append to `tests/unit/test_chain_runner.py`:

```python
# tests/unit/test_chain_runner.py — append
from core.abi.decoder import event_topic0 as _event_topic0
from core.abi.registry import AbiRegistry as _AbiRegistry
from core.config.snapshot import SnapshotAbi as _SnapshotAbi
from core.parser.abi_event import AbiEventParser as _AbiEventParser


_TRANSFER_ABI = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from",  "type": "address", "indexed": True},
        {"name": "to",    "type": "address", "indexed": True},
        {"name": "value", "type": "uint256", "indexed": False},
    ],
}


def test_chain_runner_pipeline_includes_abi_event_parser_when_registry_given() -> None:
    """Construction-time wiring check: passing `abi_registry=` adds an
    AbiEventParser to the pipeline. Without it, the pipeline stays at
    native + erc20.
    """
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    ))
    chain = _chain()
    runner_with = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
        abi_registry=reg,
    )
    types_with = [type(p).__name__ for p in runner_with._pipeline._parsers]
    assert "AbiEventParser" in types_with

    runner_without = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    types_without = [type(p).__name__ for p in runner_without._pipeline._parsers]
    assert "AbiEventParser" not in types_without
    # Sanity: native + erc20 are still there in both cases.
    assert "NativeTransferParser" in types_with and "NativeTransferParser" in types_without
    assert "Erc20TransferParser" in types_with and "Erc20TransferParser" in types_without
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_pipeline_includes_abi_event_parser_when_registry_given -v`
Expected: `TypeError: ChainRunner.__init__() got an unexpected keyword argument 'abi_registry'`.

- [ ] **Step 3: Add `abi_registry` to `ChainRunner.__init__`**

Edit `apps/worker/chain_runner.py`:

1. Add import alongside `NativeTransferParser` / `Erc20TransferParser`:

```python
from core.abi.registry import AbiRegistry
from core.parser.abi_event import AbiEventParser
```

2. Extend the `__init__` signature with `abi_registry: AbiRegistry | None = None,`:

```python
    def __init__(
        self,
        *,
        chain: SnapshotChain,
        adapter_factory: AdapterFactory,
        channel_factory: ChannelFactory,
        checkpoint_repo: _CheckpointRepo,
        notifier_max_concurrency: int = 50,
        abi_registry: AbiRegistry | None = None,
    ) -> None:
```

3. Build the parser list conditionally:

```python
        parsers: list[Parser] = [
            NativeTransferParser(chain_id=self._chain.id),
            Erc20TransferParser(chain_id=self._chain.id),
        ]
        if abi_registry is not None:
            parsers.append(AbiEventParser(chain_id=self._chain.id, registry=abi_registry))
        self._pipeline = ParserPipeline(parsers)
```

You'll also need `from core.parser.base import Parser` (M1's `chain_runner.py` doesn't currently import it — add it next to the other parser imports so the `parsers: list[Parser] = [...]` annotation type-checks).

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_chain_runner.py -v`
Expected: existing tests still pass + new wiring test passes.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(runner): optional abi_registry kwarg threads AbiEventParser into pipeline"
```

### Task 4.5: `_Worker` owns the `AbiRegistry` and refreshes it on reconcile

The worker holds the single `AbiRegistry` instance and calls `registry.refresh(snap)` at the top of `_reconcile` so all runners observe the same ABI state for a given snapshot version.

**Files:**
- Modify: `apps/worker/main.py`
- Test: extend `tests/integration/test_worker_config_reload.py` with an ABI-CRUD reload case.

- [ ] **Step 1: Write the failing integration test**

Append to `tests/integration/test_worker_config_reload.py` (or create a new test file `tests/integration/test_worker_abi_reload.py` if the existing file is already large — pick whichever keeps each file under ~400 lines). The added test asserts that:

1. Worker boots with an empty ABI set; registry is empty.
2. POST `/api/abis` creates an ABI.
3. Worker observes the new snapshot via Redis pub/sub.
4. `worker._registry.lookup_event_by_topic0(<known>)` resolves to the new ABI.

```python
# tests/integration/test_worker_abi_reload.py (new file)
from __future__ import annotations

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import _Worker
from core.abi.decoder import event_topic0
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


_TRANSFER = {
    "type": "event", "name": "Transfer",
    "inputs": [
        {"name": "from", "type": "address", "indexed": True},
        {"name": "to",   "type": "address", "indexed": True},
        {"name": "value","type": "uint256", "indexed": False},
    ],
}


async def test_worker_registry_picks_up_new_abi_on_config_reload(
    tmp_path, redis_url: str
) -> None:
    """E2E-style integration: API creates an ABI, worker's _registry sees it
    on the very next reconcile cycle."""
    db_url = f"sqlite+aiosqlite:///{tmp_path / 'abi.sqlite'}"
    db = Database(db_url)
    await db.connect()
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    settings = Settings(database={"url": db_url}, redis={"url": redis_url})
    worker = _Worker(settings)
    await worker.start()
    run_task = asyncio.create_task(worker.run())

    # Give the watcher a moment to flush the initial (empty) snapshot.
    await asyncio.sleep(0.3)

    bus = RedisBus(url=redis_url)
    await bus.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: db
        app.dependency_overrides[get_bus] = lambda: bus

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
            r = await c.post("/api/abis", json={
                "name": "erc20", "kind": "evm_abi", "body": [_TRANSFER],
            })
            assert r.status_code == 201, r.text

        # Wait up to 3 seconds for the worker to observe the bumped version.
        target = event_topic0(_TRANSFER).lower()
        for _ in range(60):
            result = worker._registry.lookup_event_by_topic0(target)
            if result is not None:
                break
            await asyncio.sleep(0.05)
        assert worker._registry.lookup_event_by_topic0(target) is not None
    finally:
        await bus.disconnect()
        await worker.shutdown()
        run_task.cancel()
        try:
            await run_task
        except asyncio.CancelledError:
            pass
        await db.disconnect()
```

The `redis_url` fixture is provided by `tests/conftest.py` (M1).

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/integration/test_worker_abi_reload.py -v`
Expected: `AttributeError: '_Worker' object has no attribute '_registry'`.

- [ ] **Step 3: Add `_registry` to `_Worker` and call `refresh` from `_reconcile`**

Edit `apps/worker/main.py`:

1. Import at the top:

```python
from core.abi.registry import AbiRegistry
```

2. In `_Worker.__init__`, add the registry alongside `_db`/`_bus`:

```python
        self._registry = AbiRegistry()
```

3. In `_reconcile`, refresh the registry **before** iterating chains so each runner constructed in this pass picks up the fresh state:

```python
    async def _reconcile(self, snap: ConfigSnapshot) -> None:
        self._registry.refresh(snap)
        enabled = {c.id: c for c in snap.chains}
        # ... rest unchanged ...
```

4. Pass the registry into new `ChainRunner` instances (in the `else` branch of the existing reconcile loop):

```python
                runner = ChainRunner(
                    chain=cfg,
                    adapter_factory=_default_adapter_factory,
                    channel_factory=_default_channel_factory,
                    checkpoint_repo=self._checkpoint_adapter,
                    abi_registry=self._registry,
                )
```

Existing runners (`if chain_id in self._runners` branch) continue to use the registry they were given at construction time — same instance, same refresh — so they see the new ABIs automatically.

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/integration/test_worker_abi_reload.py -v`
Expected: PASS within ~3 seconds.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/main.py tests/integration/test_worker_abi_reload.py
git commit -m "feat(worker): own AbiRegistry; refresh on every reconcile"
```

### Task 4.6: End-to-end AbiEventParser dispatch through `ChainRunner`

A high-level test: feed a block containing an ERC-20 Transfer log into a `ChainRunner` constructed with a populated `AbiRegistry` and a subscription `match_kind="event"`. The notifier should receive a payload with `name="Transfer"` and decoded args.

**Files:**
- Test: extend `tests/unit/test_chain_runner.py`

- [ ] **Step 1: Append the test**

```python
# tests/unit/test_chain_runner.py — append
@pytest.mark.asyncio
async def test_chain_runner_dispatches_abi_event_match() -> None:
    """ChainRunner with an AbiRegistry containing ERC-20 Transfer fires
    a `kind="event", name="Transfer"` dispatch for the subscription."""
    chain = _chain()
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    ))
    blocks = [_block_with_erc20_log(n, value=n * 100) for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], match_kind="event", match_name="Transfer")],
        channels=[_ch("c1")],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
        abi_registry=reg,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        names = {c["event"]["name"] for c in coll.calls}
        assert names == {"Transfer"}
        kinds = {c["event"]["kind"] for c in coll.calls}
        assert kinds == {"event"}
        # value is decoded by the registry's EVM event decoder.
        values = {c["event"]["args"]["value"] for c in coll.calls}
        assert values == {"100", "200"}
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
```

- [ ] **Step 2: Run, expect PASS**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_dispatches_abi_event_match -v`
Expected: PASS. (The implementation work was finished in Task 4.4; this test is a regression seal.)

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_chain_runner.py
git commit -m "test(runner): end-to-end ABI-event dispatch with populated registry"
```

### Task 4.7: Chunk 4 close-out

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: all M1 + chunk-1 + chunk-2 + chunk-3 + chunk-4 tests pass.

- [ ] **Step 2: Lint / type check**

Run: `make lint typecheck`
Expected: clean.

---

## Chunk 5: `AbiCallParser`

Closes spec §3 and §4.2's "ABI call" gap. Chunks 2 and 4 already shipped the `AbiRegistry` decoder cache and the topic0 lookup; chunk 5 adds the **selector** lookup and the call-side parser. The shape mirrors chunk 4 but on the `block.txs` axis instead of `block.logs`, with one important contract difference: **unknown selectors are skipped, not downgraded** (spec §4.2: "asserts unknown-selector skip (NOT downgrade — call parser is opt-in per subscription)").

**Spec §3, §4.2, §6 scope:**
- Add `AbiRegistry.lookup_function_by_selector(selector) → (fn_name, CallDecoder) | None` backed by a `_selector_index: dict[selector, (abi_id, fn_name)]` built at refresh time (same collision rule as chunk 4: first-write-wins + warning log).
- Implement `AbiCallParser` (`core/parser/abi_call.py`) that walks `block.txs`, extracts the first 4 bytes of `tx.input` as the selector, looks it up in the registry, and emits `Event(kind="call", name=<fn_name>, args=<decoded>, contract=tx.to_addr.lower())` for matches.
- **Skip rules (no event emitted):** tx with `status != 1` (failed); `input` shorter than 10 chars (`"0x" + 4 bytes`); unknown selector (per spec §4.2); decode failure on a known selector (log warning + skip — the spec's "downgrade" path applies only to events, not calls).
- Extend `ChainRunner` so that when `abi_registry` is supplied it appends **both** `AbiEventParser` (chunk 4) **and** `AbiCallParser` to the pipeline.

**Interaction with chunk 4 (`AbiEventParser`):**
A single ERC-20 `transfer(...)` tx produces both a `kind="event"` (from `AbiEventParser` decoding the emitted `Transfer` log) and a `kind="call"` (from `AbiCallParser` decoding the calldata) — plus the `kind="token_transfer"` from chunk 3. All three flow downstream; the matcher routes by `match_kind`. There is no de-dup — that's by design (a sub keyed on `call` matches the calldata; one keyed on `event` matches the receipt log; one keyed on `token_transfer` matches the standardised ERC-20 shape).

**Interaction with chunk 4's `_Worker._registry`:**
Zero changes needed. The worker already builds the registry, calls `refresh(snap)` at the top of `_reconcile`, and passes the registry to every `ChainRunner`. As soon as `ChainRunner.__init__` extends the parser list to also append `AbiCallParser`, the call-side path lights up using the same registry instance.

**New files this chunk:**
- `core/parser/abi_call.py`
- `tests/unit/test_abi_call_parser.py`

**Modified files this chunk:**
- `core/abi/registry.py` — `_selector_index` build + `lookup_function_by_selector`.
- `tests/unit/test_abi_registry.py` — `lookup_function_by_selector` tests.
- `apps/worker/chain_runner.py` — append `AbiCallParser` to the parser list alongside `AbiEventParser`.
- `tests/unit/test_chain_runner.py` — extend with one ABI-call dispatch test.

**Out of scope this chunk:**
- Anchor IDL call decoding (spec §2 explicitly defers; the design notes "AnchorIdlEventParser handles 80%+ of observability use cases").
- Internal calls / sub-calls — M2 parses the top-level `tx.input` only. Tracing internal calls would require `trace_block` RPC, which is non-standard and out of scope.
- Multicall / batched call decoding — a `multicall(bytes[])` tx surfaces as `name="multicall"` with the raw bytes array in `args["data"]`; subscribers needing per-inner-call routing roll their own filter.

### Task 5.1: `AbiRegistry.lookup_function_by_selector`

Parallel to chunk 4's topic0 path: build a `selector → (abi_id, fn_name)` index at refresh time, expose a `lookup_function_by_selector` method that reuses the per-`(abi_id, selector)` decoder cache from `get_call_decoder`.

**Files:**
- Modify: `core/abi/registry.py`
- Test: extend `tests/unit/test_abi_registry.py`

- [ ] **Step 1: Append the failing tests**

```python
# tests/unit/test_abi_registry.py — append (at end of file)

# `_FN_TRANSFER` was already added in Task 2.7. We re-use it here.

_FN_APPROVE = {
    "type": "function", "name": "approve",
    "inputs": [
        {"name": "spender", "type": "address"},
        {"name": "value",   "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}


def test_lookup_function_by_selector_returns_decoder_for_known_selector() -> None:
    snap = _snap_with(SnapshotAbi(
        id="a1", name="erc20", kind="evm_abi",
        body=[_FN_TRANSFER, _FN_APPROVE],
    ))
    r = AbiRegistry()
    r.refresh(snap)
    sel = function_selector(_FN_TRANSFER)
    result = r.lookup_function_by_selector(sel)
    assert result is not None
    name, decoder = result
    assert name == "transfer"
    args = decoder(
        "0xa9059cbb"
        "000000000000000000000000bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        "00000000000000000000000000000000000000000000000000000000000003e7"
    )
    assert args["to"] == "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    assert args["value"] == "999"


def test_lookup_function_by_selector_returns_none_for_unknown() -> None:
    snap = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_FN_TRANSFER]))
    r = AbiRegistry()
    r.refresh(snap)
    assert r.lookup_function_by_selector("0xdeadbeef") is None


def test_lookup_function_picks_first_abi_on_selector_collision(caplog) -> None:
    """Two ABIs both declare `transfer(address,uint256)` → same 4-byte
    selector. First-write-wins with a warning log; stability across
    repeated lookups."""
    snap = _snap_with(
        SnapshotAbi(id="a1", name="erc20a", kind="evm_abi", body=[_FN_TRANSFER]),
        SnapshotAbi(id="a2", name="erc20b", kind="evm_abi", body=[_FN_TRANSFER]),
    )
    r = AbiRegistry()
    with caplog.at_level("WARNING"):
        r.refresh(snap)
    sel = function_selector(_FN_TRANSFER)
    first = r.lookup_function_by_selector(sel)
    assert first is not None
    # Stability: looking up twice returns the same decoder identity.
    assert r.lookup_function_by_selector(sel) is first


def test_selector_index_rebuilt_on_abi_removal() -> None:
    """After an ABI is removed, its functions should no longer be looked-up-able."""
    snap_with = _snap_with(SnapshotAbi(id="a1", name="x", kind="evm_abi", body=[_FN_TRANSFER]))
    snap_without = _snap_with()
    r = AbiRegistry()
    r.refresh(snap_with)
    sel = function_selector(_FN_TRANSFER)
    assert r.lookup_function_by_selector(sel) is not None
    r.refresh(snap_without)
    assert r.lookup_function_by_selector(sel) is None
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_registry.py -k lookup_function_by_selector -v`
Expected: `AttributeError: 'AbiRegistry' object has no attribute 'lookup_function_by_selector'`.

- [ ] **Step 3: Add `_selector_index` build + `lookup_function_by_selector`**

Edit `core/abi/registry.py`. In `__init__`, add the new instance attribute alongside `_topic0_index` (added in chunk 4):

```python
        self._selector_index: dict[str, tuple[str, str]] = {}  # selector → (abi_id, fn_name)
```

In `refresh()`, after the existing `self._rebuild_topic0_index()` call, add the symmetric call:

```python
        self._rebuild_topic0_index()
        self._rebuild_selector_index()
```

Add the new index-building method next to `_rebuild_topic0_index` for cohesion:

```python
    def _rebuild_selector_index(self) -> None:
        idx: dict[str, tuple[str, str]] = {}
        for abi_id, abi in self._abis.items():
            body = abi.body if isinstance(abi.body, list) else [abi.body]
            for entry in body:
                if entry.get("type") != "function":
                    continue
                try:
                    sel = function_selector(entry).lower()
                except Exception:  # noqa: BLE001 — malformed ABI entry, skip
                    log.warning(
                        "abi_registry.selector_compute_failed",
                        abi_id=abi_id,
                        function=entry.get("name"),
                    )
                    continue
                if sel in idx:
                    log.warning(
                        "abi_registry.selector_collision",
                        selector=sel,
                        first=idx[sel],
                        second=(abi_id, entry.get("name")),
                    )
                    continue  # first-write-wins
                idx[sel] = (abi_id, entry.get("name", ""))
        self._selector_index = idx
```

Add the `lookup_function_by_selector` method next to `lookup_event_by_topic0`:

```python
    def lookup_function_by_selector(
        self, selector: str
    ) -> tuple[str, CallDecoder] | None:
        """Resolve a tx's 4-byte calldata selector to a `(fn_name, decoder)` pair.

        Returns None if no known ABI declares a function with this selector.
        The decoder return value reuses the per-`(abi_id, selector)` cache
        from `get_call_decoder`, so repeated calls return the same callable.
        """
        entry = self._selector_index.get(selector.lower())
        if entry is None:
            return None
        abi_id, fn_name = entry
        decoder = self.get_call_decoder(abi_id, selector)
        return fn_name, decoder
```

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_registry.py -v`
Expected: all existing tests + 4 new tests pass (13 total: 5 from Task 2.5 + 4 from Task 2.7 + 4 from Task 4.2 + 4 from Task 5.1 minus any overlap — adjust based on actual count, but the new 4 must pass).

- [ ] **Step 5: Commit**

```bash
git add core/abi/registry.py tests/unit/test_abi_registry.py
git commit -m "feat(abi): selector index + lookup_function_by_selector with collision handling"
```

### Task 5.2: `AbiCallParser` (match + skip-unknown)

The parser walks `block.txs` and emits one `kind="call"` Event per tx whose selector resolves to a known function. Unknown selectors are skipped silently (per spec §4.2). Decode failures on a known selector are logged + skipped (no downgrade path for calls).

**Files:**
- Create: `core/parser/abi_call.py`
- Test: `tests/unit/test_abi_call_parser.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_abi_call_parser.py
from __future__ import annotations

from core.abi.decoder import function_selector
from core.abi.registry import AbiRegistry
from core.chains.types import Block, BlockHeader, Tx
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.abi_call import AbiCallParser


_FN_TRANSFER = {
    "type": "function", "name": "transfer",
    "inputs": [
        {"name": "to",    "type": "address"},
        {"name": "value", "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}
_FN_APPROVE = {
    "type": "function", "name": "approve",
    "inputs": [
        {"name": "spender", "type": "address"},
        {"name": "value",   "type": "uint256"},
    ],
    "outputs": [{"name": "", "type": "bool"}],
}
_TRANSFER_SEL = function_selector(_FN_TRANSFER)   # "0xa9059cbb"
_APPROVE_SEL = function_selector(_FN_APPROVE)     # "0x095ea7b3"
_TO = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def _registry_with(*entries: dict) -> AbiRegistry:
    snap = ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=list(entries))],
    )
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _block(txs: list[Tx]) -> Block:
    return Block(
        header=BlockHeader(number=42, hash="0xh42", parent_hash="0xh41", timestamp=1700000000),
        txs=txs,
        logs=[],
    )


def _transfer_calldata(value: int = 999) -> str:
    return (
        _TRANSFER_SEL
        + "0" * 24 + _TO
        + format(value, "064x")
    )


def test_emits_call_kind_for_known_selector_with_decoded_args() -> None:
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    tx = Tx(
        hash="0xt1", index=0, from_addr="0xf00", to_addr="0xCAFE",
        value=0, input=_transfer_calldata(value=999), status=1,
    )
    events = list(p.parse(_block([tx])))
    assert len(events) == 1
    e = events[0]
    assert e.kind == "call"
    assert e.name == "transfer"
    assert e.contract == "0xcafe"  # lowercased
    assert e.args == {
        "to":    "0x" + _TO,
        "value": "999",
    }
    assert e.chain_id == "eth-mainnet"
    assert e.block_number == 42
    assert e.tx_hash == "0xt1"
    assert e.tx_index == 0
    assert e.log_index is None  # calls have no log_index


def test_skips_tx_with_unknown_selector_no_downgrade() -> None:
    """Per spec §4.2: unknown selector → skip, NOT downgrade.
    A subscriber asked for kind=call with a specific ABI; if the tx isn't
    against that ABI we don't manufacture noise."""
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    unknown = "0xdeadbeef" + "00" * 32
    tx = Tx(hash="0xt2", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=unknown, status=1)
    assert list(p.parse(_block([tx]))) == []


def test_skips_tx_with_empty_or_short_input() -> None:
    """A native-only tx (input=='0x') or a tx with <4 bytes of calldata is
    not a contract call. Skip without warning."""
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    txs = [
        Tx(hash="0xt3", index=0, from_addr="0xf", to_addr="0xc", value=1, input="0x", status=1),
        Tx(hash="0xt4", index=1, from_addr="0xf", to_addr="0xc", value=0, input="0xa905", status=1),  # 2 bytes
        Tx(hash="0xt5", index=2, from_addr="0xf", to_addr="0xc", value=0, input="", status=1),
    ]
    assert list(p.parse(_block(txs))) == []


def test_skips_failed_tx() -> None:
    """A failed tx (status=0) didn't execute the contract's state-changing
    code path. Match NativeTransferParser's convention: don't emit events
    for failed txs."""
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    tx = Tx(hash="0xt6", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=_transfer_calldata(), status=0)
    assert list(p.parse(_block([tx]))) == []


def test_skips_contract_creation_tx() -> None:
    """A contract-creation tx has `to_addr=None`; its `input` is initcode,
    not calldata. The first 4 bytes are arbitrary bytecode and may
    coincidentally collide with a known selector — emitting a call event
    in that case would be a false positive. Skip without warning."""
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    # Initcode that happens to start with the transfer selector's bytes:
    initcode = _transfer_calldata(value=999)
    tx = Tx(hash="0xt8", index=0, from_addr="0xf", to_addr=None,
            value=0, input=initcode, status=1)
    assert list(p.parse(_block([tx]))) == []


def test_skips_known_selector_on_decode_failure(caplog) -> None:
    """Calldata that has the right selector but malformed args (e.g.
    truncated) is logged + skipped — not emitted as a half-decoded call.
    The spec's `kind="event"` downgrade applies only to events; calls
    are opt-in per subscription so silence-on-failure is the safer default."""
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    # Right selector, but truncated args (only 16 bytes of address, no value).
    bad = _TRANSFER_SEL + "00" * 16
    tx = Tx(hash="0xt7", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=bad, status=1)
    with caplog.at_level("WARNING"):
        events = list(p.parse(_block([tx])))
    assert events == []
    assert any("abi_call_parser.decode_failed" in r.message for r in caplog.records)


def test_emits_one_event_per_matching_tx() -> None:
    """Block with three txs: one transfer, one approve, one unknown.
    Parser emits two events (transfer + approve)."""
    reg = _registry_with(_FN_TRANSFER, _FN_APPROVE)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    txs = [
        Tx(hash="0xtA", index=0, from_addr="0xf", to_addr="0xC1",
           value=0, input=_transfer_calldata(value=100), status=1),
        Tx(hash="0xtB", index=1, from_addr="0xf", to_addr="0xC2",
           value=0,
           input=_APPROVE_SEL + "0" * 24 + _TO + format(7, "064x"),
           status=1),
        Tx(hash="0xtC", index=2, from_addr="0xf", to_addr="0xC3",
           value=0, input="0xdead" + "beef" * 16, status=1),  # unknown
    ]
    events = list(p.parse(_block(txs)))
    assert [e.name for e in events] == ["transfer", "approve"]
    assert [e.kind for e in events] == ["call", "call"]
    assert [e.tx_index for e in events] == [0, 1]


def test_preserves_raw_input_for_debugging() -> None:
    """The matched-call Event carries the original input bytes in `raw` so
    downstream tooling can recover the exact calldata without re-encoding."""
    reg = _registry_with(_FN_TRANSFER)
    p = AbiCallParser(chain_id="eth-mainnet", registry=reg)
    calldata = _transfer_calldata(value=42)
    tx = Tx(hash="0xt9", index=0, from_addr="0xf", to_addr="0xc",
            value=0, input=calldata, status=1)
    events = list(p.parse(_block([tx])))
    assert len(events) == 1
    assert events[0].raw["input"] == calldata
    assert events[0].raw["tx_hash"] == "0xt9"
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_abi_call_parser.py -v`
Expected: `ImportError: cannot import name 'AbiCallParser'`.

- [ ] **Step 3: Implement `core/parser/abi_call.py`**

```python
# core/parser/abi_call.py
"""ABI-driven call parser.

Per spec §4.2 / §3:
- Walks `block.txs`; for each tx whose `input` starts with a known 4-byte
  selector, emits an `Event(kind="call", name=<fn_name>, args=<decoded>)`.
- **Skip rules (no event emitted):**
  - `tx.status != 1` — failed tx (matches NativeTransferParser convention).
  - `tx.input` is empty / shorter than 10 chars (`"0x" + 4 bytes`) — not a
    contract call.
  - Selector is unknown to the registry — per spec §4.2, call parsing is
    opt-in per subscription; we don't manufacture noise for unrelated txs.
  - Decoder raises `DecodeFailed` — log warning + skip (no downgrade path
    for calls; the spec §6 downgrade applies only to events).

The parser does NOT pre-bind to specific ABI IDs — it consults the shared
`AbiRegistry` on every tx, so newly-added ABIs become observable on the
very next block without rebuilding the pipeline.
"""
from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.abi.errors import DecodeFailed
from core.abi.registry import AbiRegistry
from core.chains.types import Block, Tx
from core.parser.event import Event

log = structlog.get_logger(__name__)

_SELECTOR_HEX_LEN = 10  # "0x" + 8 hex chars = 4 bytes


class AbiCallParser:
    def __init__(self, *, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: Block) -> Iterable[Event]:
        h = block.header
        for tx in block.txs:
            ev = self._handle_tx(tx, h.number, h.hash, h.timestamp)
            if ev is not None:
                yield ev

    def _handle_tx(
        self,
        tx: Tx,
        block_number: int,
        block_hash: str,
        block_ts: int,
    ) -> Event | None:
        if tx.status != 1:
            return None
        if tx.to_addr is None:
            return None  # contract-creation tx — calldata is initcode, not a call
        inp = tx.input or ""
        if len(inp) < _SELECTOR_HEX_LEN:
            return None
        selector = inp[:_SELECTOR_HEX_LEN].lower()

        lookup = self._registry.lookup_function_by_selector(selector)
        if lookup is None:
            return None  # spec §4.2: unknown selector → skip, NOT downgrade

        fn_name, decoder = lookup
        try:
            args = decoder(inp)
        except DecodeFailed as exc:
            log.warning(
                "abi_call_parser.decode_failed",
                selector=selector,
                function=fn_name,
                tx_hash=tx.hash,
                error=str(exc),
            )
            return None

        return Event(
            chain_id=self._chain_id,
            block_number=block_number,
            block_hash=block_hash,
            block_timestamp=block_ts,
            tx_hash=tx.hash,
            tx_index=tx.index,
            log_index=None,
            kind="call",
            contract=tx.to_addr.lower(),
            name=fn_name,
            args=args,
            raw={
                "tx_hash": tx.hash,
                "input": inp,
            },
        )
```

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_abi_call_parser.py -v`
Expected: 8 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/parser/abi_call.py tests/unit/test_abi_call_parser.py
git commit -m "feat(parser): AbiCallParser with skip-on-unknown-selector semantics"
```

### Task 5.3: Wire `AbiCallParser` into `ChainRunner`

When `abi_registry` is supplied, `ChainRunner` now appends **both** `AbiEventParser` (chunk 4) and `AbiCallParser` (this chunk) to the pipeline.

**Files:**
- Modify: `apps/worker/chain_runner.py`

- [ ] **Step 1: Append the failing test**

Append to `tests/unit/test_chain_runner.py`:

```python
# tests/unit/test_chain_runner.py — append
def test_chain_runner_pipeline_includes_abi_call_parser_when_registry_given() -> None:
    """Construction-time wiring check: passing `abi_registry=` adds **both**
    AbiEventParser and AbiCallParser to the pipeline. Without it, the pipeline
    stays at native + erc20."""
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_TRANSFER_ABI])],
    ))
    chain = _chain()
    runner_with = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
        abi_registry=reg,
    )
    types_with = [type(p).__name__ for p in runner_with._pipeline._parsers]
    assert "AbiEventParser" in types_with
    assert "AbiCallParser" in types_with

    runner_without = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: _FakeAdapter([]),
        channel_factory=lambda _cfg: _CollectingChannel(),
        checkpoint_repo=_CheckpointStub(),
    )
    types_without = [type(p).__name__ for p in runner_without._pipeline._parsers]
    assert "AbiCallParser" not in types_without
    assert "AbiEventParser" not in types_without
    # Sanity: native + erc20 still there in both.
    assert "NativeTransferParser" in types_with and "NativeTransferParser" in types_without
    assert "Erc20TransferParser" in types_with and "Erc20TransferParser" in types_without
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_pipeline_includes_abi_call_parser_when_registry_given -v`
Expected: FAIL — `assert "AbiCallParser" in types_with` fails (only AbiEventParser was added by chunk 4).

- [ ] **Step 3: Append `AbiCallParser` to the registry-conditional branch**

Edit `apps/worker/chain_runner.py`. Add import near the other parser imports:

```python
from core.parser.abi_call import AbiCallParser
```

Then extend the conditional parser-list build (added in Task 4.4) to append both parsers:

```python
        parsers: list[Parser] = [
            NativeTransferParser(chain_id=self._chain.id),
            Erc20TransferParser(chain_id=self._chain.id),
        ]
        if abi_registry is not None:
            parsers.append(AbiEventParser(chain_id=self._chain.id, registry=abi_registry))
            parsers.append(AbiCallParser(chain_id=self._chain.id, registry=abi_registry))
        self._pipeline = ParserPipeline(parsers)
```

Order matters only for stable test output — the matcher keys on `(chain_id, kind)`, so routing is order-independent. Keeping `AbiEventParser` before `AbiCallParser` matches the spec §5 data-flow order (logs before calls).

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_chain_runner.py -v`
Expected: all existing tests still pass + new wiring test passes.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(runner): append AbiCallParser alongside AbiEventParser when registry present"
```

### Task 5.4: End-to-end `AbiCallParser` dispatch through `ChainRunner`

A high-level test: feed a block containing an ERC-20 `transfer(...)` tx into a `ChainRunner` constructed with a populated `AbiRegistry` and a subscription `match_kind="call"`, `match_name="transfer"`. The notifier should receive a payload with `kind="call"`, `name="transfer"`, and decoded args.

**Files:**
- Test: extend `tests/unit/test_chain_runner.py`

- [ ] **Step 1: Append the test**

```python
# tests/unit/test_chain_runner.py — append
def _block_with_erc20_transfer_call(n: int, *, to_hex: str, value: int) -> Block:
    """Build a block whose single tx is a well-formed ERC-20 `transfer(to, value)`
    call to contract address `0xtoken`. Calldata: selector + 32-byte address +
    32-byte uint256."""
    pad24 = "0" * 24
    sel = "a9059cbb"  # transfer(address,uint256) — see chunk 2 test for derivation
    calldata = "0x" + sel + pad24 + to_hex + format(value, "064x")
    return Block(
        header=_hdr(n, parent=f"0xh{n-1}" if n > 0 else "0x0"),
        txs=[
            Tx(hash=f"0xt{n}", index=0, from_addr="0xf0", to_addr="0xtoken",
               value=0, input=calldata, status=1),
        ],
        logs=[],
    )


@pytest.mark.asyncio
async def test_chain_runner_dispatches_abi_call_match() -> None:
    """ChainRunner with an AbiRegistry containing ERC-20 `transfer` fires a
    `kind="call", name="transfer"` dispatch for the subscription."""
    _FN_TRANSFER = {
        "type": "function", "name": "transfer",
        "inputs": [
            {"name": "to",    "type": "address"},
            {"name": "value", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    }
    chain = _chain()
    reg = _AbiRegistry()
    reg.refresh(ConfigSnapshot(
        version=1, subscriptions=[], channels=[], chains=[],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_FN_TRANSFER])],
    ))
    to_hex = "bbbb" + "00" * 18
    blocks = [_block_with_erc20_transfer_call(n, to_hex=to_hex, value=n * 100)
              for n in (1, 2, 3, 4)]
    adapter = _FakeAdapter(blocks)
    coll = _CollectingChannel()
    snap = ConfigSnapshot(
        version=1,
        chains=[chain],
        subscriptions=[_sub(channel_ids=["c1"], match_kind="call", match_name="transfer")],
        channels=[_ch("c1")],
        abis=[_SnapshotAbi(id="a1", name="erc20", kind="evm_abi", body=[_FN_TRANSFER])],
    )
    cp = _CheckpointStub()

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: coll,
        checkpoint_repo=cp,
        abi_registry=reg,
    )
    await runner.start(snap)
    task = asyncio.create_task(runner.run())
    try:
        for b in blocks:
            await adapter.push_head(b.header)
        # Same confirmation arithmetic as chunk 3 / 4 dispatch tests.
        for _ in range(20):
            if len(coll.calls) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(coll.calls) == 2
        kinds = {c["event"]["kind"] for c in coll.calls}
        assert kinds == {"call"}
        names = {c["event"]["name"] for c in coll.calls}
        assert names == {"transfer"}
        values = {c["event"]["args"]["value"] for c in coll.calls}
        # block 1 → 100, block 2 → 200
        assert values == {"100", "200"}
    finally:
        await runner.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
```

- [ ] **Step 2: Run, expect PASS**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_dispatches_abi_call_match -v`
Expected: PASS. (Implementation work finished in Task 5.3; this test is a regression seal.)

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_chain_runner.py
git commit -m "test(runner): end-to-end ABI-call dispatch with populated registry"
```

### Task 5.5: Chunk 5 close-out

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: all M1 + chunks 1-5 tests pass.

- [ ] **Step 2: Lint / type check**

Run: `make lint typecheck`
Expected: clean.

---

## Chunk 6: `Channel.__init_subclass__` enforcement

Closes spec §4.3. M1 already has the `Channel` ABC with `type: ClassVar[str]` declared and a `register_channel(cls)` function that raises on duplicate-type registrations from different classes (`core/notifier/channel.py:30`). The remaining seam is that subclasses can be defined without ever calling `register_channel` (it's a separate, explicit call at module bottom in `core/notifier/http.py:73`), and the omission only surfaces when the worker first tries to instantiate the missing channel type. Chunk 6 closes that hole with an `__init_subclass__` hook that:

1. **Guards `type`** — refuses any subclass that doesn't declare `type: str` as a non-empty class attribute.
2. **Auto-registers** — calls `register_channel(cls)` at class-definition time so a forgotten registration becomes impossible.

This is the last bit of plumbing that lets chunks 7 (`RedisStreamsChannel`) and 8 (`WebSocketChannel`) be added simply by defining the subclass — the registration is automatic and the worker's existing `_channel_factory` lookup (`apps/worker/main.py`'s `_default_channel_factory` → `CHANNEL_REGISTRY[snap.type]`) wires them up.

**Spec §4.3 scope:**
- Add `Channel.__init_subclass__` that enforces `type: str` declaration AND calls `register_channel(cls)` automatically.
- Keep `register_channel` callable for explicit registration in tests (idempotent on same-class re-registration — unchanged behaviour).
- Drop the now-redundant explicit `register_channel(HttpChannel)` call at `core/notifier/http.py:73`.
- Update `tests/unit/test_channel_registry.py` to reflect the new semantics: duplicate-type detection now fires at **class-definition time**, not at the explicit-`register_channel`-call time.

**Why a preparatory rename first (Task 6.1):**
Two M1 test files both define an in-test `class _CollectingChannel(Channel)` with `type = "collect"`:
- `tests/unit/test_notifier.py:14`
- `tests/unit/test_chain_runner.py:53`

Today this is harmless because neither class is registered. The instant `__init_subclass__` auto-registers, the second import to load will raise `ValueError: channel type 'collect' already registered`, breaking every test in the second-loaded file. The first task disambiguates the two `type` strings — a pure no-op cleanup commit that unblocks the rest of the chunk.

**Modified files this chunk:**
- `core/notifier/channel.py` — add `__init_subclass__`.
- `core/notifier/http.py` — drop the explicit `register_channel(HttpChannel)` line.
- `tests/unit/test_notifier.py` — rename `_CollectingChannel.type` to `"collect-notifier"`.
- `tests/unit/test_chain_runner.py` — rename `_CollectingChannel.type` to `"collect-runner"` (and update the `SnapshotChannel(type=...)` literal in the same file).
- `tests/unit/test_channel_registry.py` — rewrite tests to reflect class-definition-time duplicate detection.

**Out of scope this chunk:**
- New channel implementations — those are chunks 7 (`RedisStreamsChannel`) and 8 (`WebSocketChannel`).
- Channel config-shape validation — channels still trust their `config: dict[str, Any]` blob at construction; per-channel JSON-schema validation is M3+ territory.
- Cleanup of `CHANNEL_REGISTRY` after test runs — tests that need a clean registry use a local `del CHANNEL_REGISTRY[type]` teardown; a global autouse fixture would silently mask leaks.

**Note on indirect subclasses:** Because `__init_subclass__` fires on every subclass — direct or indirect — defining `class CustomHttpChannel(HttpChannel)` without changing `type` will raise `ValueError` (the inherited `type = "http"` collides with `HttpChannel`'s own registration). Intentional: there is no use case in M2 for two `Channel` classes sharing a `type` string. Subclasses meant to extend behaviour must override `type`.

### Task 6.1: Preparatory — disambiguate test `_CollectingChannel.type`

The two test files defining `_CollectingChannel(Channel)` with the same `type = "collect"` will conflict the moment `__init_subclass__` auto-registers. Rename each to a globally unique string up front so this commit is purely mechanical and the next task's behaviour change is the only delta.

**Files:**
- Modify: `tests/unit/test_notifier.py:15` (the `_CollectingChannel.type = "collect"` line)
- Modify: `tests/unit/test_chain_runner.py:54` (the `_CollectingChannel.type = "collect"` line) **and** `tests/unit/test_chain_runner.py:50` (the `SnapshotChannel(..., type="collect", ...)` literal in the `_ch(id_: str)` helper)

- [ ] **Step 1: Rename in `tests/unit/test_notifier.py`**

Change:

```python
class _CollectingChannel(Channel):
    type = "collect"
```

to:

```python
class _CollectingChannel(Channel):
    type = "collect-notifier"
```

(`test_notifier.py` constructs `_CollectingChannel()` directly inside the test bodies. It doesn't go through `CHANNEL_REGISTRY` lookup. The `type` string is only ever consulted by `register_channel`; the rename has no behavioural side-effects today.)

- [ ] **Step 2: Rename in `tests/unit/test_chain_runner.py`**

Change the class:

```python
class _CollectingChannel(Channel):
    type = "collect"
```

to:

```python
class _CollectingChannel(Channel):
    type = "collect-runner"
```

And update the `_ch` helper at line ~50:

```python
def _ch(id_: str) -> SnapshotChannel:
    return SnapshotChannel(id=id_, name="hook", type="collect", config={})
```

to:

```python
def _ch(id_: str) -> SnapshotChannel:
    return SnapshotChannel(id=id_, name="hook", type="collect-runner", config={})
```

(The `SnapshotChannel.type` field carries the string used by the worker's channel factory to look up the registered class. Since `test_chain_runner.py` injects `channel_factory=lambda _cfg: _CollectingChannel()` directly into the runner constructor, the type string in the snapshot is metadata-only and any unique string works. But keeping the two consistent within the file improves readability.)

- [ ] **Step 3: Run, expect PASS**

Run: `pytest tests/unit/test_notifier.py tests/unit/test_chain_runner.py -v`
Expected: both files pass — no behaviour change, just a string rename.

- [ ] **Step 4: Commit**

```bash
git add tests/unit/test_notifier.py tests/unit/test_chain_runner.py
git commit -m "test(notifier): disambiguate _CollectingChannel types for upcoming auto-register"
```

### Task 6.2: Add `Channel.__init_subclass__` (type check + auto-register)

The new hook does two things: (a) refuses subclasses without a non-empty `type: str` attribute, (b) calls `register_channel(cls)` automatically. The existing `register_channel` is unchanged — it stays callable in tests and is idempotent on same-class re-registration. The duplicate detection logic is unchanged; it just gets exercised one frame earlier (at `class` block execution, not at explicit-call time).

**Files:**
- Modify: `core/notifier/channel.py`
- Rewrite: `tests/unit/test_channel_registry.py`

- [ ] **Step 1: Rewrite `tests/unit/test_channel_registry.py` with the new contract**

Replace the file contents:

```python
# tests/unit/test_channel_registry.py
from __future__ import annotations

from typing import Any

import pytest

from core.notifier.channel import CHANNEL_REGISTRY, Channel, register_channel


# At module load this auto-registers because of __init_subclass__.
class _FakeChannel(Channel):
    type = "fake"

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, payload: dict[str, Any]) -> None: ...


def test_subclass_with_type_auto_registers_at_class_definition() -> None:
    """Defining a Channel subclass with a `type` attribute registers it
    automatically — no explicit `register_channel(cls)` call needed."""
    assert CHANNEL_REGISTRY["fake"] is _FakeChannel


def test_subclass_without_type_attr_raises_type_error() -> None:
    """A Channel subclass that forgets to declare `type` is a programming
    error — surfaced at class-definition time, not at first-use time."""
    with pytest.raises(TypeError, match="must declare a `type` class attribute"):
        class _Missing(Channel):  # noqa: N801 — intentional missing-type repro
            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...


def test_subclass_with_empty_type_raises_type_error() -> None:
    """An empty `type` string is functionally equivalent to a missing one and
    must also be refused."""
    with pytest.raises(TypeError, match="must declare a `type` class attribute"):
        class _Empty(Channel):
            type = ""
            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...


def test_subclass_with_duplicate_type_raises_at_class_definition() -> None:
    """Defining a SECOND Channel subclass with a type already in the registry
    raises immediately — before any code attempts to use the duplicate."""
    with pytest.raises(ValueError, match="already registered"):
        class _Dup(Channel):
            type = "fake"  # collides with _FakeChannel above
            async def start(self) -> None: ...
            async def stop(self) -> None: ...
            async def send(self, payload: dict[str, Any]) -> None: ...


def test_explicit_register_channel_remains_idempotent_for_same_class() -> None:
    """`register_channel(cls)` on a class that's already auto-registered with
    the SAME class object is a no-op. This is the path tests use when they
    need an explicit registration call for clarity."""
    register_channel(_FakeChannel)
    register_channel(_FakeChannel)  # second call must not raise
    assert CHANNEL_REGISTRY["fake"] is _FakeChannel
```

- [ ] **Step 2: Run, expect FAIL.**

Run: `pytest tests/unit/test_channel_registry.py -v`
Expected: at least three failures —
- `test_subclass_with_type_auto_registers_at_class_definition` fails: `_FakeChannel` isn't in `CHANNEL_REGISTRY` (no auto-register yet).
- `test_subclass_without_type_attr_raises_type_error` fails: the class block executes successfully (no TypeError raised).
- `test_subclass_with_empty_type_raises_type_error` fails: same reason.
- `test_subclass_with_duplicate_type_raises_at_class_definition` fails: the duplicate class block doesn't raise (the M1 duplicate check fires only at explicit `register_channel`).

- [ ] **Step 3: Add `__init_subclass__` to `Channel`**

Edit `core/notifier/channel.py`. Insert the hook into the `Channel` class body, after the `type: ClassVar[str]` declaration and before the abstract methods:

```python
class Channel(ABC):
    """Abstract base for a notification channel driver.

    Lifecycle: `start()` → many `send()` → `stop()`. Implementations should be
    safe to construct from a `SnapshotChannel.config` dict; the worker calls
    `start()` once on first use per chain pipeline.

    Subclasses must declare a non-empty `type: ClassVar[str]` and are
    auto-registered in `CHANNEL_REGISTRY` at class-definition time.
    """

    type: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        t = getattr(cls, "type", None)
        if not isinstance(t, str) or not t:
            raise TypeError(
                f"{cls.__name__} must declare a `type` class attribute "
                f"(non-empty str). Got {t!r}."
            )
        register_channel(cls)

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def send(self, payload: dict[str, Any]) -> None: ...
```

Note on ordering: `register_channel` is defined **below** the `Channel` class in `channel.py`. That's fine — `__init_subclass__` references `register_channel` by name in its function body, not at class-definition time. The first `__init_subclass__` invocation happens when a subclass is defined (e.g. `HttpChannel`), which is in a different module imported after `channel.py` finishes loading. By then `register_channel` is a resolvable global.

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_channel_registry.py -v`
Expected: all 5 PASS.

- [ ] **Step 5: Full suite regression sweep**

Run: `pytest tests/ -v`
Expected: no failures introduced. Specifically:
- `test_notifier.py` still passes (`_CollectingChannel.type = "collect-notifier"` from Task 6.1 auto-registers cleanly).
- `test_chain_runner.py` still passes (`_CollectingChannel.type = "collect-runner"` from Task 6.1 auto-registers cleanly).
- `HttpChannel` is still in `CHANNEL_REGISTRY["http"]` (auto-registered by `__init_subclass__`; the explicit `register_channel(HttpChannel)` at `http.py:73` is now a no-op).

- [ ] **Step 6: Commit**

```bash
git add core/notifier/channel.py tests/unit/test_channel_registry.py
git commit -m "feat(notifier): Channel.__init_subclass__ enforces type attr and auto-registers"
```

### Task 6.3: Drop the now-redundant `register_channel(HttpChannel)` call

With auto-registration in place, the explicit call at `core/notifier/http.py:73` is no-op-on-same-class — safe to delete. Removing it is the canonical way to demonstrate the new contract for chunks 7 / 8: define the subclass, and registration is done.

**Files:**
- Modify: `core/notifier/http.py:73`

- [ ] **Step 1: Append a regression test**

Append to `tests/unit/test_channel_registry.py`:

```python
def test_http_channel_remains_registered_without_explicit_call() -> None:
    """Regression seal for Task 6.3: removing the explicit
    `register_channel(HttpChannel)` from `core/notifier/http.py` must
    not break the registry — __init_subclass__ covers it."""
    from core.notifier.http import HttpChannel
    assert CHANNEL_REGISTRY["http"] is HttpChannel
```

- [ ] **Step 2: Run, expect PASS even before removal**

Run: `pytest tests/unit/test_channel_registry.py::test_http_channel_remains_registered_without_explicit_call -v`
Expected: PASS — because `register_channel(HttpChannel)` is idempotent on same class, both the explicit call and `__init_subclass__` set the same entry. This is the "test passes both before and after the change" pattern; the value is the regression seal.

- [ ] **Step 3: Delete the explicit call**

Edit `core/notifier/http.py`. Remove the trailing line:

```python
register_channel(HttpChannel)
```

Also drop the now-unused `register_channel` from the import at the top:

```python
from core.notifier.channel import Channel, register_channel
```

→

```python
from core.notifier.channel import Channel
```

(If `register_channel` is referenced elsewhere in `http.py`, leave the import. Check with `grep -n register_channel core/notifier/http.py` first — expected: zero remaining references after the deletion.)

- [ ] **Step 4: Run, expect PASS.**

Run: `pytest tests/unit/test_channel_registry.py -v && pytest tests/unit/test_notifier.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add core/notifier/http.py tests/unit/test_channel_registry.py
git commit -m "refactor(notifier): drop explicit register_channel(HttpChannel) — __init_subclass__ covers it"
```

### Task 6.4: Chunk 6 close-out

- [ ] **Step 1: Run the full test suite**

Run: `pytest tests/ -v`
Expected: all M1 + chunks 1-6 tests pass.

- [ ] **Step 2: Lint / type check**

Run: `make lint typecheck`
Expected: clean.

---


## Chunk 7: `RedisStreamsChannel`

Closes spec §4.4. M1 ships a single notification channel — `HttpChannel`. Chunk 7 adds the second concrete `Channel` subclass: a Redis Streams MQ driver that `XADD`s the same `event_payload` (spec §8) the HTTP channel posts, with an optional `MAXLEN ~ N` cap for bounded stream length. The channel reuses M1's `core/notifier/retry.py` for 3-attempt exponential backoff and the **existing** `RedisBus` connection that `_Worker` already owns — it does not open its own Redis client.

The single largest design seam this chunk has to chew through is **how the channel gets a Redis handle**. M1's `_default_channel_factory(cfg: SnapshotChannel) -> Channel` (`apps/worker/main.py:39`) takes only the config dict and constructs the channel — there's no parameter for "shared infrastructure". `RedisStreamsChannel` needs the `aioredis.Redis` client. Three options were considered:

1. **Channel opens its own Redis client** from a URL in `cfg.config`. Rejected: forces two `redis.asyncio.Redis` instances per worker process (the bus already has one) and the channel needs to manage its own lifecycle.
2. **Inject the URL, channel reconstructs the client.** Same downsides as (1) — duplicate connection pools — and the URL becomes part of the per-subscription config blob, leaking transport-layer detail into the subscription schema.
3. **Inject the existing `RedisBus` (or its `aioredis.Redis` client) into the factory and pass it to every channel constructor.** Channels that don't need it (`HttpChannel`) accept the keyword and ignore it; channels that need it (`RedisStreamsChannel`, and chunk 8's `WebSocketChannel`) require it. This is what spec §4.4 means by "uses the existing `RedisBus` connection".

Option 3 is the design. Concretely:
- `RedisBus` exposes a `.client` property that returns its `aioredis.Redis` instance (asserts the bus has been `connect()`ed).
- The `Channel` ABC documents (but does not enforce) that subclass constructors accept `(*, config: dict[str, Any], bus: RedisBus | None = None)` plus their own kwargs. `RedisBus | None` keeps direct test construction ergonomic (`HttpChannel(config={...})` still works without a bus).
- `HttpChannel.__init__` is widened to accept `bus` and ignore it.
- The worker's module-level `_default_channel_factory` is replaced with a `_make_channel_factory(bus)` closure built once at `_Worker.start()` time.

The reason to plumb `bus` to **every** channel uniformly (rather than special-casing `cfg.type == "mq"` inside the factory) is that chunk 8 adds a second bus-consuming channel, and a uniform interface keeps the factory dispatch a single line: `cls(config=cfg.config, bus=bus)`. No `isinstance` chains, no class-level "wants_bus" flags.

**Spec §4.4 scope:**
- New `core/notifier/redis_streams.py` with `class RedisStreamsChannel(Channel)`, `type = "mq"`. The `"mq"` string matches M1's existing `ChannelType` enum slot (`core/config/models.py:48-51` defines `ChannelType: mq | http | ws`); the runtime registry uses the same enum value as the DB column. M2 commits to one MQ driver per process — if M3 adds Kafka/RabbitMQ, the design choice between "extend the enum" vs "driver-select via config" gets revisited then.
- Constructor: `__init__(self, *, config: dict[str, Any], bus: RedisBus, base_delay: float = 1.0)`. Reads `config["stream"]` (required) and `config.get("maxlen")` (optional `int | None`). Raises `KeyError` if `stream` is missing — channel-config validation is M3+ territory and a missing `stream` is a programmer error, not a runtime fallback.
- `send(payload)`: serialize `payload` as JSON, `XADD <stream> [MAXLEN ~ N] * data <json>`. Wraps the `xadd` call in `retry_with_backoff(max_attempts=3, base_delay=…)` (matches `HttpChannel` policy).
- `start()` / `stop()`: no-ops. The channel does not own the Redis connection.
- The channel auto-registers via chunk 6's `__init_subclass__` hook; the worker only needs the side-effecting `from core.notifier.redis_streams import RedisStreamsChannel  # noqa: F401` import to make the class definition execute at process start.

**Trim semantics:** Spec §6 explicitly calls out `MAXLEN ~` (approximate trim) as best-effort. `redis-py`'s `xadd(..., maxlen=N, approximate=True)` already issues `MAXLEN ~ N` in the same `XADD` call, so a trim failure can only happen if the entire `XADD` fails — and that is already retried by `retry_with_backoff`. There is **no separate `XTRIM` follow-up call**; the spec line "Trim failures log a warning but do not fail the delivery" only applies if a future refactor splits trim into its own call. We add a short comment in the code documenting that this branch is intentionally not exercised today.

**Modified files this chunk:**
- `core/bus/redis_bus.py` — add `.client` property.
- `core/notifier/channel.py` — extend ABC docstring noting the `bus` keyword convention (no signature change to `Channel.__init__` since `Channel` is abstract and has no `__init__` of its own).
- `core/notifier/http.py` — widen `HttpChannel.__init__` to accept `bus` (ignored).
- `core/notifier/redis_streams.py` — **new**: `RedisStreamsChannel`.
- `apps/worker/main.py` — replace module-level `_default_channel_factory` with `_make_channel_factory(bus)` closure built in `_Worker._reconcile` (or `_make_channel_factory` called from `start()` and stashed on `self`). Add the side-effect import for the new channel module.
- `tests/unit/test_http_channel.py` — every `HttpChannel(...)` constructor call gains `bus=None`. (Or — equivalently — we make `bus` default to `None`, which keeps the existing tests untouched. **We're going with the default `bus=None`**; the unit tests are not modified.)
- `tests/unit/test_redis_streams_channel.py` — **new**: unit tests with an `AsyncMock` Redis client.
- `tests/integration/test_redis_streams_channel.py` — **new**: testcontainers Redis round-trip (`XADD` then `XREAD` from the same stream).

**Out of scope this chunk:**
- Consumer-side semantics — workers don't consume from these streams. The downstream consumer (`xreadgroup`-style) is a user-of-this-project concern, not in M2.
- Multi-stream fanout — one channel ↔ one stream by config. Multi-stream fanout is a subscription-level concern (the subscription can list multiple `channel_ids`).
- `XADD NOMKSTREAM` semantics — we always allow auto-creation. The spec is silent on pre-creation; if a user wants strict pre-creation, they can configure the stream out-of-band and an `XADD` on a deleted stream just recreates it (Redis default behaviour).
- Channel config-shape validation — channels still trust their `config: dict[str, Any]` blob at construction; per-channel JSON-schema validation is M3+ territory (same out-of-scope line as chunks 6 and 8).

**Note on `aioredis.Redis` import path:** M1 uses `redis.asyncio as aioredis` (`core/bus/redis_bus.py:8`), not the old separate `aioredis` package. This chunk continues that import style — `from redis.asyncio import Redis` — and types the channel's client field as `Redis` from that namespace.

### Task 7.1: Preparatory — `RedisBus.client` property + factory closure + `HttpChannel(bus=None)`

Three small mechanical edits land in one commit because they're a single conceptual change: "the channel factory can now hand a Redis client to channel constructors". The new channel in Task 7.3 depends on this scaffolding; doing it in a separate commit makes the diff for the actual channel implementation pure.

**Files:**
- Modify: `core/bus/redis_bus.py` — add `client` property.
- Modify: `core/notifier/http.py` — widen `__init__` signature.
- Modify: `apps/worker/main.py` — replace module-level `_default_channel_factory` with a closure built in `_Worker`.
- Test: existing `tests/unit/test_http_channel.py` continues to pass unchanged; a new test verifies `_make_channel_factory` injects the bus.

- [ ] **Step 1: Write the failing test for the bus injection contract**

This is the only NEW behaviour landing in this task — the rest is signature widening. Pick `tests/unit/test_channel_registry.py` (already touched by chunk 6) as the home for this test since it's the file that covers channel registration / construction, and add:

```python
# tests/unit/test_channel_registry.py — append at end of file
from typing import Any
from unittest.mock import AsyncMock

from core.bus.redis_bus import RedisBus
from core.config.snapshot import SnapshotChannel
from core.notifier.channel import CHANNEL_REGISTRY, Channel


def test_make_channel_factory_injects_bus_into_constructor() -> None:
    """`_make_channel_factory(bus)` returns a factory that hands `bus` to every
    channel constructor it builds. HTTP ignores it; bus-consuming channels read it."""
    from apps.worker.main import _make_channel_factory

    seen: dict[str, Any] = {}

    class _ProbeChannel(Channel):
        type = "probe-7-1"

        def __init__(self, *, config: dict[str, Any], bus: RedisBus | None = None) -> None:
            seen["config"] = config
            seen["bus"] = bus

        async def start(self) -> None: ...
        async def stop(self) -> None: ...
        async def send(self, payload: dict[str, Any]) -> None: ...

    try:
        fake_bus = AsyncMock(spec=RedisBus)
        factory = _make_channel_factory(fake_bus)
        cfg = SnapshotChannel(id="c-7-1", name="probe", type="probe-7-1", config={"k": "v"})
        ch = factory(cfg)
        assert isinstance(ch, _ProbeChannel)
        assert seen["config"] == {"k": "v"}
        assert seen["bus"] is fake_bus
    finally:
        del CHANNEL_REGISTRY["probe-7-1"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/unit/test_channel_registry.py::test_make_channel_factory_injects_bus_into_constructor -v`
Expected: FAIL with `ImportError: cannot import name '_make_channel_factory' from 'apps.worker.main'` (the symbol doesn't exist yet).

- [ ] **Step 3: Add `RedisBus.client` property**

Edit `core/bus/redis_bus.py`, add after `disconnect()`:

```python
    @property
    def client(self) -> aioredis.Redis:
        """Return the underlying redis-py async client. Raises if `connect()`
        has not been called. Used by `Channel` subclasses that need direct
        Redis commands (e.g. `RedisStreamsChannel.send` issues `XADD`)."""
        assert self._client is not None, "RedisBus.connect() must be called first"
        return self._client
```

- [ ] **Step 4: Widen `HttpChannel.__init__` to accept `bus`**

In `core/notifier/http.py`, change the constructor signature:

```python
    def __init__(
        self,
        *,
        config: dict[str, Any],
        bus: "RedisBus | None" = None,  # ignored; accepted for uniform factory wiring
        base_delay: float = 1.0,
    ) -> None:
```

And add `from core.bus.redis_bus import RedisBus  # noqa: TC001 — only for type` to the top of the file. (Or use `TYPE_CHECKING`-guarded import — either works. The `noqa: TC001` keeps ruff happy if a `TYPE_CHECKING` rule fires.)

Reference the parameter as `_ = bus` inside `__init__` (or skip it entirely — Python lets you accept a kwarg without binding it to anything beyond the parameter slot). To keep linters quiet without a no-op statement, prefix the parameter name as `_bus` instead of `bus`:

```python
    def __init__(
        self,
        *,
        config: dict[str, Any],
        bus: "RedisBus | None" = None,
        base_delay: float = 1.0,
    ) -> None:
        del bus  # accepted for uniform factory wiring; HttpChannel doesn't need it
```

The `del bus` makes the intent explicit and silences "unused argument" warnings without renaming the parameter — the factory always passes `bus=` by keyword.

- [ ] **Step 5: Replace module-level `_default_channel_factory` with `_make_channel_factory(bus)`**

In `apps/worker/main.py`:

1. Delete the existing function at lines 39-41:
   ```python
   def _default_channel_factory(cfg: SnapshotChannel) -> Channel:
       cls = CHANNEL_REGISTRY[cfg.type]
       return cls(config=cfg.config)  # type: ignore[call-arg]
   ```
2. Add a closure factory builder above `_Worker`:
   ```python
   def _make_channel_factory(bus: RedisBus) -> Callable[[SnapshotChannel], Channel]:
       """Build a channel factory closed over the shared Redis bus.

       Channels that need direct Redis access (`RedisStreamsChannel`, `WebSocketChannel`)
       receive `bus` and use it; channels that don't (`HttpChannel`) accept the kwarg
       and ignore it. Keeping the dispatch uniform avoids per-type branching here.
       """
       def factory(cfg: SnapshotChannel) -> Channel:
           cls = CHANNEL_REGISTRY[cfg.type]
           return cls(config=cfg.config, bus=bus)  # type: ignore[call-arg]
       return factory
   ```
   You'll need `from collections.abc import Callable` at the top of the file.
3. In `_Worker._reconcile`, the `ChainRunner(... channel_factory=_default_channel_factory ...)` call (currently line 134) becomes:
   ```python
   channel_factory=_make_channel_factory(self._bus),
   ```
   Each new `ChainRunner` gets its own factory instance, but they all close over the same `self._bus`. Building once and stashing on `self` is a future micro-optimisation; per-call closure construction is cheap.

- [ ] **Step 6: Re-run the unit test**

Run: `pytest tests/unit/test_channel_registry.py::test_make_channel_factory_injects_bus_into_constructor -v`
Expected: PASS.

- [ ] **Step 7: Run the full HTTP channel suite to confirm widened signature is non-breaking**

Run: `pytest tests/unit/test_http_channel.py -v`
Expected: All 6 existing tests still PASS. (None of them pass `bus=`; the new default `None` keeps them working.)

- [ ] **Step 8: Lint / type-check**

Run: `make lint typecheck`
Expected: clean. The `# type: ignore[call-arg]` on `cls(config=cfg.config, bus=bus)` stays — `Channel` is abstract with no `__init__` constraint, so pyright cannot see any concrete subclass's signature through the `type[Channel]` mapping. The ignore is structurally required.

- [ ] **Step 9: Commit**

```bash
git add core/bus/redis_bus.py core/notifier/http.py apps/worker/main.py tests/unit/test_channel_registry.py
git commit -m "feat(notifier): bus-aware channel factory + RedisBus.client property"
```

### Task 7.2: Red — `RedisStreamsChannel` unit tests

Drives Task 7.3. Three behaviours under test: (a) basic `XADD` with the JSON payload as the `data` field; (b) `MAXLEN ~ N` is forwarded when configured; (c) transient `RedisError` is retried, persistent failure raises `RetryExhausted`. We mock the `aioredis.Redis` client with a plain `AsyncMock()` so the test surface stays in-process and doesn't depend on a live Redis (the IT in Task 7.5 covers the wire-level contract).

**Why plain `AsyncMock()` and NOT `AsyncMock(spec=Redis)`:** `redis.asyncio.Redis.xadd` is declared with a return type of `Awaitable[Any] | Any` rather than as a real `async def` — `inspect.iscoroutinefunction(Redis.xadd)` returns `False`. `AsyncMock(spec=Redis)` would therefore auto-wrap `xadd` as a plain `MagicMock`, and `await client.xadd(...)` inside `RedisStreamsChannel.send` would raise `TypeError: 'MagicMock' object can't be awaited`. Plain `AsyncMock()` (no spec) makes every accessed attribute an `AsyncMock` — which is what the implementation expects to await. We lose the spec-typo safety net in exchange for a working async surface; the integration test (Task 7.5) catches any attribute-name drift against the real client.

**Files:**
- Create: `tests/unit/test_redis_streams_channel.py`

- [ ] **Step 1: Write the failing test file**

```python
# tests/unit/test_redis_streams_channel.py
from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from core.bus.redis_bus import RedisBus
from core.notifier.redis_streams import RedisStreamsChannel
from core.notifier.retry import RetryExhausted


def _fake_bus_with_client(client: AsyncMock) -> AsyncMock:
    """Build an `AsyncMock` `RedisBus` whose `.client` property returns `client`.

    `AsyncMock(spec=RedisBus)` doesn't auto-attach `.client` because the spec
    inspects instance attributes, so we override the attribute directly.

    The `client` arg is expected to be a plain `AsyncMock()` (no `spec=Redis`)
    — see the chunk preamble for why spec-mode breaks `await client.xadd(...)`.
    """
    bus = AsyncMock(spec=RedisBus)
    bus.client = client
    return bus


@pytest.mark.asyncio
async def test_xadd_sends_json_payload_to_configured_stream() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events"}, bus=bus)
    await ch.start()
    try:
        payload: dict[str, Any] = {"k": 1, "subscription_id": "s1"}
        await ch.send(payload)
    finally:
        await ch.stop()

    client.xadd.assert_awaited_once()
    args, kwargs = client.xadd.call_args
    assert args[0] == "events"  # stream name
    fields = args[1]
    assert json.loads(fields["data"]) == payload
    assert "maxlen" not in kwargs  # no cap configured


@pytest.mark.asyncio
async def test_xadd_forwards_maxlen_when_configured() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events", "maxlen": 1000}, bus=bus)
    await ch.start()
    try:
        await ch.send({"k": 1})
    finally:
        await ch.stop()

    args, kwargs = client.xadd.call_args
    assert kwargs.get("maxlen") == 1000
    assert kwargs.get("approximate") is True  # MAXLEN ~ N, not strict


@pytest.mark.asyncio
async def test_transient_redis_error_is_retried_then_succeeds() -> None:
    client = AsyncMock()
    client.xadd.side_effect = [RedisError("temporary"), b"1700000000000-0"]
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.xadd.await_count == 2


@pytest.mark.asyncio
async def test_persistent_redis_error_raises_retry_exhausted() -> None:
    client = AsyncMock()
    client.xadd.side_effect = RedisError("hard down")
    bus = _fake_bus_with_client(client)
    ch = RedisStreamsChannel(config={"stream": "events"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        with pytest.raises(RetryExhausted):
            await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.xadd.await_count == 3  # max_attempts


@pytest.mark.asyncio
async def test_missing_stream_config_raises_at_construction() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    with pytest.raises(KeyError):
        RedisStreamsChannel(config={"maxlen": 100}, bus=bus)


def test_type_attribute_matches_db_enum_mq_slot() -> None:
    # Confirms the auto-registration key is the `"mq"` string that matches
    # `ChannelType.mq` in the DB enum, so that `CHANNEL_REGISTRY[snap.type]`
    # resolves when the worker dequeues a snapshot row with `type = ChannelType.mq`.
    assert RedisStreamsChannel.type == "mq"
```

- [ ] **Step 2: Run the new tests to confirm they fail**

Run: `pytest tests/unit/test_redis_streams_channel.py -v`
Expected: 6 FAILs with `ModuleNotFoundError: No module named 'core.notifier.redis_streams'`.

### Task 7.3: Green — implement `RedisStreamsChannel`

**Files:**
- Create: `core/notifier/redis_streams.py`

- [ ] **Step 1: Write the minimal implementation**

```python
# core/notifier/redis_streams.py
from __future__ import annotations

import json
from functools import partial
from typing import Any

import structlog
from redis.exceptions import RedisError

from core.bus.redis_bus import RedisBus
from core.notifier.channel import Channel
from core.notifier.retry import retry_with_backoff

log = structlog.get_logger(__name__)


class RedisStreamsChannel(Channel):
    """XADD-based Redis Streams notification driver.

    Reuses the worker's shared `RedisBus` connection — does not own a client.
    `MAXLEN ~ N` trimming is issued in the same `XADD` call (`approximate=True`)
    so there is no separate trim-failure code path; a failed trim implies a
    failed `XADD` and is therefore covered by the standard retry policy.
    """

    type = "mq"  # matches ChannelType.mq in the DB enum

    def __init__(
        self,
        *,
        config: dict[str, Any],
        bus: RedisBus,
        base_delay: float = 1.0,
    ) -> None:
        self._stream: str = config["stream"]  # required; KeyError surfaces misconfig
        self._maxlen: int | None = config.get("maxlen")
        self._bus = bus
        self._base_delay = base_delay

    async def start(self) -> None:
        # The bus is started/stopped by the worker; this channel is a thin user
        # of that connection.
        return None

    async def stop(self) -> None:
        return None

    async def send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"))
        await retry_with_backoff(
            partial(self._xadd_once, body=body),
            max_attempts=3,
            base_delay=self._base_delay,
        )

    async def _xadd_once(self, *, body: str) -> None:
        client = self._bus.client
        try:
            kwargs: dict[str, Any] = {}
            if self._maxlen is not None:
                kwargs["maxlen"] = self._maxlen
                kwargs["approximate"] = True
            await client.xadd(self._stream, {"data": body}, **kwargs)
        except RedisError:
            # Re-raise as-is; retry_with_backoff classifies it as a retryable error.
            raise
```

- [ ] **Step 2: Re-run the unit tests**

Run: `pytest tests/unit/test_redis_streams_channel.py -v`
Expected: all 6 PASS.

- [ ] **Step 3: Lint / type-check**

Run: `make lint typecheck`
Expected: clean.

### Task 7.4: Wire the new channel into the worker's registry side-effect imports

The class auto-registers via chunk 6's `__init_subclass__` hook, but that only fires once the module is imported. The worker entrypoint needs to import `core.notifier.redis_streams` so the class definition runs before any snapshot reconciliation attempts a `CHANNEL_REGISTRY["mq"]` lookup.

**Files:**
- Modify: `apps/worker/main.py` — add the side-effect import.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_channel_registry.py`:

```python
def test_mq_channel_is_registered_via_worker_import() -> None:
    """Importing `apps.worker.main` should register the `mq` channel type
    because the worker's side-effect imports include `core.notifier.redis_streams`.
    This guards against forgetting the import line after adding a new channel."""
    import apps.worker.main  # noqa: F401 — side-effect: triggers channel registration

    from core.notifier.channel import CHANNEL_REGISTRY
    assert "mq" in CHANNEL_REGISTRY
```

(No `importlib.reload` — Python caches modules in `sys.modules`, so a plain `import` is enough. Reload would not re-execute downstream class statements anyway, so it doesn't defend against test pollution.)

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pytest tests/unit/test_channel_registry.py::test_mq_channel_is_registered_via_worker_import -v`
Expected: FAIL — `"mq" not in CHANNEL_REGISTRY` because nothing imports the module yet.

- [ ] **Step 3: Add the side-effect import to `apps/worker/main.py`**

Below the existing `from core.notifier.http import HttpChannel  # noqa: F401 — side-effect: register http` line, add:

```python
from core.notifier.redis_streams import RedisStreamsChannel  # noqa: F401 — side-effect: register redis_streams
```

- [ ] **Step 4: Re-run the test**

Run: `pytest tests/unit/test_channel_registry.py::test_mq_channel_is_registered_via_worker_import -v`
Expected: PASS.

- [ ] **Step 5: Run the full channel-registry test file**

Run: `pytest tests/unit/test_channel_registry.py -v`
Expected: all tests still pass — the new test is independent of chunk 6's tests; nothing else is touched.

### Task 7.5: Integration test — round-trip via testcontainers Redis

Unit tests with mocked `xadd` prove the channel issues the right calls; this test proves the end-to-end contract: a real `XADD` shows up on a real `XREAD`. Uses the same `testcontainers.redis.RedisContainer` pattern as `tests/integration/test_bus.py`.

**Files:**
- Create: `tests/integration/test_redis_streams_channel.py`

- [ ] **Step 1: Write the integration test**

```python
# tests/integration/test_redis_streams_channel.py
from __future__ import annotations

import json

import pytest
from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]

from core.bus.redis_bus import RedisBus
from core.notifier.redis_streams import RedisStreamsChannel

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_xadd_round_trip_against_real_redis() -> None:
    with RedisContainer("redis:7-alpine") as rc:
        url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}/0"
        bus = RedisBus(url)
        await bus.connect()
        try:
            ch = RedisStreamsChannel(config={"stream": "evt-it"}, bus=bus)
            await ch.start()
            try:
                await ch.send({"k": 1, "subscription_id": "s1"})
                await ch.send({"k": 2, "subscription_id": "s2"})
            finally:
                await ch.stop()

            # Read back via XREAD from beginning.
            client = bus.client
            entries = await client.xread({"evt-it": "0"}, block=100, count=10)
            assert entries  # list of (stream, [(id, {field: value}), ...])
            stream_name, items = entries[0]
            assert stream_name in ("evt-it", b"evt-it")
            assert len(items) == 2
            for _entry_id, fields in items:
                # redis-py with decode_responses=True (used by RedisBus) returns str keys.
                data = fields.get("data") or fields.get(b"data")
                if isinstance(data, bytes):
                    data = data.decode()
                payload = json.loads(data)
                assert payload["k"] in (1, 2)
        finally:
            await bus.disconnect()


@pytest.mark.asyncio
async def test_maxlen_caps_stream_length_approximately() -> None:
    """With `maxlen=2 approximate=True`, after pushing ~200 entries the stream
    length is far below the unbounded total. Redis trims in macro-node chunks
    (~100 entries default), so the cap is approximate and can overshoot the
    nominal `maxlen` by tens of entries on a small stream — but it MUST be
    well below the unbounded push count. We push 200 and assert `< 200` with
    a generous ceiling of 150 to leave headroom for redis-version drift."""
    with RedisContainer("redis:7-alpine") as rc:
        url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}/0"
        bus = RedisBus(url)
        await bus.connect()
        try:
            ch = RedisStreamsChannel(config={"stream": "evt-cap", "maxlen": 2}, bus=bus)
            await ch.start()
            try:
                for i in range(200):
                    await ch.send({"i": i})
            finally:
                await ch.stop()

            client = bus.client
            length = await client.xlen("evt-cap")
            # Approximate trim can overshoot but should be far below 200.
            assert length < 150, f"expected approximate trim well below 200, got {length}"
        finally:
            await bus.disconnect()
```

- [ ] **Step 2: Run the integration tests**

Run: `pytest tests/integration/test_redis_streams_channel.py -v -m integration`
Expected: both tests PASS. The first run pulls the `redis:7-alpine` image (~30s on cold cache); subsequent runs are 1-2s.

If `make test` is configured to skip the `integration` marker by default, that's fine — the test runs explicitly with `-m integration` here and in CI's IT job.

### Task 7.6: Run the full suite, lint, type-check, and commit

- [ ] **Step 1: Run all unit tests**

Run: `pytest tests/unit/ -v`
Expected: clean. Counts roughly: prior baseline + 6 new redis-streams tests + 1 new factory test + 1 new registration test = +8 vs. chunk 6.

- [ ] **Step 2: Run all integration tests**

Run: `pytest tests/integration/ -v -m integration`
Expected: chunks 1-6's IT (e.g. `test_bus.py`, `test_worker_config_reload.py`) plus the 2 new redis-streams tests PASS.

- [ ] **Step 3: Lint / type-check**

Run: `make lint typecheck`
Expected: clean. The `# type: ignore[call-arg]` on the factory's `cls(config=..., bus=...)` is correct here — `CHANNEL_REGISTRY` types its values as `type[Channel]`, and `Channel` is abstract with no `__init__` constraint, so a concrete subclass's specific kwargs aren't visible to the type-checker at the factory boundary.

- [ ] **Step 4: Commit**

```bash
git add core/notifier/redis_streams.py \
        tests/unit/test_redis_streams_channel.py \
        tests/unit/test_channel_registry.py \
        tests/integration/test_redis_streams_channel.py \
        apps/worker/main.py
git commit -m "feat(notifier): add RedisStreamsChannel with MAXLEN trim + retry"
```

---
