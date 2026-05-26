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

## Chunk 8: `WebSocketChannel` + `/ws` server

Closes spec §4.5. Two loosely coupled pieces ship in this chunk and communicate via Redis Pub/Sub:

1. **`WebSocketChannel`** (`core/notifier/websocket.py`) — a `Channel` subclass with `type = "ws"` (matches `ChannelType.ws` in the DB enum, same alignment story as chunk 7's `mq`). `send(payload)` calls `redis.publish(<fanout_channel>, json.dumps(payload))`. Retries on `RedisError` via the shared `retry.py` helper, 3 attempts. Reuses the worker's `RedisBus` connection (`bus=bus` kwarg from chunk 7's `_make_channel_factory`).
2. **`/ws?channel_id=<uuid>` server** (`apps/web/routers/ws.py`) — a FastAPI WebSocket route that resolves `channel_id` → `config["ws_fanout_channel"]` via `ChannelRepo`, subscribes to that Redis pubsub channel, and proxies every received message to the connected WS client through a bounded `asyncio.Queue(maxsize=256)`. Slow consumers drop messages and log a rate-limited warning. Multiple clients per `channel_id` are allowed: each opens its own Redis pubsub subscription, and Redis fans out at the broker level.

The channel and the server **do not share in-process state** — they only share a Redis pubsub channel name (`ws_fanout_channel` in the channel's config blob, used as the publish target by `WebSocketChannel.send` and as the subscribe target by the `/ws` handler). This decoupling matters because the worker process and the web process are independent: the worker `XADD`s nothing for `ws`-type subscriptions; it just publishes, and the web process is the sole subscriber.

**Why Redis Pub/Sub and not Streams for the WS fanout path:**
Pub/Sub is at-most-once: messages with no live subscriber are dropped at the broker, and there is no replay. This is intentional per spec §2 (no auth / no backfill among non-goals): WS clients are observability listeners, not source-of-truth consumers. A bound `RedisStreamsChannel` can be added to the same subscription if the user needs at-least-once for that flow — the two channel types compose by subscription configuration, not by code coupling.

**Why one Redis subscription per WS client** (rather than one shared subscription per `channel_id` with in-process tee'ing):
The shared-subscription approach is more efficient on Redis (one network subscribe per `channel_id` regardless of client count) but adds a per-process registry that must be reference-counted and cleaned up on the last disconnect. With per-client subscriptions, each WS handler owns its own pubsub generator: setup and teardown are tied to the WS connection lifecycle (no shared state, no race conditions). The cost is one extra Redis subscription per client, which is acceptable at M2 scale (no thundering-herd numbers are expected; spec §2 explicitly limits "production scale" to "10s of chains, 100s of subscriptions"). Per-client subscriptions is the simpler design and we choose it knowingly.

**Spec §4.5 scope:**
- New `core/notifier/websocket.py` with `class WebSocketChannel(Channel)`, `type = "ws"`.
- Constructor: `__init__(self, *, config: dict[str, Any], bus: RedisBus, base_delay: float = 1.0)`. Reads `config["ws_fanout_channel"]` (required, `KeyError` on missing). `start()` / `stop()` are no-ops — like `RedisStreamsChannel`, this channel does not own the Redis connection.
- `send(payload)`: `retry_with_backoff(partial(bus.client.publish, ws_fanout_channel, body), max_attempts=3, base_delay=...)` where `body = json.dumps(payload, separators=(",", ":"))`.
- New `apps/web/routers/ws.py` with a single WebSocket endpoint:
  ```python
  @router.websocket("/ws")
  async def ws_endpoint(websocket: WebSocket, channel_id: str, ...) -> None: ...
  ```
- `apps/web/main.py` includes the new WS router.
- Back-pressure: per-client `asyncio.Queue(maxsize=256)`; when full, the producer coroutine **drops** the message and increments a per-client warn-throttle counter. A throttle-aware logger emits at most one warning per client per 60 s window. The constant `_QUEUE_SIZE = 256` and `_WARN_WINDOW_S = 60.0` live as module constants in `ws.py` so they're trivially patchable from tests.

**Modified files this chunk:**
- `core/notifier/websocket.py` — **new**: `WebSocketChannel`.
- `apps/web/routers/ws.py` — **new**: `/ws` endpoint.
- `apps/web/main.py:99-105` — add `from apps.web.routers import ws as ws_router` and `app.include_router(ws_router.router)`.
- `apps/worker/main.py` — add side-effect import `from core.notifier.websocket import WebSocketChannel  # noqa: F401` so the worker also registers the `ws` channel type at process start. (The web process technically doesn't need the channel class itself — only the worker publishes — but importing it in both places keeps `CHANNEL_REGISTRY` symmetric and makes the registration-side-effect test cover the worker entrypoint.)
- `tests/unit/test_websocket_channel.py` — **new**: unit tests for the channel class.
- `tests/unit/test_web_ws.py` — **new**: unit tests for the `/ws` route with a fake bus.
- `tests/unit/test_channel_registry.py` — append registration-side-effect test for `"ws"`.
- `tests/integration/test_ws_fanout.py` — **new**: testcontainers Redis + in-process FastAPI `/ws` + worker's `WebSocketChannel.send` → real broker → live client roundtrip.

**Out of scope this chunk:**
- WebSocket authentication — spec §2 explicit non-goal.
- Reconnect / backfill — at-most-once, no replay. If at-least-once is needed, bind the subscription to a `RedisStreamsChannel` (chunk 7) as well.
- Multi-process WS coordination — every `/ws` connection serves itself from the same Redis pubsub stream; horizontal scale-out works because Redis pubsub fans out across all subscribers regardless of process boundary.
- Channel config JSON-schema validation — same out-of-scope line as chunks 6 and 7.

**Note on FastAPI WebSocket test client:** Starlette's `TestClient.websocket_connect("/ws?channel_id=...")` is a synchronous context manager that opens a real ASGI WS connection. Unlike the HTTP variant of `TestClient`, the WS test session runs the ASGI app on a **separate thread with its own asyncio loop** (the "portal" loop), while the test body keeps running in the pytest-asyncio loop (or in the test thread directly for sync test bodies). This has two consequences the chunk's tests must work around:

1. **Cross-loop `asyncio.Queue` is unsafe.** A queue created inside the WS route (portal loop) cannot be `await q.put(...)`'d from the test body (different loop). The `_FakeBus` in Task 8.4 captures the route's loop at `subscribe()` time and exposes a SYNC `feed_threadsafe(...)` method that uses `loop.call_soon_threadsafe(queue.put_nowait, payload)` to schedule the put on the correct loop. The route-side queue (`asyncio.Queue(maxsize=_QUEUE_SIZE)` in `ws.py`) is created and consumed entirely in the portal loop, so it's safe there.
2. **`WebSocketTestSession.receive_text()` takes no `timeout` kwarg.** It is a blocking call that reads from a thread-internal buffer; pytest's default test timeout (set via `pytest.ini` if configured) bounds runaway tests. The unit-level WS tests rely on deterministic feed-then-receive ordering rather than a per-call timeout.

The unit-level WS tests in this chunk **monkeypatch `_resolve_fanout_channel`** so the route never touches the DB on the WS path. This sidesteps a related cross-loop hazard (the `db` fixture's async engine is bound to the pytest-asyncio loop, and the route would call `db.session()` on the portal loop). DB-side resolver logic is covered by separate non-WS unit tests that call `_resolve_fanout_channel(...)` directly in the pytest-asyncio loop, where the engine is at home. The integration test (Task 8.6) uses the same monkeypatch strategy and focuses on the real Redis ↔ WS roundtrip. (`tests/unit/test_web_chains.py` shows the non-WS TestClient pattern; the WS variant is a documented subset of the same API.)

### Task 8.1: Red — `WebSocketChannel` unit tests

Mirrors the structure of `tests/unit/test_redis_streams_channel.py` from chunk 7: same `AsyncMock(spec=RedisBus)` + plain `AsyncMock()` client + side_effect-driven retry tests, same docstring rationale for why we avoid `spec=Redis`. Five behaviours: (a) basic publish; (b) JSON-encodes payload; (c) transient `RedisError` retried; (d) persistent error → `RetryExhausted`; (e) missing `ws_fanout_channel` config raises at construction.

**Files:**
- Create: `tests/unit/test_websocket_channel.py`

- [ ] **Step 1: Write the failing test file**

```python
# tests/unit/test_websocket_channel.py
from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import RedisError

from core.bus.redis_bus import RedisBus
from core.notifier.retry import RetryExhausted
from core.notifier.websocket import WebSocketChannel


def _fake_bus_with_client(client: AsyncMock) -> AsyncMock:
    """Same helper as in test_redis_streams_channel.py. Uses plain AsyncMock()
    for the client because `redis.asyncio.Redis.publish` is not declared as
    `async def` (same gotcha as `.xadd`)."""
    bus = AsyncMock(spec=RedisBus)
    bus.client = client
    return bus


@pytest.mark.asyncio
async def test_publish_sends_json_payload_to_configured_fanout_channel() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-x"}, bus=bus)
    await ch.start()
    try:
        await ch.send({"k": 1, "subscription_id": "s1"})
    finally:
        await ch.stop()

    client.publish.assert_awaited_once()
    args, _kwargs = client.publish.call_args
    assert args[0] == "fanout-x"
    assert json.loads(args[1]) == {"k": 1, "subscription_id": "s1"}


@pytest.mark.asyncio
async def test_transient_redis_error_is_retried_then_succeeds() -> None:
    client = AsyncMock()
    client.publish.side_effect = [RedisError("temporary"), 1]
    bus = _fake_bus_with_client(client)
    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-x"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.publish.await_count == 2


@pytest.mark.asyncio
async def test_persistent_redis_error_raises_retry_exhausted() -> None:
    client = AsyncMock()
    client.publish.side_effect = RedisError("hard down")
    bus = _fake_bus_with_client(client)
    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-x"}, bus=bus, base_delay=0.0)
    await ch.start()
    try:
        with pytest.raises(RetryExhausted):
            await ch.send({"k": 1})
    finally:
        await ch.stop()
    assert client.publish.await_count == 3


@pytest.mark.asyncio
async def test_missing_fanout_channel_config_raises_at_construction() -> None:
    client = AsyncMock()
    bus = _fake_bus_with_client(client)
    with pytest.raises(KeyError):
        WebSocketChannel(config={}, bus=bus)


def test_type_attribute_matches_db_enum_ws_slot() -> None:
    # Confirms the auto-registration key matches `ChannelType.ws` in the DB.
    assert WebSocketChannel.type == "ws"
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_websocket_channel.py -v`
Expected: 5 FAILs with `ModuleNotFoundError: No module named 'core.notifier.websocket'`.

### Task 8.2: Green — implement `WebSocketChannel`

**Files:**
- Create: `core/notifier/websocket.py`

- [ ] **Step 1: Write the minimal implementation**

```python
# core/notifier/websocket.py
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


class WebSocketChannel(Channel):
    """Redis Pub/Sub-backed notification driver for WebSocket fan-out.

    Publishes payloads to a Redis pubsub channel; the `/ws?channel_id=` server
    in `apps/web/routers/ws.py` subscribes to the same channel and proxies
    messages to connected WebSocket clients. At-most-once semantics — messages
    with no live subscriber are dropped at the broker.
    """

    type = "ws"

    def __init__(
        self,
        *,
        config: dict[str, Any],
        bus: RedisBus,
        base_delay: float = 1.0,
    ) -> None:
        self._fanout_channel: str = config["ws_fanout_channel"]
        self._bus = bus
        self._base_delay = base_delay

    async def start(self) -> None:
        # The bus is started/stopped by the worker; nothing to do here.
        return None

    async def stop(self) -> None:
        return None

    async def send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":"))
        await retry_with_backoff(
            partial(self._publish_once, body=body),
            max_attempts=3,
            base_delay=self._base_delay,
        )

    async def _publish_once(self, *, body: str) -> None:
        client = self._bus.client
        try:
            await client.publish(self._fanout_channel, body)
        except RedisError:
            raise
```

- [ ] **Step 2: Re-run the unit tests**

Run: `pytest tests/unit/test_websocket_channel.py -v`
Expected: all 5 PASS.

- [ ] **Step 3: Lint / type-check**

Run: `make lint typecheck`
Expected: clean.

### Task 8.3: Wire the WS channel into the worker's registry side-effect imports

Same pattern as Task 7.4. Without the side-effect import, the `"ws"` key never lands in `CHANNEL_REGISTRY` and the worker would `KeyError` at first reconcile for a `ws`-type subscription.

**Files:**
- Modify: `apps/worker/main.py` — add side-effect import.
- Modify: `tests/unit/test_channel_registry.py` — add registration test.

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_channel_registry.py`:

```python
def test_ws_channel_is_registered_via_worker_import() -> None:
    """Importing `apps.worker.main` should register the `ws` channel type
    because the worker's side-effect imports include `core.notifier.websocket`."""
    import apps.worker.main  # noqa: F401 — side-effect: triggers channel registration

    from core.notifier.channel import CHANNEL_REGISTRY
    assert "ws" in CHANNEL_REGISTRY
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pytest tests/unit/test_channel_registry.py::test_ws_channel_is_registered_via_worker_import -v`
Expected: FAIL — `"ws" not in CHANNEL_REGISTRY`.

- [ ] **Step 3: Add the side-effect import to `apps/worker/main.py`**

Below the existing redis_streams side-effect import added in chunk 7 Task 7.4 Step 3, add:

```python
from core.notifier.websocket import WebSocketChannel  # noqa: F401 — side-effect: register ws
```

- [ ] **Step 4: Re-run the test**

Run: `pytest tests/unit/test_channel_registry.py::test_ws_channel_is_registered_via_worker_import -v`
Expected: PASS.

- [ ] **Step 5: Run the full channel-registry suite**

Run: `pytest tests/unit/test_channel_registry.py -v`
Expected: all tests still pass (chunks 6, 7's tests + the new one).

### Task 8.4: Red — `/ws` route unit tests with a fake bus

Six behaviours under test, split into two layers:

**Route-level tests** (sync `def`, `_resolve_fanout_channel` monkeypatched so the route never touches the DB on the WS path; the `_FakeBus` is cross-loop-safe — see preamble note):
(a) Connection that resolves a valid `channel_id` to a `ws_fanout_channel` receives every message fed via `bus.feed_threadsafe(...)`.
(b) Connection with an unresolvable `channel_id` is closed immediately with code 1008 (policy violation).
(c) Slow-consumer scenario: with `_QUEUE_SIZE` patched to 4 and 20 messages fed without immediate draining, the route does not crash and the first messages received are valid JSON in monotonic order. (Exact drop count is not asserted because the consumer task drains concurrently — a focused drop-path test with caplog can be added later if needed.)

**Resolver-direct tests** (async `def` with `db` fixture, no WS at all — runs entirely in the pytest-asyncio loop so the engine sits at home):
(d) `_resolve_fanout_channel` returns the `ws_fanout_channel` config value for a `ws`-type channel row.
(e) `_resolve_fanout_channel` returns `None` for an unknown `channel_id`.
(f) `_resolve_fanout_channel` returns `None` for a non-`ws`-type channel row (e.g. an HTTP channel).

**Files:**
- Create: `tests/unit/test_web_ws.py`

- [ ] **Step 1: Write the failing test file**

```python
# tests/unit/test_web_ws.py
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.web.routers import ws as ws_module
from core.config.db import Database
from core.config.models import Base, ChannelType
from core.config.repositories import ChannelRepo


class _FakeBus:
    """Cross-loop-safe RedisBus stand-in. The test body runs in
    pytest-asyncio's loop (or the test thread for sync tests); the FastAPI
    route runs in Starlette TestClient's portal loop. `feed_threadsafe()`
    is a SYNC method that schedules `queue.put_nowait(payload)` on the
    portal loop via `loop.call_soon_threadsafe(...)`, so the queue and the
    feeder live in the same loop where the queue was created."""

    def __init__(self) -> None:
        self._queues: dict[str, asyncio.Queue[dict[str, Any] | None]] = {}
        self._loops: dict[str, asyncio.AbstractEventLoop] = {}

    async def ping(self) -> bool:
        return True

    async def subscribe(
        self, channel: str, *, ready: asyncio.Event | None = None
    ) -> AsyncIterator[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self._queues[channel] = q
        self._loops[channel] = loop
        if ready is not None:
            ready.set()
        try:
            while True:
                msg = await q.get()
                if msg is None:  # sentinel: close the generator
                    return
                yield msg
        finally:
            self._queues.pop(channel, None)
            self._loops.pop(channel, None)

    def feed_threadsafe(self, channel: str, payload: dict[str, Any]) -> None:
        # Poll briefly for the subscribe() generator to attach in the
        # portal loop. The route schedules `producer_task` after `accept()`,
        # so the queue usually exists within a few ms of websocket_connect().
        for _ in range(200):  # ~2s budget
            if channel in self._loops and channel in self._queues:
                break
            time.sleep(0.01)
        else:
            raise RuntimeError(f"no subscriber registered for {channel}")
        loop = self._loops[channel]
        queue = self._queues[channel]
        loop.call_soon_threadsafe(queue.put_nowait, payload)

    def stop_threadsafe(self, channel: str) -> None:
        loop = self._loops.get(channel)
        q = self._queues.get(channel)
        if loop is None or q is None:
            return
        loop.call_soon_threadsafe(q.put_nowait, None)


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


def _client(db: Database, bus: _FakeBus) -> TestClient:
    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: bus
    return TestClient(app)


def _stub_resolver(mapping: dict[str, str | None]):
    """Return an async resolver that maps `channel_id` to a fanout name or
    `None`. Used to bypass the DB lookup in unit tests."""
    async def _resolver(channel_id: str, _db: Database) -> str | None:
        return mapping.get(channel_id)
    return _resolver


# -- Route-level tests (sync; resolver monkeypatched; DB unused by route) -----

def test_ws_client_receives_messages_from_fanout_channel(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        ws_module,
        "_resolve_fanout_channel",
        _stub_resolver({"valid": "fanout-recv"}),
    )
    bus = _FakeBus()
    with _client(db, bus) as c:
        with c.websocket_connect("/ws?channel_id=valid") as ws:
            bus.feed_threadsafe("fanout-recv", {"hello": "world"})
            text = ws.receive_text()
    assert json.loads(text) == {"hello": "world"}


def test_unknown_channel_id_closes_with_policy_violation(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from starlette.websockets import WebSocketDisconnect

    monkeypatch.setattr(
        ws_module, "_resolve_fanout_channel", _stub_resolver({})
    )
    bus = _FakeBus()
    with _client(db, bus) as c:
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with c.websocket_connect("/ws?channel_id=missing") as ws:
                ws.receive_text()
    assert exc_info.value.code == 1008  # policy violation


def test_slow_consumer_does_not_crash_when_queue_is_full(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With `_QUEUE_SIZE=4` and 20 messages fed without immediate draining,
    the producer's `put_nowait` raises `QueueFull` and the route's drop
    branch is exercised. We assert: (a) no exception leaks; (b) the first 4
    messages received are valid JSON in monotonic order. Exact drop count
    is not asserted because the consumer task drains concurrently with the
    producer in the portal loop — when a drop occurs is a function of
    portal-loop scheduling, not test input."""
    monkeypatch.setattr(ws_module, "_QUEUE_SIZE", 4)
    monkeypatch.setattr(
        ws_module,
        "_resolve_fanout_channel",
        _stub_resolver({"X": "fanout-drop"}),
    )

    bus = _FakeBus()
    with _client(db, bus) as c:
        with c.websocket_connect("/ws?channel_id=X") as ws:
            for i in range(20):
                bus.feed_threadsafe("fanout-drop", {"i": i})
            received: list[dict[str, Any]] = []
            for _ in range(4):
                received.append(json.loads(ws.receive_text()))
            bus.stop_threadsafe("fanout-drop")
    assert len(received) == 4
    nums = [m["i"] for m in received]
    assert nums == sorted(nums)  # FIFO order until drop kicks in


# -- Resolver-direct tests (async; DB used; no WS) ----------------------------

@pytest.mark.asyncio
async def test_resolver_returns_fanout_for_ws_channel(db: Database) -> None:
    async with db.session() as s:
        row = await ChannelRepo(s).create(
            name="wsx",
            type=ChannelType.ws,
            config={"ws_fanout_channel": "fanout-direct"},
        )
        await s.commit()
        channel_id = row.id
    result = await ws_module._resolve_fanout_channel(channel_id, db)
    assert result == "fanout-direct"


@pytest.mark.asyncio
async def test_resolver_returns_none_for_unknown_channel(db: Database) -> None:
    result = await ws_module._resolve_fanout_channel(
        "00000000-0000-0000-0000-000000000000", db
    )
    assert result is None


@pytest.mark.asyncio
async def test_resolver_returns_none_for_non_ws_type(db: Database) -> None:
    async with db.session() as s:
        row = await ChannelRepo(s).create(
            name="webhook",
            type=ChannelType.http,
            config={"url": "http://example.com/hook"},
        )
        await s.commit()
        channel_id = row.id
    result = await ws_module._resolve_fanout_channel(channel_id, db)
    assert result is None
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_web_ws.py -v`
Expected: 6 FAILs with `ModuleNotFoundError: No module named 'apps.web.routers.ws'`.

### Task 8.5: Green — implement `/ws` route

**Files:**
- Create: `apps/web/routers/ws.py`
- Modify: `apps/web/main.py:99-105` — include the new router.

- [ ] **Step 1: Write the minimal `/ws` handler**

```python
# apps/web/routers/ws.py
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import structlog
from fastapi import APIRouter, Depends, WebSocket, status

from apps.web.deps import get_bus, get_db
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import ChannelType
from core.config.repositories import ChannelRepo

log = structlog.get_logger(__name__)
router = APIRouter(tags=["ws"])

# Tunable knobs (patched in tests; documented in the chunk preamble).
_QUEUE_SIZE = 256
_WARN_WINDOW_S = 60.0


async def _resolve_fanout_channel(
    channel_id: str,
    db: Database,
) -> str | None:
    """Look up the `ws_fanout_channel` config value for `channel_id`. Returns
    None if the row is missing OR the row's type is not `ws`."""
    async with db.session() as s:
        row = await ChannelRepo(s).get(channel_id)
    if row is None or row.type != ChannelType.ws:
        return None
    fanout: str | None = row.config.get("ws_fanout_channel")
    return fanout


@router.websocket("/ws")
async def ws_endpoint(
    websocket: WebSocket,
    channel_id: str,
    db: Database = Depends(get_db),  # noqa: B008
    bus: RedisBus = Depends(get_bus),  # noqa: B008
) -> None:
    # `get_db` is a plain `def` returning `Database` (not an async generator),
    # so it unwinds cleanly on WS close and `app.dependency_overrides[get_db]`
    # works for unit tests. We avoid `Depends(get_session)` (which IS an
    # async generator) because async-gen Depends in WS routes don't unwind
    # cleanly on early disconnect.
    fanout = await _resolve_fanout_channel(channel_id, db)
    if fanout is None:
        # Accept-then-close pattern: starlette requires accept() before close().
        await websocket.accept()
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=_QUEUE_SIZE)
    last_warn_at = 0.0

    async def _producer() -> None:
        nonlocal last_warn_at
        ready = asyncio.Event()
        gen = bus.subscribe(fanout, ready=ready)
        try:
            async for msg in gen:
                body = json.dumps(msg, separators=(",", ":"))
                try:
                    queue.put_nowait(body)
                except asyncio.QueueFull:
                    now = time.monotonic()
                    if now - last_warn_at >= _WARN_WINDOW_S:
                        log.warning(
                            "ws.slow_consumer_dropping_messages",
                            channel_id=channel_id,
                            fanout=fanout,
                        )
                        last_warn_at = now
        finally:
            await gen.aclose()  # type: ignore[attr-defined]

    async def _consumer() -> None:
        while True:
            body = await queue.get()
            await websocket.send_text(body)

    producer_task = asyncio.create_task(_producer(), name=f"ws.producer:{channel_id}")
    consumer_task = asyncio.create_task(_consumer(), name=f"ws.consumer:{channel_id}")
    try:
        # Either side exiting (client disconnect, broker close) ends the
        # session. We propagate by cancelling the other coroutine.
        done, pending = await asyncio.wait(
            {producer_task, consumer_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        for t in done | pending:
            try:
                await t
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                # WebSocketDisconnect on consumer-side is expected (client gone).
                # Anything else is a programmer error and worth a log line so
                # operators see a footprint when a WS session dies non-cleanly.
                log.warning(
                    "ws.session_ended_with_exception",
                    channel_id=channel_id,
                    task=t.get_name(),
                    exc=repr(exc),
                )
    finally:
        try:
            await websocket.close()
        except RuntimeError:
            # Already closed by the client; ignore.
            pass
```

Wire-up in `apps/web/main.py`. Insert below the existing router imports:

```python
    from apps.web.routers import ws as ws_router  # noqa: E402

    app.include_router(ws_router.router)
```

- [ ] **Step 2: Re-run the route tests**

Run: `pytest tests/unit/test_web_ws.py -v`
Expected: all 6 PASS (3 route tests + 3 resolver-direct tests). The slow-consumer test may flake if portal-loop scheduling shifts; if it does, the fix is to lower the feed count or insert a `time.sleep(0)` between feeds — but on a quiet CI host it should be deterministic because the fake's `feed_threadsafe` schedules a sync `put_nowait` per call.

- [ ] **Step 3: Lint / type-check**

Run: `make lint typecheck`
Expected: clean. The `Depends(get_db)` / `Depends(get_bus)` pattern is intentional — see comment in the handler.

### Task 8.6: Integration test — full WS fanout via testcontainers Redis

End-to-end proof: spin up real Redis, build a `WebSocketChannel` against it, build the FastAPI app against it, connect a WS client, send via the channel, receive via the WS client. This is the test that catches Redis-API-shape mismatches that the mocked unit tests can't. The IT monkeypatches `_resolve_fanout_channel` so the test focuses on the Redis ↔ WS roundtrip; DB-side resolver logic is already covered by the resolver-direct unit tests in Task 8.4.

**Files:**
- Create: `tests/integration/test_ws_fanout.py`

- [ ] **Step 1: Write the integration test**

```python
# tests/integration/test_ws_fanout.py
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from testcontainers.redis import RedisContainer  # type: ignore[import-untyped]

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.web.routers import ws as ws_module
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.notifier.websocket import WebSocketChannel

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def real_redis() -> AsyncIterator[RedisBus]:
    with RedisContainer("redis:7-alpine") as rc:
        url = f"redis://{rc.get_container_host_ip()}:{rc.get_exposed_port(6379)}/0"
        bus = RedisBus(url)
        await bus.connect()
        try:
            yield bus
        finally:
            await bus.disconnect()


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    # The route reads `db` via `Depends(get_db)` but the resolver is
    # monkeypatched below, so the DB sits idle. An in-memory engine is
    # cheap and keeps the dependency override type-honest.
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    yield d
    await d.disconnect()


@pytest.mark.asyncio
async def test_end_to_end_fanout(
    real_redis: RedisBus, db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _resolver(_channel_id: str, _db: Database) -> str | None:
        return "fanout-it"
    monkeypatch.setattr(ws_module, "_resolve_fanout_channel", _resolver)

    app = create_app(lifespan=None)
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_bus] = lambda: real_redis

    ch = WebSocketChannel(config={"ws_fanout_channel": "fanout-it"}, bus=real_redis)
    await ch.start()
    try:
        with TestClient(app) as c, c.websocket_connect("/ws?channel_id=any") as ws:
            # Redis Pub/Sub is at-most-once: if we publish before the
            # /ws handler's `bus.subscribe(...)` has attached, the message
            # is dropped at the broker. A small fixed delay matches the
            # existing test_bus.py pattern; if this flakes, raise to 0.5s
            # and add a single retry of `ch.send(...)` — at-most-once
            # semantics make retry an acceptable TEST pattern, NOT a code
            # change in the route or channel.
            await asyncio.sleep(0.2)
            await ch.send({"e2e": True, "n": 1})
            text = ws.receive_text()
    finally:
        await ch.stop()

    assert json.loads(text) == {"e2e": True, "n": 1}
```

- [ ] **Step 2: Run the integration test**

Run: `pytest tests/integration/test_ws_fanout.py -v -m integration`
Expected: PASS. First run pulls `redis:7-alpine`; subsequent runs are 1-2s.

If the test is flaky due to the at-most-once nature of pubsub (subscribe-vs-publish race), bump the `asyncio.sleep(0.2)` to `0.5` and add a single retry of `ch.send(...)` followed by `ws.receive_text()`. Document the retry in the test docstring — at-most-once semantics make this an acceptable test pattern, NOT a code change.

### Task 8.7: Close-out

- [ ] **Step 1: Run all unit tests**

Run: `pytest tests/unit/ -v`
Expected: clean. New count vs chunk 7: +5 (WS channel) +6 (WS route + resolver) +1 (registration test) = +12 tests.

- [ ] **Step 2: Run all integration tests**

Run: `pytest tests/integration/ -v -m integration`
Expected: chunks 1-7 IT + the new `test_ws_fanout.py` PASS.

- [ ] **Step 3: Lint / type-check**

Run: `make lint typecheck`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add core/notifier/websocket.py \
        apps/web/routers/ws.py \
        apps/web/main.py \
        apps/worker/main.py \
        tests/unit/test_websocket_channel.py \
        tests/unit/test_web_ws.py \
        tests/unit/test_channel_registry.py \
        tests/integration/test_ws_fanout.py
git commit -m "feat(notifier): WebSocketChannel + /ws fanout server with back-pressure"
```

---

## Chunk 9: `arg_filters` Pydantic tighten + EVM ERC-20 E2E

The EVM-segment close-out. Three pieces that ship together because they share the same theme — "harden the EVM path end-to-end and prove it" — and because the offender-scanner + Pydantic tighten are both small enough that batching avoids a tag whiplash between sub-commits.

1. **`arg_filters` Pydantic tightening** (`apps/web/schemas.py`): closes spec §9.1 item 5. The schema currently accepts `dict[str, Any]`, which lets the API store nested dicts that `filters.evaluate(...)` will silently mis-match against. Chunk 9 narrows the value type to `str | int | bool | list[str | int | bool]` and additionally runs the existing `core.matcher.filters.validate(...)` (typo grammar — `_eq`/`_ne`/etc. rejected) inside a Pydantic `field_validator` so the rejection surfaces at API time with a 422, not at first match attempt.
2. **`scripts/validate_arg_filters.py`** — a one-shot operator script that scans every `subscriptions.arg_filters` JSON blob in the database and prints any row whose value shape does not conform to the tightened schema OR fails `filters.validate(...)`. Returns exit code 0 (clean) or 1 (offenders printed). M2 ships no migration for this — it's an inspection tool the operator runs once at upgrade time. Spec §4.8 final sentence.
3. **EVM ERC-20 E2E** (`tests/e2e/test_evm_erc20_e2e.py`): the capstone test for the EVM segment. Anvil from chunk 11 of M1's E2E fixture is reused; a minimal ERC-20 contract is compiled via `py-solc-x` at session setup, deployed, and the test drives the full chain `Transfer(...)` → block → `Erc20TransferParser` → Matcher → `HttpChannel` → webhook receiver path. Asserts payload shape per spec §8 with `kind="token_transfer"` and `args.from / args.to / args.value`.

After Task 9.6, the branch is tagged `m2-evm-complete`. The Solana segment (chunks 10–14) builds on the same skeleton but does NOT depend on this tag at code level — the tag exists only for human navigation.

**Modified files this chunk:**
- `apps/web/schemas.py` — narrow `arg_filters` value type + `field_validator`.
- `scripts/__init__.py` — **new** if absent (empty marker).
- `scripts/validate_arg_filters.py` — **new**: offender-scanner.
- `tests/unit/test_schemas_arg_filters.py` — **new**: Pydantic-level rejection tests.
- `tests/unit/test_validate_arg_filters_script.py` — **new**: scanner output tests.
- `tests/e2e/conftest.py:60-…` — extend with `erc20_token` fixture (depends on `anvil` fixture from M1).
- `tests/e2e/test_evm_erc20_e2e.py` — **new**: the ERC-20 E2E test.
- `pyproject.toml` — add `py-solc-x>=2.0,<3` to `[project.optional-dependencies].dev`.

**Out of scope this chunk:**
- Backfilling bad existing rows. The scanner only PRINTS offenders; rewriting them is an operator decision (could be deletion, partial-update, or schema migration in M3). Spec §4.8 stops at "print offenders for operator review".
- API-time conversion of legacy `arg_filters` blobs into the tightened shape. If the operator wants to migrate values (e.g. wrap a bare int in a list), they do it manually. The scanner identifies; it does not transform.
- Solana E2E — that's the entire Solana segment (chunks 10–14).
- Multi-chain E2E. Anvil only. Solana E2E is chunk 14.
- `core/matcher/filters.py` — no logic change. We only EXPORT `validate` from a stable import path; the file itself is untouched. (Spec §4.8: "filters.validate (existing function)".)

**Pydantic version note:** This codebase pins Pydantic v2 (`pydantic>=2,<3` in `pyproject.toml`, confirmed by `pydantic.ConfigDict` usage in `apps/web/schemas.py`). Chunk 9 uses `from pydantic import field_validator` and the v2 decorator signature `@field_validator("arg_filters")`. If the implementer is reading the v1 docs by accident, the v1 equivalent is `@validator(...)` — that would silently no-op because Pydantic v2 doesn't recognize the name. Stick with `field_validator`.

**`py-solc-x` note:** First test run downloads the solc 0.8.20 binary (~2-3 s, cached under `~/.solcx/`). Subsequent runs are O(ms). If the host can't reach `binaries.soliditylang.org` (offline CI), the test fails at compile-time with a clear `SolcInstallationError`; treat this the same as the "anvil not installed" skip in M1 chunk 11 — gate the E2E with the existing `e2e` marker so CI labels can opt out.

### Task 9.1: Red — Pydantic schema rejection tests

Tests live in their own file rather than appending to `test_web_subscriptions.py` because the focus is the schema validator in isolation (no DB / API layer needed). Three behaviour groups: (a) accepted value shapes round-trip; (b) value-type rejections produce a Pydantic `ValidationError`; (c) typo-grammar rejection delegates to `filters.validate`.

**Files:**
- Create: `tests/unit/test_schemas_arg_filters.py`

- [ ] **Step 1: Write the failing test file**

```python
# tests/unit/test_schemas_arg_filters.py
from __future__ import annotations

import pytest
from pydantic import ValidationError

from apps.web.schemas import SubscriptionCreate


def _base(**overrides):
    payload = dict(
        name="s1",
        chain_id="eth-mainnet",
        address=None,
        abi_id=None,
        match_kind="native_transfer",
        match_name=None,
        arg_filters={},
        enabled=True,
    )
    payload.update(overrides)
    return payload


# -- accepted shapes ----------------------------------------------------------

def test_arg_filters_accepts_string_value() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"from": "0xabc"}))
    assert s.arg_filters == {"from": "0xabc"}


def test_arg_filters_accepts_int_value() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"value_gte": 1000}))
    assert s.arg_filters == {"value_gte": 1000}


def test_arg_filters_accepts_bool_value() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"is_live": True}))
    assert s.arg_filters is not None
    assert s.arg_filters["is_live"] is True


def test_arg_filters_accepts_list_of_primitives() -> None:
    s = SubscriptionCreate(**_base(arg_filters={"to_in": ["0x1", "0x2", "0x3"]}))
    assert s.arg_filters == {"to_in": ["0x1", "0x2", "0x3"]}


def test_arg_filters_empty_dict_is_accepted() -> None:
    s = SubscriptionCreate(**_base(arg_filters={}))
    assert s.arg_filters == {}


# -- value-type rejections ----------------------------------------------------

def test_arg_filters_rejects_nested_dict() -> None:
    with pytest.raises(ValidationError) as exc_info:
        SubscriptionCreate(**_base(arg_filters={"from": {"nested": "dict"}}))
    # The error should reference the offending field path.
    msg = str(exc_info.value)
    assert "arg_filters" in msg


def test_arg_filters_rejects_float_value() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"value_gte": 1.5}))


def test_arg_filters_rejects_none_value() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"x": None}))


def test_arg_filters_rejects_list_with_dict_element() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(
            **_base(arg_filters={"to_in": ["0x1", {"nested": "x"}]})
        )


def test_arg_filters_rejects_list_with_float_element() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"vals_in": [1, 2.5]}))


# -- typo-grammar rejection (delegates to filters.validate) -------------------

def test_arg_filters_rejects_typo_eq_suffix() -> None:
    """`_eq` is a forbidden typo — `filters.validate` catches it."""
    with pytest.raises(ValidationError) as exc_info:
        SubscriptionCreate(**_base(arg_filters={"value_eq": 100}))
    assert "value_eq" in str(exc_info.value) or "unknown operator" in str(exc_info.value)


def test_arg_filters_rejects_typo_ne_suffix() -> None:
    with pytest.raises(ValidationError):
        SubscriptionCreate(**_base(arg_filters={"to_ne": "0x1"}))


def test_arg_filters_accepts_valid_operator_suffixes() -> None:
    """`_in`, `_gte`, `_lte`, plain field — all pass `filters.validate`."""
    s = SubscriptionCreate(**_base(arg_filters={
        "to": "0xabc",
        "to_in": ["0x1", "0x2"],
        "value_gte": 100,
        "value_lte": 200,
    }))
    assert set(s.arg_filters.keys()) == {"to", "to_in", "value_gte", "value_lte"}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_schemas_arg_filters.py -v`
Expected: the 5 acceptance tests PASS (current `dict[str, Any]` accepts anything). The 7 rejection tests FAIL because nothing rejects nested dicts / floats / `_eq` today.

### Task 9.2: Green — tighten `arg_filters` schema

**Files:**
- Modify: `apps/web/schemas.py:9-12`, `:66` — add type alias + `field_validator` on `SubscriptionCreate`. (`SubscriptionOut.arg_filters` at line 80 stays `dict[str, Any]` deliberately; do NOT tighten the OUT schema.)

- [ ] **Step 1: Update the imports**

Replace the existing `from typing import Any, Literal` and `from pydantic import BaseModel, ConfigDict, Field` with:

```python
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from core.matcher.filters import FilterError, validate as _validate_filter_keys

ArgFilterValue = str | int | bool | list[str | int | bool]
```

The module-level type alias lives at the top of the file (after imports) so both `SubscriptionCreate` and `SubscriptionOut` reuse the same shape. (`SubscriptionOut` keeps the loose `dict[str, Any]` for back-compat with already-stored rows that may pre-date this tightening — the OUT schema is informational only and `Any` keeps deserialization permissive. The IN schema is the gate.)

- [ ] **Step 2: Tighten the `SubscriptionCreate.arg_filters` field**

Replace line 66 of `apps/web/schemas.py`:

```python
# was: arg_filters: dict[str, Any] = Field(default_factory=dict)
arg_filters: dict[str, ArgFilterValue] = Field(default_factory=dict)
```

- [ ] **Step 3: Add the typo-grammar validator**

Append a `field_validator` to the `SubscriptionCreate` class body, AFTER the existing fields:

```python
    @field_validator("arg_filters")
    @classmethod
    def _check_operator_grammar(
        cls, v: dict[str, ArgFilterValue]
    ) -> dict[str, ArgFilterValue]:
        """Reject typo'd operator suffixes (`_eq`, `_ne`, ...) by delegating
        to `core.matcher.filters.validate`. Raises Pydantic ValidationError
        with a clear message instead of crashing at first match attempt."""
        try:
            _validate_filter_keys(v)
        except FilterError as exc:
            raise ValueError(str(exc)) from exc
        return v
```

`field_validator` re-wraps the raised `ValueError` into a Pydantic `ValidationError` automatically; the API layer's `RequestValidationError` handler then returns 422 with the error string. (The wrap-as-ValueError pattern is the documented v2 idiom — Pydantic does NOT catch arbitrary exceptions from validators by design.)

- [ ] **Step 4: Re-run the schema tests**

Run: `pytest tests/unit/test_schemas_arg_filters.py -v`
Expected: all 12 PASS.

- [ ] **Step 5: Re-run the existing subscriptions API tests**

Run: `pytest tests/unit/test_web_subscriptions.py -v`
Expected: clean — the test suite uses simple equality filters that survive tightening. If a test breaks because it was using a nested dict, that's a real bug being surfaced; update the test to use a valid shape rather than relaxing the schema.

- [ ] **Step 6: Lint + typecheck**

Run: `make lint typecheck`
Expected: clean. mypy may warn on `dict[str, ArgFilterValue]` containing `bool` because `bool` is a subtype of `int` in Python — the union `str | int | bool` is technically redundant. Keep `bool` explicit anyway for readability; if mypy complains, add `# type: ignore[misc]` to the alias line.

- [ ] **Step 7: Commit**

```bash
git add apps/web/schemas.py tests/unit/test_schemas_arg_filters.py
git commit -m "feat(api): tighten arg_filters Pydantic value-type + delegate to filters.validate"
```

### Task 9.3: Offender-scanner script

A one-shot operator tool: read every `subscriptions.arg_filters` JSON column in the DB, run it through the same validator the schema uses, and print every offending row. Exit 0 if clean, 1 if any offender. The script imports `core.matcher.filters.validate` and uses `pydantic.TypeAdapter(dict[str, ArgFilterValue]).validate_python(...)` so the two checks (value-shape + typo grammar) match `SubscriptionCreate` exactly. Future schema drift only needs to update the type alias.

**Files:**
- Create: `scripts/__init__.py` (empty marker — only if not present already)
- Create: `scripts/validate_arg_filters.py`
- Create: `tests/unit/test_validate_arg_filters_script.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_validate_arg_filters_script.py
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from core.config.db import Database
from core.config.models import Base, MatchKind, Subscription
from scripts.validate_arg_filters import scan_database


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def _insert_subscription(
    db: Database, *, name: str, arg_filters: dict[str, Any]
) -> str:
    """Insert directly via the ORM, bypassing the Pydantic schema so we can
    seed rows that the tightened schema would reject."""
    async with db.session() as s:
        row = Subscription(
            name=name,
            chain_id="eth-mainnet",
            address=None,
            abi_id=None,
            match_kind=MatchKind.native_transfer,
            match_name=None,
            arg_filters=arg_filters,
            enabled=True,
        )
        s.add(row)
        await s.commit()
        return row.id


@pytest.mark.asyncio
async def test_scanner_returns_empty_for_clean_database(db: Database) -> None:
    await _insert_subscription(db, name="good1", arg_filters={"from": "0xabc"})
    await _insert_subscription(db, name="good2", arg_filters={"value_gte": 100})
    offenders = await scan_database(db)
    assert offenders == []


@pytest.mark.asyncio
async def test_scanner_flags_nested_dict_in_value(db: Database) -> None:
    bad_id = await _insert_subscription(
        db, name="bad-nested", arg_filters={"from": {"nested": "x"}}
    )
    offenders = await scan_database(db)
    assert len(offenders) == 1
    assert offenders[0].subscription_id == bad_id
    assert offenders[0].name == "bad-nested"
    assert "from" in offenders[0].reason or "nested" in offenders[0].reason


@pytest.mark.asyncio
async def test_scanner_flags_typo_operator(db: Database) -> None:
    bad_id = await _insert_subscription(
        db, name="bad-eq", arg_filters={"value_eq": 100}
    )
    offenders = await scan_database(db)
    assert len(offenders) == 1
    assert offenders[0].subscription_id == bad_id
    assert "value_eq" in offenders[0].reason or "unknown operator" in offenders[0].reason


@pytest.mark.asyncio
async def test_scanner_reports_multiple_offenders(db: Database) -> None:
    await _insert_subscription(db, name="ok", arg_filters={"from": "0x1"})
    await _insert_subscription(db, name="bad1", arg_filters={"x": None})
    await _insert_subscription(db, name="bad2", arg_filters={"y_ne": "z"})
    offenders = await scan_database(db)
    names = {o.name for o in offenders}
    assert names == {"bad1", "bad2"}


def test_main_exits_zero_when_clean(monkeypatch, capsys) -> None:
    """`main()` returns 0 and prints a one-liner when there are no offenders."""
    from scripts import validate_arg_filters as mod

    async def _empty_scan(_db: Database):
        return []

    async def _fake_connect(self):
        return None

    async def _fake_disconnect(self):
        return None

    monkeypatch.setattr(mod, "scan_database", _empty_scan)
    monkeypatch.setattr(Database, "connect", _fake_connect)
    monkeypatch.setattr(Database, "disconnect", _fake_disconnect)

    rc = mod.main(["--database-url", "sqlite+aiosqlite:///:memory:"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "no offenders" in captured.out.lower() or "0 offenders" in captured.out.lower()


def test_main_exits_one_when_offenders(monkeypatch, capsys) -> None:
    from scripts import validate_arg_filters as mod

    async def _scan(_db: Database):
        return [mod.Offender(subscription_id="abc", name="bad-row", reason="bad value shape")]

    async def _fake_connect(self):
        return None

    async def _fake_disconnect(self):
        return None

    monkeypatch.setattr(mod, "scan_database", _scan)
    monkeypatch.setattr(Database, "connect", _fake_connect)
    monkeypatch.setattr(Database, "disconnect", _fake_disconnect)

    rc = mod.main(["--database-url", "sqlite+aiosqlite:///:memory:"])
    assert rc == 1
    captured = capsys.readouterr()
    assert "bad-row" in captured.out
    assert "bad value shape" in captured.out
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_validate_arg_filters_script.py -v`
Expected: 6 FAILs with `ModuleNotFoundError: No module named 'scripts.validate_arg_filters'`.

- [ ] **Step 3: Create the package marker if absent**

```python
# scripts/__init__.py
# (empty marker — makes `scripts` an importable package for tests)
```

(If `scripts/` already exists as a package from M1, skip this step. `ls scripts/__init__.py` confirms.)

- [ ] **Step 4: Implement the scanner**

```python
# scripts/validate_arg_filters.py
"""One-shot operator script: scan `subscriptions.arg_filters` rows for any
value shape that the M2-tightened API schema would reject.

Usage:
    python -m scripts.validate_arg_filters --database-url <url>

Exit 0 when no offenders; 1 when offenders are printed.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select

from apps.web.schemas import ArgFilterValue
from core.config.db import Database
from core.config.models import Subscription
from core.matcher.filters import FilterError, validate as _validate_filter_keys

_VALUE_SHAPE = TypeAdapter(dict[str, ArgFilterValue])


@dataclass(frozen=True)
class Offender:
    subscription_id: str
    name: str
    reason: str


async def scan_database(db: Database) -> list[Offender]:
    """Iterate every Subscription row and check `arg_filters` against the
    M2 schema. Returns a list of `Offender` records, one per failing row."""
    offenders: list[Offender] = []
    async with db.session() as s:
        result = await s.execute(select(Subscription))
        for row in result.scalars().all():
            try:
                _VALUE_SHAPE.validate_python(row.arg_filters)
                _validate_filter_keys(row.arg_filters)
            except ValidationError as exc:
                offenders.append(
                    Offender(
                        subscription_id=row.id,
                        name=row.name,
                        reason=f"value shape: {exc.errors()[0]['msg']}",
                    )
                )
            except FilterError as exc:
                offenders.append(
                    Offender(
                        subscription_id=row.id,
                        name=row.name,
                        reason=f"operator grammar: {exc}",
                    )
                )
    return offenders


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Scan subscriptions.arg_filters for M2-incompatible rows."
    )
    parser.add_argument(
        "--database-url",
        required=True,
        help="SQLAlchemy URL (e.g. postgresql+asyncpg://... or sqlite+aiosqlite:///path).",
    )
    args = parser.parse_args(argv)

    async def _run() -> int:
        db = Database(args.database_url)
        await db.connect()
        try:
            offenders = await scan_database(db)
        finally:
            await db.disconnect()

        if not offenders:
            print("✔ no offenders — all arg_filters rows pass the M2 schema")
            return 0

        print(f"✗ {len(offenders)} offender(s):")
        for o in offenders:
            print(f"  - id={o.subscription_id} name={o.name!r} reason={o.reason}")
        return 1

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Re-run the tests**

Run: `pytest tests/unit/test_validate_arg_filters_script.py -v`
Expected: all 6 PASS.

- [ ] **Step 6: Smoke-run against a freshly-migrated SQLite DB**

The script expects a `subscriptions` table to exist. Against a bare in-memory DB you'd get `OperationalError: no such table: subscriptions` — that's the unmigrated-DB failure mode, not a bug. Smoke-test against a temp file with the schema applied:

```bash
python -c "
import asyncio
from core.config.db import Database
from core.config.models import Base
async def _run():
    d = Database('sqlite+aiosqlite:///./_smoke.sqlite')
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await d.disconnect()
asyncio.run(_run())
"
python -m scripts.validate_arg_filters \
    --database-url "sqlite+aiosqlite:///./_smoke.sqlite"
rm -f ./_smoke.sqlite
```

Expected: prints `✔ no offenders` (or `no offenders`) and exits 0. If the script crashes with `no such table: subscriptions`, the schema-creation block above didn't run — re-check the working directory.

- [ ] **Step 7: Lint + typecheck**

Run: `make lint typecheck`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add scripts/__init__.py scripts/validate_arg_filters.py \
        tests/unit/test_validate_arg_filters_script.py
git commit -m "feat(scripts): offender-scanner for arg_filters M2 schema"
```

### Task 9.4: ERC-20 E2E fixture — compile + deploy a minimal token

The fixture compiles a 30-line Solidity ERC-20 with `py-solc-x`, deploys it to Anvil (reusing M1's `anvil` fixture), mints the full supply to `anvil.accounts[0]`, and yields a small handle with `address`, `abi`, and `deployer_pk`. Compile happens once per session; deploy happens once per test invocation.

**Files:**
- Modify: `pyproject.toml` — add `py-solc-x>=2.0,<3` to `[project.optional-dependencies].dev`.
- Modify: `tests/e2e/conftest.py` — append the new fixture and helpers.

- [ ] **Step 1: Add `py-solc-x` to dev deps**

In `pyproject.toml` under `[project.optional-dependencies]`, the `dev` array gets one new entry:

```toml
# pyproject.toml — ADD inside [project.optional-dependencies].dev
"py-solc-x>=2.0,<3",
```

Re-resolve and install:

```bash
pip install -e ".[dev]"
```

- [ ] **Step 2: Append the ERC-20 fixture to `tests/e2e/conftest.py`**

Add at the bottom of the existing M1 conftest (do NOT overwrite the M1 `anvil` / `webhook_receiver` fixtures; APPEND):

```python
# tests/e2e/conftest.py  --  ADD below the existing M1 fixtures
# (M1 conftest already imports `dataclass` at module top — reuse it; do NOT
# re-import.)

from functools import lru_cache

from eth_account import Account
from web3 import AsyncHTTPProvider, AsyncWeb3


# Minimal mintable ERC-20 — compiles to ~700 bytes of runtime bytecode.
# Public Transfer(address,address,uint256) signature is the one
# `Erc20TransferParser` (chunk 3) decodes.
_ERC20_SOURCE = """
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;
contract MiniToken {
    mapping(address => uint256) public balanceOf;
    event Transfer(address indexed from, address indexed to, uint256 value);
    constructor(uint256 supply) {
        balanceOf[msg.sender] = supply;
        emit Transfer(address(0), msg.sender, supply);
    }
    function transfer(address to, uint256 amount) external returns (bool) {
        require(balanceOf[msg.sender] >= amount, "low balance");
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        emit Transfer(msg.sender, to, amount);
        return true;
    }
}
"""

_ERC20_INITIAL_SUPPLY = 10 ** 24  # 1,000,000 tokens (18 decimals not enforced here)
_SOLC_VERSION = "0.8.20"


@dataclass
class Erc20Handle:
    address: str
    abi: list[dict]
    deployer_pk: str
    deployer_address: str


def _ensure_solc_installed() -> None:
    """Make sure solc `_SOLC_VERSION` is on disk. Calls `pytest.skip` from
    the FIXTURE (not from inside `lru_cache`) if the install attempt fails
    — typically because the host can't reach `binaries.soliditylang.org`.

    `install_solc` is a no-op when the version is already installed (it
    checks `get_installed_solc_versions()` internally), so calling it once
    per test session is cheap on warm hosts."""
    import solcx  # type: ignore[import-untyped]

    if _SOLC_VERSION in {str(v) for v in solcx.get_installed_solc_versions()}:
        return
    try:
        solcx.install_solc(_SOLC_VERSION)
    except solcx.exceptions.SolcInstallationError:
        pytest.skip(
            f"solc {_SOLC_VERSION} not installable (offline?); ERC-20 E2E "
            "requires network access to binaries.soliditylang.org"
        )


@lru_cache(maxsize=1)
def _compile_erc20() -> tuple[str, list[dict]]:
    """Compile the inline Solidity source ONCE per test session. Caches the
    (bytecode, abi) tuple in-process. Assumes `_ensure_solc_installed()`
    has already been called from the fixture."""
    import solcx  # type: ignore[import-untyped]

    out = solcx.compile_source(
        _ERC20_SOURCE,
        output_values=["abi", "bin"],
        solc_version=_SOLC_VERSION,
    )
    # `out` keys look like '<stdin>:MiniToken'.
    _, artifact = next(iter(out.items()))
    return artifact["bin"], artifact["abi"]


async def _deploy_erc20(anvil: AnvilHandle) -> Erc20Handle:
    bytecode, abi = _compile_erc20()
    deployer_pk = anvil.private_keys[0]
    deployer = Account.from_key(deployer_pk).address

    w3 = AsyncWeb3(AsyncHTTPProvider(anvil.rpc_url))
    try:
        Token = w3.eth.contract(abi=abi, bytecode=bytecode)
        # NOTE: `AsyncContractConstructor.build_transaction` is `async def`
        # in modern web3.py — it MUST be awaited. Same applies to
        # `contract.functions.<fn>(...).build_transaction(...)`.
        nonce = await w3.eth.get_transaction_count(deployer, "pending")
        tx = await Token.constructor(_ERC20_INITIAL_SUPPLY).build_transaction({
            "from": deployer,
            "nonce": nonce,
            "gas": 2_000_000,
            "gasPrice": await w3.eth.gas_price,
            "chainId": anvil.chain_id,
        })
        signed = Account.sign_transaction(tx, deployer_pk)
        h = await w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = await w3.eth.wait_for_transaction_receipt(h, timeout=10.0)
        assert receipt.status == 1, f"ERC-20 deploy failed: {receipt}"
        return Erc20Handle(
            address=receipt.contractAddress,
            abi=abi,
            deployer_pk=deployer_pk,
            deployer_address=deployer,
        )
    finally:
        await w3.provider.disconnect()


@pytest_asyncio.fixture
async def erc20_token(anvil: AnvilHandle) -> Erc20Handle:
    """Deploy a minimal ERC-20 to the running Anvil node. Returns the deployed
    address, the ABI, and the deployer credentials (the deployer holds the
    initial supply and can `transfer` to anyone).

    Solc install (if needed) happens here — outside `lru_cache` — so that
    `pytest.skip` cleanly skips the test instead of poisoning a cache slot."""
    _ensure_solc_installed()
    return await _deploy_erc20(anvil)
```

- [ ] **Step 3: Smoke-test the fixture**

A quick sanity check that the fixture deploys without errors. This is a one-off invocation; don't commit a placeholder test for it — the ERC-20 E2E test in Task 9.5 exercises the fixture end-to-end.

```bash
python -m pytest tests/e2e/conftest.py --collect-only -q
```

Expected: no import errors. (Anvil/solcx aren't invoked at collect time.)

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml tests/e2e/conftest.py
git commit -m "test(e2e): erc20_token fixture compiles MiniToken via py-solc-x"
```

### Task 9.5: ERC-20 E2E — full chain to webhook

Mirrors M1 chunk 11 Task 11.2's structure. Drives the API to create chain + ERC-20-scoped subscription + webhook channel, runs the worker in-process, calls `transfer()` on the deployed token N times, then asserts N payloads arrived with `kind="token_transfer"`.

**Files:**
- Create: `tests/e2e/test_evm_erc20_e2e.py`

- [ ] **Step 1: Write the test**

```python
# tests/e2e/test_evm_erc20_e2e.py
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from eth_account import Account
from httpx import ASGITransport, AsyncClient
from web3 import AsyncHTTPProvider, AsyncWeb3

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import run_worker
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


TRANSFER_COUNT = 3
DELIVERY_TIMEOUT_S = 30.0


@pytest_asyncio.fixture
async def db_url(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'e2e_erc20.sqlite'}"


@pytest_asyncio.fixture
async def initialised_db(db_url: str) -> AsyncIterator[Database]:
    d = Database(db_url)
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def _send_erc20_transfer(
    w3: AsyncWeb3,
    *,
    token_address: str,
    token_abi: list[dict],
    sender_pk: str,
    to: str,
    amount: int,
    chain_id: int,
) -> str:
    """Sign + submit a single `transfer(to, amount)` call.

    Uses `"pending"` for the nonce so back-to-back transfers within one
    block window each get a fresh nonce (matches M1's helper). `build_transaction`
    on an `AsyncContract` function is `async def` in modern web3.py — must
    be awaited."""
    sender = Account.from_key(sender_pk).address
    contract = w3.eth.contract(address=token_address, abi=token_abi)
    nonce = await w3.eth.get_transaction_count(sender, "pending")
    tx = await contract.functions.transfer(to, amount).build_transaction({
        "from": sender,
        "nonce": nonce,
        "gas": 120_000,
        "gasPrice": await w3.eth.gas_price,
        "chainId": chain_id,
    })
    signed = Account.sign_transaction(tx, sender_pk)
    h = await w3.eth.send_raw_transaction(signed.raw_transaction)
    return h.hex()


async def test_erc20_transfer_anvil_to_webhook(
    anvil, webhook_receiver, erc20_token, initialised_db, db_url, redis_url,
) -> None:
    """Anvil + deployed ERC-20 → Worker → HttpChannel → in-process webhook.

    Asserts payload conforms to spec §8 with kind=token_transfer:
    contract address matches the deployed token, args.from/to/value present,
    value is a decimal string carrying the transferred amount.
    """
    settings = Settings(
        database={"url": db_url},
        redis={"url": redis_url},
    )

    # 1) Seed chain + channel + ERC-20-scoped subscription via the real API.
    bus_writer = RedisBus(url=redis_url)
    await bus_writer.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: initialised_db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test",
        ) as c:
            r = await c.post("/api/chains", json={
                "id": "anvil-local", "kind": "evm",
                "rpc_http": anvil.rpc_url, "rpc_ws": None,
                "confirmations": 1, "poll_interval_ms": 500,
                "enabled": True,
            })
            assert r.status_code == 201, r.text

            r = await c.post("/api/channels", json={
                "name": "e2e-erc20-hook", "type": "http",
                "config": {"url": webhook_receiver.url, "method": "POST"},
            })
            assert r.status_code == 201
            channel_id = r.json()["id"]

            r = await c.post("/api/subscriptions", json={
                "name": "erc20-on-minitoken",
                "chain_id": "anvil-local",
                "address": erc20_token.address.lower(),  # scope to the deployed token
                "abi_id": None,
                "match_kind": "token_transfer",
                "match_name": None,
                "arg_filters": {},
                "enabled": True,
            })
            assert r.status_code == 201, r.text
            sub_id = r.json()["id"]

            r = await c.post(f"/api/subscriptions/{sub_id}/channels",
                             json={"channel_id": channel_id})
            assert r.status_code == 204
    finally:
        await bus_writer.disconnect()

    # 2) Start the worker.
    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(settings, stop_event))
    await asyncio.sleep(1.0)

    # 3) Submit N ERC-20 transfers.
    w3 = AsyncWeb3(AsyncHTTPProvider(anvil.rpc_url))
    try:
        recipient = anvil.accounts[1]
        submitted_hashes: list[str] = []
        for i in range(TRANSFER_COUNT):
            h = await _send_erc20_transfer(
                w3,
                token_address=erc20_token.address,
                token_abi=erc20_token.abi,
                sender_pk=erc20_token.deployer_pk,
                to=recipient,
                amount=10 ** 18 * (i + 1),  # 1, 2, 3 tokens (assuming 18 decimals)
                chain_id=anvil.chain_id,
            )
            submitted_hashes.append(h.lower().removeprefix("0x"))

        # 4) Wait for the receiver to collect N payloads.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DELIVERY_TIMEOUT_S
        timed_out = False
        while len(webhook_receiver.received) < TRANSFER_COUNT:
            if loop.time() > deadline:
                timed_out = True
                break
            await asyncio.sleep(0.5)
    finally:
        await w3.provider.disconnect()

    # 5) Stop the worker.
    stop_event.set()
    try:
        await asyncio.wait_for(worker_task, timeout=10.0)
    except asyncio.TimeoutError:
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(worker_task, timeout=3.0)

    if timed_out:
        pytest.fail(
            f"only {len(webhook_receiver.received)}/{TRANSFER_COUNT} ERC-20 "
            f"payloads received within {DELIVERY_TIMEOUT_S}s"
        )

    # 6) Assert payload shape per spec §8 with kind=token_transfer.
    received_tx_hashes = {
        p["event"]["tx_hash"].lower().removeprefix("0x")
        for p in webhook_receiver.received
    }
    for h in submitted_hashes:
        assert h in received_tx_hashes, f"missing tx {h} in {received_tx_hashes}"

    sample = webhook_receiver.received[0]
    assert sample["subscription_id"] == sub_id
    assert sample["subscription_name"] == "erc20-on-minitoken"
    assert sample["chain_id"] == "anvil-local"
    assert "delivery_id" in sample
    assert "delivered_at" in sample

    ev = sample["event"]
    assert ev["kind"] == "token_transfer"
    assert ev["address"].lower() == erc20_token.address.lower()
    assert isinstance(ev["block_number"], int) and ev["block_number"] >= 1
    assert ev["block_hash"].startswith("0x")
    assert ev["tx_hash"].startswith("0x")
    assert "from" in ev["args"] and "to" in ev["args"] and "value" in ev["args"]
    assert isinstance(ev["args"]["value"], str)
    assert int(ev["args"]["value"]) > 0
    # The deployer is the only sender — every Transfer's `from` should match.
    assert ev["args"]["from"].lower() == erc20_token.deployer_address.lower()
```

- [ ] **Step 2: Run the test**

```bash
make test-e2e
```

Or directly: `pytest tests/e2e/test_evm_erc20_e2e.py -v -m e2e`

Expected: 1 PASS within ~30 s. First run downloads solc 0.8.20 (~3 s extra). If the test SKIPs with "anvil not installed" or "solc not installable", install Foundry / unblock network and re-run.

- [ ] **Step 3: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: green.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/test_evm_erc20_e2e.py
git commit -m "test(e2e): erc-20 transfer anvil → worker → webhook end-to-end"
```

### Task 9.6: Close-out — tag `m2-evm-complete`

- [ ] **Step 1: Run the full unit + integration suite**

```bash
make test
```
Expected: every chunk 1–9 unit and integration test passes.

- [ ] **Step 2: Run the full E2E suite**

```bash
make test-e2e
```
Expected: both `test_native_transfer_e2e.py` (from M1) and the new `test_evm_erc20_e2e.py` PASS. Total run time ≈ 1 min on a warm host.

- [ ] **Step 3: Tag the EVM-complete milestone**

```bash
git tag m2-evm-complete
git tag -l m2-evm-complete   # verify
```

The `m2-evm-complete` tag marks the end of the EVM segment of M2: ERC-20 / event / call parsers, MQ + WS channels, hardened arg_filters schema, and an end-to-end Anvil → webhook ERC-20 test. The Solana segment (chunks 10–14) starts on top of this commit.

`m2-evm-complete` is informational — chunks 10–14 don't read it. The tag exists to let humans navigate "`git checkout m2-evm-complete` and the EVM segment is fully working" without sifting through the chunk history.

---
## Chunk 10: `SolanaAdapter` + `chains.commitment` migration

First chunk of the Solana segment. Lays the foundation the next four chunks build on:

1. **Block-type plumbing** — extend `core/chains/types.py` with `SolanaBlock`, `SolanaTransaction`, `SolanaTokenBalance`, `SolanaInstruction`, plus the `SolanaChainAdapter` Protocol in `core/chains/adapter.py`. The EVM `ChainAdapter` Protocol stays untouched (spec §4.6 names the divergence explicitly).
2. **Schema migration** — add a nullable `commitment: str` column to `chains` via alembic `0002_solana_commitment.py`. Solana rows must supply a `commitment`; EVM rows leave it `NULL`.
3. **API kind-validation** — `ChainCreate` gains a `commitment: Literal["confirmed","finalized"] | None` field plus a `@model_validator(mode="after")` that enforces "EVM ↔ confirmations" / "Solana ↔ commitment". Drift in either direction → 422.
4. **Snapshot plumbing** — `SnapshotChain` carries `commitment`. `load_snapshot` reads it through.
5. **`SolanaAdapter` itself** — `core/chains/solana.py`. `solders` for typed RPC request/response, `httpx.AsyncClient` for HTTP transport. Three behavior gates: (a) `getBlock(slot)` returning `null` (HTTP 200) → missed slot, advance checkpoint; (b) 4xx/5xx or transport error → wrapped in `retry_with_backoff(..., max_attempts=3, base_delay=1.0)` from `core/notifier/retry.py` (same helper EVM channels already use); (c) `subscribe_heads` polls `getSlot` at the configured commitment.
6. **Worker factory branch** — `apps/worker/main.py:_default_adapter_factory` branches on `cfg.kind`. Solana returns a `SolanaAdapter`; EVM is unchanged. Hard-error path (M1's `NotImplementedError`) is removed.
7. **Integration test** — `tests/integration/test_solana_adapter.py` against a session-scoped `solana-test-validator` (function-scoped would pay the 5-10 s cold start per test; the validator's state across tests is acceptable because each IT only reads). Validator install/skip-on-missing pattern matches M1's anvil fixture.

The new pipeline-side glue (`SolanaParserPipeline`, `SolanaParser` protocol, ChainRunner pipeline selection) is chunk 11. Chunk 10 ends with the adapter callable from a unit test mocking `httpx.AsyncClient` and from an IT against a real validator. ChainRunner does NOT yet branch on `chain.kind` — that's chunk 11's job.

**Modified files this chunk:**
- `core/chains/types.py` — append `SolanaBlock`, `SolanaTransaction`, `SolanaTokenBalance`, `SolanaInstruction`. Existing EVM dataclasses unchanged.
- `core/chains/adapter.py` — append `SolanaChainAdapter` Protocol. Existing `ChainAdapter` unchanged.
- `core/chains/solana.py` — **new**: `SolanaAdapter` class.
- `core/config/models.py:73` — add `commitment: Mapped[str | None]` to `Chain`.
- `migrations/versions/0002_solana_commitment.py` — **new**.
- `core/config/snapshot.py` — add `commitment: str | None` to `SnapshotChain`; pass through in `load_snapshot`.
- `apps/web/schemas.py` — `ChainCreate.commitment` + `model_validator`; `ChainOut.commitment` (response).
- `apps/web/routers/chains.py:25-33` — pass `commitment` through to `ChainRepo.create`.
- `core/config/repositories.py:28-44` — `ChainRepo.create(commitment=...)` kwarg.
- `apps/worker/main.py:24-36` — branch `_default_adapter_factory` on `cfg.kind`.
- `pyproject.toml` — add `solders>=0.21,<0.30`, `httpx>=0.27,<0.29` (already pinned in M1; verify), and `solana>=0.34` is NOT added.
- `tests/integration/test_solana_adapter.py` — **new**.
- `tests/integration/conftest.py` — append `solana_validator` session-scoped fixture (file already exists from M1 with the `db` fixture; add the new fixture below it).
- `tests/unit/test_solana_types.py` — **new**.
- `tests/unit/test_solana_adapter.py` — **new**: httpx-mocked unit tests.
- `tests/unit/test_solana_adapter_protocol.py` — **new**: structural Protocol-conformance check for `SolanaChainAdapter`.
- `tests/unit/test_worker_adapter_factory.py` — **new**: covers EVM/Solana/unknown-kind branches.
- `tests/unit/test_chain_create_kind_validation.py` — **new**: Pydantic cross-field test.
- `tests/unit/test_snapshot_chain_commitment.py` — **new**: `SnapshotChain.commitment` round-trips from DB.
- `tests/integration/test_alembic_solana_migration.py` — **new**: migration apply / downgrade tests.

**Spec §8 row 10 reconciliation:** the spec lists `core/settings.py` as a touched file for this milestone row. It is **not** touched in this chunk. `commitment` is per-chain DB state (read from `chains.commitment`), not a global setting; nothing needs to land in `core/settings.py`. If you're cross-referencing the spec, this is intentional — do not invent a settings change.

**Out of scope this chunk:**
- `SolanaParser` protocol and parsers (`SolNativeTransferParser`, `SplTransferParser`, `AnchorIdlEventParser`). Chunks 11–13.
- `SolanaParserPipeline`. Chunk 11.
- `ChainRunner` branching. Chunk 11 (because runner wiring is meaningless without the pipeline class to wire to).
- WS-based head subscription for Solana. Spec §4.6 specifies HTTP polling at commitment; native Solana WS for slot updates is an M3 ergonomic improvement.
- Solana E2E. Chunk 14.
- Touch of `core/notifier/retry.py`. The retry helper is M1-stable; the adapter imports it.

**Library version pins (record in `pyproject.toml`):**
- `solders>=0.21,<0.30` — tested against 0.21–0.24. Major-bump risk noted in spec §9.2.
- `httpx>=0.27,<0.29` — already pinned in M1 for the webhook channel.

**Naming pin (spec §4.6):** `ChainAdapter` (existing) is the EVM Protocol; `SolanaChainAdapter` (new) is the Solana Protocol. The concrete class for Solana is `SolanaAdapter` (matches `EvmAdapter` naming on the EVM side). Do NOT collapse these — the divergent return type of `fetch_block` (`Block` vs `SolanaBlock`) makes a single Protocol impossible to type-check. **`SolanaChainAdapter` is defined exactly once, in Task 10.5 of this chunk; chunks 11–14 import it but never redefine.**

**Solders + httpx usage note:** The pattern is "build a typed request with `solders`, serialize to JSON, POST via `httpx`, parse the response with `solders`'s typed response class". Solders provides the request/response *types*; the transport is yours. Example for `getSlot`:

```python
from solders.rpc.requests import GetSlot
from solders.rpc.responses import GetSlotResp
from solders.rpc.config import RpcContextConfig
from solders.commitment_config import CommitmentLevel

req = GetSlot(RpcContextConfig(commitment=CommitmentLevel.Confirmed), id=1)
resp = await client.post(self._rpc_url, content=req.to_json(), headers={"content-type": "application/json"})
parsed = GetSlotResp.from_json(resp.text)
slot = parsed.value  # int
```

Same idiom for `getBlock` → `GetBlockResp`. The null-block case shows up as `parsed.value is None`.

### Task 10.1: Solana block types

**Files:**
- Modify: `core/chains/types.py` — append Solana dataclasses.
- Create: `tests/unit/test_solana_types.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_solana_types.py
from __future__ import annotations

import pytest

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)


def test_solana_token_balance_round_trip() -> None:
    tb = SolanaTokenBalance(
        account_index=2,
        mint="So11111111111111111111111111111111111111112",
        owner="11111111111111111111111111111111",
        amount=1_000_000,
        decimals=6,
    )
    assert tb.mint.endswith("112")
    assert tb.amount == 1_000_000


def test_solana_instruction_carries_program_and_accounts() -> None:
    ix = SolanaInstruction(
        program_id="11111111111111111111111111111111",
        accounts=["A", "B"],
        data_b58="3Bxs4h",
        stack_depth=1,
    )
    assert ix.program_id == "11111111111111111111111111111111"
    assert ix.accounts == ["A", "B"]


def test_solana_transaction_holds_balances_and_logs() -> None:
    tx = SolanaTransaction(
        signature="5xyz",
        slot=100,
        success=True,
        fee=5000,
        account_keys=["A", "B"],
        pre_balances=[10**9, 0],
        post_balances=[10**9 - 5000, 0],
        pre_token_balances=[],
        post_token_balances=[],
        log_messages=["Program X invoke [1]"],
        instructions=[],
    )
    assert tx.success is True
    assert tx.fee == 5000
    assert tx.log_messages[0].startswith("Program X")


def test_solana_block_top_level_shape() -> None:
    block = SolanaBlock(
        slot=100,
        block_hash="hash100",
        parent_slot=99,
        block_time=1_700_000_000,
        transactions=[],
    )
    assert block.slot == 100
    assert block.parent_slot == 99


def test_solana_block_is_frozen() -> None:
    block = SolanaBlock(slot=1, block_hash="h", parent_slot=0, block_time=None, transactions=[])
    with pytest.raises((AttributeError, TypeError)):
        block.slot = 2  # type: ignore[misc]
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_solana_types.py -v`
Expected: 5 FAILs with `ImportError: cannot import name 'SolanaBlock' from 'core.chains.types'`.

- [ ] **Step 3: Implement the types**

Append to `core/chains/types.py`:

```python
# core/chains/types.py  --  APPEND below the existing EVM dataclasses


@dataclass(frozen=True)
class SolanaTokenBalance:
    """Snapshot of one (account, mint) SPL balance at a point in a tx.

    `account_index` is the position in the tx's `account_keys` array — paired
    with `pre_token_balances` / `post_token_balances`, lets parsers determine
    transfer amounts without re-walking instructions.
    """
    account_index: int
    mint: str
    owner: str | None
    amount: int  # raw base-units of the SPL token (apply `decimals` for human display)
    decimals: int


@dataclass(frozen=True)
class SolanaInstruction:
    """A single instruction inside a Solana transaction.

    `stack_depth=1` means top-level; `>1` means an inner CPI (cross-program
    invocation). Parsers that only care about top-level system transfers
    filter on `stack_depth == 1`.
    """
    program_id: str
    accounts: list[str]
    data_b58: str  # base58-encoded instruction data (Solana's canonical form)
    stack_depth: int


@dataclass(frozen=True)
class SolanaTransaction:
    signature: str
    slot: int
    success: bool  # meta.err is None
    fee: int  # lamports
    account_keys: list[str]
    pre_balances: list[int]   # lamport balances pre-tx, indexed by account_keys
    post_balances: list[int]  # …post-tx
    pre_token_balances: list[SolanaTokenBalance]
    post_token_balances: list[SolanaTokenBalance]
    log_messages: list[str]
    instructions: list[SolanaInstruction]


@dataclass(frozen=True)
class SolanaBlock:
    slot: int
    block_hash: str
    parent_slot: int
    block_time: int | None  # unix seconds; can be None on fresh validator
    transactions: list[SolanaTransaction]
```

- [ ] **Step 4: Re-run the tests**

Run: `pytest tests/unit/test_solana_types.py -v`
Expected: 5 PASS.

- [ ] **Step 5: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean. `dataclass(frozen=True)` already covered by M1 EVM types.

- [ ] **Step 6: Commit**

```bash
git add core/chains/types.py tests/unit/test_solana_types.py
git commit -m "feat(chains): SolanaBlock + SolanaTransaction dataclasses"
```

### Task 10.2: `chains.commitment` migration + ORM column

The DB column is nullable so existing EVM rows survive unchanged. Solana rows MUST set `commitment` at API time (enforced in Task 10.3). The migration script is hand-written (not autogenerated) to keep it readable.

**Files:**
- Modify: `core/config/models.py:73-75` — append `commitment` to `Chain`.
- Modify: `core/config/repositories.py:28-44` — accept `commitment` kwarg in `ChainRepo.create`.
- Create: `migrations/versions/0002_solana_commitment.py`.
- Create: `tests/integration/test_alembic_solana_migration.py`.

- [ ] **Step 1: Write the failing migration test**

```python
# tests/integration/test_alembic_solana_migration.py
"""Apply 0002_solana_commitment against a fresh SQLite DB and check the
column exists; then downgrade and check it's gone.

This is an IT (not a unit test) because alembic touches a real connection.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command as alembic_command
from alembic.config import Config
from sqlalchemy import create_engine, text


@pytest.fixture
def alembic_cfg(tmp_path: Path) -> Config:
    cfg = Config("alembic.ini")
    cfg.set_main_option(
        "sqlalchemy.url", f"sqlite:///{tmp_path / 'mig.sqlite'}"
    )
    return cfg


def test_upgrade_0002_adds_commitment_column(alembic_cfg: Config) -> None:
    alembic_command.upgrade(alembic_cfg, "0002")
    url = alembic_cfg.get_main_option("sqlalchemy.url")
    assert url is not None
    engine = create_engine(url)
    with engine.connect() as conn:
        cols = conn.execute(
            text("PRAGMA table_info(chains)")
        ).fetchall()
    names = [r[1] for r in cols]
    assert "commitment" in names, f"commitment not in {names}"


def test_downgrade_0002_removes_commitment_column(alembic_cfg: Config) -> None:
    alembic_command.upgrade(alembic_cfg, "0002")
    alembic_command.downgrade(alembic_cfg, "0001")
    url = alembic_cfg.get_main_option("sqlalchemy.url")
    assert url is not None
    engine = create_engine(url)
    with engine.connect() as conn:
        cols = conn.execute(
            text("PRAGMA table_info(chains)")
        ).fetchall()
    names = [r[1] for r in cols]
    assert "commitment" not in names
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/integration/test_alembic_solana_migration.py -v`
Expected: both tests fail with `FAILED: ...Can't locate revision identified by '0002'` (the file doesn't exist yet).

- [ ] **Step 3: Create the migration file**

```python
# migrations/versions/0002_solana_commitment.py
"""solana commitment

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-26 09:00:00.000000
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.add_column(
            sa.Column("commitment", sa.String(length=16), nullable=True)
        )


def downgrade() -> None:
    with op.batch_alter_table("chains") as batch:
        batch.drop_column("commitment")
```

The `batch_alter_table` wrapper is required for SQLite, which does not support `ALTER TABLE ... DROP COLUMN` directly; alembic emulates it by table-rename + recopy. On Postgres the wrapper is a no-op.

- [ ] **Step 4: Add `commitment` to the ORM model**

`core/config/models.py:73`, just after the existing `confirmations` column:

```python
    confirmations: Mapped[int] = mapped_column(Integer, nullable=False, default=12)
    commitment: Mapped[str | None] = mapped_column(String(16), nullable=True)
    poll_interval_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=3000)
```

- [ ] **Step 5: Add `commitment` to `ChainRepo.create`**

`core/config/repositories.py:28-44`:

```python
    async def create(
        self,
        *,
        id: str,
        kind: ChainKind,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
        commitment: str | None,
        poll_interval_ms: int,
        enabled: bool,
    ) -> Chain:
        c = Chain(
            id=id, kind=kind, rpc_http=rpc_http, rpc_ws=rpc_ws,
            confirmations=confirmations, commitment=commitment,
            poll_interval_ms=poll_interval_ms, enabled=enabled,
        )
        self.s.add(c)
        await self.s.flush()
        return c
```

(`commitment` is required-keyword. Callers pass `commitment=None` for EVM rows; the API layer enforces non-null for Solana — Task 10.3.)

- [ ] **Step 6: Re-run the migration tests**

Run: `pytest tests/integration/test_alembic_solana_migration.py -v`
Expected: both PASS.

- [ ] **Step 7: Re-run the full unit + IT suite**

Run: `pytest tests/unit tests/integration -v`
Expected: green. The M1 chain-router tests now pass `commitment=None` through the API → repo path; if they break, the API hasn't been updated yet (Task 10.3 fixes that). Run them last.

Run: `pytest tests/unit/test_web_chains.py -v`
Expected: This will likely FAIL right now because `ChainRepo.create` requires `commitment=` kwarg but `apps/web/routers/chains.py` doesn't pass it yet. That's expected — Task 10.3 fixes the router. Verify the failure mode is exactly "TypeError: create() missing 1 required keyword-only argument: 'commitment'" and proceed; do NOT skip ahead and fix the router from here.

- [ ] **Step 8: Commit**

```bash
git add migrations/versions/0002_solana_commitment.py \
        core/config/models.py core/config/repositories.py \
        tests/integration/test_alembic_solana_migration.py
git commit -m "feat(db): chains.commitment column + 0002 migration"
```

(Web tests stay red for one commit; Task 10.3's commit makes them green again. This split is deliberate — keeps the migration commit reviewable on its own. **If `make test` is invoked between this commit and the Task 10.3 commit, the `TypeError: create() missing 1 required keyword-only argument: 'commitment'` failures in `tests/unit/test_web_chains.py` are expected; do NOT roll back. The very next task fixes them.**)

### Task 10.3: `ChainCreate` kind-validation

EVM chains supply `confirmations`; Solana chains supply `commitment`. The cross-field rule is enforced by a Pydantic v2 `@model_validator(mode="after")`. The validator runs AFTER per-field validators, so `kind` and `confirmations` / `commitment` are already typed by the time we check their joint shape.

**Files:**
- Modify: `apps/web/schemas.py:11-23` — add import + `commitment` field + validator.
- Modify: `apps/web/schemas.py:26-35` — add `commitment` to `ChainOut`.
- Modify: `apps/web/routers/chains.py:25-33` — pass `commitment` through.
- Create: `tests/unit/test_chain_create_kind_validation.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_chain_create_kind_validation.py
from __future__ import annotations

import pytest
from pydantic import ValidationError

from apps.web.schemas import ChainCreate


def _evm_base(**overrides):
    base = dict(
        id="eth-mainnet", kind="evm",
        rpc_http="http://x", rpc_ws=None,
        confirmations=12, commitment=None,
        poll_interval_ms=3000, enabled=True,
    )
    base.update(overrides)
    return base


def _sol_base(**overrides):
    base = dict(
        id="sol-devnet", kind="solana",
        rpc_http="http://x", rpc_ws=None,
        confirmations=0, commitment="confirmed",
        poll_interval_ms=400, enabled=True,
    )
    base.update(overrides)
    return base


# -- happy paths -------------------------------------------------------------

def test_evm_chain_with_confirmations_only() -> None:
    c = ChainCreate(**_evm_base())
    assert c.kind == "evm" and c.commitment is None and c.confirmations == 12


def test_solana_chain_with_commitment_only() -> None:
    c = ChainCreate(**_sol_base())
    assert c.kind == "solana" and c.commitment == "confirmed" and c.confirmations == 0


def test_solana_chain_finalized_is_accepted() -> None:
    c = ChainCreate(**_sol_base(commitment="finalized"))
    assert c.commitment == "finalized"


# -- cross-field rejections --------------------------------------------------

def test_evm_chain_with_commitment_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        ChainCreate(**_evm_base(commitment="confirmed"))
    assert "commitment" in str(exc.value).lower()


def test_solana_chain_without_commitment_is_rejected() -> None:
    with pytest.raises(ValidationError) as exc:
        ChainCreate(**_sol_base(commitment=None))
    assert "commitment" in str(exc.value).lower()


def test_solana_chain_with_invalid_commitment_value_is_rejected() -> None:
    with pytest.raises(ValidationError):
        ChainCreate(**_sol_base(commitment="processed"))


# -- legacy compatibility ----------------------------------------------------

def test_evm_chain_legacy_payload_without_commitment_is_accepted() -> None:
    """Legacy clients that don't send `commitment` for EVM still work."""
    payload = _evm_base()
    payload.pop("commitment")
    c = ChainCreate(**payload)
    assert c.commitment is None
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_chain_create_kind_validation.py -v`
Expected: many fail with `ValidationError` about the unrecognized `commitment` field (because `ChainCreate` doesn't have it yet), or with the cross-field rules not being enforced.

- [ ] **Step 3: Update `ChainCreate`**

`apps/web/schemas.py` (top imports + `ChainCreate` body):

```python
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# … (existing ArgFilterValue alias from chunk 9 stays here) …


class ChainCreate(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    kind: Literal["evm", "solana"]
    rpc_http: str = Field(min_length=1)
    rpc_ws: str | None = None
    confirmations: int = Field(ge=0, le=10_000)
    commitment: Literal["confirmed", "finalized"] | None = None
    poll_interval_ms: int = Field(ge=100, le=60_000)
    enabled: bool = True

    @model_validator(mode="after")
    def _check_kind_specific_fields(self) -> "ChainCreate":
        if self.kind == "evm" and self.commitment is not None:
            raise ValueError(
                "evm chains must not set 'commitment' (use 'confirmations' instead)"
            )
        if self.kind == "solana" and self.commitment is None:
            raise ValueError(
                "solana chains require 'commitment' ('confirmed' or 'finalized')"
            )
        return self
```

- [ ] **Step 4: Update `ChainOut`**

```python
class ChainOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    commitment: str | None
    poll_interval_ms: int
    enabled: bool
```

- [ ] **Step 5: Pass `commitment` through the chain router**

`apps/web/routers/chains.py:25-33` in `create_chain`:

```python
    row = await repo.create(
        id=payload.id,
        kind=ChainKind(payload.kind),
        rpc_http=payload.rpc_http,
        rpc_ws=payload.rpc_ws,
        confirmations=payload.confirmations,
        commitment=payload.commitment,
        poll_interval_ms=payload.poll_interval_ms,
        enabled=payload.enabled,
    )
```

- [ ] **Step 6: Re-run the tests**

Run: `pytest tests/unit/test_chain_create_kind_validation.py tests/unit/test_web_chains.py -v`
Expected: chain validation tests all PASS; the M1 chain-router tests pass again (the `TypeError` from Task 10.2 Step 7 is gone).

If `test_web_chains.py:test_create_chain_persists_and_publishes` sends the legacy EVM payload without `commitment`, that path is exercised by the new `test_evm_chain_legacy_payload_without_commitment_is_accepted` — both should pass.

- [ ] **Step 7: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add apps/web/schemas.py apps/web/routers/chains.py \
        tests/unit/test_chain_create_kind_validation.py
git commit -m "feat(api): ChainCreate kind-validation + commitment field"
```

### Task 10.4: Snapshot plumbing — `SnapshotChain.commitment`

The snapshot is what the worker reads. Adding `commitment` here is the bridge between the new DB column and the adapter factory in Task 10.7.

**Files:**
- Modify: `core/config/snapshot.py:37-45` — append `commitment` to `SnapshotChain`.
- Modify: `core/config/snapshot.py:72-82` — pass `commitment` through in `load_snapshot`.
- Modify: `tests/unit/test_snapshot.py` (if a Snapshot-builder test exists) OR — Create: `tests/unit/test_snapshot_chains_commitment.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_snapshot_chains_commitment.py
from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from core.config.db import Database
from core.config.models import Base, Chain, ChainKind
from core.config.snapshot import load_snapshot


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    d = Database("sqlite+aiosqlite:///:memory:")
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


@pytest.mark.asyncio
async def test_snapshot_carries_commitment_for_solana(db: Database) -> None:
    async with db.session() as s:
        s.add(Chain(
            id="sol-devnet", kind=ChainKind.solana,
            rpc_http="http://x", rpc_ws=None,
            confirmations=0, commitment="confirmed",
            poll_interval_ms=400, enabled=True,
        ))
        await s.commit()
    async with db.session() as s:
        snap = await load_snapshot(s)
    [sol] = snap.chains
    assert sol.id == "sol-devnet"
    assert sol.kind == "solana"
    assert sol.commitment == "confirmed"


@pytest.mark.asyncio
async def test_snapshot_commitment_is_none_for_evm(db: Database) -> None:
    async with db.session() as s:
        s.add(Chain(
            id="eth", kind=ChainKind.evm,
            rpc_http="http://x", rpc_ws=None,
            confirmations=12, commitment=None,
            poll_interval_ms=3000, enabled=True,
        ))
        await s.commit()
    async with db.session() as s:
        snap = await load_snapshot(s)
    [evm] = snap.chains
    assert evm.commitment is None
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_snapshot_chains_commitment.py -v`
Expected: 2 FAIL with `AttributeError: 'SnapshotChain' object has no attribute 'commitment'`.

- [ ] **Step 3: Extend `SnapshotChain`**

`core/config/snapshot.py:37`:

```python
@dataclass(frozen=True)
class SnapshotChain:
    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    commitment: str | None
    poll_interval_ms: int
```

- [ ] **Step 4: Pass `commitment` through `load_snapshot`**

`core/config/snapshot.py:72`:

```python
    snap_chains = [
        SnapshotChain(
            id=c.id,
            kind=c.kind.value,
            rpc_http=c.rpc_http,
            rpc_ws=c.rpc_ws,
            confirmations=c.confirmations,
            commitment=c.commitment,
            poll_interval_ms=c.poll_interval_ms,
        )
        for c in chains_rows
    ]
```

- [ ] **Step 5: Re-run the tests**

Run: `pytest tests/unit/test_snapshot_chains_commitment.py -v`
Expected: both PASS.

- [ ] **Step 6: Re-run the full snapshot test file**

Run: `pytest tests/unit/test_snapshot.py -v`
Expected: existing tests still pass — `SnapshotChain` gained a field but its callers use kwargs.

- [ ] **Step 7: Commit**

```bash
git add core/config/snapshot.py tests/unit/test_snapshot_chains_commitment.py
git commit -m "feat(config): SnapshotChain.commitment plumbed from DB"
```

### Task 10.5: `SolanaChainAdapter` Protocol

A typed shape the worker factory can return. Lives in `core/chains/adapter.py` alongside the EVM Protocol so both contracts are visible at a glance.

**Files:**
- Modify: `core/chains/adapter.py` — append `SolanaChainAdapter`.
- Create: `tests/unit/test_solana_adapter_protocol.py`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_solana_adapter_protocol.py
from __future__ import annotations

from collections.abc import AsyncIterator

from core.chains.adapter import SolanaChainAdapter
from core.chains.types import BlockHeader, SolanaBlock


class _StubAdapter:
    chain_id = "sol-devnet"
    commitment = "confirmed"

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_latest_slot(self) -> int: return 0
    async def fetch_block(self, slot: int) -> SolanaBlock | None: return None
    def subscribe_heads(self) -> AsyncIterator[BlockHeader]:
        async def _gen() -> AsyncIterator[BlockHeader]:
            if False:
                yield  # pragma: no cover
        return _gen()


def test_stub_conforms_to_solana_chain_adapter() -> None:
    assert isinstance(_StubAdapter(), SolanaChainAdapter)
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pytest tests/unit/test_solana_adapter_protocol.py -v`
Expected: FAIL with `ImportError: cannot import name 'SolanaChainAdapter' from 'core.chains.adapter'`.

- [ ] **Step 3: Add the Protocol**

`core/chains/adapter.py`, append (plus extend the existing `typing` and `core.chains.types` imports at the top of the file with `Literal` and `SolanaBlock` respectively):

```python
# add to the existing `from typing import ...` line: , Literal
# add to the existing `from core.chains.types import ...` line: , SolanaBlock


@runtime_checkable
class SolanaChainAdapter(Protocol):
    """Solana-side counterpart to ``ChainAdapter``. Diverges deliberately:
    - ``commitment`` (not ``confirmations``) carries the finality preference.
    - ``fetch_block`` returns ``SolanaBlock | None`` because Solana slots
      may be empty (missed-slot case — handled by caller as "advance one
      slot and continue", per spec §6).
    - ``get_latest_slot`` replaces ``get_latest_block_number``: Solana
      slots and blocks are NOT 1:1.
    - There is no ``fetch_logs`` — Solana programs emit logs inside the
      block's transactions; the parser pipeline reads them from
      ``SolanaTransaction.log_messages``.
    """

    chain_id: str
    commitment: Literal["confirmed", "finalized"]

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def get_latest_slot(self) -> int: ...
    async def fetch_block(self, slot: int) -> SolanaBlock | None: ...
    def subscribe_heads(self) -> AsyncIterator[BlockHeader]: ...
```

(The Solana adapter still yields the EVM `BlockHeader` from `subscribe_heads` — `BlockHeader` is intentionally chain-agnostic. The runner only needs `number` (which is the slot for Solana), `hash`, `parent_hash`, `timestamp`. The Solana adapter computes `hash`/`parent_hash` from the slot's `blockhash` and `parent_slot`'s `blockhash`.)

- [ ] **Step 4: Re-run the test**

Run: `pytest tests/unit/test_solana_adapter_protocol.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add core/chains/adapter.py tests/unit/test_solana_adapter_protocol.py
git commit -m "feat(chains): SolanaChainAdapter Protocol"
```

### Task 10.6: `SolanaAdapter` — unit tests with httpx mocking (Red)

Three behaviour gates from spec §6:
1. `getBlock(slot)` HTTP 200 with `result: null` → returns `None` (missed slot).
2. `getBlock(slot)` HTTP 4xx/5xx or transport error → retried by `core.notifier.retry.retry_with_backoff` (3 attempts, exponential); after max retries, the helper raises `RetryExhausted` whose `__cause__` is the original `httpx.HTTPStatusError`.
3. `getSlot` returns the integer at the configured commitment.

We mock `httpx.AsyncClient` via `httpx.MockTransport` so tests run without a validator. The integration test (Task 10.9) exercises the real RPC. Tests inject a no-op `sleep` into the adapter to skip the 1-second / 4-second retry waits.

**Files:**
- Create: `tests/unit/test_solana_adapter.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_solana_adapter.py
"""Unit tests for SolanaAdapter — httpx mocked, no real RPC.

Five contracts under test:
- subscribe-style getSlot returns int at configured commitment.
- getBlock(slot) returning JSON-RPC `result: null` -> Python `None` (missed slot).
- getBlock(slot) returning a full block -> populated `SolanaBlock`.
- getBlock(slot) returning persistent 5xx -> `RetryExhausted`, three attempts made.
- `meta.err` non-null on a tx -> `tx.success is False`.

The mocked handlers always dispatch on `body["method"]` so a single transport
can satisfy any call sequence the adapter makes (currently `connect()` opens
the client without an RPC ping, so the tests would still work without method
dispatch — but the dispatch idiom makes the tests robust to future ping
additions).
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from core.chains.solana import SolanaAdapter
from core.chains.types import SolanaBlock
from core.notifier.retry import RetryExhausted


async def _no_sleep(_: float) -> None:  # injected into the adapter to keep tests fast
    return None


def _ok_json(payload: dict[str, Any]) -> httpx.Response:
    return httpx.Response(200, content=json.dumps(payload).encode())


def _err_json(code: int) -> httpx.Response:
    return httpx.Response(code, content=b'{"error":"boom"}')


def _mock_transport(handler):
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_get_latest_slot_returns_int() -> None:
    calls: list[dict[str, Any]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content.decode())
        calls.append(body)
        assert body["method"] == "getSlot"
        return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": 12345})

    adapter = SolanaAdapter(
        chain_id="sol", rpc_url="http://x",
        commitment="confirmed", poll_interval_ms=400,
        transport=_mock_transport(handler),
        sleep=_no_sleep,
    )
    await adapter.connect()
    try:
        slot = await adapter.get_latest_slot()
    finally:
        await adapter.disconnect()
    assert slot == 12345
    assert calls[0]["method"] == "getSlot"


@pytest.mark.asyncio
async def test_fetch_block_missed_slot_returns_none() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content.decode())
        # Solana RPC returns `result: null` for missed slots, HTTP 200.
        # Handler dispatches on method so future `connect()`-time pings would still work.
        if body["method"] == "getSlot":
            return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": 1000})
        assert body["method"] == "getBlock"
        return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": None})

    adapter = SolanaAdapter(
        chain_id="sol", rpc_url="http://x",
        commitment="confirmed", poll_interval_ms=400,
        transport=_mock_transport(handler),
        sleep=_no_sleep,
    )
    await adapter.connect()
    try:
        result = await adapter.fetch_block(slot=999)
    finally:
        await adapter.disconnect()
    assert result is None


@pytest.mark.asyncio
async def test_fetch_block_decodes_full_block() -> None:
    """A non-null `getBlock` response with one tx becomes a populated SolanaBlock."""
    block_payload = {
        "blockhash": "BHASH",
        "previousBlockhash": "PHASH",
        "parentSlot": 99,
        "blockTime": 1_700_000_000,
        "transactions": [{
            "transaction": {
                "signatures": ["SIG1"],
                "message": {
                    "accountKeys": ["A", "B"],
                    "instructions": [{
                        "programIdIndex": 0,
                        "accounts": [0, 1],
                        "data": "3Bxs4h",
                        "stackHeight": 1,
                    }],
                },
            },
            "meta": {
                "err": None,
                "fee": 5000,
                "preBalances": [10**9, 0],
                "postBalances": [10**9 - 5000, 0],
                "preTokenBalances": [],
                "postTokenBalances": [],
                "logMessages": ["Program A invoke [1]"],
                "innerInstructions": [],
            },
        }],
    }

    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content.decode())
        if body["method"] == "getSlot":
            return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": 100})
        assert body["method"] == "getBlock"
        return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": block_payload})

    adapter = SolanaAdapter(
        chain_id="sol", rpc_url="http://x",
        commitment="confirmed", poll_interval_ms=400,
        transport=_mock_transport(handler),
        sleep=_no_sleep,
    )
    await adapter.connect()
    try:
        block = await adapter.fetch_block(slot=100)
    finally:
        await adapter.disconnect()
    assert isinstance(block, SolanaBlock)
    assert block.slot == 100
    assert block.parent_slot == 99
    assert block.block_hash == "BHASH"
    assert block.block_time == 1_700_000_000
    [tx] = block.transactions
    assert tx.signature == "SIG1"
    assert tx.success is True
    assert tx.fee == 5000
    assert tx.account_keys == ["A", "B"]
    assert tx.log_messages == ["Program A invoke [1]"]
    [ix] = tx.instructions
    assert ix.program_id == "A"
    assert ix.accounts == ["A", "B"]
    assert ix.stack_depth == 1


@pytest.mark.asyncio
async def test_fetch_block_5xx_exhausts_retries() -> None:
    """Persistent 5xx on `getBlock` -> `RetryExhausted` after 3 attempts;
    the original `httpx.HTTPStatusError` is preserved as `__cause__`."""
    call_count = 0

    def handler(req: httpx.Request) -> httpx.Response:
        nonlocal call_count
        body = json.loads(req.content.decode())
        if body["method"] == "getSlot":
            return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": 100})
        # getBlock always 5xx
        call_count += 1
        return _err_json(503)

    adapter = SolanaAdapter(
        chain_id="sol", rpc_url="http://x",
        commitment="confirmed", poll_interval_ms=400,
        transport=_mock_transport(handler),
        sleep=_no_sleep,
    )
    await adapter.connect()
    try:
        with pytest.raises(RetryExhausted) as exc_info:
            await adapter.fetch_block(slot=100)
    finally:
        await adapter.disconnect()
    # `core.notifier.retry.retry_with_backoff` re-raises after max_attempts;
    # the adapter passes max_attempts=3.
    assert call_count == 3, f"expected 3 RPC attempts under retry, got {call_count}"
    assert isinstance(exc_info.value.__cause__, httpx.HTTPStatusError)


@pytest.mark.asyncio
async def test_failed_tx_marked_not_success() -> None:
    """`meta.err` non-null → tx.success = False."""
    block_payload = {
        "blockhash": "B", "previousBlockhash": "P",
        "parentSlot": 0, "blockTime": 0,
        "transactions": [{
            "transaction": {
                "signatures": ["SIG"],
                "message": {"accountKeys": ["A"], "instructions": []},
            },
            "meta": {
                "err": {"InstructionError": [0, "Custom"]},
                "fee": 5000, "preBalances": [0], "postBalances": [0],
                "preTokenBalances": [], "postTokenBalances": [],
                "logMessages": [], "innerInstructions": [],
            },
        }],
    }

    def handler(req: httpx.Request) -> httpx.Response:
        body = json.loads(req.content.decode())
        if body["method"] == "getSlot":
            return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": 1})
        assert body["method"] == "getBlock"
        return _ok_json({"jsonrpc": "2.0", "id": body["id"], "result": block_payload})

    adapter = SolanaAdapter(
        chain_id="sol", rpc_url="http://x",
        commitment="confirmed", poll_interval_ms=400,
        transport=_mock_transport(handler),
        sleep=_no_sleep,
    )
    await adapter.connect()
    try:
        block = await adapter.fetch_block(slot=1)
    finally:
        await adapter.disconnect()
    assert block is not None
    [tx] = block.transactions
    assert tx.success is False
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `pytest tests/unit/test_solana_adapter.py -v`
Expected: 5 FAIL with `ModuleNotFoundError: No module named 'core.chains.solana'`.

### Task 10.7: `SolanaAdapter` (Green)

**Files:**
- Modify: `pyproject.toml` — add `solders>=0.21,<0.30` to dependencies (NOT dev — used at runtime by the worker).
- Create: `core/chains/solana.py`.

- [ ] **Step 1: Add `solders` to runtime deps**

In `pyproject.toml`, the `[project].dependencies` array:

```toml
"solders>=0.21,<0.30",
```

Re-resolve:

```bash
pip install -e ".[dev]"
```

- [ ] **Step 2: Implement `SolanaAdapter`**

```python
# core/chains/solana.py
"""HTTP-only Solana adapter using `solders` for RPC type-safety and `httpx`
for transport.

Per spec §4.6:
- HTTP-only polling at the configured commitment (no WebSocket).
- ``fetch_block`` returns ``SolanaBlock | None``; ``None`` represents a
  missed slot (the JSON-RPC `result: null` case).
- Retries (3 attempts, exponential) on transport errors and 5xx via
  ``core.notifier.retry.retry_with_backoff`` — reused, not reimplemented.

solders + httpx idiom: build a typed request (e.g. ``GetSlot(...)``),
serialize with ``.to_json()``, POST via ``httpx.AsyncClient``, parse the
response with the matching ``*Resp.from_json(...)``. NOTE: chunk 10 keeps
the JSON-RPC walk manual (the response shape is the simplest of any RPC
method we use); solders' typed responses get exercised in chunks 12 / 13
where the request side benefits most from ``Pubkey`` and borsh helpers.
"""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from functools import partial
from typing import Literal

import httpx
import structlog

from core.chains.types import (
    BlockHeader,
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)
from core.notifier.retry import retry_with_backoff

log = structlog.get_logger(__name__)


class SolanaAdapter:
    chain_id: str
    commitment: Literal["confirmed", "finalized"]

    def __init__(
        self,
        *,
        chain_id: str,
        rpc_url: str,
        commitment: Literal["confirmed", "finalized"],
        poll_interval_ms: int,
        transport: httpx.AsyncBaseTransport | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.chain_id = chain_id
        self.commitment = commitment
        self._rpc_url = rpc_url
        self._poll_interval_s = poll_interval_ms / 1000.0
        self._transport = transport
        self._sleep = sleep
        self._client: httpx.AsyncClient | None = None
        self._req_id = 0

    async def connect(self) -> None:
        # No RPC ping at connect: a real RPC error surfaces lazily on the
        # first `get_latest_slot` / `fetch_block`, which is where the runner
        # already has retry/abort logic. A ping here would add a second
        # failure mode that callers don't expect.
        self._client = httpx.AsyncClient(
            transport=self._transport,
            timeout=httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0),
        )

    async def disconnect(self) -> None:
        if self._client is not None:
            with contextlib.suppress(Exception):
                await self._client.aclose()
            self._client = None

    async def _rpc(self, method: str, params: list) -> dict:
        assert self._client is not None
        self._req_id += 1
        body = {
            "jsonrpc": "2.0",
            "id": self._req_id,
            "method": method,
            "params": params,
        }

        async def _post() -> dict:
            resp = await self._client.post(  # type: ignore[union-attr]
                self._rpc_url,
                json=body,
                headers={"content-type": "application/json"},
            )
            # 4xx and 5xx alike → HTTPStatusError → retried by retry_with_backoff
            # (which classifies every Exception except `RetryAbort` as retryable).
            # Spec §6 wants 4xx retried too (transient gateway errors look like 4xx
            # in some Solana RPC providers), so this is intentional.
            resp.raise_for_status()
            return resp.json()

        return await retry_with_backoff(
            partial(_post),
            max_attempts=3,
            base_delay=1.0,
            sleep=self._sleep,
        )

    async def get_latest_slot(self) -> int:
        result = await self._rpc("getSlot", [{"commitment": self.commitment}])
        return int(result["result"])

    async def fetch_block(self, slot: int) -> SolanaBlock | None:
        params = [
            slot,
            {
                "commitment": self.commitment,
                "encoding": "json",
                "maxSupportedTransactionVersion": 0,
                "transactionDetails": "full",
                "rewards": False,
            },
        ]
        result = await self._rpc("getBlock", params)
        raw = result.get("result")
        if raw is None:
            return None
        return _decode_block(slot=slot, raw=raw)

    def subscribe_heads(self) -> AsyncIterator[BlockHeader]:
        """Poll ``getSlot`` at ``poll_interval_ms`` and yield BlockHeader
        whenever the slot advances. ``hash`` / ``parent_hash`` are filled
        with the slot integers stringified — chunk 11's ChainRunner doesn't
        use them for reorg detection on Solana (commitment level guarantees
        finality), so a stable placeholder is fine. TODO(chunk 11): the
        Solana parser pipeline consumes ``SolanaBlock`` directly and never
        reads ``BlockHeader.hash`` / ``parent_hash`` / ``timestamp``; keep
        them as placeholders rather than over-engineering."""
        return self._poll_heads()

    async def _poll_heads(self) -> AsyncIterator[BlockHeader]:
        last = -1
        while True:
            slot = await self.get_latest_slot()
            if slot > last:
                yield BlockHeader(
                    number=slot,
                    hash=str(slot),
                    parent_hash=str(slot - 1) if slot > 0 else "",
                    timestamp=0,
                )
                last = slot
            await asyncio.sleep(self._poll_interval_s)


# -- decode helpers ---------------------------------------------------------


def _decode_token_balance(raw: dict) -> SolanaTokenBalance:
    ut = raw.get("uiTokenAmount", {}) or {}
    return SolanaTokenBalance(
        account_index=int(raw["accountIndex"]),
        mint=str(raw["mint"]),
        owner=raw.get("owner"),
        amount=int(ut.get("amount", "0")),
        decimals=int(ut.get("decimals", 0)),
    )


def _decode_instruction(
    raw: dict, account_keys: list[str], stack_depth: int
) -> SolanaInstruction:
    program_idx = int(raw["programIdIndex"])
    # `stackHeight` exists on inner instructions (validator >= 1.14) but is absent
    # on top-level instructions; fall back to the caller-supplied `stack_depth`
    # (`1` for the top-level loop, incremented for inner CPI walks in chunk 13).
    raw_stack = raw.get("stackHeight")
    return SolanaInstruction(
        program_id=account_keys[program_idx],
        accounts=[account_keys[i] for i in raw.get("accounts", [])],
        data_b58=str(raw.get("data", "")),
        stack_depth=int(raw_stack) if raw_stack is not None else stack_depth,
    )


def _decode_transaction(raw: dict) -> SolanaTransaction:
    meta = raw.get("meta") or {}
    tx = raw.get("transaction") or {}
    message = tx.get("message") or {}
    account_keys = [str(k) for k in message.get("accountKeys", [])]
    signatures = list(tx.get("signatures") or [])

    top_level = [
        _decode_instruction(ix, account_keys, stack_depth=1)
        for ix in message.get("instructions", [])
    ]
    # Inner instructions are emitted inside a separate `innerInstructions`
    # bucket per top-level position; chunk 11's parsers walk both.
    inner: list[SolanaInstruction] = []
    for inner_block in meta.get("innerInstructions") or []:
        for ix in inner_block.get("instructions") or []:
            inner.append(_decode_instruction(ix, account_keys, stack_depth=2))

    return SolanaTransaction(
        signature=signatures[0] if signatures else "",
        slot=0,  # filled in by caller — block-level field
        success=meta.get("err") is None,
        fee=int(meta.get("fee", 0) or 0),
        account_keys=account_keys,
        pre_balances=[int(x) for x in meta.get("preBalances", []) or []],
        post_balances=[int(x) for x in meta.get("postBalances", []) or []],
        pre_token_balances=[_decode_token_balance(t) for t in meta.get("preTokenBalances", []) or []],
        post_token_balances=[_decode_token_balance(t) for t in meta.get("postTokenBalances", []) or []],
        log_messages=[str(m) for m in meta.get("logMessages", []) or []],
        instructions=top_level + inner,
    )


def _decode_block(*, slot: int, raw: dict) -> SolanaBlock:
    txs = [_decode_transaction(t) for t in raw.get("transactions", []) or []]
    # Backfill the slot — _decode_transaction can't see it from `raw`.
    txs = [
        SolanaTransaction(
            signature=t.signature,
            slot=slot,
            success=t.success,
            fee=t.fee,
            account_keys=t.account_keys,
            pre_balances=t.pre_balances,
            post_balances=t.post_balances,
            pre_token_balances=t.pre_token_balances,
            post_token_balances=t.post_token_balances,
            log_messages=t.log_messages,
            instructions=t.instructions,
        )
        for t in txs
    ]
    return SolanaBlock(
        slot=slot,
        block_hash=str(raw.get("blockhash", "")),
        parent_slot=int(raw.get("parentSlot", 0) or 0),
        block_time=raw.get("blockTime"),
        transactions=txs,
    )
```

The adapter is intentionally NOT using `solders`-typed responses for the body parsing — direct JSON walking is simpler, and `solders` 0.21–0.24 has small API drift on the response side that's brittle to pin against. The `solders` dep is held in `pyproject.toml` for chunk 12 (SPL) and chunk 13 (Anchor IDL) which need `Pubkey` validation + borsh helpers, not for the adapter itself.

- [ ] **Step 3: Re-run the unit tests**

Run: `pytest tests/unit/test_solana_adapter.py -v`
Expected: all 5 PASS. If `test_fetch_block_5xx_exhausts_retries` fails with `call_count == 1`, `retry_with_backoff` isn't being called — check the `_post` wrapper is invoked through `partial(_post)` rather than the bare coroutine. If it fails with `call_count == 3` but `RetryExhausted` is not raised, the helper signature has drifted from M1 — re-read `core/notifier/retry.py`.

- [ ] **Step 4: Lint + typecheck**

Run: `make lint && make typecheck`
Expected: clean. If mypy complains about `result["result"]` being `Any`, that's expected — we're walking arbitrary JSON. Leave the `Any`s; do not add `cast`s unless mypy is configured strict enough to require them.

- [ ] **Step 5: Commit**

```bash
git add core/chains/solana.py tests/unit/test_solana_adapter.py pyproject.toml
git commit -m "feat(chains): SolanaAdapter — http-poll RPC with missed-slot semantics"
```

### Task 10.8: Worker factory branch — Solana support

**Files:**
- Modify: `apps/worker/main.py:24-36` — branch on `cfg.kind`.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_worker_adapter_factory.py
from __future__ import annotations

from apps.worker.main import _default_adapter_factory
from core.chains.evm import EvmAdapter
from core.chains.solana import SolanaAdapter
from core.config.snapshot import SnapshotChain


def _evm_cfg() -> SnapshotChain:
    return SnapshotChain(
        id="eth", kind="evm",
        rpc_http="http://x", rpc_ws=None,
        confirmations=12, commitment=None,
        poll_interval_ms=3000,
    )


def _sol_cfg() -> SnapshotChain:
    return SnapshotChain(
        id="sol", kind="solana",
        rpc_http="http://x", rpc_ws=None,
        confirmations=0, commitment="confirmed",
        poll_interval_ms=400,
    )


def test_factory_returns_evm_adapter_for_evm_kind() -> None:
    adapter = _default_adapter_factory(_evm_cfg())
    assert isinstance(adapter, EvmAdapter)
    assert adapter.chain_id == "eth"


def test_factory_returns_solana_adapter_for_solana_kind() -> None:
    adapter = _default_adapter_factory(_sol_cfg())
    assert isinstance(adapter, SolanaAdapter)
    assert adapter.chain_id == "sol"
    assert adapter.commitment == "confirmed"


def test_factory_unknown_kind_raises() -> None:
    cfg = SnapshotChain(
        id="x", kind="dogecoin",
        rpc_http="http://x", rpc_ws=None,
        confirmations=0, commitment=None, poll_interval_ms=400,
    )
    import pytest
    with pytest.raises((NotImplementedError, ValueError)):
        _default_adapter_factory(cfg)
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `pytest tests/unit/test_worker_adapter_factory.py -v`
Expected: the Solana case fails — current factory `raise NotImplementedError(...)` on non-EVM. The EVM case passes.

- [ ] **Step 3: Branch the factory**

`apps/worker/main.py:24-36`:

```python
def _default_adapter_factory(cfg: SnapshotChain):
    if cfg.kind == "evm":
        # Keep `poll_interval_ms` plumbed through — chunk 1.3 added it and ships a
        # regression test (`tests/unit/test_evm_poll_interval.py`). Do not drop it.
        return EvmAdapter(
            chain_id=cfg.id,
            rpc_http=cfg.rpc_http,
            rpc_ws=cfg.rpc_ws,
            confirmations=cfg.confirmations,
            poll_interval_ms=cfg.poll_interval_ms,
        )
    if cfg.kind == "solana":
        assert cfg.commitment is not None, (
            f"solana chain {cfg.id!r} loaded without a commitment "
            "— DB-level invariant broken; check chain create API"
        )
        return SolanaAdapter(
            chain_id=cfg.id,
            rpc_url=cfg.rpc_http,
            commitment=cfg.commitment,  # type: ignore[arg-type]
            poll_interval_ms=cfg.poll_interval_ms,
        )
    raise NotImplementedError(f"chain kind {cfg.kind!r} not supported")
```

Also add the import at the top of `apps/worker/main.py`:

```python
from core.chains.solana import SolanaAdapter
```

The return type is intentionally NOT annotated. The two branches return different concrete types; the union type would be `EvmAdapter | SolanaAdapter`, which is fine but adds churn at every call site. ChainRunner (chunk 11) accepts both via `ChainAdapter | SolanaChainAdapter`.

- [ ] **Step 4: Re-run the test**

Run: `pytest tests/unit/test_worker_adapter_factory.py -v`
Expected: 3 PASS.

- [ ] **Step 5: Re-run the full worker IT suite**

Run: `pytest tests/integration/test_worker_*.py -v`
Expected: still green. The Solana branch isn't exercised because no Solana chain is configured in M1's IT.

- [ ] **Step 6: Commit**

```bash
git add apps/worker/main.py tests/unit/test_worker_adapter_factory.py
git commit -m "feat(worker): adapter factory branches on chain kind"
```

### Task 10.9: Integration test against `solana-test-validator`

A session-scoped fixture spins up `solana-test-validator` once per test session (5–10 s cold start), exposing an ephemeral RPC URL. The IT exercises `get_latest_slot`, `fetch_block` on a real slot, and the missed-slot case (slot intentionally chosen well above tip).

Validator install is detected at fixture-entry; tests skip if `solana-test-validator` isn't on PATH. Install instructions are in `docs/dev-setup.md` (`sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"`).

**Files:**
- Modify (append to): `tests/integration/conftest.py` — file already exists from M1 with a `db` fixture; the `solana_validator` fixture below is **additive** (do NOT overwrite the existing fixtures or imports; add the new code below them).
- Create: `tests/integration/test_solana_adapter.py`.

**pytest-asyncio scope note:** the fixture below is a plain `@pytest.fixture(scope="session")` (sync `def`, not `async def`), and the IT functions are decorated with `pytestmark = [pytest.mark.asyncio]` (default function-scoped event loop). This combination is the safest cross-version pattern for pytest-asyncio: a sync session-scoped fixture is consumed by an async function-scoped test without ever asking pytest-asyncio to reconcile event-loop scopes. If the project later moves to pytest-asyncio 1.x and starts complaining about scope mismatches, the fix is to set `asyncio_default_fixture_loop_scope = "session"` in `pyproject.toml` (NOT to make the fixture `async`).

- [ ] **Step 1: Add the validator fixture**

```python
# tests/integration/conftest.py  --  APPEND the below to the existing file (do not overwrite the M1 `db` fixture or its imports)

from __future__ import annotations

import contextlib
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import httpx
import pytest


@dataclass
class SolanaValidatorHandle:
    rpc_url: str
    process: subprocess.Popen
    ledger_path: Path


def _free_tcp_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_rpc(url: str, *, timeout_s: float = 25.0) -> None:
    deadline = time.monotonic() + timeout_s
    body = b'{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
    while time.monotonic() < deadline:
        try:
            r = httpx.post(url, content=body, headers={"content-type": "application/json"}, timeout=2.0)
            if r.status_code == 200 and r.json().get("result") == "ok":
                return
        except (httpx.RequestError, ValueError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"solana-test-validator did not become healthy at {url}")


@pytest.fixture(scope="session")
def solana_validator(tmp_path_factory) -> Iterator[SolanaValidatorHandle]:
    """Session-scoped: 5–10 s cold start; subsequent tests reuse the same
    validator. Validator state accumulates across tests, but every IT here
    is read-only so that's acceptable."""
    if shutil.which("solana-test-validator") is None:
        pytest.skip("solana-test-validator not installed; see docs/dev-setup.md")

    rpc_port = _free_tcp_port()
    faucet_port = _free_tcp_port()
    ledger = tmp_path_factory.mktemp("solana-ledger")
    proc = subprocess.Popen(
        [
            "solana-test-validator",
            "--reset",
            "--quiet",
            "--ledger", str(ledger),
            "--rpc-port", str(rpc_port),
            "--faucet-port", str(faucet_port),
            "--bind-address", "127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    rpc_url = f"http://127.0.0.1:{rpc_port}"
    try:
        _wait_for_rpc(rpc_url)
        yield SolanaValidatorHandle(rpc_url=rpc_url, process=proc, ledger_path=ledger)
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5.0)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)
```

- [ ] **Step 2: Write the integration test**

```python
# tests/integration/test_solana_adapter.py
"""IT against a real solana-test-validator.

Covers:
- get_latest_slot returns an int that increases over time.
- fetch_block at an existing slot returns a SolanaBlock with at least one tx
  (the validator always has a `vote` tx).
- fetch_block at a far-future slot returns None (missed slot semantics).
"""
from __future__ import annotations

import asyncio

import pytest

from core.chains.solana import SolanaAdapter
from core.chains.types import SolanaBlock

pytestmark = [pytest.mark.asyncio]


async def test_get_latest_slot_increases(solana_validator) -> None:
    adapter = SolanaAdapter(
        chain_id="local",
        rpc_url=solana_validator.rpc_url,
        commitment="confirmed",
        poll_interval_ms=400,
    )
    await adapter.connect()
    try:
        s1 = await adapter.get_latest_slot()
        await asyncio.sleep(2.0)
        s2 = await adapter.get_latest_slot()
        assert s1 >= 0
        assert s2 >= s1, f"slot did not advance: {s1} -> {s2}"
    finally:
        await adapter.disconnect()


async def test_fetch_existing_block_returns_solana_block(solana_validator) -> None:
    adapter = SolanaAdapter(
        chain_id="local",
        rpc_url=solana_validator.rpc_url,
        commitment="confirmed",
        poll_interval_ms=400,
    )
    await adapter.connect()
    try:
        # Pick a slot well below the tip — every populated slot returns a block.
        tip = await adapter.get_latest_slot()
        target = max(tip - 5, 1)
        # Walk backwards a few slots; pick the first non-null result.
        block: SolanaBlock | None = None
        for s in range(target, max(target - 10, 0), -1):
            block = await adapter.fetch_block(s)
            if block is not None:
                break
        assert block is not None, "no populated block found within 10 slots of tip"
        assert block.slot >= 1
        assert block.block_hash != ""
        # Validator always emits a vote tx per slot.
        assert len(block.transactions) >= 1
    finally:
        await adapter.disconnect()


async def test_fetch_far_future_slot_returns_none(solana_validator) -> None:
    adapter = SolanaAdapter(
        chain_id="local",
        rpc_url=solana_validator.rpc_url,
        commitment="confirmed",
        poll_interval_ms=400,
    )
    await adapter.connect()
    try:
        tip = await adapter.get_latest_slot()
        # Way above the tip — slot 10 000 000 ahead does NOT exist yet.
        result = await adapter.fetch_block(tip + 10_000_000)
        assert result is None, "expected None for un-produced slot"
    finally:
        await adapter.disconnect()
```

- [ ] **Step 3: Run the IT**

```bash
pytest tests/integration/test_solana_adapter.py -v
```

Expected on a host with `solana-test-validator` installed: 3 PASS in ~10–15 s (5–10 s of which is the validator boot). On a host without it: 3 SKIP.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/conftest.py tests/integration/test_solana_adapter.py
git commit -m "test(integration): solana adapter against solana-test-validator"
```

### Task 10.10: Close-out — verify full suite green

- [ ] **Step 1: Run the full unit suite**

```bash
make test
```
Expected: every chunk 1–10 unit / IT test passes. If `tests/integration/test_solana_adapter.py` SKIPs because the validator isn't installed, that's acceptable.

- [ ] **Step 2: Lint + typecheck**

```bash
make lint && make typecheck
```
Expected: clean.

- [ ] **Step 3: Final commit**

If any cleanup was needed (e.g., trailing `import core.chains.solana` left in a stub), this is the place to land it. No tag for chunk 10 — chunk 14 is the M2 close-out tag.

```bash
git status   # confirm nothing left uncommitted
```

---
## Chunk 11: `SolNativeTransferParser` + parser-pipeline split

Splits the M1 parser API into chain-segmented Protocols and gives the runner a Solana code path:

1. **Rename the EVM-side trio** — `Parser` → `EvmParser`, `ParserPipeline` → `EvmParserPipeline`, `NativeTransferParser` → `EvmNativeTransferParser`. Pure rename; no behavior change. Done in one task to keep the diff atomic.
2. **`SolanaParser` Protocol** — consumes a `SolanaBlock`, yields `Event`. The same `Event` dataclass M1 uses; the chain difference is on the input side, not the output side.
3. **`SolanaParserPipeline`** — `run(block: SolanaBlock) -> Iterable[Event]`. Same exception-isolation semantics as `EvmParserPipeline` (one bad parser doesn't poison the rest).
4. **`SolNativeTransferParser`** — parses System Program (`11111111111111111111111111111111`) `Transfer` instructions (discriminator `2u32 LE`) at `stack_depth == 1`. Emits `Event(kind="native_transfer", args={"from": ..., "to": ..., "value": str(lamports)}, contract=None)`. Same shape EVM `EvmNativeTransferParser` emits so the matcher's `arg_filters` pattern is chain-agnostic.
5. **`ChainRunner` branches on `cfg.kind`** — EVM path is unchanged (`ConfirmationBuffer`, `EvmParserPipeline`, `_process_confirmed_block(number)`). Solana path skips the buffer (commitment level handles finality, per spec §4.6), constructs `SolanaParserPipeline`, and processes confirmed slots directly via `_process_confirmed_slot(slot)`.
6. **End-to-end integration** — `ChainRunner` + `SolanaAdapter` against `solana-test-validator`, submitting a real lamport transfer via `solders` and asserting the resulting `Event` reaches a mocked `Notifier`.

The runner-branching task is the big one (~250 lines). Everything else is small.

**Modified files this chunk:**
- `core/parser/base.py` — rename `Parser` → `EvmParser`; append `SolanaParser` Protocol below.
- `core/parser/pipeline.py` — rename `ParserPipeline` → `EvmParserPipeline`; append `SolanaParserPipeline` class.
- `core/parser/native.py` — rename class `NativeTransferParser` → `EvmNativeTransferParser` (file name unchanged; one-line class rename only — keeps git blame linear).
- `core/parser/sol_native.py` — **new**: `SolNativeTransferParser`.
- `apps/worker/chain_runner.py` — branch on `self._chain.kind` for pipeline construction and head-processing path; widen `_adapter` type to `ChainAdapter | SolanaChainAdapter`.
- `tests/unit/test_native_parser.py` — rename references to `EvmNativeTransferParser`.
- `tests/unit/test_pipeline.py` — rename references to `EvmParserPipeline` and `EvmNativeTransferParser`.
- `tests/unit/test_sol_native_parser.py` — **new**.
- `tests/unit/test_solana_parser_pipeline.py` — **new**.
- `tests/unit/test_chain_runner.py` — extend with `test_chain_runner_solana_branch` (constructs the runner with a fake Solana adapter; asserts the buffer is bypassed and pipeline produces events).
- `tests/integration/test_chain_runner_solana.py` — **new**.

**Out of scope this chunk:**
- SPL token transfer parser (`SplTransferParser`) — chunk 12.
- Anchor IDL event parser — chunk 13.
- `ChainRunner` reorg-replay for Solana — spec §4.6 explicitly says commitment level handles finality; no Solana reorg path exists.
- Matcher case-folding of base58 addresses (M1 `.lower()`s `event.contract`). `SolNativeTransferParser` sets `contract=None`, so the issue doesn't surface yet; chunk 12's SPL parser (which DOES set `contract` to a base58 mint) is where it'd bite. Flagged as M2 follow-up #11; deliberately left for chunk 12.
- Adapter-side `chain_runner.py` checkpoint format change (still `(int, str)` for slot/blockhash on Solana — the existing `CheckpointRepo` accepts arbitrary strings for `last_block_hash`, and `int(slot)` fits `last_block`).

**Naming pin:** The renamed Protocol is `EvmParser`, NOT `EVMParser`. The Solana counterpart is `SolanaParser`, NOT `SolParser`. The concrete class for system transfers is `SolNativeTransferParser` (matches `EvmNativeTransferParser` length and reads cleanly in registry imports). `SOLANA_SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"` is the canonical constant — define it once in `core/parser/sol_native.py` and re-export from `core/parser/__init__.py` if any other module needs it.

**Spec §4.6 reminder:** Solana has NO `ConfirmationBuffer`. The runner's `_handle_head` for Solana yields one confirmed slot per polled `BlockHeader` from `SolanaAdapter.subscribe_heads()` (the adapter already filters out unchanged slots). Missed slots — `fetch_block(slot)` returning `None` — are skipped without raising. Checkpoint persistence still happens per processed slot.

### Task 11.1: Rename EVM-side Parser / Pipeline / NativeTransferParser

**Files:**
- Modify: `core/parser/base.py` — class rename.
- Modify: `core/parser/pipeline.py` — class rename + type annotation update.
- Modify: `core/parser/native.py` — class rename.
- Modify: `apps/worker/chain_runner.py:20-21,75` — imports + construction.
- Modify: `tests/unit/test_native_parser.py` — all references.
- Modify: `tests/unit/test_pipeline.py` — all references.

- [ ] **Step 1: Rename `Parser` → `EvmParser`**

`core/parser/base.py`:

```python
from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from core.chains.types import Block
from core.parser.event import Event


class EvmParser(Protocol):
    """An EVM parser consumes a confirmed Block and yields Events.

    Implementations should be stateless and side-effect free; the same Block
    may be re-parsed during reorg replay (per spec §4.5 confirmation buffer).

    See ``SolanaParser`` (defined in Task 11.2 below) for the Solana
    counterpart — divergent because the input dataclass differs.
    """

    def parse(self, block: Block) -> Iterable[Event]: ...
```

- [ ] **Step 2: Rename `ParserPipeline` → `EvmParserPipeline`**

`core/parser/pipeline.py`:

```python
from __future__ import annotations

from collections.abc import Iterable, Sequence

import structlog

from core.chains.types import Block
from core.parser.base import EvmParser
from core.parser.event import Event

log = structlog.get_logger(__name__)


class EvmParserPipeline:
    """Run a sequence of EVM parsers over a Block and yield all produced events.

    Any parser that raises is logged and skipped (spec §9 "Matcher exception per
    event" applies equally to parsers — pipeline keeps running).
    """

    def __init__(self, parsers: Sequence[EvmParser]) -> None:
        self._parsers = list(parsers)

    def run(self, block: Block) -> Iterable[Event]:
        for p in self._parsers:
            try:
                yield from p.parse(block)
            except Exception:  # noqa: BLE001 — isolate parser failures
                log.exception(
                    "parser.exception",
                    parser=type(p).__name__,
                    block_number=block.header.number,
                    block_hash=block.header.hash,
                )
```

- [ ] **Step 3: Rename `NativeTransferParser` → `EvmNativeTransferParser`**

`core/parser/native.py` — single-line class rename (the docstring already mentions "EVM" in the first line; no body change):

```python
class EvmNativeTransferParser:
    """Emit a native_transfer Event for each tx with value > 0 (EVM).
    ...
    """
```

- [ ] **Step 4: Update `apps/worker/chain_runner.py` imports + construction**

Replace the two parser imports and the `_pipeline` line:

```python
from core.parser.native import EvmNativeTransferParser
from core.parser.pipeline import EvmParserPipeline
```

And inside `__init__` (currently `chain_runner.py:75`):

```python
self._pipeline = EvmParserPipeline([EvmNativeTransferParser(chain_id=self._chain.id)])
```

(Task 11.5 widens this to `EvmParserPipeline | SolanaParserPipeline` by deferring construction to `start()` — for now, keep it on `__init__` so existing tests don't move.)

- [ ] **Step 5: Update the test files**

`tests/unit/test_native_parser.py` — replace all three `NativeTransferParser` references at lines 2, 14, 60, 79 with `EvmNativeTransferParser`.

`tests/unit/test_pipeline.py` — replace lines 5, 6, 36, 56:
- `from core.parser.native import NativeTransferParser` → `from core.parser.native import EvmNativeTransferParser`
- `from core.parser.pipeline import ParserPipeline` → `from core.parser.pipeline import EvmParserPipeline`
- `ParserPipeline(parsers=[...])` → `EvmParserPipeline(parsers=[...])` (both occurrences)
- `NativeTransferParser(chain_id="x")` → `EvmNativeTransferParser(chain_id="x")`

- [ ] **Step 6: Run the full unit suite to confirm rename is clean**

```bash
pytest tests/unit/test_native_parser.py tests/unit/test_pipeline.py tests/unit/test_chain_runner.py -v
```

Expected: every test still passes. If any test fails with `ImportError` or `NameError`, the rename missed a call-site — grep for the old name:

```bash
rg -n '\bParser\b|\bParserPipeline\b|\bNativeTransferParser\b' core/ apps/ tests/
```

(should only return `EvmParser`, `EvmParserPipeline`, `EvmNativeTransferParser`, plus the chunk-12-and-later `SolanaParser`/`SolanaParserPipeline` which don't exist yet.)

- [ ] **Step 7: Lint / type-check**

Run: `make lint typecheck`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add core/parser/base.py core/parser/pipeline.py core/parser/native.py \
        apps/worker/chain_runner.py \
        tests/unit/test_native_parser.py tests/unit/test_pipeline.py
git commit -m "refactor(parser): rename Parser/ParserPipeline/NativeTransferParser to Evm* prefix"
```

### Task 11.2: `SolanaParser` Protocol + `SolanaParserPipeline`

**Files:**
- Modify: `core/parser/base.py` — append `SolanaParser` Protocol below `EvmParser`.
- Modify: `core/parser/pipeline.py` — append `SolanaParserPipeline` below `EvmParserPipeline`.
- Create: `tests/unit/test_solana_parser_pipeline.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_solana_parser_pipeline.py
"""SolanaParserPipeline mirrors EvmParserPipeline:
- yields all events from all parsers in order
- isolates per-parser exceptions (one bad parser ≠ dead pipeline)
"""
from __future__ import annotations

from collections.abc import Iterable

import pytest

from core.chains.types import SolanaBlock
from core.parser.event import Event
from core.parser.pipeline import SolanaParserPipeline


def _block() -> SolanaBlock:
    return SolanaBlock(
        slot=42, block_hash="H42", parent_slot=41,
        block_time=1_700_000_000, transactions=[],
    )


class _Fake:
    """Yields one event named after `tag` for any SolanaBlock."""
    def __init__(self, tag: str) -> None:
        self._tag = tag

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        yield Event(
            chain_id="sol", block_number=block.slot, block_hash=block.block_hash,
            block_timestamp=block.block_time or 0,
            tx_hash=f"sig-{self._tag}", tx_index=None, log_index=None,
            kind="native_transfer", contract=None, name=self._tag,
            args={}, raw={},
        )


class _Broken:
    def parse(self, block: SolanaBlock) -> Iterable[Event]:  # noqa: ARG002
        raise RuntimeError("synthetic parser failure")
        yield  # pragma: no cover  -- generator marker


def test_solana_pipeline_yields_in_parser_order() -> None:
    pipe = SolanaParserPipeline(parsers=[_Fake("a"), _Fake("b")])
    events = list(pipe.run(_block()))
    assert [e.name for e in events] == ["a", "b"]


def test_solana_pipeline_isolates_broken_parser() -> None:
    pipe = SolanaParserPipeline(parsers=[_Broken(), _Fake("good")])
    events = list(pipe.run(_block()))
    assert [e.name for e in events] == ["good"]
```

- [ ] **Step 2: Run the test, expect FAIL with `ImportError`**

Run: `pytest tests/unit/test_solana_parser_pipeline.py -v`
Expected: `ImportError: cannot import name 'SolanaParserPipeline' from 'core.parser.pipeline'`.

- [ ] **Step 3: Add `SolanaParser` Protocol**

Append to `core/parser/base.py`:

```python
from core.chains.types import SolanaBlock  # add to the existing imports at the top


class SolanaParser(Protocol):
    """A Solana parser consumes a confirmed SolanaBlock and yields Events.

    Same statelessness / side-effect-free contract as ``EvmParser``. The
    block input differs because Solana's data model is not an EVM lookalike
    (slots vs blocks, instructions vs txs, log_messages array vs structured
    receipts).
    """

    def parse(self, block: SolanaBlock) -> Iterable[Event]: ...
```

- [ ] **Step 4: Add `SolanaParserPipeline`**

Append to `core/parser/pipeline.py`:

```python
from core.chains.types import SolanaBlock  # add to the existing imports
from core.parser.base import SolanaParser   # extend the existing import line


class SolanaParserPipeline:
    """Run a sequence of Solana parsers over a SolanaBlock and yield events.

    Mirrors ``EvmParserPipeline`` exactly; the only difference is the input
    dataclass. Same per-parser isolation policy.
    """

    def __init__(self, parsers: Sequence[SolanaParser]) -> None:
        self._parsers = list(parsers)

    def run(self, block: SolanaBlock) -> Iterable[Event]:
        for p in self._parsers:
            try:
                yield from p.parse(block)
            except Exception:  # noqa: BLE001 — isolate parser failures
                log.exception(
                    "parser.exception",
                    parser=type(p).__name__,
                    slot=block.slot,
                    block_hash=block.block_hash,
                )
```

- [ ] **Step 5: Re-run the test**

Run: `pytest tests/unit/test_solana_parser_pipeline.py -v`
Expected: 2 PASS.

- [ ] **Step 6: Lint / typecheck + the older pipeline test (regression)**

Run: `pytest tests/unit/test_pipeline.py tests/unit/test_solana_parser_pipeline.py -v && make lint typecheck`
Expected: 2 new + N original PASS; lint/typecheck clean.

- [ ] **Step 7: Commit**

```bash
git add core/parser/base.py core/parser/pipeline.py tests/unit/test_solana_parser_pipeline.py
git commit -m "feat(parser): SolanaParser Protocol + SolanaParserPipeline"
```

### Task 11.3: `SolNativeTransferParser` (Red → Green)

Parses System Program (`11111111111111111111111111111111`) `Transfer` instructions at the **top level only** (`stack_depth == 1`). Skips failed transactions (`tx.success is False`). Emits one `Event` per matching instruction.

**System Program Transfer instruction encoding** (per Solana program docs):
- Program ID: `11111111111111111111111111111111` (32 zero bytes, base58-encoded — the "all 1s" string is the canonical form).
- Data: 12 bytes — `[u32 LE discriminator = 2] + [u64 LE lamports]`.
- Accounts: `[funding_account (signer, writable), recipient (writable)]`.

`SolanaInstruction.data_b58` is the base58 encoding of those 12 bytes. The parser:
1. Base58-decodes `data_b58`.
2. Confirms length == 12 and the first 4 bytes are `b"\x02\x00\x00\x00"`.
3. Reads the u64 LE lamport amount from bytes 4..12.
4. Looks up `ix.accounts[0]` and `ix.accounts[1]` (the `SolanaInstruction.accounts` field is already resolved to base58 pubkeys by `SolanaAdapter._decode_instruction`).
5. Emits `Event(kind="native_transfer", args={"from": ..., "to": ..., "value": str(lamports)})`.

Other System Program instructions (CreateAccount discriminator 0, Assign discriminator 1, etc.) are ignored.

**Files:**
- Modify: `pyproject.toml` — add `base58` to `[project].dependencies`.
- Create: `core/parser/sol_native.py`.
- Create: `tests/unit/test_sol_native_parser.py`.

**Dependency note:** `solders` is a PyO3 binding that bundles its base58 logic in Rust — it does **not** install the PyPI `base58` package transitively. We need it explicitly for the parser to decode `SolanaInstruction.data_b58` payloads.

- [ ] **Step 1: Add `base58` to `pyproject.toml`**

Edit `pyproject.toml`'s `[project].dependencies` list:

```toml
# pyproject.toml — under [project].dependencies
"base58>=2.1,<3",
```

Then refresh the venv:

```bash
uv sync   # or `pip install -e .` depending on your toolchain
python -c "import base58; print(base58.__version__)"
```

Expected: prints `2.x.y` without ImportError. Commit this on its own so the green dep change isn't tangled with the red parser commit:

```bash
git add pyproject.toml uv.lock  # uv.lock if present
git commit -m "build(deps): add base58 (Solana System Program Transfer decoding)"
```

- [ ] **Step 2: Write the failing tests**

```python
# tests/unit/test_sol_native_parser.py
"""SolNativeTransferParser:
- decodes System Program Transfer (discriminator 2, u64 LE lamports)
- ignores other System Program ops (CreateAccount disc 0, Assign disc 1)
- ignores instructions at stack_depth > 1 (inner CPI)
- skips transactions with success == False
- emits Event with chain_id, from/to/value, kind=native_transfer
"""
from __future__ import annotations

import base58

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTransaction,
)
from core.parser.sol_native import (
    SOLANA_SYSTEM_PROGRAM_ID,
    SolNativeTransferParser,
)


def _transfer_data(lamports: int) -> str:
    """Encode the 12-byte System Program Transfer payload as base58."""
    import struct
    payload = b"\x02\x00\x00\x00" + struct.pack("<Q", lamports)
    return base58.b58encode(payload).decode()


def _ix(
    program_id: str = SOLANA_SYSTEM_PROGRAM_ID,
    accounts: list[str] | None = None,
    data_b58: str = "",
    stack_depth: int = 1,
) -> SolanaInstruction:
    return SolanaInstruction(
        program_id=program_id,
        accounts=accounts or ["FROM", "TO"],
        data_b58=data_b58,
        stack_depth=stack_depth,
    )


def _tx(
    signature: str = "SIG",
    success: bool = True,
    instructions: list[SolanaInstruction] | None = None,
) -> SolanaTransaction:
    return SolanaTransaction(
        signature=signature,
        slot=100,
        success=success,
        fee=5000,
        account_keys=["FROM", "TO"],
        pre_balances=[10**9, 0],
        post_balances=[10**9 - 5000 - 1_000, 1_000],
        pre_token_balances=[],
        post_token_balances=[],
        log_messages=[],
        instructions=instructions or [],
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(
        slot=100, block_hash="H100", parent_slot=99,
        block_time=1_700_000_000, transactions=txs,
    )


def test_emits_native_transfer_for_system_program_transfer() -> None:
    ix = _ix(data_b58=_transfer_data(1_000))
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol-mainnet")
    [event] = list(p.parse(block))
    assert event.chain_id == "sol-mainnet"
    assert event.kind == "native_transfer"
    assert event.contract is None
    assert event.args == {"from": "FROM", "to": "TO", "value": "1000"}
    assert event.block_number == 100
    assert event.block_hash == "H100"
    assert event.block_timestamp == 1_700_000_000
    assert event.tx_hash == "SIG"


def test_ignores_non_transfer_system_ops() -> None:
    # CreateAccount has discriminator 0 (4 bytes of zeros) — must be ignored.
    create_acct_data = base58.b58encode(b"\x00\x00\x00\x00" + b"\x00" * 8).decode()
    ix = _ix(data_b58=create_acct_data)
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_ignores_non_system_programs() -> None:
    ix = _ix(program_id="SomeOtherProgramId11111111111111111111111", data_b58=_transfer_data(500))
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_ignores_inner_cpi_transfers() -> None:
    # Top-level transfers from a user wallet are stack_depth=1.
    # Transfers inside a CPI (e.g., a vault program forwarding lamports) are
    # stack_depth >= 2 — out of scope for "user-issued transfer" semantics.
    ix = _ix(data_b58=_transfer_data(1_000), stack_depth=2)
    block = _block([_tx(instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_skips_failed_transactions() -> None:
    ix = _ix(data_b58=_transfer_data(1_000))
    block = _block([_tx(success=False, instructions=[ix])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []


def test_emits_multiple_when_one_tx_has_multiple_transfers() -> None:
    ix1 = _ix(data_b58=_transfer_data(100))
    ix2 = _ix(data_b58=_transfer_data(200))
    block = _block([_tx(instructions=[ix1, ix2])])
    p = SolNativeTransferParser(chain_id="sol")
    events = list(p.parse(block))
    assert [e.args["value"] for e in events] == ["100", "200"]


def test_ignores_malformed_transfer_payload() -> None:
    # Length != 12 -> ignore silently rather than crash the pipeline.
    short = base58.b58encode(b"\x02\x00\x00\x00").decode()
    block = _block([_tx(instructions=[_ix(data_b58=short)])])
    p = SolNativeTransferParser(chain_id="sol")
    assert list(p.parse(block)) == []
```

- [ ] **Step 3: Run the tests, expect FAIL on import**

Run: `pytest tests/unit/test_sol_native_parser.py -v`
Expected: `ImportError: cannot import name 'SolNativeTransferParser' from 'core.parser.sol_native'`.

(If the failure is instead `ModuleNotFoundError: No module named 'base58'`, Step 1 was skipped — go back and add the dep before continuing.)

- [ ] **Step 4: Implement `SolNativeTransferParser`**

```python
# core/parser/sol_native.py
"""SolNativeTransferParser: parses Solana System Program Transfer instructions.

Per spec §4.6, this is the Solana counterpart to ``EvmNativeTransferParser``.
The emitted Event shape is identical so the matcher's ``arg_filters`` pattern
(``{"to": "<addr>"}``) is chain-agnostic.

Scope:
- Top-level instructions only (stack_depth == 1). Inner CPI transfers are
  not "user-issued" and are out of scope here — chunk 13's AnchorIdlEventParser
  is the right tool when a program wraps lamport movement in custom events.
- Successful transactions only.
- Malformed payloads are silently ignored (length != 12 or discriminator != 2).
  The pipeline's per-parser exception isolation would catch a raise, but we
  prefer not to spam logs on every non-Transfer System op the validator emits.
"""
from __future__ import annotations

import struct
from collections.abc import Iterable

import base58
import structlog

from core.chains.types import SolanaBlock
from core.parser.event import Event

log = structlog.get_logger(__name__)


SOLANA_SYSTEM_PROGRAM_ID = "11111111111111111111111111111111"
_TRANSFER_DISCRIMINATOR = b"\x02\x00\x00\x00"  # u32 LE = 2


class SolNativeTransferParser:
    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            for ix in tx.instructions:
                if ix.program_id != SOLANA_SYSTEM_PROGRAM_ID:
                    continue
                if ix.stack_depth != 1:
                    continue
                lamports = _maybe_decode_transfer(ix.data_b58)
                if lamports is None:
                    continue
                if len(ix.accounts) < 2:
                    continue
                yield Event(
                    chain_id=self._chain_id,
                    block_number=block.slot,
                    block_hash=block.block_hash,
                    block_timestamp=block.block_time or 0,
                    tx_hash=tx.signature,
                    tx_index=None,  # Solana txs aren't ordered by an index in the same way as EVM
                    log_index=None,
                    kind="native_transfer",
                    contract=None,
                    name=None,
                    args={
                        "from": ix.accounts[0],
                        "to": ix.accounts[1],
                        "value": str(lamports),
                    },
                    raw={"signature": tx.signature, "slot": block.slot},
                )


def _maybe_decode_transfer(data_b58: str) -> int | None:
    """Return lamports if `data_b58` decodes to a System Program Transfer
    payload, else None. Catches the malformed-base58 case too — base58
    errors are not exceptional inside a busy block."""
    try:
        raw = base58.b58decode(data_b58)
    except ValueError:
        return None
    if len(raw) != 12 or raw[:4] != _TRANSFER_DISCRIMINATOR:
        return None
    (lamports,) = struct.unpack("<Q", raw[4:12])
    return int(lamports)
```

- [ ] **Step 5: Re-run the tests**

Run: `pytest tests/unit/test_sol_native_parser.py -v`
Expected: 7 PASS.

- [ ] **Step 6: Lint / typecheck**

```bash
make lint typecheck
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add core/parser/sol_native.py tests/unit/test_sol_native_parser.py
git commit -m "feat(parser): SolNativeTransferParser for System Program transfers"
```

### Task 11.4: `ChainRunner` branches on `chain.kind`

The bulk of the chunk. The runner now drives two physically distinct chain protocols:
- **EVM** — `ChainAdapter` (block-numbered), `ConfirmationBuffer`, `EvmParserPipeline`, ancestor pre-fetch, reorg replay.
- **Solana** — `SolanaChainAdapter` (slot-numbered), NO buffer, `SolanaParserPipeline`, no ancestor pre-fetch, no reorg replay, missed-slot returns `None` from `fetch_block`.

The split lives entirely inside `ChainRunner`. Callers (`apps/worker/main.py`'s `run_worker`) don't see the difference — they still construct one `ChainRunner` per chain regardless of kind.

**Files:**
- Modify: `apps/worker/chain_runner.py` — branch in `start`, `_handle_head`, and split `_process_confirmed_block` into EVM + Solana variants. Widen `_adapter` type.
- Modify: `tests/unit/test_chain_runner.py` — add `test_chain_runner_solana_branch` (fake Solana adapter + fake Solana parser; assert the buffer is not consulted and events surface).

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_chain_runner.py`:

```python
# tests/unit/test_chain_runner.py  --  APPEND

import pytest

from core.chains.types import (
    SolanaBlock,
    SolanaTransaction,
)


class _FakeSolanaAdapter:
    """Yields one BlockHeader for slot 100 then stops."""

    chain_id = "sol"
    commitment = "confirmed"

    def __init__(self) -> None:
        self.connect_calls = 0
        self.disconnect_calls = 0
        self._slot_pulled = False

    async def connect(self) -> None:
        self.connect_calls += 1

    async def disconnect(self) -> None:
        self.disconnect_calls += 1

    async def get_latest_slot(self) -> int:
        return 100

    async def fetch_block(self, slot: int) -> SolanaBlock | None:
        if slot != 100:
            return None
        return SolanaBlock(
            slot=100, block_hash="H100", parent_slot=99,
            block_time=1_700_000_000,
            transactions=[
                SolanaTransaction(
                    signature="SIG", slot=100, success=True, fee=5000,
                    account_keys=["A", "B"],
                    pre_balances=[10**9, 0], post_balances=[10**9 - 5000, 0],
                    pre_token_balances=[], post_token_balances=[],
                    log_messages=[], instructions=[],  # parser is mocked, no real ix
                ),
            ],
        )

    def subscribe_heads(self):
        from core.chains.types import BlockHeader

        async def _gen():
            if not self._slot_pulled:
                self._slot_pulled = True
                yield BlockHeader(number=100, hash="100", parent_hash="99", timestamp=0)
        return _gen()


@pytest.mark.asyncio
async def test_chain_runner_solana_branch_bypasses_buffer() -> None:
    """Construct a ChainRunner for a Solana chain and confirm:
    - it uses SolanaParserPipeline (not EvmParserPipeline)
    - no ConfirmationBuffer is instantiated
    - a fake Solana parser's emitted event reaches the (mocked) notifier
    """
    from collections.abc import Iterable

    from apps.worker.chain_runner import ChainRunner
    from core.config.snapshot import (
        ConfigSnapshot,
        SnapshotChain,
        SnapshotChannel,
    )
    from core.parser.event import Event
    from core.parser.pipeline import SolanaParserPipeline

    chain = SnapshotChain(
        id="sol", kind="solana", rpc_http="http://x", rpc_ws=None,
        confirmations=0, commitment="confirmed", poll_interval_ms=400,
    )

    class _FakeSolParser:
        def parse(self, block: SolanaBlock) -> Iterable[Event]:
            yield Event(
                chain_id="sol", block_number=block.slot, block_hash=block.block_hash,
                block_timestamp=block.block_time or 0,
                tx_hash="SIG", tx_index=None, log_index=None,
                kind="native_transfer", contract=None, name=None,
                args={"from": "A", "to": "B", "value": "1000"}, raw={},
            )

    dispatched: list[Event] = []

    class _FakeCheckpointRepo:
        def __init__(self) -> None:
            self.saved: list[tuple[str, int, str]] = []

        async def get(self, chain_id: str):
            return None

        async def save(self, chain_id: str, last_block: int, last_block_hash: str) -> None:
            self.saved.append((chain_id, last_block, last_block_hash))

    cp = _FakeCheckpointRepo()
    fake_adapter = _FakeSolanaAdapter()

    snap = ConfigSnapshot(version=1, chains=[chain], subscriptions=[], channels=[])

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: fake_adapter,
        channel_factory=lambda _cfg: None,  # type: ignore[arg-type, return-value]
        checkpoint_repo=cp,
    )
    # Inject the Solana parser before start() so the runner's auto-built
    # pipeline picks it up (start() consults `chain.kind`).
    runner._solana_parsers_override = [_FakeSolParser()]  # type: ignore[attr-defined]

    # Mock the notifier so we don't actually open channels.
    class _MockNotifier:
        async def start(self, _channels): pass
        async def stop(self): pass
        async def dispatch(self, event: Event, _hits): dispatched.append(event)

    await runner.start(snap)
    runner._notifier = _MockNotifier()  # type: ignore[assignment]

    # Run one iteration of the head loop manually.
    headers = runner._adapter.subscribe_heads()  # type: ignore[union-attr]
    async for h in headers:
        await runner._handle_head(h)

    assert fake_adapter.connect_calls == 1
    assert isinstance(runner._pipeline, SolanaParserPipeline), (
        "Solana chains must use SolanaParserPipeline, not EvmParserPipeline"
    )
    assert runner._buffer is None, "Solana runner must skip ConfirmationBuffer"
    assert len(dispatched) == 0, (
        "no subscriptions in the snapshot, so notifier.dispatch should not fire"
    )
    assert cp.saved == [("sol", 100, "H100")], (
        "checkpoint must persist after processing the confirmed slot"
    )

    await runner.stop()
    assert fake_adapter.disconnect_calls == 1
```

- [ ] **Step 2: Run, expect FAIL**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer -v`
Expected: FAIL — current `ChainRunner.__init__` hard-builds `EvmParserPipeline([EvmNativeTransferParser(...)])` and instantiates `ConfirmationBuffer` in `start()` unconditionally. Either the `isinstance` check fails or `start()` errors because `cfg.confirmations=0` is fine but the buffer instantiation runs anyway.

- [ ] **Step 3: Branch `ChainRunner.__init__` / `start` / `_handle_head` on `chain.kind`**

Replace `apps/worker/chain_runner.py` body (changes are surgical; full diff below):

```python
# apps/worker/chain_runner.py  --  REPLACE the import block at the top:

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from typing import Protocol

import structlog

from core.abi.registry import AbiRegistry
from core.chains.adapter import ChainAdapter, SolanaChainAdapter
from core.chains.confirmation_buffer import ConfirmationBuffer, ReorgEvent
from core.chains.types import BlockHeader, SolanaBlock
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
)
from core.matcher.matcher import Matcher
from core.notifier.channel import Channel
from core.notifier.notifier import Notifier
from core.parser.abi_call import AbiCallParser
from core.parser.abi_event import AbiEventParser
from core.parser.erc20 import Erc20TransferParser
from core.parser.event import Event
from core.parser.native import EvmNativeTransferParser
from core.parser.pipeline import EvmParserPipeline, SolanaParserPipeline
from core.parser.sol_native import SolNativeTransferParser
```

Then replace `__init__`:

```python
AdapterFactory = Callable[[SnapshotChain], ChainAdapter | SolanaChainAdapter]


class ChainRunner:
    """Owns one chain's pipeline.

    Two physical paths share this class:

    - **EVM** — confirmation buffer + reorg replay (M1 path; unchanged).
    - **Solana** — no buffer; commitment level (`confirmed`/`finalized`)
      handles finality per spec §4.6. The adapter's polling loop already
      filters duplicate slots, and missed-slot `fetch_block(slot)` returning
      `None` is treated as "skip and advance".

    The kind branch is on ``self._chain.kind``; callers (apps.worker.main)
    don't see it.

    Lifecycle is otherwise identical to M1:
      1. ``start(snap)`` — construct adapter (and ``await adapter.connect()``),
         optional confirmation buffer, parser pipeline, matcher, notifier;
         seed ``resume_from`` from the persisted checkpoint.
      2. ``run()`` — drive ``subscribe_heads()`` through the kind-appropriate
         head handler.
      3. ``apply_snapshot(snap)`` — rebuild matcher + notifier in place.
      4. ``stop()`` — cancel listener, drain in-flight notifications (<=30s),
         disconnect adapter.
    """

    DRAIN_TIMEOUT_S = 30.0

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
        self._chain = chain
        self._adapter_factory = adapter_factory
        self._channel_factory = channel_factory
        self._cp = checkpoint_repo
        self._notifier_max_concurrency = notifier_max_concurrency
        self._abi_registry = abi_registry

        self._adapter: ChainAdapter | SolanaChainAdapter | None = None
        self._buffer: ConfirmationBuffer | None = None  # EVM only
        # Pipeline is built in ``start()`` so it can read ``chain.kind``.
        # ``_evm_parsers_override`` / ``_solana_parsers_override`` exist for
        # unit tests; production code does NOT set them.
        self._pipeline: EvmParserPipeline | SolanaParserPipeline | None = None
        self._evm_parsers_override: list | None = None
        self._solana_parsers_override: list | None = None
        self._matcher: Matcher | None = None
        self._notifier: Notifier | None = None
        self._current_snap: ConfigSnapshot | None = None
        self._buffer_tip_hash: str | None = None
        self._stop = asyncio.Event()
        self._snap_lock = asyncio.Lock()
        self.resume_from: tuple[int, str] | None = None

    async def start(self, snap: ConfigSnapshot) -> None:
        self._adapter = self._adapter_factory(self._chain)
        connect = getattr(self._adapter, "connect", None)
        if callable(connect):
            await connect()

        if self._chain.kind == "evm":
            self._buffer = ConfirmationBuffer(confirmations=self._chain.confirmations)
            evm_parsers: list = [
                EvmNativeTransferParser(chain_id=self._chain.id),
                Erc20TransferParser(chain_id=self._chain.id),
            ]
            if self._abi_registry is not None:
                evm_parsers.append(AbiEventParser(chain_id=self._chain.id, registry=self._abi_registry))
                evm_parsers.append(AbiCallParser(chain_id=self._chain.id, registry=self._abi_registry))
            self._pipeline = EvmParserPipeline(
                self._evm_parsers_override or evm_parsers
            )
        elif self._chain.kind == "solana":
            # No buffer for Solana — commitment handles finality (spec §4.6).
            self._buffer = None
            self._pipeline = SolanaParserPipeline(
                self._solana_parsers_override
                or [SolNativeTransferParser(chain_id=self._chain.id)]
            )
        else:
            raise NotImplementedError(f"chain kind {self._chain.kind!r} not supported")

        self.resume_from = await self._cp.get(self._chain.id)
        if self.resume_from is not None:
            log.info(
                "chain_runner.resuming_from_checkpoint",
                chain_id=self._chain.id,
                last_block=self.resume_from[0],
                last_block_hash=self.resume_from[1],
            )
        self._matcher = Matcher(snap)
        self._notifier = Notifier(
            channel_factory=self._channel_factory,
            max_concurrency=self._notifier_max_concurrency,
        )
        await self._notifier.start(snap.channels)
        self._current_snap = snap
```

`apply_snapshot` / `run` / `stop` stay byte-for-byte the same.

Now `_handle_head` becomes a dispatcher; split off the EVM body and add a Solana body:

```python
    async def _handle_head(self, header: BlockHeader) -> None:
        assert self._adapter is not None and self._pipeline is not None
        assert self._matcher is not None and self._notifier is not None
        matcher = self._matcher
        notifier = self._notifier

        if self._chain.kind == "evm":
            await self._handle_head_evm(header, matcher=matcher, notifier=notifier)
        elif self._chain.kind == "solana":
            await self._handle_head_solana(header, matcher=matcher, notifier=notifier)
        else:  # pragma: no cover -- start() already rejects
            raise NotImplementedError(self._chain.kind)

    async def _handle_head_evm(
        self,
        header: BlockHeader,
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        # Body identical to the previous _handle_head except for the rename of
        # `_process_confirmed_block` → `_process_confirmed_evm_block`.
        assert self._buffer is not None and self._adapter is not None

        cache: dict[str, BlockHeader] = {}
        if self._buffer_tip_hash is not None and self._buffer_tip_hash != header.parent_hash:
            cache = await self._prefetch_ancestors_for(header)

        def resolve_parent(n: int, h: str) -> BlockHeader:
            try:
                return cache[h]
            except KeyError as e:
                raise KeyError(f"ancestor {h} at height {n} not in prefetch cache") from e

        result = self._buffer.handle_new_head(header, resolve_parent=resolve_parent)
        self._buffer_tip_hash = header.hash

        confirmed: list[BlockHeader]
        if isinstance(result, ReorgEvent):
            if result.deep:
                log.error(
                    "chain_runner.deep_reorg",
                    chain_id=self._chain.id,
                    divergent_oldest=result.divergent_oldest,
                    new_head=result.new_head.number if result.new_head else None,
                )
            confirmed = result.confirmed
        else:
            confirmed = result

        for h in confirmed:
            await self._process_confirmed_evm_block(
                h.number, matcher=matcher, notifier=notifier
            )

    async def _handle_head_solana(
        self,
        header: BlockHeader,
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        # No buffer. Each polled head is treated as one confirmed slot at the
        # configured commitment. `header.number` is the slot integer (see
        # SolanaAdapter._poll_heads).
        await self._process_confirmed_solana_slot(
            header.number, matcher=matcher, notifier=notifier
        )

    async def _process_confirmed_evm_block(
        self,
        number: int,
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        # Renamed from `_process_confirmed_block`. Body unchanged.
        assert self._adapter is not None and self._pipeline is not None
        block = await self._adapter.fetch_block(number)  # type: ignore[union-attr]
        events = list(self._pipeline.run(block))  # type: ignore[arg-type]
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if not hits:
                continue
            await notifier.dispatch(event, hits)
        await self._cp.save(self._chain.id, block.header.number, block.header.hash)

    async def _process_confirmed_solana_slot(
        self,
        slot: int,
        *,
        matcher: Matcher,
        notifier: Notifier,
    ) -> None:
        assert self._adapter is not None and self._pipeline is not None
        block: SolanaBlock | None = await self._adapter.fetch_block(slot)  # type: ignore[union-attr, assignment]
        if block is None:
            # Missed slot — advance checkpoint to the slot anyway so we don't
            # re-poll the same gap on restart. The empty string is fine for
            # `last_block_hash` because the DDL is `Mapped[str]` with
            # `nullable=False` (see `core/config/models.py:132`); NOT NULL
            # rejects only `None`, not `""`.
            log.info("chain_runner.solana_missed_slot", chain_id=self._chain.id, slot=slot)
            await self._cp.save(self._chain.id, slot, "")
            return
        events: Iterable[Event] = self._pipeline.run(block)  # type: ignore[arg-type]
        for event in events:
            hits = [(sub, chans) for sub, chans in matcher.match(event) if chans]
            if not hits:
                continue
            await notifier.dispatch(event, hits)
        await self._cp.save(self._chain.id, block.slot, block.block_hash)
```

`_prefetch_ancestors_for` stays unchanged (it's EVM-only and is called only from `_handle_head_evm`).

- [ ] **Step 4: Re-run the new test**

Run: `pytest tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer -v`
Expected: PASS.

- [ ] **Step 5: Grep for pre-`start()` `_pipeline` access (regression guard)**

The pipeline moved from `__init__` to `start()`, so any test that touches `runner._pipeline` before awaiting `runner.start(snap)` will now see `None` instead of an `EvmParserPipeline`. Confirm none exist:

```bash
rg -n "runner\._pipeline|self\._pipeline" tests/
```

Expected: every match is either inside `_handle_head_*` / `_process_confirmed_*` (where `start()` has already run) OR an assertion that *follows* a `runner.start(...)` call. If you find a pre-start access, fix that test before continuing — the M1 unit suite must stay green.

- [ ] **Step 6: Re-run the full chain_runner unit suite for regression**

Run: `pytest tests/unit/test_chain_runner.py -v`
Expected: every previous EVM test still passes (the `_handle_head` / `_process_confirmed_block` rename is internal; the EVM path's behavior is byte-for-byte identical).

- [ ] **Step 7: Re-run worker IT for regression**

Run: `pytest tests/integration/test_worker_*.py -v`
Expected: still green.

- [ ] **Step 8: Lint / typecheck**

Run: `make lint typecheck`
Expected: clean. The `# type: ignore[union-attr]` annotations on `fetch_block` call sites are intentional — `ChainAdapter.fetch_block(int) -> Block` and `SolanaChainAdapter.fetch_block(int) -> SolanaBlock | None` have incompatible return types, and we've already narrowed by `chain.kind` at the call site.

- [ ] **Step 9: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(runner): ChainRunner branches on chain.kind (EVM buffer / Solana direct)"
```

### Task 11.5: Integration test — ChainRunner + SolanaAdapter end-to-end

Wires the runner to a real `solana-test-validator`, submits a known lamport transfer with `solders`, and asserts the resulting `Event` reaches a fake `Notifier`.

The validator pre-funds the default identity (`~/.config/solana/id.json` if present; otherwise the validator auto-generates one and exposes it via the faucet at startup). For determinism, this test generates its own keypair, airdrops it via the faucet, then submits a transfer to a second keypair.

**Files:**
- Create: `tests/integration/test_chain_runner_solana.py`.

- [ ] **Step 1: Write the IT**

```python
# tests/integration/test_chain_runner_solana.py
"""End-to-end IT: ChainRunner + SolanaAdapter against solana-test-validator.

Submits a 1_000_000 lamport (0.001 SOL) transfer from a freshly funded
keypair to a second keypair, runs the chain runner for a few seconds,
and asserts a `native_transfer` Event reaches the fake notifier.

Reuses the session-scoped `solana_validator` fixture from
tests/integration/conftest.py (chunk 10).
"""
from __future__ import annotations

import asyncio
import contextlib

import httpx
import pytest
from solders.hash import Hash
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction
from solders.message import Message

from apps.worker.chain_runner import ChainRunner
from core.chains.solana import SolanaAdapter
from core.config.snapshot import (
    ConfigSnapshot,
    SnapshotChain,
    SnapshotChannel,
    SnapshotSubscription,
)
from core.parser.event import Event

pytestmark = [pytest.mark.asyncio]


async def _airdrop(rpc_url: str, recipient: Pubkey, lamports: int) -> None:
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "requestAirdrop",
        "params": [str(recipient), lamports],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(rpc_url, json=body)
    r.raise_for_status()
    assert "result" in r.json(), r.text


async def _latest_blockhash(rpc_url: str) -> str:
    body = {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(rpc_url, json=body)
    r.raise_for_status()
    return r.json()["result"]["value"]["blockhash"]


async def _send_raw_tx(rpc_url: str, signed_tx_b64: str) -> str:
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
        "params": [signed_tx_b64, {"encoding": "base64"}],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(rpc_url, json=body)
    r.raise_for_status()
    return r.json()["result"]


async def _wait_for_signature(rpc_url: str, sig: str, *, timeout_s: float = 25.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout_s
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "getSignatureStatuses",
        "params": [[sig], {"searchTransactionHistory": True}],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        while asyncio.get_event_loop().time() < deadline:
            r = await client.post(rpc_url, json=body)
            statuses = r.json().get("result", {}).get("value", [None])[0]
            if statuses and statuses.get("confirmationStatus") in ("confirmed", "finalized"):
                return
            await asyncio.sleep(0.5)
    raise TimeoutError(f"signature {sig} did not confirm within {timeout_s}s")


class _FakeNotifier:
    def __init__(self) -> None:
        self.dispatched: list[Event] = []

    async def start(self, _channels): pass
    async def stop(self): pass
    async def dispatch(self, event: Event, _hits) -> None:
        self.dispatched.append(event)


class _FakeCheckpointRepo:
    def __init__(self) -> None:
        self.last: tuple[str, int, str] | None = None

    async def get(self, chain_id: str): return None
    async def save(self, chain_id: str, last_block: int, last_block_hash: str):
        self.last = (chain_id, last_block, last_block_hash)


async def test_chain_runner_solana_end_to_end(solana_validator) -> None:
    sender = Keypair()
    recipient = Keypair()

    # Airdrop 0.5 SOL to the sender, wait for confirmation.
    await _airdrop(solana_validator.rpc_url, sender.pubkey(), 500_000_000)
    # Airdrops are usually fast on the local validator but not instant.
    await asyncio.sleep(2.0)

    # Build, sign, and send a 1_000_000 lamport transfer.
    transfer_ix = transfer(
        TransferParams(
            from_pubkey=sender.pubkey(),
            to_pubkey=recipient.pubkey(),
            lamports=1_000_000,
        ),
    )
    blockhash_str = await _latest_blockhash(solana_validator.rpc_url)
    # solders' Message/Transaction require a typed Hash, not the raw base58 str.
    recent_blockhash = Hash.from_string(blockhash_str)
    msg = Message.new_with_blockhash([transfer_ix], sender.pubkey(), recent_blockhash)
    tx = Transaction([sender], msg, recent_blockhash)
    import base64
    raw = base64.b64encode(bytes(tx)).decode()
    sig = await _send_raw_tx(solana_validator.rpc_url, raw)
    await _wait_for_signature(solana_validator.rpc_url, sig)

    # Construct the ChainRunner aimed at the local validator.
    chain = SnapshotChain(
        id="sol-local",
        kind="solana",
        rpc_http=solana_validator.rpc_url,
        rpc_ws=None,
        confirmations=0,
        commitment="confirmed",
        poll_interval_ms=400,
    )
    sub = SnapshotSubscription(
        id="sub1", name="watch-recipient", chain_id="sol-local",
        address=None, abi_id=None,
        match_kind="native_transfer", match_name=None,
        arg_filters={"to": str(recipient.pubkey())},
        enabled=True, channel_ids=["c1"],
    )
    channel = SnapshotChannel(id="c1", name="fake", type="http", config={"url": "http://x"})
    snap = ConfigSnapshot(version=1, chains=[chain], subscriptions=[sub], channels=[channel])

    adapter = SolanaAdapter(
        chain_id="sol-local", rpc_url=solana_validator.rpc_url,
        commitment="confirmed", poll_interval_ms=400,
    )

    notifier = _FakeNotifier()
    cp = _FakeCheckpointRepo()

    # The Notifier created inside `runner.start()` iterates `snap.channels` and
    # calls `channel_factory(cfg)` for each — returning `None` would crash inside
    # `Notifier.start()` when it tries to `await channel.connect()`. Provide a
    # no-op Channel stub so `start()` succeeds; we then swap in `_FakeNotifier`
    # before driving `run()` so dispatch goes to the test's list instead.
    class _StubChannel:
        async def connect(self) -> None: pass
        async def disconnect(self) -> None: pass
        async def send(self, _event: Event) -> None: pass

    runner = ChainRunner(
        chain=chain,
        adapter_factory=lambda _cfg: adapter,
        channel_factory=lambda _cfg: _StubChannel(),  # type: ignore[arg-type, return-value]
        checkpoint_repo=cp,
    )
    await runner.start(snap)
    # Stop the real Notifier started by `start()` so we don't leak its worker
    # pool, then swap in the fake. Safe to do here: no head has been polled yet
    # (run() hasn't been awaited).
    await runner._notifier.stop()  # type: ignore[union-attr]
    runner._notifier = notifier  # type: ignore[assignment]

    # Drive the runner for up to 20 s, polling for at least one matching event.
    async def _drive():
        try:
            await runner.run()
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(_drive())
    deadline = asyncio.get_event_loop().time() + 20.0
    while asyncio.get_event_loop().time() < deadline:
        matches = [
            e for e in notifier.dispatched
            if e.args.get("to") == str(recipient.pubkey())
        ]
        if matches:
            break
        await asyncio.sleep(0.5)

    await runner.stop()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    matches = [
        e for e in notifier.dispatched
        if e.args.get("to") == str(recipient.pubkey())
    ]
    assert matches, (
        f"no native_transfer event matched recipient={recipient.pubkey()!s} "
        f"after airdrop+transfer; got {len(notifier.dispatched)} total events"
    )
    e = matches[0]
    assert e.kind == "native_transfer"
    assert e.args["from"] == str(sender.pubkey())
    assert e.args["value"] == "1000000"
    assert cp.last is not None  # checkpoint persisted
```

- [ ] **Step 2: Run the IT**

```bash
pytest tests/integration/test_chain_runner_solana.py -v
```

Expected on a host with `solana-test-validator` installed: 1 PASS in ~30 s (5–10 s validator boot + 2 s airdrop confirm + ≤20 s runner poll). On a host without the validator: SKIP (the `solana_validator` fixture skips at fixture-entry).

If the IT fails with `BlockhashNotFound`, the airdrop hasn't confirmed yet — increase the `asyncio.sleep(2.0)` between airdrop and transfer to `4.0`. The validator's "default behaviour" is sub-second confirmation but cold-start hosts (especially CI) may need slack.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_chain_runner_solana.py
git commit -m "test(integration): end-to-end ChainRunner + SolanaAdapter native transfer"
```

### Task 11.6: Close-out

- [ ] **Step 1: Full suite**

```bash
make test
```

Expected: every chunk 1–11 unit + IT test passes (Solana ITs SKIP without the validator installed).

- [ ] **Step 2: Lint + typecheck**

```bash
make lint && make typecheck
```

Expected: clean.

- [ ] **Step 3: Final commit (only if cleanup landed)**

```bash
git status
```

If `git status` is empty, this step is a no-op. No tag for chunk 11 — chunk 14 is the M2 close-out tag.

---
## Chunk 12: `SplTransferParser` + chain-aware case-folding

Two M2 deliverables in one chunk:

1. **`SplTransferParser`** — parses SPL Token Program (legacy) and Token-2022 Program `Transfer` (discriminator 3) and `TransferChecked` (discriminator 12) instructions; emits `kind="token_transfer"` with `{from, to, value, mint}`; resolves the mint via the instruction's accounts (TransferChecked) or via `meta.post_token_balances` (Transfer).
2. **Chain-aware case-folding fix in `Matcher` and `filters._norm`** — M1's blanket `.lower()` on subscription addresses and arg values corrupts Solana base58 (case-sensitive). The fix: only lowercase strings that start with `0x` (EVM hex shape). M2 follow-up #11 (deferred from chunk 11) is closed by this task.

Order matters: the matcher fix lands FIRST (Task 12.1) so that Task 12.2's SPL parser tests can use `contract=<base58 mint>` and exercise case-sensitive matching without lower-casing the mint pubkey.

**Why no IT in this chunk:** the end-to-end Solana proof (native + SPL transfers reaching a real webhook) is chunk 14's job. Doing it here would either duplicate chunk 14 or split SPL coverage between two chunks. Keeping chunk 12 unit-test only also keeps it under the 1000-line target.

**Modified files this chunk:**
- `core/matcher/matcher.py` — replace `.lower()` calls with a shape-aware `_norm` helper.
- `core/matcher/filters.py` — same `_norm` change, applied to `eq` and `_in` operators.
- `core/parser/spl_transfer.py` — **new**.
- `apps/worker/chain_runner.py:start` — add `SplTransferParser` to the Solana default parser list (alongside `SolNativeTransferParser`).
- `tests/unit/test_matcher.py` — extend with two Solana-side tests (case-sensitive mint match + base58 arg filter).
- `tests/unit/test_filters.py` — extend with case-sensitivity assertions for base58 strings.
- `tests/unit/test_spl_transfer_parser.py` — **new**.
- `tests/unit/test_chain_runner.py` — extend `test_chain_runner_solana_branch_bypasses_buffer` to assert the runner's default Solana pipeline now includes BOTH `SolNativeTransferParser` and `SplTransferParser`.

**Out of scope this chunk:**
- `transferWithFee` (Token-2022 extension instruction with fee discriminator) — rare in current production traffic; flagged as M2 follow-up #12.
- Multi-signer Transfer (`accounts[2..]` contains multisig signers) — parser still emits one Event; signers are not exposed in `event.args` (they're in `raw` already).
- Approve / Revoke / MintTo / Burn — these would be `kind="event"` or `kind="call"`-style; explicitly out of M2 (spec §4.7).
- Pre-balance/Post-balance reconciliation as a sanity check on the decoded `amount` — the on-chain decoded amount IS the source of truth; adding a reconciliation would just add fragility.

**Naming pin:** `SplTransferParser` (singular). Constants `SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"` and `SPL_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"` live in `core/parser/spl_transfer.py` and are *not* re-exported (no downstream module imports them in M2; chunk 13's `AnchorIdlEventParser` keys on different IDs). Both IDs are 43 base58 chars and decode to valid 32-byte Ed25519 pubkeys — verify with `solders.pubkey.Pubkey.from_string(...)` if you doubt the bytes; a typo here silently mismatches every prod tx.

**Case-folding rule:** A string is "EVM-hex-shaped" iff it `startswith("0x")`. We don't tighten further (e.g., to length 42) because M1 tests deliberately use short `0xAAA`-style hex stubs; tightening would regress unit-test ergonomics. Base58 pubkeys never start with `0x`, so the rule is safe.

### Task 12.1: Chain-aware case-folding in `Matcher` and `filters._norm`

The M1 matcher blindly lowercases every string in `sub.address` / `event.contract` and every comparand inside `filters.evaluate`. That worked for EVM (where addresses are case-insensitive hex per EIP-55) but corrupts Solana base58 pubkeys, which are case-sensitive. The fix is shape-based: lowercase only strings that start with `0x`.

**Files:**
- Modify: `core/matcher/matcher.py` — replace `.lower()` on `sub.address`/`event.contract` with a `_norm` helper.
- Modify: `core/matcher/filters.py:22-24` — tighten `_norm` to skip non-`0x` strings.
- Modify: `tests/unit/test_matcher.py` — add two Solana case-sensitivity tests.
- Modify: `tests/unit/test_filters.py` — add a base58 case-sensitivity assertion to the existing eq and `_in` tests.

- [ ] **Step 1: Write the failing tests (matcher)**

Append to `tests/unit/test_matcher.py`:

```python
def test_address_match_case_sensitive_for_solana_base58() -> None:
    """Base58 pubkeys are case-sensitive — the matcher must NOT lowercase them.

    Reproducer: a Solana subscription with the canonical mint address
    `Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB` (USDC) must NOT match an
    event with `contract='es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb'`.
    Pre-fix, the M1 matcher's `.lower()` collapsed both sides, mis-matching.
    """
    sub = _sub(
        chain_id="sol",
        match_kind="token_transfer",
        address="Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    )
    m = Matcher(_snap([sub], [_ch()]))
    ev = Event(
        chain_id="sol",
        block_number=1, block_hash="H1", block_timestamp=0,
        tx_hash="SIG", tx_index=None, log_index=None,
        kind="token_transfer",
        contract="es9vmfrzacermjfrf4h2fyd4kconky11mcce8benwnyb",  # wrong case
        name=None, args={"to": "B", "value": "1"}, raw={},
    )
    assert list(m.match(ev)) == []


def test_address_match_case_sensitive_match_for_solana_base58() -> None:
    """The same subscription DOES match the canonical-case mint."""
    canonical = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
    sub = _sub(chain_id="sol", match_kind="token_transfer", address=canonical)
    m = Matcher(_snap([sub], [_ch()]))
    ev = Event(
        chain_id="sol",
        block_number=1, block_hash="H1", block_timestamp=0,
        tx_hash="SIG", tx_index=None, log_index=None,
        kind="token_transfer",
        contract=canonical,
        name=None, args={"to": "B", "value": "1"}, raw={},
    )
    assert len(list(m.match(ev))) == 1
```

- [ ] **Step 2: Write the failing tests (filters)**

Append to `tests/unit/test_filters.py`:

```python
def test_eq_string_case_sensitive_for_non_hex() -> None:
    """`{to: "B"}` must NOT match `{to: "b"}` — only EVM-hex strings fold."""
    assert evaluate({"to": "B"}, {"to": "b"}) is False
    assert evaluate({"to": "B"}, {"to": "B"}) is True


def test_eq_string_case_insensitive_for_evm_hex() -> None:
    """Preserve M1 EVM-hex case-folding: `0xABC` == `0xabc`."""
    assert evaluate({"to": "0xABC"}, {"to": "0xabc"}) is True


def test_in_filter_case_sensitive_for_base58() -> None:
    """`{address_in: [<base58>]}` must NOT fold."""
    canonical = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
    lowered = canonical.lower()
    assert evaluate({"address_in": [canonical]}, {"address": lowered}) is False
    assert evaluate({"address_in": [canonical]}, {"address": canonical}) is True
```

(Verify the existing `tests/unit/test_filters.py` has the `evaluate` import at the top — `from core.matcher.filters import evaluate`. If not, add it.)

- [ ] **Step 3: Run both test files, expect FAIL**

```bash
pytest tests/unit/test_matcher.py::test_address_match_case_sensitive_for_solana_base58 \
       tests/unit/test_matcher.py::test_address_match_case_sensitive_match_for_solana_base58 \
       tests/unit/test_filters.py::test_eq_string_case_sensitive_for_non_hex \
       tests/unit/test_filters.py::test_in_filter_case_sensitive_for_base58 -v
```

Expected: the two `case_sensitive` tests FAIL (M1 lowercases both sides, so the mismatched-case event wrongly matches); the two `case_insensitive_match` and `evm_hex` tests PASS even before the fix (they exercise the lowercased path).

- [ ] **Step 4: Implement `_norm` in `core/matcher/filters.py`**

Replace lines 22-24 of `core/matcher/filters.py`:

```python
def _norm(v: Any) -> Any:
    """Equality folding is restricted to EVM-shape hex (starts with ``0x``).

    EVM addresses are case-insensitive per EIP-55 — `0xABC` and `0xabc` are
    the same account. Solana base58 pubkeys (and any other case-sensitive
    string) must NOT be folded; they're returned verbatim.

    The heuristic is conservative: `startswith("0x")` is enough because
    base58 has no leading-zero canonicalization (zero-byte → `1`), so no
    Solana address ever starts with `0x`.
    """
    if isinstance(v, str) and v.startswith("0x"):
        return v.lower()
    return v
```

- [ ] **Step 5: Implement chain-aware comparison in `core/matcher/matcher.py`**

Replace the `_matches` body (lines 45-52):

```python
def _matches(self, sub: SnapshotSubscription, event: Event) -> bool:
    if sub.address is not None:
        if event.contract is None:
            return False
        if _addr_norm(sub.address) != _addr_norm(event.contract):
            return False
    if sub.match_name is not None and event.name != sub.match_name:
        return False
    return evaluate(sub.arg_filters or {}, event.args)
```

And add the module-private helper above the `Matcher` class (right below the imports — the docstring at the top of the helper makes the rule explicit so a future reader doesn't try to "fix" the asymmetry):

```python
def _addr_norm(s: str) -> str:
    """Lowercase EVM-hex addresses; preserve case for everything else.

    See ``core.matcher.filters._norm`` for the same rule applied to arg
    values. Kept separate (rather than importing) to avoid a circular
    import via ``filters``; the rule is tiny enough to duplicate.
    """
    return s.lower() if s.startswith("0x") else s
```

(The `_norm` and `_addr_norm` duplication is intentional and called out in the docstring — `filters` is imported by `matcher`, so re-importing `_norm` from `filters` would be fine, but the matcher comparison is exclusively about addresses while `filters._norm` works on arbitrary arg values. Keeping them separate documents intent.)

- [ ] **Step 6: Re-run the test selection, all four PASS**

```bash
pytest tests/unit/test_matcher.py::test_address_match_case_sensitive_for_solana_base58 \
       tests/unit/test_matcher.py::test_address_match_case_sensitive_match_for_solana_base58 \
       tests/unit/test_filters.py::test_eq_string_case_sensitive_for_non_hex \
       tests/unit/test_filters.py::test_in_filter_case_sensitive_for_base58 -v
```

Expected: 4 PASS.

- [ ] **Step 7: Full matcher + filters regression**

```bash
pytest tests/unit/test_matcher.py tests/unit/test_filters.py -v
```

Expected: every M1 test still passes — the M1 `test_address_match_case_insensitive` uses `0xAAA` / `0xaaa` (still hex-shaped → still folded) and `test_arg_filter_range_applied` doesn't exercise string folding.

- [ ] **Step 8: Lint / typecheck**

Run: `make lint typecheck`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add core/matcher/matcher.py core/matcher/filters.py \
        tests/unit/test_matcher.py tests/unit/test_filters.py
git commit -m "fix(matcher): case-fold EVM hex only; preserve case for Solana base58"
```

### Task 12.2: `SplTransferParser` (Red → Green)

Parses SPL Token Program (legacy + 2022) `Transfer` and `TransferChecked` instructions.

**Encoding reference** (per SPL Token program docs, identical for legacy and 2022):

| Variant | Disc | Data length | Data layout |
|---------|------|-------------|-------------|
| `Transfer` | 3 | 9 bytes | `[u8 disc][u64 LE amount]` |
| `TransferChecked` | 12 | 10 bytes | `[u8 disc][u64 LE amount][u8 decimals]` |

| Variant | Accounts |
|---------|----------|
| `Transfer` | `[source_token_account, dest_token_account, owner_or_multisig, ...optional_signers]` |
| `TransferChecked` | `[source_token_account, mint, dest_token_account, owner_or_multisig, ...optional_signers]` |

For `Transfer`, the mint is NOT in the instruction accounts — it must be looked up from the transaction's `post_token_balances` by finding the `SolanaTokenBalance` whose `account_index` matches the source token account's position in `tx.account_keys`. (`post_token_balances` is the canonical source even though `pre_token_balances` would also work; using post is conventional in indexer code because it survives account creation.)

For `TransferChecked`, the mint is `accounts[1]` directly.

**Stack-depth policy:** the parser accepts BOTH top-level (`stack_depth == 1`) AND inner CPI (`stack_depth >= 2`) SPL transfers. This is intentional and differs from `SolNativeTransferParser` (which top-only): SPL transfers via CPI are first-class user-observable events for indexer use cases (DEX swaps, programs forwarding to users, vault withdrawals all manifest as CPI'd SPL Transfers).

**Failed-tx policy:** same as native — skip if `tx.success is False`.

**Self-transfer policy:** if source == dest token account (rare but legal), emit one Event. The pre/post balances would be identical in that case, but the on-chain instruction did fire, so the indexer reports it.

**Files:**
- Create: `core/parser/spl_transfer.py`.
- Create: `tests/unit/test_spl_transfer_parser.py`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_spl_transfer_parser.py
"""SplTransferParser:
- decodes Transfer (disc 3) with mint resolved from post_token_balances
- decodes TransferChecked (disc 12) with mint from accounts[1]
- recognizes BOTH legacy Token Program and Token-2022 Program IDs
- ignores instructions on non-Token programs
- emits inner-CPI transfers (stack_depth >= 2) — they're first-class
- skips failed transactions
- ignores malformed payloads (wrong length, wrong discriminator)
- if Transfer's source account isn't in post_token_balances, the parser
  drops the instruction silently (rare — only happens if the indexer is
  served a partial block by the RPC node)
"""
from __future__ import annotations

import struct

import base58

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)
from core.parser.spl_transfer import (
    SPL_TOKEN_2022_PROGRAM_ID,
    SPL_TOKEN_PROGRAM_ID,
    SplTransferParser,
)


def _transfer_data(amount: int) -> str:
    """Transfer (disc 3): [u8 3][u64 LE amount]."""
    return base58.b58encode(b"\x03" + struct.pack("<Q", amount)).decode()


def _transfer_checked_data(amount: int, decimals: int) -> str:
    """TransferChecked (disc 12): [u8 12][u64 LE amount][u8 decimals]."""
    return base58.b58encode(b"\x0c" + struct.pack("<Q", amount) + bytes([decimals])).decode()


def _tb(account_index: int, mint: str, amount: int, decimals: int = 6) -> SolanaTokenBalance:
    return SolanaTokenBalance(
        account_index=account_index, mint=mint, owner=None,
        amount=amount, decimals=decimals,
    )


def _tx(
    *,
    signature: str = "SIG",
    success: bool = True,
    account_keys: list[str] | None = None,
    post_token_balances: list[SolanaTokenBalance] | None = None,
    instructions: list[SolanaInstruction] | None = None,
) -> SolanaTransaction:
    return SolanaTransaction(
        signature=signature, slot=100, success=success, fee=5000,
        account_keys=account_keys or ["SRC", "DST", "OWNER", "TOKEN_PROG"],
        pre_balances=[0, 0, 10**9, 0],
        post_balances=[0, 0, 10**9 - 5000, 0],
        pre_token_balances=[],
        post_token_balances=post_token_balances or [],
        log_messages=[],
        instructions=instructions or [],
    )


def _block(txs: list[SolanaTransaction]) -> SolanaBlock:
    return SolanaBlock(
        slot=100, block_hash="H100", parent_slot=99,
        block_time=1_700_000_000, transactions=txs,
    )


def test_decodes_transfer_with_mint_from_post_token_balances() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(123_456_789),
        stack_depth=1,
    )
    tx = _tx(
        instructions=[ix],
        post_token_balances=[_tb(account_index=0, mint="MintA", amount=1)],
    )
    [event] = list(SplTransferParser(chain_id="sol").parse(_block([tx])))
    assert event.kind == "token_transfer"
    assert event.contract == "MintA"
    assert event.args == {
        "from": "SRC", "to": "DST", "value": "123456789", "mint": "MintA",
    }
    assert event.tx_hash == "SIG"
    assert event.block_number == 100


def test_decodes_transfer_checked_with_mint_from_accounts() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "MintB", "DST", "OWNER"],
        data_b58=_transfer_checked_data(1_000_000, decimals=6),
        stack_depth=1,
    )
    tx = _tx(
        account_keys=["SRC", "MintB", "DST", "OWNER", "TOKEN_PROG"],
        instructions=[ix],
        # Deliberately NO post_token_balances — TransferChecked doesn't need them.
        post_token_balances=[],
    )
    [event] = list(SplTransferParser(chain_id="sol").parse(_block([tx])))
    assert event.contract == "MintB"
    assert event.args == {
        "from": "SRC", "to": "DST", "value": "1000000", "mint": "MintB",
    }


def test_recognizes_token_2022_program_id() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_2022_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(42),
        stack_depth=1,
    )
    tx = _tx(
        instructions=[ix],
        post_token_balances=[_tb(0, "Mint2022", 1)],
    )
    [event] = list(SplTransferParser(chain_id="sol").parse(_block([tx])))
    assert event.contract == "Mint2022"
    assert event.args["value"] == "42"


def test_ignores_non_token_programs() -> None:
    ix = SolanaInstruction(
        program_id="11111111111111111111111111111111",  # System Program
        accounts=["A", "B", "C"],
        data_b58=_transfer_data(99),
        stack_depth=1,
    )
    tx = _tx(instructions=[ix])
    assert list(SplTransferParser(chain_id="sol").parse(_block([tx]))) == []


def test_emits_inner_cpi_transfer() -> None:
    """Inner-CPI SPL transfers (stack_depth >= 2) ARE first-class for SPL."""
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(10),
        stack_depth=3,  # nested two programs deep
    )
    tx = _tx(
        instructions=[ix],
        post_token_balances=[_tb(0, "MintA", 1)],
    )
    [event] = list(SplTransferParser(chain_id="sol").parse(_block([tx])))
    assert event.args["value"] == "10"


def test_skips_failed_transactions() -> None:
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(1),
        stack_depth=1,
    )
    tx = _tx(
        success=False,
        instructions=[ix],
        post_token_balances=[_tb(0, "MintA", 1)],
    )
    assert list(SplTransferParser(chain_id="sol").parse(_block([tx]))) == []


def test_ignores_malformed_transfer_payload() -> None:
    """Length != 9 for Transfer or != 10 for TransferChecked → silent skip."""
    short = base58.b58encode(b"\x03\x00").decode()  # disc 3 but only 2 bytes
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=short,
        stack_depth=1,
    )
    tx = _tx(
        instructions=[ix],
        post_token_balances=[_tb(0, "MintA", 1)],
    )
    assert list(SplTransferParser(chain_id="sol").parse(_block([tx]))) == []


def test_drops_transfer_when_source_account_missing_from_post_token_balances() -> None:
    """Transfer (disc 3) without a matching post_token_balances entry → skip."""
    ix = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(7),
        stack_depth=1,
    )
    # post_token_balances references a DIFFERENT account_index (1, not 0).
    tx = _tx(
        instructions=[ix],
        post_token_balances=[_tb(account_index=1, mint="MintX", amount=1)],
    )
    assert list(SplTransferParser(chain_id="sol").parse(_block([tx]))) == []


def test_emits_one_event_per_instruction_within_a_tx() -> None:
    ix1 = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(100),
        stack_depth=1,
    )
    ix2 = SolanaInstruction(
        program_id=SPL_TOKEN_PROGRAM_ID,
        accounts=["SRC", "DST", "OWNER"],
        data_b58=_transfer_data(200),
        stack_depth=1,
    )
    tx = _tx(
        instructions=[ix1, ix2],
        post_token_balances=[_tb(0, "MintA", 1)],
    )
    events = list(SplTransferParser(chain_id="sol").parse(_block([tx])))
    assert [e.args["value"] for e in events] == ["100", "200"]
```

- [ ] **Step 2: Run, expect FAIL on import**

```bash
pytest tests/unit/test_spl_transfer_parser.py -v
```

Expected: `ImportError: cannot import name 'SplTransferParser' from 'core.parser.spl_transfer'`.

(If you see `ModuleNotFoundError: No module named 'base58'`, chunk 11 Task 11.3 Step 1 was skipped — `base58` is already a project dep from that chunk. Verify with `python -c "import base58"`.)

- [ ] **Step 3: Implement `SplTransferParser`**

```python
# core/parser/spl_transfer.py
"""SplTransferParser: parses SPL Token Program (legacy + 2022) Transfer
and TransferChecked instructions.

Per spec §4.7, the emitted Event shape is `kind="token_transfer"`,
`contract=<mint base58>`, `args={"from", "to", "value", "mint"}`. This
mirrors the EVM `Erc20TransferParser` field set so the matcher's
`arg_filters` are chain-portable across `to`/`from`/`value`.

Mint resolution:
- TransferChecked (disc 12) — mint is `instruction.accounts[1]` directly.
- Transfer (disc 3)         — mint is looked up in `tx.post_token_balances`
                              by matching `account_index` against the source
                              account's position in `tx.account_keys`. If
                              the source isn't found, the instruction is
                              dropped silently (RPC partial-block edge case).

Stack depth:
- Both top-level and inner-CPI SPL transfers are emitted (unlike System
  Program transfers, which we restrict to top-level only). SPL transfers
  through CPI ARE the usual interaction shape for DEX swaps, vault
  withdrawals, etc. — first-class for indexer use cases.
"""
from __future__ import annotations

import struct
from collections.abc import Iterable

import base58
import structlog

from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTransaction,
)
from core.parser.event import Event

log = structlog.get_logger(__name__)


SPL_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
SPL_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_TOKEN_PROGRAM_IDS = frozenset({SPL_TOKEN_PROGRAM_ID, SPL_TOKEN_2022_PROGRAM_ID})

_TRANSFER_DISC = 3
_TRANSFER_CHECKED_DISC = 12


class SplTransferParser:
    def __init__(self, chain_id: str) -> None:
        self._chain_id = chain_id

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            for ix in tx.instructions:
                if ix.program_id not in _TOKEN_PROGRAM_IDS:
                    continue
                event = self._maybe_decode(block, tx, ix)
                if event is not None:
                    yield event

    def _maybe_decode(
        self,
        block: SolanaBlock,
        tx: SolanaTransaction,
        ix: SolanaInstruction,
    ) -> Event | None:
        try:
            raw = base58.b58decode(ix.data_b58)
        except ValueError:
            return None
        if not raw:
            return None
        disc = raw[0]
        if disc == _TRANSFER_DISC:
            return self._decode_transfer(block, tx, ix, raw)
        if disc == _TRANSFER_CHECKED_DISC:
            return self._decode_transfer_checked(block, tx, ix, raw)
        return None

    def _decode_transfer(
        self,
        block: SolanaBlock,
        tx: SolanaTransaction,
        ix: SolanaInstruction,
        raw: bytes,
    ) -> Event | None:
        if len(raw) != 9 or len(ix.accounts) < 3:
            return None
        (amount,) = struct.unpack("<Q", raw[1:9])
        src = ix.accounts[0]
        dst = ix.accounts[1]
        mint = _resolve_mint_from_balances(tx, src)
        if mint is None:
            # RPC served a tx without post_token_balances coverage for this
            # source — rare, but skipping is safer than emitting a half-event.
            return None
        return self._build_event(block, tx, src=src, dst=dst, amount=amount, mint=mint)

    def _decode_transfer_checked(
        self,
        block: SolanaBlock,
        tx: SolanaTransaction,
        ix: SolanaInstruction,
        raw: bytes,
    ) -> Event | None:
        if len(raw) != 10 or len(ix.accounts) < 4:
            return None
        (amount,) = struct.unpack("<Q", raw[1:9])
        # raw[9] is `decimals`; we ignore it (the mint's on-chain decimals is
        # authoritative; the value here is just a cross-check the program does).
        src = ix.accounts[0]
        mint = ix.accounts[1]
        dst = ix.accounts[2]
        return self._build_event(block, tx, src=src, dst=dst, amount=amount, mint=mint)

    def _build_event(
        self,
        block: SolanaBlock,
        tx: SolanaTransaction,
        *,
        src: str,
        dst: str,
        amount: int,
        mint: str,
    ) -> Event:
        return Event(
            chain_id=self._chain_id,
            block_number=block.slot,
            block_hash=block.block_hash,
            block_timestamp=block.block_time or 0,
            tx_hash=tx.signature,
            tx_index=None,
            log_index=None,
            kind="token_transfer",
            contract=mint,
            name=None,
            args={
                "from": src,
                "to": dst,
                "value": str(amount),
                "mint": mint,
            },
            raw={"signature": tx.signature, "slot": block.slot},
        )


def _resolve_mint_from_balances(tx: SolanaTransaction, source_account: str) -> str | None:
    """Find the mint for `source_account` by looking up its index in
    `tx.account_keys` and matching that against `tx.post_token_balances`."""
    try:
        idx = tx.account_keys.index(source_account)
    except ValueError:
        return None
    for tb in tx.post_token_balances:
        if tb.account_index == idx:
            return tb.mint
    return None
```

- [ ] **Step 4: Re-run the tests**

```bash
pytest tests/unit/test_spl_transfer_parser.py -v
```

Expected: 9 PASS.

- [ ] **Step 5: Lint / typecheck**

```bash
make lint typecheck
```

Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add core/parser/spl_transfer.py tests/unit/test_spl_transfer_parser.py
git commit -m "feat(parser): SplTransferParser (legacy Token Program + Token-2022)"
```

### Task 12.3: Wire `SplTransferParser` into `ChainRunner`'s default Solana pipeline

Adds `SplTransferParser(chain_id=self._chain.id)` to the Solana parser list in `ChainRunner.start()`, so that every Solana-kind chain runs BOTH the native parser (chunk 11) and the SPL parser (chunk 12) out of the box.

**Files:**
- Modify: `apps/worker/chain_runner.py:start` — extend the Solana parser list.
- Modify: `tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer` — assert both default parsers are present.

- [ ] **Step 1: Tighten the existing chunk-11 chain_runner test**

In `tests/unit/test_chain_runner.py`, find `test_chain_runner_solana_branch_bypasses_buffer` (added in chunk 11 Task 11.4). Make two edits:

**(a)** Replace the line:

```python
# Inject the Solana parser before start() so the runner's auto-built
# pipeline picks it up (start() consults `chain.kind`).
runner._solana_parsers_override = [_FakeSolParser()]  # type: ignore[attr-defined]
```

with:

```python
# Leave `_solana_parsers_override = None` so `start()` builds the DEFAULT
# Solana parser list. We then assert that list is BOTH SolNativeTransferParser
# and SplTransferParser (Task 12.3 wiring).
```

**(b)** Delete the now-orphaned `class _FakeSolParser:` block defined inside the same test function (chunk 11 Task 11.4 Step 4 added it). With the override removed in (a) it has no remaining references and would linger as dead code. The exact block to delete is — verbatim from chunk 11:

```python
class _FakeSolParser:
    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        yield Event(
            chain_id="sol", block_number=block.slot, block_hash=block.block_hash,
            block_timestamp=block.block_time or 0,
            tx_hash="SIG", tx_index=None, log_index=None,
            kind="native_transfer", contract=None, name=None,
            args={"from": "A", "to": "B", "value": "1000"}, raw={},
        )
```

The class sits immediately above the `dispatched: list[Event] = []` line. Delete the entire `class _FakeSolParser:` definition (including its `parse` method and the `yield Event(...)` body). After deletion, the next line in the test function should be `dispatched: list[Event] = []`.

If — and ONLY if — the `Event` and/or `Iterable` and/or `SolanaBlock` imports at the top of `tests/unit/test_chain_runner.py` are now unused, also remove them. (`Event` is likely still used by the `dispatched` type annotation; `Iterable` and `SolanaBlock` may be removable. Let `ruff`/`make lint` flag what's actually unused.)

And add — AFTER `await runner.start(snap)` — these assertions (right above the existing `assert fake_adapter.connect_calls == 1`):

```python
from core.parser.sol_native import SolNativeTransferParser
from core.parser.spl_transfer import SplTransferParser

assert runner._pipeline is not None
pipeline_parsers = runner._pipeline._parsers  # type: ignore[attr-defined]
parser_types = {type(p) for p in pipeline_parsers}
assert SolNativeTransferParser in parser_types, (
    "default Solana pipeline must include SolNativeTransferParser"
)
assert SplTransferParser in parser_types, (
    "default Solana pipeline must include SplTransferParser"
)
```

Also adjust the `dispatched` assertion at the bottom — without the `_FakeSolParser` injection, no event will be produced from the empty `instructions=[]` test transaction, so:

```python
assert len(dispatched) == 0, (
    "no SPL/System Program instructions in the test tx AND no subscriptions "
    "in the snapshot — either alone would zero dispatched"
)
```

(replaces the previous "no subscriptions in the snapshot" wording — still accurate, but now the test exercises the real default parser list).

- [ ] **Step 2: Run the test, expect FAIL**

```bash
pytest tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer -v
```

Expected: FAIL — `SplTransferParser not in parser_types`. The chunk-11 `start()` only constructs `SolNativeTransferParser`.

- [ ] **Step 3: Extend `start()` in `apps/worker/chain_runner.py`**

Locate the Solana branch in `ChainRunner.start()` (added in chunk 11 Task 11.4). Replace:

```python
elif self._chain.kind == "solana":
    # No buffer for Solana — commitment handles finality (spec §4.6).
    self._buffer = None
    self._pipeline = SolanaParserPipeline(
        self._solana_parsers_override
        or [SolNativeTransferParser(chain_id=self._chain.id)]
    )
```

with:

```python
elif self._chain.kind == "solana":
    # No buffer for Solana — commitment handles finality (spec §4.6).
    self._buffer = None
    self._pipeline = SolanaParserPipeline(
        self._solana_parsers_override
        or [
            SolNativeTransferParser(chain_id=self._chain.id),
            SplTransferParser(chain_id=self._chain.id),
        ]
    )
```

And add to the imports at the top of `apps/worker/chain_runner.py`:

```python
from core.parser.spl_transfer import SplTransferParser
```

(Keep the imports alphabetized in the parser block: `native`, `pipeline`, `sol_native`, `spl_transfer`.)

- [ ] **Step 4: Re-run the test**

```bash
pytest tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer -v
```

Expected: PASS.

- [ ] **Step 5: Full chain_runner regression**

```bash
pytest tests/unit/test_chain_runner.py -v
```

Expected: every EVM test still green; the Solana test PASSes with the new assertions.

- [ ] **Step 6: Lint / typecheck**

```bash
make lint typecheck
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(runner): include SplTransferParser in default Solana parser list"
```

### Task 12.4: Close-out

- [ ] **Step 1: Full suite**

```bash
make test
```

Expected: every chunk 1–12 unit + IT test passes. Solana ITs SKIP on hosts without `solana-test-validator`.

- [ ] **Step 2: Lint + typecheck**

```bash
make lint && make typecheck
```

Expected: clean.

- [ ] **Step 3: M2 follow-up bookkeeping**

Open `docs/superpowers/plans/2026-05-26-m2-implementation.md`, find the "M2 follow-ups" list (at the top of the plan or in chunk 1, wherever it lives), and mark follow-up #11 (matcher case-folding for base58) as **DONE**: Task 12.1 closed it. If a `transferWithFee` line doesn't exist as #12, add it now (one-line entry).

Run:

```bash
rg -n "^- \[ \].*matcher.*lower\(\)" docs/
rg -n "^- \[x\].*matcher.*case-folding" docs/
```

Expected: the first returns nothing (or only mentions that have nothing to do with the bug); the second returns the bookkeeping line you just toggled. (If the follow-ups list isn't checkbox-styled, just edit the prose to read "✅ M2 c12".)

- [ ] **Step 4: Commit any bookkeeping edits**

```bash
git status
```

If `git status` shows the plan file modified, commit:

```bash
git add docs/superpowers/plans/2026-05-26-m2-implementation.md
git commit -m "docs(plan): mark M2 follow-up #11 (matcher case-folding) DONE"
```

If `git status` is empty, this step is a no-op. No tag for chunk 12 — chunk 14 is the M2 close-out tag.

---
## Chunk 13: `AnchorIdlEventParser` + `borsh-construct` decoder path

The third (and last) Solana parser ships. It decodes Anchor IDL events emitted via the `emit!` macro, which surface in transaction logs as `Program data: <base64>` lines. The 8-byte discriminator at the head of each payload selects the event schema; the registry resolves `(program_id, discriminator) → (abi_id, event_name, schema)` so the parser borsh-decodes the body using the right struct. The remaining tasks wire the new parser into `ChainRunner.start()` so every Solana-kind chain runs all three Solana parsers out of the box once an `AbiRegistry` exists.

**Why a new registry index instead of reusing the topic0 index from chunk 4?** EVM `topic0` is a 32-byte keccak hash, globally unique-ish across ABIs; Solana Anchor discriminators are an 8-byte sha256 prefix (`sha256("event:<EventName>")[:8]`), which is short enough that two unrelated programs can collide by accident. Spec §6 (row "Anchor IDL discriminator collision") resolves this by scoping every lookup by the *emitting* program ID — recovered at decode time from the surrounding `Program <pid> invoke [N]` / `Program <pid> success` log frame. The new index therefore keys on `(program_id_b58, discriminator_hex)`.

**Anchor IDL ↔ program_id binding.** An Anchor IDL JSON has `metadata.address` set to the program's deployed pubkey. The index build reads `body["metadata"]["address"]`; IDLs missing it are logged and skipped.

**Scope-limiting type support.** Chunk 13's borsh schema covers IDL fields of type: `bool`, `u8`/`u16`/`u32`/`u64`/`u128`, `i8`/`i16`/`i32`/`i64`/`i128`, `bytes`, `string`, `pubkey` (and the older alias `publicKey`). Nested `{defined: ...}`, vec/option/array are explicitly **out of scope**: any IDL event using them is logged-and-skipped at index-build time and never reaches `borsh-construct`. M3 can extend the synthesizer.

**Files (this chunk):**
- Modify: `pyproject.toml` — add `borsh-construct>=0.1.0,<0.2`.
- Modify: `core/abi/decoder.py` — add three Anchor helpers.
- Modify: `core/abi/registry.py` — `(program_id, discriminator)` index + `lookup_idl_event_by_discriminator()`.
- Create: `core/parser/anchor_event.py` — `AnchorIdlEventParser(chain_id, registry)`.
- Modify: `apps/worker/chain_runner.py` — extend Solana branch in `start()`.
- Create: `tests/unit/test_anchor_event_decoder.py`.
- Create: `tests/unit/test_anchor_event_parser.py`.
- Modify: `tests/unit/test_abi_registry.py` — append IDL tests.
- Modify: `tests/unit/test_chain_runner.py` — extend the existing Solana branch test.

### Naming + constants pin (read once, refer often)

| Symbol | Value |
|--------|-------|
| Anchor event discriminator | `sha256(f"event:{event_name}".encode())[:8]` (8 bytes; `emit!` prefix) |
| `Program data:` log line | `"Program data: <base64>"` — one space, no quotes |
| `Program ... invoke` line | `"Program <base58 pubkey> invoke [<depth>]"` (1-indexed) |
| `Program ... success` line | `"Program <base58 pubkey> success"` (closes most-recent matching frame) |
| IDL → program_id | `body["metadata"]["address"]` (Anchor IDL convention) |
| Supported scalar types | `bool`, `u8`–`u128`, `i8`–`i128`, `bytes`, `string`, `pubkey`/`publicKey` |
| Pubkey on-wire size | 32 bytes (decoded as fixed bytes; converted to base58 for `args`) |
| Success `Event.kind` | `"event"` (matches `AbiEventParser`; post-§4.9 rename) |
| Failure-downgrade `Event.kind` | `"event"` with `args={}` and `raw={"program_data", "program_id", "discriminator"}` |
| Unknown program_id or discriminator | **Skip silently** — emit nothing |

---

### Task 13.1: Add `borsh-construct` dep + Anchor IDL decoder helpers

Adds the `borsh-construct` Python package as a project dependency and lays down three module-level helpers in `core/abi/decoder.py` that the registry and the parser will reuse:

- `anchor_event_discriminator(event_name: str) -> bytes` — pure function, computes the 8-byte sha256 prefix.
- `build_anchor_event_struct(idl_event: dict[str, Any]) -> Construct` — synthesizes a `borsh_construct.CStruct` from an IDL event entry, returning `None` if any field type is out of scope.
- `decode_anchor_event(struct: Construct, body_bytes: bytes) -> dict[str, Any]` — wraps `struct.parse(body_bytes)` and translates exceptions into `DecodeFailed`, plus converts `pubkey` fields (raw 32-byte bytes) into base58 strings for `Event.args`.

**Files:**
- Modify: `pyproject.toml` — add `borsh-construct>=0.1.0,<0.2` to `[project].dependencies`.
- Modify: `core/abi/decoder.py` — append the three helpers and the unsupported-type guard.
- Create: `tests/unit/test_anchor_event_decoder.py` — 6 unit tests.

- [ ] **Step 1: Add `borsh-construct` to `pyproject.toml`**

Open `pyproject.toml` and add to the `dependencies` array under `[project]` (keep the list alphabetized to match the existing style):

```toml
    "borsh-construct>=0.1.0,<0.2",
```

The other Solana-side dep (`base58`) was added in chunk 11 Task 11.3 Step 1; `borsh-construct` is the second Solana-side runtime dep introduced by M2 and the last one — the Solana E2E in chunk 14 needs only a CLI binary (`solana-test-validator`), not a new Python dep.

Commit this in isolation before writing any tests so the dep is available to install when `make install` runs in CI for the test commits that follow:

```bash
make install
git add pyproject.toml
git commit -m "feat(deps): add borsh-construct for Anchor IDL event decoding"
```

`make install` is expected to PASS (resolves `borsh-construct` from PyPI; pulls the tiny `construct` C-extension wheel as a transitive dep).

- [ ] **Step 2: Write the failing decoder tests**

Create `tests/unit/test_anchor_event_decoder.py`:

```python
from __future__ import annotations

import hashlib

import pytest

from core.abi.decoder import (
    anchor_event_discriminator,
    build_anchor_event_struct,
    decode_anchor_event,
)
from core.abi.errors import DecodeFailed


# A small fixture IDL event entry covering every supported scalar type.
_IDL_EVENT_FULL = {
    "name": "TradeExecuted",
    "fields": [
        {"name": "trader", "type": "pubkey"},
        {"name": "side", "type": "u8"},
        {"name": "size", "type": "u64"},
        {"name": "price", "type": "u128"},
        {"name": "memo", "type": "string"},
        {"name": "is_market", "type": "bool"},
        {"name": "raw_blob", "type": "bytes"},
    ],
}


def test_anchor_event_discriminator_is_sha256_prefix() -> None:
    name = "TradeExecuted"
    expected = hashlib.sha256(f"event:{name}".encode()).digest()[:8]
    assert anchor_event_discriminator(name) == expected
    # And the prefix is exactly 8 bytes — Anchor's wire format.
    assert len(anchor_event_discriminator(name)) == 8


def test_build_struct_returns_none_for_unsupported_field_type() -> None:
    # `{defined: "..."}` requires the type registry from IDL `types[]`, which is
    # explicitly out of scope for chunk 13.
    idl = {
        "name": "Compound",
        "fields": [{"name": "inner", "type": {"defined": "InnerStruct"}}],
    }
    assert build_anchor_event_struct(idl) is None


def test_build_struct_returns_none_for_vec_field_type() -> None:
    idl = {
        "name": "BatchEmitted",
        "fields": [{"name": "items", "type": {"vec": "u64"}}],
    }
    assert build_anchor_event_struct(idl) is None


def test_round_trip_decode_full_scalar_event() -> None:
    struct = build_anchor_event_struct(_IDL_EVENT_FULL)
    assert struct is not None

    # Build a sample payload by hand using `struct.build` so we know the bytes
    # are valid borsh, then re-parse via the helper to verify the decoder.
    trader_pubkey_bytes = bytes(range(32))  # arbitrary 32-byte pubkey
    payload = struct.build({
        "trader": trader_pubkey_bytes,
        "side": 1,
        "size": 12345,
        "price": 9876543210123456789,
        "memo": "spot fill",
        "is_market": True,
        "raw_blob": b"\xde\xad\xbe\xef",
    })

    decoded = decode_anchor_event(struct, payload)

    # `pubkey` fields must be base58-encoded for the Event.args dict.
    import base58
    assert decoded["trader"] == base58.b58encode(trader_pubkey_bytes).decode()
    assert decoded["side"] == 1
    assert decoded["size"] == 12345
    assert decoded["price"] == 9876543210123456789
    assert decoded["memo"] == "spot fill"
    assert decoded["is_market"] is True
    assert decoded["raw_blob"] == "deadbeef"  # bytes → hex string


def test_decode_raises_on_short_payload() -> None:
    struct = build_anchor_event_struct(_IDL_EVENT_FULL)
    assert struct is not None
    with pytest.raises(DecodeFailed):
        decode_anchor_event(struct, b"\x00\x01")  # nowhere near the right size


def test_decode_raises_on_garbage_string_length() -> None:
    # Build a half-formed payload: valid pubkey + side + size + price but then
    # a string-length prefix that exceeds the remaining bytes.
    struct = build_anchor_event_struct(_IDL_EVENT_FULL)
    assert struct is not None
    bad = (
        bytes(range(32))      # trader
        + b"\x01"             # side
        + (12345).to_bytes(8, "little")   # size
        + (1).to_bytes(16, "little")      # price
        + (10**6).to_bytes(4, "little")   # memo length = 1_000_000 (way too big)
        + b"hi"               # only 2 bytes of memo
    )
    with pytest.raises(DecodeFailed):
        decode_anchor_event(struct, bad)
```

- [ ] **Step 3: Run the failing tests**

```bash
pytest tests/unit/test_anchor_event_decoder.py -v
```

Expected: ImportError on the three `core.abi.decoder` symbols (`anchor_event_discriminator`, `build_anchor_event_struct`, `decode_anchor_event`). They don't exist yet.

- [ ] **Step 4: Implement the helpers**

Append to `core/abi/decoder.py` (after the existing EVM helpers — keep the EVM block first since chunks 2–5 land first chronologically):

```python
# -------------------------------------------------------------------------
# Solana Anchor IDL decoders (added in chunk 13)
#
# Used by `AnchorIdlEventParser` and the registry's IDL-event index.
# Scope: see the "Naming + constants pin" in the plan — scalar types only;
# nested IDL types defer to M3.
# -------------------------------------------------------------------------
import hashlib
from typing import Any  # if not already imported at the top of the file

import base58
from borsh_construct import (
    Bool,
    Bytes,
    I8, I16, I32, I64, I128,
    String,
    U8, U16, U32, U64, U128,
    CStruct,
)
from construct import Bytes as _CBytes  # 32-byte fixed-length for pubkey


_PUBKEY_BYTES = _CBytes(32)

_SCALAR_TYPES: dict[str, Any] = {
    "bool": Bool,
    "u8": U8, "u16": U16, "u32": U32, "u64": U64, "u128": U128,
    "i8": I8, "i16": I16, "i32": I32, "i64": I64, "i128": I128,
    "string": String,
    "bytes": Bytes,
    "pubkey": _PUBKEY_BYTES,
    "publicKey": _PUBKEY_BYTES,  # older IDLs use the camelCase alias
}


def anchor_event_discriminator(event_name: str) -> bytes:
    """Return the 8-byte Anchor event discriminator for ``event_name``.

    Algorithm: ``sha256(f"event:{event_name}").digest()[:8]``.
    This is the exact prefix Anchor's `emit!` macro lays down before the
    borsh-encoded event payload.
    """
    return hashlib.sha256(f"event:{event_name}".encode()).digest()[:8]


def build_anchor_event_struct(idl_event: dict[str, Any]) -> CStruct | None:
    """Synthesize a borsh-construct schema from an IDL event entry.

    Returns ``None`` if any field's type falls outside the chunk-13 scope
    (defined types, vec/option/array — see the plan's scope decision).
    Callers should log-and-skip when this returns ``None``.
    """
    fields = []
    for field in idl_event.get("fields", []):
        ftype = field.get("type")
        if not isinstance(ftype, str):
            # `{defined: "..."}`, `{vec: ...}`, `{option: ...}`, `{array: [...]}` — all out of scope.
            return None
        ctor = _SCALAR_TYPES.get(ftype)
        if ctor is None:
            return None
        fields.append(field["name"] / ctor)
    return CStruct(*fields)


def decode_anchor_event(struct: CStruct, body_bytes: bytes) -> dict[str, Any]:
    """Decode an Anchor event body using ``struct`` and post-process for JSON.

    Post-processing:
      * `pubkey` fields (32-byte raw bytes) → base58 strings, matching the
        format used by the rest of the Solana parsers (`account_keys` etc.).
      * `bytes` fields → hex strings, since bytes don't round-trip through
        JSON without a transform.

    Raises ``DecodeFailed`` on any borsh-construct exception.
    """
    try:
        parsed = struct.parse(body_bytes)
    except Exception as exc:  # noqa: BLE001 — borsh-construct raises many concrete types
        raise DecodeFailed(f"anchor borsh decode failed: {exc}") from exc

    out: dict[str, Any] = {}
    for key in parsed.keys():
        if key.startswith("_"):  # construct internal fields
            continue
        val = parsed[key]
        if isinstance(val, (bytes, bytearray)):
            # Pubkey fields are exactly 32 bytes; everything else (`bytes` IDL
            # type) is variable length. We use the length as a heuristic so
            # the caller doesn't have to thread the IDL field type through.
            if len(val) == 32:
                out[key] = base58.b58encode(bytes(val)).decode()
            else:
                out[key] = bytes(val).hex()
        else:
            out[key] = val
    return out
```

Also extend the existing module-level docstring so the next reader knows this file now covers two chains:

```python
"""ABI decoders for EVM (eth-abi) and Solana Anchor IDL (borsh-construct).

The EVM half lands in chunk 2 (Task 2.6); the Solana half lands in chunk 13.
Both halves expose pure functions — the registry layer owns caching and
the parsers own log-line walking.
"""
```

(Replace the original "EVM ABI decoders ... Solana Anchor IDL decoders land in chunk 13" docstring from chunk 2 Task 2.6 with this updated version — keeping the file's preamble accurate.)

- [ ] **Step 5: Re-run the decoder tests**

```bash
pytest tests/unit/test_anchor_event_decoder.py -v
```

Expected: 6 PASS.

- [ ] **Step 6: Lint + typecheck**

```bash
make lint typecheck
```

Expected: clean. (`ruff` may warn about the `import base58` and `import hashlib` being at module body bottom rather than top — if so, hoist them to join the other imports at the top of `core/abi/decoder.py`. The placement shown in Step 4 is purely for diff readability in this plan.)

- [ ] **Step 7: Commit**

```bash
git add core/abi/decoder.py tests/unit/test_anchor_event_decoder.py
git commit -m "feat(abi): Anchor IDL event decoder helpers (borsh-construct)"
```

---

### Task 13.2: `AbiRegistry.lookup_idl_event_by_discriminator`

Adds a second discriminator-keyed index on `AbiRegistry`, this one keyed by `(program_id_b58, discriminator_hex)`. Built at refresh time from every ABI with `kind == "solana_idl"`. The lookup returns `(event_name, struct, abi_id)` so the parser can both borsh-decode the body and stamp `abi_id` into the emitted `Event` for downstream subscription matching.

**Files:**
- Modify: `core/abi/registry.py` — add `_idl_event_index`, `_rebuild_idl_event_index()`, call it in `refresh()`, expose `lookup_idl_event_by_discriminator()`.
- Modify: `tests/unit/test_abi_registry.py` — add 5 new tests at the end of the file.

- [ ] **Step 1: Write the failing registry tests**

Append to `tests/unit/test_abi_registry.py`:

```python
# ---------------------------------------------------------------------------
# Anchor IDL discriminator lookup (chunk 13)
# ---------------------------------------------------------------------------

import hashlib

_PROG_A = "PrgA11111111111111111111111111111111111111"
_PROG_B = "PrgB22222222222222222222222222222222222222"


def _idl_event(name: str, fields: list[dict]) -> dict:
    return {"name": name, "fields": fields}


def _idl_body(program_id: str, events: list[dict]) -> dict:
    return {
        "metadata": {"address": program_id, "name": "test_program", "version": "0.1.0"},
        "instructions": [],
        "events": events,
    }


def _disc_hex(name: str) -> str:
    return hashlib.sha256(f"event:{name}".encode()).digest()[:8].hex()


def test_lookup_idl_event_returns_hit_for_known_program_and_discriminator() -> None:
    body = _idl_body(_PROG_A, [
        _idl_event("Trade", [{"name": "size", "type": "u64"}]),
    ])
    snap = _snap_with(abis=[SnapshotAbi(id="idl1", name="prog_a", kind="solana_idl", body=body)])
    r = AbiRegistry()
    r.refresh(snap)

    hit = r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("Trade"))
    assert hit is not None
    event_name, struct, abi_id = hit
    assert event_name == "Trade"
    assert abi_id == "idl1"
    # Smoke-test the returned schema: build then parse the round-trip.
    payload = struct.build({"size": 99})
    parsed = struct.parse(payload)
    assert parsed["size"] == 99


def test_lookup_idl_event_returns_none_for_unknown_program() -> None:
    body = _idl_body(_PROG_A, [_idl_event("Trade", [{"name": "size", "type": "u64"}])])
    snap = _snap_with(abis=[SnapshotAbi(id="idl1", name="prog_a", kind="solana_idl", body=body)])
    r = AbiRegistry()
    r.refresh(snap)

    assert r.lookup_idl_event_by_discriminator(_PROG_B, _disc_hex("Trade")) is None


def test_lookup_idl_event_returns_none_for_unknown_discriminator() -> None:
    body = _idl_body(_PROG_A, [_idl_event("Trade", [{"name": "size", "type": "u64"}])])
    snap = _snap_with(abis=[SnapshotAbi(id="idl1", name="prog_a", kind="solana_idl", body=body)])
    r = AbiRegistry()
    r.refresh(snap)

    assert r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("DoesNotExist")) is None


def test_lookup_idl_event_skips_idl_with_no_metadata_address(caplog) -> None:
    # IDL body missing `metadata.address` — should be skipped (warning logged)
    # and yield no index entries.
    body = {"instructions": [], "events": [_idl_event("Trade", [{"name": "size", "type": "u64"}])]}
    snap = _snap_with(abis=[SnapshotAbi(id="idl_no_addr", name="orphan", kind="solana_idl", body=body)])
    r = AbiRegistry()
    with caplog.at_level("WARNING"):
        r.refresh(snap)

    assert r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("Trade")) is None
    assert any("idl_no_program_id" in rec.message for rec in caplog.records)


def test_lookup_idl_event_skips_event_with_unsupported_field_type(caplog) -> None:
    # An IDL with one supported and one unsupported event — only the supported
    # one should be indexed; the unsupported one should log and be skipped.
    body = _idl_body(_PROG_A, [
        _idl_event("Trade", [{"name": "size", "type": "u64"}]),
        _idl_event("Compound", [{"name": "inner", "type": {"defined": "InnerStruct"}}]),
    ])
    snap = _snap_with(abis=[SnapshotAbi(id="idl1", name="prog_a", kind="solana_idl", body=body)])
    r = AbiRegistry()
    with caplog.at_level("WARNING"):
        r.refresh(snap)

    assert r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("Trade")) is not None
    assert r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("Compound")) is None
    assert any("idl_event_schema_unsupported" in rec.message for rec in caplog.records)


def test_refresh_evicts_idl_index_for_removed_abi() -> None:
    body_v1 = _idl_body(_PROG_A, [_idl_event("Trade", [{"name": "size", "type": "u64"}])])
    snap1 = _snap_with(abis=[SnapshotAbi(id="idl1", name="prog_a", kind="solana_idl", body=body_v1)])
    snap2 = _snap_with(abis=[])  # everything removed

    r = AbiRegistry()
    r.refresh(snap1)
    assert r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("Trade")) is not None
    r.refresh(snap2)
    assert r.lookup_idl_event_by_discriminator(_PROG_A, _disc_hex("Trade")) is None
```

> Note on `SnapshotAbi`: in chunk 2 it was defined with `kind: Literal["evm_abi", "solana_idl"]` and `body: dict[str, Any]` — the IDL body shape above is compatible. If the `SnapshotAbi` import line at the top of `test_abi_registry.py` doesn't already exist, add it next to the other `from core.config.snapshot import` imports.

- [ ] **Step 2: Run the failing tests**

```bash
pytest tests/unit/test_abi_registry.py -v -k idl
```

Expected: 6 FAIL with `AttributeError: 'AbiRegistry' object has no attribute 'lookup_idl_event_by_discriminator'`.

- [ ] **Step 3: Add the IDL event index to `AbiRegistry`**

Edit `core/abi/registry.py`. In `__init__`, add a third instance attribute alongside the existing `_topic0_index` (added in chunk 4) and `_selector_index` (added in chunk 5):

```python
        # (program_id_b58, discriminator_hex) → (abi_id, event_name, struct)
        self._idl_event_index: dict[tuple[str, str], tuple[str, str, Any]] = {}
```

(`Any` keeps `borsh-construct` out of the public typing surface; the concrete `CStruct` type is recovered inside `_rebuild_idl_event_index` by importing the helpers locally. `borsh-construct` is a hard dep declared in `pyproject.toml` per Task 13.1 Step 1, so there's no extras-conditional import to worry about.)

Add the rebuild helper just below `_rebuild_selector_index` (so all three rebuilds live next to each other):

```python
    def _rebuild_idl_event_index(self) -> None:
        from core.abi.decoder import (
            anchor_event_discriminator,
            build_anchor_event_struct,
        )

        idx: dict[tuple[str, str], tuple[str, str, Any]] = {}
        for abi_id, abi in self._abis.items():
            if abi.kind != "solana_idl":
                continue
            body = abi.body if isinstance(abi.body, dict) else {}
            program_id = (body.get("metadata") or {}).get("address")
            if not isinstance(program_id, str) or not program_id:
                log.warning("abi_registry.idl_no_program_id", abi_id=abi_id)
                continue
            for event in body.get("events", []) or []:
                name = event.get("name")
                if not isinstance(name, str) or not name:
                    continue
                struct = build_anchor_event_struct(event)
                if struct is None:
                    log.warning(
                        "abi_registry.idl_event_schema_unsupported",
                        abi_id=abi_id,
                        program_id=program_id,
                        event=name,
                    )
                    continue
                disc_hex = anchor_event_discriminator(name).hex()
                key = (program_id, disc_hex)
                if key in idx:
                    log.warning(
                        "abi_registry.idl_discriminator_collision",
                        program_id=program_id,
                        discriminator=disc_hex,
                        first=idx[key][:2],   # (abi_id, event_name)
                        second=(abi_id, name),
                    )
                    continue  # first-write-wins
                idx[key] = (abi_id, name, struct)
        self._idl_event_index = idx
```

In `refresh()`, add a call to `_rebuild_idl_event_index()` alongside the existing index-rebuild calls:

```python
        self._rebuild_topic0_index()      # chunk 4
        self._rebuild_selector_index()    # chunk 5
        self._rebuild_idl_event_index()   # chunk 13
        log.info("abi_registry.refreshed", count=len(new_abis))
```

(The existing `log.info` line stays; the three rebuild calls precede it. Do NOT touch the eviction / hash-comparison block that runs BEFORE the index rebuilds — chunks 2–5 already cover it.)

Finally, add the public lookup method:

```python
    def lookup_idl_event_by_discriminator(
        self, program_id: str, discriminator_hex: str
    ) -> tuple[str, Any, str] | None:
        """Resolve an Anchor IDL event by (program_id, discriminator).

        ``program_id`` is the base58 pubkey from the surrounding `Program <pid>
        invoke` log line. ``discriminator_hex`` is the lowercase hex of the
        first 8 bytes of the `Program data:` payload.

        Returns ``(event_name, borsh_struct, abi_id)`` on hit; ``None`` on miss.
        """
        key = (program_id, discriminator_hex.lower())
        entry = self._idl_event_index.get(key)
        if entry is None:
            return None
        abi_id, event_name, struct = entry
        return event_name, struct, abi_id
```

(Type-hint note: returning `Any` for the struct keeps `borsh_construct` out of the public typing surface. Internal callers downcast as needed.)

- [ ] **Step 4: Re-run the tests**

```bash
pytest tests/unit/test_abi_registry.py -v -k idl
```

Expected: 6 PASS.

- [ ] **Step 5: Full registry regression**

```bash
pytest tests/unit/test_abi_registry.py -v
```

Expected: every chunk-2 through chunk-5 test still PASS + the new IDL tests PASS.

- [ ] **Step 6: Lint + typecheck**

```bash
make lint typecheck
```

Expected: clean. (If `mypy` complains about `tuple[str, Any, str]` vs more specific types, it's fine — the registry intentionally returns `Any` for the struct to avoid leaking `borsh-construct` into the public type surface.)

- [ ] **Step 7: Commit**

```bash
git add core/abi/registry.py tests/unit/test_abi_registry.py
git commit -m "feat(abi): IDL event discriminator index in AbiRegistry"
```

---

### Task 13.3: `AnchorIdlEventParser` — log-line walk + invoke-stack scoping

The parser walks `tx.log_messages` once per transaction, maintains a stack of currently-open `Program <pid> invoke` frames, and for every `Program data: <base64>` line emits one `Event` whose program_id is the top of the stack at that moment. The decode path: 8-byte discriminator + body → registry lookup → borsh decode → `kind="event"`. On decode failure for a known schema, emit a downgraded `kind="event"` with the raw base64 in `event.raw`. On unknown program_id or unknown discriminator → no event (silent).

**Stack semantics (Solana RPC log format):**
- `Program <pid> invoke [N]` opens a new frame at depth N (1-indexed). N is the CPI depth: 1 for top-level instructions, 2+ for cross-program invocations.
- `Program <pid> success` or `Program <pid> failed: ...` closes the most-recent frame matching that `pid` (in practice the top frame; we tolerate mismatches by popping until the pid matches, then continuing).
- `Program <pid> consumed N of M compute units` is purely informational and does NOT affect the stack.
- `Program log:` and `Program data:` lines belong to the program at the TOP of the stack at the moment they appear.

We do NOT need to perfectly mirror the validator's stack semantics — only enough to attribute each `Program data:` line to the correct emitting program. The depth integer in `invoke [N]` is informational; the order of `invoke`/`success`/`failed` lines is what drives our stack.

**Files:**
- Create: `core/parser/anchor_event.py` — the parser class.
- Create: `tests/unit/test_anchor_event_parser.py` — 7 unit tests.

- [ ] **Step 1: Write the failing parser tests**

Create `tests/unit/test_anchor_event_parser.py`:

```python
from __future__ import annotations

import base64
import hashlib

import pytest

from core.abi.registry import AbiRegistry
from core.chains.types import (
    SolanaBlock,
    SolanaInstruction,
    SolanaTokenBalance,
    SolanaTransaction,
)
from core.config.snapshot import ConfigSnapshot, SnapshotAbi
from core.parser.anchor_event import AnchorIdlEventParser


# ---- helpers -----------------------------------------------------------

PROG_A = "PrgA11111111111111111111111111111111111111"
PROG_B = "PrgB22222222222222222222222222222222222222"


def _idl_body(program_id: str) -> dict:
    return {
        "metadata": {"address": program_id, "name": "test_program", "version": "0.1.0"},
        "instructions": [],
        "events": [
            {
                "name": "Trade",
                "fields": [
                    {"name": "side", "type": "u8"},
                    {"name": "size", "type": "u64"},
                ],
            }
        ],
    }


def _registry_with(*program_ids: str) -> AbiRegistry:
    abis = [
        SnapshotAbi(id=f"idl-{pid[:4]}", name=f"prog_{pid[:4]}", kind="solana_idl", body=_idl_body(pid))
        for pid in program_ids
    ]
    snap = ConfigSnapshot(version=1, chains=[], abis=abis, subscriptions=[], channels=[])
    r = AbiRegistry()
    r.refresh(snap)
    return r


def _program_data_b64(side: int, size: int) -> str:
    # Build a valid Anchor event payload for `Trade {side, size}`.
    from borsh_construct import CStruct, U8, U64
    schema = CStruct("side" / U8, "size" / U64)
    body = schema.build({"side": side, "size": size})
    disc = hashlib.sha256(b"event:Trade").digest()[:8]
    return base64.b64encode(disc + body).decode()


def _tx_with_logs(logs: list[str], *, signature: str = "SIG") -> SolanaTransaction:
    return SolanaTransaction(
        signature=signature, slot=100, success=True, fee=5000,
        account_keys=[], pre_balances=[], post_balances=[],
        pre_token_balances=[], post_token_balances=[],
        log_messages=logs, instructions=[],
    )


def _block(*txs: SolanaTransaction) -> SolanaBlock:
    return SolanaBlock(
        slot=100, block_hash="HASH" + "0" * 40,
        parent_slot=99, block_time=1700000000,
        transactions=list(txs),
    )


# ---- tests --------------------------------------------------------------


def test_parser_emits_event_for_top_level_program_data() -> None:
    reg = _registry_with(PROG_A)
    logs = [
        f"Program {PROG_A} invoke [1]",
        f"Program data: {_program_data_b64(side=1, size=42)}",
        f"Program {PROG_A} success",
    ]
    block = _block(_tx_with_logs(logs))
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)

    events = list(p.parse(block))
    assert len(events) == 1
    ev = events[0]
    assert ev.kind == "event"
    assert ev.chain_id == "sol"
    assert ev.contract == PROG_A           # program_id is in `contract`
    assert ev.name == "Trade"
    assert ev.args == {"side": 1, "size": 42}
    assert ev.tx_hash == "SIG"


def test_parser_scopes_program_id_to_innermost_invoke_frame() -> None:
    # CPI: PROG_B is called from inside PROG_A. The `Program data:` line that
    # sits between PROG_B's invoke and success must be attributed to PROG_B,
    # NOT PROG_A.
    reg = _registry_with(PROG_A, PROG_B)
    logs = [
        f"Program {PROG_A} invoke [1]",
        f"Program {PROG_B} invoke [2]",
        f"Program data: {_program_data_b64(side=2, size=7)}",
        f"Program {PROG_B} success",
        f"Program {PROG_A} success",
    ]
    block = _block(_tx_with_logs(logs))
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)

    events = list(p.parse(block))
    assert len(events) == 1
    assert events[0].contract == PROG_B
    assert events[0].args == {"side": 2, "size": 7}


def test_parser_skips_program_data_with_unknown_program_id() -> None:
    # PROG_B has an IDL, but the data is emitted under PROG_A which we don't
    # know — silent skip.
    reg = _registry_with(PROG_B)
    logs = [
        f"Program {PROG_A} invoke [1]",
        f"Program data: {_program_data_b64(side=1, size=1)}",
        f"Program {PROG_A} success",
    ]
    block = _block(_tx_with_logs(logs))
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)

    assert list(p.parse(block)) == []


def test_parser_skips_program_data_with_unknown_discriminator() -> None:
    # PROG_A is known, but the discriminator doesn't match any indexed event.
    reg = _registry_with(PROG_A)
    bogus_body = b"\x00" * 16
    bogus_disc = hashlib.sha256(b"event:DoesNotExist").digest()[:8]
    logs = [
        f"Program {PROG_A} invoke [1]",
        f"Program data: {base64.b64encode(bogus_disc + bogus_body).decode()}",
        f"Program {PROG_A} success",
    ]
    block = _block(_tx_with_logs(logs))
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)

    assert list(p.parse(block)) == []


def test_parser_downgrades_to_kind_event_on_borsh_decode_failure() -> None:
    # Known discriminator (`Trade`) but the payload is truncated → decode fails
    # → emit kind="event" with empty args and raw payload preserved.
    reg = _registry_with(PROG_A)
    disc = hashlib.sha256(b"event:Trade").digest()[:8]
    bad_payload = base64.b64encode(disc + b"\x01").decode()  # missing 8 bytes of u64
    logs = [
        f"Program {PROG_A} invoke [1]",
        f"Program data: {bad_payload}",
        f"Program {PROG_A} success",
    ]
    block = _block(_tx_with_logs(logs))
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)

    events = list(p.parse(block))
    assert len(events) == 1
    ev = events[0]
    assert ev.kind == "event"
    assert ev.name == "Trade"
    assert ev.args == {}
    assert ev.raw["program_data"] == bad_payload
    assert ev.raw["program_id"] == PROG_A
    assert ev.raw["discriminator"] == disc.hex()


def test_parser_skips_failed_transactions() -> None:
    reg = _registry_with(PROG_A)
    logs = [
        f"Program {PROG_A} invoke [1]",
        f"Program data: {_program_data_b64(side=1, size=42)}",
        f"Program {PROG_A} failed: instruction error",
    ]
    tx = SolanaTransaction(
        signature="SIG", slot=100, success=False, fee=5000,  # success=False
        account_keys=[], pre_balances=[], post_balances=[],
        pre_token_balances=[], post_token_balances=[],
        log_messages=logs, instructions=[],
    )
    p = AnchorIdlEventParser(chain_id="sol", registry=_registry_with(PROG_A))

    assert list(p.parse(_block(tx))) == []


def test_parser_ignores_program_data_with_no_open_invoke_frame() -> None:
    # Malformed log: `Program data:` appears with no preceding invoke. Skip.
    reg = _registry_with(PROG_A)
    logs = [
        f"Program data: {_program_data_b64(side=1, size=42)}",
    ]
    block = _block(_tx_with_logs(logs))
    p = AnchorIdlEventParser(chain_id="sol", registry=reg)

    assert list(p.parse(block)) == []
```

- [ ] **Step 2: Run the failing tests**

```bash
pytest tests/unit/test_anchor_event_parser.py -v
```

Expected: ImportError on `core.parser.anchor_event` — the module doesn't exist yet.

- [ ] **Step 3: Implement the parser**

Create `core/parser/anchor_event.py`:

```python
"""Anchor IDL event parser.

Walks each transaction's ``log_messages`` once, attributing every
``Program data: <base64>`` line to the emitting program_id (via an
invoke/success stack), and decoding the payload against the IDL event
schema resolved through ``AbiRegistry.lookup_idl_event_by_discriminator``.

On unknown program_id or unknown discriminator → skip silently (we don't
own that program's IDL). On borsh decode failure for a known schema →
emit a downgraded ``kind="event"`` with the raw payload preserved in
``event.raw`` (spec §4.1 / §8 fallback).
"""
from __future__ import annotations

import base64
from collections.abc import Iterable
from typing import TYPE_CHECKING

import structlog

from core.abi.errors import DecodeFailed
from core.chains.types import SolanaBlock, SolanaTransaction
from core.parser.event import Event

if TYPE_CHECKING:
    from core.abi.registry import AbiRegistry


log = structlog.get_logger(__name__)


_PROGRAM_DATA_PREFIX = "Program data: "
_PROGRAM_INVOKE_PREFIX = "Program "
_INVOKE_SUFFIX_MARKER = " invoke ["
_SUCCESS_SUFFIX = " success"
_FAILED_SUFFIX_MARKER = " failed:"


class AnchorIdlEventParser:
    """Decode Anchor IDL events from Solana transaction log_messages."""

    def __init__(self, chain_id: str, registry: AbiRegistry) -> None:
        self._chain_id = chain_id
        self._registry = registry

    def parse(self, block: SolanaBlock) -> Iterable[Event]:
        for tx in block.transactions:
            if not tx.success:
                continue
            yield from self._parse_tx(block, tx)

    def _parse_tx(
        self, block: SolanaBlock, tx: SolanaTransaction
    ) -> Iterable[Event]:
        stack: list[str] = []   # base58 program_ids, deepest last
        for line in tx.log_messages:
            if line.startswith(_PROGRAM_DATA_PREFIX):
                if not stack:
                    log.debug(
                        "anchor_parser.program_data_without_invoke",
                        signature=tx.signature,
                    )
                    continue
                ev = self._maybe_decode(block, tx, stack[-1], line)
                if ev is not None:
                    yield ev
                continue
            if line.startswith(_PROGRAM_INVOKE_PREFIX):
                pid = self._parse_invoke(line)
                if pid is not None:
                    stack.append(pid)
                    continue
                pid = self._parse_close(line)
                if pid is not None:
                    self._pop_to(stack, pid)

    @staticmethod
    def _parse_invoke(line: str) -> str | None:
        # "Program <pid> invoke [N]"
        idx = line.find(_INVOKE_SUFFIX_MARKER)
        if idx == -1:
            return None
        return line[len(_PROGRAM_INVOKE_PREFIX):idx]

    @staticmethod
    def _parse_close(line: str) -> str | None:
        # "Program <pid> success" or "Program <pid> failed: ..."
        if line.endswith(_SUCCESS_SUFFIX):
            return line[len(_PROGRAM_INVOKE_PREFIX):-len(_SUCCESS_SUFFIX)]
        idx = line.find(_FAILED_SUFFIX_MARKER)
        if idx != -1:
            return line[len(_PROGRAM_INVOKE_PREFIX):idx]
        return None

    @staticmethod
    def _pop_to(stack: list[str], pid: str) -> None:
        # Pop frames until pid matches the top, then pop one more (the matching
        # frame). If pid never appears, leave the stack alone — best-effort
        # tolerance for malformed log sequences.
        for i in range(len(stack) - 1, -1, -1):
            if stack[i] == pid:
                del stack[i:]
                return

    def _maybe_decode(
        self,
        block: SolanaBlock,
        tx: SolanaTransaction,
        program_id: str,
        log_line: str,
    ) -> Event | None:
        b64 = log_line[len(_PROGRAM_DATA_PREFIX):]
        try:
            raw = base64.b64decode(b64, validate=True)
        except ValueError:
            return None
        if len(raw) < 8:
            return None
        disc, body = raw[:8], raw[8:]
        disc_hex = disc.hex()

        hit = self._registry.lookup_idl_event_by_discriminator(program_id, disc_hex)
        if hit is None:
            return None
        event_name, struct, abi_id = hit

        try:
            from core.abi.decoder import decode_anchor_event
            args = decode_anchor_event(struct, body)
        except DecodeFailed as exc:
            log.warning(
                "anchor_parser.decode_failed",
                program_id=program_id,
                event=event_name,
                abi_id=abi_id,
                err=str(exc),
            )
            return self._build_event(
                block, tx,
                program_id=program_id,
                event_name=event_name,
                args={},
                raw={
                    "program_data": b64,
                    "program_id": program_id,
                    "discriminator": disc_hex,
                    "abi_id": abi_id,
                },
            )

        return self._build_event(
            block, tx,
            program_id=program_id,
            event_name=event_name,
            args=args,
            raw={"program_data": b64, "abi_id": abi_id},
        )

    def _build_event(
        self,
        block: SolanaBlock,
        tx: SolanaTransaction,
        *,
        program_id: str,
        event_name: str,
        args: dict,
        raw: dict,
    ) -> Event:
        return Event(
            chain_id=self._chain_id,
            block_number=block.slot,
            block_hash=block.block_hash,
            block_timestamp=block.block_time or 0,
            tx_hash=tx.signature,
            tx_index=None,
            log_index=None,
            kind="event",
            contract=program_id,
            name=event_name,
            args=args,
            raw=raw,
        )
```

> One implementation note for the reviewer-of-the-implementer: the `_pop_to` heuristic is intentionally lenient. A perfectly-formed Solana validator log is balanced — each `invoke` has exactly one matching `success`/`failed`. We tolerate stray closes and missing closes because malformed logs should not break parsing on the happy paths.

- [ ] **Step 4: Re-run the parser tests**

```bash
pytest tests/unit/test_anchor_event_parser.py -v
```

Expected: 7 PASS.

- [ ] **Step 5: Lint + typecheck**

```bash
make lint typecheck
```

Expected: clean. (`TYPE_CHECKING` is used to break the `AbiRegistry → AnchorIdlEventParser → AbiRegistry` import cycle that would arise if the parser imported the registry concretely; mypy still sees the annotation.)

- [ ] **Step 6: Commit**

```bash
git add core/parser/anchor_event.py tests/unit/test_anchor_event_parser.py
git commit -m "feat(parser): AnchorIdlEventParser with invoke-stack scoping"
```

---

### Task 13.4: Wire `AnchorIdlEventParser` into `ChainRunner.start()`

The Solana branch in `ChainRunner.start()` currently builds `[SolNativeTransferParser, SplTransferParser]` (chunks 11 + 12). Task 13.4 appends `AnchorIdlEventParser(chain_id=..., registry=abi_registry)` to that list when an `AbiRegistry` is wired through — exactly mirroring the `AbiEventParser` pattern from chunk 4 (conditional on `abi_registry is not None`).

**Files:**
- Modify: `apps/worker/chain_runner.py:start` — extend the Solana parser list.
- Modify: `tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer` — assert the third default parser is present.

- [ ] **Step 1: Tighten the chain_runner Solana test**

In `tests/unit/test_chain_runner.py`, find `test_chain_runner_solana_branch_bypasses_buffer` (chunks 11 + 12). Two edits:

**(a)** The test currently constructs the `ChainRunner` without an `AbiRegistry` (chunks 11 + 12 didn't need one). Anchor parser wiring is conditional on registry presence, so we need to inject one. Add — just after the existing `cp = _FakeCheckpointRepo()` line — a registry fixture for `PROG_A`:

Place the imports at the top of `tests/unit/test_chain_runner.py` (next to the other `from core.*` imports), and put the registry construction inside the test function — just after the existing `cp = _FakeCheckpointRepo()` line:

```python
# At the top of the file (with the other imports):
from core.abi.registry import AbiRegistry
from core.config.snapshot import SnapshotAbi

# Inside test_chain_runner_solana_branch_bypasses_buffer, after `cp = _FakeCheckpointRepo()`:
_PROG_FOR_TEST = "PrgA11111111111111111111111111111111111111"
abi_registry = AbiRegistry()
abi_registry.refresh(ConfigSnapshot(
    version=1, chains=[], subscriptions=[], channels=[],
    abis=[SnapshotAbi(
        id="idl1", name="prog_a", kind="solana_idl",
        body={
            "metadata": {"address": _PROG_FOR_TEST, "name": "p", "version": "0.1"},
            "instructions": [],
            "events": [{"name": "Trade", "fields": [{"name": "size", "type": "u64"}]}],
        },
    )],
))
```

Then pass `abi_registry=abi_registry` to the `ChainRunner(...)` call. The kwarg was added in chunk 4 Task 4.5 and preserved through chunk 11's `__init__` rewrite (chunk 11 Task 11.4 Step "replace `__init__`" stores it as `self._abi_registry`).

**(b)** Extend the parser-type assertions (added in chunk 12 Task 12.3 Step 1) to include `AnchorIdlEventParser`. Replace:

```python
from core.parser.sol_native import SolNativeTransferParser
from core.parser.spl_transfer import SplTransferParser

assert runner._pipeline is not None
pipeline_parsers = runner._pipeline._parsers  # type: ignore[attr-defined]
parser_types = {type(p) for p in pipeline_parsers}
assert SolNativeTransferParser in parser_types, (
    "default Solana pipeline must include SolNativeTransferParser"
)
assert SplTransferParser in parser_types, (
    "default Solana pipeline must include SplTransferParser"
)
```

with:

```python
from core.parser.anchor_event import AnchorIdlEventParser
from core.parser.sol_native import SolNativeTransferParser
from core.parser.spl_transfer import SplTransferParser

assert runner._pipeline is not None
pipeline_parsers = runner._pipeline._parsers  # type: ignore[attr-defined]
parser_types = {type(p) for p in pipeline_parsers}
assert SolNativeTransferParser in parser_types, (
    "default Solana pipeline must include SolNativeTransferParser"
)
assert SplTransferParser in parser_types, (
    "default Solana pipeline must include SplTransferParser"
)
assert AnchorIdlEventParser in parser_types, (
    "default Solana pipeline must include AnchorIdlEventParser (registry was wired)"
)
```

- [ ] **Step 2: Run the test, expect FAIL**

```bash
pytest tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer -v
```

Expected: FAIL — `AnchorIdlEventParser not in parser_types`. Chunk 12's `start()` only constructs the two transfer parsers.

- [ ] **Step 3: Extend the Solana branch in `start()`**

Locate the Solana branch in `ChainRunner.start()` (added in chunk 11 Task 11.4, extended in chunk 12 Task 12.3 Step 3). Replace:

```python
elif self._chain.kind == "solana":
    # No buffer for Solana — commitment handles finality (spec §4.6).
    self._buffer = None
    self._pipeline = SolanaParserPipeline(
        self._solana_parsers_override
        or [
            SolNativeTransferParser(chain_id=self._chain.id),
            SplTransferParser(chain_id=self._chain.id),
        ]
    )
```

with:

```python
elif self._chain.kind == "solana":
    # No buffer for Solana — commitment handles finality (spec §4.6).
    self._buffer = None
    sol_parsers: list[SolanaParser] = [
        SolNativeTransferParser(chain_id=self._chain.id),
        SplTransferParser(chain_id=self._chain.id),
    ]
    if self._abi_registry is not None:
        sol_parsers.append(
            AnchorIdlEventParser(chain_id=self._chain.id, registry=self._abi_registry)
        )
    self._pipeline = SolanaParserPipeline(
        self._solana_parsers_override or sol_parsers
    )
```

(The `SolanaParser` type — added in chunk 11 Task 11.2 — needs to be importable here. Add `from core.parser.base import SolanaParser` next to the other `core.parser.*` imports at the top of `chain_runner.py`. `chain_runner.py` does NOT currently import anything from `core.parser.base`, so this is a new line, not an addition to an existing one.)

Add to the imports at the top of `apps/worker/chain_runner.py`:

```python
from core.parser.anchor_event import AnchorIdlEventParser
```

(Keep the parser imports alphabetized: `abi_call`, `abi_event`, `anchor_event`, `erc20`, `native`, `pipeline`, `sol_native`, `spl_transfer`.)

- [ ] **Step 4: Re-run the test**

```bash
pytest tests/unit/test_chain_runner.py::test_chain_runner_solana_branch_bypasses_buffer -v
```

Expected: PASS.

- [ ] **Step 5: Full chain_runner regression**

```bash
pytest tests/unit/test_chain_runner.py -v
```

Expected: every EVM test still green; the Solana test PASSes with the new assertion.

- [ ] **Step 6: Lint + typecheck**

```bash
make lint typecheck
```

Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add apps/worker/chain_runner.py tests/unit/test_chain_runner.py
git commit -m "feat(worker): wire AnchorIdlEventParser into ChainRunner.start()"
```

---

### Task 13.5: Close-out — full regression, contributor docs

Final sweep. The chunk's intended scope is now fully implemented and unit-tested. Verify the full unit test suite is green and ABI flags / structures are clean.

**Files:**
- (Verification only — no new files.)

- [ ] **Step 1: Full unit-test sweep**

```bash
pytest tests/unit -v
```

Expected: every test green. The Solana segment (chunks 10–13) now contributes:

- `test_solana_types.py` (chunk 10)
- `test_solana_adapter.py` (chunk 10)
- `test_sol_native_parser.py` (chunk 11)
- `test_spl_transfer_parser.py` (chunk 12)
- `test_anchor_event_decoder.py` (chunk 13, this chunk)
- `test_anchor_event_parser.py` (chunk 13, this chunk)
- `test_abi_registry.py` (extended in chunks 4, 5, 13)
- `test_chain_runner.py` (extended in chunks 11, 12, 13)

…in addition to every EVM-segment test still passing.

- [ ] **Step 2: Lint + typecheck on the full tree**

```bash
make lint typecheck
```

Expected: clean.

- [ ] **Step 3: Confirm `Event.kind` values across the Solana parsers**

Quick sanity-grep to verify the three Solana parsers emit the expected `kind` values:

```bash
grep -nE 'kind="(native_transfer|token_transfer|event)"' core/parser/sol_native.py core/parser/spl_transfer.py core/parser/anchor_event.py
```

Expected output:
- `core/parser/sol_native.py: kind="native_transfer"`
- `core/parser/spl_transfer.py: kind="token_transfer"`
- `core/parser/anchor_event.py: kind="event"` (two occurrences — happy path and fallback)

These match spec §4.7 and the post-§4.9 `EventKind` literal set. No drift.

- [ ] **Step 4: Final chunk-13 commit (verification record)**

There are no source changes in Task 13.5 — Tasks 13.1 → 13.4 each committed individually. Use a no-op commit only if you want a clean checkpoint marker; otherwise, skip and proceed to chunk 14.

(If you do want a checkpoint, the message should be `chore: close out M2 chunk 13 (Anchor IDL event parser)`.)

---

**Chunk 13 done.** The Solana parser surface is now complete: native lamport transfers (chunk 11), SPL token transfers (chunk 12), and Anchor IDL events (chunk 13) all flow through the same `SolanaParserPipeline → Matcher → Notifier` path that the EVM segment built. Chunk 14 brings the E2E that exercises the full Solana stack against `solana-test-validator`.
## Chunk 14: Solana E2E + `m2-complete` tag

The M2 close-out. Drives a real `solana-test-validator` subprocess through the full chain: validator → `SolanaAdapter` → `SolanaParserPipeline` (`SolNativeTransferParser` + `SplTransferParser` from chunks 11 & 12) → `Matcher` → `HttpChannel` → in-process webhook receiver. Two E2E tests: one for a system-program lamport transfer (native SOL), one for an SPL Token Program transfer of a freshly-minted token. After both pass and the full regression sweep is green, the branch is tagged `m2-complete`.

Two facts shape this chunk:

1. **`solana-test-validator` cold-start is 5–10 s** (vs Anvil's <1 s — spec §9.2). The validator fixture is `scope="session"`: one boot per test session, reused across both E2E tests AND the chunk-10/11 integration tests. Function-scoped would 4× the wallclock per `pytest` invocation.
2. **The chunk-10 fixture lives in `tests/integration/conftest.py`.** pytest's conftest cascade only flows *downward*, so a fixture in `tests/integration/conftest.py` is invisible to `tests/e2e/`. Task 14.1 *promotes* the fixture up to `tests/conftest.py` (the root-level conftest that already hosts `redis_url`) and removes it from `tests/integration/conftest.py`. After the move, the chunk-10/11 ITs see it via cascade; the new chunk-14 E2E tests see it via the same cascade. **One source of truth.**

**Modified files this chunk:**
- `tests/conftest.py` — promote `solana_validator` fixture + `SolanaValidatorHandle` dataclass + `_free_tcp_port` + `_wait_for_rpc` helpers from `tests/integration/conftest.py`.
- `tests/integration/conftest.py` — remove the same code (DON'T leave a stale duplicate; pytest will raise `Fixture "solana_validator" already defined` on collection).
- `tests/e2e/conftest.py` — APPEND Solana E2E helpers: airdrop, native transfer submitter, SPL-via-CLI mint setup, `funded_sender`, `spl_mint` fixtures.
- `tests/e2e/test_solana_native_e2e.py` — **new**: native SOL transfer → webhook.
- `tests/e2e/test_solana_spl_e2e.py` — **new**: SPL token transfer → webhook.

**Out of scope this chunk:**
- Anchor IDL event E2E. Chunk 13's unit tests are authoritative for `AnchorIdlEventParser`; adding an on-validator program deploy + IDL upload would 2× the chunk's complexity for a single extra `kind="event"` assertion. The wire shape (`Event(kind="event", name=<event>, args={...})`) is identical to the EVM ABI event path, which IS covered E2E (chunk 5 unit + ChainRunner integration).
- Multi-chain (EVM + Solana in one test). The EVM ERC-20 E2E (chunk 9) and the Solana E2E (this chunk) are separate processes by design — sharing a worker between them would just exercise the worker's chain-multiplexing code, not new ground.
- Reorg E2E for Solana. Solana's `commitment="confirmed"` semantics make a reorg vanishingly rare on the local validator (no forks under load). M1's EVM reorg E2E (against Anvil's `anvil_reorg` RPC) already exercises the `ConfirmationBuffer` logic that matters; Solana's branch bypasses the buffer (`confirmations=0`), so there's no Solana-specific reorg code to E2E.
- `transferChecked`-only coverage. Chunk 12's `SplTransferParser` unit tests cover both `Transfer` (discriminator 3) and `TransferChecked` (discriminator 12). The E2E here drives whichever `spl-token transfer` emits by default (currently `TransferChecked` for decimaled mints — the CLI picks it). Forcing both ix variants from the CLI is fragile and adds zero coverage over the unit tests.

**Toolchain prerequisite:** the host needs `solana-test-validator` AND `spl-token` on PATH. Both binaries ship with the Anza Solana CLI install (`sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"` — referenced in `docs/dev-setup.md`). If either is missing, the relevant test SKIPs at fixture-entry — same pattern as M1's `anvil` skip and chunk 10's `solana-test-validator` skip.

**Determinism note:** `solana-test-validator` confirms transactions deterministically once a target slot is produced, but slot production is wall-clock driven (~400 ms/slot). The tests below use a 60 s delivery timeout (vs chunk 9's 30 s) because the worker has to round-trip TWO confirmation cycles — slot finalization on the validator side, plus the worker's own poll interval (400 ms). A snappy host typically finishes in 8–15 s; the headroom prevents flakes on busy CI.

**Tag `m2-complete` placement:** the tag lands AFTER Task 14.5's full suite sweep, not after Task 14.4's last test commit. Reason: if a hidden cross-chunk regression surfaces only when EVM + Solana tests run together (different event-loop policies, port collisions, etc.), the tag should sit on a commit where THAT combination is green — not on the SPL test commit which doesn't exercise the EVM suite.

### Task 14.1: Promote `solana_validator` fixture to shared `tests/conftest.py`

The chunk-10 fixture currently lives in `tests/integration/conftest.py`. pytest's conftest cascade is unidirectional: a fixture defined under `tests/integration/` is invisible to `tests/e2e/`. Promoting it to `tests/conftest.py` makes it visible to both subtrees without duplication.

This task is a **pure move** — no behaviour change. The fixture body, the `SolanaValidatorHandle` dataclass, and the two private helpers (`_free_tcp_port`, `_wait_for_rpc`) are removed from `tests/integration/conftest.py` and added verbatim to `tests/conftest.py`. The chunk-10 and chunk-11 ITs continue to see the fixture via the new ancestor location.

**Files:**
- Modify: `tests/conftest.py` — APPEND the fixture and helpers below the existing `redis_url` fixture.
- Modify: `tests/integration/conftest.py` — DELETE the fixture and helpers (and the `httpx`, `shutil`, `socket`, `subprocess`, `time`, `dataclass`, `Path`, `Iterator`, `contextlib`, `pytest` imports if no other code in that file still uses them; M1's `db` fixture and its imports stay).

- [ ] **Step 1: Move the fixture and helpers to `tests/conftest.py`**

Append the following to `tests/conftest.py`, **after** the existing `redis_url` fixture (do not touch `redis_url` or its imports):

```python
# tests/conftest.py  --  APPEND below the existing M1 redis_url fixture
import contextlib
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import httpx
import pytest


@dataclass
class SolanaValidatorHandle:
    rpc_url: str
    process: subprocess.Popen
    ledger_path: Path


def _free_tcp_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_rpc(url: str, *, timeout_s: float = 25.0) -> None:
    deadline = time.monotonic() + timeout_s
    body = b'{"jsonrpc":"2.0","id":1,"method":"getHealth"}'
    while time.monotonic() < deadline:
        try:
            r = httpx.post(url, content=body, headers={"content-type": "application/json"}, timeout=2.0)
            if r.status_code == 200 and r.json().get("result") == "ok":
                return
        except (httpx.RequestError, ValueError):
            pass
        time.sleep(0.5)
    raise TimeoutError(f"solana-test-validator did not become healthy at {url}")


@pytest.fixture(scope="session")
def solana_validator(tmp_path_factory) -> Iterator[SolanaValidatorHandle]:
    """Session-scoped: 5–10 s cold start; subsequent tests reuse the same
    validator. State accumulates across tests in a session; every consumer
    in this repo is either read-only or uses fresh keypairs / mints so the
    shared state is fine."""
    if shutil.which("solana-test-validator") is None:
        pytest.skip("solana-test-validator not installed; see docs/dev-setup.md")

    rpc_port = _free_tcp_port()
    faucet_port = _free_tcp_port()
    ledger = tmp_path_factory.mktemp("solana-ledger")
    proc = subprocess.Popen(
        [
            "solana-test-validator",
            "--reset",
            "--quiet",
            "--ledger", str(ledger),
            "--rpc-port", str(rpc_port),
            "--faucet-port", str(faucet_port),
            "--bind-address", "127.0.0.1",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    rpc_url = f"http://127.0.0.1:{rpc_port}"
    try:
        _wait_for_rpc(rpc_url)
        yield SolanaValidatorHandle(rpc_url=rpc_url, process=proc, ledger_path=ledger)
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5.0)
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5.0)
```

(The block above is byte-identical to what chunk 10 placed in `tests/integration/conftest.py` — verify with `git diff` after Step 2.)

- [ ] **Step 2: Remove the same code from `tests/integration/conftest.py`**

Open `tests/integration/conftest.py`. Delete:
- The `SolanaValidatorHandle` dataclass.
- The `_free_tcp_port` function.
- The `_wait_for_rpc` function.
- The `solana_validator` fixture.
- Any of these imports that are now unused by the remaining code: `contextlib`, `shutil`, `socket`, `subprocess`, `time`, `dataclass`, `Path`, `Iterator`, `httpx`, `pytest`. (If `pytest` is still used by M1's `db` fixture decorator — check before removing.)

`make lint` will catch unused imports — let ruff drive the cleanup if you're unsure.

- [ ] **Step 3: Verify the chunks 10/11 ITs still see the fixture**

Run the existing Solana ITs:

```bash
pytest tests/integration/test_solana_adapter.py tests/integration/test_chain_runner_solana.py -v
```

Expected: all 4 tests PASS (or all 4 SKIP if `solana-test-validator` isn't installed). If you get `fixture 'solana_validator' not found`, the move didn't land in `tests/conftest.py` correctly — re-check the file path.

- [ ] **Step 4: Lint + typecheck**

```bash
make lint && make typecheck
```

Expected: clean. If `make lint` flags unused imports in `tests/integration/conftest.py`, remove them per Step 2's note.

- [ ] **Step 5: Commit**

```bash
git add tests/conftest.py tests/integration/conftest.py
git commit -m "test(conftest): promote solana_validator fixture to tests/conftest.py"
```

### Task 14.2: Solana E2E helpers in `tests/e2e/conftest.py`

Appends the Solana-specific helper layer to the existing E2E conftest. Three pieces:

1. **Async RPC helpers** mirroring chunk-11 Task 11.5: `_solana_airdrop`, `_solana_latest_blockhash`, `_solana_send_native_transfer`, `_solana_wait_for_signature`. Centralised here so the two new test files don't duplicate the JSON-RPC boilerplate.
2. **An `spl-token` CLI wrapper** + helpers to write a `solders.Keypair` to a CLI-compatible JSON file (`_write_keypair_file`) and to skip cleanly when `spl-token` isn't on PATH (`_require_spl_token_cli`). `spl-token` is the bundled Solana CLI tool that creates mints/ATAs and mints/transfers tokens; rebuilding those instructions from raw `solders` would add ~200 lines for zero correctness gain.
3. **Two pytest fixtures**: `funded_sender` (function-scoped — fresh keypair + airdrop per test) and `spl_mint` (function-scoped — fresh mint, ATA, and pre-minted balance per test). Per-test scoping avoids cross-test interference even though the validator process is shared.

The `spl-token` CLI is invoked as a subprocess. Its output is line-prefixed (`Creating token <pubkey>`, `Creating account <pubkey>`, `Signature: <sig>`) and parsed with simple `splitlines` + `startswith` — no regexes. If a future `spl-token` release reshapes the output, the helpers will raise `RuntimeError` with the full stdout/stderr in the message, making the failure mode obvious.

**Files:**
- Modify: `tests/e2e/conftest.py` — APPEND below M1's `anvil` / `webhook_receiver` and chunk-9's `erc20_token` fixtures.

- [ ] **Step 1: Append the Solana helpers to `tests/e2e/conftest.py`**

Add at the bottom of the existing file (do NOT remove or modify the M1 / chunk-9 fixtures):

```python
# tests/e2e/conftest.py  --  APPEND below the existing fixtures
# (M1 conftest already imports `asyncio`, `contextlib`, `shutil`, `subprocess`,
# `pytest`, `pytest_asyncio`, `httpx`, `dataclass` at module top — reuse them;
# do NOT re-import.)

import base64
import json
from pathlib import Path
from typing import NamedTuple

from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import Message
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer as system_transfer
from solders.transaction import Transaction


# -- async Solana RPC helpers (signed-tx & airdrop) --------------------------

async def _solana_airdrop(rpc_url: str, recipient: Pubkey, lamports: int) -> None:
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "requestAirdrop",
        "params": [str(recipient), lamports],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(rpc_url, json=body)
    r.raise_for_status()
    assert "result" in r.json(), r.text


async def _solana_latest_blockhash(rpc_url: str) -> str:
    body = {"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash"}
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(rpc_url, json=body)
    r.raise_for_status()
    return r.json()["result"]["value"]["blockhash"]


async def _solana_wait_for_signature(
    rpc_url: str, sig: str, *, timeout_s: float = 25.0
) -> None:
    """Poll `getSignatureStatuses` until `confirmationStatus` reaches
    `confirmed` (or `finalized`). Raises `TimeoutError` on miss."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "getSignatureStatuses",
        "params": [[sig], {"searchTransactionHistory": True}],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        while asyncio.get_running_loop().time() < deadline:
            r = await client.post(rpc_url, json=body)
            statuses = r.json().get("result", {}).get("value", [None])[0]
            if statuses and statuses.get("confirmationStatus") in ("confirmed", "finalized"):
                return
            await asyncio.sleep(0.5)
    raise TimeoutError(f"signature {sig} did not confirm within {timeout_s}s")


async def _solana_send_native_transfer(
    rpc_url: str, sender: Keypair, recipient: Pubkey, lamports: int
) -> str:
    """Build, sign, and submit a System Program transfer. Returns the base58
    signature. Waits for confirmation before returning so the caller can
    safely assert the tx exists in the canonical chain right after the call."""
    ix = system_transfer(TransferParams(
        from_pubkey=sender.pubkey(),
        to_pubkey=recipient,
        lamports=lamports,
    ))
    blockhash_str = await _solana_latest_blockhash(rpc_url)
    recent_blockhash = Hash.from_string(blockhash_str)
    msg = Message.new_with_blockhash([ix], sender.pubkey(), recent_blockhash)
    tx = Transaction([sender], msg, recent_blockhash)
    body = {
        "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
        "params": [base64.b64encode(bytes(tx)).decode(), {"encoding": "base64"}],
    }
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(rpc_url, json=body)
    r.raise_for_status()
    sig = r.json()["result"]
    await _solana_wait_for_signature(rpc_url, sig)
    return sig


# -- spl-token CLI wrapper ---------------------------------------------------

def _require_spl_token_cli() -> None:
    """Skip cleanly if `spl-token` isn't on PATH. Called from fixtures that
    invoke the CLI."""
    if shutil.which("spl-token") is None:
        pytest.skip(
            "spl-token CLI not installed (ships with the Anza Solana CLI); "
            "see docs/dev-setup.md"
        )


def _write_keypair_file(kp: Keypair, path: Path) -> None:
    """The Solana CLI expects a keypair as a JSON array of 64 bytes
    (`[123,45,...]`). `solders.Keypair` serialises to the same 64-byte secret
    via `bytes(kp)`; convert and write."""
    path.write_text(json.dumps(list(bytes(kp))))


def _spl_token(args: list[str], *, rpc_url: str, fee_payer: Path) -> str:
    """Invoke `spl-token` once and return its stdout. Raises `RuntimeError`
    with full stdout+stderr on non-zero exit so test failures point at the
    actual CLI message instead of a generic CalledProcessError."""
    cmd = [
        "spl-token", "--url", rpc_url, "--fee-payer", str(fee_payer),
        *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            f"spl-token CLI failed (rc={proc.returncode}):\n"
            f"  cmd:    {' '.join(cmd)}\n"
            f"  stdout: {proc.stdout!r}\n"
            f"  stderr: {proc.stderr!r}"
        )
    return proc.stdout


def _parse_cli_line(out: str, prefix: str) -> str:
    """Extract the pubkey at the end of a line that starts with `prefix`.

    Example: parses `Creating token Es9vMFrza...` → `Es9vMFrza...` when called
    with `prefix="Creating token"`. Raises `RuntimeError` (with the full
    stdout) if no such line exists — that means the CLI changed its output
    shape, which is a real test-infra failure worth investigating.
    """
    for line in out.splitlines():
        if line.startswith(prefix):
            parts = line.split()
            if len(parts) >= len(prefix.split()) + 1:
                return parts[-1]
    raise RuntimeError(f"spl-token output had no `{prefix}` line: {out!r}")


# -- fixtures ----------------------------------------------------------------

@dataclass
class SolanaSenderHandle:
    keypair: Keypair
    keypair_path: Path
    pubkey_b58: str


@pytest_asyncio.fixture
async def funded_sender(solana_validator, tmp_path) -> SolanaSenderHandle:
    """Fresh keypair, airdropped 1 SOL, persisted to a CLI-compatible JSON
    file. Function-scoped so each test gets a clean account (no carry-over
    nonce, no leftover token balances)."""
    kp = Keypair()
    await _solana_airdrop(solana_validator.rpc_url, kp.pubkey(), 1_000_000_000)  # 1 SOL
    # Airdrops are usually confirmed within a slot (~400 ms) on the local
    # validator, but cold-start hosts can be slower. Wait briefly so the
    # subsequent `spl-token` calls actually see the lamports.
    await asyncio.sleep(2.0)

    kp_path = tmp_path / "sender.json"
    _write_keypair_file(kp, kp_path)
    return SolanaSenderHandle(
        keypair=kp,
        keypair_path=kp_path,
        pubkey_b58=str(kp.pubkey()),
    )


class SplMintHandle(NamedTuple):
    mint_address: str
    sender_keypair: Keypair
    sender_keypair_path: Path
    recipient_keypair: Keypair
    recipient_pubkey_b58: str


@pytest_asyncio.fixture
async def spl_mint(
    solana_validator, funded_sender, tmp_path
) -> SplMintHandle:
    """Mints a fresh SPL token (6 decimals), creates a token account for the
    sender, mints 1_000_000 base units to it, and prepares (but does NOT
    create on-chain) a recipient keypair. The first SPL transfer in the test
    will create the recipient ATA via `--fund-recipient`."""
    _require_spl_token_cli()
    rpc = solana_validator.rpc_url

    # 1) Create the mint. `--mint-authority` defaults to fee-payer.
    out = _spl_token(
        ["create-token", "--decimals", "6"],
        rpc_url=rpc, fee_payer=funded_sender.keypair_path,
    )
    mint_address = _parse_cli_line(out, "Creating token")

    # 2) Create the sender's ATA for that mint.
    out = _spl_token(
        ["create-account", mint_address],
        rpc_url=rpc, fee_payer=funded_sender.keypair_path,
    )
    # `Creating account <ATA>` — captured but not returned; spl-token uses the
    # sender's ATA implicitly for subsequent `mint` and `transfer` calls.
    _parse_cli_line(out, "Creating account")

    # 3) Mint 1_000_000 base units (1 token at 6 decimals) to the sender.
    _spl_token(
        ["mint", mint_address, "1"],
        rpc_url=rpc, fee_payer=funded_sender.keypair_path,
    )

    # 4) Prepare a recipient keypair (no on-chain account yet — the first
    #    `--fund-recipient` transfer will create the ATA).
    recipient = Keypair()
    return SplMintHandle(
        mint_address=mint_address,
        sender_keypair=funded_sender.keypair,
        sender_keypair_path=funded_sender.keypair_path,
        recipient_keypair=recipient,
        recipient_pubkey_b58=str(recipient.pubkey()),
    )


async def _spl_transfer_one(
    rpc_url: str, fee_payer: Path, mint: str, amount_decimal: str, recipient_b58: str,
) -> str:
    """Issue ONE `spl-token transfer` and return the returned signature.
    `--fund-recipient` auto-creates the recipient's ATA if missing (and
    `--allow-unfunded-recipient` lets us pay for that account creation from
    the sender's lamports). The first call in a test typically funds the ATA
    (taking ~0.002 SOL); subsequent calls reuse it."""
    out = _spl_token(
        [
            "transfer", mint, amount_decimal, recipient_b58,
            "--fund-recipient",
            "--allow-unfunded-recipient",
        ],
        rpc_url=rpc_url, fee_payer=fee_payer,
    )
    return _parse_cli_line(out, "Signature:")
```

- [ ] **Step 2: Collect-only smoke**

```bash
python -m pytest tests/e2e/conftest.py --collect-only -q
```

Expected: no import errors. If `solders` isn't installed, the import will fail — `solders` was added in chunk 10 Task 10.1, so this is a sanity check that chunks 10+ all landed cleanly.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/conftest.py
git commit -m "test(e2e): solana helpers — airdrop, native send, spl-token CLI wrapper"
```

### Task 14.3: E2E test — native SOL transfer → webhook

Mirrors chunk 9 Task 9.5's structure. Drives the API to create a Solana chain, an HTTP channel, and a `native_transfer` subscription scoped to a specific recipient pubkey. Starts the worker in-process, airdrops then transfers SOL between two fresh keypairs N times, and asserts N webhook payloads arrived with `kind="native_transfer"`.

**Subscription scoping:** `address=None` (native transfers are chain-wide, not contract-scoped) + `arg_filters={"to": <recipient_pubkey_b58>}`. The `arg_filters` keeps the test deterministic — the validator's internal vote txs, plus the funded_sender's airdrop, also generate balance diffs the parser may pick up, but those won't have `to == <our recipient>` so they don't match the subscription. (Confirmed by the chunk 12 case-folding fix: base58 pubkeys are matched case-sensitively, so a typo in the recipient pubkey would surface as zero matches rather than spurious matches.)

**Why N=2 (not 3 like chunk 9):** each Solana transfer settles in ~400 ms, but the validator's slot-production cadence means consecutive transfers can land in the same slot. N=2 with a short gap ensures different slots, which exercises the worker's polling loop more meaningfully than a single tx without inflating wallclock.

**Files:**
- Create: `tests/e2e/test_solana_native_e2e.py`

- [ ] **Step 1: Write the test**

```python
# tests/e2e/test_solana_native_e2e.py
"""Solana native SOL transfer E2E.

Drives:
    solana-test-validator
        -> SolanaAdapter (chunk 10)
        -> SolanaParserPipeline (SolNativeTransferParser from chunk 11)
        -> Matcher (chunk-12 case-aware case-folding)
        -> Notifier
        -> HttpChannel
        -> in-process webhook receiver (M1 fixture)

Asserts payload conforms to spec §8 with `kind="native_transfer"`.
"""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from solders.keypair import Keypair

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import run_worker
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

from tests.e2e.conftest import _solana_send_native_transfer  # type: ignore[attr-defined]

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


TRANSFER_COUNT = 2
LAMPORTS_PER_TRANSFER = 1_000_000  # 0.001 SOL each
DELIVERY_TIMEOUT_S = 60.0


@pytest_asyncio.fixture
async def db_url(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'e2e_sol_native.sqlite'}"


@pytest_asyncio.fixture
async def initialised_db(db_url: str) -> AsyncIterator[Database]:
    d = Database(db_url)
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def test_solana_native_transfer_to_webhook(
    solana_validator, webhook_receiver, funded_sender,
    initialised_db, db_url, redis_url,
) -> None:
    """Native SOL transfers from `funded_sender` to a fresh recipient land
    as `kind="native_transfer"` webhook deliveries with chain_id="sol-local".
    """
    recipient = Keypair()
    settings = Settings(
        database={"url": db_url},
        redis={"url": redis_url},
    )

    # 1) Seed config via the real API.
    bus_writer = RedisBus(url=redis_url)
    await bus_writer.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: initialised_db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test",
        ) as c:
            r = await c.post("/api/chains", json={
                "id": "sol-local",
                "kind": "solana",
                "rpc_http": solana_validator.rpc_url,
                "rpc_ws": None,
                "confirmations": 0,
                "commitment": "confirmed",
                "poll_interval_ms": 400,
                "enabled": True,
            })
            assert r.status_code == 201, r.text

            r = await c.post("/api/channels", json={
                "name": "e2e-sol-native-hook", "type": "http",
                "config": {"url": webhook_receiver.url, "method": "POST"},
            })
            assert r.status_code == 201
            channel_id = r.json()["id"]

            r = await c.post("/api/subscriptions", json={
                "name": "watch-recipient-native",
                "chain_id": "sol-local",
                "address": None,                  # native transfers are chain-wide
                "abi_id": None,
                "match_kind": "native_transfer",
                "match_name": None,
                "arg_filters": {"to": str(recipient.pubkey())},  # case-sensitive
                "enabled": True,
            })
            assert r.status_code == 201, r.text
            sub_id = r.json()["id"]

            r = await c.post(f"/api/subscriptions/{sub_id}/channels",
                             json={"channel_id": channel_id})
            assert r.status_code == 204
    finally:
        await bus_writer.disconnect()

    # 2) Start the worker. Give it a moment to hot-load the snapshot and
    #    register the Solana chain runner.
    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(settings, stop_event))
    await asyncio.sleep(2.0)

    # 3) Submit N native transfers from `funded_sender` to `recipient`.
    submitted_sigs: list[str] = []
    try:
        for _ in range(TRANSFER_COUNT):
            sig = await _solana_send_native_transfer(
                solana_validator.rpc_url,
                funded_sender.keypair,
                recipient.pubkey(),
                LAMPORTS_PER_TRANSFER,
            )
            submitted_sigs.append(sig)
            # Force a tiny gap so the two transfers land in distinct slots
            # — Solana slots are ~400 ms.
            await asyncio.sleep(0.6)

        # 4) Wait for the receiver to collect N payloads.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DELIVERY_TIMEOUT_S
        timed_out = False
        while True:
            matches = [
                p for p in webhook_receiver.received
                if p.get("event", {}).get("args", {}).get("to") == str(recipient.pubkey())
            ]
            if len(matches) >= TRANSFER_COUNT:
                break
            if loop.time() > deadline:
                timed_out = True
                break
            await asyncio.sleep(0.5)
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(worker_task, timeout=10.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(worker_task, timeout=3.0)

    matches = [
        p for p in webhook_receiver.received
        if p.get("event", {}).get("args", {}).get("to") == str(recipient.pubkey())
    ]
    if timed_out:
        pytest.fail(
            f"only {len(matches)}/{TRANSFER_COUNT} native-transfer payloads received "
            f"within {DELIVERY_TIMEOUT_S}s (total deliveries: {len(webhook_receiver.received)})"
        )

    # 5) Assert payload shape per spec §8 with kind=native_transfer.
    sample = matches[0]
    assert sample["subscription_id"] == sub_id
    assert sample["subscription_name"] == "watch-recipient-native"
    assert sample["chain_id"] == "sol-local"
    assert "delivery_id" in sample
    assert "delivered_at" in sample

    ev = sample["event"]
    assert ev["kind"] == "native_transfer"
    # native_transfer has no contract address on Solana (system program is implicit).
    assert ev.get("address") in (None, "")
    assert isinstance(ev["block_number"], int) and ev["block_number"] >= 1
    # Solana block_hash is base58, NOT 0x-prefixed hex (chunk 12 case-folding fix).
    assert isinstance(ev["block_hash"], str) and len(ev["block_hash"]) > 0
    assert not ev["block_hash"].startswith("0x")
    # tx_hash on Solana is the base58 signature; chunk-11 parser sets it directly.
    assert isinstance(ev["tx_hash"], str) and len(ev["tx_hash"]) > 0
    assert ev["tx_hash"] in submitted_sigs

    assert "from" in ev["args"] and "to" in ev["args"] and "value" in ev["args"]
    assert ev["args"]["from"] == str(funded_sender.keypair.pubkey())
    assert ev["args"]["to"] == str(recipient.pubkey())
    assert ev["args"]["value"] == str(LAMPORTS_PER_TRANSFER)
```

- [ ] **Step 2: Run the test**

```bash
pytest tests/e2e/test_solana_native_e2e.py -v -m e2e
```

Expected on a host with `solana-test-validator` installed: 1 PASS in ~20–40 s (5–10 s validator boot + 2 s airdrop + ~2 × 0.5 s transfers + worker poll headroom). On a host without the validator: 1 SKIP.

If the test fails with `BlockhashNotFound`, the airdrop hasn't fully settled — increase `await asyncio.sleep(2.0)` after airdrop in `funded_sender` to 4 s.

If the test fails with "0/2 native-transfer payloads received", check:
1. `RedisBus` ping passing (worker started cleanly).
2. The `chains` API call accepted `commitment="confirmed"` (chunk 10 schema must have landed).
3. `arg_filters={"to": str(recipient.pubkey())}` matches the parser's `args["to"]` (chunk 11's parser stringifies the pubkey via `str(...)`).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_solana_native_e2e.py
git commit -m "test(e2e): solana native-transfer validator → worker → webhook"
```

### Task 14.4: E2E test — SPL token transfer → webhook

Same skeleton as Task 14.3, but the subscription is scoped to a freshly-minted SPL token (`address=<mint_b58>`, `match_kind="token_transfer"`). The `spl_mint` fixture handles all the upfront token setup; the test body issues N `spl-token transfer` CLI calls and asserts N webhook deliveries with `kind="token_transfer"` and `event.args.mint` matching the mint address.

**Why `address=<mint>` is the right scope:** chunk 12's `SplTransferParser` sets `event.contract=<mint base58>`. The matcher maps `subscription.address` against `event.contract`, so `address=<mint>` filters on the specific mint. We do NOT also set `arg_filters={"mint": ...}` — that would be a redundant double-filter; `address` already does the work.

**Decimals note:** the mint in `spl_mint` is created with `--decimals 6` (six decimal places). When `spl-token transfer <mint> <amount> <to>` runs with a *human-readable* amount like `0.1`, the on-chain `Transfer`/`TransferChecked` ix sees the raw base-units integer `100000`. The chunk-12 parser emits `args.value=str(base_units_int)`, so the E2E expects `"100000"`, not `"0.1"`.

**Files:**
- Create: `tests/e2e/test_solana_spl_e2e.py`

- [ ] **Step 1: Write the test**

```python
# tests/e2e/test_solana_spl_e2e.py
"""Solana SPL token transfer E2E.

Drives:
    solana-test-validator (with spl-token CLI building the mint+ATA setup)
        -> SolanaAdapter (chunk 10)
        -> SolanaParserPipeline (SplTransferParser from chunk 12)
        -> Matcher (case-sensitive on base58 mint, chunk-12 fix)
        -> Notifier
        -> HttpChannel
        -> in-process webhook receiver

Asserts payload conforms to spec §8 with `kind="token_transfer"` and
`event.args.mint == <freshly-minted mint pubkey>`.
"""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from apps.web.deps import get_bus, get_db
from apps.web.main import create_app
from apps.worker.main import run_worker
from core.bus.redis_bus import RedisBus
from core.config.db import Database
from core.config.models import Base
from core.settings import Settings

from tests.e2e.conftest import _spl_transfer_one  # type: ignore[attr-defined]

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


TRANSFER_COUNT = 2
# 0.1 token at 6 decimals == 100000 base units. spl-token's CLI takes the
# decimal form; the parser sees and emits the base-units integer.
TRANSFER_DECIMAL_AMOUNT = "0.1"
TRANSFER_BASE_UNITS = 100_000
DELIVERY_TIMEOUT_S = 60.0


@pytest_asyncio.fixture
async def db_url(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'e2e_sol_spl.sqlite'}"


@pytest_asyncio.fixture
async def initialised_db(db_url: str) -> AsyncIterator[Database]:
    d = Database(db_url)
    await d.connect()
    async with d.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield d
    await d.disconnect()


async def test_solana_spl_transfer_to_webhook(
    solana_validator, webhook_receiver, spl_mint,
    initialised_db, db_url, redis_url,
) -> None:
    """SPL token transfers from `spl_mint.sender` to `spl_mint.recipient` land
    as `kind="token_transfer"` webhook deliveries with the mint pubkey echoed
    in `event.address` (case-sensitive).
    """
    settings = Settings(
        database={"url": db_url},
        redis={"url": redis_url},
    )

    # 1) Seed config via the real API.
    bus_writer = RedisBus(url=redis_url)
    await bus_writer.connect()
    try:
        app = create_app(lifespan=None)
        app.dependency_overrides[get_db] = lambda: initialised_db
        app.dependency_overrides[get_bus] = lambda: bus_writer

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test",
        ) as c:
            r = await c.post("/api/chains", json={
                "id": "sol-local",
                "kind": "solana",
                "rpc_http": solana_validator.rpc_url,
                "rpc_ws": None,
                "confirmations": 0,
                "commitment": "confirmed",
                "poll_interval_ms": 400,
                "enabled": True,
            })
            assert r.status_code == 201, r.text

            r = await c.post("/api/channels", json={
                "name": "e2e-sol-spl-hook", "type": "http",
                "config": {"url": webhook_receiver.url, "method": "POST"},
            })
            assert r.status_code == 201
            channel_id = r.json()["id"]

            r = await c.post("/api/subscriptions", json={
                "name": "spl-on-mint",
                "chain_id": "sol-local",
                "address": spl_mint.mint_address,        # case-sensitive base58
                "abi_id": None,
                "match_kind": "token_transfer",
                "match_name": None,
                "arg_filters": {},
                "enabled": True,
            })
            assert r.status_code == 201, r.text
            sub_id = r.json()["id"]

            r = await c.post(f"/api/subscriptions/{sub_id}/channels",
                             json={"channel_id": channel_id})
            assert r.status_code == 204
    finally:
        await bus_writer.disconnect()

    # 2) Start the worker.
    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(settings, stop_event))
    await asyncio.sleep(2.0)

    # 3) Submit N SPL transfers via the CLI.
    submitted_sigs: list[str] = []
    try:
        for _ in range(TRANSFER_COUNT):
            sig = await asyncio.get_running_loop().run_in_executor(
                None,
                _spl_transfer_one,
                solana_validator.rpc_url,
                spl_mint.sender_keypair_path,
                spl_mint.mint_address,
                TRANSFER_DECIMAL_AMOUNT,
                spl_mint.recipient_pubkey_b58,
            )
            submitted_sigs.append(sig)
            # spl-token transfer is blocking + slot-driven; small inter-call gap
            # keeps consecutive transfers in distinct slots.
            await asyncio.sleep(0.6)

        # 4) Wait for the receiver.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DELIVERY_TIMEOUT_S
        timed_out = False
        while True:
            matches = [
                p for p in webhook_receiver.received
                if p.get("event", {}).get("kind") == "token_transfer"
                and p.get("event", {}).get("args", {}).get("mint") == spl_mint.mint_address
            ]
            if len(matches) >= TRANSFER_COUNT:
                break
            if loop.time() > deadline:
                timed_out = True
                break
            await asyncio.sleep(0.5)
    finally:
        stop_event.set()
        try:
            await asyncio.wait_for(worker_task, timeout=10.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
                await asyncio.wait_for(worker_task, timeout=3.0)

    matches = [
        p for p in webhook_receiver.received
        if p.get("event", {}).get("kind") == "token_transfer"
        and p.get("event", {}).get("args", {}).get("mint") == spl_mint.mint_address
    ]
    if timed_out:
        pytest.fail(
            f"only {len(matches)}/{TRANSFER_COUNT} token-transfer payloads received "
            f"within {DELIVERY_TIMEOUT_S}s (total deliveries: {len(webhook_receiver.received)})"
        )

    # 5) Assert payload shape per spec §8 with kind=token_transfer.
    sample = matches[0]
    assert sample["subscription_id"] == sub_id
    assert sample["subscription_name"] == "spl-on-mint"
    assert sample["chain_id"] == "sol-local"

    ev = sample["event"]
    assert ev["kind"] == "token_transfer"
    # On Solana, `event.address` is the mint pubkey (base58, case-sensitive).
    assert ev["address"] == spl_mint.mint_address
    assert isinstance(ev["block_number"], int) and ev["block_number"] >= 1
    assert isinstance(ev["tx_hash"], str) and ev["tx_hash"] in submitted_sigs

    assert {"from", "to", "value", "mint"}.issubset(ev["args"].keys())
    assert ev["args"]["mint"] == spl_mint.mint_address
    assert ev["args"]["from"] == str(spl_mint.sender_keypair.pubkey())
    assert ev["args"]["to"] == spl_mint.recipient_pubkey_b58
    assert ev["args"]["value"] == str(TRANSFER_BASE_UNITS)
```

- [ ] **Step 2: Run the test**

```bash
pytest tests/e2e/test_solana_spl_e2e.py -v -m e2e
```

Expected on a host with `solana-test-validator` AND `spl-token` installed: 1 PASS in ~30–50 s. On a host missing either tool: 1 SKIP.

Common failure modes:
- `spl-token CLI failed (rc=1) … insufficient funds for rent`: the `funded_sender` airdrop wasn't large enough to cover mint creation + ATA rent + N transfer ATAs. Increase the airdrop from `1_000_000_000` to `2_000_000_000` lamports in `funded_sender`.
- `0/2 token-transfer payloads received`: confirm chunk 12's case-folding fix landed. The mint address is base58 and case-sensitive; the seeded `subscription.address` and the parser's `event.contract` must match byte-for-byte. If they don't, the matcher silently rejects.
- `KeyError: 'mint'`: chunk 12's `SplTransferParser` is supposed to populate `args["mint"]` for both `Transfer` and `TransferChecked`. If only `TransferChecked` populates it, the CLI may be emitting `Transfer` here; revisit chunk 12's mint resolution path (`meta.post_token_balances` for `Transfer`).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/test_solana_spl_e2e.py
git commit -m "test(e2e): solana spl-token transfer validator → worker → webhook"
```

### Task 14.5: Close-out — full regression + tag `m2-complete`

The M2 close-out. Same skeleton as chunk 9 Task 9.6, with one extra step: every chunk-1-through-14 test runs together to surface any cross-chunk regressions before the tag lands.

- [ ] **Step 1: Run the full unit suite**

```bash
make test
```

Expected: every chunk 1–14 unit + integration test passes. Solana ITs (chunks 10, 11) SKIP on hosts without `solana-test-validator`; everything else runs.

If a unit test fails, fix it BEFORE proceeding. The tag must sit on green.

- [ ] **Step 2: Run the full E2E suite**

```bash
make test-e2e
```

Expected: four E2E tests PASS:
1. `tests/e2e/test_native_transfer_e2e.py` (M1 — Anvil native ETH transfer).
2. `tests/e2e/test_evm_erc20_e2e.py` (chunk 9 — Anvil ERC-20 transfer).
3. `tests/e2e/test_solana_native_e2e.py` (chunk 14 Task 14.3).
4. `tests/e2e/test_solana_spl_e2e.py` (chunk 14 Task 14.4).

Total wallclock on a warm host: ~2–3 min (solana-test-validator cold-start dominates the Solana side; both Solana tests share the session-scoped validator).

On a host without `solana-test-validator`: tests 3 & 4 SKIP, total wallclock ~1 min.
On a host without `spl-token`: test 4 SKIPs, test 3 still passes.

- [ ] **Step 3: Lint + typecheck**

```bash
make lint && make typecheck
```

Expected: clean. No new lints from the chunk-14 additions; no new mypy errors.

- [ ] **Step 4: Verify `git status` is clean**

```bash
git status
```

Expected: `working tree clean`. If anything is uncommitted, decide whether it belongs in a follow-up commit (`git add ... && git commit`) or should be reverted (`git restore ...`). The tag must sit on a clean tree.

- [ ] **Step 5: Tag `m2-complete`**

```bash
git tag m2-complete
git tag -l m2-complete   # verify
```

The `m2-complete` tag marks the end of M2: every spec §4 deliverable shipped (ERC-20 / ABI event / ABI call / Anchor IDL parsers; MQ / WS channels; Solana native + SPL parsers; hardened arg_filters; full EVM + Solana E2E).

`m2-complete` is informational — future chunks (M3 onward) don't read it. The tag exists so a human can `git checkout m2-complete` and find a known-good state: EVM + Solana both fully wired, two E2E tests for each chain, and every unit / integration suite green.

- [ ] **Step 6: Final commit (only if cleanup landed)**

If Step 4 surfaced anything worth committing (e.g. a trailing TODO or doc tweak found while writing the close-out), commit it now BEFORE the tag — `git tag` attaches to whatever HEAD points at, so a forgotten file becomes "post-tag" and the tag no longer reflects the working state.

If Step 4 was already clean, this step is a no-op.

```bash
git status   # final sanity
```

**Chunk 14 done. M2 complete.** Branch `feat/m2-design` is ready for review and merge to `main`. The merge should be a `--no-ff` merge (or squash, per repo convention) so the chunk history is preserved as a single logical M2 unit. Post-merge, push the `m2-complete` tag (`git push origin m2-complete`) so it's visible to other clones.
