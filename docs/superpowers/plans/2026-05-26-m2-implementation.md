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

