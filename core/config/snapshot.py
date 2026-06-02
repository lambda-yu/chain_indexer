from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from core.config.repositories import (
    AbiRepo,
    ChainRepo,
    ConfigVersionRepo,
    SubscriptionRepo,
)


@dataclass(frozen=True)
class SnapshotSubscription:
    id: str
    name: str
    chain_id: str
    address: str | None
    abi_id: str | None
    match_kind: str
    match_name: str | None
    arg_filters: dict[str, Any]
    enabled: bool
    channel_ids: list[str] = field(default_factory=list)
    start_block: int | None = None
    business_name: str | None = None


@dataclass(frozen=True)
class SnapshotChannel:
    id: str
    name: str
    type: str
    config: dict[str, Any]


@dataclass(frozen=True)
class SnapshotChain:
    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int
    commitment: str | None = None
    trace_internal_calls: bool = False
    log_query_range_blocks: int = 100
    slot_query_range_blocks: int = 1000
    rpc_http_fallbacks: list[str] = field(default_factory=list)
    rpc_timeout_ms: int = 10000


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

    def subscriptions_for_chain(self, chain_id: str) -> list[SnapshotSubscription]:
        return [s for s in self.subscriptions if s.chain_id == chain_id and s.enabled]

    def channels_for_subscription(
        self, sub: SnapshotSubscription
    ) -> list[SnapshotChannel]:
        by_id = {c.id: c for c in self.channels}
        return [by_id[cid] for cid in sub.channel_ids if cid in by_id]

    def abi_by_id(self, abi_id: str) -> SnapshotAbi | None:
        for a in self.abis:
            if a.id == abi_id:
                return a
        return None


async def load_snapshot(session: AsyncSession) -> ConfigSnapshot:
    """Build a ConfigSnapshot from the database in a single transaction."""
    version = await ConfigVersionRepo(session).get()
    chains_rows = await ChainRepo(session).list_enabled()
    abi_rows = await AbiRepo(session).list_all()
    sub_bindings = await SubscriptionRepo(session).list_enabled_with_channels()

    snap_chains = [
        SnapshotChain(
            id=c.id,
            kind=c.kind.value,
            rpc_http=c.rpc_http,
            rpc_ws=c.rpc_ws,
            confirmations=c.confirmations,
            poll_interval_ms=c.poll_interval_ms,
            commitment=c.commitment,
            trace_internal_calls=bool(c.trace_internal_calls) if c.trace_internal_calls is not None else False,
            log_query_range_blocks=c.log_query_range_blocks,
            slot_query_range_blocks=c.slot_query_range_blocks,
            rpc_http_fallbacks=c.rpc_http_fallbacks,
            rpc_timeout_ms=c.rpc_timeout_ms,
        )
        for c in chains_rows
    ]

    snap_abis = [
        SnapshotAbi(id=a.id, name=a.name, kind=a.kind.value, body=a.body)
        for a in abi_rows
    ]

    snap_channels_by_id: dict[str, SnapshotChannel] = {}
    snap_subs: list[SnapshotSubscription] = []
    for sub, channels in sub_bindings:
        # Skip subscriptions with no bound channels — no point processing events
        if not channels:
            continue
        for ch in channels:
            snap_channels_by_id.setdefault(
                ch.id,
                SnapshotChannel(id=ch.id, name=ch.name, type=ch.type.value, config=ch.config),
            )
        snap_subs.append(
            SnapshotSubscription(
                id=sub.id,
                name=sub.name,
                chain_id=sub.chain_id,
                address=sub.address,
                abi_id=sub.abi_id,
                match_kind=sub.match_kind.value,
                match_name=sub.match_name,
                arg_filters=sub.arg_filters or {},
                enabled=sub.enabled,
                channel_ids=[c.id for c in channels],
                start_block=sub.start_block,
                business_name=sub.business_name,
            )
        )

    # Only include chains that have at least one active subscription
    active_chain_ids = {s.chain_id for s in snap_subs}
    snap_chains = [c for c in snap_chains if c.id in active_chain_ids]

    return ConfigSnapshot(
        version=version,
        subscriptions=snap_subs,
        channels=list(snap_channels_by_id.values()),
        chains=snap_chains,
        abis=snap_abis,
    )
