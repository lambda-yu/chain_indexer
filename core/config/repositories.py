from __future__ import annotations

from typing import Any

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from core.config.models import (
    Abi,
    AbiKind,
    Chain,
    ChainKind,
    Channel,
    ChannelType,
    Checkpoint,
    ConfigVersion,
    DeliveryRecord,
    MatchKind,
    Subscription,
    SubscriptionChannel,
)


class ChainRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(
        self,
        *,
        id: str,
        kind: ChainKind,
        rpc_http: str,
        rpc_ws: str | None,
        confirmations: int,
        poll_interval_ms: int,
        enabled: bool,
        commitment: str | None = None,
        trace_internal_calls: bool = False,
        log_query_range_blocks: int = 100,
        slot_query_range_blocks: int = 1000,
        rpc_http_fallbacks: list[str] | None = None,
        rpc_timeout_ms: int = 10000,
    ) -> Chain:
        c = Chain(
            id=id, kind=kind, rpc_http=rpc_http, rpc_ws=rpc_ws,
            confirmations=confirmations, poll_interval_ms=poll_interval_ms,
            enabled=enabled, commitment=commitment,
            trace_internal_calls=trace_internal_calls,
            log_query_range_blocks=log_query_range_blocks,
            slot_query_range_blocks=slot_query_range_blocks,
            rpc_http_fallbacks=rpc_http_fallbacks if rpc_http_fallbacks is not None else [],
            rpc_timeout_ms=rpc_timeout_ms,
        )
        self.s.add(c)
        await self.s.flush()
        return c

    async def get(self, chain_id: str) -> Chain | None:
        r = await self.s.execute(select(Chain).where(Chain.id == chain_id))
        return r.scalar_one_or_none()

    async def list_enabled(self) -> list[Chain]:
        r = await self.s.execute(select(Chain).where(Chain.enabled.is_(True)))
        return list(r.scalars().all())

    async def list_all(self) -> list[Chain]:
        r = await self.s.execute(select(Chain))
        return list(r.scalars().all())

    async def update(self, chain_id: str, **fields: Any) -> None:
        await self.s.execute(sa_update(Chain).where(Chain.id == chain_id).values(**fields))

    async def delete(self, chain_id: str) -> None:
        c = await self.get(chain_id)
        if c is not None:
            await self.s.delete(c)


class ChannelRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(self, *, name: str, type: ChannelType, config: dict[str, Any]) -> Channel:
        c = Channel(name=name, type=type, config=config)
        self.s.add(c)
        await self.s.flush()
        return c

    async def get(self, channel_id: str) -> Channel | None:
        r = await self.s.execute(select(Channel).where(Channel.id == channel_id))
        return r.scalar_one_or_none()

    async def list_all(self) -> list[Channel]:
        r = await self.s.execute(select(Channel))
        return list(r.scalars().all())

    async def update(self, channel_id: str, **fields: Any) -> None:
        await self.s.execute(sa_update(Channel).where(Channel.id == channel_id).values(**fields))

    async def delete(self, channel_id: str) -> None:
        c = await self.get(channel_id)
        if c is not None:
            await self.s.delete(c)


class SubscriptionRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(
        self,
        *,
        name: str,
        chain_id: str,
        address: str | None,
        abi_id: str | None,
        match_kind: MatchKind,
        match_name: str | None,
        arg_filters: dict[str, Any],
        enabled: bool,
        start_block: int | None = None,
    ) -> Subscription:
        sub = Subscription(
            name=name, chain_id=chain_id, address=address, abi_id=abi_id,
            match_kind=match_kind, match_name=match_name, arg_filters=arg_filters,
            enabled=enabled, start_block=start_block,
        )
        self.s.add(sub)
        await self.s.flush()
        return sub

    async def get(self, sub_id: str) -> Subscription | None:
        r = await self.s.execute(select(Subscription).where(Subscription.id == sub_id))
        return r.scalar_one_or_none()

    async def list_all(self) -> list[Subscription]:
        r = await self.s.execute(select(Subscription))
        return list(r.scalars().all())

    async def list_enabled_with_channels(self) -> list[tuple[Subscription, list[Channel]]]:
        subs_res = await self.s.execute(
            select(Subscription).where(Subscription.enabled.is_(True))
        )
        subs = list(subs_res.scalars().all())
        out: list[tuple[Subscription, list[Channel]]] = []
        for sub in subs:
            ch_res = await self.s.execute(
                select(Channel)
                .join(SubscriptionChannel, SubscriptionChannel.channel_id == Channel.id)
                .where(SubscriptionChannel.subscription_id == sub.id)
            )
            out.append((sub, list(ch_res.scalars().all())))
        return out

    async def bind_channel(self, sub_id: str, channel_id: str) -> None:
        self.s.add(SubscriptionChannel(subscription_id=sub_id, channel_id=channel_id))
        await self.s.flush()

    async def unbind_channel(self, sub_id: str, channel_id: str) -> None:
        await self.s.execute(
            sa_delete(SubscriptionChannel).where(
                SubscriptionChannel.subscription_id == sub_id,
                SubscriptionChannel.channel_id == channel_id,
            )
        )

    async def update(self, sub_id: str, **fields: Any) -> None:
        await self.s.execute(sa_update(Subscription).where(Subscription.id == sub_id).values(**fields))

    async def delete(self, sub_id: str) -> None:
        sub = await self.get(sub_id)
        if sub is not None:
            await self.s.delete(sub)


class AbiRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(self, *, name: str, kind: AbiKind, body: Any) -> Abi:
        a = Abi(name=name, kind=kind, body=body)
        self.s.add(a)
        await self.s.flush()
        return a

    async def get(self, abi_id: str) -> Abi | None:
        r = await self.s.execute(select(Abi).where(Abi.id == abi_id))
        return r.scalar_one_or_none()

    async def list_all(self) -> list[Abi]:
        r = await self.s.execute(select(Abi))
        return list(r.scalars().all())

    async def delete(self, abi_id: str) -> None:
        a = await self.get(abi_id)
        if a is not None:
            await self.s.delete(a)


class CheckpointRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def get(self, chain_id: str) -> Checkpoint | None:
        r = await self.s.execute(select(Checkpoint).where(Checkpoint.chain_id == chain_id))
        return r.scalar_one_or_none()

    async def upsert(self, chain_id: str, *, last_block: int, last_block_hash: str) -> None:
        # SELECT-then-INSERT/UPDATE: deliberately portable across SQLite/PG/MySQL
        # instead of dialect-specific ON CONFLICT. Single-writer worker process,
        # so the race window is benign in practice.
        existing = await self.get(chain_id)
        if existing is None:
            self.s.add(
                Checkpoint(
                    chain_id=chain_id, last_block=last_block, last_block_hash=last_block_hash
                )
            )
        else:
            existing.last_block = last_block
            existing.last_block_hash = last_block_hash
        await self.s.flush()


class ConfigVersionRepo:
    """Single-row, monotonic version counter. Bumped on every config write."""

    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def get(self) -> int:
        r = await self.s.execute(select(ConfigVersion).where(ConfigVersion.id == 1))
        row = r.scalar_one_or_none()
        return row.version if row else 0

    async def bump(self) -> int:
        row = await self.s.get(ConfigVersion, 1)
        if row is None:
            row = ConfigVersion(id=1, version=1)
            self.s.add(row)
            await self.s.flush()
            return 1
        row.version += 1
        await self.s.flush()
        return row.version


class DeliveryRecordRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def create(
        self, *, subscription_id: str, channel_id: str, chain_id: str,
        event_payload: dict[str, Any], error: str | None = None, attempts: int = 1,
        status: str = "success",
    ) -> DeliveryRecord:
        from core.config.models import DeliveryStatus
        row = DeliveryRecord(
            subscription_id=subscription_id, channel_id=channel_id, chain_id=chain_id,
            event_payload=event_payload, error=error, attempts=attempts,
            status=DeliveryStatus(status),
        )
        self.s.add(row)
        await self.s.flush()
        return row

    async def get(self, delivery_id: str) -> DeliveryRecord | None:
        r = await self.s.execute(select(DeliveryRecord).where(DeliveryRecord.id == delivery_id))
        return r.scalar_one_or_none()

    async def list_all(
        self,
        limit: int = 100,
        subscription_id: str | None = None,
        status: str | None = None,
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

    async def mark_resolved(self, delivery_id: str) -> None:
        from datetime import datetime, timezone
        from core.config.models import DeliveryStatus
        await self.s.execute(
            sa_update(DeliveryRecord)
            .where(DeliveryRecord.id == delivery_id)
            .values(status=DeliveryStatus.resolved, resolved_at=datetime.now(timezone.utc))
        )

    async def cleanup_success(self, *, keep: int, batch: int) -> int:
        """Delete oldest status='success' rows so at most `keep` remain.

        Returns the number of rows actually deleted (≤ batch). Only touches
        status='success'; failed/retrying/resolved rows are never affected.
        """
        from sqlalchemy.engine import CursorResult

        from core.config.models import DeliveryStatus
        inner = (
            select(DeliveryRecord.id)
            .where(DeliveryRecord.status == DeliveryStatus.success)
            .order_by(DeliveryRecord.created_at.desc())
            .offset(keep)
            .limit(batch)
        )
        result = await self.s.execute(
            sa_delete(DeliveryRecord).where(DeliveryRecord.id.in_(inner))
        )
        assert isinstance(result, CursorResult)
        return result.rowcount or 0

    async def bump_attempt(self, delivery_id: str, *, error: str) -> None:
        """Increment attempts and overwrite error. Used by manual retry on failure."""
        await self.s.execute(
            sa_update(DeliveryRecord)
            .where(DeliveryRecord.id == delivery_id)
            .values(
                attempts=DeliveryRecord.attempts + 1,
                error=error,
            )
        )

    async def delete(self, delivery_id: str) -> None:
        await self.s.execute(sa_delete(DeliveryRecord).where(DeliveryRecord.id == delivery_id))
