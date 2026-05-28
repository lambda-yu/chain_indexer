from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy import Enum as SAEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _uuid() -> str:
    return str(uuid.uuid4())


class Base(DeclarativeBase):
    pass


class ChainKind(enum.StrEnum):
    evm = "evm"
    solana = "solana"


class MatchKind(enum.StrEnum):
    native_transfer = "native_transfer"
    token_transfer = "token_transfer"
    event = "event"
    call = "call"


class AbiKind(enum.StrEnum):
    evm_abi = "evm_abi"
    solana_idl = "solana_idl"


class ChannelType(enum.StrEnum):
    mq = "mq"
    http = "http"
    ws = "ws"
    kafka = "kafka"
    rabbitmq = "rabbitmq"


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Chain(Base, TimestampMixin):
    __tablename__ = "chains"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[ChainKind] = mapped_column(SAEnum(ChainKind, name="chain_kind"), nullable=False)
    rpc_http: Mapped[str] = mapped_column(Text, nullable=False)
    rpc_ws: Mapped[str | None] = mapped_column(Text, nullable=True)
    confirmations: Mapped[int] = mapped_column(Integer, nullable=False, default=12)
    poll_interval_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=3000)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    commitment: Mapped[str | None] = mapped_column(String(32), nullable=True, default=None)
    trace_internal_calls: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=None)
    log_query_range_blocks: Mapped[int] = mapped_column(
        Integer, nullable=False, default=100, server_default="100"
    )
    slot_query_range_blocks: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1000, server_default="1000"
    )


class Abi(Base, TimestampMixin):
    __tablename__ = "abis"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[AbiKind] = mapped_column(SAEnum(AbiKind, name="abi_kind"), nullable=False)
    body: Mapped[dict[str, Any] | list[Any]] = mapped_column(JSON, nullable=False)


class Subscription(Base, TimestampMixin):
    __tablename__ = "subscriptions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    chain_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("chains.id", ondelete="CASCADE"), nullable=False, index=True
    )
    address: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    abi_id: Mapped[str | None] = mapped_column(
        ForeignKey("abis.id", ondelete="SET NULL"), nullable=True
    )
    match_kind: Mapped[MatchKind] = mapped_column(SAEnum(MatchKind, name="match_kind"), nullable=False)
    match_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    arg_filters: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    start_block: Mapped[int | None] = mapped_column(BigInteger, nullable=True, default=None)
    last_processed_block: Mapped[int | None] = mapped_column(BigInteger, nullable=True, default=None)


class Channel(Base, TimestampMixin):
    __tablename__ = "channels"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[ChannelType] = mapped_column(SAEnum(ChannelType, name="channel_type"), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


class SubscriptionChannel(Base):
    __tablename__ = "subscription_channels"

    subscription_id: Mapped[str] = mapped_column(
        ForeignKey("subscriptions.id", ondelete="CASCADE"), primary_key=True
    )
    channel_id: Mapped[str] = mapped_column(
        ForeignKey("channels.id", ondelete="CASCADE"), primary_key=True
    )


class Checkpoint(Base):
    __tablename__ = "checkpoints"

    chain_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("chains.id", ondelete="CASCADE"), primary_key=True
    )
    last_block: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_block_hash: Mapped[str] = mapped_column(String(80), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class ConfigVersion(Base):
    __tablename__ = "config_version"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)


class DeliveryStatus(enum.StrEnum):
    success = "success"
    failed = "failed"
    retrying = "retrying"
    resolved = "resolved"


class DeliveryRecord(Base):
    __tablename__ = "delivery_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    subscription_id: Mapped[str] = mapped_column(String(36), nullable=False)
    channel_id: Mapped[str] = mapped_column(String(36), nullable=False)
    chain_id: Mapped[str] = mapped_column(String(64), nullable=False)
    event_payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[DeliveryStatus] = mapped_column(
        String(16), nullable=False, default=DeliveryStatus.success
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
