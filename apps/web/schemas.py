"""Request/response models for the management API.

Schemas mirror the DB rows but are decoupled from ORM types so we can evolve
the wire format independently. UUID fields are strings (`uuid.uuid4()` is
already stringified in the ORM defaults — Chunk 2 Task 2.2).
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ---- Chains ---------------------------------------------------------------


class ChainCreate(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    kind: Literal["evm", "solana"]
    rpc_http: str = Field(min_length=1)
    rpc_ws: str | None = None
    confirmations: int = Field(ge=0, le=10_000)
    poll_interval_ms: int = Field(ge=100, le=60_000)
    enabled: bool = True


class ChainOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    kind: str  # serialized enum value
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int
    enabled: bool


# ---- Channels -------------------------------------------------------------


class ChannelCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    type: Literal["mq", "http", "ws"]
    config: dict[str, Any] = Field(default_factory=dict)


class ChannelOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    type: str
    config: dict[str, Any]


# ---- Subscriptions --------------------------------------------------------


class SubscriptionCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    chain_id: str
    address: str | None = None
    abi_id: str | None = None
    match_kind: Literal["native_transfer", "token_transfer", "event", "call"]
    match_name: str | None = None
    arg_filters: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    chain_id: str
    address: str | None
    abi_id: str | None
    match_kind: str
    match_name: str | None
    arg_filters: dict[str, Any]
    enabled: bool


class ChannelBindRequest(BaseModel):
    channel_id: str
