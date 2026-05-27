"""Request/response models for the management API.

Schemas mirror the DB rows but are decoupled from ORM types so we can evolve
the wire format independently. UUID fields are strings (`uuid.uuid4()` is
already stringified in the ORM defaults — Chunk 2 Task 2.2).
"""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from core.matcher.filters import FilterError
from core.matcher.filters import validate as _validate_filter_keys

ArgFilterValue = str | int | bool | list[str | int | bool]

# ---- Chains ---------------------------------------------------------------


class ChainCreate(BaseModel):
    id: str = Field(min_length=1, max_length=64)
    kind: Literal["evm", "solana"]
    rpc_http: str = Field(min_length=1)
    rpc_ws: str | None = None
    confirmations: int = Field(ge=0, le=10_000, default=0)
    poll_interval_ms: int = Field(ge=100, le=60_000, default=3000)
    commitment: Literal["confirmed", "finalized"] | None = None
    trace_internal_calls: bool = False
    enabled: bool = True

    @model_validator(mode="after")
    def _check_kind_fields(self) -> ChainCreate:
        if self.kind == "solana" and self.commitment is None:
            raise ValueError("Solana chains must specify commitment")
        if self.kind == "evm" and self.commitment is not None:
            raise ValueError("EVM chains must not specify commitment")
        return self


class ChainOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    kind: str
    rpc_http: str
    rpc_ws: str | None
    confirmations: int
    poll_interval_ms: int
    commitment: str | None
    trace_internal_calls: bool | None
    enabled: bool


# ---- Channels -------------------------------------------------------------


class ChannelCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    type: Literal["mq", "http", "ws", "kafka", "rabbitmq"]
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
    arg_filters: dict[str, ArgFilterValue] = Field(default_factory=dict)
    start_block: int | None = None
    enabled: bool = True

    @field_validator("arg_filters")
    @classmethod
    def _check_operator_grammar(
        cls, v: dict[str, ArgFilterValue],
    ) -> dict[str, ArgFilterValue]:
        try:
            _validate_filter_keys(v)
        except FilterError as exc:
            raise ValueError(str(exc)) from exc
        return v


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
    start_block: int | None
    last_processed_block: int | None
    enabled: bool


class ChannelBindRequest(BaseModel):
    channel_id: str


class SubscriptionDetail(SubscriptionOut):
    channel_ids: list[str] = Field(default_factory=list)


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
