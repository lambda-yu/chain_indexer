"""Per-chain RPC-side log filter set, derived from the ConfigSnapshot.

Used by ChainRunner to drive `eth_getLogs(addresses=..., topics=...)` so the
node filters server-side instead of returning every log to the indexer.
"""
from __future__ import annotations

from dataclasses import dataclass

import structlog

from core.abi.decoder import event_topic0
from core.abi.registry import AbiRegistry
from core.config.snapshot import ConfigSnapshot
from core.parser.erc20 import ERC20_TRANSFER_TOPIC0  # re-exported below

log = structlog.get_logger(__name__)

__all__ = ["EvmLogFilterSet", "build_evm_log_filter", "ERC20_TRANSFER_TOPIC0"]

_LOG_CONSUMING_KINDS = frozenset({"event", "token_transfer"})


@dataclass(frozen=True)
class EvmLogFilterSet:
    """Filter set to pass into `eth_getLogs` for a chain.

    - `addresses=None`  → don't filter by address.
    - `topic0s=None`    → don't filter by topic0.
    - `skip_logs=True`  → don't call `eth_getLogs` at all.
    """

    addresses: list[str] | None
    topic0s: list[str] | None
    skip_logs: bool

    @property
    def topics_param(self) -> list[list[str]] | None:
        """Shape required by `eth_getLogs` `topics` field.

        Position 0 of the outer list matches log topic 0. The inner list is
        OR-of-candidates. Returns None when topic filtering is disabled.
        """
        return [list(self.topic0s)] if self.topic0s else None


def build_evm_log_filter(
    snapshot: ConfigSnapshot,
    chain_id: str,
    abi_registry: AbiRegistry | None,
) -> EvmLogFilterSet:
    """Build the filter set for `chain_id` from the snapshot's enabled subs."""
    relevant = [
        s for s in snapshot.subscriptions_for_chain(chain_id)
        if s.match_kind in _LOG_CONSUMING_KINDS
    ]
    if not relevant:
        return EvmLogFilterSet(addresses=None, topic0s=None, skip_logs=True)

    # Addresses
    addresses: list[str] | None
    if any(s.address is None for s in relevant):
        addresses = None
    else:
        addresses = sorted({s.address.lower() for s in relevant if s.address is not None})

    # Topic0s
    topic0s: list[str] | None = None
    topics_set: set[str] = set()
    bailed = False
    for s in relevant:
        if s.match_kind == "token_transfer":
            topics_set.add(ERC20_TRANSFER_TOPIC0)
            continue
        # match_kind == "event"
        if s.abi_id is None or s.match_name is None or abi_registry is None:
            bailed = True
            break
        t0 = _event_topic0_for(abi_registry, s.abi_id, s.match_name)
        if t0 is None:
            bailed = True
            break
        topics_set.add(t0)
    if not bailed:
        topic0s = sorted(topics_set)

    return EvmLogFilterSet(addresses=addresses, topic0s=topic0s, skip_logs=False)


def _event_topic0_for(registry: AbiRegistry, abi_id: str, event_name: str) -> str | None:
    """Compute topic0 for the named event in the given abi, or None on miss."""
    try:
        body = registry.get_body(abi_id)
    except Exception:  # noqa: BLE001
        return None
    entries = body if isinstance(body, list) else [body]
    for entry in entries:
        if entry.get("type") != "event":
            continue
        if entry.get("name") != event_name:
            continue
        try:
            return event_topic0(entry).lower()
        except Exception:  # noqa: BLE001
            log.warning("filter_set.event_topic0_failed", abi_id=abi_id, event_name=event_name)
            return None
    return None
