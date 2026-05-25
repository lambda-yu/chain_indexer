from __future__ import annotations

import time
import uuid
from typing import Any

from core.config.snapshot import SnapshotSubscription
from core.parser.event import Event


def _now_unix() -> int:
    return int(time.time())


def _gen_id() -> str:
    return str(uuid.uuid4())


def build_payload(*, event: Event, subscription: SnapshotSubscription) -> dict[str, Any]:
    """Uniform notification payload (spec §8). Two dedupe keys:
    - logical: (chain_id, tx_hash, log_index, block_hash) — survives reorgs
    - delivery_id: per-attempt idempotency key for at-least-once retry
    """
    return {
        "subscription_id": subscription.id,
        "subscription_name": subscription.name,
        "chain_id": event.chain_id,
        "event": {
            "kind": event.kind,
            "name": event.name,
            "contract": event.contract,
            "block_number": event.block_number,
            "block_hash": event.block_hash,
            "block_timestamp": event.block_timestamp,
            "tx_hash": event.tx_hash,
            "tx_index": event.tx_index,
            "log_index": event.log_index,
            "args": dict(event.args),
        },
        "delivered_at": _now_unix(),
        "delivery_id": _gen_id(),
    }
