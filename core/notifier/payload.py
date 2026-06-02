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


def _safe(obj: Any) -> Any:
    if isinstance(obj, bytes):
        return "0x" + obj.hex()
    if isinstance(obj, dict):
        return {str(k): _safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe(v) for v in obj]
    if isinstance(obj, (int, float, bool, str)) or obj is None:
        return obj
    return str(obj)


def build_payload(
    *, event: Event, subscription: SnapshotSubscription, replay: bool = False
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "subscription_id": subscription.id,
        "subscription_name": subscription.name,
    }
    if subscription.business_name:
        payload["business_name"] = subscription.business_name
    payload["chain_id"] = event.chain_id
    payload["event"] = {
        "kind": event.kind,
        "name": event.name,
        "contract": _safe(event.contract),
        "block_number": event.block_number,
        "block_hash": _safe(event.block_hash),
        "block_timestamp": event.block_timestamp,
        "tx_hash": _safe(event.tx_hash),
        "tx_index": event.tx_index,
        "log_index": event.log_index,
        "args": _safe(dict(event.args)),
    }
    payload["delivered_at"] = _now_unix()
    payload["delivery_id"] = _gen_id()
    if replay:
        payload["replay"] = True
    return payload
