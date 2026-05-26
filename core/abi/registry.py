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
