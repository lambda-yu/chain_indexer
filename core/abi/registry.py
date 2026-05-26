from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

import structlog

from core.abi.decoder import (
    anchor_event_discriminator,
    build_anchor_event_struct,
    decode_event,
    decode_function_call,
    event_topic0,
    function_selector,
)
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
        self._topic0_index: dict[str, tuple[str, str]] = {}  # topic0 → (abi_id, event_name)
        self._topic0_cache: dict[str, tuple[str, Any]] = {}
        self._selector_index: dict[str, tuple[str, str]] = {}  # selector → (abi_id, fn_name)
        self._selector_cache: dict[str, tuple[str, Any]] = {}
        # Anchor IDL: (program_id, disc_hex) → (abi_id, event_name, struct)
        self._anchor_index: dict[tuple[str, str], tuple[str, str, Any]] = {}
        self._anchor_cache: dict[tuple[str, str], tuple[str, Any] | None] = {}

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
        self._rebuild_topic0_index()
        self._topic0_cache.clear()
        self._rebuild_selector_index()
        self._selector_cache.clear()
        self._rebuild_anchor_index()
        self._anchor_cache.clear()
        log.info("abi_registry.refreshed", count=len(new_abis))

    def _evict(self, abi_id: str) -> None:
        for key in list(self._decoders.keys()):
            if key[0] == abi_id:
                self._decoders.pop(key, None)
        self._hashes.pop(abi_id, None)

    def _rebuild_topic0_index(self) -> None:
        idx: dict[str, tuple[str, str]] = {}
        for abi_id, abi in self._abis.items():
            body = abi.body if isinstance(abi.body, list) else [abi.body]
            for entry in body:
                if entry.get("type") != "event":
                    continue
                try:
                    t0 = event_topic0(entry).lower()
                except Exception:  # noqa: BLE001
                    log.warning(
                        "abi_registry.topic0_compute_failed",
                        abi_id=abi_id,
                        event=entry.get("name"),
                    )
                    continue
                if t0 in idx:
                    log.warning(
                        "abi_registry.topic0_collision",
                        topic0=t0,
                        first=idx[t0],
                        second=(abi_id, entry.get("name")),
                    )
                    continue
                idx[t0] = (abi_id, entry.get("name", ""))
        self._topic0_index = idx

    def lookup_event_by_topic0(
        self, topic0: str,
    ) -> tuple[str, EventDecoder] | None:
        key = topic0.lower()
        cached = self._topic0_cache.get(key)
        if cached is not None:
            return cached
        entry = self._topic0_index.get(key)
        if entry is None:
            return None
        abi_id, event_name = entry
        decoder = self.get_event_decoder(abi_id, topic0)
        result = (event_name, decoder)
        self._topic0_cache[key] = result
        return result

    def _rebuild_selector_index(self) -> None:
        idx: dict[str, tuple[str, str]] = {}
        for abi_id, abi in self._abis.items():
            body = abi.body if isinstance(abi.body, list) else [abi.body]
            for entry in body:
                if entry.get("type") != "function":
                    continue
                try:
                    sel = function_selector(entry).lower()
                except Exception:  # noqa: BLE001
                    log.warning(
                        "abi_registry.selector_compute_failed",
                        abi_id=abi_id,
                        function=entry.get("name"),
                    )
                    continue
                if sel in idx:
                    log.warning(
                        "abi_registry.selector_collision",
                        selector=sel,
                        first=idx[sel],
                        second=(abi_id, entry.get("name")),
                    )
                    continue
                idx[sel] = (abi_id, entry.get("name", ""))
        self._selector_index = idx

    def lookup_function_by_selector(
        self, selector: str,
    ) -> tuple[str, CallDecoder] | None:
        key = selector.lower()
        cached = self._selector_cache.get(key)
        if cached is not None:
            return cached
        entry = self._selector_index.get(key)
        if entry is None:
            return None
        abi_id, fn_name = entry
        decoder = self.get_call_decoder(abi_id, selector)
        result = (fn_name, decoder)
        self._selector_cache[key] = result
        return result

    def _rebuild_anchor_index(self) -> None:
        idx: dict[tuple[str, str], tuple[str, str, Any]] = {}
        for abi_id, abi in self._abis.items():
            if abi.kind != "solana_idl":
                continue
            body = abi.body if isinstance(abi.body, dict) else {}
            program_id = body.get("metadata", {}).get("address")
            if not program_id:
                log.warning("abi_registry.idl_missing_program_id", abi_id=abi_id)
                continue
            for ev in body.get("events", []):
                name = ev.get("name", "")
                struct = build_anchor_event_struct(ev, types_section=body.get("types", []))
                if struct is None:
                    log.info("abi_registry.idl_event_unsupported_types", abi_id=abi_id, event_name=name)
                    continue
                disc = anchor_event_discriminator(name).hex()
                key = (program_id, disc)
                if key in idx:
                    log.warning("abi_registry.anchor_disc_collision", key=key, second_abi=abi_id)
                    continue
                idx[key] = (abi_id, name, struct)
        self._anchor_index = idx

    def lookup_idl_event_by_discriminator(
        self, program_id: str, discriminator_hex: str,
    ) -> tuple[str, Any] | None:
        key = (program_id, discriminator_hex)
        cached = self._anchor_cache.get(key)
        if cached is not None:
            return cached
        entry = self._anchor_index.get(key)
        if entry is None:
            return None
        _abi_id, event_name, struct = entry
        result = (event_name, struct)
        self._anchor_cache[key] = result
        return result

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

    EventDecoder = Callable[..., dict[str, Any]]
    CallDecoder = Callable[[str], dict[str, Any]]

    def get_event_decoder(self, abi_id: str, topic0: str) -> EventDecoder:
        key = (abi_id, topic0.lower())
        cached = self._decoders.get(key)
        if cached is not None:
            return cached  # type: ignore[no-any-return]

        a = self._abis.get(abi_id)
        if a is None:
            raise AbiNotFound(abi_id)

        body = a.body if isinstance(a.body, list) else [a.body]
        for entry in body:
            if entry.get("type") != "event":
                continue
            if event_topic0(entry).lower() == topic0.lower():
                event_abi = entry

                def _decoder(*, topics: list[str], data: str, _ev: dict[str, Any] = event_abi) -> dict[str, Any]:
                    return decode_event(_ev, topics, data)

                self._decoders[key] = _decoder
                return _decoder
        raise KeyError(f"no event with topic0 {topic0} in abi {abi_id}")

    def get_call_decoder(self, abi_id: str, selector: str) -> CallDecoder:
        key = (abi_id, selector.lower())
        cached = self._decoders.get(key)
        if cached is not None:
            return cached  # type: ignore[no-any-return]

        a = self._abis.get(abi_id)
        if a is None:
            raise AbiNotFound(abi_id)

        body = a.body if isinstance(a.body, list) else [a.body]
        for entry in body:
            if entry.get("type") != "function":
                continue
            if function_selector(entry).lower() == selector.lower():
                fn_abi = entry

                def _decoder(calldata: str, _fn: dict[str, Any] = fn_abi) -> dict[str, Any]:
                    return decode_function_call(_fn, calldata)

                self._decoders[key] = _decoder
                return _decoder
        raise KeyError(f"no function with selector {selector} in abi {abi_id}")
