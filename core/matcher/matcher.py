from __future__ import annotations

from collections.abc import Iterable

import structlog

from core.config.snapshot import ConfigSnapshot, SnapshotChannel, SnapshotSubscription
from core.matcher.filters import evaluate
from core.parser.event import Event

log = structlog.get_logger(__name__)


def _addr_norm(s: str) -> str:
    return s.lower() if s.startswith("0x") else s


class Matcher:
    """Index subscriptions by `(chain_id, match_kind)` for O(1) candidate lookup.

    Within the candidate set, address (case-insensitive), match_name, and
    arg_filters are checked sequentially. The number of candidates per
    (chain, kind) is small in practice, so a linear scan is fine.

    The Matcher operates on a `ConfigSnapshot`; rebuild a fresh Matcher on
    hot-reload. It does NOT mutate the snapshot.
    """

    def __init__(self, snapshot: ConfigSnapshot) -> None:
        self._snapshot = snapshot
        self._by_key: dict[tuple[str, str], list[SnapshotSubscription]] = {}
        for s in snapshot.subscriptions:
            if not s.enabled:
                continue
            self._by_key.setdefault((s.chain_id, s.match_kind), []).append(s)
        self._channels: dict[str, SnapshotChannel] = {c.id: c for c in snapshot.channels}

    def match(self, event: Event) -> Iterable[tuple[SnapshotSubscription, list[SnapshotChannel]]]:
        candidates = self._by_key.get((event.chain_id, event.kind), [])
        for sub in candidates:
            try:
                if not self._matches(sub, event):
                    continue
                channels = [self._channels[cid] for cid in sub.channel_ids if cid in self._channels]
                yield sub, channels
            except Exception:  # noqa: BLE001 — per-event isolation (spec §9)
                log.exception("matcher.exception", subscription_id=sub.id, tx_hash=event.tx_hash)

    def _matches(self, sub: SnapshotSubscription, event: Event) -> bool:
        if sub.address is not None and (
            event.contract is None or _addr_norm(sub.address) != _addr_norm(event.contract)
        ):
            return False
        if sub.match_name is not None and event.name != sub.match_name:
            return False
        return evaluate(sub.arg_filters or {}, event.args)
