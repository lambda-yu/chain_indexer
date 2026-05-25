from __future__ import annotations

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from core.chains.types import BlockHeader


@dataclass(frozen=True)
class ReorgEvent:
    """Result of `handle_new_head` when a reorg is detected.

    Fields:
    - `rewound_to`: height of the common ancestor still in the buffer (-1 if deep).
    - `new_branch`: ordered headers from ancestor+1 to the new head (empty if deep).
    - `dropped`: headers popped off the divergent branch (empty if deep).
    - `confirmed`: any headers that became confirmed (depth >= confirmations) as
      a result of the reorg's tip advance. Caller MUST consume these.
    - `deep`: True if no common ancestor was found within the buffer's history.
    - `divergent_oldest`: when deep, the oldest height the buffer held at time
      of detection (so the listener can log the affected range).
    - `new_head`: when deep, the header that triggered the reset.
    """

    rewound_to: int
    new_branch: list[BlockHeader] = field(default_factory=list)
    dropped: list[BlockHeader] = field(default_factory=list)
    confirmed: list[BlockHeader] = field(default_factory=list)
    deep: bool = False
    divergent_oldest: int = -1
    new_head: BlockHeader | None = None


class ConfirmationBuffer:
    """Sliding window of the last `confirmations` headers.

    Append a header to advance the tip. Any header whose depth (tip - height)
    reaches `confirmations` is emitted as "confirmed" and removed from the buffer.

    On parent_hash mismatch, call `handle_new_head` which walks back via
    `resolve_parent(n, hash) -> BlockHeader` to find the common ancestor.
    `resolve_parent` MUST return a header whose `.hash == hash` and `.number == n`;
    exceptions raised by it propagate to the caller.

    Deep reorg (no ancestor within buffer) causes the buffer to be reseeded with
    the new head so subsequent appends link cleanly.
    """

    def __init__(self, confirmations: int) -> None:
        if confirmations < 0:
            raise ValueError("confirmations must be >= 0")
        self._confirmations = confirmations
        self._buf: deque[BlockHeader] = deque()

    def __len__(self) -> int:
        return len(self._buf)

    def tip(self) -> BlockHeader | None:
        return self._buf[-1] if self._buf else None

    def append(self, head: BlockHeader) -> list[BlockHeader]:
        """Append a head that links cleanly to the current tip (or is the first).

        Returns the list of headers that just became "confirmed" (depth >= confirmations).
        Raises ValueError if the head does not link.
        """
        if self._buf and self._buf[-1].hash != head.parent_hash:
            raise ValueError(
                f"append({head.number}/{head.hash}) does not link to tip "
                f"({self._buf[-1].number}/{self._buf[-1].hash}); call handle_new_head instead"
            )
        self._buf.append(head)
        return self._drain_confirmed()

    def handle_new_head(
        self,
        head: BlockHeader,
        *,
        resolve_parent: Callable[[int, str], BlockHeader],
    ) -> ReorgEvent | list[BlockHeader]:
        """Process a head that may or may not link.

        Clean link -> behaves like `append`, returns the confirmed list (no ReorgEvent).
        Mismatch -> walks back via `resolve_parent` until a hash matches a buffered
        header, then replaces the divergent suffix and returns a ReorgEvent.
        """
        if not self._buf or self._buf[-1].hash == head.parent_hash:
            return self.append(head)

        # Walk back along the new branch.
        branch: list[BlockHeader] = [head]
        cursor = head
        by_hash = {h.hash: h for h in self._buf}
        max_walk = len(self._buf) + 1
        for _ in range(max_walk):
            if cursor.parent_hash in by_hash:
                ancestor = by_hash[cursor.parent_hash]
                # Pop divergent suffix from buffer; collect what we dropped.
                dropped: list[BlockHeader] = []
                while self._buf and self._buf[-1].hash != ancestor.hash:
                    dropped.append(self._buf.pop())
                dropped.reverse()  # restore oldest-first order
                # Append new branch (currently in reverse order).
                new_branch_ordered = list(reversed(branch))
                for h in new_branch_ordered:
                    self._buf.append(h)
                confirmed = self._drain_confirmed()
                return ReorgEvent(
                    rewound_to=ancestor.number,
                    new_branch=new_branch_ordered,
                    dropped=dropped,
                    confirmed=confirmed,
                )
            # Step one block back along the new branch.
            parent = resolve_parent(cursor.number - 1, cursor.parent_hash)
            branch.append(parent)
            cursor = parent

        # Exhausted the walk: deep reorg. Reset buffer to just the new head so
        # the listener can resume linking.
        divergent_oldest = self._buf[0].number if self._buf else -1
        self._buf.clear()
        self._buf.append(head)
        # Drain in case confirmations=0 (the new head is itself instantly confirmed).
        confirmed = self._drain_confirmed()
        return ReorgEvent(
            rewound_to=-1,
            new_branch=[],
            dropped=[],
            confirmed=confirmed,
            deep=True,
            divergent_oldest=divergent_oldest,
            new_head=head,
        )

    def _drain_confirmed(self) -> list[BlockHeader]:
        """Pop headers whose depth (tip - height) >= confirmations and return them."""
        if not self._buf:
            return []
        tip_num = self._buf[-1].number
        confirmed: list[BlockHeader] = []
        while self._buf and (tip_num - self._buf[0].number) >= self._confirmations:
            confirmed.append(self._buf.popleft())
        return confirmed
