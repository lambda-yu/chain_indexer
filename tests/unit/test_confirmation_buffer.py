from __future__ import annotations

import pytest

from core.chains.confirmation_buffer import ConfirmationBuffer, ReorgEvent
from core.chains.types import BlockHeader


def H(n: int, h: str, parent: str) -> BlockHeader:
    return BlockHeader(number=n, hash=h, parent_hash=parent, timestamp=1700000000 + n)


def test_steady_state_emits_after_confirmations() -> None:
    """With confirmations=2, block N is emitted only when head reaches N+2."""
    buf = ConfirmationBuffer(confirmations=2)
    assert buf.append(H(1, "0x1", "0x0")) == []
    assert buf.append(H(2, "0x2", "0x1")) == []
    confirmed = buf.append(H(3, "0x3", "0x2"))
    assert [c.number for c in confirmed] == [1]
    confirmed = buf.append(H(4, "0x4", "0x3"))
    assert [c.number for c in confirmed] == [2]


def test_confirmations_zero_emits_immediately() -> None:
    """confirmations=0: every appended head is confirmed at once; buffer stays empty."""
    buf = ConfirmationBuffer(confirmations=0)
    confirmed = buf.append(H(1, "0x1", "0x0"))
    assert [c.number for c in confirmed] == [1]
    assert len(buf) == 0


def test_append_rejects_non_linking_head() -> None:
    buf = ConfirmationBuffer(confirmations=2)
    buf.append(H(1, "0x1", "0x0"))
    with pytest.raises(ValueError, match="does not link"):
        buf.append(H(2, "0x2", "0xWRONG"))


def test_handle_new_head_linking_returns_confirmed_list() -> None:
    """If the new head links cleanly, no reorg event; just returns the confirmed list."""
    buf = ConfirmationBuffer(confirmations=1)
    buf.append(H(1, "0x1", "0x0"))
    calls: list[tuple[int, str]] = []

    def resolve(n: int, h: str) -> BlockHeader:
        calls.append((n, h))
        raise AssertionError("resolve_parent must not be called on clean link")

    out = buf.handle_new_head(H(2, "0x2", "0x1"), resolve_parent=resolve)
    assert isinstance(out, list)
    assert [b.number for b in out] == [1]
    assert calls == []


def test_single_block_reorg_rewinds_one() -> None:
    """parent_hash mismatch one block back: rewinds to depth 1, replaces tip."""
    buf = ConfirmationBuffer(confirmations=3)
    buf.append(H(1, "0x1", "0x0"))
    buf.append(H(2, "0x2a", "0x1"))  # branch A at height 2
    # New head at 3, parent 0x2b — must walk back to find ancestor at height 1.
    result = buf.handle_new_head(
        H(3, "0x3b", "0x2b"),
        resolve_parent=lambda n, h: {
            (2, "0x2b"): H(2, "0x2b", "0x1"),
        }[(n, h)],
    )
    assert isinstance(result, ReorgEvent)
    assert result.rewound_to == 1
    assert [b.hash for b in result.new_branch] == ["0x2b", "0x3b"]
    assert [b.hash for b in result.dropped] == ["0x2a"]
    # Buffer tip is now the new branch.
    tip = buf.tip()
    assert tip is not None
    assert tip.hash == "0x3b"


def test_multi_block_reorg_walks_back_through_branch() -> None:
    """Reorg spans 3 buffered blocks: walk back through 3 resolve_parent calls."""
    buf = ConfirmationBuffer(confirmations=5)
    buf.append(H(1, "0x1", "0x0"))
    buf.append(H(2, "0x2a", "0x1"))
    buf.append(H(3, "0x3a", "0x2a"))
    buf.append(H(4, "0x4a", "0x3a"))
    # Incoming H(5, "0x5b", "0x4b"). Branch b shares ancestor at height 1.
    branch_b = {
        (4, "0x4b"): H(4, "0x4b", "0x3b"),
        (3, "0x3b"): H(3, "0x3b", "0x2b"),
        (2, "0x2b"): H(2, "0x2b", "0x1"),
    }
    result = buf.handle_new_head(
        H(5, "0x5b", "0x4b"),
        resolve_parent=lambda n, h: branch_b[(n, h)],
    )
    assert isinstance(result, ReorgEvent)
    assert result.deep is False
    assert result.rewound_to == 1
    assert [b.hash for b in result.new_branch] == ["0x2b", "0x3b", "0x4b", "0x5b"]
    assert [b.hash for b in result.dropped] == ["0x2a", "0x3a", "0x4a"]
    tip = buf.tip()
    assert tip is not None
    assert tip.hash == "0x5b"


def test_reorg_that_advances_tip_returns_newly_confirmed_in_event() -> None:
    """When a reorg's new branch advances the tip past `confirmations`, blocks that
    become confirmed must be reported on the event, not silently dropped."""
    buf = ConfirmationBuffer(confirmations=2)
    buf.append(H(10, "0x10", "0x9"))
    buf.append(H(11, "0x11a", "0x10"))
    # New head at 12 with parent 0x11b. Ancestor is 0x10.
    result = buf.handle_new_head(
        H(12, "0x12b", "0x11b"),
        resolve_parent=lambda n, h: {(11, "0x11b"): H(11, "0x11b", "0x10")}[(n, h)],
    )
    assert isinstance(result, ReorgEvent)
    # After rewind to 10 and appending [11b, 12b], tip=12, confirmations=2
    # → height 10 drains (depth=2 ≥ 2); 11b stays (depth=1).
    assert [b.hash for b in result.confirmed] == ["0x10"]


def test_deep_reorg_returns_actionable_event_and_resets_buffer() -> None:
    """If no common ancestor is found within the buffer, deep=True; buffer is
    reseeded with just the new head so subsequent appends link correctly."""
    buf = ConfirmationBuffer(confirmations=2)
    buf.append(H(10, "0x10", "0x9"))
    buf.append(H(11, "0x11", "0x10"))
    new_head = H(12, "0x12x", "0x11x")
    result = buf.handle_new_head(
        new_head,
        resolve_parent=lambda n, h: H(n, h, f"{h}-no-match"),  # never matches
    )
    assert isinstance(result, ReorgEvent)
    assert result.deep is True
    assert result.divergent_oldest == 10
    assert result.new_head is not None
    assert result.new_head.hash == "0x12x"
    # Buffer reseeded with the new head only.
    tip = buf.tip()
    assert tip is not None
    assert tip.hash == "0x12x"
    # Next append from the new branch must link cleanly.
    buf.append(H(13, "0x13x", "0x12x"))


def test_buffer_size_caps_at_confirmations() -> None:
    """Steady-state buffer size equals `confirmations` (oldest drains on each new head)."""
    buf = ConfirmationBuffer(confirmations=3)
    for i in range(1, 11):
        buf.append(H(i, f"0x{i}", f"0x{i - 1}"))
    assert len(buf) == 3
