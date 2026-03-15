"""Per-partition offset watermark tracking for Drakkar framework."""

import bisect
from enum import StrEnum


class OffsetState(StrEnum):
    PENDING = "pending"
    COMPLETED = "completed"


class OffsetTracker:
    """Tracks per-partition offset completion for watermark-based commits.

    Offsets are registered as pending when work starts, and marked
    completed when processing finishes. The committable offset is the
    highest consecutive completed offset + 1 (the next offset to read).
    """

    def __init__(self) -> None:
        self._offsets: dict[int, OffsetState] = {}
        self._sorted_offsets: list[int] = []
        self._last_committed: int | None = None

    @property
    def pending_count(self) -> int:
        return sum(1 for s in self._offsets.values() if s == OffsetState.PENDING)

    @property
    def completed_count(self) -> int:
        return sum(1 for s in self._offsets.values() if s == OffsetState.COMPLETED)

    @property
    def total_tracked(self) -> int:
        return len(self._offsets)

    @property
    def last_committed(self) -> int | None:
        return self._last_committed

    def register(self, offset: int) -> None:
        """Mark an offset as pending (in-flight)."""
        if offset in self._offsets:
            return
        self._offsets[offset] = OffsetState.PENDING
        bisect.insort(self._sorted_offsets, offset)

    def complete(self, offset: int) -> None:
        """Mark an offset as completed."""
        if offset not in self._offsets:
            return
        self._offsets[offset] = OffsetState.COMPLETED

    def committable(self) -> int | None:
        """Return the next offset to commit (highest consecutive completed + 1).

        Returns None if no offsets can be committed yet.
        """
        if not self._sorted_offsets:
            return None

        commit_up_to: int | None = None

        for offset in self._sorted_offsets:
            if self._offsets[offset] == OffsetState.COMPLETED:
                commit_up_to = offset
            else:
                break

        if commit_up_to is None:
            return None

        return commit_up_to + 1

    def acknowledge_commit(self, committed_offset: int) -> None:
        """Clean up tracked offsets at or below committed_offset - 1.

        Called after a successful Kafka offset commit.
        """
        self._last_committed = committed_offset
        idx = bisect.bisect_left(self._sorted_offsets, committed_offset)
        for offset in self._sorted_offsets[:idx]:
            del self._offsets[offset]
        del self._sorted_offsets[:idx]

    def has_pending(self) -> bool:
        """Check if there are any pending (in-flight) offsets."""
        return any(s == OffsetState.PENDING for s in self._offsets.values())

    def clear(self) -> None:
        """Remove all tracked offsets."""
        self._offsets.clear()
        self._sorted_offsets.clear()
