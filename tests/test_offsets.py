"""Tests for Drakkar offset watermark tracker."""

from drakkar.offsets import OffsetState, OffsetTracker


def test_empty_tracker():
    tracker = OffsetTracker()
    assert tracker.committable() is None
    assert tracker.total_tracked == 0
    assert tracker.pending_count == 0
    assert tracker.completed_count == 0
    assert not tracker.has_pending()


def test_register_single_offset():
    tracker = OffsetTracker()
    tracker.register(100)
    assert tracker.total_tracked == 1
    assert tracker.pending_count == 1
    assert tracker.committable() is None
    assert tracker.has_pending()


def test_complete_single_offset():
    tracker = OffsetTracker()
    tracker.register(100)
    tracker.complete(100)
    assert tracker.completed_count == 1
    assert tracker.pending_count == 0
    assert tracker.committable() == 101


def test_watermark_with_gap():
    tracker = OffsetTracker()
    for offset in [100, 101, 102, 103]:
        tracker.register(offset)

    tracker.complete(100)
    tracker.complete(101)
    tracker.complete(103)  # gap at 102

    assert tracker.committable() == 102  # can only commit up to 101


def test_watermark_gap_fills():
    tracker = OffsetTracker()
    for offset in [100, 101, 102, 103]:
        tracker.register(offset)

    tracker.complete(100)
    tracker.complete(103)
    tracker.complete(101)
    assert tracker.committable() == 102  # still blocked at 102

    tracker.complete(102)
    assert tracker.committable() == 104  # all done, next offset is 104


def test_register_idempotent():
    tracker = OffsetTracker()
    tracker.register(100)
    tracker.register(100)
    assert tracker.total_tracked == 1


def test_complete_unknown_offset_is_noop():
    tracker = OffsetTracker()
    tracker.complete(999)  # should not raise
    assert tracker.total_tracked == 0


def test_acknowledge_commit_cleans_up():
    tracker = OffsetTracker()
    for offset in [100, 101, 102, 103]:
        tracker.register(offset)
        tracker.complete(offset)

    assert tracker.committable() == 104
    tracker.acknowledge_commit(104)

    assert tracker.total_tracked == 0
    assert tracker.last_committed == 104


def test_acknowledge_commit_partial():
    tracker = OffsetTracker()
    for offset in [100, 101, 102, 103]:
        tracker.register(offset)

    tracker.complete(100)
    tracker.complete(101)
    tracker.acknowledge_commit(102)  # committed up to 101

    assert tracker.total_tracked == 2  # 102 and 103 remain
    assert tracker.last_committed == 102


def test_clear():
    tracker = OffsetTracker()
    for offset in [100, 101, 102]:
        tracker.register(offset)
    tracker.complete(100)

    tracker.clear()
    assert tracker.total_tracked == 0
    assert tracker.committable() is None
    assert not tracker.has_pending()


def test_out_of_order_registration():
    tracker = OffsetTracker()
    tracker.register(103)
    tracker.register(100)
    tracker.register(102)
    tracker.register(101)

    tracker.complete(100)
    tracker.complete(101)
    assert tracker.committable() == 102


def test_consecutive_commit_cycles():
    tracker = OffsetTracker()

    # first batch
    for offset in [0, 1, 2]:
        tracker.register(offset)
        tracker.complete(offset)
    assert tracker.committable() == 3
    tracker.acknowledge_commit(3)

    # second batch
    for offset in [3, 4, 5]:
        tracker.register(offset)
        tracker.complete(offset)
    assert tracker.committable() == 6
    tracker.acknowledge_commit(6)

    assert tracker.total_tracked == 0
    assert tracker.last_committed == 6


def test_has_pending_with_mixed_state():
    tracker = OffsetTracker()
    tracker.register(10)
    tracker.register(11)
    tracker.complete(10)
    assert tracker.has_pending()  # 11 is still pending

    tracker.complete(11)
    assert not tracker.has_pending()


def test_pending_count_matches_a_full_recount_through_every_transition():
    """The incrementally-maintained counter must never drift from reality.

    ``pending_count`` is O(1) precisely because it is no longer recomputed:
    it is read twice per message and polled every 50ms while draining, and a
    scan degrades exactly when the tracked set grows. That makes the counter
    load-bearing, so it is pinned here against a brute-force recount after
    every kind of transition — including the idempotent and no-op paths that
    are the easy ones to miscount.
    """
    tracker = OffsetTracker()

    def recount() -> int:
        return sum(1 for s in tracker._offsets.values() if s is OffsetState.PENDING)

    def check(label: str) -> None:
        assert tracker.pending_count == recount(), label
        assert tracker.completed_count == len(tracker._offsets) - recount(), label
        assert tracker.has_pending() == (recount() > 0), label

    for offset in range(10):
        tracker.register(offset)
    check('after register')

    tracker.register(3)  # duplicate registration is a no-op
    check('after duplicate register')

    for offset in (0, 1, 2, 5):
        tracker.complete(offset)
    check('after complete')

    tracker.complete(5)  # completing twice must not double-decrement
    check('after duplicate complete')

    tracker.complete(99)  # unknown offset is a no-op
    check('after unknown complete')

    committable = tracker.committable()
    assert committable == 3, 'offsets 0-2 are the gap-free completed prefix'
    tracker.acknowledge_commit(committable)
    check('after acknowledge_commit')

    tracker.clear()
    check('after clear')
    assert tracker.pending_count == 0
