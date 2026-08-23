"""Tests for task cost / speed / throughput (drakkar/throughput.py)."""

from __future__ import annotations

import json

import pytest

from drakkar.config import ThroughputConfig
from drakkar.throughput import WINDOW_SECONDS, ThroughputTracker, parse_cost


def make_config(cost_label='file_size_bytes', min_cost=0.0) -> ThroughputConfig:
    return ThroughputConfig(cost_label=cost_label, min_cost=min_cost)


class RecorderSpy:
    def __init__(self) -> None:
        self.frames: list[dict] = []

    def broadcast_throughput(self, windows: dict) -> None:
        self.frames.append(windows)


# ---- parse_cost ---------------------------------------------------------------


@pytest.mark.parametrize(
    ('labels', 'expected'),
    [
        ({'file_size_bytes': '8388608'}, 8388608.0),
        ({'file_size_bytes': '0.5'}, 0.5),
        ({'file_size_bytes': 'not a number'}, None),
        ({'file_size_bytes': 'inf'}, None),
        ({'file_size_bytes': 'nan'}, None),
        ({'other_label': '10'}, None),
        ({}, None),
        (None, None),
    ],
)
def test_parse_cost_reads_the_configured_label_tolerantly(labels, expected):
    assert parse_cost(labels, make_config()) == expected


def test_parse_cost_is_none_when_feature_off():
    assert parse_cost({'file_size_bytes': '100'}, make_config(cost_label='')) is None


def test_parse_cost_applies_min_cost():
    config = make_config(min_cost=50_000)
    assert parse_cost({'file_size_bytes': '49999'}, config) is None
    assert parse_cost({'file_size_bytes': '50000'}, config) == 50_000.0


# ---- observe_completion --------------------------------------------------------


def test_observe_completion_returns_cost_and_speed():
    tracker = ThroughputTracker(make_config())
    result = tracker.observe_completion({'file_size_bytes': '1000000'}, duration_seconds=2.0, now=100.0)
    assert result == (1_000_000.0, 500_000.0)


def test_observe_completion_excludes_zero_duration_and_unlabeled():
    tracker = ThroughputTracker(make_config())
    assert tracker.observe_completion({'file_size_bytes': '100'}, duration_seconds=0.0, now=100.0) is None
    assert tracker.observe_completion({}, duration_seconds=1.0, now=100.0) is None
    assert tracker.window_stats(now=100.0)['30']['tasks'] == 0


def test_observe_completion_excludes_below_min_cost():
    tracker = ThroughputTracker(make_config(min_cost=50_000))
    assert tracker.observe_completion({'file_size_bytes': '30000'}, duration_seconds=1.0, now=100.0) is None
    assert tracker.window_stats(now=100.0)['30']['tasks'] == 0


# ---- window_stats --------------------------------------------------------------


def test_window_stats_slices_by_completion_recency():
    tracker = ThroughputTracker(make_config())
    # 100 cost-units at t=1040 (only in the 30 s window) and 200 at
    # t=1059.5 (in every window).
    tracker.observe_completion({'file_size_bytes': '100'}, 1.0, now=1040.0)
    tracker.observe_completion({'file_size_bytes': '200'}, 1.0, now=1059.5)

    stats = tracker.window_stats(now=1060.0)

    assert stats['1'] == {'throughput': 200.0, 'task_rate': 1.0, 'tasks': 1}
    assert stats['5'] == {'throughput': 40.0, 'task_rate': 0.2, 'tasks': 1}
    assert stats['30'] == {'throughput': 10.0, 'task_rate': pytest.approx(2 / 30, abs=0.001), 'tasks': 2}


def test_window_stats_reports_zeros_when_idle():
    # Quiet windows must report zeros, not vanish — the UI track draws an
    # honest dip during an incident instead of a gap.
    tracker = ThroughputTracker(make_config())
    stats = tracker.window_stats(now=500.0)
    assert set(stats) == {str(w) for w in WINDOW_SECONDS}
    for values in stats.values():
        assert values == {'throughput': 0.0, 'task_rate': 0.0, 'tasks': 0}


def test_completions_evicted_past_the_largest_window():
    tracker = ThroughputTracker(make_config())
    tracker.observe_completion({'file_size_bytes': '100'}, 1.0, now=1000.0)
    stats = tracker.window_stats(now=1031.0)  # 31 s later
    assert stats['30']['tasks'] == 0
    assert len(tracker._completions) == 0


# ---- emit tick -----------------------------------------------------------------


def test_emit_once_broadcasts_frame_and_updates_gauges():
    from drakkar.metrics import throughput_gauge

    spy = RecorderSpy()
    tracker = ThroughputTracker(make_config(), recorder=spy)  # type: ignore[arg-type]
    tracker.observe_completion({'file_size_bytes': '600'}, 1.0)

    tracker.emit_once()

    (frame,) = spy.frames
    assert frame['1']['tasks'] == 1
    assert frame['1']['throughput'] == 600.0
    assert throughput_gauge.labels(window='1')._value.get() == 600.0


def test_emit_once_without_recorder_still_updates_gauges():
    from drakkar.metrics import task_rate_gauge

    tracker = ThroughputTracker(make_config(), recorder=None)
    tracker.observe_completion({'file_size_bytes': '300'}, 1.0)

    tracker.emit_once()  # must not raise

    assert task_rate_gauge.labels(window='1')._value.get() == 1.0


async def test_start_and_stop_clean():
    tracker = ThroughputTracker(make_config())
    tracker.start()
    assert tracker._task is not None
    await tracker.stop()
    assert tracker._task is None


# ---- recorder frame shape ------------------------------------------------------


def test_broadcast_throughput_frame_shape(tmp_path):
    from drakkar.recorder import EventRecorder
    from tests.test_recorder import WORKER_NAME, make_debug_config

    rec = EventRecorder(make_debug_config(tmp_path), worker_name=WORKER_NAME)
    sub = rec.subscribe(event_types=['throughput'])
    windows = {'1': {'throughput': 5.0, 'task_rate': 1.0, 'tasks': 1}}

    rec.broadcast_throughput(windows)

    frame = sub.get_nowait()
    assert frame['event'] == 'throughput'
    assert frame['dt']
    assert json.loads(frame['metadata']) == {'windows': windows}
    assert len(rec._buffer) == 0  # broadcast-only: never touches the DB buffer
