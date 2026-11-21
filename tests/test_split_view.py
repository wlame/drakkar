"""The read-only view of split jobs: partitions, job rows, phases, timeouts and the disabled shape."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from drakkar.config.split import SplitConfig
from drakkar.split.models import PartResult
from drakkar.split.records import PARTS_PARTITION_BASE, JobRef, LogEntry, open_body, result_body
from drakkar.split.table import JobTable
from drakkar.split.tracker import FollowedPartition
from drakkar.split.view import build_split_state, near_timeout_threshold

GROUP = 'items-group'
PARTITION = 7
ATTEMPT = '1111111111111111'
OPENED_MS = 1_793_000_000_000
NOW_MS = OPENED_MS + 60_000
NOW_S = 5000.0
PARTS_TOPIC = 'items_g_split_parts'
RESULTS_TOPIC = 'items_g_split_results'


def ref(offset: int, partition: int = PARTITION) -> JobRef:
    return JobRef(group=GROUP, partition=partition, offset=offset, attempt=1, attempt_id=ATTEMPT)


def table_with(*jobs: dict[str, Any], partition: int = PARTITION) -> JobTable:
    """A table with one job per spec: ``offset``, ``opened_ms``, ``parts``, ``ok`` and ``failed`` results."""
    table = JobTable(partition=partition, group=GROUP)
    log_offset = 0
    for job in jobs:
        job_ref = ref(job['offset'], partition)
        opened_ms = job.get('opened_ms', OPENED_MS)
        body = open_body(worker='w', instance='i-1', request_id=f'req_{job["offset"]}')
        table.apply(LogEntry(ref=job_ref, kind='open', body=body), offset=log_offset, timestamp_ms=opened_ms)
        log_offset += 1
        if job.get('parts') is not None:
            done_ms = job.get('done_ms', opened_ms)
            entry = LogEntry(ref=job_ref, kind='split-done', body={'parts': job['parts']})
            table.apply(entry, offset=log_offset, timestamp_ms=done_ms)
            log_offset += 1
        outcomes = ['ok'] * job.get('ok', 0) + ['failed'] * job.get('failed', 0)
        result_ms = job.get('result_ms', opened_ms)
        for index, outcome in enumerate(outcomes):
            result = PartResult(index=index, worker='w2', outcome=outcome, tasks_succeeded=2)
            entry = LogEntry(ref=job_ref, kind='result', body=result_body(result))
            table.apply(entry, offset=log_offset, timestamp_ms=result_ms)
            log_offset += 1
    return table


@dataclass
class FakeOwner:
    phases: dict[int, str] = field(default_factory=dict)

    def phase_of(self, offset: int) -> str | None:
        return self.phases.get(offset)


@dataclass
class FakeTracker:
    partitions: list[FollowedPartition]

    def followed(self) -> list[FollowedPartition]:
        return list(self.partitions)


@dataclass
class FakeRuntime:
    tracker: FakeTracker
    owner_map: dict[int, FakeOwner] = field(default_factory=dict)
    parts_topic: str = PARTS_TOPIC
    results_topic: str = RESULTS_TOPIC

    def owners(self) -> dict[int, FakeOwner]:
        return dict(self.owner_map)


def live(table: JobTable, *, partition: int = PARTITION, since: float = NOW_S - 30.0) -> FollowedPartition:
    return FollowedPartition(partition=partition, restoring=False, state_since=since, read_position=12, table=table)


def state(runtime: Any, *, settings: SplitConfig | None = None, limit: int = 100) -> dict[str, Any]:
    if runtime is not None and settings is None:
        settings = SplitConfig(enabled=True, job_timeout='1h', idle_timeout='10m')
    return build_split_state(
        runtime,
        settings,
        worker='worker-2',
        instance='abcd',
        source_topic='items',
        consumer_group=GROUP,
        now_ms=NOW_MS,
        now_s=NOW_S,
        limit=limit,
        result_write_failures=4,
    )


def test_view_disabled_shape():
    assert state(None) == {
        'enabled': False,
        'worker': {'name': 'worker-2', 'instance': 'abcd'},
        'source_topic': None,
        'consumer_group': None,
        'parts_topic': None,
        'results_topic': None,
        'parts_partition_base': None,
        'settings': None,
        'partitions': [],
        'jobs': [],
        'jobs_total': 0,
        'result_write_failures_total': 0,
    }


def test_view_lists_live_partition_and_job():
    table = table_with({'offset': 500, 'parts': 10, 'ok': 3, 'failed': 1})
    answer = state(FakeRuntime(FakeTracker([live(table)]), {PARTITION: FakeOwner()}))
    assert answer['enabled'] is True
    assert answer['source_topic'] == 'items'
    assert answer['consumer_group'] == GROUP
    assert answer['parts_topic'] == PARTS_TOPIC
    assert answer['results_topic'] == RESULTS_TOPIC
    assert answer['parts_partition_base'] == PARTS_PARTITION_BASE
    assert answer['settings'] == {'job_timeout_seconds': 3600, 'idle_timeout_seconds': 600, 'max_attempts': 3}
    assert answer['partitions'] == [
        {
            'partition': PARTITION,
            'state': 'live',
            'state_seconds': 30.0,
            'read_position': 12,
            'open_jobs': 1,
            'closed_jobs': 0,
        }
    ]
    [row] = answer['jobs']
    assert row['partition'] == PARTITION
    assert row['offset'] == 500
    assert row['request_id'] == 'req_500'
    assert row['attempt'] == 1
    assert row['phase'] == 'waiting'
    assert (row['parts'], row['received'], row['failed'], row['dlq_parts']) == (10, 4, 1, 0)
    assert (row['tasks_succeeded'], row['tasks_failed']) == (8, 0)
    assert row['age_seconds'] == 60.0
    assert answer['jobs_total'] == 1
    assert answer['result_write_failures_total'] == 4


def test_view_restoring_partition_has_null_counts_and_no_rows():
    restoring = FollowedPartition(
        partition=PARTITION, restoring=True, state_since=NOW_S - 2.5, read_position=None, table=None
    )
    answer = state(FakeRuntime(FakeTracker([restoring])))
    assert answer['partitions'] == [
        {
            'partition': PARTITION,
            'state': 'restoring',
            'state_seconds': 2.5,
            'read_position': None,
            'open_jobs': None,
            'closed_jobs': None,
        }
    ]
    assert answer['jobs'] == []
    assert answer['jobs_total'] == 0


def test_view_partitions_are_sorted_and_count_closed_jobs():
    table = table_with({'offset': 10, 'parts': 1, 'ok': 1}, {'offset': 20}, partition=9)
    table.apply(
        LogEntry(
            ref=ref(10, 9),
            kind='closed',
            body={'outcome': 'aggregated', 'received': 1, 'parts': 1, 'duration_seconds': 1.0},
        ),
        offset=99,
        timestamp_ms=OPENED_MS,
    )
    other = table_with({'offset': 5})
    answer = state(FakeRuntime(FakeTracker([live(table, partition=9), live(other)])))
    assert [(p['partition'], p['open_jobs'], p['closed_jobs']) for p in answer['partitions']] == [
        (PARTITION, 1, 0),
        (9, 1, 1),
    ]


def test_view_job_before_split_done_is_splitting_with_null_parts():
    table = table_with({'offset': 500, 'ok': 2})
    [row] = state(FakeRuntime(FakeTracker([live(table)])))['jobs']
    assert row['phase'] == 'splitting'
    assert row['parts'] is None
    assert row['received'] == 2


def test_view_owner_ending_phase_wins():
    table = table_with({'offset': 500})
    owner = FakeOwner({500: 'ending'})
    [row] = state(FakeRuntime(FakeTracker([live(table)]), {PARTITION: owner}))['jobs']
    assert row['phase'] == 'ending'


def test_view_waiting_message_phase():
    table = table_with({'offset': 500, 'parts': 3}, {'offset': 600})
    owner = FakeOwner({500: 'waiting_message', 600: 'waiting_message'})
    rows = state(FakeRuntime(FakeTracker([live(table)]), {PARTITION: owner}))['jobs']
    # A job with no split-done is splitting, whatever the owner waits for.
    assert [(row['offset'], row['phase']) for row in rows] == [(500, 'waiting_message'), (600, 'splitting')]


def test_view_limit_keeps_oldest_and_reports_total():
    table = table_with(
        {'offset': 10, 'opened_ms': OPENED_MS + 3000},
        {'offset': 20, 'opened_ms': OPENED_MS + 1000},
        {'offset': 30, 'opened_ms': OPENED_MS + 2000},
    )
    answer = state(FakeRuntime(FakeTracker([live(table)])), limit=2)
    assert [row['offset'] for row in answer['jobs']] == [20, 30]
    assert answer['jobs_total'] == 3


def _timeout(job: dict[str, Any], settings: SplitConfig) -> tuple[str | None, str | None]:
    [row] = state(FakeRuntime(FakeTracker([live(table_with(job))])), settings=settings)['jobs']
    return row['timeout_at'], row['timeout_kind']


def test_view_timeout_at_job_and_idle():
    settings = SplitConfig(enabled=True, job_timeout='1h', idle_timeout='10m')
    # Before split-done only the job timeout applies.
    assert _timeout({'offset': 1}, settings) == ('2026-10-26T08:33:20.000000Z', 'job')
    # After split-done the idle timeout counts from the last progress, here a result 52 minutes in.
    late = {'offset': 1, 'parts': 5, 'ok': 1, 'result_ms': OPENED_MS + 3_120_000}
    assert _timeout(late, settings) == ('2026-10-26T08:33:20.000000Z', 'job')
    early = {'offset': 1, 'parts': 5, 'ok': 1, 'result_ms': OPENED_MS + 60_000}
    assert _timeout(early, settings) == ('2026-10-26T07:44:20.000000Z', 'idle')
    # A complete job has no timeout.
    assert _timeout({'offset': 1, 'parts': 1, 'ok': 1}, settings) == (None, None)
    idle_only = SplitConfig.model_construct(enabled=True, job_timeout=0, idle_timeout=600, max_attempts=3)
    assert _timeout({'offset': 1}, idle_only) == (None, None)
    both_off = SplitConfig.model_construct(enabled=True, job_timeout=0, idle_timeout=0, max_attempts=3)
    assert _timeout({'offset': 1, 'parts': 5}, both_off) == (None, None)


def test_view_datetimes_use_rfc3339_micro():
    table = table_with({'offset': 500, 'parts': 4, 'ok': 1, 'result_ms': OPENED_MS + 1234})
    [row] = state(FakeRuntime(FakeTracker([live(table)])))['jobs']
    assert row['opened_at'] == '2026-10-26T07:33:20.000000Z'
    assert row['last_progress_at'] == '2026-10-26T07:33:21.234000Z'


@pytest.mark.parametrize(
    ('timeout_seconds', 'threshold'),
    [(300, 75.0), (1000, 250.0), (1800, 300.0), (86400, 8640.0)],
)
def test_near_timeout_threshold_is_ten_percent_with_a_300_second_floor_and_a_quarter_cap(timeout_seconds, threshold):
    assert near_timeout_threshold(timeout_seconds) == threshold


def test_view_deadline_tie_is_the_job_timeout():
    settings = SplitConfig(enabled=True, job_timeout='1h', idle_timeout='10m')
    # The last result is 50 minutes in, so the idle deadline equals the job deadline.
    tie = {'offset': 1, 'parts': 5, 'ok': 1, 'result_ms': OPENED_MS + 3_000_000}
    assert _timeout(tie, settings) == ('2026-10-26T08:33:20.000000Z', 'job')


def test_view_past_deadline_is_still_shown():
    settings = SplitConfig.model_construct(enabled=True, job_timeout=30, idle_timeout=0, max_attempts=3)
    # NOW_MS is 60 s after the open, so the 30 s job timeout passed 30 s ago.
    assert _timeout({'offset': 1}, settings) == ('2026-10-26T07:33:50.000000Z', 'job')


def test_view_state_seconds_are_rounded_to_milliseconds():
    followed = live(table_with(), since=NOW_S - 30.123456)
    [row] = state(FakeRuntime(FakeTracker([followed])))['partitions']
    assert row['state_seconds'] == 30.123
