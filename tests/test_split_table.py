"""The job table: attempts, part counts, completion, timeouts and the restart position."""

import random
from typing import Any

import pytest

from drakkar.split.models import PartDLQ, PartResult, SinkCounts
from drakkar.split.records import JobRef, LogEntry, closed_body, open_body, result_body
from drakkar.split.table import FOREIGN_SPLIT_GRACE_SECONDS, Applied, IgnoreReason, JobTable, PartFailure

GROUP = 'items-group'
PARTITION = 7
JOB = 500
ATTEMPT_1 = '1111111111111111'
ATTEMPT_2 = '2222222222222222'


def ref(
    offset: int = JOB, attempt: int = 1, attempt_id: str = ATTEMPT_1, group: str = GROUP, partition: int = PARTITION
):
    return JobRef(group=group, partition=partition, offset=offset, attempt=attempt, attempt_id=attempt_id)


def open_entry(job_ref: JobRef, instance: str = 'i-1') -> LogEntry:
    return LogEntry(ref=job_ref, kind='open', body=open_body(worker='w1', instance=instance, request_id='req_1'))


def done_entry(job_ref: JobRef, parts: int) -> LogEntry:
    return LogEntry(ref=job_ref, kind='split-done', body={'parts': parts})


def cancelled_entry(job_ref: JobRef) -> LogEntry:
    return LogEntry(ref=job_ref, kind='split-cancelled', body={})


def result_entry(job_ref: JobRef, index: int, **fields) -> LogEntry:
    result = PartResult(index=index, worker='w2', outcome=fields.pop('outcome', 'ok'), **fields)
    return LogEntry(ref=job_ref, kind='result', body=result_body(result))


def closed_entry(job_ref: JobRef, outcome: Any = 'aggregated') -> LogEntry:
    body = closed_body(outcome='aggregated', received=0, parts=0, duration_seconds=0.0)
    body['outcome'] = outcome
    return LogEntry(ref=job_ref, kind='closed', body=body)


class Log:
    """Applies entries to a table at increasing offsets and timestamps."""

    def __init__(self, table: JobTable | None = None, start_offset: int = 0, start_ms: int = 1_000_000) -> None:
        self.table = table or JobTable(partition=PARTITION, group=GROUP)
        self.offset = start_offset
        self.ms = start_ms

    def put(self, entry: LogEntry, *, after_ms: int = 10) -> Applied:
        self.ms += after_ms
        applied = self.table.apply(entry, offset=self.offset, timestamp_ms=self.ms)
        self.offset += 1
        return applied


@pytest.fixture
def log() -> Log:
    return Log()


def test_open_results_then_split_done_complete_once(log):
    assert log.put(open_entry(ref())).opened
    completions = [log.put(result_entry(ref(), index)).completed for index in range(3)]
    applied = log.put(done_entry(ref(), 3))
    assert completions == [False, False, False]
    assert applied.completed
    assert applied.job is not None and applied.job.is_complete
    assert not log.put(result_entry(ref(), 1)).completed


def test_results_before_split_done_and_the_last_result_after_it(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 0))
    assert not log.put(done_entry(ref(), 2)).completed
    applied = log.put(result_entry(ref(), 1))
    assert applied.completed
    assert applied.job is not None and applied.job.missing_parts() == []


def test_last_result_and_split_done_in_one_batch_complete_once(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 0))
    batch = [result_entry(ref(), 1), done_entry(ref(), 2)]
    completed = [log.put(entry, after_ms=0).completed for entry in batch]
    assert completed == [False, True]


def test_split_done_with_no_parts_completes_at_once(log):
    log.put(open_entry(ref()))
    assert log.put(done_entry(ref(), 0)).completed


def test_duplicate_part_is_ignored(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 0, tasks_succeeded=2))
    applied = log.put(result_entry(ref(), 0, tasks_succeeded=5))
    assert applied.ignored is IgnoreReason.DUPLICATE
    assert applied.job is not None
    assert (applied.job.received_count, applied.job.tasks_succeeded) == (1, 2)


def test_duplicate_open_and_split_done_are_ignored(log):
    log.put(open_entry(ref()))
    assert log.put(open_entry(ref())).ignored is IgnoreReason.DUPLICATE
    log.put(done_entry(ref(), 2))
    assert log.put(done_entry(ref(), 2)).ignored is IgnoreReason.DUPLICATE


def test_record_of_an_old_attempt_id_is_ignored(log):
    log.put(open_entry(ref()))
    log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)))
    old = ref(attempt=1, attempt_id=ATTEMPT_1)
    assert log.put(result_entry(old, 0)).ignored is IgnoreReason.OLD_ATTEMPT
    assert log.put(done_entry(old, 1)).ignored is IgnoreReason.OLD_ATTEMPT
    assert log.put(closed_entry(old)).ignored is IgnoreReason.OLD_ATTEMPT


def test_higher_attempt_number_with_another_id_is_old(log):
    log.put(open_entry(ref()))
    stray = ref(attempt=9, attempt_id='9999999999999999')
    assert log.put(result_entry(stray, 0)).ignored is IgnoreReason.OLD_ATTEMPT


def test_latest_open_starts_the_attempt_whatever_its_number(log):
    log.put(open_entry(ref(attempt=3, attempt_id=ATTEMPT_2)))
    applied = log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_1)))
    assert applied.opened
    assert applied.job is not None and (applied.job.attempt, applied.job.attempt_id) == (2, ATTEMPT_1)


def test_second_open_resets_counters_and_counts_a_crashed_attempt(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 0, outcome='failed', error='bad', tasks_failed=1))
    applied = log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2), instance='i-2'))
    job = applied.job
    assert job is not None
    assert job.crashed_attempts == 1
    assert (job.received_count, job.failed_parts, job.tasks_failed, job.first_failure) == (0, 0, 0, None)
    assert (job.attempt, job.instance, job.parts, job.has_result(0)) == (2, 'i-2', None, False)


def test_second_open_after_split_cancelled_is_not_a_crash(log):
    log.put(open_entry(ref()))
    log.put(cancelled_entry(ref()))
    applied = log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)))
    assert applied.job is not None
    assert (applied.job.crashed_attempts, applied.job.cancelled) == (0, False)


def test_second_open_after_split_done_is_not_a_crash(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 4))
    applied = log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)))
    assert applied.job is not None and applied.job.crashed_attempts == 0


def test_records_after_closed_are_ignored(log):
    log.put(open_entry(ref()))
    applied = log.put(closed_entry(ref(), 'timeout'))
    assert applied.closed
    for entry in (result_entry(ref(), 0), done_entry(ref(), 1), cancelled_entry(ref()), closed_entry(ref())):
        assert log.put(entry).ignored is IgnoreReason.CLOSED_JOB
    assert log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2))).ignored is IgnoreReason.CLOSED_JOB
    assert log.table.job(JOB).closed == 'timeout'


def test_closed_job_keeps_its_place_and_frees_its_results(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 9, deliveries={'kafka/out': SinkCounts(delivered=1)}, outcome='failed', error='x'))
    job = log.put(closed_entry(ref(), 'dlq')).job
    assert job is not None
    assert (job.partition, job.offset, job.first_open_offset, job.closed) == (PARTITION, JOB, 0, 'dlq')
    assert (len(job.received_bits), job.deliveries, job.first_failure) == (0, {}, None)
    assert not log.put(result_entry(ref(), 0)).completed


def test_closed_without_open_keeps_the_job_closed(log):
    applied = log.put(closed_entry(ref()))
    assert applied.closed
    assert log.table.job(JOB).closed == 'aggregated'
    assert log.table.position(99) == 0


def test_records_with_no_open_are_unknown_jobs(log):
    for entry in (result_entry(ref(), 0), done_entry(ref(), 1), cancelled_entry(ref())):
        applied = log.put(entry)
        assert (applied.ignored, applied.job) == (IgnoreReason.UNKNOWN_JOB, None)
    assert len(log.table) == 0


def test_result_at_or_above_known_parts_is_an_unknown_job(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 2))
    assert log.put(result_entry(ref(), 2)).ignored is IgnoreReason.UNKNOWN_JOB


def test_split_done_drops_earlier_results_beyond_its_parts(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 5, outcome='failed', error='late'))
    log.put(result_entry(ref(), 0))
    applied = log.put(done_entry(ref(), 1))
    assert applied.completed
    job = applied.job
    assert job is not None
    assert (job.received_count, job.has_result(5), job.first_failure) == (1, False, None)


def test_split_done_drops_results_on_a_byte_boundary(log):
    log.put(open_entry(ref()))
    for index in (0, 7, 8, 20):
        log.put(result_entry(ref(), index))
    job = log.put(done_entry(ref(), 8)).job
    assert job is not None
    assert (job.received_count, job.has_result(7), job.has_result(8)) == (2, True, False)
    assert job.missing_parts() == [1, 2, 3, 4, 5, 6]


def test_entry_of_another_group_is_ignored(log):
    applied = log.put(open_entry(ref(group='other-group')))
    assert (applied.ignored, applied.job) == (IgnoreReason.FOREIGN_GROUP, None)
    assert len(log.table) == 0


def test_entry_of_another_partition_is_malformed(log):
    assert log.put(open_entry(ref(partition=8))).ignored is IgnoreReason.MALFORMED


@pytest.mark.parametrize(
    'body',
    [
        {'worker': 'w2'},
        {'part': 0, 'worker': 'w2', 'outcome': 'maybe'},
        {'part': 'zero', 'worker': 'w2', 'outcome': 'ok'},
        {'part': -1, 'worker': 'w2', 'outcome': 'ok'},
        {'part': 2**40, 'worker': 'w2', 'outcome': 'ok'},
        {'part': 0, 'worker': 'w2', 'outcome': 'ok', 'tasks_failed': 'many'},
    ],
    ids=['no_part', 'bad_outcome', 'text_part', 'negative', 'huge', 'bad_counter'],
)
def test_result_with_a_bad_body_is_malformed(log, body):
    log.put(open_entry(ref()))
    applied = log.put(LogEntry(ref=ref(), kind='result', body=body))
    assert applied.ignored is IgnoreReason.MALFORMED
    assert applied.job is not None and applied.job.received_count == 0


@pytest.mark.parametrize('parts', [-1, '3', True, None, 1.5, 2**24 + 1, 2**62])
def test_split_done_with_bad_parts_is_malformed(log, parts):
    log.put(open_entry(ref()))
    applied = log.put(LogEntry(ref=ref(), kind='split-done', body={'parts': parts}))
    assert applied.ignored is IgnoreReason.MALFORMED
    assert applied.job is not None and applied.job.parts is None


def test_open_without_a_string_instance_is_malformed(log):
    applied = log.put(LogEntry(ref=ref(), kind='open', body={'worker': 'w1', 'instance': 5}))
    assert (applied.ignored, applied.job) == (IgnoreReason.MALFORMED, None)


def test_closed_with_an_unknown_outcome_is_malformed(log):
    log.put(open_entry(ref()))
    assert log.put(closed_entry(ref(), 'finished')).ignored is IgnoreReason.MALFORMED
    assert log.table.job(JOB).closed is None


@pytest.mark.parametrize('outcome', [['aggregated'], {'outcome': 'aggregated'}])
def test_closed_with_an_outcome_that_is_not_text_is_malformed(log, outcome):
    log.put(open_entry(ref()))
    assert log.put(closed_entry(ref(), outcome)).ignored is IgnoreReason.MALFORMED
    assert log.table.job(JOB).closed is None


def test_counters_and_deliveries_are_summed(log):
    log.put(open_entry(ref()))
    log.put(
        result_entry(
            ref(),
            0,
            tasks_succeeded=3,
            tasks_failed=1,
            tasks_replaced=2,
            deliveries={'kafka/out': SinkCounts(delivered=2, skipped=1), 'redis/main': SinkCounts(failed=1)},
        )
    )
    dlq = PartDLQ(stage='sink', sink='kafka/out', error='boom')
    job = log.put(
        result_entry(
            ref(), 1, outcome='failed', dlq=dlq, tasks_succeeded=1, deliveries={'kafka/out': SinkCounts(failed=4)}
        )
    ).job
    assert job is not None
    assert (job.received_count, job.failed_parts, job.dlq_parts) == (2, 1, 1)
    assert (job.tasks_succeeded, job.tasks_failed, job.tasks_replaced) == (4, 1, 2)
    assert job.deliveries == {
        'kafka/out': SinkCounts(delivered=2, skipped=1, failed=4),
        'redis/main': SinkCounts(failed=1),
    }
    assert job.last_result_offset == 2


def test_first_failure_is_the_lowest_index_whatever_the_order(log):
    log.put(open_entry(ref()))
    dlq = PartDLQ(stage='execute', task_id='t-3', error='exit 1')
    log.put(result_entry(ref(), 5, outcome='failed', error='late part'))
    log.put(result_entry(ref(), 3, outcome='failed', dlq=dlq))
    log.put(result_entry(ref(), 4, outcome='failed', error='middle'))
    log.put(result_entry(ref(), 0))
    job = log.table.job(JOB)
    assert job.first_failure == PartFailure(index=3, dlq=dlq, error=None)


def test_first_failure_keeps_an_error_text(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 2, outcome='failed', error='parse failed'))
    assert log.table.job(JOB).first_failure == PartFailure(index=2, dlq=None, error='parse failed')


def test_position_is_the_first_open_of_the_oldest_job(log):
    log.put(result_entry(ref(offset=400), 0))
    log.put(open_entry(ref(offset=600)))
    log.put(open_entry(ref(offset=550)))
    log.put(open_entry(ref(offset=600, attempt=2, attempt_id=ATTEMPT_2)))
    assert log.table.position(next_offset=10) == 1


def test_position_holds_at_a_closed_job_that_is_not_retired(log):
    log.put(open_entry(ref(offset=500)))
    log.put(open_entry(ref(offset=501)))
    log.put(closed_entry(ref(offset=500)))
    assert log.table.position(next_offset=3) == 0
    log.table.retire_through(501)
    assert log.table.position(next_offset=3) == 1


def test_position_of_an_empty_table_is_the_next_offset():
    assert JobTable(partition=PARTITION, group=GROUP).position(next_offset=42) == 42


def test_retire_through_removes_jobs_below_the_committed_offset(log):
    for offset in (502, 500, 501):
        log.put(open_entry(ref(offset=offset)))
    log.put(closed_entry(ref(offset=500)))
    retired = log.table.retire_through(502)
    assert [job.offset for job in retired] == [500, 501]
    assert [job.offset for job in log.table.open_jobs()] == [502]
    assert (len(log.table), log.table.job(500)) == (1, None)


def test_open_jobs_leave_out_closed_ones_and_sort_by_offset(log):
    for offset in (503, 501, 502):
        log.put(open_entry(ref(offset=offset)))
    log.put(closed_entry(ref(offset=502)))
    assert [job.offset for job in log.table.open_jobs()] == [501, 503]


def test_job_timeout_counts_from_the_first_open_also_after_a_second_attempt(log):
    log.put(open_entry(ref()), after_ms=0)
    opened_at = log.ms
    log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)), after_ms=5_000)
    table = log.table
    assert table.job(JOB).opened_at_ms == opened_at
    assert table.due(now_ms=opened_at + 9_999, job_timeout=10, idle_timeout=0) == []
    assert [
        (job.offset, kind) for job, kind in table.due(now_ms=opened_at + 10_000, job_timeout=10, idle_timeout=0)
    ] == [(JOB, 'job')]


def test_idle_timeout_counts_from_the_last_new_result(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 3))
    log.put(result_entry(ref(), 0), after_ms=1_000)
    progress = log.ms
    log.put(result_entry(ref(), 0), after_ms=4_000)
    table = log.table
    assert table.job(JOB).last_progress_ms == progress
    assert table.due(now_ms=progress + 4_999, job_timeout=0, idle_timeout=5) == []
    due = table.due(now_ms=progress + 5_000, job_timeout=0, idle_timeout=5)
    assert [(job.offset, kind) for job, kind in due] == [(JOB, 'idle')]


def test_idle_timeout_does_not_run_before_split_done(log):
    log.put(open_entry(ref()))
    log.put(result_entry(ref(), 0), after_ms=1_000)
    # The split still publishes parts: only the job timeout can fire.
    assert log.table.due(now_ms=log.ms + 10**6, job_timeout=0, idle_timeout=5) == []
    assert [kind for _, kind in log.table.due(now_ms=log.ms + 10**6, job_timeout=10, idle_timeout=5)] == ['job']
    log.put(done_entry(ref(), 2), after_ms=1_000)
    assert log.table.due(now_ms=log.ms + 4_999, job_timeout=0, idle_timeout=5) == []
    assert [kind for _, kind in log.table.due(now_ms=log.ms + 5_000, job_timeout=0, idle_timeout=5)] == ['idle']


def test_a_complete_job_is_never_due(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 1))
    log.put(result_entry(ref(), 0))
    assert log.table.job(JOB).is_complete
    assert log.table.due(now_ms=log.ms + 10**9, job_timeout=10, idle_timeout=5) == []


def test_the_open_time_of_the_current_attempt_moves_with_each_attempt(log):
    log.put(open_entry(ref()), after_ms=0)
    first = log.ms
    assert log.table.job(JOB).attempt_opened_at_ms == first
    log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)), after_ms=5_000)
    job = log.table.job(JOB)
    assert (job.opened_at_ms, job.attempt_opened_at_ms) == (first, first + 5_000)


def test_split_done_counts_as_progress(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 3), after_ms=7_000)
    assert log.table.job(JOB).last_progress_ms == log.ms


def test_job_timeout_wins_over_idle_and_a_job_appears_once(log):
    log.put(open_entry(ref()))
    due = log.table.due(now_ms=log.ms + 100_000, job_timeout=10, idle_timeout=5)
    assert [kind for _, kind in due] == ['job']


def test_zero_turns_both_timeouts_off_and_closed_jobs_are_never_due(log):
    log.put(open_entry(ref(offset=500)))
    log.put(open_entry(ref(offset=501)))
    log.put(closed_entry(ref(offset=501)))
    assert log.table.due(now_ms=log.ms + 10**9, job_timeout=0, idle_timeout=0) == []
    assert [job.offset for job, _ in log.table.due(now_ms=log.ms + 10**9, job_timeout=1, idle_timeout=0)] == [500]


def test_large_job_keeps_a_small_bit_set_and_no_result_body(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 100_000))
    big = {'payload': 'x' * 1000}
    for index in range(100_000):
        log.put(result_entry(ref(), index, data=big if index == 0 else None), after_ms=0)
    job = log.table.job(JOB)
    assert job.is_complete
    assert len(job.received_bits) < 16 * 1024
    assert 'x' * 1000 not in repr(vars(job))


# A randomized log: three jobs, two attempts each, with duplicates and stray records.

RANDOM_JOBS = (300, 301, 302)
RANDOM_SEED = 20261121
RANDOM_ROUNDS = 300


def _job_records(rng: random.Random, offset: int) -> list[LogEntry]:
    """A legal sequence for one job: a first attempt that crashes, is cancelled or is done, then a second one."""
    first = ref(offset=offset, attempt=1, attempt_id=f'{offset:08d}aaaaaaaa')
    second = ref(offset=offset, attempt=2, attempt_id=f'{offset:08d}bbbbbbbb')
    parts = rng.randint(0, 5)
    records: list[LogEntry] = [open_entry(first)]
    records += [result_entry(first, index) for index in range(rng.randint(0, parts))]
    ending = rng.choice(['crash', 'cancel', 'done'])
    if ending == 'cancel':
        records.append(cancelled_entry(first))
    elif ending == 'done':
        records.append(done_entry(first, parts))
    records.append(open_entry(second, instance='i-2'))
    late_old = [result_entry(first, rng.randrange(6)) for _ in range(rng.randint(0, 2))]
    results = [
        result_entry(second, index, outcome=rng.choice(['ok', 'failed']), tasks_succeeded=rng.randint(0, 3), error='e')
        for index in range(parts)
    ]
    results += [result_entry(second, rng.randrange(parts)) for _ in range(rng.randint(0, 2)) if parts]
    rng.shuffle(results)
    cut = rng.randint(0, len(results))
    records += [*results[:cut], done_entry(second, parts), *results[cut:], *late_old]
    if rng.random() < 0.5:
        records.append(open_entry(second))
    if rng.random() < 0.5:
        records.append(closed_entry(second, rng.choice(['aggregated', 'timeout'])))
        records.append(result_entry(second, 0))
    return records


def _random_log(rng: random.Random) -> list[LogEntry]:
    """Interleave the jobs' sequences, keeping each job's order, and add stray records."""
    queues = [_job_records(rng, offset) for offset in RANDOM_JOBS]
    queues.append([result_entry(ref(offset=299), 0), open_entry(ref(offset=303, group='other-group'))])
    merged: list[LogEntry] = []
    while any(queues):
        queue = rng.choice([queue for queue in queues if queue])
        merged.append(queue.pop(0))
    return merged


def _apply_from(table: JobTable, entries: list[LogEntry], start: int) -> JobTable:
    for offset in range(start, len(entries)):
        table.apply(entries[offset], offset=offset, timestamp_ms=1_000 * (offset + 1))
    return table


def test_restore_from_any_position_ends_equal_to_reading_the_whole_log():
    rng = random.Random(RANDOM_SEED)
    for _ in range(RANDOM_ROUNDS):
        entries = _random_log(rng)
        whole = _apply_from(JobTable(partition=PARTITION, group=GROUP), entries, 0)
        cut = rng.randint(0, len(entries))
        before = JobTable(partition=PARTITION, group=GROUP)
        for offset in range(cut):
            before.apply(entries[offset], offset=offset, timestamp_ms=1_000 * (offset + 1))
        restored = _apply_from(JobTable(partition=PARTITION, group=GROUP), entries, before.position(cut))
        assert len(restored) == len(whole)
        for offset in RANDOM_JOBS:
            assert restored.job(offset) == whole.job(offset)


def test_missing_parts_is_empty_while_parts_is_unknown(log):
    log.put(open_entry(ref()))
    job = log.table.job(JOB)
    assert (job.missing_parts(), job.has_result(-1), job.is_complete) == ([], False, False)


def test_duplicate_split_cancelled_is_ignored(log):
    log.put(open_entry(ref()))
    log.put(cancelled_entry(ref()))
    assert log.put(cancelled_entry(ref())).ignored is IgnoreReason.DUPLICATE


def test_entry_of_an_unknown_kind_is_malformed(log):
    entry = LogEntry(ref=ref(), kind='open', body={})
    object.__setattr__(entry, 'kind', 'rename')
    assert log.put(entry).ignored is IgnoreReason.MALFORMED


def test_a_snapshot_of_a_job_keeps_its_attempt_when_a_later_open_resets_the_job(log):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 2))
    log.put(result_entry(ref(), 0, deliveries={'kafka/out': SinkCounts(delivered=1)}))
    log.put(result_entry(ref(), 1))
    job = log.table.job(JOB)
    last_result = job.last_result_offset
    kept = job.snapshot()

    log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)))

    # The live job is on the new attempt, with nothing counted yet.
    assert (job.attempt_id, job.parts, job.received_count, job.last_result_offset) == (ATTEMPT_2, None, 0, None)
    assert job.deliveries == {}
    # The copy still is the attempt it was made from.
    assert (kept.attempt_id, kept.attempt, kept.parts, kept.received_count) == (ATTEMPT_1, 1, 2, 2)
    assert (kept.first_open_offset, kept.last_result_offset) == (0, last_result)
    assert kept.is_complete and kept.missing_parts() == []
    assert kept.deliveries['kafka/out'].delivered == 1


@pytest.mark.parametrize(
    ('ending', 'left_behind'),
    [(None, True), ('split-done', False), ('split-cancelled', False), ('closed', False)],
)
def test_a_split_is_left_behind_only_with_no_end_after_the_grace_time(log, ending, left_behind):
    log.put(open_entry(ref()))
    opened = log.ms
    endings = {
        'split-done': done_entry(ref(), 1),
        'split-cancelled': cancelled_entry(ref()),
        'closed': closed_entry(ref()),
    }
    if ending is not None:
        log.put(endings[ending])
    job = log.table.job(JOB)
    grace_ms = FOREIGN_SPLIT_GRACE_SECONDS * 1000

    assert not job.is_split_left_behind(opened + grace_ms - 1)
    assert job.is_split_left_behind(opened + grace_ms) is left_behind


def test_the_grace_time_of_a_split_counts_from_the_open_of_the_current_attempt(log):
    log.put(open_entry(ref()))
    log.put(open_entry(ref(attempt=2, attempt_id=ATTEMPT_2)), after_ms=200_000)
    job = log.table.job(JOB)
    grace_ms = FOREIGN_SPLIT_GRACE_SECONDS * 1000

    assert not job.is_split_left_behind(log.ms + grace_ms - 1)
    assert job.is_split_left_behind(log.ms + grace_ms)


@pytest.mark.parametrize('kind', ['split-done', 'result'])
def test_idle_timeout_counts_from_the_time_of_the_reader_when_the_clock_of_the_writer_is_behind(kind):
    table = JobTable(partition=PARTITION, group=GROUP)
    reader_ms = 1_000_000
    behind_ms = reader_ms - 30_000
    table.apply(open_entry(ref()), offset=0, timestamp_ms=behind_ms, applied_at_ms=reader_ms)
    last = done_entry(ref(), 2)
    if kind == 'result':
        table.apply(last, offset=1, timestamp_ms=behind_ms - 60_000, applied_at_ms=reader_ms - 60_000)
        last = result_entry(ref(), 0)

    table.apply(last, offset=2, timestamp_ms=behind_ms, applied_at_ms=reader_ms)

    assert table.due(now_ms=reader_ms + 59_999, job_timeout=0, idle_timeout=60) == []
    assert [kind for _, kind in table.due(now_ms=reader_ms + 60_000, job_timeout=0, idle_timeout=60)] == ['idle']


def test_a_record_time_ahead_of_the_reader_stays_the_progress_time():
    table = JobTable(partition=PARTITION, group=GROUP)
    table.apply(open_entry(ref()), offset=0, timestamp_ms=1_000_000, applied_at_ms=1_000_000)

    table.apply(done_entry(ref(), 2), offset=1, timestamp_ms=1_020_000, applied_at_ms=1_000_000)

    assert table.due(now_ms=1_020_000 + 59_999, job_timeout=0, idle_timeout=60) == []


@pytest.mark.parametrize(
    'change',
    [
        {'part': True},
        {'part': '0'},
        {'part': 0.0},
        {'tasks_succeeded': -1},
        {'tasks_failed': True},
        {'tasks_replaced': '2'},
        {'deliveries': {'kafka/results': {'delivered': -1, 'skipped': 0, 'failed': 0}}},
        {'deliveries': {'kafka/results': {'delivered': 0, 'skipped': True, 'failed': 0}}},
    ],
)
def test_a_result_whose_numbers_are_not_counts_is_malformed_and_counts_nothing(log, change):
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 2))
    entry = result_entry(ref(), 0)
    entry.body.update(change)

    applied = log.put(entry)

    job = log.table.job(JOB)
    assert applied.ignored is IgnoreReason.MALFORMED
    assert (job.received_count, job.tasks_succeeded, job.tasks_failed, job.deliveries) == (0, 0, 0, {})


def test_a_closed_job_keeps_what_readers_ask_and_frees_the_rest(log):
    first_open = log.offset
    log.put(open_entry(ref()))
    log.put(done_entry(ref(), 2))
    log.put(result_entry(ref(), 0, tasks_succeeded=3, tasks_failed=1, tasks_replaced=1, outcome='failed'))
    closed_at = log.offset

    log.put(closed_entry(ref(), 'dlq'))

    job = log.table.job(JOB)
    kept = (job.partition, job.offset, job.first_open_offset, job.closed, job.closed_offset, job.attempt_id)
    assert kept == (PARTITION, JOB, first_open, 'dlq', closed_at, ATTEMPT_1)
    freed = (job.request_id, job.received_count, job.failed_parts, job.dlq_parts, job.last_result_offset)
    assert freed == (None, 0, 0, 0, None)
    assert (job.tasks_succeeded, job.tasks_failed, job.tasks_replaced) == (0, 0, 0)
    assert (job.deliveries, job.first_failure, bytes(job.received_bits)) == ({}, None, b'')
