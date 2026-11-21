"""The read-only view of the split jobs of one worker, as ``GET /api/v1/debug/split`` answers it.

Pure code: it reads the job tables the tracker keeps, asks each owner for the
phases only it knows, and builds the answer. It does no I/O and changes no
state, so it can run on the event loop inside one bounded dispatch.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from drakkar.split.records import PARTS_PARTITION_BASE
from drakkar.split.table import MS_PER_SECOND, DueKind, JobState
from drakkar.timefmt import format_rfc3339_micro

if TYPE_CHECKING:
    from drakkar.config.split import SplitConfig
    from drakkar.split.owner import JobPhase, SplitOwner
    from drakkar.split.runtime import SplitRuntime
    from drakkar.split.tracker import FollowedPartition

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# A job is near its timeout when its time left is below
# min(max(10 % of the timeout, 300 s), 25 % of the timeout). The floor keeps a
# long timeout from warning too late; the cap keeps a short one from warning
# for most of its life.
NEAR_TIMEOUT_SHARE = 0.1
NEAR_TIMEOUT_MIN_SECONDS = 300.0
NEAR_TIMEOUT_MAX_SHARE = 0.25


def format_epoch_ms(ms: int) -> str:
    """Render epoch milliseconds in the canonical timestamp format, exactly."""
    return format_rfc3339_micro(_EPOCH + timedelta(milliseconds=ms))


def near_timeout_threshold(timeout_seconds: float) -> float:
    """The time left, in seconds, below which a job with this timeout is near it."""
    floored = max(NEAR_TIMEOUT_SHARE * timeout_seconds, NEAR_TIMEOUT_MIN_SECONDS)
    return min(floored, NEAR_TIMEOUT_MAX_SHARE * timeout_seconds)


def build_split_state(
    runtime: SplitRuntime | None,
    settings: SplitConfig | None,
    *,
    worker: str,
    instance: str,
    source_topic: str | None,
    consumer_group: str | None,
    now_ms: int,
    now_s: float,
    limit: int,
    result_write_failures: int,
) -> dict[str, Any]:
    """The whole answer of the split jobs route.

    ``runtime`` is None when split is off; the answer then has ``enabled``
    false, no topics and no settings, empty lists and zero totals.
    ``now_ms`` is the epoch clock in milliseconds, the clock of the job
    tables; ``now_s`` is a ``time.monotonic()`` time, the clock of the
    partition states. ``limit`` is the most job rows; the oldest are kept.
    """
    worker_block = {'name': worker, 'instance': instance}
    if runtime is None or settings is None:
        return _disabled_state(worker_block)
    followed = sorted(runtime.tracker.followed(), key=lambda part: part.partition)
    owners = runtime.owners()
    jobs = [(part.partition, job) for part in followed if part.table is not None for job in part.table.open_jobs()]
    jobs.sort(key=lambda item: (item[1].opened_at_ms, item[0], item[1].offset))
    return {
        'enabled': True,
        'worker': worker_block,
        'source_topic': source_topic,
        'consumer_group': consumer_group,
        'parts_topic': runtime.parts_topic,
        'results_topic': runtime.results_topic,
        'parts_partition_base': PARTS_PARTITION_BASE,
        'settings': {
            'job_timeout_seconds': settings.job_timeout,
            'idle_timeout_seconds': settings.idle_timeout,
            'max_attempts': settings.max_attempts,
        },
        'partitions': [_partition_row(part, now_s) for part in followed],
        'jobs': [_job_row(partition, job, owners.get(partition), settings, now_ms) for partition, job in jobs[:limit]],
        'jobs_total': len(jobs),
        'result_write_failures_total': result_write_failures,
    }


def _disabled_state(worker_block: dict[str, str]) -> dict[str, Any]:
    """The answer of a worker with split off."""
    return {
        'enabled': False,
        'worker': worker_block,
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


def _partition_row(part: FollowedPartition, now_s: float) -> dict[str, Any]:
    """One followed partition; the counts of a restoring one are null, because its table is partial."""
    open_jobs: int | None = None
    closed_jobs: int | None = None
    if part.table is not None:
        open_jobs = len(part.table.open_jobs())
        closed_jobs = len(part.table) - open_jobs
    return {
        'partition': part.partition,
        'state': 'restoring' if part.restoring else 'live',
        'state_seconds': round(max(now_s - part.state_since, 0.0), 3),
        'read_position': part.read_position,
        'open_jobs': open_jobs,
        'closed_jobs': closed_jobs,
    }


def _job_row(
    partition: int, job: JobState, owner: SplitOwner | None, settings: SplitConfig, now_ms: int
) -> dict[str, Any]:
    """One open job of a live partition."""
    deadline = job_deadline(job, job_timeout=settings.job_timeout, idle_timeout=settings.idle_timeout)
    return {
        'partition': partition,
        'offset': job.offset,
        'request_id': job.request_id,
        'attempt': job.attempt,
        'phase': job_phase(job, None if owner is None else owner.phase_of(job.offset)),
        'parts': job.parts,
        'received': job.received_count,
        'failed': job.failed_parts,
        'dlq_parts': job.dlq_parts,
        'tasks_succeeded': job.tasks_succeeded,
        'tasks_failed': job.tasks_failed,
        'opened_at': format_epoch_ms(job.opened_at_ms),
        'age_seconds': max(now_ms - job.opened_at_ms, 0) / MS_PER_SECOND,
        'last_progress_at': format_epoch_ms(job.last_progress_ms),
        'timeout_at': None if deadline is None else format_epoch_ms(deadline[0]),
        'timeout_kind': None if deadline is None else deadline[1],
    }


def job_phase(job: JobState, owner_phase: JobPhase | None) -> JobPhase:
    """The phase of an open job, the first that applies: ending, splitting, waiting_message, waiting."""
    if owner_phase == 'ending':
        return 'ending'
    if job.parts is None:
        return 'splitting'
    if owner_phase == 'waiting_message':
        return 'waiting_message'
    return 'waiting'


def job_deadline(job: JobState, *, job_timeout: int, idle_timeout: int) -> tuple[int, DueKind] | None:
    """The epoch milliseconds and the kind of the first timeout that will apply to ``job``, or None.

    The rules are those of :meth:`JobState.due_kind`: no timeout for a
    closed or complete job; the job timeout counts from the first ``open``;
    the idle timeout runs only after ``split-done`` and counts from the last
    progress, which ``split-done`` itself sets. The job timeout wins a tie.
    """
    if job.closed is not None or job.is_complete:
        return None
    candidates: list[tuple[int, DueKind]] = []
    if job_timeout > 0:
        candidates.append((job.opened_at_ms + job_timeout * MS_PER_SECOND, 'job'))
    if idle_timeout > 0 and job.parts is not None:
        candidates.append((job.last_progress_ms + idle_timeout * MS_PER_SECOND, 'idle'))
    if not candidates:
        return None
    # ``min`` keeps the first of equal deadlines, and the job timeout is first.
    return min(candidates, key=lambda candidate: candidate[0])
