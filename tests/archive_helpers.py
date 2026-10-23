"""Helpers shared by the archive and election tests.

Sealed recorder files in a cluster's history folder, laid out as sealing
leaves them, the election's provider call counters, and a won archive slot.
"""

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from drakkar.cluster_tasks import SLOT_WON, ClusterElection, ClusterTask, SlotContext, SlotLease
from drakkar.metrics import provider_calls
from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, format_dt
from drakkar.recorder.history import history_dir, meta_path, write_meta
from drakkar.schedule import ACQUIRE_WINDOW_SECONDS, ARCHIVE_TASK_NAME, task_scope

HOUR = 3600.0
DAY = 24 * HOUR
CLUSTER = 'search-fleet'


def sealed_db_name(worker: str, start: float) -> str:
    """Sealed recorder file name for a worker whose file opened at ``start``."""
    return f'{worker}-{datetime.fromtimestamp(start, tz=UTC).strftime("%Y-%m-%d__%H_%M_%S")}.db'


def history_folder(tmp_path: Path, cluster: str = CLUSTER) -> Path:
    """The history folder of ``cluster`` under ``<tmp_path>/shared``, created."""
    folder = Path(history_dir(str(tmp_path / 'shared'), cluster))
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def make_sealed_db(folder: Path, worker: str, start: float, *, event_count: int = 2) -> str:
    """One sealed recorder file (rollback journal) with its meta file, as sealing leaves them."""
    path = folder / sealed_db_name(worker, start)
    db = sqlite3.connect(str(path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        """INSERT INTO worker_config
           (id, worker_name, cluster_name, ip_address, debug_port, created_at, created_at_dt)
           VALUES (1, ?, ?, ?, ?, ?, ?)""",
        [worker, CLUSTER, '10.0.0.1', 8080, start, format_dt(start)],
    )
    db.executescript(SCHEMA_EVENTS)
    for i in range(event_count):
        ts = start + i
        db.execute(
            'INSERT INTO events (ts, dt, event, partition, offset) VALUES (?, ?, ?, ?, ?)',
            [ts, format_dt(ts), 'consumed', 0, i],
        )
    db.commit()
    db.close()
    write_meta(meta_path(str(path)), {'schema': 1, 'worker': worker})
    return str(path)


def due_start(now: float, *, days_back: int = 3) -> float:
    """A file start time inside a window that is comfortably due at ``now``."""
    day_start = (now // DAY) * DAY
    return day_start - days_back * DAY + 10 * HOUR


def election_calls(provider: str, op: str, outcome: str) -> float:
    """``drakkar_provider_calls_total`` of the election for one provider, operation and outcome."""
    return provider_calls.labels(feature='election', provider=provider, op=op, outcome=outcome)._value.get()


async def run_won_slot(task: ClusterTask, election: ClusterElection, *, cluster: str = CLUSTER, slot: int = 0) -> str:
    """Run one won slot of ``task`` as the slot loop does: start the provider, take the lease, run, release.

    Returns the token of the slot's lease.
    """
    started = await election.ensure_started(ACQUIRE_WINDOW_SECONDS)
    assert started.ok, started
    lease = SlotLease(election, scope=task_scope(cluster, task.name), lease_seconds=task.lease_seconds)
    outcome, _ = await lease.acquire(ACQUIRE_WINDOW_SECONDS)
    assert outcome == SLOT_WON
    try:
        await task.run(SlotContext(slot=slot, lease=lease))
    finally:
        await lease.release()
    return lease.token


class SteppingClock:
    """A slot loop clock that jumps over each sleep at once until ``limit``; a sleep past it waits until cancelled.

    A slot loop on it reaches its slots without waiting and parks at the
    first sleep that would pass ``limit``.
    """

    def __init__(self, now: float, limit: float) -> None:
        self.now = now
        self.limit = limit

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        if self.now + seconds > self.limit:
            await asyncio.Event().wait()
        self.now += seconds
        await asyncio.sleep(0)


def idle_archive_task() -> ClusterTask:
    """A stand-in ``history.archive`` task for a stand-in recorder: hourly slots whose won run does nothing."""

    async def run(_ctx: SlotContext) -> None:
        return None

    return ClusterTask(
        name=ARCHIVE_TASK_NAME, run=run, interval=3600, cron=None, lease_seconds=60, on_error='continue', system=True
    )
