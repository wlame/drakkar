"""Plant an already-due history window into the harness's shared folder.

The archive-election half of the unclean-exit scenario needs an archive pass
that runs for seconds, right now, instead of after the next hourly rotation.
This writes ``--files`` sealed recorder files of one cluster into
``<shared>/history/<sanitized cluster>/``, each with ``--events-per-file``
events and a real meta file, all in one hour window that ended at least an
hour ago. With the harness settings (``window_hours: 1``,
``due_after_hours: 1``) that window is due at once, so the next timer tick
of an archive pass merges, compresses and publishes it.

The files go through the recorder's own sealing path: the schema of
``drakkar.recorder.schema``, ``seal_closed_file`` (rollback journal, meta
built from the file) and ``publish_sealed`` (meta file first, then the
database). The file name carries the time the archive pass reads, so
``parse_db_start_ts`` places every file in the planted window.

Usage:
    uv run python integration/plant_history.py --shared-dir=integration/shared \\
        --cluster='main cluster' --files=40 --events-per-file=2000 --hours-ago=3

Prints one line:
    PLANTED scope=<sanitized cluster> window_start=<epoch> window_end=<epoch> files=<n> archive=<file name>

Exits 1, writing nothing, when the window's archive or one of the files to
plant already exists (a leftover of an earlier run).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import secrets
import sqlite3
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from drakkar.dbfiles import secure_db_file
from drakkar.recorder.archive import archive_file_name
from drakkar.recorder.helpers import format_dt
from drakkar.recorder.history import history_dir, sanitize_cluster
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE
from drakkar.recorder.sealing import publish_sealed, seal_closed_file

# The archive window and due delay of integration/worker/drakkar.yaml
# (history.archive.window_hours and due_after_hours), in seconds.
WINDOW_SECONDS = 3600
DUE_AFTER_SECONDS = 3600
# A window that starts this many hours ago or more is due at once:
# its end is at most hours_ago - 1 hours ago.
MIN_HOURS_AGO = (WINDOW_SECONDS + DUE_AFTER_SECONDS) // 3600
# One file per second of the window, so every name is distinct.
MAX_FILES = WINDOW_SECONDS
WORKER_PREFIX = 'planted'
DEFAULT_CLUSTER = 'main cluster'
DEFAULT_FILES = 20
DEFAULT_EVENTS_PER_FILE = 1000
DEFAULT_HOURS_AGO = 3
# Random bytes behind each event's stdout (written as hex, twice as long):
# random text keeps gzip and the merge busy, so the size of the window
# sets how long one archive pass takes.
STDOUT_RANDOM_BYTES = 512
NAME_TIME_FORMAT = '%Y-%m-%d__%H_%M_%S'


@dataclass(frozen=True)
class PlantResult:
    """The planted window: its bounds, the archive a pass will publish, and the sealed files."""

    scope: str
    window_start: int
    window_end: int
    archive: str
    sources: tuple[str, ...]

    def line(self) -> str:
        """The ``PLANTED`` line the scenario script reads."""
        return (
            f'PLANTED scope={self.scope} window_start={self.window_start} '
            f'window_end={self.window_end} files={len(self.sources)} archive={self.archive}'
        )


class AlreadyPlantedError(RuntimeError):
    """The window's archive or a file to plant already exists."""


def planted_window(hours_ago: int, now: float) -> tuple[int, int]:
    """The epoch-aligned window that holds the moment ``hours_ago`` hours before ``now``."""
    start = int((now - hours_ago * 3600) // WINDOW_SECONDS * WINDOW_SECONDS)
    return start, start + WINDOW_SECONDS


def planted_worker(index: int) -> str:
    """The worker name of file ``index``: one worker per file, so every name is distinct."""
    return f'{WORKER_PREFIX}-{index:04d}'


def planted_file_name(index: int, window_start: int) -> str:
    """The name of file ``index``, opened ``index`` seconds into the window."""
    opened_at = datetime.fromtimestamp(window_start + index, tz=UTC)
    return f'{planted_worker(index)}-{opened_at.strftime(NAME_TIME_FORMAT)}.db'


def write_recorder_file(path: str, *, worker: str, cluster: str, opened_at: float, events: int) -> None:
    """Write a closed recorder file: the recorder schema, a worker_config row and ``events`` task events."""
    secure_db_file(path)
    rng = random.Random(f'{worker}:{opened_at}')
    conn = sqlite3.connect(path)
    try:
        conn.executescript(SCHEMA_EVENTS + SCHEMA_WORKER_CONFIG + SCHEMA_WORKER_STATE)
        conn.execute(
            'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) VALUES (1, ?, ?, ?, ?)',
            (worker, cluster, opened_at, format_dt(opened_at)),
        )
        rows = []
        for number in range(events):
            # Spread over the file's first second, so every event stays in the window.
            ts = opened_at + number / max(events, 1)
            stdout = rng.randbytes(STDOUT_RANDOM_BYTES).hex()
            rows.append((ts, format_dt(ts), 'task_completed', 0, number, f'{worker}-{number}', len(stdout), stdout))
        conn.executemany(
            'INSERT INTO events (ts, dt, event, partition, offset, task_id, stdout_size, stdout) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            rows,
        )
        conn.commit()
    finally:
        conn.close()


async def _seal(staged: str, *, worker: str, cluster: str) -> dict[str, Any]:
    """Seal one staged file the way a rotation does; returns its meta."""
    return await seal_closed_file(
        staged, worker=worker, instance=secrets.token_hex(8), cluster=cluster, sealed_at=datetime.now(UTC)
    )


def plant(*, shared_dir: str, cluster: str, files: int, events_per_file: int, hours_ago: int) -> PlantResult:
    """Write ``files`` sealed files of ``cluster`` into one due window; raise ``AlreadyPlantedError`` first if taken."""
    window_start, window_end = planted_window(hours_ago, time.time())
    folder = history_dir(shared_dir, cluster)
    archive = archive_file_name(cluster, window_start, window_end)
    if os.path.exists(os.path.join(folder, archive)):
        raise AlreadyPlantedError(f'the archive {archive} of the window to plant already exists in {folder}')
    names = [planted_file_name(index, window_start) for index in range(files)]
    taken = [name for name in names if os.path.exists(os.path.join(folder, name))]
    if taken:
        raise AlreadyPlantedError(f'{len(taken)} files to plant already exist in {folder}, first {taken[0]}')

    # Every file is written and sealed before the first one is published, so
    # an archive pass that starts while this runs sees the whole window or
    # none of it. The staging folder is a dot folder inside the shared folder:
    # on the same filesystem each publish is a rename, and no worker reads it.
    sealed: list[tuple[str, dict[str, Any]]] = []
    sources: list[str] = []
    with tempfile.TemporaryDirectory(prefix='.plant-history-', dir=shared_dir) as staging:
        for index, name in enumerate(names):
            worker = planted_worker(index)
            staged = os.path.join(staging, name)
            write_recorder_file(
                staged, worker=worker, cluster=cluster, opened_at=window_start + index, events=events_per_file
            )
            sealed.append((staged, asyncio.run(_seal(staged, worker=worker, cluster=cluster))))
        for staged, meta in sealed:
            destination, _ = publish_sealed(staged, meta, folder)
            sources.append(destination)
    return PlantResult(
        scope=sanitize_cluster(cluster),
        window_start=window_start,
        window_end=window_end,
        archive=archive,
        sources=tuple(sorted(sources)),
    )


def _bounded_int(low: int, high: int | None = None):
    """An argparse type: an integer from ``low`` to ``high``."""

    def parse(text: str) -> int:
        value = int(text)
        if value < low or (high is not None and value > high):
            limit = f'from {low} to {high}' if high is not None else f'at least {low}'
            raise argparse.ArgumentTypeError(f'must be {limit}, got {value}')
        return value

    return parse


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """The command line of the generator."""
    parser = argparse.ArgumentParser(description='Plant an already-due history window into a shared folder.')
    parser.add_argument('--shared-dir', required=True, help='The shared folder (the harness: integration/shared).')
    parser.add_argument('--cluster', default=DEFAULT_CLUSTER, help='The cluster name, before sanitizing.')
    parser.add_argument(
        '--files', type=_bounded_int(1, MAX_FILES), default=DEFAULT_FILES, help='Sealed files to plant.'
    )
    parser.add_argument(
        '--events-per-file',
        type=_bounded_int(1),
        default=DEFAULT_EVENTS_PER_FILE,
        help='Events in each file.',
    )
    parser.add_argument(
        '--hours-ago',
        type=_bounded_int(MIN_HOURS_AGO),
        default=DEFAULT_HOURS_AGO,
        help='The window holds the moment this many hours ago.',
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Plant the window and print the ``PLANTED`` line; 1 when the window is already taken."""
    args = parse_args(argv)
    try:
        result = plant(
            shared_dir=args.shared_dir,
            cluster=args.cluster,
            files=args.files,
            events_per_file=args.events_per_file,
            hours_ago=args.hours_ago,
        )
    except AlreadyPlantedError as exc:
        print(f'plant_history: {exc}', file=sys.stderr)
        return 1
    print(result.line())
    return 0


if __name__ == '__main__':
    sys.exit(main())
