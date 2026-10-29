"""The two scopes of the archive pass: the whole cluster, or the files of one worker."""

import asyncio
import json
import os
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

import drakkar.recorder.archive as archive_mod
import drakkar.recorder.core as core_mod
from drakkar.app import DrakkarApp
from drakkar.cluster_tasks import archive_scope
from drakkar.config import DrakkarConfig, RecorderArchiveConfig, RecorderConfig
from drakkar.periodic import periodic, run_own_system_task
from drakkar.periodic_roster import resolve_periodic_roster
from drakkar.recorder import EventRecorder
from drakkar.recorder.archive import archive_file_name, list_archives, run_archive_pass
from drakkar.recorder.history import meta_path
from drakkar.schedule import ARCHIVE_TASK_NAME, ClusterTaskDefinition, cluster_tasks_hash
from tests.archive_helpers import CLUSTER, DAY, SteppingClock, due_start, history_folder, make_sealed_db, sealed_db_name
from tests.conftest import kafka_connections, wait_for
from tests.test_sources_validation import KafkaOnlyHandler

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'history_meta.json').read_text(encoding='utf-8'))
INSTANCE = '1111111111111111'
WORKER = 'w'
# Starts with this worker's name: a prefix match would take its files.
NEIGHBOUR = 'w-1'
# The definition hash of a worker with no cluster task.
NO_TASKS_HASH = '4f53cda18c2baa0c'
FILESYSTEM_ON = {'enabled': True, 'provider': 'filesystem'}


def _config(**values) -> DrakkarConfig:
    defaults = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True}},
        'storage': {'dir': '/data', 'shared_dir': '/mnt/shared'},
        'autodiscover': FILESYSTEM_ON,
        'ui': {'enabled': True, 'release': {'enabled': False}},
    }
    return DrakkarConfig(**{**defaults, **values})


def _archive_config(**overrides) -> RecorderArchiveConfig:
    return RecorderArchiveConfig(**{'window': '24h', 'merged_retention': 0, 'retention': 0, **overrides})


def _own_pass(tmp_path, *, worker: str = WORKER, cfg: RecorderArchiveConfig | None = None, **kwargs) -> None:
    """One own-scope pass of ``worker`` over ``<tmp_path>/shared``."""
    run_archive_pass(
        str(tmp_path / 'shared'),
        cluster=CLUSTER,
        instance=INSTANCE,
        archive=cfg or _archive_config(),
        only_worker=worker,
        **kwargs,
    )


def _make_archive(folder: Path, end: float, *, worker: str | None = None) -> Path:
    path = folder / archive_file_name(CLUSTER, end - DAY, end, worker=worker)
    path.write_bytes(b'not-really-gzip')
    return path


def _age(path: Path, seconds: float) -> None:
    stamp = time.time() - seconds
    os.utime(path, (stamp, stamp))


def _names(folder: Path, pattern: str = '*') -> list[str]:
    return sorted(path.name for path in folder.glob(pattern))


# --- the scope ------------------------------------------------------------


def test_scope_is_cluster_with_autodiscover_and_shared_dir():
    assert archive_scope(_config()) == 'cluster'


def test_scope_does_not_depend_on_the_ui():
    assert archive_scope(_config(ui={'enabled': False})) == 'cluster'


def test_scope_is_worker_without_shared_dir_even_with_autodiscover():
    config = _config(
        storage={'dir': '/data'},
        autodiscover={'enabled': True, 'provider': 'redis'},
        connections={**kafka_connections(), 'redis': {'main': {'url': 'redis://localhost:6379/0'}}},
    )

    assert archive_scope(config) == 'worker'


def test_scope_is_worker_with_autodiscover_off():
    assert archive_scope(_config(autodiscover={'enabled': False})) == 'worker'
    assert archive_scope(_config(autodiscover={'enabled': False}, storage={'dir': '/data'})) == 'worker'


@pytest.mark.parametrize(
    'values',
    [
        {'recorder': {'enabled': False}},
        {'recorder': {'enabled': False}, 'storage': {'dir': '', 'shared_dir': '/mnt/shared'}},
        {
            'recorder': {'provider': 'postgres'},
            'connections': {**kafka_connections(), 'postgres': {'main': {'dsn': 'postgresql://localhost/db'}}},
        },
        {'recorder': {'archive': {'enabled': False}}},
    ],
    ids=['recorder-off', 'recorder-off-no-dir', 'postgres', 'archive-off'],
)
def test_scope_is_none_when_recorder_off_or_postgres(values):
    assert archive_scope(_config(**values)) is None


# --- which files an own-scope pass takes ----------------------------------


def test_own_scope_takes_only_this_workers_files(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    mine = make_sealed_db(folder, WORKER, start)
    neighbour = make_sealed_db(folder, NEIGHBOUR, start)
    window_start = (start // DAY) * DAY

    _own_pass(tmp_path)

    assert not Path(mine).exists()
    assert not Path(meta_path(mine)).exists()
    assert Path(neighbour).exists()
    assert Path(meta_path(neighbour)).exists()
    assert _names(folder, '*.db.gz') == [archive_file_name(CLUSTER, window_start, window_start + DAY, worker=WORKER)]


def test_own_scope_of_the_neighbour_leaves_this_workers_files(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    mine = make_sealed_db(folder, WORKER, start)
    neighbour = make_sealed_db(folder, NEIGHBOUR, start)
    window_start = (start // DAY) * DAY

    _own_pass(tmp_path, worker=NEIGHBOUR)

    assert Path(mine).exists()
    assert not Path(neighbour).exists()
    assert _names(folder, '*.db.gz') == [archive_file_name(CLUSTER, window_start, window_start + DAY, worker=NEIGHBOUR)]


def test_own_scope_takes_files_of_a_worker_name_with_unsafe_characters(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    mine = make_sealed_db(folder, 'node.example', start)
    window_start = (start // DAY) * DAY

    _own_pass(tmp_path, worker='node.example')

    assert not Path(mine).exists()
    assert _names(folder, '*.db.gz') == [
        archive_file_name(CLUSTER, window_start, window_start + DAY, worker='node_example')
    ]


def test_own_scope_leaves_a_file_whose_name_has_no_time_stamp(tmp_path):
    folder = history_folder(tmp_path)
    hand_made = folder / 'w.db'
    hand_made.write_bytes(b'')
    _age(hand_made, 10 * DAY)

    _own_pass(tmp_path)

    assert hand_made.exists()
    assert _names(folder, '*.db.gz') == []


def test_cluster_scope_takes_every_workers_files(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    first = make_sealed_db(folder, WORKER, start)
    second = make_sealed_db(folder, NEIGHBOUR, start)
    fence = MagicMock(token='0123456789abcdef', provider_name='scripted')
    fence.is_held.return_value = True
    window_start = (start // DAY) * DAY

    run_archive_pass(
        str(tmp_path / 'shared'),
        cluster=CLUSTER,
        instance=INSTANCE,
        archive=_archive_config(),
        fence=fence,
    )

    assert not Path(first).exists()
    assert not Path(second).exists()
    assert _names(folder, '*.db.gz') == [archive_file_name(CLUSTER, window_start, window_start + DAY)]


def test_own_scope_names_its_work_files_with_a_token_of_its_own(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    make_sealed_db(folder, WORKER, due_start(time.time()))
    seen: list[str] = []
    real_compress = archive_mod._compress

    def spy(source_path: str, dest_path: str) -> None:
        seen.append(Path(dest_path).name)
        real_compress(source_path, dest_path)

    monkeypatch.setattr(archive_mod, '_compress', spy)

    _own_pass(tmp_path)

    (work_file,) = seen
    assert work_file.startswith(f'.{CLUSTER}-{WORKER}-')
    assert len(work_file.split('.db.gz.')[1].removesuffix('.tmp')) == 16


def test_own_scope_pass_can_stop_between_windows(tmp_path):
    folder = history_folder(tmp_path)
    mine = make_sealed_db(folder, WORKER, due_start(time.time()))
    stop = threading.Event()
    stop.set()

    _own_pass(tmp_path, stop=stop)

    assert Path(mine).exists()
    assert _names(folder, '*.db.gz') == []


# --- what an own-scope pass prunes ----------------------------------------


def test_own_scope_prunes_only_this_workers_archives(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    old_mine = _make_archive(folder, now - 10 * DAY, worker=WORKER)
    recent_mine = _make_archive(folder, now - DAY, worker=WORKER)
    old_neighbour = _make_archive(folder, now - 10 * DAY, worker=NEIGHBOUR)
    old_cluster = _make_archive(folder, now - 10 * DAY)

    with capture_logs() as logs:
        _own_pass(tmp_path, cfg=_archive_config(retention='3d'))

    assert not old_mine.exists()
    assert recent_mine.exists()
    assert old_neighbour.exists()
    assert old_cluster.exists()
    assert [entry['name'] for entry in logs if entry['event'] == 'recorder_archive_expired'] == [old_mine.name]


def test_cluster_scope_prunes_no_own_scope_archive(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    old_cluster = _make_archive(folder, now - 10 * DAY)
    old_mine = _make_archive(folder, now - 10 * DAY, worker=WORKER)
    fence = MagicMock(token='0123456789abcdef', provider_name='scripted')

    run_archive_pass(
        str(tmp_path / 'shared'),
        cluster=CLUSTER,
        instance=INSTANCE,
        archive=_archive_config(retention='3d'),
        fence=fence,
    )

    assert not old_cluster.exists()
    assert old_mine.exists()


def test_own_scope_prunes_merge_results(tmp_path):
    history_folder(tmp_path)
    merged = tmp_path / 'shared' / 'merged'
    merged.mkdir()
    old = merged / 'merged-2026-09-01__10_00_00-0123456789abcdef.db'
    young = merged / 'merged-2026-10-10__10_00_00-0123456789abcdef.db'
    for path, age_days in ((old, 40), (young, 3)):
        path.write_bytes(b'merged')
        _age(path, age_days * DAY)

    _own_pass(tmp_path, cfg=_archive_config(merged_retention='32d'))

    assert not old.exists()
    assert young.exists()


def test_own_scope_pass_runs_without_a_merged_folder(tmp_path):
    history_folder(tmp_path)

    _own_pass(tmp_path, cfg=_archive_config(merged_retention='32d'))

    assert not (tmp_path / 'shared' / 'merged').exists()


@pytest.mark.parametrize(
    'name',
    [
        f'.{CLUSTER}-{WORKER}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.merge.db',
        f'.{CLUSTER}-{WORKER}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.merge.db-wal',
        f'.{CLUSTER}-{WORKER}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.tmp',
        f'.{WORKER}-2026-07-14__00_00_00.db.0123456789abcdef.incoming',
        f'{WORKER}-2026-07-14__00_00_00.db.meta.json',
        '.clock.2222222222222222',
        '.archive.lock.stale.0123456789abcdef',
    ],
    ids=['merge', 'merge-sidecar', 'compress', 'sealing-copy', 'orphan-meta', 'clock-marker', 'old-lock-copy'],
)
def test_own_scope_sweeps_this_workers_old_work_files_and_the_shared_markers(tmp_path, name):
    folder = history_folder(tmp_path)
    path = folder / name
    path.write_bytes(b'x')
    _age(path, 3 * 3600)

    _own_pass(tmp_path)

    assert not path.exists()


@pytest.mark.parametrize(
    'name',
    [
        f'.{CLUSTER}-{NEIGHBOUR}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.merge.db',
        f'.{CLUSTER}-{NEIGHBOUR}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.tmp',
        f'.{CLUSTER}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.tmp',
        f'.{NEIGHBOUR}-2026-07-14__00_00_00.db.0123456789abcdef.incoming',
        f'{NEIGHBOUR}-2026-07-14__00_00_00.db.meta.json',
    ],
    ids=['merge', 'compress', 'cluster-compress', 'sealing-copy', 'orphan-meta'],
)
def test_own_scope_sweeps_nothing_of_another_worker(tmp_path, name):
    folder = history_folder(tmp_path)
    path = folder / name
    path.write_bytes(b'x')
    _age(path, 3 * 3600)

    _own_pass(tmp_path)

    assert path.exists()


# --- the name and the readers ---------------------------------------------


@pytest.mark.parametrize('case', FIXTURE['archive_names'], ids=lambda case: case['name'])
def test_own_scope_archive_name_matches_the_fixture(case):
    name = archive_file_name(case['cluster'], case['from'], case['to'], worker=case['worker'])

    assert name == case['name']


@pytest.mark.parametrize('case', FIXTURE['own_sealed_files'], ids=lambda case: f'{case["worker"]}:{case["file"]}')
def test_own_sealed_files_match_the_fixture(case):
    assert archive_mod.is_own_sealed_name(case['file'], case['worker']) is case['own']


def test_list_archives_takes_the_cluster_from_the_folder(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    own = _make_archive(folder, now - DAY, worker=WORKER)
    whole = _make_archive(folder, now - 2 * DAY)

    infos = list_archives(str(folder))

    assert [(info.name, info.cluster) for info in infos] == [(own.name, CLUSTER), (whole.name, CLUSTER)]


def test_own_scope_footprint_counts_this_workers_archives_and_no_neighbours(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    mine = _make_archive(folder, now - 2 * DAY, worker=WORKER)
    whole = _make_archive(folder, now - 3 * DAY)
    neighbour = _make_archive(folder, now - 2 * DAY, worker=NEIGHBOUR)
    neighbour.write_bytes(b'x' * 4096)

    _own_pass(tmp_path)

    assert archive_mod.recorder_archive_files._value.get() == 2
    assert archive_mod.recorder_archive_bytes._value.get() == mine.stat().st_size + whole.stat().st_size


def test_cluster_scope_footprint_counts_no_own_scope_archive(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    whole = _make_archive(folder, now - 3 * DAY)
    _make_archive(folder, now - 2 * DAY, worker=WORKER)

    archive_mod.observe_archive_footprint(str(tmp_path / 'shared'), CLUSTER)

    assert archive_mod.recorder_archive_files._value.get() == 1
    assert archive_mod.recorder_archive_bytes._value.get() == whole.stat().st_size


# --- the roster and the hash ----------------------------------------------


class _NoTasksHandler:
    pass


class _ClusterTaskHandler(KafkaOnlyHandler):
    @periodic(60, scope='cluster')
    async def rebuild_index(self) -> None:
        return None


def test_cluster_scope_name_and_hash_are_unchanged():
    config = _config()

    roster = resolve_periodic_roster(_NoTasksHandler(), config)

    assert roster.archive_scope == 'cluster'
    assert roster.cluster_task_names == [ARCHIVE_TASK_NAME]
    assert roster.cluster_hash == cluster_tasks_hash(
        (ClusterTaskDefinition(name=ARCHIVE_TASK_NAME, interval=3600, cron=None, lease_seconds=60),)
    )
    assert archive_file_name('a/b c', 0.0, 86400.0) == 'a_b_c-1970-01-01_00-00__1970-01-02_00-00.db.gz'


def test_own_scope_runs_with_no_election_and_no_hash_entry(tmp_path):
    app = DrakkarApp(
        handler=KafkaOnlyHandler(),
        config=_config(autodiscover={'enabled': False}, storage={'dir': str(tmp_path)}),
        worker_id=WORKER,
    )

    app._lifecycle._build_periodic_roster()

    assert app._periodic_roster is not None
    assert app._periodic_roster.archive_scope == 'worker'
    assert app._periodic_roster.cluster_task_names == []
    assert app._periodic_roster.cluster_hash == NO_TASKS_HASH
    assert app._cluster_election is None


def test_own_scope_leaves_the_hash_of_user_cluster_tasks_alone():
    config = _config(
        storage={'dir': '/data'},
        autodiscover=FILESYSTEM_ON | {'provider': 'redis'},
        connections={
            **kafka_connections(),
            'redis': {'main': {'url': 'redis://localhost:6379/0'}},
        },
    )

    roster = resolve_periodic_roster(_ClusterTaskHandler(), config)

    assert roster.archive_scope == 'worker'
    assert roster.cluster_task_names == ['rebuild_index']


# --- the loop of the own scope --------------------------------------------


class _RowRecorder:
    """Keeps the arguments of each periodic run row."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def record_periodic_run(self, **row) -> None:
        self.rows.append(row)


async def _run_loop_until(condition, loop_task: asyncio.Task) -> None:
    try:
        await wait_for(condition)
    finally:
        loop_task.cancel()
        await asyncio.wait({loop_task})


async def test_own_task_runs_at_the_slots_of_its_interval_as_a_system_row_with_no_slot():
    first_slot = 1_800_000_000
    interval = 3600
    clock = SteppingClock(first_slot - 1, limit=first_slot + interval)
    recorder = _RowRecorder()
    moments: list[float] = []

    async def body() -> None:
        moments.append(clock.time())

    loop_task = asyncio.create_task(
        run_own_system_task(ARCHIVE_TASK_NAME, body, interval=interval, cron=None, recorder=recorder, clock=clock)
    )
    await _run_loop_until(lambda: len(moments) == 2, loop_task)

    assert moments == [first_slot, first_slot + interval]
    assert recorder.rows[0]['name'] == ARCHIVE_TASK_NAME
    assert recorder.rows[0]['system'] is True
    assert recorder.rows[0]['slot'] is None


async def test_own_task_runs_at_the_fire_times_of_its_cron():
    first_slot = 1_800_000_000
    clock = SteppingClock(first_slot - 1, limit=first_slot + 3600)
    moments: list[float] = []

    async def body() -> None:
        moments.append(clock.time())

    loop_task = asyncio.create_task(
        run_own_system_task(ARCHIVE_TASK_NAME, body, interval=None, cron='0 * * * *', recorder=None, clock=clock)
    )
    await _run_loop_until(lambda: len(moments) == 2, loop_task)

    assert moments == [first_slot, first_slot + 3600]


async def test_own_task_skips_the_slots_that_pass_during_a_run_and_goes_on_after_a_failure():
    first_slot = 1_800_000_000
    interval = 3600
    clock = SteppingClock(first_slot - 1, limit=first_slot + 2 * interval)
    moments: list[float] = []

    async def body() -> None:
        moments.append(clock.time())
        if len(moments) == 1:
            clock.now += interval + 1
            raise RuntimeError('broken pass')

    with capture_logs() as logs:
        loop_task = asyncio.create_task(
            run_own_system_task(ARCHIVE_TASK_NAME, body, interval=interval, cron=None, recorder=None, clock=clock)
        )
        await _run_loop_until(lambda: len(moments) == 2, loop_task)

    assert moments == [first_slot, first_slot + 2 * interval]
    assert [entry['event'] for entry in logs if entry['event'] == 'periodic_task_failed'] == ['periodic_task_failed']


# --- the recorder and the lifecycle ---------------------------------------


def _recorder(tmp_path, *, archive: RecorderArchiveConfig | None = None) -> EventRecorder:
    (tmp_path / 'storage').mkdir(exist_ok=True)
    return EventRecorder(
        RecorderConfig(flush_interval='60s', archive=archive or RecorderArchiveConfig()),
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='sqlite',
        storage_dir=str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
        instance=INSTANCE,
    )


async def test_own_pass_of_the_recorder_runs_with_no_fence_for_this_worker_in_its_thread(tmp_path, monkeypatch):
    rec = _recorder(tmp_path)
    seen: dict = {}

    def spy(shared_dir, **kwargs) -> None:
        seen.update(kwargs, shared_dir=shared_dir, thread=threading.current_thread().ident)

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)

    await rec.run_own_archive_pass()
    await rec.stop()

    assert seen['shared_dir'] == str(tmp_path / 'shared')
    assert seen['only_worker'] == WORKER
    assert seen['fence'] is None
    assert seen['stop'] is rec._archive_stop
    assert seen['thread'] != threading.current_thread().ident


async def test_own_pass_of_the_recorder_does_nothing_once_stopped(tmp_path, monkeypatch):
    rec = _recorder(tmp_path)
    passes: list[dict] = []
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: passes.append(kwargs))
    await rec.stop()

    await rec.run_own_archive_pass()

    assert passes == []


async def test_own_pass_that_outlives_the_stop_is_logged_with_its_own_detail(tmp_path, monkeypatch):
    rec = _recorder(tmp_path)
    release = threading.Event()
    started = threading.Event()

    def stuck_pass(shared_dir, **kwargs) -> None:
        started.set()
        release.wait(30)

    monkeypatch.setattr(core_mod, 'run_archive_pass', stuck_pass)
    slot_task = asyncio.create_task(rec.run_own_archive_pass())
    await wait_for(started.is_set)
    slot_task.cancel()
    await asyncio.wait({slot_task})
    try:
        with capture_logs() as logs:
            await rec._wait_for_archive_pass(time.monotonic() + 0.01, 0.0)
    finally:
        release.set()
        await rec.stop()

    (abandoned,) = [entry for entry in logs if entry['event'] == 'recorder_archive_pass_abandoned']
    assert abandoned['detail'] == core_mod.ARCHIVE_OWN_PASS_ABANDONED_DETAIL


def _lifecycle_app(tmp_path, **values) -> DrakkarApp:
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=_config(**values), worker_id=WORKER)
    app._lifecycle._build_periodic_roster()
    app._recorder = MagicMock()
    return app


async def test_own_scope_starts_its_slot_loop_and_no_cluster_task(tmp_path):
    app = _lifecycle_app(tmp_path, autodiscover={'enabled': False}, storage={'dir': str(tmp_path)})
    started_cluster_tasks: list = []
    app._lifecycle._start_cluster_task = started_cluster_tasks.append

    app._lifecycle._start_archive_task()

    try:
        (loop_task,) = app._periodic_tasks
        assert loop_task.get_name() == f'periodic:{ARCHIVE_TASK_NAME}'
        assert started_cluster_tasks == []
    finally:
        loop_task.cancel()
        await asyncio.wait({loop_task})


def test_cluster_scope_starts_the_cluster_task(tmp_path):
    app = _lifecycle_app(tmp_path)
    started_cluster_tasks: list = []
    app._lifecycle._start_cluster_task = started_cluster_tasks.append

    app._lifecycle._start_archive_task()

    assert started_cluster_tasks == [app._recorder.archive_cluster_task.return_value]
    assert app._periodic_tasks == []


def test_no_scope_starts_nothing(tmp_path):
    app = _lifecycle_app(tmp_path, recorder={'archive': {'enabled': False}})
    started_cluster_tasks: list = []
    app._lifecycle._start_cluster_task = started_cluster_tasks.append

    app._lifecycle._start_archive_task()

    assert started_cluster_tasks == []
    assert app._periodic_tasks == []


async def test_own_scope_worker_warns_about_archives_kept_for_ever(tmp_path):
    app = DrakkarApp(
        handler=KafkaOnlyHandler(),
        config=_config(
            autodiscover={'enabled': False},
            storage={'dir': str(tmp_path)},
            recorder={'archive': {'retention': 0}},
        ),
        worker_id=WORKER,
    )
    with capture_logs() as logs:
        await app._lifecycle._start_recorder()
    try:
        assert [entry for entry in logs if entry['event'] == 'recorder_archives_unbounded']
    finally:
        await app._recorder.stop()


# --- a real own-scope pass end to end -------------------------------------


async def test_own_slot_loop_archives_this_workers_due_window_and_writes_a_row_with_no_scope(tmp_path):
    rec = _recorder(tmp_path, archive=_archive_config())
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    mine = make_sealed_db(folder, WORKER, start)
    neighbour = make_sealed_db(folder, NEIGHBOUR, start)
    rows: list[dict] = []
    real_record = rec.record_periodic_run

    def record(*args, **row) -> None:
        rows.append(row)
        real_record(*args, **row)

    rec.record_periodic_run = record  # type: ignore[method-assign]
    first_slot = 1_800_000_000
    clock = SteppingClock(first_slot - 1, limit=first_slot)
    loop_task = asyncio.create_task(
        run_own_system_task(
            ARCHIVE_TASK_NAME, rec.run_own_archive_pass, interval=3600, cron=None, recorder=rec, clock=clock
        )
    )
    await _run_loop_until(lambda: bool(rows), loop_task)
    await rec.stop()

    assert not Path(mine).exists()
    assert Path(neighbour).exists()
    assert sealed_db_name(NEIGHBOUR, start) in _names(folder)
    (row,) = rows
    assert (row['name'], row['status'], row['system'], row['slot']) == (ARCHIVE_TASK_NAME, 'ok', True, None)
