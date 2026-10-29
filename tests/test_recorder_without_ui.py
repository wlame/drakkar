"""The recorder as a feature of its own: which object a worker builds, and stage M with no memory store."""

import json
import sqlite3
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig, PostgresConnectionConfig, RecorderConfig, ResilienceConfig
from drakkar.metrics import (
    recorder_memory_allocated_bytes,
    recorder_memory_bytes,
    recorder_memory_oldest_age_seconds,
    recorder_memory_rows,
)
from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from drakkar.recorder.helpers import make_db_path
from drakkar.recorder.postgres import RecorderPostgresSettings, build_record
from drakkar.recorder.schema import EVENT_COLUMNS, SCHEMA_EVENTS, EventType
from drakkar.uiserver import routes_debug
from tests.conftest import kafka_connections, wait_for
from tests.recorder_pg_fake import RecorderFakePostgres
from tests.test_sources_validation import KafkaOnlyHandler
from tests.test_trace_history_postgres import _client

WORKER = 'w-1'
CLUSTER = 'search'


def _app(tmp_path, *, ui_enabled: bool, recorder_enabled: bool, **recorder) -> DrakkarApp:
    return DrakkarApp(
        handler=KafkaOnlyHandler(),
        config=DrakkarConfig(
            connections=kafka_connections(),
            storage={'dir': str(tmp_path / 'storage')},
            cluster={'name': CLUSTER},
            api={'port': 8123, 'public_url': 'http://w-1.example:8123'},
            ui={'enabled': ui_enabled, 'release': {'enabled': False}},
            recorder={'enabled': recorder_enabled, **recorder},
            metrics={'enabled': False},
            runtime_health={'enabled': False},
            executor={'binary_path': '/usr/bin/echo'},
            sinks={'kafka': {'output_results': {'topic': 'output-results'}}},
            sources={'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
        ),
        worker_id=WORKER,
    )


@pytest.fixture
def no_api_server(monkeypatch) -> MagicMock:
    server = MagicMock(return_value=AsyncMock())
    monkeypatch.setattr('drakkar.uiserver.server.APIServer', server)
    return server


def _file_recorder(tmp_path, **recorder) -> EventRecorder:
    (tmp_path / 'storage').mkdir(exist_ok=True)
    return EventRecorder(
        RecorderConfig(**{'flush_interval': '60s', **recorder}),
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='sqlite',
        memory_enabled=False,
        storage_dir=str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
        instance='0123456789abcdef',
    )


def _event(offset: int, **fields) -> dict:
    return {'ts': time.time(), 'event': 'consumed', 'partition': 0, 'offset': offset, **fields}


def _offsets_in(path) -> list[int]:
    conn = sqlite3.connect(path)
    try:
        return [row[0] for row in conn.execute('SELECT "offset" FROM events ORDER BY id')]
    finally:
        conn.close()


def _ids_in(path) -> list[int]:
    conn = sqlite3.connect(path)
    try:
        return [row[0] for row in conn.execute('SELECT id FROM events ORDER BY id')]
    finally:
        conn.close()


# --- which recorder a worker builds ---


async def test_no_recorder_object_with_ui_and_recorder_off(tmp_path, no_api_server):
    app = _app(tmp_path, ui_enabled=False, recorder_enabled=False)

    await app._lifecycle._start_recorder_and_api()

    assert app._recorder is None
    no_api_server.assert_called_once()
    assert no_api_server.call_args.kwargs['ui'] is None
    assert no_api_server.call_args.kwargs['recorder'] is None


async def test_memory_only_with_ui_on_and_recorder_off(tmp_path, no_api_server):
    app = _app(tmp_path, ui_enabled=True, recorder_enabled=False)

    await app._lifecycle._start_recorder_and_api()
    try:
        assert app._recorder is not None
        assert app._recorder.persistence == 'none'
        assert app._recorder.memory_store is not None
        assert app._recorder.db_path == ''
        no_api_server.assert_called_once()
    finally:
        await app._recorder.stop()


async def test_no_memory_store_with_ui_off_and_recorder_on(tmp_path, no_api_server):
    (tmp_path / 'storage').mkdir()
    app = _app(tmp_path, ui_enabled=False, recorder_enabled=True, flush_interval='60s')
    for gauge in (
        recorder_memory_rows,
        recorder_memory_bytes,
        recorder_memory_allocated_bytes,
        recorder_memory_oldest_age_seconds,
    ):
        gauge.set(0)

    await app._lifecycle._start_recorder_and_api()
    recorder = app._recorder
    try:
        assert recorder is not None
        assert recorder.persistence == 'sqlite'
        assert recorder.memory_store is None
        assert recorder.memory_coverage() is None
        assert recorder.db_path != ''
        assert no_api_server.call_args.kwargs['ui'] is None

        recorder._record(_event(1))
        recorder._record(_event(2))
        await recorder.flush_memory()

        assert list(recorder._intake) == []
        assert [row_id for row_id, _ in recorder._file_queue] == [1, 2]
        assert recorder._last_id == 2
        assert recorder_memory_rows._value.get() == 0
        assert recorder_memory_bytes._value.get() == 0
        assert recorder_memory_allocated_bytes._value.get() == 0
        assert recorder_memory_oldest_age_seconds._value.get() == 0
    finally:
        await recorder.stop()


async def test_both_stores_with_ui_and_recorder_on(tmp_path, no_api_server):
    (tmp_path / 'storage').mkdir()
    app = _app(tmp_path, ui_enabled=True, recorder_enabled=True)

    await app._lifecycle._start_recorder_and_api()
    try:
        assert app._recorder.memory_store is not None
        assert app._recorder.db_path != ''
    finally:
        await app._recorder.stop()


# --- stage M with no memory store ---


async def test_the_memory_loop_hands_rows_on_with_no_memory_store(tmp_path):
    rec = _file_recorder(tmp_path, flush_interval='1s')
    await rec.start()
    try:
        assert rec._memory_task is not None
        rec._record(_event(5))
        await wait_for(lambda: rec.db_path and not rec._intake and not rec._file_queue)
        await rec.flush()
        assert _offsets_in(rec.db_path) == [5]
    finally:
        await rec.stop()


async def test_a_flush_with_no_memory_store_writes_the_rows_to_the_file(tmp_path):
    rec = _file_recorder(tmp_path)
    await rec.start()
    try:
        rec._record(_event(1))
        rec._record(_event(2))
        await rec.flush()

        assert _offsets_in(rec.db_path) == [1, 2]
        assert _ids_in(rec.db_path) == [1, 2]
    finally:
        await rec.stop()


async def test_final_flush_writes_queued_rows_with_no_memory_store(tmp_path):
    rec = _file_recorder(tmp_path)
    await rec.start()
    live_path = rec.db_path
    rec._record(_event(1))
    rec._record(_event(2))

    await rec.stop()

    sealed = tmp_path / 'shared' / 'history' / CLUSTER / live_path.rsplit('/', 1)[1]
    assert sealed.exists()
    assert _offsets_in(sealed) == [1, 2]
    assert _ids_in(sealed) == [1, 2]


async def test_last_breath_writes_with_no_memory_store(tmp_path):
    rec = _file_recorder(tmp_path)
    await rec.start()
    try:
        rec._record(_event(1))
        await rec.flush_memory()
        rec._record(_event(2))
        assert [row_id for row_id, _ in rec._file_queue] == [1]
        assert len(rec._intake) == 1

        rec._last_breath_flush()

        assert _offsets_in(rec.db_path) == [1, 2]
        assert _ids_in(rec.db_path) == [1, 2]
        assert not rec._file_queue
        assert not rec._intake
    finally:
        await rec.stop()


async def test_reads_with_no_memory_store_answer_from_the_file(tmp_path):
    rec = _file_recorder(tmp_path)
    await rec.start()
    try:
        rec._record(_event(1))
        rec._record(_event(2))
        await rec.flush()

        assert sorted(row['offset'] for row in await rec.get_events()) == [1, 2]
        assert [row['offset'] for row in await rec.cross_trace(0, 1)] == [1]
    finally:
        await rec.stop()


async def test_a_rotation_with_no_memory_store_writes_the_rows_to_the_closing_file(tmp_path, monkeypatch):
    names = iter(('first', 'second'))
    real_make_db_path = core_mod.make_db_path
    monkeypatch.setattr(
        core_mod, 'make_db_path', lambda folder, worker: real_make_db_path(folder, f'{worker}-{next(names)}')
    )
    rec = _file_recorder(tmp_path)
    await rec.start()
    try:
        first_path = rec.db_path
        rec._record(_event(1))
        await rec._rotate()
        assert rec.db_path != first_path
        rec._record(_event(2))
        await rec.flush()

        assert _ids_in(rec.db_path) == [2]
    finally:
        await rec.stop()
    (closed,) = tmp_path.rglob('*-first-*.db')
    assert _offsets_in(closed) == [1]


async def test_capture_events_off_keeps_no_rows_with_no_memory_store(tmp_path):
    rec = _file_recorder(tmp_path, capture={'events': False})
    await rec.start()
    try:
        assert rec._memory_task is None
        rec._record(_event(1))
        assert not rec._intake
    finally:
        await rec.stop()


async def test_no_store_at_all_appends_nothing():
    rec = EventRecorder(RecorderConfig(), worker_name=WORKER, persistence='none', memory_enabled=False)
    await rec.start()
    try:
        assert rec._memory_task is None
        rec._record(_event(1))
        assert not rec._intake
    finally:
        await rec.stop()


async def test_stage_p_runs_with_no_memory_store():
    fake = RecorderFakePostgres()
    settings = RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn='postgresql://drakkar:secret@db:5432/drakkar'),
        table_prefix='',
        retention_hours=24,
        store_completed_output=False,
        max_output=0,
        max_size=0,
        batch_rows=100,
        queue_max_bytes=16 * 1024 * 1024,
        resilience=ResilienceConfig(max_retries=0),
    )
    rec = EventRecorder(
        RecorderConfig(flush_interval='60s'),
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='postgres',
        memory_enabled=False,
        instance='0123456789abcdef',
        postgres=settings,
        postgres_open_pool=fake.open_pool,
    )
    await rec.start()
    try:
        assert rec.memory_store is None
        assert rec._pg_writer is not None
        rec._record(_event(1))
        await rec.flush_memory()
        assert len(rec._pg_queue) == 1
        await rec._pg_thread.run(rec._pg_writer.run_once, None)
    finally:
        await rec.stop()
    assert fake.copied_ids() == [1]


async def test_a_full_intake_with_no_memory_store_gives_the_loop_a_turn_between_chunks(tmp_path, monkeypatch):
    rec = _file_recorder(tmp_path)
    await rec.start()
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', 2)
    turns = 0
    real_sleep = core_mod.asyncio.sleep

    async def counting_sleep(seconds):
        nonlocal turns
        if seconds == 0:
            turns += 1
        await real_sleep(seconds)

    monkeypatch.setattr(core_mod.asyncio, 'sleep', counting_sleep)
    try:
        for offset in range(5):
            rec._record(_event(offset))
        await rec.flush_memory()
        assert turns == 3
        assert [row_id for row_id, _ in rec._file_queue] == [1, 2, 3, 4, 5]
    finally:
        await rec.stop()


# --- recorder_started ---


@pytest.mark.parametrize(
    'persistence,memory_enabled',
    [('none', True), ('sqlite', True), ('sqlite', False)],
    ids=['memory_only', 'both', 'file_only'],
)
async def test_recorder_started_has_no_ignored_keys(tmp_path, persistence, memory_enabled):
    rec = EventRecorder(
        RecorderConfig(),
        worker_name=WORKER,
        persistence=persistence,
        memory_enabled=memory_enabled,
        storage_dir=str(tmp_path) if persistence == 'sqlite' else '',
    )

    with capture_logs() as captured:
        await rec.start()
    await rec.stop()

    (line,) = [entry for entry in captured if entry['event'] == 'recorder_started']
    assert 'ignored_keys' not in line
    assert line['persistence'] == persistence
    assert ('memory_max_rows' in line) is memory_enabled


# --- the worker_config row ---


async def _config_row(rec: EventRecorder, config: DrakkarConfig) -> dict:
    await rec.write_config(config)
    assert rec._db is not None
    async with rec._db.execute('SELECT * FROM worker_config WHERE id = 1') as cursor:
        columns = [column[0] for column in cursor.description]
        return dict(zip(columns, await cursor.fetchone(), strict=True))


async def test_worker_config_debug_port_and_url_come_from_the_constructor(tmp_path, no_api_server):
    (tmp_path / 'storage').mkdir()
    app = _app(tmp_path, ui_enabled=False, recorder_enabled=True, flush_interval='60s')

    await app._lifecycle._start_recorder_and_api()
    try:
        row = await _config_row(app._recorder, app._config)
    finally:
        await app._recorder.stop()

    assert row['debug_port'] == 8123
    assert row['debug_url'] == 'http://w-1.example:8123'


async def test_worker_config_env_vars_read_capture_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv('GIT_SHA', 'abc123')
    rec = EventRecorder(
        RecorderConfig(capture={'env_vars': ['GIT_SHA']}),
        worker_name=WORKER,
        persistence='sqlite',
        storage_dir=str(tmp_path),
    )
    await rec.start()
    try:
        row = await _config_row(rec, _app(tmp_path, ui_enabled=False, recorder_enabled=False)._config)
    finally:
        await rec.stop()

    assert json.loads(row['env_vars_json']) == {'GIT_SHA': 'abc123'}


# --- the history search with the recorder off ---


async def test_history_search_with_recorder_off_reads_no_folder(tmp_path, monkeypatch):
    def no_sealed_files(*_args, **_kwargs):
        raise AssertionError('the sealed history folder was searched')

    monkeypatch.setattr(routes_debug, 'trace_history', no_sealed_files)
    storage = tmp_path / 'storage'
    storage.mkdir()
    closed_file = make_db_path(str(storage), WORKER)
    conn = sqlite3.connect(closed_file)
    conn.executescript(SCHEMA_EVENTS)
    conn.execute(core_mod.INSERT_EVENT_SQL, core_mod._row_tuple(1, _event(42, task_id='in-a-closed-file', dt='d')))
    conn.commit()
    conn.close()
    rec = EventRecorder(
        RecorderConfig(flush_interval='60s'),
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='none',
        storage_dir=str(storage),
        shared_dir=str(tmp_path / 'shared'),
    )
    await rec.start()
    try:
        rec._record(_event(42, task_id='t-1'))
        await rec.flush_memory()
        async with _client(rec) as client:
            history = await client.get('/api/v1/debug/trace', params={'partition': 0, 'offset': 42, 'history': 1})
            live = await client.get('/api/v1/debug/trace', params={'partition': 0, 'offset': 42})
    finally:
        await rec.stop()

    assert history.status_code == 200
    assert [row['task_id'] for row in history.json()] == ['t-1']
    assert [row['task_id'] for row in live.json()] == ['t-1']


# --- runtime health data and the Postgres row builder ---


@pytest.mark.parametrize('max_output', [0, 1024], ids=['no_output_limit', 'output_limit'])
@pytest.mark.parametrize('store_completed_output', [True, False], ids=['with_output', 'without_output'])
def test_runtime_stall_stacks_survive_the_postgres_row_builder(store_completed_output, max_output):
    metadata = json.dumps({'duration_ms': 1500.0, 'stacks': 'x' * 300_000})
    row = tuple(
        {'id': 1, 'ts': 1.0, 'dt': 'd', 'event': EventType.RUNTIME_STALL, 'metadata': metadata}.get(column)
        for column in EVENT_COLUMNS
    )

    record = build_record(
        row,
        cluster=CLUSTER,
        worker=WORKER,
        instance='i',
        store_completed_output=store_completed_output,
        max_output=max_output,
    )

    assert record[3 + EVENT_COLUMNS.index('metadata')] == metadata
