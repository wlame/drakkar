"""The history search of the ``postgres`` recorder mode: ``trace?history=1`` over the events table.

No database: the search connects through the recording fake of
``tests/recorder_pg_fake.py``. The route tests replace ``asyncpg.connect``,
which the search looks up at call time.
"""

import asyncio
import time
from unittest.mock import MagicMock

import asyncpg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from structlog.testing import capture_logs

from drakkar.config import DrakkarConfig, PostgresConnectionConfig, ResilienceConfig
from drakkar.providers.postgres import RECORDER_STATEMENT_TEMPLATES, render_recorder
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder import EventRecorder
from drakkar.recorder.memstore import MIB
from drakkar.recorder.postgres import RecorderPostgresSettings, build_record
from drakkar.recorder.postgres_history import PostgresHistoryError, search_postgres_history
from drakkar.recorder.schema import EVENT_COLUMNS
from drakkar.uiserver import routes_debug
from drakkar.uiserver.routes_debug import create_debug_router
from drakkar.uiserver.server import UIDeps
from tests.conftest import make_ui_config
from tests.recorder_pg_fake import RecorderFakePostgres

WORKER = 'w-1'
CLUSTER = 'fast cluster'
DSN = 'postgresql://drakkar:secret@db:5432/drakkar'
PASSWORD = 'secret'
SET_LOCAL = RECORDER_STATEMENT_TEMPLATES['set_local']
FAILED_BODY = {'detail': 'history search failed'}
TRACE_ENDPOINT = '/api/v1/debug/trace'
# A search limit short enough for a test, and the bound a stopped search must answer within.
SHORT_LIMIT_SECONDS = 0.05
ANSWER_BOUND_SECONDS = 1.0


class FakeClock:
    """A monotonic clock the test moves."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _settings(*, table_prefix: str = '') -> RecorderPostgresSettings:
    return RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn=DSN),
        table_prefix=table_prefix,
        retention_hours=6,
        store_output=False,
        batch_rows=100,
        queue_max_bytes=128 * MIB,
        resilience=ResilienceConfig(),
    )


def _trace_row(row_id: int, worker: str = 'w-2') -> dict:
    row: dict = dict.fromkeys(EVENT_COLUMNS)
    row.update(id=row_id, ts=1.5 + row_id, dt='dt', event='consumed', partition=3, offset=42, origin='kafka')
    row['worker_name'] = worker
    return row


def _event_row() -> tuple:
    values = {'id': 1, 'ts': 1.5, 'dt': 'dt', 'event': 'consumed', 'origin': 'kafka'}
    return tuple(values.get(column) for column in EVENT_COLUMNS)


def _statement(table_prefix: str = '') -> str:
    settings = _settings(table_prefix=table_prefix)
    return render_recorder(RECORDER_STATEMENT_TEMPLATES['trace'], schema=settings.schema, table=settings.table)


def _logs(captured: list[dict], event: str) -> list[dict]:
    return [entry for entry in captured if entry['event'] == event]


# --- the search ---


async def test_trace_history_postgres_rows_have_event_columns_plus_worker_name_only():
    fake = RecorderFakePostgres()
    fake.trace_rows = [_trace_row(1), _trace_row(2)]

    rows = await search_postgres_history(_settings(), CLUSTER, 3, 42, max_seconds=5.0, connect=fake.connect)

    # The statement projects the event columns plus worker_name and nothing else.
    statement = _statement()
    projection = statement.rsplit(' FROM (SELECT DISTINCT ON', 1)[0].rsplit(') SELECT ', 1)[1]
    selected = [column.strip().removeprefix('d.').strip('"') for column in projection.split(',')]
    assert selected == [*EVENT_COLUMNS, 'worker AS worker_name']
    assert [list(row) for row in rows] == [[*EVENT_COLUMNS, 'worker_name']] * 2
    assert [row['id'] for row in rows] == [1, 2]


async def test_trace_history_postgres_sends_the_own_cluster_to_the_prefixed_table():
    fake = RecorderFakePostgres()

    await search_postgres_history(_settings(table_prefix='fast'), CLUSTER, 3, 42, max_seconds=5.0, connect=fake.connect)

    (fetch,) = [call for call in fake.calls if call[0] == 'fetch']
    assert fetch == ('fetch', _statement('fast'), (CLUSTER, 3, 42))
    assert 'drakkar.fast_recorder_events e' in fetch[1]


@pytest.mark.parametrize(
    ('max_seconds', 'expected'), [(2.5, '2500'), (0.0004, '1'), (10.0, '10000'), (0.0019, '1'), (1.0009, '1000')]
)
async def test_trace_history_postgres_sets_statement_timeout_in_whole_milliseconds(max_seconds, expected):
    fake = RecorderFakePostgres()

    await search_postgres_history(_settings(), CLUSTER, 3, 42, max_seconds=max_seconds, connect=fake.connect)

    assert [call[0] if call[0] != 'execute' else call for call in fake.calls] == [
        'connect',
        'BEGIN',
        ('execute', SET_LOCAL, ('statement_timeout', expected)),
        ('execute', SET_LOCAL, ('jit', 'off')),
        'fetch',
        'COMMIT',
        'conn_close',
    ]


async def test_trace_history_postgres_opens_one_connection_and_closes_it():
    fake = RecorderFakePostgres()

    await search_postgres_history(_settings(), CLUSTER, 3, 42, max_seconds=5.0, connect=fake.connect)

    (conn,) = fake.connections
    assert (conn.closed, conn.terminated) == (True, False)
    assert fake.calls[0] == ('connect', DSN)


@pytest.mark.parametrize(
    ('max_seconds', 'expected'), [(float('inf'), '3600000'), (3600.5, '3600000'), (1e9, '3600000'), (3600.0, '3600000')]
)
async def test_trace_history_postgres_caps_the_time_limit_at_one_hour(max_seconds, expected):
    fake = RecorderFakePostgres()

    rows = await search_postgres_history(_settings(), CLUSTER, 3, 42, max_seconds=max_seconds, connect=fake.connect)

    assert rows == []
    assert ('execute', SET_LOCAL, ('statement_timeout', expected)) in fake.calls


async def test_trace_history_postgres_sends_the_cluster_with_nul_replaced_as_the_writer_stores_it():
    fake = RecorderFakePostgres()
    cluster = 'fast\x00cluster'

    await search_postgres_history(_settings(), cluster, 3, 42, max_seconds=5.0, connect=fake.connect)

    (fetch,) = [call for call in fake.calls if call[0] == 'fetch']
    stored = build_record(_event_row(), cluster=cluster, worker=WORKER, instance='i', store_output=False)[0]
    assert fetch[2][0] == stored == 'fast\ufffdcluster'


@pytest.mark.parametrize('knob', ['conn_close_error', 'hang_conn_close'])
async def test_trace_history_postgres_keeps_its_rows_when_the_close_fails_and_terminates(knob):
    fake = RecorderFakePostgres()
    fake.trace_rows = [_trace_row(1)]
    if knob == 'conn_close_error':
        fake.conn_close_error = OSError('connection reset')
    else:
        fake.hang_conn_close = True

    started_at = time.monotonic()
    rows = await search_postgres_history(
        _settings(), CLUSTER, 3, 42, max_seconds=SHORT_LIMIT_SECONDS, connect=fake.connect
    )

    assert time.monotonic() - started_at < ANSWER_BOUND_SECONDS
    assert [row['id'] for row in rows] == [1]
    (conn,) = fake.connections
    assert conn.terminated
    assert [call[0] for call in fake.calls][-3:] == ['COMMIT', 'conn_close', 'terminate']


@pytest.mark.parametrize(
    ('knob', 'reason'),
    [('trace_error', 'error'), ('hang_trace', 'timeout')],
)
async def test_trace_history_postgres_closes_the_connection_on_error_and_on_timeout(knob, reason):
    fake = RecorderFakePostgres()
    if knob == 'trace_error':
        fake.trace_error = asyncpg.exceptions.QueryCanceledError('canceling statement due to statement timeout')
    else:
        fake.hang_trace = True

    with pytest.raises(PostgresHistoryError) as raised:
        await search_postgres_history(
            _settings(), CLUSTER, 3, 42, max_seconds=SHORT_LIMIT_SECONDS, connect=fake.connect
        )

    assert raised.value.reason == reason
    (conn,) = fake.connections
    # Nothing more is sent after a failure: no ROLLBACK, no COMMIT; the socket is closed.
    assert conn.terminated
    assert 'ROLLBACK' not in [call[0] for call in fake.calls]
    assert 'COMMIT' not in [call[0] for call in fake.calls]


@pytest.mark.parametrize(('knob', 'reason'), [('connect_error', 'error'), ('hang_connect', 'timeout')])
async def test_trace_history_postgres_connect_failure_leaves_no_connection(knob, reason):
    fake = RecorderFakePostgres()
    if knob == 'connect_error':
        fake.connect_error = ConnectionRefusedError('connection refused')
    else:
        fake.hang_connect = True

    with pytest.raises(PostgresHistoryError) as raised:
        await search_postgres_history(
            _settings(), CLUSTER, 3, 42, max_seconds=SHORT_LIMIT_SECONDS, connect=fake.connect
        )

    assert raised.value.reason == reason
    assert fake.connections == []


async def test_trace_history_postgres_cancel_closes_the_connection_and_leaves_as_a_cancel():
    fake = RecorderFakePostgres()
    fake.hang_trace = True
    search = asyncio.create_task(
        search_postgres_history(_settings(), CLUSTER, 3, 42, max_seconds=60.0, connect=fake.connect)
    )
    while not any(call[0] == 'fetch' for call in fake.calls):
        await asyncio.sleep(0)

    search.cancel()
    with pytest.raises(asyncio.CancelledError):
        await search

    assert fake.connections[0].terminated


# --- the route ---


def _drakkar_app() -> MagicMock:
    app = MagicMock()
    app._config = DrakkarConfig(sources={'kafka': {'enabled': True}})
    app._config.ui.release.enabled = False
    return app


def _client(rec: EventRecorder, clock: FakeClock | None = None) -> AsyncClient:
    """A client of the debug routes alone, with the rate limit of the failure line on ``clock``."""
    deps = UIDeps(rec.config, rec, _drakkar_app())
    if clock is not None:
        deps.postgres_read_log = RateLimitedLog(clock)
    app = FastAPI()
    app.include_router(create_debug_router(deps))
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


@pytest.fixture
async def started(tmp_path):
    """Start recorders built in the test and stop them at the end."""
    recorders: list[EventRecorder] = []

    async def start(persistence: str, *, max_seconds: float = 5.0) -> EventRecorder:
        storage = tmp_path / 'storage'
        storage.mkdir(exist_ok=True)
        shared = tmp_path / 'shared'
        shared.mkdir(exist_ok=True)
        config = make_ui_config(enabled=True, flush_interval_seconds=60, trace={'history_max_seconds': max_seconds})
        config.release.enabled = False
        rec = EventRecorder(
            config,
            worker_name=WORKER,
            cluster_name=CLUSTER,
            persistence=persistence,
            storage_dir=str(storage) if persistence == 'sqlite' else '',
            shared_dir=str(shared),
            postgres=_settings() if persistence == 'postgres' else None,
            postgres_open_pool=RecorderFakePostgres().open_pool,
        )
        await rec.start()
        recorders.append(rec)
        return rec

    yield start
    for rec in recorders:
        await rec.stop()


@pytest.fixture
def history_fake(monkeypatch) -> RecorderFakePostgres:
    """The server the history search reaches; the sealed-file search must not run."""
    fake = RecorderFakePostgres()
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    def no_sealed_files(*_args, **_kwargs):
        raise AssertionError('the sealed history folder was searched')

    monkeypatch.setattr(routes_debug, 'trace_history', no_sealed_files)
    return fake


async def test_trace_history_postgres_sends_the_own_cluster_and_never_lists_the_history_folder(started, history_fake):
    rec = await started('postgres')
    history_fake.trace_rows = [_trace_row(1), _trace_row(2, worker='w-3')]

    async with _client(rec) as client:
        resp = await client.get(TRACE_ENDPOINT, params={'partition': 3, 'offset': 42, 'history': 1})

    assert resp.status_code == 200
    assert resp.json() == history_fake.trace_rows
    (fetch,) = [call for call in history_fake.calls if call[0] == 'fetch']
    assert fetch[2] == (CLUSTER, 3, 42)


async def test_trace_history_postgres_failure_answers_503_only_for_history(started, history_fake):
    rec = await started('postgres')
    history_fake.trace_error = asyncpg.exceptions.UndefinedTableError('relation does not exist')

    async with _client(rec) as client:
        history = await client.get(TRACE_ENDPOINT, params={'partition': 3, 'offset': 42, 'history': 1})
        own = await client.get(TRACE_ENDPOINT, params={'partition': 3, 'offset': 42})

    assert (history.status_code, history.json()) == (503, FAILED_BODY)
    assert history.content == b'{"detail":"history search failed"}'
    # The own trace never reaches Postgres and keeps its empty shape.
    assert (own.status_code, own.json()) == (200, [])
    assert len(history_fake.connections) == 1


async def test_trace_history_postgres_timeout_answers_503_within_the_limit_and_logs_once(started, history_fake):
    rec = await started('postgres', max_seconds=SHORT_LIMIT_SECONDS)
    history_fake.hang_trace = True
    clock = FakeClock()
    params = {'partition': 3, 'offset': 42, 'history': 1}

    with capture_logs() as captured:
        async with _client(rec, clock) as client:
            started_at = time.monotonic()
            first = await client.get(TRACE_ENDPOINT, params=params)
            elapsed = time.monotonic() - started_at
            second = await client.get(TRACE_ENDPOINT, params=params)
            clock.now += 61
            third = await client.get(TRACE_ENDPOINT, params=params)

    assert [resp.status_code for resp in (first, second, third)] == [503, 503, 503]
    assert elapsed < ANSWER_BOUND_SECONDS
    lines = _logs(captured, 'ui_read_postgres_failed')
    assert [(line['reason'], line['suppressed']) for line in lines] == [('timeout', 0), ('timeout', 1)]
    assert lines[0] | {'log_level': 'warning'} == {
        'event': 'ui_read_postgres_failed',
        'log_level': 'warning',
        'category': 'debug',
        'endpoint': TRACE_ENDPOINT,
        'reason': 'timeout',
        'error': 'timed out',
        'suppressed': 0,
    }
    assert all(conn.terminated for conn in history_fake.connections)


async def test_trace_history_postgres_error_text_has_no_dsn(started, history_fake):
    rec = await started('postgres')
    history_fake.connect_error = OSError(f'could not connect to {DSN}')

    with capture_logs() as captured:
        async with _client(rec) as client:
            resp = await client.get(TRACE_ENDPOINT, params={'partition': 3, 'offset': 42, 'history': 1})

    (line,) = _logs(captured, 'ui_read_postgres_failed')
    assert line['reason'] == 'error'
    assert line['error'].startswith('could not connect to postgresql://')
    assert PASSWORD not in line['error']
    assert PASSWORD not in resp.text
    assert resp.json() == FAILED_BODY


@pytest.mark.parametrize('persistence', ['sqlite', 'none'])
async def test_trace_history_sqlite_and_none_still_read_sealed_files(started, monkeypatch, persistence):
    rec = await started(persistence)
    searched: list[tuple] = []

    def sealed_files(folder, partition, offset, *, max_files, max_seconds):
        searched.append((folder, partition, offset, max_files, max_seconds))
        return [{'id': 1}]

    async def no_postgres(**_kwargs):
        raise AssertionError('the postgres history was searched')

    monkeypatch.setattr(routes_debug, 'trace_history', sealed_files)
    monkeypatch.setattr(asyncpg, 'connect', no_postgres)

    async with _client(rec) as client:
        resp = await client.get(TRACE_ENDPOINT, params={'partition': 3, 'offset': 42, 'history': 1})

    assert (resp.status_code, resp.json()) == (200, [{'id': 1}])
    assert searched == [(rec.history_dir, 3, 42, 256, 5.0)]
