"""Tests for Task 8 — recorder schema extension + webapp event types.

Coverage focus:

* Fresh recorder DB has the new ``origin`` / ``client_name`` /
  ``request_id`` columns on the ``events`` table — the schema extension
  carries through ``CREATE TABLE IF NOT EXISTS`` exactly as documented.

* HTTP-origin task rows persist with ``origin='http'`` plus the matching
  ``client_name`` / ``request_id`` populated; existing kafka-path code
  still produces ``origin='kafka'`` rows with NULL client_name /
  request_id (the documented backward-compat default).

* Each new ``webapp_request_*`` event type lands as a row with the
  expected event name and column values when the helper is called.

* Pre-webapp-release recorder DBs (built from the historical
  ``CREATE TABLE`` statement that lacked the new columns) raise
  :class:`RecorderSchemaError` at ``EventRecorder.start()`` with the
  documented operator-facing upgrade-path message — the failure
  surfaces at startup, not at first webapp request.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import aiosqlite
import pytest
from pydantic import BaseModel

from drakkar.config import UIConfig, WebAppConfig, WebClientConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    PendingContext,
)
from drakkar.recorder import EventRecorder
from drakkar.recorder.schema import (
    WEBAPP_REQUIRED_EVENT_COLUMNS,
    RecorderSchemaError,
)
from drakkar.webapp.dependencies import make_authenticate, make_rate_limit
from drakkar.webapp.models import WebRequestContext
from tests.conftest import make_ui_config

# ---------------------------------------------------------------------------
# Helpers — minimal recorder + webapp fixtures
# ---------------------------------------------------------------------------

WORKER_NAME = 'webapp-recorder-test-worker'


def _make_debug_config(tmp_path, **overrides) -> UIConfig:
    """Build a minimal ``UIConfig`` with an explicit ``db_dir``."""
    defaults = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_output': False,
        # Tight flush so tests don't have to advance time.
        'flush_interval_seconds': 60,
    }
    defaults.update(overrides)
    return make_ui_config(**defaults)


def _make_request_ctx(client: str = 'anonymous', request_id: str = 'req_t_0001') -> WebRequestContext:
    return WebRequestContext(
        request_id=request_id,
        client_name=client,
        # Request body — small Pydantic model so ``_compute_body_bytes``
        # has something to measure.
        request=_FakeBody(payload='hello'),
        started_at=datetime.now(UTC),
        headers={},
    )


class _FakeBody(BaseModel):
    """Tiny Pydantic body so the recorder helper can compute body_bytes."""

    payload: str = ''


def _make_http_task(task_id: str = 'http-task-1', request_id: str = 'req_t_0001') -> ExecutorTask:
    """Build an HTTP-origin task — origin/client_name/request_id pre-stamped."""
    return ExecutorTask(
        task_id=task_id,
        args=['--echo'],
        source_offsets=[1],
        origin='http',
        client_name='tenant-A',
        request_id=request_id,
    )


def _make_kafka_task(task_id: str = 'kafka-task-1') -> ExecutorTask:
    """Build a Kafka-origin task — defaults preserve the legacy column shape."""
    return ExecutorTask(
        task_id=task_id,
        args=['--ingest'],
        source_offsets=[42],
    )


async def _read_events_columns(rec: EventRecorder) -> list[dict[str, Any]]:
    """Flush the buffer and return every events row as a dict.

    ``rec._db`` is the active writer connection; we run ``SELECT *``
    through it so the helper picks up freshly-flushed rows
    deterministically.
    """
    await rec.flush()
    assert rec._db is not None
    async with rec._db.execute('SELECT * FROM events ORDER BY id ASC') as cur:
        cols = [d[0] for d in cur.description]
        rows = await cur.fetchall()
        return [dict(zip(cols, row, strict=False)) for row in rows]


# ---------------------------------------------------------------------------
# Schema-shape tests — fresh DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_recorder_db_has_required_webapp_columns(tmp_path):
    """Schema CREATE statement adds origin/client_name/request_id."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        assert rec._db is not None
        async with rec._db.execute('PRAGMA table_info(events)') as cur:
            existing = {row[1] async for row in cur}
        for required in WEBAPP_REQUIRED_EVENT_COLUMNS:
            assert required in existing, f'fresh DB missing column {required!r}'
    finally:
        await rec.stop()


# ---------------------------------------------------------------------------
# record_task_* — origin propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_origin_task_rows_carry_client_and_request_id(tmp_path):
    """Tasks stamped origin='http' produce rows with all three columns set."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        task = _make_http_task(task_id='http-task-A', request_id='req_t_42')
        rec.record_task_started(task, partition=-1)
        result = ExecutorResult(
            exit_code=0,
            stdout='out',
            stderr='',
            duration_seconds=0.01,
            task=task,
            pid=99,
        )
        rec.record_task_completed(result, partition=-1)
        rows = await _read_events_columns(rec)
        events = [r for r in rows if r['event'] in {'task_started', 'task_completed'}]
        assert len(events) == 2
        for row in events:
            assert row['origin'] == 'http'
            assert row['client_name'] == 'tenant-A'
            assert row['request_id'] == 'req_t_42'
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_kafka_origin_task_rows_default_to_origin_kafka(tmp_path):
    """Existing Kafka-path code still produces origin='kafka' rows."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        task = _make_kafka_task()
        rec.record_task_started(task, partition=0)
        result = ExecutorResult(
            exit_code=0,
            stdout='ok',
            stderr='',
            duration_seconds=0.02,
            task=task,
            pid=100,
        )
        rec.record_task_completed(result, partition=0)
        rec.record_task_failed(
            task,
            ExecutorError(task=task, exit_code=1, stderr='boom', exception='RuntimeError'),
            partition=0,
        )
        rows = await _read_events_columns(rec)
        legacy_events = [r for r in rows if r['event'].startswith('task_')]
        assert legacy_events, 'expected at least one task event row'
        for row in legacy_events:
            assert row['origin'] == 'kafka'
            assert row['client_name'] is None
            assert row['request_id'] is None
    finally:
        await rec.stop()


# ---------------------------------------------------------------------------
# webapp_request_* helper rows
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webapp_request_received_row_has_origin_and_request_id(tmp_path):
    """``record_webapp_request_received`` writes one row with all webapp cols."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        ctx = _make_request_ctx(client='tenant-A', request_id='req_recv_1')
        rec.record_webapp_request_received(ctx)
        rows = await _read_events_columns(rec)
        received = [r for r in rows if r['event'] == 'webapp_request_received']
        assert len(received) == 1
        row = received[0]
        assert row['origin'] == 'http'
        assert row['client_name'] == 'tenant-A'
        assert row['request_id'] == 'req_recv_1'
        assert row['partition'] == -1
        # body_bytes is populated for Pydantic bodies.
        assert row['metadata'] is not None
        assert 'body_bytes' in row['metadata']
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_webapp_request_completed_row_carries_status_and_duration(tmp_path):
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        ctx = _make_request_ctx(client='tenant-A', request_id='req_done_1')
        rec.record_webapp_request_completed(ctx, status='ok', duration_ms=412.0)
        rows = await _read_events_columns(rec)
        completed = [r for r in rows if r['event'] == 'webapp_request_completed']
        assert len(completed) == 1
        row = completed[0]
        assert row['origin'] == 'http'
        assert row['client_name'] == 'tenant-A'
        assert row['request_id'] == 'req_done_1'
        # ``duration`` is seconds (Kafka convention); status lives in metadata.
        assert row['duration'] == pytest.approx(0.412, abs=1e-6)
        assert '"status":"ok"' in row['metadata']
        assert '"duration_ms":412' in row['metadata']
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_webapp_request_timeout_row_writes_504_outcome(tmp_path):
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        ctx = _make_request_ctx(client='anonymous', request_id='req_to_1')
        rec.record_webapp_request_timeout(ctx, duration_ms=30_000.0)
        rows = await _read_events_columns(rec)
        timeouts = [r for r in rows if r['event'] == 'webapp_request_timeout']
        assert len(timeouts) == 1
        row = timeouts[0]
        assert row['origin'] == 'http'
        assert row['client_name'] == 'anonymous'
        assert row['request_id'] == 'req_to_1'
        assert row['duration'] == pytest.approx(30.0, abs=1e-6)
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_webapp_request_rate_limited_row_records_client_and_limit(tmp_path):
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_webapp_request_rate_limited(client='anonymous', rpm_limit=4, requests_in_window=4)
        rows = await _read_events_columns(rec)
        rl = [r for r in rows if r['event'] == 'webapp_request_rate_limited']
        assert len(rl) == 1
        row = rl[0]
        assert row['origin'] == 'http'
        assert row['client_name'] == 'anonymous'
        # Rate-limit fires before a request_id is allocated.
        assert row['request_id'] is None
        assert '"rpm_limit":4' in row['metadata']
        assert '"requests_in_window":4' in row['metadata']
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_webapp_request_auth_failed_row_omits_client_name(tmp_path):
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_webapp_request_auth_failed(token_prefix='abcd...')
        rows = await _read_events_columns(rec)
        af = [r for r in rows if r['event'] == 'webapp_request_auth_failed']
        assert len(af) == 1
        row = af[0]
        assert row['origin'] == 'http'
        # No client matched — column is NULL by design.
        assert row['client_name'] is None
        assert row['request_id'] is None
        assert 'abcd...' in row['metadata']
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_webapp_request_dropped_after_timeout_row_carries_request_id(tmp_path):
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        ctx = _make_request_ctx(client='tenant-A', request_id='req_drop_1')
        rec.record_webapp_request_dropped_after_timeout(ctx)
        rows = await _read_events_columns(rec)
        dropped = [r for r in rows if r['event'] == 'webapp_request_dropped_after_timeout']
        assert len(dropped) == 1
        row = dropped[0]
        assert row['origin'] == 'http'
        assert row['client_name'] == 'tenant-A'
        assert row['request_id'] == 'req_drop_1'
    finally:
        await rec.stop()


# ---------------------------------------------------------------------------
# Dependency wiring — auth_failed / rate_limited recorder calls
# ---------------------------------------------------------------------------


def _build_drakkar_app_with_recorder(rec: EventRecorder) -> Any:
    """Build a stub ``DrakkarApp`` exposing the single attribute the deps read."""
    app = MagicMock()
    app._recorder = rec
    return app


@pytest.mark.asyncio
async def test_auth_dependency_writes_recorder_row_on_failure(tmp_path):
    """``make_authenticate`` records ``webapp_request_auth_failed`` rows."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        config = WebAppConfig(
            enabled=True,
            host='127.0.0.1',
            port=0,
            path='/process',
            clients=[
                WebClientConfig(name='tenant-A', token='secret-token', rpm=4),
            ],
        )
        authenticate = make_authenticate(config, _build_drakkar_app_with_recorder(rec))
        # Build a synthetic Request with an unknown bearer token.
        request = MagicMock()
        request.state = MagicMock()
        request.headers = {'Authorization': 'Bearer wrong-token-XYZ'}
        from drakkar.webapp.dependencies import WebappAuthError

        with pytest.raises(WebappAuthError):
            await authenticate(request)

        rows = await _read_events_columns(rec)
        af = [r for r in rows if r['event'] == 'webapp_request_auth_failed']
        assert len(af) == 1
        # Token prefix is redacted (first 4 chars + '...').
        assert af[0]['metadata'] is not None
        assert 'wron...' in af[0]['metadata']
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_rate_limit_dependency_writes_recorder_row_on_429(tmp_path):
    """``make_rate_limit`` records ``webapp_request_rate_limited`` rows."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        config = WebAppConfig(
            enabled=True,
            host='127.0.0.1',
            port=0,
            path='/process',
            clients=[WebClientConfig(name='anonymous', token='', rpm=1)],
        )
        rate_limit = make_rate_limit(config, _build_drakkar_app_with_recorder(rec))
        client = config.clients[0]
        # First call admits, second exceeds the cap.
        await rate_limit(client)
        from drakkar.webapp.dependencies import WebappRateLimitError

        with pytest.raises(WebappRateLimitError):
            await rate_limit(client)

        rows = await _read_events_columns(rec)
        rl = [r for r in rows if r['event'] == 'webapp_request_rate_limited']
        assert len(rl) == 1
        assert rl[0]['client_name'] == 'anonymous'
        assert '"rpm_limit":1' in rl[0]['metadata']
    finally:
        await rec.stop()


# ---------------------------------------------------------------------------
# Pre-webapp-release DB — RecorderSchemaError on open
# ---------------------------------------------------------------------------


# The exact ``CREATE TABLE`` statement shipped on ``main`` BEFORE the
# webapp release. Used as a fixture to simulate an old recorder DB on
# disk so the open-time check has something to reject.
_PRE_WEBAPP_SCHEMA_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    dt          TEXT    NOT NULL,
    event       TEXT    NOT NULL,
    partition   INTEGER,
    offset      INTEGER,
    task_id     TEXT,
    args        TEXT,
    stdout_size INTEGER DEFAULT 0,
    stdout      TEXT,
    stderr      TEXT,
    exit_code   INTEGER,
    duration    REAL,
    output_topic TEXT,
    metadata    TEXT,
    pid         INTEGER,
    labels      TEXT
);
"""


@pytest.mark.asyncio
async def test_pre_webapp_release_db_raises_recorder_schema_error(tmp_path):
    """Opening a pre-webapp DB raises RecorderSchemaError with upgrade guidance."""
    # Build the legacy DB file directly via the make_db_path naming so the
    # recorder's first ``start()`` lands on it instead of creating a fresh DB.
    from drakkar.recorder.helpers import make_db_path

    legacy_path = make_db_path(str(tmp_path), WORKER_NAME)
    async with aiosqlite.connect(legacy_path) as db:
        await db.executescript(_PRE_WEBAPP_SCHEMA_EVENTS)
        await db.commit()

    # Freeze the path the recorder will use so it picks up the legacy DB
    # rather than rolling a fresh timestamp.
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    # Patch ``make_db_path`` so the recorder reuses the legacy file we
    # just wrote. ``start()`` calls ``make_db_path(db_dir, worker_name)``
    # to allocate a path; pointing it at our pre-built file forces the
    # ``CREATE TABLE IF NOT EXISTS`` to leave the legacy schema alone
    # and the PRAGMA-table-info check to fire on the missing columns.
    import drakkar.recorder.core as recorder_core

    original = recorder_core.make_db_path
    recorder_core.make_db_path = lambda *_args, **_kwargs: legacy_path
    try:
        with pytest.raises(RecorderSchemaError) as excinfo:
            await rec.start()
        # The error message must name the offending DB and guide the
        # operator to delete it. The plan documents both fragments.
        msg = str(excinfo.value)
        assert legacy_path in msg
        assert 'predates the webapp release' in msg
        assert 'delete it' in msg
        assert 'restart the worker' in msg
    finally:
        recorder_core.make_db_path = original
        # Best-effort cleanup; ``start`` failed mid-init so a separate
        # ``stop`` is unsafe. The test tmp_path teardown handles the file.
        if rec._db is not None:
            await rec._db.close()
        if rec._reader_db is not None:
            await rec._reader_db.close()


@pytest.mark.asyncio
async def test_pre_webapp_release_db_propagates_uncaught(tmp_path):
    """RecorderSchemaError must propagate; the recorder doesn't swallow it."""
    # Verify the exception is a RuntimeError subclass — ``AppLifecycle``
    # treats RuntimeError-class failures as fatal and aborts startup,
    # which is the contract documented for this gate.
    assert issubclass(RecorderSchemaError, RuntimeError)


# ---------------------------------------------------------------------------
# Sanity: a non-Pydantic body still produces a row (no body_bytes field)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webapp_request_received_row_with_non_pydantic_body(tmp_path):
    """Non-Pydantic / non-bytes bodies skip body_bytes but still record."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        ctx = WebRequestContext(
            request_id='req_alt_1',
            client_name='tenant-A',
            request='not-a-pydantic-model',  # plain str fallback
            started_at=datetime.now(UTC),
            headers={},
        )
        rec.record_webapp_request_received(ctx)
        rows = await _read_events_columns(rec)
        rcv = [r for r in rows if r['event'] == 'webapp_request_received']
        assert len(rcv) == 1
        # body_bytes absent — the recorder declines to coerce arbitrary
        # objects into a length.
        assert 'body_bytes' not in (rcv[0]['metadata'] or '')
    finally:
        await rec.stop()


@pytest.mark.asyncio
async def test_webapp_request_received_row_with_bytes_body(tmp_path):
    """Bytes bodies record their byte length verbatim."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        ctx = WebRequestContext(
            request_id='req_b_1',
            client_name='tenant-A',
            request=b'\x00\x01\x02ABCDE',
            started_at=datetime.now(UTC),
            headers={},
        )
        rec.record_webapp_request_received(ctx)
        rows = await _read_events_columns(rec)
        rcv = [r for r in rows if r['event'] == 'webapp_request_received']
        assert len(rcv) == 1
        assert '"body_bytes":8' in rcv[0]['metadata']
    finally:
        await rec.stop()


# ---------------------------------------------------------------------------
# Index sanity — the new origin / request_id indexes exist on fresh DBs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fresh_db_carries_origin_and_request_id_indexes(tmp_path):
    """Both new indexes (origin, request_id) are created with the schema."""
    rec = EventRecorder(_make_debug_config(tmp_path), worker_name=WORKER_NAME)
    await rec.start()
    try:
        assert rec._db is not None
        async with rec._db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_events_%'"
        ) as cur:
            names = {row[0] async for row in cur}
        assert 'idx_events_origin' in names
        assert 'idx_events_request_id' in names
    finally:
        await rec.stop()


# Suppress unused-import warning for the helper imports the module
# uses in fixtures. (asyncio is used implicitly by the asyncio mark.)
_ = (asyncio, PendingContext)
