"""Shared test fixtures for Drakkar tests."""

import asyncio
import contextlib
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import yaml

from drakkar import partition as partition_module
from drakkar.cache.changes import parse_changes_query
from drakkar.cache.models import _now_ms
from drakkar.cache.sql import CHANGES_FIELDS, OWN_MARK_SQL, SCHEMA_CACHE_ENTRIES, changes_query
from drakkar.config import CacheExchangeConfig, UIConfig, UIRecorderConfig
from drakkar.fleet.service import PeerView
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    SourceMessage,
)

_RECORDER_FIELDS = frozenset(UIRecorderConfig.model_fields)


def make_ui_config(**kwargs) -> UIConfig:
    """Build a ``UIConfig`` from flat kwargs, routing recorder-tier fields.

    Test convenience so the many call sites that mix server settings with
    persistence settings (``make_ui_config(port=8080, db_dir='/tmp')``)
    stay one-liners after the ``debug.*``→``ui.*`` merge. ``recorder=`` /
    ``release=`` kwargs still pass through for tests that want the nested
    form explicitly.
    """
    recorder_kwargs = {key: kwargs.pop(key) for key in list(kwargs) if key in _RECORDER_FIELDS}
    if recorder_kwargs:
        assert 'recorder' not in kwargs, 'mix of flat recorder kwargs and recorder= is ambiguous'
        kwargs['recorder'] = UIRecorderConfig(**recorder_kwargs)
    return UIConfig(**kwargs)


def split_cache_overrides(overrides: dict | None) -> tuple[dict, CacheExchangeConfig]:
    """Split a test's cache overrides into ``CacheConfig`` fields and exchange settings.

    Cache-engine tests pass one ``cache_overrides`` dict; its ``'exchange'``
    entry (a ``CacheExchangeConfig`` or a plain dict) becomes the engine's
    ``fleet.cache_exchange`` settings and the rest builds the ``CacheConfig``.
    """
    remaining = dict(overrides or {})
    exchange = remaining.pop('exchange', None)
    if exchange is None:
        exchange = CacheExchangeConfig()
    elif isinstance(exchange, dict):
        exchange = CacheExchangeConfig(**exchange)
    return remaining, exchange


# Default ceiling for wait_for. It is a safety net that turns a stuck test
# into a readable failure — not an assertion about how fast the code is. The
# suite runs one worker per core and many of these conditions wait on a real
# subprocess, so under that contention a few seconds is not a generous budget:
# five seconds produced a flaky failure roughly one run in three. Nothing pays
# this cost when the condition holds, and pytest-timeout still bounds a true hang
# at 60 s.
WAIT_FOR_TIMEOUT_SECONDS = 20.0


async def wait_for(condition, timeout=WAIT_FOR_TIMEOUT_SECONDS, interval=0.05):
    """Poll until condition() returns True or timeout expires."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(interval)
    raise TimeoutError(f'Condition not met within {timeout}s')


# The partition loop blocks this long on an empty queue before waking to
# retry a commit. In production a second is right — it costs nothing and
# bounds how long a quiet partition holds progress. In a test it is dead
# waiting: every test that stops a running processor pays it once, which was
# most of the suite's tests over one second.
TEST_IDLE_POLL_TIMEOUT = 0.02


@pytest.fixture(autouse=True)
def fast_idle_poll(monkeypatch: pytest.MonkeyPatch) -> None:
    """Shorten the partition loop's idle wake-up for every test.

    Only the wake-up cadence changes: the loop does the same work, and a
    test that needs the production value can set it back.
    """
    monkeypatch.setattr(partition_module, 'IDLE_POLL_TIMEOUT', TEST_IDLE_POLL_TIMEOUT)


def wire_kafka_source(app, consumer=None):
    """Bind the app's sources and attach a fake consumer, as startup does.

    Tests that drive ``kafka_source.on_assign`` or ``_shutdown`` directly
    skip ``_async_run``, so without this the source has no
    ``SourceContext`` and no consumer. Returns the Kafka source.

    Binding needs an executor pool; a test that only exercises the
    rebalance or commit paths never touches one, so a stand-in is
    supplied rather than making every such test build a real pool.
    """
    if app._executor_pool is None:
        app._executor_pool = MagicMock(active_count=0, max_executors=1)
    app._lifecycle._bind_sources()
    source = app.kafka_source
    assert source is not None, 'sources.kafka must be enabled for this helper'
    source.consumer = AsyncMock() if consumer is None else consumer
    return source


@pytest.fixture
def source_message() -> SourceMessage:
    return SourceMessage(
        topic='test-topic',
        partition=0,
        offset=42,
        key=b'key-1',
        value=b'{"data": "hello"}',
        timestamp=1700000000,
    )


@pytest.fixture
def executor_task() -> ExecutorTask:
    return ExecutorTask(
        task_id='task-001',
        args=['--input', 'test.txt'],
        metadata={'source': 'test'},
        source_offsets=[42],
    )


@pytest.fixture
def executor_result(executor_task: ExecutorTask) -> ExecutorResult:
    return ExecutorResult(
        exit_code=0,
        stdout='result line 1\nresult line 2\n',
        stderr='',
        duration_seconds=1.5,
        task=executor_task,
    )


@pytest.fixture
def executor_error(executor_task: ExecutorTask) -> ExecutorError:
    return ExecutorError(
        task=executor_task,
        exit_code=1,
        stderr='error: file not found',
    )


@pytest.fixture
def minimal_config_dict() -> dict:
    return {
        'executor': {
            'binary_path': '/usr/bin/echo',
        },
        'sources': {'kafka': {'enabled': True}},
    }


@pytest.fixture
def full_config_dict() -> dict:
    return {
        'kafka': {
            'brokers': 'kafka1:9092,kafka2:9092',
        },
        'sources': {
            'kafka': {
                'enabled': True,
                'topic': 'input-events',
                'consumer_group': 'drakkar-workers',
                'max_poll_records': 200,
                'max_poll_interval_ms': 600_000,
                'session_timeout_ms': 30_000,
                'heartbeat_interval_ms': 5_000,
            },
        },
        'executor': {
            'binary_path': '/usr/local/bin/processor',
            'max_executors': 40,
            'task_timeout_seconds': 300,
            'window_size': 100,
            'max_retries': 5,
            'drain_timeout_seconds': 10,
            'backpressure_high_multiplier': 16,
            'backpressure_low_multiplier': 2,
        },
        'sinks': {
            'kafka': {'results': {'topic': 'output-results'}},
            'postgres': {'main': {'dsn': 'postgresql://user:pass@db:5432/app', 'pool_min': 5, 'pool_max': 20}},
        },
        'metrics': {
            'enabled': True,
            'port': 9091,
        },
        'logging': {
            'level': 'DEBUG',
            'format': 'console',
        },
    }


@pytest.fixture
def config_yaml_file(full_config_dict: dict, tmp_path: Path) -> Path:
    config_path = tmp_path / 'drakkar.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(full_config_dict, f)
    return config_path


@pytest.fixture
def minimal_config_yaml_file(minimal_config_dict: dict, tmp_path: Path) -> Path:
    config_path = tmp_path / 'drakkar.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(minimal_config_dict, f)
    return config_path


class FakePeers:
    """Peer workers for cache-exchange tests, served in this process.

    Each peer is a cache DB file ``<folder>/<name>.db`` answered through the
    real changes query (``parse_changes_query`` + ``changes_sql``) by an
    ``httpx.MockTransport``. A peer is listed (has a card) once its cluster
    is set; its card carries the ``features.cache`` marks of the rows it
    originated, as a real worker publishes them.
    """

    def __init__(self, folder: Path) -> None:
        self.folder = folder
        self.folder.mkdir(parents=True, exist_ok=True)
        self.clusters: dict[str, str] = {}
        self.cards: dict[str, dict[str, Any]] = {}
        self.online: dict[str, bool | None] = {}
        # Per-peer failure: an HTTP status to answer, or an exception to raise.
        self.failures: dict[str, Any] = {}
        # Per-peer delay in seconds before answering.
        self.delays: dict[str, float] = {}
        self.requests: list[tuple[str, dict[str, str]]] = []

    def db_path(self, name: str) -> Path:
        return self.folder / f'{name}.db'

    def seed(self, name: str, rows: list[dict[str, Any]]) -> None:
        """Insert rows into the peer's cache DB (defaults: global scope, origin = the peer, timestamps 1000)."""
        with contextlib.closing(sqlite3.connect(self.db_path(name))) as db, db:
            db.executescript(SCHEMA_CACHE_ENTRIES)
            for r in rows:
                value = r.get('value', '"v"')
                db.execute(
                    'INSERT OR REPLACE INTO cache_entries (key, scope, value, size_bytes, created_at_ms, '
                    'updated_at_ms, expires_at_ms, origin_worker_id) VALUES (?,?,?,?,?,?,?,?)',
                    (
                        r['key'],
                        r.get('scope', 'global'),
                        value,
                        r.get('size_bytes', len(value.encode('utf-8'))),
                        r.get('created_at_ms', 1000),
                        r.get('updated_at_ms', 1000),
                        r.get('expires_at_ms'),
                        r.get('origin_worker_id', name),
                    ),
                )

    def marks(self, name: str) -> dict[str, dict[str, Any]]:
        """The peer's card marks: its newest own row per exchanged scope."""
        if not self.db_path(name).exists():
            return {}
        found: dict[str, dict[str, Any]] = {}
        with contextlib.closing(sqlite3.connect(self.db_path(name))) as db:
            for scope in ('cluster', 'global'):
                row = db.execute(OWN_MARK_SQL, (name, scope)).fetchone()
                if row is not None:
                    found[scope] = {'ms': row[0], 'key': row[1]}
        return found

    def views(self) -> list[PeerView]:
        """The registry's peer views: one per peer whose cluster is set."""
        views = []
        for name, cluster in sorted(self.clusters.items()):
            card = {
                'worker': name,
                'cluster': cluster,
                'url': f'http://{name}.peers.test:8080/',
                'state': 'running',
                'features': {'cache': self.marks(name)},
                **self.cards.get(name, {}),
            }
            views.append(PeerView(card=card, age_seconds=1.0, online=self.online.get(name, True), last_seen_ts=0.0))
        return views

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        name = request.url.host.split('.')[0]
        params = dict(request.url.params)
        self.requests.append((name, params))
        if name in self.delays:
            await asyncio.sleep(self.delays[name])
        failure = self.failures.get(name)
        if isinstance(failure, BaseException):
            raise failure
        if isinstance(failure, int):
            return httpx.Response(failure, json={'error': 'failing on purpose'})
        query = parse_changes_query(params)
        if isinstance(query, str):
            return httpx.Response(400, json={'error': query})
        sql, args = changes_query(
            origin=name, scopes=query.scopes, now_ms=_now_ms(), after=query.after, limit=query.limit + 1
        )
        with contextlib.closing(sqlite3.connect(self.db_path(name))) as db:
            fetched = db.execute(sql, args).fetchall()
        rows = [dict(zip(CHANGES_FIELDS, row, strict=True)) for row in fetched[: query.limit]]
        return httpx.Response(200, json={'rows': rows, 'more': len(fetched) > query.limit})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self._handle)


def fake_peers(engine: Any) -> FakePeers:
    """The engine's fake peer fleet, created and wired on first use (folder ``<storage.dir>/peers``)."""
    peers = getattr(engine, '_test_peers', None)
    if peers is None:
        peers = FakePeers(Path(engine._storage_dir) / 'peers')
        engine._test_peers = peers
        engine._peer_views = peers.views
        engine._peer_transport = peers.transport()
    return peers


def set_peer_cluster(engine: Any, peer: str, cluster: str | None) -> None:
    """List ``peer`` in the registry with ``cluster``; ``None`` leaves it unlisted (never pulled)."""
    peers = fake_peers(engine)
    if cluster is None:
        peers.clusters.pop(peer, None)
    else:
        peers.clusters[peer] = cluster
