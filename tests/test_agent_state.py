"""The state snapshot of the agent routes: each part from fakes, the payload rule and the main-loop dispatch."""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from drakkar.agent.params import NO_RECORDER_STORE_NOTE, STATE_PARTS
from drakkar.agent.registry import dumps
from drakkar.agent.rules import RULE_METRICS
from drakkar.agent.state import (
    MAX_RECENT_FAILURES,
    STDERR_TAIL_BYTES,
    autodiscover_part,
    budget_part,
    counter_total,
    failure_row,
    health_part,
    partitions_part,
    read_recent_failures,
    read_state,
    recent_failures_query,
    recorder_part,
    rule_metric_values,
    sinks_part,
    stderr_tail,
)
from drakkar.budget import PendingBudget
from drakkar.config import DrakkarConfig
from drakkar.recorder.schema import SCHEMA_EVENTS
from drakkar.schedule import cluster_tasks_hash
from drakkar.sinks.manager import SinkStats
from tests.conftest import kafka_connections

PAYLOAD_KEYS = ('stderr_tail', 'args')


def _config(**sections: Any) -> DrakkarConfig:
    values: dict[str, Any] = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'ui': {'enabled': False, 'release': {'enabled': False}},
        'agent': {'enabled': True},
    }
    return DrakkarConfig(**{**values, **sections})


# --- health ---


def test_health_with_the_monitor_off():
    assert health_part(None, uptime_seconds=12.5, stalls_total=0) == {
        'state': 'off',
        'heartbeat_age_ms': None,
        'current_lag_ms': None,
        'window_peak_lag_ms': None,
        'stalls_total': 0,
        'open_episode': None,
        'uptime_seconds': 12.5,
    }


def test_health_from_the_monitor_view():
    monitor = MagicMock()
    monitor.brief_view.return_value = {
        'state': 'lagging',
        'heartbeat_age_ms': 250.0,
        'current_lag_ms': 12.25,
        'window_peak_lag_ms': 180.0,
        'open_episode': 'cpu_bound',
    }

    part = health_part(monitor, uptime_seconds=60.0, stalls_total=3.0)

    assert dumps(part) == dumps(
        {
            'state': 'lagging',
            'heartbeat_age_ms': 250,
            'current_lag_ms': 12.25,
            'window_peak_lag_ms': 180,
            'stalls_total': 3,
            'open_episode': 'cpu_bound',
            'uptime_seconds': 60,
        }
    )


def test_the_monitor_brief_view_reads_the_window_peak():
    from drakkar.config.runtime import RuntimeHealthConfig
    from drakkar.runtimehealth import RuntimeHealthMonitor

    monitor = RuntimeHealthMonitor(RuntimeHealthConfig(enabled=True))
    monitor._window.add(time.time(), 0.25)

    view = monitor.brief_view()

    assert list(view) == ['state', 'heartbeat_age_ms', 'current_lag_ms', 'window_peak_lag_ms', 'open_episode']
    assert view['state'] == 'healthy'
    assert view['window_peak_lag_ms'] == 250.0
    assert view['open_episode'] is None


def test_the_monitor_brief_view_names_the_verdict_of_the_open_episode():
    from drakkar.config.runtime import RuntimeHealthConfig
    from drakkar.runtimehealth import RuntimeHealthMonitor, _Episode

    monitor = RuntimeHealthMonitor(RuntimeHealthConfig(enabled=True))
    monitor._episode = _Episode(time.monotonic(), time.time(), max_stacks=4)

    view = monitor.brief_view()

    assert view['open_episode'] == monitor.snapshot()['current_episode']['verdict']
    assert isinstance(view['open_episode'], str)


# --- budget ---


async def test_budget_from_the_pending_budget_and_the_pool():
    budget = PendingBudget(max_tasks=100, max_bytes=4096)
    budget.charge(7, 512)
    budget.add_waiter()
    budget.add_waiter().set_result(False)
    pool = SimpleNamespace(active_count=3, max_executors=4, waiting_count=2)

    part = budget_part(budget, pool, backpressure_active=True, config=_config())

    assert part == {
        'tasks': 7,
        'max_tasks': 100,
        'bytes': 512,
        'max_bytes': 4096,
        'waiting_loops': 1,
        'backpressure_active': True,
        'pool_active': 3,
        'pool_max': 4,
        'pool_waiting': 2,
    }


def test_budget_before_start_reads_the_limits_of_the_config():
    config = _config(executor={'max_executors': 3, 'max_pending_tasks': 50, 'max_pending_bytes': '32MiB'})

    part = budget_part(None, None, backpressure_active=False, config=config)

    assert part == {
        'tasks': 0,
        'max_tasks': 50,
        'bytes': 0,
        'max_bytes': 32 * 1024 * 1024,
        'waiting_loops': 0,
        'backpressure_active': False,
        'pool_active': 0,
        'pool_max': 3,
        'pool_waiting': 0,
    }


# --- partitions ---


def _processor(*, queued: int = 0, pending: int = 0, committed: int | None = None, dead: bool = False) -> Any:
    tracker = SimpleNamespace(pending_count=pending, last_committed=committed)
    return SimpleNamespace(queue_size=queued, offset_tracker=tracker, is_dead=dead)


def _source(**fields: Any) -> Any:
    values: dict[str, Any] = {
        'processors': {},
        'paused': False,
        'stalled_partitions': set(),
        'dead_partitions_paused': set(),
    }
    return SimpleNamespace(**{**values, **fields})


def test_partitions_without_a_kafka_source_are_empty():
    assert partitions_part(None, debug_paused=False, lags={}) == []


def test_partitions_name_every_pause_reason_sorted():
    source = _source(
        processors={
            2: _processor(queued=4, pending=3, committed=99),
            0: _processor(dead=True),
            1: _processor(),
        },
        paused=True,
        stalled_partitions={1},
        dead_partitions_paused={0},
    )

    rows = partitions_part(source, debug_paused=True, lags={2: 17})

    assert rows == [
        {
            'partition': 0,
            'queued': 0,
            'pending_offsets': 0,
            'committed': None,
            'lag': None,
            'paused': True,
            'paused_by': ['backpressure', 'death', 'debug'],
            'dead': True,
        },
        {
            'partition': 1,
            'queued': 0,
            'pending_offsets': 0,
            'committed': None,
            'lag': None,
            'paused': True,
            'paused_by': ['backpressure', 'debug'],
            'dead': False,
        },
        {
            'partition': 2,
            'queued': 4,
            'pending_offsets': 3,
            'committed': 99,
            'lag': 17,
            'paused': True,
            'paused_by': ['backpressure', 'debug'],
            'dead': False,
        },
    ]


def test_a_stalled_partition_is_paused_with_no_named_reason():
    source = _source(processors={5: _processor()}, stalled_partitions={5})

    (row,) = partitions_part(source, debug_paused=False, lags={})

    assert (row['paused'], row['paused_by']) == (True, [])


# --- sinks ---


def _sink(state: str) -> Any:
    return SimpleNamespace(circuit_state=state)


def test_sinks_sorted_with_breaker_and_delivery_age():
    manager = SimpleNamespace(
        sinks={
            ('postgres', 'main-db'): _sink('open'),
            ('kafka', 'results'): _sink('closed'),
            ('http', 'hook'): _sink('half_open'),
        },
        get_all_stats=lambda: {
            ('kafka', 'results'): SinkStats(delivered_count=12, error_count=1, last_delivery_ts=990.0),
            ('postgres', 'main-db'): SinkStats(error_count=4),
        },
    )

    rows = sinks_part(manager, now=1000.25)

    assert rows == [
        {
            'type': 'http',
            'name': 'hook',
            'breaker_open': True,
            'delivered_total': 0,
            'failed_total': 0,
            'last_delivery_seconds': None,
        },
        {
            'type': 'kafka',
            'name': 'results',
            'breaker_open': False,
            'delivered_total': 12,
            'failed_total': 1,
            'last_delivery_seconds': 10.25,
        },
        {
            'type': 'postgres',
            'name': 'main-db',
            'breaker_open': True,
            'delivered_total': 0,
            'failed_total': 4,
            'last_delivery_seconds': None,
        },
    ]


# --- recorder ---


def test_recorder_without_a_recorder_object():
    config = _config(recorder={'enabled': False})

    assert recorder_part(config, None, now=0.0) == {
        'enabled': False,
        'provider': 'none',
        'memory': None,
        'sqlite': None,
        'postgres': None,
    }


def test_recorder_is_the_block_of_the_databases_route(monkeypatch):
    calls: list[Any] = []

    def fake_block(config: Any, recorder: Any, *, now: float) -> dict[str, Any]:
        calls.append((config, recorder, now))
        return {'enabled': True, 'provider': 'sqlite'}

    monkeypatch.setattr('drakkar.agent.state.recorder_block', fake_block)
    config = _config()
    recorder = object()

    assert recorder_part(config, recorder, now=5.0) == {'enabled': True, 'provider': 'sqlite'}
    assert calls == [(config, recorder, 5.0)]


# --- autodiscover ---


def _view(worker: str, *, online: bool | None, cluster: str = 'main', hash_: str | None = None) -> Any:
    card: dict[str, Any] = {'worker': worker, 'cluster': cluster}
    if hash_ is not None:
        card['features'] = {'periodic': {'cluster_hash': hash_}}
    return SimpleNamespace(card=card, online=online)


def test_autodiscover_off():
    assert autodiscover_part(_config(), None, own_cluster='main', own_hash='0' * 16) == {
        'enabled': False,
        'provider': None,
        'peers_online': None,
        'peers_offline': None,
        'cluster_tasks_match': None,
    }


def _autodiscover_config() -> DrakkarConfig:
    return _config(
        api={'enabled': True},
        storage={'shared_dir': '/tmp/shared'},
        autodiscover={'enabled': True, 'provider': 'filesystem'},
    )


def test_autodiscover_before_the_registry_started():
    assert autodiscover_part(_autodiscover_config(), None, own_cluster='main', own_hash='x') == {
        'enabled': True,
        'provider': 'filesystem',
        'peers_online': None,
        'peers_offline': None,
        'cluster_tasks_match': None,
    }


def test_autodiscover_counts_peers_and_compares_the_cluster_hash_of_online_peers():
    own = cluster_tasks_hash(())
    other = 'a' * 16
    fleet = SimpleNamespace(
        peer_views=lambda: [
            _view('w-1', online=True, hash_=own),
            _view('w-2', online=False, hash_=other),
            _view('w-3', online=None),
            _view('w-4', online=True, cluster='elsewhere', hash_=other),
        ]
    )

    part = autodiscover_part(_autodiscover_config(), fleet, own_cluster='main', own_hash=own)

    assert part == {
        'enabled': True,
        'provider': 'filesystem',
        'peers_online': 2,
        'peers_offline': 1,
        'cluster_tasks_match': True,
    }


def test_autodiscover_reports_a_mismatch_of_an_online_peer_of_the_same_cluster():
    fleet = SimpleNamespace(peer_views=lambda: [_view('w-1', online=True, hash_='b' * 16)])

    part = autodiscover_part(_autodiscover_config(), fleet, own_cluster='main', own_hash='c' * 16)

    assert part['cluster_tasks_match'] is False


# --- recent failures ---


def test_stderr_tail_keeps_the_last_bytes_at_a_character_boundary():
    assert stderr_tail('short') == 'short'
    assert stderr_tail('') == ''
    text = 'x' * 10 + 'é' * STDERR_TAIL_BYTES
    tail = stderr_tail(text)
    assert len(tail.encode('utf-8')) <= STDERR_TAIL_BYTES
    assert tail == 'é' * (STDERR_TAIL_BYTES // 2)
    odd = 'é' * STDERR_TAIL_BYTES + 'z'
    assert stderr_tail(odd) == 'é' * (STDERR_TAIL_BYTES // 2 - 1) + 'z'


def _failure(**fields: Any) -> dict[str, Any]:
    row = {
        'id': 7,
        'ts': 1700000000.5,
        'partition': 3,
        'offset': None,
        'task_id': 't-1',
        'exit_code': 2,
        'duration': 1.25,
        'error_kind': 'nonzero_exit',
        'stderr': 'boom',
        'args': '["--flag", "value"]',
    }
    return {**row, **fields}


def test_a_failure_row_without_payloads():
    assert failure_row(_failure(), include_payloads=False) == {
        'task_id': 't-1',
        'ts': 1700000000.5,
        'partition': 3,
        'offset': None,
        'exit_code': 2,
        'duration': 1.25,
        'error_kind': 'nonzero_exit',
    }


def test_a_failure_row_with_payloads():
    row = failure_row(_failure(), include_payloads=True)

    assert row['stderr_tail'] == 'boom'
    assert row['args'] == ['--flag', 'value']
    assert list(row)[-2:] == list(PAYLOAD_KEYS)


@pytest.mark.parametrize(
    ('stderr', 'args', 'tail', 'arg_list'),
    [
        (None, None, '', []),
        ('', 'not json', '', []),
        ('e', '{"a": 1}', 'e', []),
        ('e', '[1, "x"]', 'e', ['1', 'x']),
    ],
)
def test_a_failure_row_with_missing_or_odd_payload_columns(stderr, args, tail, arg_list):
    row = failure_row(_failure(stderr=stderr, args=args), include_payloads=True)

    assert (row['stderr_tail'], row['args']) == (tail, arg_list)


def test_an_error_kind_that_is_not_text_is_null():
    assert failure_row(_failure(error_kind=5), include_payloads=False)['error_kind'] is None


def _events_db(rows: list[dict[str, Any]]) -> sqlite3.Connection:
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA_EVENTS)
    for row in rows:
        columns = ', '.join(f'"{name}"' for name in row)
        marks = ', '.join('?' for _ in row)
        db.execute(f'INSERT INTO events ({columns}) VALUES ({marks})', list(row.values()))
    return db


@pytest.mark.parametrize('include_payloads', [False, True])
def test_the_failures_query_reads_the_newest_failed_tasks(include_payloads):
    rows = [
        {
            'id': number,
            'ts': 100.0 + number // 2,
            'dt': 'x',
            'event': 'task_failed',
            'task_id': f't-{number}',
            'metadata': json.dumps({'exception': None, 'kind': 'timeout', 'stdin': 'secret input'}),
            'stderr': 'err',
            'args': '["a"]',
        }
        for number in range(1, 30)
    ]
    rows.append({'id': 40, 'ts': 999.0, 'dt': 'x', 'event': 'task_completed', 'task_id': 'done'})
    rows.append({'id': 41, 'ts': 1.0, 'dt': 'x', 'event': 'task_failed', 'task_id': 'odd', 'metadata': 'not json'})
    query, params = recent_failures_query(include_payloads=include_payloads)

    with closing(_events_db(rows)) as db:
        cursor = db.execute(query, params)
        columns = [description[0] for description in cursor.description]
        found = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    assert len(found) == MAX_RECENT_FAILURES
    assert [row['task_id'] for row in found[:3]] == ['t-29', 't-28', 't-27']
    assert found[0]['error_kind'] == 'timeout'
    assert ('stderr' in columns, 'args' in columns) == (include_payloads, include_payloads)
    assert 'metadata' not in columns
    assert 'secret input' not in json.dumps(found)


def test_the_failures_query_reads_no_kind_from_bad_metadata():
    bad = {'id': 1, 'ts': 1.0, 'dt': 'x', 'event': 'task_failed', 'task_id': 'x', 'metadata': '{bad'}
    query, params = recent_failures_query(include_payloads=False)

    with closing(_events_db([bad])) as db:
        (row,) = db.execute(query, params).fetchall()

    assert row[-1] is None


def _store(columns: list[str], rows: list[tuple[Any, ...]]) -> Any:
    store = MagicMock()
    store.is_open = True
    store.read = AsyncMock(return_value=(columns, rows))
    return store


async def test_recent_failures_without_a_recorder_store():
    assert await read_recent_failures(None, include_payloads=False) == {
        'failures': [],
        'note': NO_RECORDER_STORE_NOTE,
    }
    recorder = SimpleNamespace(memory_store=None)
    assert (await read_recent_failures(recorder, include_payloads=False))['note'] == NO_RECORDER_STORE_NOTE


async def test_recent_failures_read_the_memory_store_after_its_flush():
    columns = ['id', 'ts', 'partition', 'offset', 'task_id', 'exit_code', 'duration', 'error_kind']
    store = _store(columns, [(3, 5.0, 1, None, 't', 1, None, 'nonzero_exit')])
    recorder = SimpleNamespace(memory_store=store, flush_memory=AsyncMock())

    answer = await read_recent_failures(recorder, include_payloads=False)

    recorder.flush_memory.assert_awaited_once()
    assert answer == {
        'failures': [
            {
                'task_id': 't',
                'ts': 5.0,
                'partition': 1,
                'offset': None,
                'exit_code': 1,
                'duration': None,
                'error_kind': 'nonzero_exit',
            }
        ]
    }


async def test_recent_failures_with_a_read_that_gives_no_answer():
    store = MagicMock()
    store.is_open = False
    recorder = SimpleNamespace(memory_store=store, flush_memory=AsyncMock())

    assert await read_recent_failures(recorder, include_payloads=False) == {
        'failures': [],
        'note': NO_RECORDER_STORE_NOTE,
    }


async def test_recent_failures_when_the_store_closes_under_the_read(monkeypatch):
    monkeypatch.setattr('drakkar.agent.state.read_memory', AsyncMock(return_value=None))
    recorder = SimpleNamespace(memory_store=_store([], []), flush_memory=AsyncMock())

    assert await read_recent_failures(recorder, include_payloads=False) == {'failures': []}


# --- metrics ---


def test_counter_total_sums_every_label_set():
    from prometheus_client import CollectorRegistry, Counter

    counter = Counter('agent_state_test_total', 'help', ['partition'], registry=CollectorRegistry())
    counter.labels(partition='0').inc(2)
    counter.labels(partition='1').inc(3)

    assert counter_total(counter) == 5


def test_rule_metric_values_name_every_rule_metric():
    assert list(rule_metric_values()) == list(RULE_METRICS)


# --- the whole snapshot ---


def _state_app(config: DrakkarConfig, **fields: Any) -> Any:
    app = MagicMock()
    app._config = config
    app._start_time = time.monotonic() - 30
    app._runtime_health = None
    app._pending_budget = None
    app._executor_pool = None
    app._paused = False
    app.kafka_source = None
    app.consume_pause = SimpleNamespace(active=False)
    app.sink_manager = SimpleNamespace(sinks={}, get_all_stats=dict)
    app._recorder = None
    app.fleet = None
    app.worker_identity = SimpleNamespace(cluster='main')
    app.cluster_tasks_hash = cluster_tasks_hash(())
    for name, value in fields.items():
        setattr(app, name, value)
    return app


async def test_read_state_answers_every_part_in_the_contract_order():
    state = await read_state(_state_app(_config()), STATE_PARTS, include_payloads=False)

    assert list(state) == list(STATE_PARTS)
    assert state['health']['state'] == 'off'
    assert state['partitions'] == []
    assert state['recent_failures'] == {'failures': [], 'note': NO_RECORDER_STORE_NOTE}


async def test_read_state_answers_only_the_asked_parts_and_skips_the_loop_when_none_needs_it(monkeypatch):
    dispatch = AsyncMock()
    monkeypatch.setattr('drakkar.agent.state.dispatch_bounded', dispatch)

    state = await read_state(_state_app(_config()), ['health', 'recorder', 'autodiscover'], include_payloads=False)

    assert list(state) == ['health', 'recorder', 'autodiscover']
    dispatch.assert_not_awaited()


async def test_a_main_loop_that_does_not_answer_gives_empty_loop_parts(monkeypatch):
    async def no_answer(coro: Any, main_loop: Any) -> None:
        coro.close()

    monkeypatch.setattr('drakkar.agent.state.dispatch_bounded', no_answer)

    state = await read_state(_state_app(_config()), STATE_PARTS, include_payloads=False)

    assert state['budget'] == {}
    assert state['partitions'] == []
    assert state['sinks'] == []
    assert state['recent_failures'] == {'failures': []}
    assert state['health']['state'] == 'off'


async def test_payload_text_reaches_only_recent_failures_and_only_when_switched_on():
    stderr = 'PAYLOAD-STDERR-TEXT'
    args = '["PAYLOAD-ARG"]'

    async def read(query: str, params: Any) -> tuple[list[str], list[tuple[Any, ...]]]:
        columns = ['id', 'ts', 'partition', 'offset', 'task_id', 'exit_code', 'duration', 'error_kind']
        row: tuple[Any, ...] = (1, 2.0, 0, None, 't', 1, 0.5, 'nonzero_exit')
        if 'stderr' in query:
            columns += ['stderr', 'args']
            row += (stderr, args)
        return columns, [row]

    store = MagicMock()
    store.is_open = True
    store.read = read
    recorder = SimpleNamespace(memory_store=store, flush_memory=AsyncMock(), persistence='none')
    app = _state_app(_config(), _recorder=recorder)

    off = await read_state(app, STATE_PARTS, include_payloads=False)
    on = await read_state(app, STATE_PARTS, include_payloads=True)

    assert 'PAYLOAD' not in dumps(off)
    (failure,) = on['recent_failures']['failures']
    assert (failure['stderr_tail'], failure['args']) == (stderr, ['PAYLOAD-ARG'])
    without_failures = {part: value for part, value in on.items() if part != 'recent_failures'}
    assert 'PAYLOAD' not in dumps(without_failures)


async def test_read_state_reads_the_live_objects():
    budget = PendingBudget(max_tasks=10, max_bytes=100)
    budget.charge(10, 1)
    source = _source(processors={0: _processor(queued=2)})
    app = _state_app(
        _config(),
        _pending_budget=budget,
        kafka_source=source,
        consume_pause=SimpleNamespace(active=True),
        sink_manager=SimpleNamespace(sinks={('kafka', 'out'): _sink('open')}, get_all_stats=dict),
    )

    state = await read_state(app, STATE_PARTS, include_payloads=False)

    assert state['budget']['tasks'] == 10
    assert state['partitions'][0]['paused_by'] == ['debug']
    assert state['sinks'][0]['breaker_open'] is True
    assert state['health']['uptime_seconds'] >= 30
