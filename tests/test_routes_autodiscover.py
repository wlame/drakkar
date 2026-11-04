"""``GET /api/v1/debug/autodiscover`` and ``/autodiscover/leases`` (contract v1.30).

Hermetic: a real registry service over the in-memory provider, a real
election service over the in-memory election, a mocked app with a real
config. The main loop is the test's own loop (the dispatch then runs the
read inline).
"""

from __future__ import annotations

import contextlib
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from structlog.testing import capture_logs

from drakkar.cluster_tasks import SLOT_HELD_ELSEWHERE, ClusterElection, SlotRecord
from drakkar.config import DrakkarConfig, RecorderConfig, ResilienceConfig, UIConfig
from drakkar.fleet.card import WorkerIdentity
from drakkar.fleet.service import FleetRegistry
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import EventRecorder
from drakkar.recorder.election import ElectionContext
from drakkar.recorder.lease import LeaseHolder
from drakkar.schedule import ClusterTaskDefinition
from drakkar.uiserver import routes_databases
from drakkar.uiserver.autodiscover_status import (
    LEASE_FAILED,
    card_rows,
    display_key,
    lease_row,
    lease_skeletons,
    lease_status,
)
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections
from tests.fleet_fakes import ElectionStore, MemoryElection, MemoryRegistry, MemoryStore

CLUSTER = 'search'
WORKER = 'w-1'
INSTANCE = '0123456789abcdef'
AUTODISCOVER = '/api/v1/debug/autodiscover'
LEASES = '/api/v1/debug/autodiscover/leases'
TASK = 'nightly_report'
SCOPE = f'{CLUSTER}/{TASK}'
ARCHIVE_SCOPE = f'{CLUSTER}/history.archive'


async def _no_sleep(_seconds: float) -> None:
    return None


def _config(*, autodiscover: bool = True, shared_dir: str = '/srv/drakkar-shared') -> DrakkarConfig:
    values: dict[str, Any] = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True}},
        'storage': {'shared_dir': shared_dir},
        'ui': {'enabled': True, 'release': {'enabled': False}},
        'recorder': {'enabled': False},
    }
    if autodiscover:
        values['autodiscover'] = {'enabled': True, 'provider': 'filesystem'}
    return DrakkarConfig(**values)


def _recorder() -> EventRecorder:
    return EventRecorder(RecorderConfig(), worker_name=WORKER, cluster_name=CLUSTER, persistence='none')


@contextlib.asynccontextmanager
async def _client(config: DrakkarConfig, *, fleet=None, election=None, roster=None):
    drakkar_app = MagicMock()
    drakkar_app._config = config
    drakkar_app.fleet = fleet
    drakkar_app.cluster_election = election
    drakkar_app.periodic_roster = roster
    app = create_ui_app(UIConfig(enabled=True), _recorder(), drakkar_app)
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
        yield client


def _identity(worker: str = WORKER, cluster: str = CLUSTER) -> WorkerIdentity:
    return WorkerIdentity(
        worker=worker,
        instance=INSTANCE,
        cluster=cluster,
        url='http://10.0.0.1:8080/',
        backend='python',
        version='2.5.0',
        started_at=datetime(2026, 11, 8, 9, 0, tzinfo=UTC),
    )


def _peer(worker: str, cluster: str = CLUSTER, **extra: Any) -> dict[str, Any]:
    card = {
        'schema': 1,
        'worker': worker,
        'instance': 'fedcba9876543210',
        'cluster': cluster,
        'backend': 'python',
        'version': '2.5.0',
        'url': f'http://{worker}:8080/',
        'state': 'running',
        'started_at': '2026-11-08T09:00:00.000000Z',
        'heartbeat_at': '2026-11-08T09:59:58.000000Z',
    }
    card.update(extra)
    return card


async def _registry(peers: list[dict[str, Any]], *, publish: bool = True) -> FleetRegistry:
    """A registry service over the memory provider, started, with one publish and one refresh done."""
    store = MemoryStore()
    for peer in peers:
        store.cards[peer['worker']] = (peer, store.now)
    config = _config().autodiscover
    fleet = FleetRegistry(
        MemoryRegistry(store),
        provider_name='filesystem',
        config=config,
        identity=_identity(),
        caller=ResilientCaller(config.resilience, feature='registry', provider='filesystem', sleep=_no_sleep),
    )
    if publish:
        assert await fleet._ensure_started()
        await fleet._publish_once('running')
        await fleet._refresh_once()
    return fleet


# --- /debug/autodiscover ---


async def test_autodiscover_with_autodiscover_off_answers_404():
    async with _client(_config(autodiscover=False)) as client:
        resp = await client.get(AUTODISCOVER)

    assert resp.status_code == 404
    assert resp.json() == {'error': 'autodiscover is off'}


async def test_autodiscover_answers_the_registry_this_worker_and_the_cards_own_cluster_first():
    fleet = await _registry([_peer('w-3', cluster='other'), _peer('w-2', features={'ui': True}), _peer('a-0', 'zzz')])

    async with _client(_config(), fleet=fleet) as client:
        resp = await client.get(AUTODISCOVER)

    assert resp.status_code == 200
    body = resp.json()
    registry = body['registry']
    assert registry['provider'] == 'filesystem'
    assert registry['connection'] is None
    assert registry['started'] is True
    assert registry['breaker_open'] is False
    assert registry['offline_after_seconds'] == 60
    assert registry['refreshed_at'].endswith('Z') and registry['last_publish_at'].endswith('Z')
    assert body['worker'] == {'name': WORKER, 'cluster': CLUSTER, 'instance': INSTANCE, 'key': 'memory/workers/w-1'}
    assert [(row['worker'], row['cluster']) for row in body['workers']] == [
        ('w-2', CLUSTER),
        ('w-3', 'other'),
        ('a-0', 'zzz'),
    ]
    first = body['workers'][0]
    assert first['features'] == {'ui': True}
    assert first['key'] == 'memory/workers/w-2'
    assert first['online'] is True
    assert first['state'] == 'running' and first['backend'] == 'python'
    assert body['workers'][1]['features'] == {}
    assert body['truncated'] is False


async def test_autodiscover_before_the_provider_started_has_no_keys_and_no_times():
    fleet = await _registry([], publish=False)

    async with _client(_config(), fleet=fleet) as client:
        body = (await client.get(AUTODISCOVER)).json()

    assert body['registry']['started'] is False
    assert (body['registry']['refreshed_at'], body['registry']['last_publish_at']) == (None, None)
    assert body['worker']['key'] is None
    assert body['workers'] == []


def test_card_rows_stop_at_the_limit_and_say_so():
    views = [SimpleNamespace(card=_peer(f'w-{i}'), age_seconds=1.0, online=True) for i in range(3)]

    rows, truncated = card_rows(views, own_cluster=CLUSTER, card_key=lambda worker: None, limit=2)

    assert [row['worker'] for row in rows] == ['w-0', 'w-1']
    assert truncated is True


@pytest.mark.parametrize(
    ('key', 'expected'),
    [
        (None, None),
        ('/srv/shared/fleet/workers/w-1.json', 'fleet/workers/w-1.json'),
        ("drakkar.fleet_workers (worker = 'w-1')", "drakkar.fleet_workers (worker = 'w-1')"),
        ('/elsewhere/w-1.json', '/elsewhere/w-1.json'),
    ],
)
def test_display_key_makes_a_path_in_the_shared_folder_relative(key, expected):
    assert display_key(key, '/srv/shared') == expected


def test_display_key_without_a_shared_folder_keeps_the_key():
    assert display_key('/any/path', '') == '/any/path'


# --- /debug/autodiscover/leases ---


def _roster() -> SimpleNamespace:
    return SimpleNamespace(
        definitions=(
            ClusterTaskDefinition(name='history.archive', interval=3600, cron=None, lease_seconds=60),
            ClusterTaskDefinition(name=TASK, interval=None, cron='0 3 * * *', lease_seconds=120),
        )
    )


def _election(provider: MemoryElection, **resilience: Any) -> ClusterElection:
    return ClusterElection(
        provider,
        ResilientCaller(
            ResilienceConfig(**{'max_retries': 0, **resilience}),
            feature='election',
            provider='memory',
            category='periodic',
            sleep=_no_sleep,
        ),
        provider_name='memory',
        context=ElectionContext(
            worker=WORKER, instance=INSTANCE, cluster=CLUSTER, shared_dir='', logger=MagicMock(), options={}
        ),
    )


async def test_leases_of_a_worker_without_a_cluster_task_answer_no_election():
    async with _client(_config()) as client:
        resp = await client.get(LEASES)

    assert resp.status_code == 200
    assert resp.json() == {'election': None, 'leases': []}


async def test_leases_before_the_election_started_make_no_store_call():
    provider = MemoryElection(ElectionStore())
    election = _election(provider)

    async with _client(_config(), election=election, roster=_roster()) as client:
        body = (await client.get(LEASES)).json()

    assert body['election'] == {'provider': 'memory', 'started': False, 'breaker_open': False}
    assert [(row['task'], row['status'], row['holder'], row['key']) for row in body['leases']] == [
        ('history.archive', 'not_started', None, None),
        (TASK, 'not_started', None, None),
    ]
    assert body['leases'][0]['system'] is True and body['leases'][1]['system'] is False
    assert body['leases'][0]['scope'] == ARCHIVE_SCOPE
    assert isinstance(body['leases'][0]['next_slot_ts'], int)
    assert provider.calls['start'] == 0 and provider.calls['holder'] == 0


async def test_leases_name_the_holder_of_a_held_scope_and_a_free_one():
    store = ElectionStore()
    other = MemoryElection(store)
    await other.start(
        ElectionContext(
            worker='w-2', instance='fedcba9876543210', cluster=CLUSTER, shared_dir='', logger=None, options={}
        )
    )
    await other.acquire(ARCHIVE_SCOPE, 'b' * 16, 60)
    store.advance(15)
    provider = MemoryElection(store)
    election = _election(provider)
    assert (await election.ensure_started(5.0)).ok
    election.record_slot('history.archive', 1_762_596_000, SLOT_HELD_ELSEWHERE)

    async with _client(_config(), election=election, roster=_roster()) as client:
        body = (await client.get(LEASES)).json()

    archive, task = body['leases']
    assert archive['status'] == 'held'
    assert archive['holder'] == {
        'worker': 'w-2',
        'instance': 'fedcba9876543210',
        'acquired_at': None,
        'age_seconds': 15.0,
        'expires_in_seconds': 45.0,
    }
    assert archive['key'] == f'memory/task-locks/{ARCHIVE_SCOPE}'
    assert archive['last_slot']['slot'] == 1_762_596_000
    assert archive['last_slot']['outcome'] == SLOT_HELD_ELSEWHERE
    assert archive['last_slot']['at'].endswith('Z')
    assert (task['status'], task['holder'], task['last_slot']) == ('free', None, None)
    assert provider.calls['holder'] == 2


async def test_leases_with_the_breaker_open_make_no_store_call():
    provider = MemoryElection(ElectionStore())
    election = _election(provider, breaker_failures=1)
    assert (await election.ensure_started(5.0)).ok

    async def fail() -> None:
        raise OSError('store down')

    await election.caller.call('acquire', fail)

    async with _client(_config(), election=election, roster=_roster()) as client:
        body = (await client.get(LEASES)).json()

    assert body['election']['breaker_open'] is True
    assert {row['status'] for row in body['leases']} == {'breaker_open'}
    assert provider.calls['holder'] == 0


async def test_leases_whose_read_fails_are_failed_and_logged_once():
    class Broken(MemoryElection):
        async def holder(self, scope: str) -> LeaseHolder | None:
            raise RuntimeError('the store answered nonsense')

    election = _election(Broken(ElectionStore()))
    assert (await election.ensure_started(5.0)).ok

    with capture_logs() as logs:
        async with _client(_config(), election=election, roster=_roster()) as client:
            body = (await client.get(LEASES)).json()

    assert {row['status'] for row in body['leases']} == {'failed'}
    failed = [entry for entry in logs if entry['event'] == 'ui_read_provider_failed']
    assert failed == [
        {
            'event': 'ui_read_provider_failed',
            'log_level': 'warning',
            'category': 'debug',
            'endpoint': LEASES,
            'provider': 'memory',
            'op': 'holder',
            'error': 'holder error',
            'suppressed': 0,
        }
    ]


async def test_leases_are_failed_when_the_main_loop_does_not_answer(monkeypatch):
    election = _election(MemoryElection(ElectionStore()))
    assert (await election.ensure_started(5.0)).ok

    async def no_answer(coro, main_loop, default=None):
        coro.close()
        return default

    monkeypatch.setattr(routes_databases, 'dispatch_bounded', no_answer)

    with capture_logs() as logs:
        async with _client(_config(), election=election, roster=_roster()) as client:
            body = (await client.get(LEASES)).json()

    assert {row['status'] for row in body['leases']} == {'failed'}
    errors = [entry['error'] for entry in logs if entry['event'] == 'ui_read_provider_failed']
    assert errors == ['the main loop did not answer in time']


async def test_leases_after_the_election_stopped_make_no_store_call():
    provider = MemoryElection(ElectionStore())
    election = _election(provider)
    assert (await election.ensure_started(5.0)).ok
    await election.stop(1.0)

    async with _client(_config(), election=election, roster=_roster()) as client:
        body = (await client.get(LEASES)).json()

    assert {row['status'] for row in body['leases']} == {'not_started'}
    assert provider.calls['holder'] == 0


async def test_leases_show_no_key_when_the_provider_cannot_name_it():
    class NoKey(MemoryElection):
        def lease_key(self, scope: str) -> str:
            raise RuntimeError('not started')

    election = _election(NoKey(ElectionStore()))
    assert (await election.ensure_started(5.0)).ok

    async with _client(_config(), election=election, roster=_roster()) as client:
        body = (await client.get(LEASES)).json()

    assert {row['key'] for row in body['leases']} == {None}


async def test_leases_make_a_filesystem_lock_path_relative_to_the_shared_folder(tmp_path):
    class FolderKeys(MemoryElection):
        def lease_key(self, scope: str) -> str:
            return os.path.join(str(tmp_path), 'tasks', scope + '.lock')

    election = _election(FolderKeys(ElectionStore()))
    assert (await election.ensure_started(5.0)).ok

    async with _client(_config(shared_dir=str(tmp_path)), election=election, roster=_roster()) as client:
        body = (await client.get(LEASES)).json()

    assert body['leases'][0]['key'] == f'tasks/{ARCHIVE_SCOPE}.lock'


def test_lease_row_derives_the_time_left_from_the_age_when_the_store_has_none():
    skeleton = lease_skeletons(
        [ClusterTaskDefinition(name=TASK, interval=60, cron=None, lease_seconds=120)],
        cluster=CLUSTER,
        now=1_762_596_000.0,
        last_slots={TASK: SlotRecord(slot=1_762_595_940, outcome='won', at=1_762_595_940.5)},
    )[0]

    row = lease_row(
        skeleton,
        status='held',
        holder=LeaseHolder(worker='w-2', instance=None, acquired_at='t', age_seconds=150.0),
        key=None,
    )

    assert skeleton.next_slot_ts == 1_762_596_060
    assert row['holder']['expires_in_seconds'] == -30.0
    assert row['last_slot'] == {'slot': 1_762_595_940, 'outcome': 'won', 'at': '2025-11-08T09:59:00.500000Z'}


def test_lease_skeleton_of_a_cron_that_never_fires_has_no_next_slot():
    skeleton = lease_skeletons(
        [ClusterTaskDefinition(name=TASK, interval=None, cron='0 0 31 2 *', lease_seconds=60)],
        cluster=CLUSTER,
        now=1_762_596_000.0,
        last_slots={},
    )[0]

    assert skeleton.next_slot_ts is None


def test_lease_status_of_a_read_that_raised_is_failed():
    assert lease_status(True, RuntimeError('boom')) == (LEASE_FAILED, None)


async def test_a_holder_read_that_raises_marks_only_its_row_failed(monkeypatch):
    election = _election(MemoryElection(ElectionStore()))
    assert (await election.ensure_started(5.0)).ok
    real_read = election.read_holder

    async def read_holder(scope: str):
        if scope == ARCHIVE_SCOPE:
            raise RuntimeError('the read broke before the caller')
        return await real_read(scope)

    monkeypatch.setattr(election, 'read_holder', read_holder)

    with capture_logs() as logs:
        async with _client(_config(), election=election, roster=_roster()) as client:
            body = (await client.get(LEASES)).json()

    assert [row['status'] for row in body['leases']] == ['failed', 'free']
    errors = [entry['error'] for entry in logs if entry['event'] == 'ui_read_provider_failed']
    assert errors == ['the read broke before the caller']
