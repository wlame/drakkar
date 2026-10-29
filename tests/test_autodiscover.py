"""The optional ``autodiscover`` feature: one provider for the worker registry and the election of cluster tasks."""

import asyncio

import pytest
from prometheus_client import REGISTRY
from pydantic import ValidationError
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.cache import CacheEngine
from drakkar.cluster_tasks import archive_task_runs
from drakkar.config import AutodiscoverConfig, CacheConfig, CacheExchangeConfig, DrakkarConfig
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.fleet.service import FleetRegistry
from drakkar.periodic import periodic
from drakkar.recorder.election import BUILTIN_ELECTIONS
from drakkar.schedule import ARCHIVE_TASK_NAME
from tests.conftest import kafka_connections, wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection, MemoryRegistry, MemoryStore
from tests.test_sources_validation import KafkaOnlyHandler


def _config(tmp_path, **overrides) -> DrakkarConfig:
    values = {
        'storage': {'dir': str(tmp_path / 'storage')},
        'ui': {'enabled': True, 'port': 8123, 'release': {'enabled': False}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'executor': {'binary_path': '/usr/bin/echo', 'drain_timeout_seconds': 1},
        'sinks': {'kafka': {'output_results': {'topic': 'output-results'}}},
        'sources': {'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
    }
    values.update(overrides)
    values['connections'] = {**kafka_connections(), **values.get('connections', {})}
    return DrakkarConfig(**values)


def _app(tmp_path, handler: object | None = None, **overrides) -> DrakkarApp:
    return DrakkarApp(
        handler=handler if handler is not None else KafkaOnlyHandler(),
        config=_config(tmp_path, **overrides),
        worker_id='w-1',
    )


def _filesystem_on(tmp_path, **autodiscover) -> dict:
    """The sections of a worker with autodiscover on, through the filesystem provider."""
    return {
        'autodiscover': {'enabled': True, 'provider': 'filesystem', **autodiscover},
        'storage': {'dir': str(tmp_path / 'storage'), 'shared_dir': str(tmp_path / 'shared')},
    }


class _HandlerWithClusterTasks(KafkaOnlyHandler):
    @periodic(60, scope='cluster')
    async def zz_last(self) -> None:
        return None

    @periodic(60, scope='cluster')
    async def refresh_rates(self) -> None:
        return None


# --- the keys ---


def test_autodiscover_defaults_to_off():
    config = AutodiscoverConfig()

    assert config.enabled is False
    assert config.provider == ''
    assert config.connection == 'main'


def test_enabled_without_provider_fails_with_the_message():
    with pytest.raises(ValidationError) as raised:
        AutodiscoverConfig(enabled=True)

    assert (
        'autodiscover.enabled is true but autodiscover.provider is empty; '
        'choose one of consul, filesystem, postgres, redis'
    ) in str(raised.value)


@pytest.mark.parametrize('enabled', [True, False], ids=['on', 'off'])
def test_unknown_provider_fails_with_the_message(enabled):
    with pytest.raises(ValidationError) as raised:
        AutodiscoverConfig(enabled=enabled, provider='etcd')

    assert "autodiscover.provider must be one of consul, filesystem, postgres, redis, got 'etcd'" in str(raised.value)


def test_filesystem_needs_shared_dir(tmp_path):
    with pytest.raises(ValidationError) as raised:
        _config(tmp_path, autodiscover={'enabled': True, 'provider': 'filesystem'})

    assert 'autodiscover.provider filesystem needs storage.shared_dir' in str(raised.value)


def test_filesystem_with_autodiscover_off_loads_without_a_shared_dir(tmp_path):
    config = _config(tmp_path, autodiscover={'provider': 'filesystem'})

    assert config.autodiscover.enabled is False


@pytest.mark.parametrize(
    ('provider', 'connection_kind'),
    [('consul', 'consul'), ('postgres', 'postgres'), ('redis', 'redis')],
)
def test_a_provider_with_a_service_needs_its_connection_declared_only_when_on(tmp_path, provider, connection_kind):
    assert _config(tmp_path, autodiscover={'provider': provider}).autodiscover.provider == provider

    with pytest.raises(ValidationError) as raised:
        _config(tmp_path, autodiscover={'enabled': True, 'provider': provider, 'connection': 'locks'})

    assert f"autodiscover.connection 'locks' is not defined under connections.{connection_kind}" in str(raised.value)


def test_cache_exchange_lives_under_cache():
    assert CacheConfig().exchange == CacheExchangeConfig()
    assert 'exchange' in CacheConfig.model_fields


# --- a worker with it off ---


async def test_cluster_task_with_autodiscover_off_fails_startup(tmp_path):
    app = _app(tmp_path, handler=_HandlerWithClusterTasks())

    with pytest.raises(ValueError) as raised:
        app._lifecycle._build_periodic_roster()

    assert str(raised.value) == (
        "periodic task 'refresh_rates' has scope cluster, but autodiscover is off; "
        'set autodiscover.enabled and autodiscover.provider, or give the task scope worker'
    )


async def test_worker_with_autodiscover_off_publishes_no_card_and_runs_no_election(tmp_path, monkeypatch):
    def refuse(_options):
        raise AssertionError('no provider may be made with autodiscover off')

    monkeypatch.setitem(BUILTIN_PROVIDERS, 'filesystem', refuse)
    monkeypatch.setitem(BUILTIN_ELECTIONS, 'filesystem', refuse)
    shared = tmp_path / 'shared'
    app = _app(tmp_path, storage={'dir': str(tmp_path / 'storage'), 'shared_dir': str(shared)})

    with capture_logs() as logs:
        app._lifecycle._build_periodic_roster()
        await app._lifecycle._start_fleet()

    assert app.fleet is None
    assert app._cluster_election is None
    assert not archive_task_runs(app._config)
    assert app._periodic_roster.definitions == ()
    assert not shared.exists()
    assert logs == []


def test_the_archive_runs_only_with_autodiscover_on(tmp_path):
    off = _app(tmp_path, storage={'dir': str(tmp_path / 'storage'), 'shared_dir': str(tmp_path / 'shared')})
    on = _app(tmp_path, **_filesystem_on(tmp_path))

    off._lifecycle._build_periodic_roster()
    on._lifecycle._build_periodic_roster()

    assert not off._periodic_roster.runs_archive
    assert on._periodic_roster.runs_archive
    assert on._periodic_roster.cluster_task_names == [ARCHIVE_TASK_NAME]


# --- one provider, two callers ---


def _counter(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_registry_and_election_share_the_provider_but_keep_their_metric_labels(tmp_path, monkeypatch):
    store = MemoryStore()
    election_store = ElectionStore()
    monkeypatch.setitem(BUILTIN_PROVIDERS, 'filesystem', lambda _options: MemoryRegistry(store))
    monkeypatch.setitem(BUILTIN_ELECTIONS, 'filesystem', lambda _options: MemoryElection(election_store))
    app = _app(tmp_path, **_filesystem_on(tmp_path))
    labels = {'provider': 'filesystem', 'op': 'start', 'outcome': 'ok'}
    registry_before = _counter('drakkar_provider_calls_total', feature='registry', **labels)
    election_before = _counter('drakkar_provider_calls_total', feature='election', **labels)

    app._lifecycle._create_connections()
    app._lifecycle._build_periodic_roster()
    await app._lifecycle._start_fleet()
    await wait_for(lambda: _counter('drakkar_provider_calls_total', feature='registry', **labels) > registry_before)
    result = await app._cluster_election.ensure_started(5.0)
    await app._fleet.stop()

    assert result.ok
    assert app.fleet.provider_name == app._cluster_election.provider_name == 'filesystem'
    assert _counter('drakkar_provider_calls_total', feature='election', **labels) == election_before + 1
    assert app.fleet._caller is not app._cluster_election.caller
    assert app.fleet._caller._breaker is not app._cluster_election.caller._breaker


async def test_both_callers_take_their_settings_from_the_one_block(tmp_path):
    app = _app(tmp_path, **_filesystem_on(tmp_path, resilience={'timeout': '1500ms', 'max_retries': 0}))

    app._lifecycle._create_connections()
    election = app._lifecycle._build_cluster_election([ARCHIVE_TASK_NAME])
    registry = FleetRegistry(
        BUILTIN_PROVIDERS['filesystem']({'dir': str(tmp_path / 'shared' / 'fleet'), 'prune_after_seconds': 86_400}),
        provider_name='filesystem',
        config=app._config.autodiscover,
        identity=app.worker_identity,
    )

    assert election.caller.operation_bound_seconds == 1.5
    assert registry._caller.operation_bound_seconds == 1.5


async def test_the_cards_and_the_locks_go_under_the_shared_folder(tmp_path):
    app = _app(tmp_path, cluster_name='search', **_filesystem_on(tmp_path))

    app._lifecycle._create_connections()
    await app._lifecycle._start_fleet()
    await wait_for((tmp_path / 'shared' / 'fleet' / 'workers' / 'w-1.json').exists)
    await app._fleet.stop()
    election = app._lifecycle._build_cluster_election([ARCHIVE_TASK_NAME])

    assert election._context.shared_dir == str(tmp_path / 'shared')


# --- what a handler can change at startup ---


@pytest.mark.parametrize(
    ('changes', 'message'),
    [
        ({'provider': 'etcd'}, "autodiscover.provider must be one of consul, filesystem, postgres, redis, got 'etcd'"),
        (
            {'enabled': True},
            'autodiscover.enabled is true but autodiscover.provider is empty; '
            'choose one of consul, filesystem, postgres, redis',
        ),
        ({'enabled': True, 'provider': 'filesystem'}, 'autodiscover.provider filesystem needs storage.shared_dir'),
        (
            {'enabled': True, 'provider': 'redis'},
            "autodiscover.connection 'main' is not defined under connections.redis",
        ),
    ],
    ids=['unknown', 'empty', 'no-folder', 'no-connection'],
)
def test_a_hook_change_gets_the_message_of_the_load(tmp_path, changes, message):
    app = _app(tmp_path)
    for key, value in changes.items():
        setattr(app._config.autodiscover, key, value)

    with pytest.raises(ValueError) as raised:
        app._lifecycle._build_periodic_roster()

    assert str(raised.value) == message


# --- the cache exchange ---


def _cache_engine(tmp_path, *, peer_views=None, exchange: bool = True) -> CacheEngine:
    return CacheEngine(
        config=CacheConfig(enabled=True, exchange=CacheExchangeConfig(enabled=exchange)),
        storage_dir=str(tmp_path),
        worker_id='w-1',
        cluster_name='search',
        recorder=None,
        peer_views=peer_views,
    )


async def test_cache_exchange_with_autodiscover_off_logs_once_and_pulls_nothing(tmp_path):
    app = _app(
        tmp_path,
        cache={'enabled': True, 'exchange': {'interval_seconds': 0.01}},
    )

    with capture_logs() as logs:
        await app._lifecycle._start_cache()
        engine = app._cache_engine
        await asyncio.sleep(0.05)
        await engine._sync_once()
        await engine.stop()

    lines = [entry for entry in logs if entry['event'] == 'cache_exchange_off_no_autodiscover']
    assert len(lines) == 1
    assert lines[0]['category'] == 'cache'
    assert lines[0]['log_level'] == 'info'
    assert engine._sync_task is None
    assert engine.serves_changes is False


async def test_a_cache_exchange_switched_off_says_nothing_about_autodiscover(tmp_path):
    engine = _cache_engine(tmp_path, exchange=False)

    with capture_logs() as logs:
        await engine.start()
        await engine.stop()

    assert 'cache_exchange_off_no_autodiscover' not in [entry['event'] for entry in logs]


# --- the cluster name ---


@pytest.mark.parametrize(
    ('autodiscover_on', 'cluster_name', 'warns'),
    [(True, '', True), (True, 'search', False), (False, '', False)],
    ids=['on-empty', 'on-named', 'off-empty'],
)
def test_empty_cluster_name_warns_only_with_autodiscover_on(tmp_path, autodiscover_on, cluster_name, warns):
    sections = _filesystem_on(tmp_path) if autodiscover_on else {}
    app = _app(tmp_path, cluster_name=cluster_name, **sections)

    with capture_logs() as logs:
        app._lifecycle._build_periodic_roster()

    lines = [entry for entry in logs if entry['event'] == 'cluster_name_empty']
    if not warns:
        assert lines == []
        return
    assert len(lines) == 1
    assert lines[0]['log_level'] == 'warning'
    assert lines[0]['category'] == 'lifecycle'
    assert lines[0]['scope'] == 'default'
