"""The storage, recorder archive and autodiscover config sections."""

import re

import pytest

from drakkar.config import (
    CONNECTION_PROVIDERS,
    DEFAULT_ARCHIVE_RETENTION_DAYS,
    AutodiscoverConfig,
    CacheExchangeConfig,
    ConnectionsConfig,
    DrakkarConfig,
    RecorderArchiveConfig,
    RecorderConfig,
    StorageConfig,
    load_config,
)
from tests.conftest import kafka_connections


def _config(**sections) -> DrakkarConfig:
    """A loadable config: one enabled source, bundle fetching off, plus ``sections``."""
    sections.setdefault('sources', {'kafka': {'enabled': True}})
    sections['connections'] = {**kafka_connections(), **sections.get('connections', {})}
    ui = sections.setdefault('ui', {})
    ui.setdefault('release', {'enabled': False})
    return DrakkarConfig(**sections)


# --- defaults ------------------------------------------------------------


def test_storage_defaults():
    assert StorageConfig().dir == '/tmp/drakkar'
    assert StorageConfig().shared_dir == ''


def test_archive_defaults():
    archive = RecorderArchiveConfig()

    assert archive.enabled is True
    assert archive.window == 24
    assert archive.due_after == 48
    assert archive.retention == 30
    assert archive.merged_retention == 32
    assert archive.interval is None
    assert archive.cron is None
    assert archive.lease == 60
    assert archive.sweep_after == 7200


def test_autodiscover_defaults():
    autodiscover = AutodiscoverConfig()

    assert autodiscover.enabled is False
    assert autodiscover.provider == ''
    assert autodiscover.connection == 'main'
    assert autodiscover.heartbeat == 10
    assert autodiscover.refresh == 15
    assert autodiscover.offline_after == 60
    assert autodiscover.prune_after == 86400
    assert autodiscover.resilience.timeout == 5.0
    assert autodiscover.resilience.max_retries == 2


def test_autodiscover_fields_are_the_switch_the_provider_choice_the_timings_and_the_resilience_block():
    assert set(AutodiscoverConfig.model_fields) == {
        'enabled',
        'provider',
        'connection',
        'heartbeat',
        'refresh',
        'offline_after',
        'prune_after',
        'resilience',
    }


def test_offline_after_of_30s_loads_with_the_default_heartbeat():
    assert AutodiscoverConfig(offline_after='30s').heartbeat == 10


def test_offline_after_of_zero_fails_at_the_field():
    with pytest.raises(ValueError, match='offline_after'):
        AutodiscoverConfig(offline_after=0)


def test_cache_exchange_defaults():
    exchange = CacheExchangeConfig()

    assert exchange.enabled is True
    assert exchange.interval == 30.0
    assert exchange.page_size == 500
    assert exchange.timeout == 5.0
    assert exchange.cycle_deadline is None
    assert exchange.max_concurrent_peers == 4


def test_recorder_search_defaults():
    search = RecorderConfig().search

    assert search.max_files == 256
    assert search.timeout == 10.0


# --- derived values ------------------------------------------------------


@pytest.mark.parametrize(
    ('storage_dir', 'shared_dir', 'expected'),
    [
        ('/var/lib/drakkar', '', '/var/lib/drakkar'),
        ('/var/lib/drakkar', '/mnt/fleet', '/mnt/fleet'),
        ('', '/mnt/fleet', '/mnt/fleet'),
        ('', '', ''),
    ],
)
def test_resolved_shared_dir_falls_back_to_storage_dir(storage_dir, shared_dir, expected):
    config = _config(storage={'dir': storage_dir, 'shared_dir': shared_dir}, recorder={'enabled': False})

    assert config.resolved_shared_dir == expected


@pytest.mark.parametrize(
    ('shared_dir', 'expected'),
    [
        ('/mnt/shared', '/mnt/shared/fleet'),
        # The folder is a clean path, the same on every backend.
        ('./data/', 'data/fleet'),
        ('/mnt//shared/', '/mnt/shared/fleet'),
    ],
)
def test_resolved_registry_dir(shared_dir, expected):
    assert _config(storage={'shared_dir': shared_dir}).resolved_registry_dir == expected


_CONSUL_ON = {'enabled': True, 'provider': 'consul'}


# --- connections named by the features --------------------------------------


@pytest.mark.parametrize('provider', ['consul', 'redis', 'postgres'])
def test_autodiscover_loads_when_the_named_connection_is_declared(provider):
    connections = {
        'consul': {'cluster-a': {}},
        'redis': {'cluster-a': {}},
        'postgres': {'cluster-a': {'dsn': 'postgresql://db.example/drakkar'}},
    }

    config = _config(
        autodiscover={'enabled': True, 'provider': provider, 'connection': 'cluster-a'},
        connections=connections,
    )

    assert config.autodiscover.connection == 'cluster-a'


def test_the_filesystem_provider_ignores_the_connection_name():
    config = _config(
        autodiscover={'enabled': True, 'provider': 'filesystem', 'connection': 'not-declared'},
        storage={'shared_dir': '/mnt/shared'},
    )

    assert config.autodiscover.connection == 'not-declared'


def test_every_provider_with_a_connection_has_a_connections_kind_of_its_name():
    # The cross-check reads connections.<provider> by the provider's name.
    assert set(CONNECTION_PROVIDERS) <= set(ConnectionsConfig.model_fields)


def test_autodiscover_names_a_connection_of_its_own_kind_only():
    with pytest.raises(
        ValueError, match=re.escape("autodiscover.connection 'main' is not defined under connections.consul")
    ):
        _config(autodiscover=_CONSUL_ON, connections={'redis': {'main': {}}})


def _consul_autodiscover(**autodiscover) -> DrakkarConfig:
    """A config with autodiscover on through the consul provider, with ``autodiscover`` merged over it."""
    return _config(
        autodiscover={**_CONSUL_ON, **autodiscover},
        connections={'consul': {'main': {}}},
    )


def test_consul_offline_after_at_the_cap_loads():
    config = _consul_autodiscover(offline_after='24h')

    assert config.autodiscover.offline_after == 86_400


def test_consul_offline_after_over_the_cap_fails_the_load():
    with pytest.raises(ValueError) as excinfo:
        _consul_autodiscover(offline_after='86401s', prune_after='86401s')

    assert 'autodiscover.offline_after (1d1s) must be <= 1d when autodiscover.provider is consul' in str(excinfo.value)


@pytest.mark.parametrize('provider', ['filesystem', 'redis', 'postgres'])
def test_offline_after_over_the_consul_cap_loads_for_other_providers(provider):
    config = _config(
        autodiscover={'enabled': True, 'provider': provider, 'offline_after': '86401s', 'prune_after': '86401s'},
        storage={'shared_dir': '/mnt/shared'},
        connections={'redis': {'main': {}}, 'postgres': {'main': {'dsn': 'postgresql://db.example/drakkar'}}},
    )

    assert config.autodiscover.offline_after == 86_401


def test_consul_autodiscover_checks_the_prune_limit_before_the_offline_cap():
    # The prune check runs before the consul cap.
    with pytest.raises(ValueError) as excinfo:
        _consul_autodiscover(offline_after='86401s')

    text = str(excinfo.value)
    assert 'autodiscover.prune_after (1d) must be >= autodiscover.offline_after (1d1s)' in text
    assert 'must be <= 1d' not in text


@pytest.mark.parametrize('provider', ['filesystem', 'consul', 'redis', 'postgres'])
def test_the_archive_lease_at_its_upper_bound_loads_for_every_provider(provider):
    config = _config(
        recorder={'archive': {'lease': '24h'}},
        autodiscover={'enabled': True, 'provider': provider},
        storage={'shared_dir': '/mnt/shared'},
        connections={
            'consul': {'main': {}},
            'redis': {'main': {}},
            'postgres': {'main': {'dsn': 'postgresql://db.example/drakkar'}},
        },
    )

    assert config.recorder.archive.lease == 86_400


def test_the_resilience_block_loads_from_env_names(monkeypatch):
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    monkeypatch.setenv('DK_CONNECTIONS__KAFKA__MAIN__BROKERS', 'localhost:9092')
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')
    monkeypatch.setenv('DK_AUTODISCOVER__RESILIENCE__TIMEOUT', '2500ms')
    monkeypatch.setenv('DK_AUTODISCOVER__RESILIENCE__MAX_RETRIES', '0')
    monkeypatch.delenv('DK_CONFIG', raising=False)

    config = load_config(None)

    assert config.autodiscover.resilience.timeout == 2.5
    assert config.autodiscover.resilience.max_retries == 0


# --- env names -----------------------------------------------------------


def test_env_names_load_into_their_sections(monkeypatch):
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    monkeypatch.setenv('DK_CONNECTIONS__KAFKA__MAIN__BROKERS', 'localhost:9092')
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')
    monkeypatch.setenv('DK_STORAGE__DIR', '/var/lib/drakkar')
    monkeypatch.setenv('DK_STORAGE__SHARED_DIR', '/mnt/shared')
    monkeypatch.setenv('DK_RECORDER__ARCHIVE__DUE_AFTER', '72h')
    monkeypatch.setenv('DK_RECORDER__ARCHIVE__MERGED_RETENTION', '0')
    monkeypatch.setenv('DK_AUTODISCOVER__ENABLED', 'true')
    monkeypatch.setenv('DK_AUTODISCOVER__PROVIDER', 'filesystem')
    monkeypatch.setenv('DK_CACHE__EXCHANGE__INTERVAL', '12.5s')
    monkeypatch.setenv('DK_RECORDER__SEARCH__MAX_FILES', '64')
    monkeypatch.delenv('DK_CONFIG', raising=False)

    config = load_config(None)

    assert config.storage.dir == '/var/lib/drakkar'
    assert config.storage.shared_dir == '/mnt/shared'
    assert config.recorder.archive.due_after == 72
    assert config.recorder.archive.merged_retention == 0
    assert config.autodiscover.enabled is True
    assert config.autodiscover.provider == 'filesystem'
    assert config.cache.exchange.interval == 12.5
    assert config.recorder.search.max_files == 64


# --- archive retention against the due delay -------------------------------


def test_archive_retention_default_bounds_disk_growth():
    """The default must expire archives, because nothing else reclaims them.

    Rotation, the flush buffer and the live database are all bounded on
    their own. Archives are not: with retention at 0 a worker left on its
    defaults grows until the volume fills. The default is therefore a
    horizon, and 0 is an explicit opt-out.
    """
    assert DEFAULT_ARCHIVE_RETENTION_DAYS > 0
    assert RecorderArchiveConfig().retention == DEFAULT_ARCHIVE_RETENTION_DAYS


def test_archive_retention_default_covers_the_default_due_delay_and_window():
    """The shipped defaults must validate against each other."""
    archive = RecorderArchiveConfig()
    assert archive.due_after + archive.window <= DEFAULT_ARCHIVE_RETENTION_DAYS * 24


def test_archive_retention_covering_exactly_due_plus_window_is_accepted():
    archive = RecorderArchiveConfig(retention='3d', due_after='48h', window='24h')
    assert archive.retention == 3


def test_archive_retention_zero_is_accepted_for_any_window():
    assert RecorderArchiveConfig(retention=0, window='168h', due_after='500h').retention == 0


def test_archive_retention_explicit_value_message_has_no_default_note():
    with pytest.raises(ValueError) as excinfo:
        RecorderArchiveConfig(retention='2d')
    assert 'default' not in str(excinfo.value)
