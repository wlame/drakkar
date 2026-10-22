"""The storage, shared, history and fleet config sections and the moved-key table."""

import re

import pytest

from drakkar.config import (
    CONNECTION_PROVIDERS,
    DEFAULT_ARCHIVE_RETENTION_DAYS,
    CacheExchangeConfig,
    ConnectionsConfig,
    ConsulRegistryConfig,
    DrakkarConfig,
    FleetRegistryConfig,
    HistoryArchiveConfig,
    HistoryConfig,
    PeriodicConfig,
    SharedConfig,
    StaticPeerConfig,
    StorageConfig,
    UITraceConfig,
    load_config,
)
from drakkar.config.moved_keys import (
    MOVED_CONFIG_KEYS,
    MOVED_KEYS_HEADER,
    find_moved_keys,
    moved_keys_message,
)


def _config(**sections) -> DrakkarConfig:
    """A loadable config: one enabled source, bundle fetching off, plus ``sections``."""
    sections.setdefault('sources', {'kafka': {'enabled': True}})
    ui = sections.setdefault('ui', {})
    ui.setdefault('release', {'enabled': False})
    return DrakkarConfig(**sections)


# --- defaults ------------------------------------------------------------


def test_storage_and_shared_defaults():
    assert StorageConfig().dir == '/tmp/drakkar'
    assert SharedConfig().dir == ''


def test_history_defaults():
    history = HistoryConfig()

    assert history.archive.enabled is True
    assert history.archive.window_hours == 24
    assert history.archive.due_after_hours == 48
    assert history.archive.retention_days == 30
    assert history.archive.interval is None
    assert history.archive.cron is None
    assert history.archive.lease_seconds == 60
    assert history.archive.sweep_after_seconds == 7200
    assert history.merged_retention_days == 32


def test_fleet_registry_defaults():
    registry = FleetRegistryConfig()

    assert registry.provider == 'filesystem'
    assert registry.heartbeat_seconds == 10
    assert registry.refresh_seconds == 15
    assert registry.connection == 'main'
    assert registry.offline_after_seconds == 60
    assert registry.prune_after_seconds == 86400
    assert registry.resilience.timeout_seconds == 5.0
    assert registry.filesystem.dir == ''
    assert registry.static.peers == []
    assert registry.consul.mode == 'publish'
    assert registry.consul.service_name == 'drakkar'
    assert registry.plugin_options == {}


def test_the_old_offline_default_still_loads_with_the_default_heartbeat():
    assert FleetRegistryConfig(offline_after_seconds=30).heartbeat_seconds == 10


def test_timeout_seconds_is_no_longer_a_registry_field():
    assert 'timeout_seconds' not in FleetRegistryConfig.model_fields


def test_offline_after_seconds_of_zero_fails_at_the_field():
    with pytest.raises(ValueError, match='offline_after_seconds'):
        FleetRegistryConfig(offline_after_seconds=0)


def test_consul_registry_service_name_must_not_be_empty():
    with pytest.raises(ValueError, match='service_name'):
        ConsulRegistryConfig(service_name='')


def test_election_defaults():
    election = PeriodicConfig().election

    assert (election.provider, election.connection, election.resilience.retries, election.plugin_options) == (
        'filesystem',
        'main',
        2,
        {},
    )


def test_cache_exchange_defaults():
    exchange = CacheExchangeConfig()

    assert exchange.enabled is True
    assert exchange.interval_seconds == 30.0
    assert exchange.page_size == 500
    assert exchange.timeout_seconds == 5.0
    assert exchange.cycle_deadline_seconds is None
    assert exchange.max_concurrent_peers == 4


def test_ui_trace_defaults():
    trace = UITraceConfig()

    assert trace.history_max_files == 256
    assert trace.history_max_seconds == 10.0


def test_static_peer_cluster_defaults_to_empty():
    peer = StaticPeerConfig(name='peer-a', url='http://10.0.0.5:8080/')

    assert peer.cluster == ''


def test_plugin_options_accepts_any_mapping():
    registry = FleetRegistryConfig(provider='consul', plugin_options={'address': 'consul:8500', 'tags': ['a']})

    assert registry.plugin_options == {'address': 'consul:8500', 'tags': ['a']}


# --- moved-key table -----------------------------------------------------


def test_find_moved_keys_returns_table_order_whatever_the_tree_order():
    tree = {'cache': {'db_dir': '/c', 'peer_sync': {'batch_size': 1}}, 'ui': {'recorder': {'db_dir': '/d'}}}

    found = [key.path for key in find_moved_keys(tree)]

    assert found == ['ui.recorder.db_dir', 'cache.peer_sync.batch_size', 'cache.db_dir']


def test_find_moved_keys_reports_a_key_whose_value_is_none():
    assert [key.path for key in find_moved_keys({'ui': {'workers_offline_after_seconds': None}})] == [
        'ui.workers_offline_after_seconds'
    ]


def test_find_moved_keys_ignores_a_parent_that_is_not_a_mapping():
    assert find_moved_keys({'cache': 'not-a-section', 'ui': None}) == []


def test_find_moved_keys_ignores_the_new_keys():
    tree = {'storage': {'dir': '/s'}, 'fleet': {'cache_exchange': {'page_size': 10}}, 'cache': {'enabled': True}}

    assert find_moved_keys(tree) == []


def test_moved_keys_message_is_the_header_then_one_indented_line_per_key():
    found = [MOVED_CONFIG_KEYS[0], MOVED_CONFIG_KEYS[-1]]

    assert moved_keys_message(found) == '\n'.join(
        [MOVED_KEYS_HEADER, f'  {MOVED_CONFIG_KEYS[0].line}', f'  {MOVED_CONFIG_KEYS[-1].line}']
    )


def test_every_moved_key_line_starts_with_its_path():
    for key in MOVED_CONFIG_KEYS:
        assert key.line.startswith(f'{key.path} -> ') or key.line.startswith(f'{key.path}: removed, ')


def test_direct_construction_rejects_a_moved_key():
    with pytest.raises(ValueError, match=re.escape('ui.recorder.db_dir -> storage.dir')):
        _config(ui={'recorder': {'db_dir': '/data'}})


def test_the_archive_keys_are_the_last_moved_keys():
    assert [key.path for key in MOVED_CONFIG_KEYS[-4:]] == [
        'fleet.registry.timeout_seconds',
        'history.archive.election',
        'history.archive.lock_stale_seconds',
        'history.archive.pass_interval_seconds',
    ]


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
    config = _config(storage={'dir': storage_dir}, shared={'dir': shared_dir})

    assert config.resolved_shared_dir == expected


@pytest.mark.parametrize(
    ('storage_dir', 'registry_dir', 'expected'),
    [
        ('/var/lib/drakkar', '', '/var/lib/drakkar/fleet'),
        # The derived folder is a clean path, the same on every backend.
        ('./data/', '', 'data/fleet'),
        ('/var/lib//drakkar/', '', '/var/lib/drakkar/fleet'),
        ('/var/lib/drakkar', '/mnt/registry', '/mnt/registry'),
        ('', '/mnt/registry', '/mnt/registry'),
        ('', '', ''),
    ],
)
def test_resolved_registry_dir(storage_dir, registry_dir, expected):
    config = _config(storage={'dir': storage_dir}, fleet={'registry': {'filesystem': {'dir': registry_dir}}})

    assert config.resolved_registry_dir == expected


@pytest.mark.parametrize(
    ('sections', 'expected'),
    [
        ({}, 'filesystem'),
        ({'ui': {'enabled': False}}, 'off'),
        ({'storage': {'dir': ''}}, 'off'),
        ({'storage': {'dir': ''}, 'fleet': {'registry': {'provider': 'static'}}}, 'static'),
        ({'fleet': {'registry': {'provider': 'none'}}}, 'none'),
        ({'fleet': {'registry': {'provider': 'consul'}}, 'connections': {'consul': {'main': {}}}}, 'consul'),
        ({'ui': {'enabled': False}, 'fleet': {'registry': {'provider': 'static'}}}, 'off'),
    ],
)
def test_fleet_summary(sections, expected):
    assert _config(**sections).fleet_summary == expected


@pytest.mark.parametrize(
    ('provider', 'expected'),
    [('filesystem', ''), ('consul', ' election=consul'), ('my-plugin', ' election=my-plugin')],
)
def test_election_summary_is_silent_for_the_filesystem_provider(provider, expected):
    config = _config(
        periodic={'election': {'provider': provider}},
        connections={'consul': {'main': {}}},
    )

    assert config.election_summary == expected


# --- connections named by the features --------------------------------------


@pytest.mark.parametrize('provider', ['consul', 'redis', 'postgres'])
def test_registry_and_election_load_when_the_named_connection_is_declared(provider):
    connections = {
        'consul': {'cluster-a': {}},
        'redis': {'cluster-a': {}},
        'postgres': {'cluster-a': {'dsn': 'postgresql://db.example/drakkar'}},
    }

    config = _config(
        fleet={'registry': {'provider': provider, 'connection': 'cluster-a'}},
        periodic={'election': {'provider': provider, 'connection': 'cluster-a'}},
        connections=connections,
    )

    assert config.fleet.registry.connection == 'cluster-a'


@pytest.mark.parametrize('provider', ['filesystem', 'static', 'none', 'my-plugin'])
def test_providers_that_use_no_connection_ignore_the_connection_name(provider):
    config = _config(fleet={'registry': {'provider': provider, 'connection': 'not-declared'}})

    assert config.fleet.registry.connection == 'not-declared'


def test_election_plugin_provider_ignores_the_connection_name():
    config = _config(periodic={'election': {'provider': 'my-plugin', 'connection': 'not-declared'}})

    assert config.periodic.election.connection == 'not-declared'


def test_every_provider_with_a_connection_has_a_connections_kind_of_its_name():
    # The cross-check reads connections.<provider> by the provider's name.
    assert set(CONNECTION_PROVIDERS) == set(ConnectionsConfig.model_fields)


def test_registry_names_a_connection_of_its_own_kind_only():
    with pytest.raises(
        ValueError, match=re.escape("fleet.registry.connection 'main' is not defined under connections.consul")
    ):
        _config(fleet={'registry': {'provider': 'consul'}}, connections={'redis': {'main': {}}})


CATALOG_MODE_MESSAGE = (
    'fleet.registry.consul.mode catalog is reserved for a later release; this release ships publish only'
)


def _consul_registry(**registry) -> DrakkarConfig:
    """A config whose registry is the consul provider, with ``registry`` merged over it."""
    return _config(
        fleet={'registry': {'provider': 'consul', **registry}},
        connections={'consul': {'main': {}}},
    )


def test_consul_registry_in_catalog_mode_fails_the_load():
    with pytest.raises(ValueError) as excinfo:
        _consul_registry(consul={'mode': 'catalog'})

    assert CATALOG_MODE_MESSAGE in str(excinfo.value)


def test_consul_registry_in_publish_mode_loads():
    config = _consul_registry(consul={'mode': 'publish'})

    assert config.fleet.registry.consul.mode == 'publish'


@pytest.mark.parametrize('provider', ['filesystem', 'static', 'none', 'redis', 'postgres'])
def test_catalog_mode_loads_when_the_provider_is_not_consul(provider):
    config = _config(
        fleet={'registry': {'provider': provider, 'consul': {'mode': 'catalog'}}},
        connections={'redis': {'main': {}}, 'postgres': {'main': {'dsn': 'postgresql://db.example/drakkar'}}},
    )

    assert config.fleet.registry.consul.mode == 'catalog'


def test_consul_offline_after_seconds_at_the_cap_loads():
    config = _consul_registry(offline_after_seconds=86_400)

    assert config.fleet.registry.offline_after_seconds == 86_400


def test_consul_offline_after_seconds_over_the_cap_fails_the_load():
    with pytest.raises(ValueError) as excinfo:
        _consul_registry(offline_after_seconds=86_401, prune_after_seconds=86_401)

    assert (
        'fleet.registry.offline_after_seconds (86401) must be <= 86400 when fleet.registry.provider is consul'
        in str(excinfo.value)
    )


@pytest.mark.parametrize('provider', ['filesystem', 'static', 'none', 'redis', 'postgres'])
def test_offline_after_seconds_over_the_consul_cap_loads_for_other_providers(provider):
    config = _config(
        fleet={'registry': {'provider': provider, 'offline_after_seconds': 86_401, 'prune_after_seconds': 86_401}},
        connections={'redis': {'main': {}}, 'postgres': {'main': {'dsn': 'postgresql://db.example/drakkar'}}},
    )

    assert config.fleet.registry.offline_after_seconds == 86_401


def test_consul_registry_checks_the_prune_limit_before_the_offline_cap():
    # The prune check is the older timing check and keeps its place ahead of the consul cap.
    with pytest.raises(ValueError) as excinfo:
        _consul_registry(offline_after_seconds=86_401)

    text = str(excinfo.value)
    assert 'fleet.registry.prune_after_seconds (86400) must be >= fleet.registry.offline_after_seconds (86401)' in text
    assert 'must be <= 86400' not in text


def test_consul_registry_in_catalog_mode_reports_the_mode_before_the_offline_cap():
    with pytest.raises(ValueError) as excinfo:
        _consul_registry(offline_after_seconds=86_401, prune_after_seconds=86_401, consul={'mode': 'catalog'})

    text = str(excinfo.value)
    assert CATALOG_MODE_MESSAGE in text
    assert 'must be <= 86400' not in text


@pytest.mark.parametrize('provider', ['filesystem', 'consul', 'redis', 'postgres'])
def test_the_archive_lease_at_its_upper_bound_loads_for_every_provider(provider):
    config = _config(
        history={'archive': {'lease_seconds': 86_400}},
        periodic={'election': {'provider': provider}},
        connections={
            'consul': {'main': {}},
            'redis': {'main': {}},
            'postgres': {'main': {'dsn': 'postgresql://db.example/drakkar'}},
        },
    )

    assert config.history.archive.lease_seconds == 86_400


def test_resilience_blocks_load_from_env_names(monkeypatch):
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')
    monkeypatch.setenv('DK_FLEET__REGISTRY__RESILIENCE__TIMEOUT_SECONDS', '2.5')
    monkeypatch.setenv('DK_PERIODIC__ELECTION__RESILIENCE__RETRIES', '0')
    monkeypatch.delenv('DK_CONFIG', raising=False)

    config = load_config(None)

    assert config.fleet.registry.resilience.timeout_seconds == 2.5
    assert config.periodic.election.resilience.retries == 0


# --- env names -----------------------------------------------------------


def test_env_names_load_into_the_new_sections(monkeypatch):
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')
    monkeypatch.setenv('DK_STORAGE__DIR', '/var/lib/drakkar')
    monkeypatch.setenv('DK_SHARED__DIR', '/mnt/fleet')
    monkeypatch.setenv('DK_HISTORY__ARCHIVE__DUE_AFTER_HOURS', '72')
    monkeypatch.setenv('DK_HISTORY__MERGED_RETENTION_DAYS', '0')
    monkeypatch.setenv('DK_FLEET__REGISTRY__PROVIDER', 'static')
    monkeypatch.setenv('DK_FLEET__CACHE_EXCHANGE__INTERVAL_SECONDS', '12.5')
    monkeypatch.setenv('DK_UI__TRACE__HISTORY_MAX_FILES', '64')
    monkeypatch.delenv('DK_CONFIG', raising=False)

    config = load_config(None)

    assert config.storage.dir == '/var/lib/drakkar'
    assert config.shared.dir == '/mnt/fleet'
    assert config.history.archive.due_after_hours == 72
    assert config.history.merged_retention_days == 0
    assert config.fleet.registry.provider == 'static'
    assert config.fleet.cache_exchange.interval_seconds == 12.5
    assert config.ui.trace.history_max_files == 64


# --- archive retention against the due delay -------------------------------


def test_archive_retention_default_bounds_disk_growth():
    """The default must expire archives, because nothing else reclaims them.

    Rotation, the flush buffer and the live database are all bounded on
    their own. Archives are not: with retention at 0 a worker left on its
    defaults grows until the volume fills. The default is therefore a
    horizon, and 0 is an explicit opt-out.
    """
    assert DEFAULT_ARCHIVE_RETENTION_DAYS > 0
    assert HistoryArchiveConfig().retention_days == DEFAULT_ARCHIVE_RETENTION_DAYS


def test_archive_retention_default_covers_the_default_due_delay_and_window():
    """The shipped defaults must validate against each other."""
    archive = HistoryArchiveConfig()
    assert archive.due_after_hours + archive.window_hours <= DEFAULT_ARCHIVE_RETENTION_DAYS * 24


def test_archive_retention_covering_exactly_due_plus_window_is_accepted():
    archive = HistoryArchiveConfig(retention_days=3, due_after_hours=48, window_hours=24)
    assert archive.retention_days == 3


def test_archive_retention_zero_is_accepted_for_any_window():
    assert HistoryArchiveConfig(retention_days=0, window_hours=168, due_after_hours=500).retention_days == 0


def test_archive_retention_explicit_value_message_has_no_default_note():
    with pytest.raises(ValueError) as excinfo:
        HistoryArchiveConfig(retention_days=2)
    assert 'default' not in str(excinfo.value)
