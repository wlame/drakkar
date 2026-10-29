"""The storage, shared, history and fleet config sections."""

import re

import pytest

from drakkar.config import (
    CONNECTION_PROVIDERS,
    DEFAULT_ARCHIVE_RETENTION_DAYS,
    CacheExchangeConfig,
    ConnectionsConfig,
    DrakkarConfig,
    FleetRegistryConfig,
    HistoryArchiveConfig,
    HistoryConfig,
    PeriodicConfig,
    SharedConfig,
    StorageConfig,
    UITraceConfig,
    load_config,
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


def test_fleet_registry_fields_are_the_provider_choice_the_timings_and_the_resilience_block():
    assert set(FleetRegistryConfig.model_fields) == {
        'provider',
        'connection',
        'heartbeat_seconds',
        'refresh_seconds',
        'offline_after_seconds',
        'prune_after_seconds',
        'resilience',
    }


def test_offline_after_seconds_of_30_loads_with_the_default_heartbeat():
    assert FleetRegistryConfig(offline_after_seconds=30).heartbeat_seconds == 10


def test_the_registry_time_limit_lives_in_its_resilience_block():
    assert 'timeout_seconds' not in FleetRegistryConfig.model_fields
    assert 'timeout_seconds' in FleetRegistryConfig.model_fields['resilience'].annotation.model_fields


def test_offline_after_seconds_of_zero_fails_at_the_field():
    with pytest.raises(ValueError, match='offline_after_seconds'):
        FleetRegistryConfig(offline_after_seconds=0)


def test_election_defaults():
    election = PeriodicConfig().election

    assert (election.provider, election.connection, election.resilience.retries) == ('filesystem', 'main', 2)


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
    ('storage_dir', 'expected'),
    [
        ('/var/lib/drakkar', '/var/lib/drakkar/fleet'),
        # The folder is a clean path, the same on every backend.
        ('./data/', 'data/fleet'),
        ('/var/lib//drakkar/', '/var/lib/drakkar/fleet'),
        ('', ''),
    ],
)
def test_resolved_registry_dir(storage_dir, expected):
    config = _config(storage={'dir': storage_dir})

    assert config.resolved_registry_dir == expected


@pytest.mark.parametrize(
    ('sections', 'expected'),
    [
        ({}, 'filesystem'),
        ({'ui': {'enabled': False}}, 'off'),
        ({'storage': {'dir': ''}}, 'off'),
        ({'fleet': {'registry': {'provider': 'consul'}}, 'connections': {'consul': {'main': {}}}}, 'consul'),
        (
            {
                'ui': {'enabled': False},
                'fleet': {'registry': {'provider': 'redis'}},
                'connections': {'redis': {'main': {}}},
            },
            'off',
        ),
    ],
)
def test_fleet_summary(sections, expected):
    assert _config(**sections).fleet_summary == expected


@pytest.mark.parametrize(
    ('provider', 'expected'),
    [('filesystem', ''), ('consul', ' election=consul')],
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


def test_the_filesystem_provider_ignores_the_connection_name():
    config = _config(fleet={'registry': {'provider': 'filesystem', 'connection': 'not-declared'}})

    assert config.fleet.registry.connection == 'not-declared'


def test_every_provider_with_a_connection_has_a_connections_kind_of_its_name():
    # The cross-check reads connections.<provider> by the provider's name.
    assert set(CONNECTION_PROVIDERS) == set(ConnectionsConfig.model_fields)


def test_registry_names_a_connection_of_its_own_kind_only():
    with pytest.raises(
        ValueError, match=re.escape("fleet.registry.connection 'main' is not defined under connections.consul")
    ):
        _config(fleet={'registry': {'provider': 'consul'}}, connections={'redis': {'main': {}}})


def _consul_registry(**registry) -> DrakkarConfig:
    """A config whose registry is the consul provider, with ``registry`` merged over it."""
    return _config(
        fleet={'registry': {'provider': 'consul', **registry}},
        connections={'consul': {'main': {}}},
    )


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


@pytest.mark.parametrize('provider', ['filesystem', 'redis', 'postgres'])
def test_offline_after_seconds_over_the_consul_cap_loads_for_other_providers(provider):
    config = _config(
        fleet={'registry': {'provider': provider, 'offline_after_seconds': 86_401, 'prune_after_seconds': 86_401}},
        connections={'redis': {'main': {}}, 'postgres': {'main': {'dsn': 'postgresql://db.example/drakkar'}}},
    )

    assert config.fleet.registry.offline_after_seconds == 86_401


def test_consul_registry_checks_the_prune_limit_before_the_offline_cap():
    # The prune check runs before the consul cap.
    with pytest.raises(ValueError) as excinfo:
        _consul_registry(offline_after_seconds=86_401)

    text = str(excinfo.value)
    assert 'fleet.registry.prune_after_seconds (86400) must be >= fleet.registry.offline_after_seconds (86401)' in text
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
    monkeypatch.setenv('DK_FLEET__REGISTRY__PROVIDER', 'filesystem')
    monkeypatch.setenv('DK_FLEET__CACHE_EXCHANGE__INTERVAL_SECONDS', '12.5')
    monkeypatch.setenv('DK_UI__TRACE__HISTORY_MAX_FILES', '64')
    monkeypatch.delenv('DK_CONFIG', raising=False)

    config = load_config(None)

    assert config.storage.dir == '/var/lib/drakkar'
    assert config.shared.dir == '/mnt/fleet'
    assert config.history.archive.due_after_hours == 72
    assert config.history.merged_retention_days == 0
    assert config.fleet.registry.provider == 'filesystem'
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
