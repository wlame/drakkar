"""The ``connections`` section: defaults, env-created entries, shape rules, secret flags."""

import os
import re
import traceback
from collections.abc import Callable
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml
from pydantic import ValidationError

from drakkar.app import DrakkarApp
from drakkar.config import (
    ConnectionsConfig,
    ConsulConnectionConfig,
    DrakkarConfig,
    KafkaConnectionConfig,
    KafkaSinkConfig,
    PostgresConnectionConfig,
    RedisConnectionConfig,
    deep_merge,
    load_config,
)
from drakkar.configmeta import walk_model_fields
from drakkar.handler import BaseDrakkarHandler
from drakkar.providers.connections import Connections
from drakkar.providers.consul import resolve_client as resolve_consul_client
from drakkar.providers.redis import resolve_client as resolve_redis_client

BASE_YAML = {
    'connections': {'kafka': {'main': {}}},
    'sources': {'kafka': {'enabled': True}},
    'ui': {'release': {'enabled': False}},
}

CONSUL_PREFIX_MESSAGE = (
    "connections.consul.{name}.state_prefix '{value}' must be segments of letters, digits, '_', '.' and '-' "
    "joined by '/', with no leading or trailing '/', and no segment may be '.' or '..'"
)
CONSUL_DATACENTER_MESSAGE = (
    "connections.consul.{name}.datacenter '{value}' may only contain letters, digits, '_', '.' and '-'"
)
REDIS_PREFIX_MESSAGE = (
    "connections.redis.{name}.state_prefix '{value}' may only contain letters, digits, '_', ':', '.' and '-'"
)
SCHEMA_MESSAGE = "connections.postgres.{name}.schema '{value}' must be a lower-case identifier ([a-z_][a-z0-9_]*)"


class PlainHandler(BaseDrakkarHandler):
    """The smallest handler a ``DrakkarApp`` accepts."""

    async def arrange(self, messages, pending):
        return []


@pytest.fixture
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """No ``DK_*`` variable set; the returned patcher sets the ones a test needs."""
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    return monkeypatch


@pytest.fixture
def load(tmp_path: Path, clean_environment: pytest.MonkeyPatch) -> Callable[..., DrakkarConfig]:
    """A loader that goes through ``load_config`` like an operator's file, with only the given ``DK_*`` variables set."""

    def _load(yaml_data: dict | None = None, env: dict[str, str] | None = None) -> DrakkarConfig:
        for name, value in (env or {}).items():
            clean_environment.setenv(name, value)
        path = tmp_path / 'drakkar.yaml'
        # Keys keep their order: where a secret sits in the file decides how much of it an error can echo.
        path.write_text(yaml.safe_dump(deep_merge(BASE_YAML, yaml_data or {}), sort_keys=False), encoding='utf-8')
        return load_config(path)

    return _load


def test_a_connection_from_the_environment_alone_loads(load):
    cfg = load(env={'DK_CONNECTIONS__POSTGRES__MAIN__DSN': 'postgresql://u:p@h/db'})

    main = cfg.connections.postgres['main']
    assert main.dsn == 'postgresql://u:p@h/db'
    assert main.schema_name == 'drakkar'
    assert (main.manage_schema, main.pool_min, main.pool_max) == (True, 1, 4)


def test_the_schema_key_loads_from_yaml_and_from_the_environment(load):
    from_yaml = load({'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db', 'schema': 'audit'}}}})
    from_env = load(
        env={
            'DK_CONNECTIONS__POSTGRES__MAIN__DSN': 'postgresql://h/db',
            'DK_CONNECTIONS__POSTGRES__MAIN__SCHEMA': 'audit',
        }
    )

    assert from_yaml.connections.postgres['main'].schema_name == 'audit'
    assert from_env.connections.postgres['main'].schema_name == 'audit'


def test_an_environment_value_overrides_one_field_of_a_yaml_connection(load):
    cfg = load(
        {'connections': {'redis': {'main': {'url': 'redis://yaml-host/1', 'state_prefix': 'app:'}}}},
        env={'DK_CONNECTIONS__REDIS__MAIN__URL': 'redis://env-host/2'},
    )

    main = cfg.connections.redis['main']
    assert (main.url, main.state_prefix) == ('redis://env-host/2', 'app:')


def test_no_connections_are_defined_by_default(load):
    assert ConnectionsConfig() == ConnectionsConfig(kafka={}, postgres={}, redis={}, mongo={}, consul={})


def test_consul_and_redis_entries_take_their_defaults(load):
    cfg = load({'connections': {'consul': {'main': {}}, 'redis': {'main': {}}}})

    consul = cfg.connections.consul['main']
    assert (consul.address, consul.token, consul.datacenter, consul.state_prefix) == (
        'http://127.0.0.1:8500',
        '',
        '',
        'drakkar',
    )
    redis = cfg.connections.redis['main']
    assert (redis.url, redis.state_prefix) == ('redis://localhost:6379/0', 'drakkar:')


def test_a_postgres_connection_without_a_dsn_fails_the_load(load):
    with pytest.raises(ValidationError, match=r'connections\.postgres\.main\.dsn'):
        load({'connections': {'postgres': {'main': {}}}})


@pytest.mark.parametrize(
    'kind, field, value, message',
    [
        ('consul', 'state_prefix', '/drakkar', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', 'drakkar/', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', 'a//b', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', '', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', 'bad prefix', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', '.', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', '..', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', 'drakkar/../other', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', 'a/./b', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', './a', CONSUL_PREFIX_MESSAGE),
        ('consul', 'state_prefix', 'a/..', CONSUL_PREFIX_MESSAGE),
        ('consul', 'datacenter', 'dc 1', CONSUL_DATACENTER_MESSAGE),
        ('consul', 'datacenter', 'dc/1', CONSUL_DATACENTER_MESSAGE),
        ('consul', 'datacenter', 'dc1\n', CONSUL_DATACENTER_MESSAGE),
        ('consul', 'datacenter', 'd\u00e7', CONSUL_DATACENTER_MESSAGE),
        ('redis', 'state_prefix', 'bad prefix', REDIS_PREFIX_MESSAGE),
        ('redis', 'state_prefix', 'a/b', REDIS_PREFIX_MESSAGE),
        ('postgres', 'schema', 'Drakkar', SCHEMA_MESSAGE),
        ('postgres', 'schema', '1abc', SCHEMA_MESSAGE),
        ('postgres', 'schema', 'a-b', SCHEMA_MESSAGE),
        ('postgres', 'schema', '', SCHEMA_MESSAGE),
    ],
)
def test_an_invalid_field_fails_the_load_with_the_contract_message(load, kind, field, value, message):
    entry = {'dsn': 'postgresql://h/db', field: value} if kind == 'postgres' else {field: value}

    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {kind: {'main': entry}}})

    assert message.format(name='main', value=value) in str(excinfo.value)


@pytest.mark.parametrize(
    'kind, field, value',
    [
        ('consul', 'state_prefix', 'drakkar'),
        ('consul', 'state_prefix', 'team-a/drakkar_1.v2'),
        ('consul', 'state_prefix', '..x'),
        ('consul', 'state_prefix', 'a/...'),
        ('consul', 'state_prefix', 'v1.2/x..'),
        ('consul', 'datacenter', ''),
        ('consul', 'datacenter', 'dc1'),
        ('consul', 'datacenter', 'eu-west_1.prod'),
        ('redis', 'state_prefix', ''),
        ('redis', 'state_prefix', 'team-a:drakkar.v2_'),
        ('postgres', 'schema', '_private'),
        ('postgres', 'schema', 'drakkar_2'),
    ],
)
def test_a_valid_field_loads(load, kind, field, value):
    entry = {'dsn': 'postgresql://h/db', field: value} if kind == 'postgres' else {field: value}

    cfg = load({'connections': {kind: {'main': entry}}})

    loaded = getattr(cfg.connections, kind)['main']
    assert getattr(loaded, 'schema_name' if field == 'schema' else field) == value


def test_a_value_with_a_trailing_newline_is_not_a_valid_schema(load):
    with pytest.raises(ValidationError, match='must be a lower-case identifier'):
        load({'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db', 'schema': 'drakkar\n'}}}})


def test_the_message_names_the_connection_that_is_wrong(load):
    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'redis': {'main': {}, 'cache-2': {'state_prefix': 'x y'}}}})

    assert REDIS_PREFIX_MESSAGE.format(name='cache-2', value='x y') in str(excinfo.value)


def test_with_several_wrong_connections_the_first_by_name_is_reported(load):
    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'redis': {'b': {'state_prefix': 'x y'}, 'a': {'state_prefix': 'x y'}}}})

    assert REDIS_PREFIX_MESSAGE.format(name='a', value='x y') in str(excinfo.value)


def test_an_entry_with_a_wrong_prefix_and_a_wrong_datacenter_reports_the_prefix(load):
    entry = {'state_prefix': '/drakkar', 'datacenter': 'dc 1'}

    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'consul': {'main': entry}}})

    text = str(excinfo.value)
    assert CONSUL_PREFIX_MESSAGE.format(name='main', value='/drakkar') in text
    assert 'datacenter' not in text


def test_the_datacenter_of_an_earlier_entry_is_reported_before_the_prefix_of_a_later_one(load):
    connections = {'consul': {'a': {'datacenter': 'dc 1'}, 'b': {'state_prefix': '/drakkar'}}}

    with pytest.raises(ValidationError) as excinfo:
        load({'connections': connections})

    text = str(excinfo.value)
    assert CONSUL_DATACENTER_MESSAGE.format(name='a', value='dc 1') in text
    assert 'state_prefix' not in text


def test_a_datacenter_error_does_not_echo_the_token(load):
    entry = {'token': 'hunter2-token', 'datacenter': 'dc 1'}

    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'consul': {'main': entry}}})

    text = str(excinfo.value)
    assert CONSUL_DATACENTER_MESSAGE.format(name='main', value='dc 1') in text
    assert 'hunter2-token' not in text


def test_pool_max_below_pool_min_names_the_connection(load):
    entry = {'dsn': 'postgresql://h/db', 'pool_min': 2, 'pool_max': 1}

    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'postgres': {'main': entry}}})

    assert 'connections.postgres.main.pool_max (1) must be >= connections.postgres.main.pool_min (2)' in str(
        excinfo.value
    )


def test_pool_max_equal_to_pool_min_is_accepted(load):
    entry = {'dsn': 'postgresql://h/db', 'pool_min': 3, 'pool_max': 3}

    cfg = load({'connections': {'postgres': {'main': entry}}})

    assert (cfg.connections.postgres['main'].pool_min, cfg.connections.postgres['main'].pool_max) == (3, 3)


@pytest.mark.parametrize('field', ['pool_min', 'pool_max'])
def test_a_pool_size_below_one_fails_the_load(load, field):
    with pytest.raises(ValidationError, match=field):
        load({'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db', field: 0}}}})


def test_a_connection_model_takes_only_the_yaml_key():
    assert PostgresConnectionConfig(dsn='postgresql://h/db', schema='audit').schema_name == 'audit'

    with pytest.raises(ValidationError, match='schema_name'):
        PostgresConnectionConfig(dsn='postgresql://h/db', schema_name='audit')


def test_the_secret_fields_carry_the_secret_flag():
    secret_paths = {
        entry.path
        for model, kind in [
            (ConsulConnectionConfig, 'consul'),
            (RedisConnectionConfig, 'redis'),
            (PostgresConnectionConfig, 'postgres'),
        ]
        for entry in walk_model_fields(model, path_prefix=['connections', kind, '*'], group='connections')
        if entry.secret
    }

    assert secret_paths == {'connections.consul.*.token', 'connections.redis.*.url', 'connections.postgres.*.dsn'}


def test_every_connection_field_has_a_description():
    for model in (ConsulConnectionConfig, RedisConnectionConfig, PostgresConnectionConfig):
        for name, info in model.model_fields.items():
            assert info.description, f'{model.__name__}.{name}'


SECRET_DSN = 'postgresql://user:hunter2@host/db'


@pytest.mark.parametrize(
    'yaml_entry, env',
    [
        ({'dsn': SECRET_DSN, 'schema': 'Bad'}, {}),
        ({'schema': 'Bad', 'dsn': SECRET_DSN}, {}),
        ({'schema': 'Bad'}, {'DK_CONNECTIONS__POSTGRES__MAIN__DSN': SECRET_DSN}),
    ],
    ids=['dsn first', 'dsn last', 'dsn from the environment'],
)
def test_a_load_error_does_not_echo_the_dsn(load, yaml_entry, env):
    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'postgres': {'main': yaml_entry}}}, env=env)

    text = str(excinfo.value)
    assert SCHEMA_MESSAGE.format(name='main', value='Bad') in text
    assert 'hunter2' not in text
    assert 'input_value' not in text


def test_the_startup_path_prints_no_dsn_for_an_invalid_connection(tmp_path, clean_environment):
    # DrakkarApp(...) is where an operator's config is loaded; nothing catches the error there,
    # so what Python prints for the escaping exception is what reaches the operator.
    entry = {'schema': 'Bad', 'dsn': SECRET_DSN}
    path = tmp_path / 'drakkar.yaml'
    path.write_text(
        yaml.safe_dump({**BASE_YAML, 'connections': {'postgres': {'main': entry}}}, sort_keys=False), encoding='utf-8'
    )

    with pytest.raises(ValidationError) as excinfo:
        DrakkarApp(handler=PlainHandler(), config_path=path)

    printed = ''.join(traceback.format_exception(excinfo.value))
    assert SCHEMA_MESSAGE.format(name='main', value='Bad') in printed
    assert 'hunter2' not in printed


@pytest.mark.parametrize(
    'sections, message',
    [
        (
            {'sources': {'kafka': {'enabled': True, 'connection': 'other'}}},
            "sources.kafka.connection 'other' is not defined under connections.kafka",
        ),
        (
            {'sinks': {'kafka': {'results': {'connection': 'other', 'topic': 'out'}}}},
            "sinks.kafka.results.connection 'other' is not defined under connections.kafka",
        ),
        (
            {'sinks': {'postgres': {'main-db': {'connection': 'other'}}}},
            "sinks.postgres.main-db.connection 'other' is not defined under connections.postgres",
        ),
        (
            {'sinks': {'mongo': {'docs': {'database': 'drakkar'}}}},
            "sinks.mongo.docs.connection 'main' is not defined under connections.mongo",
        ),
        (
            {'sinks': {'redis': {'cache': {}}}},
            "sinks.redis.cache.connection 'main' is not defined under connections.redis",
        ),
        (
            {'dlq': {'connection': 'other'}},
            "dlq.connection 'other' is not defined under connections.kafka",
        ),
    ],
    ids=['kafka-source', 'kafka-sink', 'postgres-sink', 'mongo-sink', 'redis-sink', 'dlq'],
)
def test_undeclared_connection_fails_with_the_message(sections, message, load):
    with pytest.raises(ValidationError) as excinfo:
        load(sections)

    assert message in str(excinfo.value)


def test_kafka_connection_is_required_with_the_kafka_source_on(clean_environment, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump({'sources': {'kafka': {'enabled': True}}}), encoding='utf-8')

    with pytest.raises(
        ValidationError, match=re.escape("sources.kafka.connection 'main' is not defined under connections.kafka")
    ):
        load_config(path)


def test_a_consumer_not_in_use_needs_no_connection(load):
    config = load(
        {
            'connections': {'kafka': {}},
            'sources': {'kafka': {'enabled': False, 'connection': 'other'}, 'http': {'enabled': True}},
            'dlq': {'enabled': False, 'connection': 'other', 'topic': 'named-dlq'},
        }
    )

    assert config.kafka_source_connection is None
    assert config.dlq_connection is None


def test_dlq_with_a_topic_names_a_connection_on_an_http_only_worker(clean_environment, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(
        yaml.safe_dump({'sources': {'http': {'enabled': True}}, 'dlq': {'topic': 'named-dlq'}}),
        encoding='utf-8',
    )

    with pytest.raises(
        ValidationError, match=re.escape("dlq.connection 'main' is not defined under connections.kafka")
    ):
        load_config(path)


def test_dlq_provider_other_than_kafka_fails(load):
    with pytest.raises(ValidationError) as excinfo:
        load({'dlq': {'provider': 'sqs'}})

    assert "dlq.provider must be kafka, got 'sqs'" in str(excinfo.value)


@pytest.mark.parametrize(
    'kind, resolve_client, prefix',
    [('redis', resolve_redis_client, 'ops:'), ('consul', resolve_consul_client, 'ops/fleet')],
    ids=['redis', 'consul'],
)
async def test_state_prefix_is_used_by_providers(kind, resolve_client, prefix, load):
    config = load({'connections': {kind: {'main': {'state_prefix': prefix}}}})

    _, used_prefix = await resolve_client(
        connections=Connections(config.connections),
        connection='main',
        preset_client=None,
        preset_prefix='ignored',
        open_client=AsyncMock(return_value=object()),
        close_client=AsyncMock(),
        who='registry',
    )

    assert used_prefix == prefix


def test_client_config_of_a_connection_refuses_the_reserved_keys():
    with pytest.raises(ValidationError, match='client_config may not set'):
        KafkaConnectionConfig(client_config={'bootstrap.servers': 'other:9092'})


def test_a_connection_client_config_gives_way_to_the_consumer_on_a_shared_key():
    connection = KafkaConnectionConfig(client_config={'acks': '1', 'linger.ms': '5'})

    assert connection.client_config_with({'acks': 'all'}) == {'acks': 'all', 'linger.ms': '5'}


def test_a_dlq_topic_set_after_load_on_an_http_only_worker_names_the_missing_connection(load):
    """A hook may set ``dlq.topic`` on a worker that declared no Kafka connection; the read fails like the load."""
    config = load({'sources': {'kafka': {'enabled': False}, 'http': {'enabled': True}}})
    config.connections.kafka.clear()
    assert config.dlq_connection is None

    config.dlq.topic = 'named-dlq'

    with pytest.raises(ValueError, match=re.escape("dlq.connection 'main' is not defined under connections.kafka")):
        _ = config.dlq_connection


def test_a_sink_added_after_load_names_the_missing_connection(load):
    """A sink a hook adds on an undeclared connection fails with the path of its ``connection`` key."""
    config = load()
    config.sinks.kafka['results'] = KafkaSinkConfig(topic='out', connection='other')
    app = DrakkarApp(handler=PlainHandler(), config=config)

    with pytest.raises(ValueError, match=re.escape("sinks.kafka.results.connection 'other' is not defined")):
        app._build_sinks()
