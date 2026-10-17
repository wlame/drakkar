"""The ``connections`` section: defaults, env-created entries, shape rules, secret flags."""

import os
import traceback
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from drakkar.app import DrakkarApp
from drakkar.config import (
    ConnectionsConfig,
    ConsulConnectionConfig,
    DrakkarConfig,
    PostgresConnectionConfig,
    RedisConnectionConfig,
    _deep_merge,
    load_config,
)
from drakkar.configmeta import walk_model_fields
from drakkar.handler import BaseDrakkarHandler

BASE_YAML = {'sources': {'kafka': {'enabled': True}}, 'ui': {'release': {'enabled': False}}}

CONSUL_PREFIX_MESSAGE = (
    "connections.consul.{name}.key_prefix '{value}' must be segments of letters, digits, '_', '.' and '-' "
    "joined by '/', with no leading or trailing '/'"
)
REDIS_PREFIX_MESSAGE = (
    "connections.redis.{name}.key_prefix '{value}' may only contain letters, digits, '_', ':', '.' and '-'"
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
        path.write_text(yaml.safe_dump(_deep_merge(BASE_YAML, yaml_data or {}), sort_keys=False), encoding='utf-8')
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
        {'connections': {'redis': {'main': {'url': 'redis://yaml-host/1', 'key_prefix': 'app:'}}}},
        env={'DK_CONNECTIONS__REDIS__MAIN__URL': 'redis://env-host/2'},
    )

    main = cfg.connections.redis['main']
    assert (main.url, main.key_prefix) == ('redis://env-host/2', 'app:')


def test_no_connections_are_defined_by_default(load):
    cfg = load()

    assert cfg.connections == ConnectionsConfig()
    assert (cfg.connections.consul, cfg.connections.redis, cfg.connections.postgres) == ({}, {}, {})


def test_consul_and_redis_entries_take_their_defaults(load):
    cfg = load({'connections': {'consul': {'main': {}}, 'redis': {'main': {}}}})

    consul = cfg.connections.consul['main']
    assert (consul.address, consul.token, consul.datacenter, consul.key_prefix) == (
        'http://127.0.0.1:8500',
        '',
        '',
        'drakkar',
    )
    redis = cfg.connections.redis['main']
    assert (redis.url, redis.key_prefix) == ('redis://localhost:6379/0', 'drakkar:')


def test_a_postgres_connection_without_a_dsn_fails_the_load(load):
    with pytest.raises(ValidationError, match=r'connections\.postgres\.main\.dsn'):
        load({'connections': {'postgres': {'main': {}}}})


@pytest.mark.parametrize(
    'kind, field, value, message',
    [
        ('consul', 'key_prefix', '/drakkar', CONSUL_PREFIX_MESSAGE),
        ('consul', 'key_prefix', 'drakkar/', CONSUL_PREFIX_MESSAGE),
        ('consul', 'key_prefix', 'a//b', CONSUL_PREFIX_MESSAGE),
        ('consul', 'key_prefix', '', CONSUL_PREFIX_MESSAGE),
        ('consul', 'key_prefix', 'bad prefix', CONSUL_PREFIX_MESSAGE),
        ('redis', 'key_prefix', 'bad prefix', REDIS_PREFIX_MESSAGE),
        ('redis', 'key_prefix', 'a/b', REDIS_PREFIX_MESSAGE),
        ('postgres', 'schema', 'Drakkar', SCHEMA_MESSAGE),
        ('postgres', 'schema', '1abc', SCHEMA_MESSAGE),
        ('postgres', 'schema', 'a-b', SCHEMA_MESSAGE),
        ('postgres', 'schema', '', SCHEMA_MESSAGE),
    ],
)
def test_an_invalid_prefix_or_schema_fails_the_load_with_the_contract_message(load, kind, field, value, message):
    entry = {'dsn': 'postgresql://h/db', field: value} if kind == 'postgres' else {field: value}

    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {kind: {'main': entry}}})

    assert message.format(name='main', value=value) in str(excinfo.value)


@pytest.mark.parametrize(
    'kind, field, value',
    [
        ('consul', 'key_prefix', 'drakkar'),
        ('consul', 'key_prefix', 'team-a/drakkar_1.v2'),
        ('redis', 'key_prefix', ''),
        ('redis', 'key_prefix', 'team-a:drakkar.v2_'),
        ('postgres', 'schema', '_private'),
        ('postgres', 'schema', 'drakkar_2'),
    ],
)
def test_a_valid_prefix_or_schema_loads(load, kind, field, value):
    entry = {'dsn': 'postgresql://h/db', field: value} if kind == 'postgres' else {field: value}

    cfg = load({'connections': {kind: {'main': entry}}})

    loaded = getattr(cfg.connections, kind)['main']
    assert getattr(loaded, 'schema_name' if field == 'schema' else field) == value


def test_a_value_with_a_trailing_newline_is_not_a_valid_schema(load):
    with pytest.raises(ValidationError, match='must be a lower-case identifier'):
        load({'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db', 'schema': 'drakkar\n'}}}})


def test_the_message_names_the_connection_that_is_wrong(load):
    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'redis': {'main': {}, 'cache-2': {'key_prefix': 'x y'}}}})

    assert REDIS_PREFIX_MESSAGE.format(name='cache-2', value='x y') in str(excinfo.value)


def test_with_several_wrong_connections_the_first_by_name_is_reported(load):
    with pytest.raises(ValidationError) as excinfo:
        load({'connections': {'redis': {'b': {'key_prefix': 'x y'}, 'a': {'key_prefix': 'x y'}}}})

    assert REDIS_PREFIX_MESSAGE.format(name='a', value='x y') in str(excinfo.value)


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


def test_a_connection_model_can_be_built_by_attribute_name():
    cfg = PostgresConnectionConfig(dsn='postgresql://h/db', schema_name='audit')

    assert cfg.schema_name == 'audit'


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
