"""Tests for Drakkar configuration loading."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from drakkar.config import (
    DebugConfig,
    DLQConfig,
    DrakkarConfig,
    ExecutorConfig,
    FileSinkConfig,
    HttpSinkConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    MongoSinkConfig,
    PostgresSinkConfig,
    RedisSinkConfig,
    SinksConfig,
    load_config,
)

# --- KafkaConfig (source/consumer) ---


def test_kafka_config_defaults():
    cfg = KafkaConfig()
    assert cfg.brokers == 'localhost:9092'
    assert cfg.consumer_group == 'drakkar-workers'
    assert cfg.max_poll_records == 100
    assert cfg.max_poll_interval_ms == 300_000


# --- ExecutorConfig ---


def test_executor_config_binary_path_defaults_to_none():
    cfg = ExecutorConfig()
    assert cfg.binary_path is None


def test_executor_config_defaults():
    cfg = ExecutorConfig(binary_path='/usr/bin/echo')
    assert cfg.max_workers == 4
    assert cfg.task_timeout_seconds == 120
    assert cfg.window_size == 100
    assert cfg.max_retries == 3
    assert cfg.drain_timeout_seconds == 5
    assert cfg.backpressure_high_multiplier == 32
    assert cfg.backpressure_low_multiplier == 4


def test_executor_config_custom_values():
    cfg = ExecutorConfig(
        binary_path='/bin/test',
        max_retries=5,
        drain_timeout_seconds=10,
        backpressure_high_multiplier=16,
        backpressure_low_multiplier=2,
    )
    assert cfg.max_retries == 5
    assert cfg.drain_timeout_seconds == 10


def test_executor_config_rejects_empty_binary_path():
    with pytest.raises(ValidationError):
        ExecutorConfig(binary_path='')


def test_executor_config_rejects_zero_workers():
    with pytest.raises(ValidationError):
        ExecutorConfig(binary_path='/bin/echo', max_workers=0)


# --- Sink config models ---


def test_kafka_sink_config():
    cfg = KafkaSinkConfig(topic='results')
    assert cfg.topic == 'results'
    assert cfg.brokers == ''


def test_kafka_sink_config_custom_brokers():
    cfg = KafkaSinkConfig(topic='results', brokers='other:9092')
    assert cfg.brokers == 'other:9092'


def test_postgres_sink_config():
    cfg = PostgresSinkConfig(dsn='postgresql://localhost/db')
    assert cfg.pool_min == 2
    assert cfg.pool_max == 10


def test_postgres_sink_config_custom_pool():
    cfg = PostgresSinkConfig(dsn='postgresql://localhost/db', pool_min=5, pool_max=20)
    assert cfg.pool_min == 5
    assert cfg.pool_max == 20


def test_mongo_sink_config():
    cfg = MongoSinkConfig(uri='mongodb://localhost:27017', database='mydb')
    assert cfg.uri == 'mongodb://localhost:27017'
    assert cfg.database == 'mydb'


def test_http_sink_config_defaults():
    cfg = HttpSinkConfig(url='https://api.example.com/results')
    assert cfg.method == 'POST'
    assert cfg.timeout_seconds == 30
    assert cfg.headers == {}
    assert cfg.max_retries == 3


def test_http_sink_config_custom():
    cfg = HttpSinkConfig(
        url='https://api.example.com',
        method='PUT',
        timeout_seconds=10,
        headers={'Authorization': 'Bearer xxx'},
        max_retries=0,
    )
    assert cfg.method == 'PUT'
    assert cfg.headers['Authorization'] == 'Bearer xxx'


def test_redis_sink_config_defaults():
    cfg = RedisSinkConfig()
    assert cfg.url == 'redis://localhost:6379/0'
    assert cfg.key_prefix == ''


def test_redis_sink_config_custom():
    cfg = RedisSinkConfig(url='redis://cache:6379/1', key_prefix='drakkar:')
    assert cfg.key_prefix == 'drakkar:'


def test_file_sink_config_requires_base_path():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        FileSinkConfig()


def test_file_sink_config_custom():
    cfg = FileSinkConfig(base_path='/data/output')
    assert cfg.base_path == '/data/output'


# --- SinksConfig ---


def test_sinks_config_empty():
    cfg = SinksConfig()
    assert cfg.is_empty
    assert cfg.summary() == {}


def test_sinks_config_single_kafka():
    cfg = SinksConfig(kafka={'results': KafkaSinkConfig(topic='results')})
    assert not cfg.is_empty
    assert cfg.summary() == {'kafka': ['results']}


def test_sinks_config_multiple_types():
    cfg = SinksConfig(
        kafka={'results': KafkaSinkConfig(topic='results')},
        postgres={'main': PostgresSinkConfig(dsn='postgresql://localhost/db')},
    )
    assert cfg.summary() == {'kafka': ['results'], 'postgres': ['main']}


def test_sinks_config_multiple_instances_same_type():
    cfg = SinksConfig(
        kafka={
            'results': KafkaSinkConfig(topic='results'),
            'notifications': KafkaSinkConfig(topic='notifications'),
        },
    )
    assert cfg.summary() == {'kafka': ['results', 'notifications']}


def test_sinks_config_all_types():
    cfg = SinksConfig(
        kafka={'k': KafkaSinkConfig(topic='t')},
        postgres={'p': PostgresSinkConfig(dsn='postgresql://x')},
        mongo={'m': MongoSinkConfig(uri='mongodb://x', database='db')},
        http={'h': HttpSinkConfig(url='https://x')},
        redis={'r': RedisSinkConfig()},
        filesystem={'f': FileSinkConfig(base_path='/data')},
    )
    assert not cfg.is_empty
    assert set(cfg.summary().keys()) == {
        'kafka',
        'postgres',
        'mongo',
        'http',
        'redis',
        'filesystem',
    }


# --- DLQConfig ---


def test_dlq_config_defaults():
    cfg = DLQConfig()
    assert cfg.topic == ''
    assert cfg.brokers == ''


def test_dlq_config_custom():
    cfg = DLQConfig(topic='my-dlq', brokers='dlq-cluster:9092')
    assert cfg.topic == 'my-dlq'
    assert cfg.brokers == 'dlq-cluster:9092'


# --- Other config models ---


def test_metrics_config_defaults():
    cfg = MetricsConfig()
    assert cfg.enabled is True
    assert cfg.port == 9090


def test_metrics_config_rejects_invalid_port():
    with pytest.raises(ValidationError):
        MetricsConfig(port=0)
    with pytest.raises(ValidationError):
        MetricsConfig(port=99999)


def test_debug_config_defaults():
    cfg = DebugConfig()
    assert cfg.max_buffer == 50_000
    assert cfg.max_ui_rows == 5000
    assert cfg.flush_interval_seconds == 5
    assert cfg.retention_hours == 24


def test_logging_config_defaults():
    cfg = LoggingConfig()
    assert cfg.level == 'INFO'
    assert cfg.format == 'json'


def test_logging_config_valid_formats():
    assert LoggingConfig(format='json').format == 'json'
    assert LoggingConfig(format='console').format == 'console'


def test_logging_config_invalid_format():
    with pytest.raises(ValidationError):
        LoggingConfig(format='xml')


# --- DrakkarConfig (root) ---


def test_worker_name_env_default():
    cfg = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/true'))
    assert cfg.worker_name_env == 'WORKER_ID'


def test_worker_name_env_custom():
    cfg = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        worker_name_env='MY_WORKER',
    )
    assert cfg.worker_name_env == 'MY_WORKER'


def test_drakkar_config_sinks_default():
    cfg = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/true'))
    assert cfg.sinks.is_empty
    assert cfg.dlq.topic == ''


def test_drakkar_config_with_sinks():
    cfg = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        sinks=SinksConfig(kafka={'out': KafkaSinkConfig(topic='out')}),
        dlq=DLQConfig(topic='my-dlq'),
    )
    assert not cfg.sinks.is_empty
    assert cfg.dlq.topic == 'my-dlq'


# --- load_config ---


def test_load_config_from_yaml(config_yaml_file: Path):
    cfg = load_config(config_yaml_file)
    assert cfg.kafka.brokers == 'kafka1:9092,kafka2:9092'
    assert cfg.executor.binary_path == '/usr/local/bin/processor'
    assert cfg.executor.max_workers == 40


def test_load_config_minimal_yaml(minimal_config_yaml_file: Path):
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.executor.binary_path == '/usr/bin/echo'
    assert cfg.kafka.brokers == 'localhost:9092'
    assert cfg.metrics.enabled is True


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError, match='Config file not found'):
        load_config('/nonexistent/path/config.yaml')


def test_load_config_from_env_var(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DRAKKAR_CONFIG', str(minimal_config_yaml_file))
    cfg = load_config()
    assert cfg.executor.binary_path == '/usr/bin/echo'


def test_load_config_env_override(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DRAKKAR_KAFKA__BROKERS', 'override:9092')
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.kafka.brokers == 'override:9092'


def test_load_config_no_path_no_env_requires_executor(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv('DRAKKAR_CONFIG', raising=False)
    monkeypatch.setenv('DRAKKAR_EXECUTOR__BINARY_PATH', '/usr/bin/test')
    cfg = load_config()
    assert cfg.executor.binary_path == '/usr/bin/test'


def test_load_config_empty_yaml(tmp_path: Path):
    config_path = tmp_path / 'empty.yaml'
    config_path.write_text('')
    cfg = load_config(config_path)
    assert cfg.executor.binary_path is None


def test_drakkar_config_env_nested_delimiter(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DRAKKAR_EXECUTOR__BINARY_PATH', '/usr/bin/test')
    monkeypatch.setenv('DRAKKAR_EXECUTOR__MAX_WORKERS', '16')
    monkeypatch.setenv('DRAKKAR_KAFKA__SOURCE_TOPIC', 'my-topic')
    cfg = DrakkarConfig()
    assert cfg.executor.binary_path == '/usr/bin/test'
    assert cfg.executor.max_workers == 16
    assert cfg.kafka.source_topic == 'my-topic'


def test_load_config_with_sinks_yaml(tmp_path: Path):
    """YAML with sinks section parses correctly."""
    config_data = {
        'executor': {'binary_path': '/bin/echo'},
        'sinks': {
            'kafka': {'results': {'topic': 'search-results'}},
            'postgres': {'main': {'dsn': 'postgresql://localhost/db'}},
        },
        'dlq': {'topic': 'my-dlq'},
    }
    import yaml

    config_path = tmp_path / 'sinks.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)

    cfg = load_config(config_path)
    assert cfg.sinks.summary() == {'kafka': ['results'], 'postgres': ['main']}
    assert cfg.dlq.topic == 'my-dlq'


def test_config_serialization(config_yaml_file: Path):
    cfg = load_config(config_yaml_file)
    data = cfg.model_dump()
    assert data['kafka']['brokers'] == 'kafka1:9092,kafka2:9092'
    assert data['executor']['max_workers'] == 40


def test_deep_merge_recursive():
    """_deep_merge recursively merges nested dicts."""
    from drakkar.config import _deep_merge

    base = {'a': {'b': 1, 'c': 2}, 'd': 3}
    override = {'a': {'c': 99, 'e': 5}}
    result = _deep_merge(base, override)
    assert result == {'a': {'b': 1, 'c': 99, 'e': 5}, 'd': 3}
