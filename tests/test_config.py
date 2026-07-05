"""Tests for Drakkar configuration loading."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from drakkar.config import (
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
    WebAppConfig,
    WebClientConfig,
    load_config,
)
from tests.conftest import make_ui_config

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
    assert cfg.max_executors == 4
    assert cfg.task_timeout_seconds == 120
    assert cfg.window_size == 100
    assert cfg.max_retries == 3
    # drain_timeout default raised to 30 so graceful shutdown has a reasonable
    # chance of finishing in-flight subprocess tasks rather than hard-killing
    # them and relying on at-least-once replay.
    assert cfg.drain_timeout_seconds == 30
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
        ExecutorConfig(binary_path='/bin/echo', max_executors=0)


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


# --- H8: HttpSinkConfig.url validation ---


def test_http_sink_config_rejects_non_http_scheme():
    from pydantic import ValidationError

    for bad_url in ('file:///etc/passwd', 'ftp://host/x', 'gopher://host', ''):
        with pytest.raises(ValidationError):
            HttpSinkConfig(url=bad_url)


def test_http_sink_config_rejects_url_without_host():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        HttpSinkConfig(url='http:///no-host')


def test_http_sink_config_rejects_metadata_endpoints():
    """Known cloud metadata hosts must be refused. POSTing there can leak
    IAM credentials (AWS/Azure/GCP/Alibaba IMDS all live at 169.254.169.254
    or similar well-known addresses).
    """
    from pydantic import ValidationError

    for url in (
        'http://169.254.169.254/latest/meta-data',
        'https://metadata.google.internal/computeMetadata/v1/',
        'http://100.100.100.200/',  # Alibaba
        'http://192.0.0.192/',  # Oracle
    ):
        with pytest.raises(ValidationError) as exc_info:
            HttpSinkConfig(url=url)
        # Error message must explain why (operator needs to understand).
        assert 'metadata' in str(exc_info.value).lower()


def test_http_sink_config_accepts_normal_urls():
    """Loopback, private, and public http(s) URLs all remain allowed — the
    URL is operator-configured and many legitimate deployments target
    internal webhook services.
    """
    ok_urls = [
        'http://localhost:8000/webhook',
        'http://127.0.0.1:3000/',
        'http://10.0.0.5/api',
        'https://api.example.com/v1/ingest',
        'http://internal-webhook.mycompany.local/',
    ]
    for url in ok_urls:
        cfg = HttpSinkConfig(url=url)
        assert cfg.url == url


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
    # Instance names come back SORTED (not YAML order) — the summary is
    # byte-parity contractual with the Go backend, whose maps have no
    # insertion order to preserve.
    cfg = SinksConfig(
        kafka={
            'results': KafkaSinkConfig(topic='results'),
            'notifications': KafkaSinkConfig(topic='notifications'),
        },
    )
    assert cfg.summary() == {'kafka': ['notifications', 'results']}


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


def test_ui_config_defaults():
    cfg = make_ui_config()
    assert cfg.recorder.max_buffer == 50_000
    assert cfg.max_rows == 5000
    assert cfg.recorder.flush_interval_seconds == 5
    assert cfg.recorder.retention_hours == 24


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
    assert cfg.executor.max_executors == 40


def test_load_config_minimal_yaml(minimal_config_yaml_file: Path):
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.executor.binary_path == '/usr/bin/echo'
    assert cfg.kafka.brokers == 'localhost:9092'
    assert cfg.metrics.enabled is True


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError, match='Config file not found'):
        load_config('/nonexistent/path/config.yaml')


def test_load_config_from_env_var(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_CONFIG', str(minimal_config_yaml_file))
    cfg = load_config()
    assert cfg.executor.binary_path == '/usr/bin/echo'


def test_load_config_env_override(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_KAFKA__BROKERS', 'override:9092')
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.kafka.brokers == 'override:9092'


def test_load_config_no_path_no_env_requires_executor(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv('DK_CONFIG', raising=False)
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/usr/bin/test')
    cfg = load_config()
    assert cfg.executor.binary_path == '/usr/bin/test'


def test_load_config_empty_yaml(tmp_path: Path):
    config_path = tmp_path / 'empty.yaml'
    config_path.write_text('')
    cfg = load_config(config_path)
    assert cfg.executor.binary_path is None


def test_drakkar_config_env_nested_delimiter(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/usr/bin/test')
    monkeypatch.setenv('DK_EXECUTOR__MAX_EXECUTORS', '16')
    monkeypatch.setenv('DK_KAFKA__SOURCE_TOPIC', 'my-topic')
    cfg = DrakkarConfig()
    assert cfg.executor.binary_path == '/usr/bin/test'
    assert cfg.executor.max_executors == 16
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
    assert data['executor']['max_executors'] == 40


def test_deep_merge_recursive():
    """_deep_merge recursively merges nested dicts."""
    from drakkar.config import _deep_merge

    base = {'a': {'b': 1, 'c': 2}, 'd': 3}
    override = {'a': {'c': 99, 'e': 5}}
    result = _deep_merge(base, override)
    assert result == {'a': {'b': 1, 'c': 99, 'e': 5}, 'd': 3}


# --- WebClientConfig ---


def test_web_client_config_defaults():
    cfg = WebClientConfig(name='alice')
    assert cfg.name == 'alice'
    assert cfg.token == ''
    assert cfg.rpm == 4


def test_web_client_config_custom_values():
    cfg = WebClientConfig(name='tenant-a', token='secret-token', rpm=60)
    assert cfg.name == 'tenant-a'
    assert cfg.token == 'secret-token'
    assert cfg.rpm == 60


def test_web_client_config_rejects_empty_name():
    with pytest.raises(ValidationError) as exc_info:
        WebClientConfig(name='')
    assert 'non-empty' in str(exc_info.value)


def test_web_client_config_rejects_whitespace_only_name():
    with pytest.raises(ValidationError):
        WebClientConfig(name='   ')


# --- WebAppConfig defaults ---


def test_webapp_config_defaults():
    cfg = WebAppConfig()
    assert cfg.enabled is False
    assert cfg.host == '0.0.0.0'
    assert cfg.port == 8090
    assert cfg.path == '/process'
    assert cfg.sinks_enabled is False
    assert cfg.request_timeout_seconds == 30.0
    assert cfg.max_concurrent == 64
    assert len(cfg.clients) == 1
    assert cfg.clients[0].name == 'anonymous'
    assert cfg.clients[0].token == ''
    assert cfg.clients[0].rpm == 4


def test_webapp_config_custom_values():
    cfg = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=9999,
        path='/api/process',
        sinks_enabled=True,
        request_timeout_seconds=10.5,
        max_concurrent=32,
        clients=[
            WebClientConfig(name='anonymous', token='', rpm=4),
            WebClientConfig(name='tenant-a', token='token-a', rpm=60),
        ],
    )
    assert cfg.enabled is True
    assert cfg.port == 9999
    assert cfg.path == '/api/process'
    assert cfg.sinks_enabled is True
    assert len(cfg.clients) == 2


# --- WebAppConfig validation rules ---


def test_webapp_config_rejects_two_empty_token_clients():
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(
            clients=[
                WebClientConfig(name='one', token='', rpm=4),
                WebClientConfig(name='two', token='', rpm=4),
            ],
        )
    msg = str(exc_info.value)
    assert 'empty token' in msg
    assert "'one'" in msg
    assert "'two'" in msg


def test_webapp_config_rejects_duplicate_non_empty_tokens():
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(
            clients=[
                WebClientConfig(name='a', token='shared', rpm=4),
                WebClientConfig(name='b', token='shared', rpm=4),
            ],
        )
    msg = str(exc_info.value)
    assert 'unique' in msg
    assert "'a'" in msg
    assert "'b'" in msg


def test_webapp_config_rejects_zero_rpm_client():
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(
            clients=[WebClientConfig(name='zero-rpm', token='t', rpm=0)],
        )
    msg = str(exc_info.value)
    assert "'zero-rpm'" in msg
    assert 'rpm' in msg


def test_webapp_config_rejects_negative_rpm_client():
    with pytest.raises(ValidationError):
        WebAppConfig(
            clients=[WebClientConfig(name='neg-rpm', token='t', rpm=-5)],
        )


def test_webapp_config_rejects_zero_request_timeout():
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(request_timeout_seconds=0)
    assert 'request_timeout_seconds' in str(exc_info.value)


def test_webapp_config_rejects_negative_request_timeout():
    with pytest.raises(ValidationError):
        WebAppConfig(request_timeout_seconds=-1.0)


def test_webapp_config_rejects_zero_max_concurrent():
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(max_concurrent=0)
    assert 'max_concurrent' in str(exc_info.value)


def test_webapp_config_rejects_negative_max_concurrent():
    with pytest.raises(ValidationError):
        WebAppConfig(max_concurrent=-1)


@pytest.mark.parametrize(
    'bad_path',
    ['', '/', 'process', 'api/process'],
)
def test_webapp_config_rejects_invalid_path(bad_path: str):
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(path=bad_path)
    assert 'webapp.path' in str(exc_info.value)


def test_webapp_config_rejects_empty_clients_list():
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(clients=[])
    assert 'at least one client' in str(exc_info.value)


def test_webapp_config_accepts_one_anonymous_plus_named_clients():
    """Mixing one empty-token (anonymous) client with several named ones is valid."""
    cfg = WebAppConfig(
        clients=[
            WebClientConfig(name='anonymous', token='', rpm=4),
            WebClientConfig(name='tenant-a', token='tok-a', rpm=60),
            WebClientConfig(name='tenant-b', token='tok-b', rpm=120),
        ],
    )
    assert len(cfg.clients) == 3


# --- DrakkarConfig integration ---


def test_drakkar_config_webapp_default():
    cfg = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/true'))
    assert cfg.webapp.enabled is False
    assert cfg.webapp.port == 8090
    assert len(cfg.webapp.clients) == 1
    assert cfg.webapp.clients[0].name == 'anonymous'


def test_drakkar_config_webapp_from_yaml(tmp_path: Path):
    """YAML round-trip: a webapp block parses correctly."""
    config_data = {
        'executor': {'binary_path': '/bin/echo'},
        'webapp': {
            'enabled': True,
            'host': '0.0.0.0',
            'port': 8090,
            'path': '/process',
            'sinks_enabled': True,
            'request_timeout_seconds': 15.0,
            'max_concurrent': 32,
            'clients': [
                {'name': 'anonymous', 'token': '', 'rpm': 4},
                {'name': 'tenant-a', 'token': 'tok-a', 'rpm': 60},
            ],
        },
    }
    import yaml

    config_path = tmp_path / 'webapp.yaml'
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)

    cfg = load_config(config_path)
    assert cfg.webapp.enabled is True
    assert cfg.webapp.port == 8090
    assert cfg.webapp.sinks_enabled is True
    assert cfg.webapp.request_timeout_seconds == 15.0
    assert cfg.webapp.max_concurrent == 32
    assert len(cfg.webapp.clients) == 2
    assert cfg.webapp.clients[1].name == 'tenant-a'
    assert cfg.webapp.clients[1].rpm == 60


def test_drakkar_config_webapp_env_override_enabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/echo')
    monkeypatch.setenv('DK_WEBAPP__ENABLED', 'true')
    cfg = DrakkarConfig()
    assert cfg.webapp.enabled is True


def test_drakkar_config_webapp_env_override_port(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/echo')
    monkeypatch.setenv('DK_WEBAPP__PORT', '9000')
    cfg = DrakkarConfig()
    assert cfg.webapp.port == 9000


def test_drakkar_config_webapp_env_override_client_rpm(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    """``DK_WEBAPP__CLIENTS__0__RPM`` overrides the first client's rpm.

    Verifies that pydantic-settings env-var nested delimiter handling
    (already exercised for other sub-configs) works for list-of-objects
    fields too.
    """
    monkeypatch.setenv('DK_WEBAPP__CLIENTS__0__RPM', '10')
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.webapp.clients[0].rpm == 10


def test_drakkar_config_webapp_env_override_client_token(
    minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv('DK_WEBAPP__CLIENTS__0__TOKEN', 'tok-from-env')
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.webapp.clients[0].token == 'tok-from-env'
