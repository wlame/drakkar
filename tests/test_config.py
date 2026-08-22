"""Tests for Drakkar configuration loading."""

import fnmatch
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
    UIConfig,
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
    assert cfg.max_stdout_bytes == 0
    assert cfg.max_stderr_bytes == 0


def test_executor_config_rejects_empty_binary_path():
    with pytest.raises(ValidationError):
        ExecutorConfig(binary_path='')


def test_executor_config_rejects_zero_workers():
    with pytest.raises(ValidationError):
        ExecutorConfig(binary_path='/bin/echo', max_executors=0)


def test_executor_config_output_caps_default_to_unlimited():
    cfg = ExecutorConfig()
    assert cfg.max_stdout_bytes == 0
    assert cfg.max_stderr_bytes == 0


def test_executor_config_rejects_negative_stdout_cap():
    with pytest.raises(ValidationError):
        ExecutorConfig(max_stdout_bytes=-1)


def test_executor_config_rejects_negative_stderr_cap():
    with pytest.raises(ValidationError):
        ExecutorConfig(max_stderr_bytes=-1)


def test_executor_config_output_caps_env_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__MAX_STDOUT_BYTES', '1048576')
    monkeypatch.setenv('DK_EXECUTOR__MAX_STDERR_BYTES', '65536')
    cfg = DrakkarConfig()
    assert cfg.executor.max_stdout_bytes == 1048576
    assert cfg.executor.max_stderr_bytes == 65536


def _denied(name: str) -> bool:
    """Mirror ExecutorPool._is_env_key_denied against the configured defaults."""
    patterns = ExecutorConfig().env_inherit_deny
    return any(fnmatch.fnmatchcase(name.upper(), p.upper()) for p in patterns)


@pytest.mark.parametrize('name', ['AUTH_SERVICE_URL', 'CERT_PATH', 'PRIVATE_SUBNET', 'MONKEY_PATCH'])
def test_env_inherit_deny_does_not_withhold_common_non_secrets(name):
    """Withholding a variable breaks the user's binary — this list stays conservative."""
    assert not _denied(name)


@pytest.mark.parametrize('name', ['DB_PASSWD', 'HASH_SALT'])
def test_env_inherit_deny_withholds_unambiguous_secrets(name):
    assert _denied(name)


# --- Sink config models ---


def test_kafka_sink_config():
    cfg = KafkaSinkConfig(topic='results')
    assert cfg.topic == 'results'
    assert cfg.brokers == ''


def test_postgres_sink_config():
    cfg = PostgresSinkConfig(dsn='postgresql://localhost/db')
    assert cfg.pool_min == 2
    assert cfg.pool_max == 10


def test_http_sink_config_defaults():
    cfg = HttpSinkConfig(url='https://api.example.com/results')
    assert cfg.method == 'POST'
    assert cfg.timeout_seconds == 30
    assert cfg.headers == {}
    assert cfg.max_retries == 3


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


def test_http_sink_config_encoding_defaults_to_json():
    config = HttpSinkConfig(url='https://api.example.com/hook')

    assert config.encoding == 'json'


@pytest.mark.parametrize('encoding', ['json', 'form', 'multipart'])
def test_http_sink_config_accepts_every_supported_encoding(encoding):
    config = HttpSinkConfig(url='https://api.example.com/hook', encoding=encoding)

    assert config.encoding == encoding


def test_http_sink_config_rejects_unknown_encoding():
    with pytest.raises(ValidationError):
        HttpSinkConfig(url='https://api.example.com/hook', encoding='xml')


def test_http_sink_config_rejects_content_type_header():
    with pytest.raises(ValidationError) as excinfo:
        HttpSinkConfig(
            url='https://api.example.com/hook',
            encoding='form',
            headers={'Content-Type': 'application/xml'},
        )

    assert 'Content-Type' in str(excinfo.value)
    assert 'form' in str(excinfo.value)


def test_http_sink_config_rejects_content_type_header_case_insensitively():
    with pytest.raises(ValidationError):
        HttpSinkConfig(
            url='https://api.example.com/hook',
            headers={'content-type': 'application/json'},
        )


def test_http_sink_config_allows_other_headers():
    config = HttpSinkConfig(
        url='https://api.example.com/hook',
        headers={'Authorization': 'Bearer t', 'X-Trace': '1'},
    )

    assert config.headers == {'Authorization': 'Bearer t', 'X-Trace': '1'}


def test_redis_sink_config_defaults():
    cfg = RedisSinkConfig()
    assert cfg.url == 'redis://localhost:6379/0'
    assert cfg.key_prefix == ''


def test_file_sink_config_requires_base_path():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        FileSinkConfig()


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
    assert cfg.recorder.rotation_interval_hours == 1
    assert cfg.recorder.archive_window_hours == 24


def test_link_bases_strips_trailing_slash_and_validates_scheme():
    cfg = UIConfig(link_bases={'jira': 'https://jira.internal.example.com/'})
    assert cfg.link_bases == {'jira': 'https://jira.internal.example.com'}


def test_link_bases_rejects_non_http_scheme():
    with pytest.raises(ValidationError, match='http'):
        UIConfig(link_bases={'jira': 'ftp://jira.internal.example.com'})


def test_link_bases_rejects_invalid_base_name():
    with pytest.raises(ValidationError, match='identifier'):
        UIConfig(link_bases={'Jira Prod': 'https://jira.internal.example.com'})


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
    assert cfg.max_body_bytes == 10 * 1024 * 1024  # same default as the Go backend
    assert len(cfg.clients) == 1
    assert cfg.clients[0].name == 'anonymous'
    assert cfg.clients[0].token == ''
    assert cfg.clients[0].rpm == 4


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


def test_webapp_config_rejects_non_positive_max_body_bytes():
    # A zero/negative cap would reject every non-empty POST — same
    # validation rule (and message shape) as the Go backend.
    with pytest.raises(ValidationError) as exc_info:
        WebAppConfig(max_body_bytes=0)
    assert 'max_body_bytes must be > 0' in str(exc_info.value)


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


# --- Postgres operator-authored statements ---


def test_postgres_sink_config_statements_default_empty():
    assert PostgresSinkConfig(dsn='postgresql://localhost/db').statements == {}


def test_postgres_sink_config_accepts_named_statements():
    cfg = PostgresSinkConfig(
        dsn='postgresql://localhost/db',
        statements={'claim_job': 'UPDATE jobs SET status = :status WHERE id = :id'},
    )
    assert 'claim_job' in cfg.statements
    # Raw SQL is preserved — the sink compiles it, not the config model.
    assert ':status' in cfg.statements['claim_job']


@pytest.mark.parametrize('name', ['Claim_Job', '1claim', 'claim-job', 'claim job', ''])
def test_postgres_sink_config_rejects_bad_statement_names(name):
    """Names are used as structured-log fields, so they stay lowercase snake_case."""
    with pytest.raises(ValidationError, match='Invalid statement name'):
        PostgresSinkConfig(dsn='postgresql://localhost/db', statements={name: 'SELECT 1'})


@pytest.mark.parametrize('sql', ['', '   ', '\n'])
def test_postgres_sink_config_rejects_empty_sql(sql):
    with pytest.raises(ValidationError, match='empty SQL'):
        PostgresSinkConfig(dsn='postgresql://localhost/db', statements={'s': sql})


def test_postgres_sink_config_rejects_malformed_statement_sql():
    """Malformed statement config is a startup failure, not a first-delivery one."""
    with pytest.raises(ValidationError, match='positional'):
        PostgresSinkConfig(dsn='postgresql://localhost/db', statements={'s': 'UPDATE t SET a = $1'})
    with pytest.raises(ValidationError, match='Unterminated'):
        PostgresSinkConfig(dsn='postgresql://localhost/db', statements={'s': "UPDATE t SET a = 'oops"})


def test_postgres_sink_config_statements_from_yaml(minimal_config_dict, tmp_path):
    """The operator-facing path: statements declared in YAML."""
    import yaml

    from drakkar.config import load_config

    minimal_config_dict['sinks'] = {
        'postgres': {
            'main': {
                'dsn': 'postgresql://localhost/db',
                'statements': {
                    'claim_job': "UPDATE jobs SET status = :status WHERE id = :id AND status = 'pending'",
                },
            }
        }
    }
    cfg_file = tmp_path / 'drakkar.yaml'
    cfg_file.write_text(yaml.dump(minimal_config_dict))

    cfg = load_config(cfg_file)
    assert cfg.sinks.postgres['main'].statements['claim_job'].startswith('UPDATE jobs')


# --- Redis operator-authored scripts ---


def test_redis_sink_config_scripts_default_empty():
    assert RedisSinkConfig().scripts == {}


def test_redis_sink_config_accepts_named_scripts():
    cfg = RedisSinkConfig(scripts={'push_and_cap': "redis.call('LPUSH', KEYS[1], ARGV[1])"})
    assert 'push_and_cap' in cfg.scripts
    # The Lua body is preserved verbatim — the sink registers it, not the config model.
    assert 'LPUSH' in cfg.scripts['push_and_cap']


@pytest.mark.parametrize('name', ['Push_And_Cap', '1push', 'push-and-cap', 'push and cap', ''])
def test_redis_sink_config_rejects_bad_script_names(name):
    """Names are used as structured-log fields, so they stay lowercase snake_case."""
    with pytest.raises(ValidationError, match='Invalid script name'):
        RedisSinkConfig(scripts={name: 'return 1'})


@pytest.mark.parametrize('body', ['', '   ', '\n'])
def test_redis_sink_config_rejects_empty_script_bodies(body):
    with pytest.raises(ValidationError, match='empty body'):
        RedisSinkConfig(scripts={'s': body})


def test_redis_sink_config_does_not_parse_lua():
    """Deliberately unvalidated: there is no Lua parser without a server.

    Validating against a live Redis would couple worker startup to Redis
    availability — the same trade-off the Postgres sink settled by not
    calling PREPARE. A broken script fails at delivery instead.
    """
    cfg = RedisSinkConfig(scripts={'nonsense': 'this is not valid lua ((('})
    assert cfg.scripts['nonsense'] == 'this is not valid lua ((('


def test_redis_sink_config_scripts_from_yaml(minimal_config_dict, tmp_path):
    """The operator-facing path: scripts declared in YAML."""
    import yaml

    from drakkar.config import load_config

    minimal_config_dict['sinks'] = {
        'redis': {
            'cache': {
                'url': 'redis://localhost:6379/0',
                'scripts': {'push_and_cap': "redis.call('LPUSH', KEYS[1], ARGV[1])\n"},
            }
        }
    }
    cfg_file = tmp_path / 'drakkar.yaml'
    cfg_file.write_text(yaml.dump(minimal_config_dict))

    cfg = load_config(cfg_file)
    assert cfg.sinks.redis['cache'].scripts['push_and_cap'].startswith('redis.call')


def test_redis_sink_config_scripts_from_env(minimal_config_dict, tmp_path, monkeypatch):
    """Nested dict overrides are less exercised than scalar ones, so pin one."""
    import yaml

    from drakkar.config import load_config

    minimal_config_dict['sinks'] = {'redis': {'cache': {'url': 'redis://localhost:6379/0'}}}
    cfg_file = tmp_path / 'drakkar.yaml'
    cfg_file.write_text(yaml.dump(minimal_config_dict))
    monkeypatch.setenv('DK_SINKS__REDIS__CACHE__SCRIPTS__PUSH_AND_CAP', "redis.call('LPUSH', KEYS[1], ARGV[1])")

    cfg = load_config(cfg_file)
    assert cfg.sinks.redis['cache'].scripts == {'push_and_cap': "redis.call('LPUSH', KEYS[1], ARGV[1])"}


# --- Mongo operator-authored statements ---


def _mongo_config(**overrides):
    from drakkar.config import MongoSinkConfig

    return MongoSinkConfig(uri='mongodb://localhost:27017', database='app', **overrides)


def _claim_job(**overrides):
    """A well-formed statement, for tests that vary one thing about it."""
    statement = {
        'collection': 'jobs',
        'op': 'update_one',
        'filter': {'_id': ':id', 'status': 'pending'},
        'update': {'$set': {'status': ':status'}, '$inc': {'attempts': 1}},
    }
    statement.update(overrides)
    return statement


def test_mongo_sink_config_statements_default_empty():
    cfg = _mongo_config()
    assert cfg.statements == {}


def test_mongo_sink_config_accepts_a_named_statement():
    cfg = _mongo_config(statements={'claim_job': _claim_job()})

    statement = cfg.statements['claim_job']
    assert statement.collection == 'jobs'
    assert statement.op == 'update_one'
    # The template is preserved verbatim — the sink compiles it, not the
    # config type.
    assert statement.filter['_id'] == ':id'


def test_mongo_sink_config_accepts_a_pipeline_update():
    """A list update is MongoDB's own mechanism for computed updates."""
    cfg = _mongo_config(statements={'recompute': _claim_job(update=[{'$set': {'total': ':total'}}])})
    assert isinstance(cfg.statements['recompute'].update, list)


def test_mongo_sink_config_accepts_a_delete_statement_without_an_update():
    cfg = _mongo_config(
        statements={'sweep': {'collection': 'staging', 'op': 'delete_many', 'filter': {'batch': ':batch'}}}
    )
    assert cfg.statements['sweep'].update is None


@pytest.mark.parametrize(
    ('name', 'overrides', 'expected'),
    [
        # Names reach structured logs, so they stay lowercase snake_case.
        ('ClaimJob', {}, 'Invalid statement name'),
        ('1claim', {}, 'Invalid statement name'),
        ('claim-job', {}, 'Invalid statement name'),
        ('', {}, 'Invalid statement name'),
        # An empty filter matches EVERY document — delete_many({}) empties a
        # collection outright.
        ('claim_job', {'filter': {}}, 'filter'),
        ('claim_job', {'collection': ''}, 'collection'),
        # The escape hatch exists for what the declarative tier cannot
        # express, and an insert is fully expressible already.
        ('claim_job', {'op': 'insert'}, 'op'),
        ('claim_job', {'op': 'statement'}, 'op'),
        ('claim_job', {'op': 'nonsense'}, 'op'),
        # update present/absent per op.
        ('claim_job', {'update': None}, 'update'),
        (
            'sweep',
            {'op': 'delete_many', 'update': {'$set': {'a': 1}}},
            'update',
        ),
        # Malformed placeholders fail at startup, not at first delivery.
        ('claim_job', {'filter': {'_id': ':'}}, 'placeholder'),
        ('claim_job', {'filter': {':id': 1}}, 'key'),
    ],
)
def test_mongo_sink_config_rejects_bad_statements(name, overrides, expected):
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match=expected):
        _mongo_config(statements={name: _claim_job(**overrides)})


@pytest.mark.parametrize('operator', ['$where', '$function'])
def test_mongo_sink_config_rejects_server_side_javascript(operator):
    """Both execute JavaScript on the server, and an operator learns at startup."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match=r'\$'):
        _mongo_config(statements={'bad': _claim_job(filter={operator: 'this.x < 1'})})


@pytest.mark.parametrize('operator', ['$where', '$function'])
def test_mongo_sink_config_rejects_javascript_inside_a_pipeline_stage(operator):
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match=r'\$'):
        _mongo_config(statements={'bad': _claim_job(update=[{'$set': {'x': {operator: 'code'}}}])})


def test_mongo_sink_config_statements_from_yaml(minimal_config_dict, tmp_path):
    """The operator-facing path: a statement declared in YAML."""
    import yaml

    from drakkar.config import load_config

    minimal_config_dict['sinks'] = {
        'mongo': {
            'main': {
                'uri': 'mongodb://localhost:27017',
                'database': 'app',
                'statements': {'claim_job': _claim_job()},
            }
        }
    }
    cfg_file = tmp_path / 'drakkar.yaml'
    cfg_file.write_text(yaml.dump(minimal_config_dict))

    cfg = load_config(cfg_file)
    assert cfg.sinks.mongo['main'].statements['claim_job'].collection == 'jobs'


def test_mongo_sink_config_statements_from_env(minimal_config_dict, tmp_path, monkeypatch):
    """One level deeper than either companion sink's env override.

    A statement is a nested MODEL rather than a string, so the DK_ path has
    to reach a field inside a mapping entry.
    """
    import yaml

    from drakkar.config import load_config

    minimal_config_dict['sinks'] = {
        'mongo': {
            'main': {
                'uri': 'mongodb://localhost:27017',
                'database': 'app',
                'statements': {'claim_job': _claim_job()},
            }
        }
    }
    cfg_file = tmp_path / 'drakkar.yaml'
    cfg_file.write_text(yaml.dump(minimal_config_dict))
    monkeypatch.setenv('DK_SINKS__MONGO__MAIN__STATEMENTS__CLAIM_JOB__COLLECTION', 'archived_jobs')

    cfg = load_config(cfg_file)
    assert cfg.sinks.mongo['main'].statements['claim_job'].collection == 'archived_jobs'
