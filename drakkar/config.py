"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DRAKKAR_ prefix with __ for nesting (e.g., DRAKKAR_KAFKA__BROKERS).
"""

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- Kafka source (consumer) config ---


class KafkaConfig(BaseModel):
    """Kafka connection and consumer settings."""

    brokers: str = 'localhost:9092'
    source_topic: str = 'input-events'
    consumer_group: str = 'drakkar-workers'
    max_poll_records: int = 100
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 3_000


# --- Sink config models ---


class KafkaSinkConfig(BaseModel):
    """Configuration for a Kafka output sink.

    Each named instance produces messages to a specific topic.
    If `brokers` is empty, inherits from `kafka.brokers` (same cluster).
    """

    topic: str
    brokers: str = ''
    ui_url: str = ''


class PostgresSinkConfig(BaseModel):
    """Configuration for a PostgreSQL output sink.

    Each named instance connects to a database via asyncpg pool.
    """

    dsn: str
    pool_min: int = Field(default=2, ge=1)
    pool_max: int = Field(default=10, ge=1)
    ui_url: str = ''


class MongoSinkConfig(BaseModel):
    """Configuration for a MongoDB output sink.

    Each named instance connects to a database via motor AsyncIOMotorClient.
    """

    uri: str
    database: str
    ui_url: str = ''


class HttpSinkConfig(BaseModel):
    """Configuration for an HTTP output sink.

    Each named instance POSTs JSON payloads to a URL.
    """

    url: str
    method: str = 'POST'
    timeout_seconds: int = Field(default=30, ge=1)
    headers: dict[str, str] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0)
    ui_url: str = ''


class RedisSinkConfig(BaseModel):
    """Configuration for a Redis output sink.

    Each named instance connects to a Redis server and sets key-value pairs.
    """

    url: str = 'redis://localhost:6379/0'
    key_prefix: str = ''
    ui_url: str = ''


class FileSinkConfig(BaseModel):
    """Configuration for a filesystem output sink.

    Writes JSONL lines to files. `base_path` is required — all payload
    paths are resolved relative to it and contained within it.
    """

    base_path: str = Field(min_length=1)
    ui_url: str = ''


class SinksConfig(BaseModel):
    """Container for all configured sink instances, grouped by type.

    Each sink type maps sink names to their configuration.
    Example YAML::

        sinks:
          kafka:
            results:
              topic: "search-results"
          postgres:
            main-db:
              dsn: "postgresql://..."
    """

    kafka: dict[str, KafkaSinkConfig] = Field(default_factory=dict)
    postgres: dict[str, PostgresSinkConfig] = Field(default_factory=dict)
    mongo: dict[str, MongoSinkConfig] = Field(default_factory=dict)
    http: dict[str, HttpSinkConfig] = Field(default_factory=dict)
    redis: dict[str, RedisSinkConfig] = Field(default_factory=dict)
    filesystem: dict[str, FileSinkConfig] = Field(default_factory=dict)

    @property
    def is_empty(self) -> bool:
        """True if no sinks of any type are configured."""
        return not any([self.kafka, self.postgres, self.mongo, self.http, self.redis, self.filesystem])

    def summary(self) -> dict[str, list[str]]:
        """Return a dict of sink type → list of instance names.

        Useful for startup logging. Only includes types with at least one instance.
        """
        result: dict[str, list[str]] = {}
        if self.kafka:
            result['kafka'] = list(self.kafka.keys())
        if self.postgres:
            result['postgres'] = list(self.postgres.keys())
        if self.mongo:
            result['mongo'] = list(self.mongo.keys())
        if self.http:
            result['http'] = list(self.http.keys())
        if self.redis:
            result['redis'] = list(self.redis.keys())
        if self.filesystem:
            result['filesystem'] = list(self.filesystem.keys())
        return result


class DLQConfig(BaseModel):
    """Dead letter queue configuration.

    Failed sink deliveries are written to this Kafka topic.
    If `topic` is empty, defaults to `{source_topic}_dlq` at runtime.
    If `brokers` is empty, inherits from `kafka.brokers`.
    """

    topic: str = ''
    brokers: str = ''


# --- Non-sink config models ---


class ExecutorConfig(BaseModel):
    """Subprocess executor pool settings.

    ``binary_path`` is optional here — if omitted, each ``ExecutorTask``
    must provide its own ``binary_path`` in ``arrange()``, otherwise the
    task will fail with a clear error.
    """

    binary_path: str | None = Field(default=None, min_length=1)
    max_executors: int = Field(default=4, ge=1)
    task_timeout_seconds: int = Field(default=120, ge=1)
    window_size: int = Field(default=100, ge=1)
    max_retries: int = Field(default=3, ge=0)
    drain_timeout_seconds: int = Field(default=5, ge=1)
    backpressure_high_multiplier: int = Field(default=32, ge=1)
    backpressure_low_multiplier: int = Field(default=4, ge=1)


class MetricsConfig(BaseModel):
    """Prometheus metrics settings."""

    enabled: bool = True
    port: int = Field(default=9090, ge=1, le=65535)


class LoggingConfig(BaseModel):
    """Structured logging settings."""

    level: str = 'INFO'
    format: str = Field(default='json', pattern='^(json|console)$')
    output: str = Field(
        default='stderr',
        description=(
            'Log output destination. "stderr" (default) or "stdout" for standard streams, '
            'or a file path for file output. File paths support template variables: '
            '{worker_id}, {cluster_name}. Example: "/var/log/drakkar/{worker_id}.log"'
        ),
    )


class DebugConfig(BaseModel):
    """Debug flight recorder and web UI settings.

    Set ``enabled: false`` to disable the entire debug feature.
    Set ``db_dir: ""`` to run without any SQLite files on disk.

    Granular persistence flags (all require ``db_dir`` to be set):
    - ``store_events``: write processing events to the ``events`` table.
    - ``store_config``: write worker config to ``worker_config`` (enables autodiscovery).
    - ``store_state``: periodically dump counters to ``worker_state``.

    Any combination is valid — e.g. ``store_config=true`` with everything
    else ``false`` gives autodiscovery without event or state logging.
    """

    enabled: bool = True
    host: str = Field(
        default='127.0.0.1',
        description='Bind address for the debug server. Use 0.0.0.0 to expose on all interfaces.',
    )
    port: int = Field(default=8080, ge=1, le=65535)
    auth_token: str = Field(
        default='',
        description=(
            'Bearer token for sensitive debug endpoints (database download, merge). '
            'When empty, no authentication is required. When set, protected endpoints '
            'require an Authorization: Bearer <token> header or ?token=<token> query parameter.'
        ),
    )
    debug_url: str = ''
    db_dir: str = '/tmp'
    store_events: bool = True
    store_config: bool = True
    store_state: bool = True
    state_sync_interval_seconds: int = Field(default=10, ge=1)
    expose_env_vars: list[str] = Field(default_factory=list)
    rotation_interval_minutes: int = Field(default=60, ge=1)
    retention_hours: int = Field(default=24, ge=1)
    retention_max_events: int = Field(default=100_000, ge=100)
    store_output: bool = True
    flush_interval_seconds: int = Field(default=5, ge=1)
    max_buffer: int = Field(default=50_000, ge=1000)
    max_ui_rows: int = Field(default=5000, ge=100)
    log_min_duration_ms: int = Field(default=500, ge=0)
    ws_min_duration_ms: int = Field(default=500, ge=0)
    event_min_duration_ms: int = Field(default=0, ge=0)
    output_min_duration_ms: int = Field(default=500, ge=0)
    prometheus_url: str = ''
    prometheus_rate_interval: str = '5m'
    prometheus_worker_label: str = ''
    prometheus_cluster_label: str = ''
    custom_links: list[dict[str, str]] = Field(default_factory=list)


# --- Root config ---


class DrakkarConfig(BaseSettings):
    """Root configuration for a Drakkar worker.

    Combines Kafka source settings, executor pool settings,
    sink definitions, and operational configs (metrics, logging, debug).
    """

    model_config = SettingsConfigDict(
        env_prefix='DRAKKAR_',
        env_nested_delimiter='__',
    )

    worker_name_env: str = Field(
        default='WORKER_ID',
        description='Environment variable that holds the worker name for logs, metrics, and UI',
    )
    cluster_name: str = Field(
        default='',
        description='Logical cluster name for grouping workers in the debug UI',
    )
    cluster_name_env: str = Field(
        default='',
        description='Environment variable that holds the cluster name (overrides cluster_name if set)',
    )
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    executor: ExecutorConfig = Field(default_factory=ExecutorConfig)
    sinks: SinksConfig = Field(default_factory=SinksConfig)
    dlq: DLQConfig = Field(default_factory=DLQConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    debug: DebugConfig = Field(default_factory=DebugConfig)

    def config_summary(self, worker_id: str = '', cluster_name: str = '') -> str:
        """One-line human-readable config summary for startup logging and debug UI.

        Format (Option C — structured-but-readable):
        [worker/cluster] topic=... group=... exec=4w/100win/100poll retries=3/120s debug=on metrics=9090 dlq=on sinks=[kafka:a,b pg:main] log=INFO
        """
        identity = worker_id or '?'
        if cluster_name:
            identity = f'{identity}/{cluster_name}'

        ex = self.executor
        exec_part = f'{ex.max_executors}w/{ex.window_size}win/{self.kafka.max_poll_records}poll'
        retries_part = f'{ex.max_retries}/{ex.task_timeout_seconds}s'

        debug_part = f'on:{self.debug.port}' if self.debug.enabled else 'off'
        metrics_part = str(self.metrics.port) if self.metrics.enabled else 'off'

        dlq_topic = self.dlq.topic or f'{self.kafka.source_topic}_dlq'
        dlq_part = dlq_topic if self.dlq.topic else 'on'

        sink_parts: list[str] = []
        abbrevs = {
            'kafka': 'kf',
            'postgres': 'pg',
            'mongo': 'mg',
            'http': 'http',
            'redis': 'rd',
            'filesystem': 'fs',
        }
        for sink_type, names in self.sinks.summary().items():
            abbr = abbrevs.get(sink_type, sink_type)
            sink_parts.append(f'{abbr}:{",".join(names)}')
        sinks_str = ' '.join(sink_parts) if sink_parts else 'none'

        return (
            f'[{identity}]'
            f' topic={self.kafka.source_topic}'
            f' group={self.kafka.consumer_group}'
            f' exec={exec_part}'
            f' retries={retries_part}'
            f' debug={debug_part}'
            f' metrics={metrics_part}'
            f' dlq={dlq_part}'
            f' sinks=[{sinks_str}]'
            f' log={self.logging.level}'
        )


def load_config(config_path: str | Path | None = None) -> DrakkarConfig:
    """Load configuration from YAML file and environment variables.

    YAML file path is resolved in order:
    1. Explicit config_path argument
    2. DRAKKAR_CONFIG environment variable
    3. Falls back to env-only config

    Environment variables override YAML values. Use DRAKKAR_ prefix
    with __ for nesting (e.g., DRAKKAR_KAFKA__BROKERS).
    """
    if config_path is None:
        config_path = os.environ.get('DRAKKAR_CONFIG')

    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')

        with open(path) as f:
            yaml_data = yaml.safe_load(f) or {}

        # pydantic-settings ignores env vars for nested models when init
        # kwargs are passed. Fix: extract DRAKKAR_* env vars, parse them
        # into nested structure, and deep-merge on top of YAML.
        env_overrides = _parse_env_overrides('DRAKKAR_', '__')
        merged = _deep_merge(yaml_data, env_overrides)
        return DrakkarConfig(**merged)

    env_overrides = _parse_env_overrides('DRAKKAR_', '__')
    return DrakkarConfig(**env_overrides)


def _parse_env_overrides(prefix: str, delimiter: str) -> dict:
    """Extract env vars with prefix, split by delimiter into nested dict."""
    result: dict = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        # skip the config file path env var itself
        if key == f'{prefix}CONFIG':
            continue
        parts = key[len(prefix) :].lower().split(delimiter)
        d = result
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = value
    return result


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep-merge override on top of base. Override wins for leaf values."""
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result
