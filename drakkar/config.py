"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DRAKKAR_ prefix with __ for nesting (e.g., DRAKKAR_KAFKA__BROKERS).
"""

import os
from pathlib import Path
from urllib.parse import urlparse

import structlog
import yaml
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Module-scope logger for config-time warnings (field/model validators).
# These fire once per process at config load, so the sync structlog API
# is fine — no coroutine context to await in.
logger = structlog.get_logger()

# Hosts that must never be the target of an operator-configured HTTP sink.
# These are cloud metadata endpoints which, if accessible, return IAM creds
# and other secrets. No legitimate pipeline writes there, so block at config
# time rather than relying on network-level egress policy alone.
_HTTP_BLOCKED_METADATA_HOSTS = frozenset(
    {
        '169.254.169.254',  # AWS, Azure, OpenStack, Alibaba, GCP IMDSv1/v2
        '100.100.100.200',  # Alibaba Cloud metadata
        '192.0.0.192',  # Oracle Cloud metadata
        'metadata.google.internal',
        'metadata.packet.net',
    }
)

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
    # Kafka-UI (https://github.com/provectus/kafka-ui) deep-link config.
    # When both fields are set, the debug UI renders a small Kafka icon
    # next to every <partition:offset> display; the icon opens Kafka-UI
    # filtered on (source_topic, partition, offset) in a new tab.
    # Both must be set for the icon to appear; empty values disable the
    # feature silently.
    ui_url: str = ''
    ui_cluster_name: str = ''

    # Staggered startup: delay the Kafka subscribe until the next wall-clock
    # alignment boundary. During a rolling deploy, workers come up one at
    # a time and each triggers a Kafka consumer-group rebalance — which
    # stalls consumption on all other workers. Aligning startup to shared
    # boundaries lets a fleet converge on a single rebalance instead of N.
    #
    # Sequence: wait ``startup_min_wait_seconds`` (buffer for slow init),
    # then sleep until the next wall-clock instant whose Unix-epoch seconds
    # are a multiple of ``startup_align_interval_seconds`` (default :00,
    # :10, :20, :30, :40, :50 of every minute in UTC — which maps 1:1 to
    # local wall-clock seconds since timezone offsets are whole-minute).
    # Disable with ``startup_align_enabled=false`` for snappy dev iteration.
    startup_align_enabled: bool = True
    startup_min_wait_seconds: float = Field(default=4.0, ge=0.0)
    startup_align_interval_seconds: int = Field(default=10, ge=1)


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

    SSRF note: the URL is operator-configured (YAML/env), never drawn
    from message content. Validation here protects against typos and
    obvious mistakes (unsupported scheme, missing host) and refuses to
    target cloud metadata endpoints where accidentally pointing the sink
    would leak cloud IAM credentials.
    """

    url: str
    method: str = 'POST'
    timeout_seconds: int = Field(default=30, ge=1)
    headers: dict[str, str] = Field(default_factory=dict)
    max_retries: int = Field(default=3, ge=0)
    ui_url: str = ''

    @field_validator('url')
    @classmethod
    def _validate_url(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme not in ('http', 'https'):
            raise ValueError(f'HTTP sink url must use http:// or https:// scheme, got {parsed.scheme!r}')
        host = (parsed.hostname or '').lower()
        if not host:
            raise ValueError('HTTP sink url must include a host')
        if host in _HTTP_BLOCKED_METADATA_HOSTS:
            raise ValueError(
                f'HTTP sink url host {host!r} is a cloud metadata endpoint — '
                'refusing to configure. POSTing there can leak IAM credentials. '
                'If this is intentional, update _HTTP_BLOCKED_METADATA_HOSTS.'
            )
        return v


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


class CircuitBreakerConfig(BaseModel):
    """Per-sink circuit breaker settings — shared default across all sinks.

    The breaker guards a sink from burning pool slots when its downstream
    is down. After ``failure_threshold`` consecutive failed deliveries the
    circuit trips open — subsequent deliveries are NOT attempted, they
    route straight to DLQ. After ``cooldown_seconds`` elapse the next
    delivery is allowed through as a half-open probe; success closes the
    circuit, failure reopens with a renewed cooldown.

    This default applies uniformly to every configured sink. Per-sink
    overrides can be added later — for v1 a single global default is
    enough for operational resilience under a downstream outage.
    """

    failure_threshold: int = Field(
        default=5,
        ge=1,
        description=(
            'Consecutive delivery failures before the circuit trips open. '
            '5 = trip on the 5th consecutive failure (4 prior failures did '
            'not trip, the 5th did). Intermittent failures with any success '
            'in between do not accumulate.'
        ),
    )
    cooldown_seconds: float = Field(
        default=30.0,
        ge=0.0,
        description=(
            'Time the circuit stays fully open before the next delivery is '
            'allowed through as a half-open probe. While open, all deliveries '
            'route directly to DLQ — no attempt is made on the sink.'
        ),
    )


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
    circuit_breaker: CircuitBreakerConfig = Field(
        default_factory=CircuitBreakerConfig,
        description=(
            'Default circuit breaker config applied to every configured sink. '
            'Per-sink overrides are not supported in v1 — if a sink needs a '
            'different threshold/cooldown, adjust this default or add a '
            'per-sink override in a future release.'
        ),
    )

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
    env: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Environment variables passed to all executor subprocesses. '
            'Merged on top of the (filtered) parent process environment. '
            'Per-task env vars from ExecutorTask.env override these on conflict.'
        ),
    )
    env_inherit_parent: bool = Field(
        default=True,
        description=(
            'When True, the parent process env is passed to subprocesses '
            '(with deny patterns applied — see env_inherit_deny). Set False '
            'to run subprocesses with ONLY ExecutorConfig.env + '
            'ExecutorTask.env — fully isolated from parent env.'
        ),
    )
    env_inherit_deny: list[str] = Field(
        default_factory=lambda: [
            'DRAKKAR_*',  # framework internals (SINKS__, KAFKA__, DEBUG__, ...)
            '*PASSWORD*',
            '*SECRET*',
            '*TOKEN*',
            '*_KEY',
            '*_DSN',
            '*CREDENTIAL*',
        ],
        description=(
            'Case-insensitive fnmatch patterns against parent env var names. '
            'Matching vars are NOT inherited by subprocesses, even when '
            'env_inherit_parent is True. Default excludes DRAKKAR_* internals '
            'and common secret names so handler-configured secrets do not '
            'leak to executor binaries. Set to [] to fully trust the parent '
            'environment.'
        ),
    )
    max_executors: int = Field(default=4, ge=1)
    task_timeout_seconds: int = Field(default=120, ge=1)
    window_size: int = Field(default=100, ge=1)
    max_retries: int = Field(default=3, ge=0)
    drain_timeout_seconds: int = Field(
        default=30,
        ge=1,
        description=(
            'Maximum seconds to wait for in-flight executor tasks to finish '
            'during graceful shutdown or partition revocation. Set lower to '
            'speed up shutdown; set at least as high as task_timeout_seconds '
            'if you rely on clean final commits for every in-flight task. '
            'When drain times out, offsets for in-flight tasks are NOT '
            'committed (those messages will replay on restart — at-least-once).'
        ),
    )
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
            'Bearer token for the debug UI. **Empty (the default) disables auth** '
            'entirely — every endpoint (including database download, merge, and '
            'message-probe) is reachable without credentials and the WebSocket '
            'live-event stream skips both token and Origin checks. This is a '
            'deliberate opt-in design: the UI is read-only (no endpoint stops a '
            'worker, replays Kafka messages, mutates sinks, or fakes pipeline '
            'data) and Drakkar is intended for deployment inside a private '
            'contour (VPC / internal cluster / operator-only ingress). A startup '
            'warning fires whenever debug is enabled without a token so the '
            'unauthenticated posture is visible in logs. '
            'When set to a non-empty value, protected HTTP endpoints require '
            'an ``Authorization: Bearer <token>`` header or ``?token=<token>`` '
            'query parameter; WebSocket connections without a valid token are '
            'closed with code 4401, and the Origin header is validated against '
            '``allowed_ws_origins`` (or the request Host header if that list is '
            'empty). Comparison uses ``secrets.compare_digest`` to avoid timing '
            'side-channels. Trailing/leading whitespace is stripped on load to '
            'avoid silent mismatches when YAML accidentally quotes spaces.'
        ),
    )
    allowed_ws_origins: list[str] = Field(
        default_factory=list,
        description=(
            'Explicit allowlist of WebSocket origins. Empty list with non-empty '
            'auth_token defaults to same-origin only; empty list with empty '
            'auth_token = no origin check (dev workflow preserved).'
        ),
    )

    @field_validator('auth_token', mode='before')
    @classmethod
    def _strip_auth_token(cls, v: object) -> object:
        """Strip leading/trailing whitespace from ``auth_token`` on load.

        Operators sometimes write ``auth_token: " secret "`` in YAML (quoted
        to preserve a trailing space, by accident). With the raw value kept,
        ``secrets.compare_digest`` would require clients to send the literal
        space-padded string — a footgun. We strip once here so the stored
        value is the canonical token; the startup security gate and the
        ``_token_matches`` helper both see the same post-strip value.
        """
        if isinstance(v, str):
            return v.strip()
        return v

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
    # Maximum consecutive ``OperationalError`` failures tolerated on a single
    # batch before the recorder gives up and drops it. On each failure the
    # batch is re-queued at the front of the buffer so the next flush tick
    # retries it; after this many attempts the batch is discarded and the
    # ``drakkar_recorder_flush_batches_dropped_total`` counter ticks. Default
    # 3 matches the cache engine's retry budget and keeps a persistent DB
    # outage from leaking the buffer indefinitely.
    max_flush_retries: int = Field(default=3, ge=1)
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


# --- Cache config ---


class CachePeerSyncConfig(BaseModel):
    """Peer-sync settings for the handler cache.

    Controls the periodic loop that pulls entries from sibling workers' cache
    DBs. Disable by setting ``enabled=false`` — flush/cleanup continue to run,
    but no cross-worker propagation happens.

    ``peer_resolution_cache_seconds`` is intentionally NOT exposed as a knob
    in v1 (hardcoded 300s). Operators rarely change cluster_name at runtime,
    so exposing it would add config surface without real benefit. YAGNI —
    expose later if someone actually needs it.
    """

    enabled: bool = True
    interval_seconds: float = Field(default=30.0, gt=0)
    batch_size: int = Field(default=500, ge=1)
    timeout_seconds: float = Field(default=5.0, gt=0)
    # ``cycle_deadline_seconds`` is a hard wall-clock cap on one peer-sync
    # cycle. Without this bound, a single slow peer (NFS lag, disk
    # contention, unresponsive remote) could keep ``_sync_once`` in flight
    # indefinitely and starve the periodic task. ``None`` => default to
    # ``interval_seconds * 0.9`` so the deadline is always strictly less
    # than the gap between invocations. Minimum floor of 0.1s prevents
    # operators from accidentally setting a value so tight that normal
    # syncs can never complete.
    cycle_deadline_seconds: float | None = Field(
        default=None,
        ge=0.1,
        description=(
            'Hard wall-clock cap on one peer-sync cycle. When None, defaults '
            'to interval_seconds * 0.9. Prevents a single slow peer from '
            'starving the periodic task.'
        ),
    )

    @model_validator(mode='after')
    def _validate_deadline_vs_interval(self) -> 'CachePeerSyncConfig':
        """Reject explicit ``cycle_deadline_seconds >= interval_seconds``.

        The deadline's whole point is to cap a single cycle short of the next
        tick so the periodic task never overlaps itself. If the operator
        configures a deadline that's greater than or equal to the interval,
        the cap can't fire before the next invocation schedules — in that
        case the operator probably misread the fields or intended a much
        larger interval. Fail loudly at config load so the misconfiguration
        surfaces before any data flows.
        """
        if self.cycle_deadline_seconds is not None and self.cycle_deadline_seconds >= self.interval_seconds:
            raise ValueError(
                f'cache.peer_sync.cycle_deadline_seconds ({self.cycle_deadline_seconds}) must be '
                f'strictly less than cache.peer_sync.interval_seconds ({self.interval_seconds}) — '
                'otherwise the deadline can never fire before the next cycle schedules.'
            )
        return self


class CacheConfig(BaseModel):
    """Handler-accessible key/value cache, memory-backed with write-behind SQLite.

    When ``enabled=true``, every handler gains a ``self.cache`` attribute for
    sync ``set``/``peek``/``delete``/``__contains__`` and async ``get`` with
    DB fallback. Entries are periodically flushed to ``<worker>-cache.db``
    under ``db_dir`` (falls back to ``debug.db_dir`` when empty) and
    optionally pulled from sibling workers via the peer-sync loop.

    ``max_memory_entries`` defaults to ``10_000`` to prevent unbounded
    growth under write-heavy workloads; the in-memory dict uses LRU
    eviction and falls through to the DB on miss — the DB is the source
    of truth, so eviction never loses data. Explicit ``None`` disables
    the cap (unbounded cache); the engine emits a warning at startup so
    that choice is visible in logs.

    Gating rules (warn-and-continue, not fail-at-startup):
    - ``enabled=true`` but no ``db_dir`` anywhere → warning + effective-disable
    - ``peer_sync.enabled=true`` but ``debug.store_config=false`` → peer sync
      silently disabled (autodiscovery needs ``store_config``)
    """

    enabled: bool = False
    # empty → engine init falls back to debug.db_dir. Kept empty in config layer
    # so the config is pure data — resolution happens when the engine spins up.
    db_dir: str = ''
    flush_interval_seconds: float = Field(default=3.0, gt=0)
    cleanup_interval_seconds: float = Field(default=60.0, gt=0)
    # Cap for in-memory LRU entries. Default 10_000 prevents unbounded growth
    # under write-heavy workloads. Set to None for explicitly unbounded cache;
    # the engine will warn at startup so operators see the intentional choice.
    max_memory_entries: int | None = Field(
        default=10_000,
        ge=1,
        description=(
            'Cap for in-memory LRU entries. Default 10_000 prevents unbounded '
            'growth under write-heavy workloads. Set to None for explicitly '
            'unbounded cache.'
        ),
    )
    peer_sync: CachePeerSyncConfig = Field(default_factory=CachePeerSyncConfig)

    @model_validator(mode='after')
    def _warn_if_unbounded(self) -> 'CacheConfig':
        """Emit a startup warning when ``max_memory_entries`` is explicitly unbounded.

        Fires once at config load rather than inside ``CacheEngine.start()`` —
        the engine's ``start`` can be called multiple times in tests and on
        rotation, and the worker-id context the engine had is irrelevant for
        a choice that lives in the config itself. We only warn when the cache
        is actually enabled (otherwise the setting has no effect).
        """
        if self.enabled and self.max_memory_entries is None:
            logger.warning(
                'cache_max_memory_entries_unbounded',
                category='cache',
                reason='cache.max_memory_entries=None configured — memory is unbounded, monitor RSS under load',
            )
        return self


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
    cache: CacheConfig = Field(default_factory=CacheConfig)

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

        # Cache summary: 'off' when disabled; otherwise 'on:f=Ns/s=Ns|off/c=Ns[/max=N]'.
        # :g format trims trailing zeros on integer-valued floats (3.0 → '3'), keeping
        # the common case compact while still rendering fractional intervals readably.
        if not self.cache.enabled:
            cache_part = 'off'
        else:
            flush = f'{self.cache.flush_interval_seconds:g}s'
            sync = f'{self.cache.peer_sync.interval_seconds:g}s' if self.cache.peer_sync.enabled else 'off'
            cleanup = f'{self.cache.cleanup_interval_seconds:g}s'
            cache_tokens = [f'f={flush}', f's={sync}', f'c={cleanup}']
            if self.cache.max_memory_entries is not None:
                cache_tokens.append(f'max={self.cache.max_memory_entries}')
            cache_part = 'on:' + '/'.join(cache_tokens)

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
            f' cache={cache_part}'
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
