"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DK_ prefix with __ for nesting (e.g., DK_KAFKA__BROKERS).
"""

import os
from pathlib import Path
from typing import Any, Literal
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

    # Policy for source messages whose value fails input_model parsing:
    #   - 'skip'  — message flows to arrange() with payload=None (and
    #               msg.parse_error set); the handler decides what to do.
    #               Matches pre-policy behavior, now logged + counted.
    #   - 'dlq'   — message is excluded from arrange(), a ParseFailurePayload
    #               is written to the DLQ topic, and the offset commits once
    #               the DLQ write is confirmed. A failed DLQ write stalls the
    #               offset (redelivery on restart) instead of losing data.
    #   - 'raise' — fail fast: a MessageParseError propagates and stops the
    #               partition processor. Use when a parse failure means the
    #               deployment is broken (schema mismatch) and processing
    #               must not continue past it.
    on_parse_error: Literal['skip', 'dlq', 'raise'] = 'skip'

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
    # Plugin-discovered (entry-point-registered) sink instances. Maps a
    # sink type name (e.g. 'my_custom') to a dict of named instances:
    # ``{ 'instance-1': { ...config... }, 'instance-2': { ... } }``.
    # ``DrakkarApp._build_sinks`` consults ``SinkRegistry.get(type_name)``
    # for each top-level key; unknown names raise at startup so a typo
    # in YAML fails loud rather than silently dropping a sink.
    #
    # The instance config is a free-form ``dict[str, Any]`` because
    # plugin authors define the shape — Drakkar cannot validate it
    # ahead of time. The plugin's ``__init__(name, config)`` receives
    # the dict and can wrap it in its own Pydantic model for validation.
    custom: dict[str, dict[str, dict[str, Any]]] = Field(
        default_factory=dict,
        description=(
            'Plugin-discovered sink instances. Top-level keys are sink type '
            'names registered via [project.entry-points."drakkar.sinks"]; '
            'second-level keys are instance names; leaf dicts are plugin-'
            'defined config passed verbatim to the sink class constructor.'
        ),
    )
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
        return not any([self.kafka, self.postgres, self.mongo, self.http, self.redis, self.filesystem, self.custom])

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
        for type_name, instances in self.custom.items():
            if instances:
                result[type_name] = list(instances.keys())
        return result


class DLQConfig(BaseModel):
    """Dead letter queue configuration.

    Failed sink deliveries are written to this Kafka topic.
    If `topic` is empty, defaults to `{source_topic}_dlq` at runtime.
    If `brokers` is empty, inherits from `kafka.brokers`.
    """

    topic: str = ''
    brokers: str = ''

    # Strategy when the DLQ write itself fails (or no DLQ sink exists) after
    # a sink delivery already failed — the payloads have nowhere safe to go:
    #   - 'drop'  — log CRITICAL, tick drakkar_dlq_dropped_payloads_total,
    #               count the message as processed and commit its offset.
    #               The pipeline keeps moving; the payloads are lost (default).
    #   - 'stall' — do NOT commit the affected offset and pause the partition
    #               (stop fetching new messages from it). The watermark stalls
    #               and the messages are redelivered after restart/rebalance.
    #               Bounds data loss at the cost of consumer lag.
    # Either way the failure is loud: alert on
    # drakkar_dlq_send_failures_total and (for 'stall')
    # drakkar_delivery_stalled_offsets_total.
    on_send_failure: Literal['drop', 'stall'] = 'drop'


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
            'DK_*',  # framework internals (SINKS__, KAFKA__, DEBUG__, ...)
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
            'env_inherit_parent is True. Default excludes DK_* internals '
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


# --- UI hosting config ---


class UIConfig(BaseModel):
    """Decoupled drakkar-ui single-page-app hosting settings.

    The UI ships as its own versioned bundle (the separate drakkar-ui repo,
    published to GitHub Releases) so every backend on a host serves the same
    UI and looks identical. When ``enabled``, the worker resolves that bundle
    through :mod:`drakkar.uihost` (cache → fetch) and serves it in place of
    the built-in server-rendered HTML pages.

    Default-ON with an update check: on startup the worker resolves the
    latest release (or serves the shared cache) and falls back to the
    built-in Jinja pages when nothing is fetchable and the cache is empty —
    a fetch failure is never fatal, so the default is safe offline too.

    The YAML keys and semantics match the Go backend's ``ui`` config block,
    so ``DK_UI__*`` env overrides behave identically on both backends.
    """

    enabled: bool = True
    release_repo: str = Field(
        default='wlame/drakkar-ui',
        description=(
            'The "owner/name" GitHub repo that publishes UI bundles. '
            'Empty disables fetching — only a cached bundle or the embedded '
            'fallback is served.'
        ),
    )
    pinned_version: str = Field(
        default='',
        description=(
            'Known-good UI release tag this backend is built against '
            '(e.g. "v1.2.0"); the contract is API-major compatible. Empty '
            'means "no pinned version".'
        ),
    )
    cache_dir: str = Field(
        default='',
        description=(
            'Bundle cache root override. Empty uses the per-user cache dir '
            '($XDG_CACHE_HOME/drakkar/ui, falling back to ~/.cache/drakkar/ui '
            "— the same directory the Go backend's os.UserCacheDir produces "
            'on Linux, so both backends share one cache).'
        ),
    )
    check_update: bool = Field(
        default=True,
        description=(
            'Resolve the latest release tag on startup instead of only the '
            'pinned version (the "check for a new version" toggle). Already-'
            'cached versions are never re-downloaded — release tags are '
            'immutable.'
        ),
    )

    @field_validator('release_repo')
    @classmethod
    def _validate_release_repo(cls, v: str) -> str:
        """A non-empty repo must look like a GitHub ``owner/name`` slug."""
        if v and '/' not in v:
            raise ValueError(f'ui.release_repo must be "owner/name", got {v!r}')
        return v


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


# --- Webapp config ---


class WebClientConfig(BaseModel):
    """Configuration for a single webapp client (tenant).

    Each client has a name (used in metrics labels and recorder rows), an
    optional bearer token (empty string = anonymous matching for requests
    without an Authorization header), and a per-client rpm cap enforced by
    a sliding-window rate limiter on the webapp side.

    Validation rules at the WebAppConfig level:
    - At most one client may have an empty token (anonymous slot).
    - All non-empty tokens must be unique across clients.
    - rpm must be > 0 for every client.
    """

    name: str
    token: str = ''
    rpm: int = 4

    @field_validator('name')
    @classmethod
    def _validate_name_non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('webapp client name must be a non-empty string')
        return v


class WebAppConfig(BaseModel):
    """Configuration for the optional synchronous-HTTP webapp pipeline.

    When ``enabled=true``, Drakkar starts a FastAPI server on its own thread
    accepting POST requests and routing them through the same handler
    pipeline as Kafka messages. Defaults are tuned for a small dev
    deployment with one anonymous client; multi-tenant production
    deployments should configure named clients with non-empty tokens.

    Per-request flow:
    - Auth (token match) → rate-limit (per-client rpm) → dispatch to main
      loop → user's ``arrange_http_request`` → executor pool → user's
      ``on_http_request_complete`` → JSON response.

    See ``docs/webapp.md`` (added in Task 10) for the full feature guide.
    """

    enabled: bool = False
    host: str = '0.0.0.0'
    port: int = 8090
    path: str = '/process'
    sinks_enabled: bool = False
    request_timeout_seconds: float = 30.0
    max_concurrent: int = 64
    clients: list[WebClientConfig] = Field(
        default_factory=lambda: [WebClientConfig(name='anonymous', token='', rpm=4)],
        description=(
            'List of webapp clients (tenants). Defaults to a single '
            'anonymous client with empty token and rpm=4 so the webapp '
            'works out of the box for development. Production deployments '
            'should configure named clients with non-empty tokens.'
        ),
    )

    @model_validator(mode='after')
    def _validate_webapp(self) -> 'WebAppConfig':
        """Enforce webapp config invariants.

        These rules are checked at config load time so misconfigurations
        surface before any request lands. Each error message names the
        offending field/client so operators can find and fix the problem.
        """
        # path must start with '/' and not be just '/' (need a real route).
        if not self.path.startswith('/') or len(self.path) <= 1:
            raise ValueError(f"webapp.path must start with '/' and have a non-empty route, got {self.path!r}")
        # request_timeout_seconds > 0 — a zero/negative timeout would
        # cancel every request before it had a chance to start.
        if self.request_timeout_seconds <= 0:
            raise ValueError(f'webapp.request_timeout_seconds must be > 0, got {self.request_timeout_seconds}')
        # max_concurrent > 0 — semaphore with zero capacity would block all
        # requests indefinitely.
        if self.max_concurrent <= 0:
            raise ValueError(f'webapp.max_concurrent must be > 0, got {self.max_concurrent}')
        # At least one client. The default factory ensures this for an
        # omitted ``clients`` block, but explicit ``clients: []`` in YAML
        # would otherwise silently give us a webapp that rejects every
        # request — fail loud instead.
        if len(self.clients) == 0:
            raise ValueError('webapp.clients must contain at least one client')
        # Per-client rpm > 0. Zero rpm means "always rate-limit", which
        # is almost certainly a typo.
        for client in self.clients:
            if client.rpm <= 0:
                raise ValueError(f'webapp client {client.name!r} has rpm={client.rpm}; rpm must be > 0')
        # At most one client with empty token (the anonymous slot).
        # Multiple empty-token clients can never be distinguished at the
        # auth layer, so we reject the ambiguity at config time.
        empty_token_clients = [c for c in self.clients if c.token == '']
        if len(empty_token_clients) > 1:
            names = ', '.join(repr(c.name) for c in empty_token_clients)
            raise ValueError(
                f'at most one webapp client may have an empty token (anonymous); '
                f'got {len(empty_token_clients)} empty-token clients: {names}'
            )
        # All non-empty tokens unique. Two clients sharing a token would
        # collide at the auth layer; the matched client_name would be
        # nondeterministic.
        seen_tokens: dict[str, str] = {}
        for client in self.clients:
            if client.token == '':
                continue
            if client.token in seen_tokens:
                raise ValueError(
                    f'webapp clients {seen_tokens[client.token]!r} and {client.name!r} '
                    f'share the same token; tokens must be unique across clients'
                )
            seen_tokens[client.token] = client.name
        return self


# --- Root config ---


class DrakkarConfig(BaseSettings):
    """Root configuration for a Drakkar worker.

    Combines Kafka source settings, executor pool settings,
    sink definitions, and operational configs (metrics, logging, debug).
    """

    model_config = SettingsConfigDict(
        env_prefix='DK_',
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
    ui: UIConfig = Field(default_factory=UIConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    webapp: WebAppConfig = Field(default_factory=WebAppConfig)

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
    2. DK_CONFIG environment variable
    3. Falls back to env-only config

    Environment variables override YAML values. Use DK_ prefix
    with __ for nesting (e.g., DK_KAFKA__BROKERS).
    """
    if config_path is None:
        config_path = os.environ.get('DK_CONFIG')

    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')

        with open(path) as f:
            yaml_data = yaml.safe_load(f) or {}

        # pydantic-settings ignores env vars for nested models when init
        # kwargs are passed. Fix: extract DK_* env vars, parse them
        # into nested structure, and deep-merge on top of YAML.
        env_overrides = _parse_env_overrides('DK_', '__')
        merged = _deep_merge(yaml_data, env_overrides)
        # Top-level result is always a dict (the env-var prefix is fixed
        # and never numeric); the assert satisfies static typing without a
        # runtime branch.
        assert isinstance(merged, dict)
        merged = _apply_list_field_defaults(merged)
        return DrakkarConfig(**merged)

    env_overrides = _parse_env_overrides('DK_', '__')
    env_overrides = _apply_list_field_defaults(env_overrides)
    return DrakkarConfig(**env_overrides)


def _apply_list_field_defaults(merged: dict) -> dict:
    """Ensure list-of-objects env-var overrides do not erase default entries.

    When env-vars target individual list elements (e.g.
    ``DK_WEBAPP__CLIENTS__0__RPM=10``), the parser produces a partial list
    like ``[{'rpm': '10'}]`` with no other fields. If the YAML did not
    supply ``webapp.clients`` at all, Pydantic would now see this partial
    list as the entire value and reject it for missing required fields
    (``name``). To preserve the documented behaviour — env-vars override
    individual fields without forcing operators to repeat the defaults —
    we deep-merge the default ``WebAppConfig`` clients list under the
    partial override before construction.

    This is intentionally narrow (only ``webapp.clients`` for now). If
    another list-of-objects field needs the same treatment later, add it
    here with a small helper rather than introducing a generic mechanism.
    """
    webapp = merged.get('webapp')
    if not isinstance(webapp, dict):
        return merged
    clients_override = webapp.get('clients')
    if not isinstance(clients_override, list):
        return merged
    # Build the default clients list from the WebAppConfig default factory
    # and overlay the env-var override on top. We do this by dumping a
    # fresh WebAppConfig() to dict form so we are guaranteed to track any
    # future changes to the default list.
    default_clients = [c.model_dump() for c in WebAppConfig().clients]
    merged_clients = _deep_merge(default_clients, clients_override)
    new_webapp = dict(webapp)
    new_webapp['clients'] = merged_clients
    new_merged = dict(merged)
    new_merged['webapp'] = new_webapp
    return new_merged


def _parse_env_overrides(prefix: str, delimiter: str) -> dict:
    """Extract env vars with prefix, split by delimiter into nested dict.

    Numeric path segments are detected and the surrounding dict is
    converted to a list (e.g. ``DK_WEBAPP__CLIENTS__0__RPM=10`` becomes
    ``{'webapp': {'clients': [{'rpm': '10'}]}}``). This lets list-of-objects
    config fields (like ``webapp.clients``) be overridden by env vars in
    the same nested-delimiter style as scalar fields.
    """
    result: dict[str, Any] = {}
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
    # Convert numeric-keyed nested dicts to lists. The top-level result
    # is always a dict (top-level prefix segments are never numeric), so
    # the cast is safe.
    converted_result = _numeric_dicts_to_lists(result)
    assert isinstance(converted_result, dict)
    return converted_result


def _numeric_dicts_to_lists(node: Any) -> Any:
    """Recursively convert dicts with all-numeric string keys to lists.

    A dict like ``{'0': {...}, '2': {...}}`` represents a sparse list with
    indices 0 and 2. We materialise it as ``[{...}, {}, {...}]`` (filling
    gaps with empty dicts) so Pydantic can validate the surrounding model
    and so ``_deep_merge`` can later overlay it onto a YAML-supplied list
    by index.
    """
    if isinstance(node, dict):
        # Recurse into values first so nested numeric-keyed dicts are
        # converted before we decide whether to convert the parent.
        converted: dict[str, Any] = {k: _numeric_dicts_to_lists(v) for k, v in node.items()}
        if converted and all(isinstance(k, str) and k.isdigit() for k in converted):
            max_index = max(int(k) for k in converted)
            result_list: list[Any] = [{} for _ in range(max_index + 1)]
            for k, v in converted.items():
                result_list[int(k)] = v
            return result_list
        return converted
    return node


def _deep_merge(base: Any, override: Any) -> Any:
    """Deep-merge override on top of base. Override wins for leaf values.

    When both sides are lists, merge element-by-element by index: the
    override's i-th element overrides base's i-th element (recursively
    if both are dicts), and any extra base elements past the override's
    length are preserved. This supports the env-var override pattern
    where ``DK_WEBAPP__CLIENTS__0__RPM=10`` should change only the first
    client's rpm without dropping the rest of the clients defined in YAML.
    """
    if isinstance(base, dict) and isinstance(override, dict):
        result_dict: dict[Any, Any] = dict(base)
        for key, val in override.items():
            if key in result_dict:
                result_dict[key] = _deep_merge(result_dict[key], val)
            else:
                result_dict[key] = val
        return result_dict
    if isinstance(base, list) and isinstance(override, list):
        merged_list: list[Any] = []
        for i in range(max(len(base), len(override))):
            if i < len(base) and i < len(override):
                merged_list.append(_deep_merge(base[i], override[i]))
            elif i < len(override):
                merged_list.append(override[i])
            else:
                merged_list.append(base[i])
        return merged_list
    # Leaf or type mismatch: override wins.
    return override
