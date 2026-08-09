"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DK_ prefix with __ for nesting (e.g., DK_KAFKA__BROKERS).
"""

import os
import re
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

import structlog
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from drakkar.kafka_security import KafkaSecurityConfig, validate_client_config

# Safe because ``drakkar.pgsql`` imports nothing from ``drakkar``. Importing
# the compiler from under ``drakkar/sinks/`` instead would execute
# ``sinks/__init__.py``, which imports every sink, each of which imports this
# module — a partially-initialized-module ImportError at config load.
from drakkar.models import MongoOp
from drakkar.mql import compile_template
from drakkar.pgsql import compile_named_statement

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

# Statement and script names appear in structured logs and error messages, so
# they are constrained to lowercase snake_case rather than accepting arbitrary
# keys. One pattern serves both escape hatches — the constraint is the same.
_PG_STATEMENT_NAME_RE = re.compile(r'^[a-z_][a-z0-9_]*$')
_MONGO_STATEMENT_NAME_RE = _PG_STATEMENT_NAME_RE
_REDIS_SCRIPT_NAME_RE = _PG_STATEMENT_NAME_RE

# --- Kafka source (consumer) config ---


class KafkaConfig(BaseModel):
    """Kafka connection and consumer settings."""

    brokers: str = 'localhost:9092'
    source_topic: str = 'input-events'
    consumer_group: str = 'drakkar-workers'

    # Transport security for the consumer. Also inherited by Kafka sinks and
    # the DLQ producer whose own ``brokers`` field is empty (same cluster =>
    # same credentials); see ``KafkaSinkConfig.security``.
    security: KafkaSecurityConfig = Field(default_factory=KafkaSecurityConfig)

    # Raw librdkafka properties merged after ``security``, for options the
    # typed block does not model. Reserved keys are rejected at load time —
    # see ``drakkar.kafka_security.RESERVED_CLIENT_KEYS``.
    client_config: dict[str, str] = Field(default_factory=dict)

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

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)


# --- Sink config models ---


class KafkaSinkConfig(BaseModel):
    """Configuration for a Kafka output sink.

    Each named instance produces messages to a specific topic.
    If `brokers` is empty, inherits from `kafka.brokers` (same cluster) —
    and, because it is the same cluster, inherits `kafka.security` and
    `kafka.client_config` along with it. Setting `brokers` here makes the
    sink self-contained: it then uses only its own security block, which
    defaults to PLAINTEXT.
    """

    topic: str
    brokers: str = ''
    ui_url: str = ''

    # Only consulted when ``brokers`` is set; an inheriting sink takes the
    # consumer's security instead. See ``resolve_kafka_client_settings``.
    security: KafkaSecurityConfig = Field(default_factory=KafkaSecurityConfig)
    client_config: dict[str, str] = Field(default_factory=dict)

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)


class PostgresSinkConfig(BaseModel):
    """Configuration for a PostgreSQL output sink.

    Each named instance connects to a database via asyncpg pool.
    """

    dsn: str = Field(
        description='PostgreSQL connection string (asyncpg-compatible), typically embedding user and password.',
        json_schema_extra={'drakkar_secret': True},
    )
    pool_min: int = Field(default=2, ge=1)
    pool_max: int = Field(default=10, ge=1)
    statements: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Operator-authored SQL keyed by name. A PostgresPayload with '
            "op='statement' and a matching `statement` name executes the entry "
            'with its `params` bound to the :name placeholders. This is the '
            'escape hatch for SQL the declarative payload fields cannot express '
            '— value-dependent expressions and guarded predicates. Keeping the '
            'SQL here rather than in the payload means message content can never '
            'reach the statement text.'
        ),
    )
    ui_url: str = ''

    @field_validator('statements')
    @classmethod
    def _validate_statements(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject malformed statement config at startup.

        Checks only what the framework owns: the name shape, non-empty SQL,
        and that the placeholder syntax compiles. Deliberately does NOT
        verify statements against the live database — ``PREPARE`` cannot
        separate "your SQL is malformed" from "that column does not exist",
        so validating there would couple worker startup to schema state.
        Schema problems surface at delivery through ``on_delivery_error``,
        exactly as they do for an INSERT naming a missing table today.
        """
        for name, sql in value.items():
            if not _PG_STATEMENT_NAME_RE.match(name):
                raise ValueError(
                    f'Invalid statement name {name!r}: must match '
                    f'{_PG_STATEMENT_NAME_RE.pattern} (used as a structured-log field)'
                )
            if not sql.strip():
                raise ValueError(f'Statement {name!r} has empty SQL')
            compile_named_statement(sql)
        return value


# Which ops an operator-authored statement may declare. Deliberately NOT
# every non-``statement`` op: the escape hatch exists for what the
# declarative tier cannot express, and an insert is fully expressible as
# ``MongoPayload(op='insert', collection=…, data=…)``. An insert template
# would also need a DOCUMENT rather than the ``update`` this model carries,
# so admitting it would ship a meaningless field combination.
_MONGO_STATEMENT_OPS = frozenset(
    {
        MongoOp.UPDATE_ONE,
        MongoOp.UPDATE_MANY,
        MongoOp.UPSERT,
        MongoOp.DELETE_ONE,
        MongoOp.DELETE_MANY,
    }
)

# The statement ops that write a document, and therefore need ``update``.
_MONGO_UPDATE_OPS = frozenset({MongoOp.UPDATE_ONE, MongoOp.UPDATE_MANY, MongoOp.UPSERT})


class MongoStatementConfig(BaseModel):
    """One operator-authored MQL statement.

    Unlike the Postgres and Redis escape hatches this is a MODEL rather than
    a string, because MQL is structured data: a statement carries its own
    collection, operation, filter, and update. Flattening that into a string
    would mean embedding JSON in YAML, which is strictly worse to author and
    to review.

    Values reach the document through ``":name"`` placeholders bound from the
    payload's ``params``, and never by interpolation — see ``drakkar.mql``
    for the four substitution rules.
    """

    collection: str = Field(description='Target MongoDB collection.')
    op: MongoOp = Field(description='Which write operation to perform.')
    filter: dict[str, Any] = Field(
        description='Equality-or-richer predicate selecting the documents to write. Must be non-empty.'
    )
    update: dict[str, Any] | list[dict[str, Any]] | None = Field(
        default=None,
        description=(
            'Update document for the update ops, absent for the delete ops. A LIST is an '
            "aggregation pipeline — MongoDB's own mechanism for computed updates."
        ),
    )

    def template(self) -> dict[str, Any]:
        """The whole statement as one document, for compilation.

        Compiling filter and update together means one set of parameter
        names spans both, which is what lets a payload bind ``:id`` once for
        a statement that matches on it and writes it.
        """
        document: dict[str, Any] = {'filter': self.filter}
        if self.update is not None:
            document['update'] = self.update
        return document


class MongoSinkConfig(BaseModel):
    """Configuration for a MongoDB output sink.

    Each named instance connects to a database via PyMongo's AsyncMongoClient.
    """

    uri: str = Field(
        description='MongoDB connection string, typically embedding user and password.',
        json_schema_extra={'drakkar_secret': True},
    )
    database: str
    statements: dict[str, MongoStatementConfig] = Field(
        default_factory=dict,
        description=(
            'Operator-authored MQL keyed by name. A MongoPayload with '
            "op='statement' and a matching `statement` name runs the entry with "
            'its `params` bound to the ":name" placeholders. This is the escape '
            'hatch for anything the declarative fields cannot express — $inc, '
            '$push, computed pipeline updates. Keeping the MQL here rather than '
            'in the payload means message content can never reach an operator '
            'position, where a value of {"$gt": ""} would match every document.'
        ),
    )
    ui_url: str = ''

    @field_validator('statements')
    @classmethod
    def _validate_statements(cls, value: dict[str, MongoStatementConfig]) -> dict[str, MongoStatementConfig]:
        """Reject malformed statement config at startup.

        Checks only what the framework owns: the name shape, the op, a
        non-empty collection and filter, update present exactly for the ops
        that write one, and that the template compiles — which is also where
        ``$where``/``$function`` are refused, at any depth including inside
        aggregation-pipeline stages.

        Deliberately does NOT verify statements against the live database.
        Distinguishing "your MQL is malformed" from "that field does not
        exist" would couple worker startup to database state, and
        ``MongoPayload(collection='does_not_exist')`` already fails at
        delivery rather than at startup.
        """
        for name, statement in value.items():
            if not _MONGO_STATEMENT_NAME_RE.match(name):
                raise ValueError(
                    f'Invalid statement name {name!r}: must match '
                    f'{_MONGO_STATEMENT_NAME_RE.pattern} (used as a structured-log field)'
                )
            if statement.op not in _MONGO_STATEMENT_OPS:
                allowed = ', '.join(sorted(op.value for op in _MONGO_STATEMENT_OPS))
                raise ValueError(f'Statement {name!r} has op {statement.op.value!r}; allowed ops are: {allowed}')
            if not statement.collection:
                raise ValueError(f'Statement {name!r} has an empty collection')
            if not statement.filter:
                raise ValueError(
                    f'Statement {name!r} has an empty filter, which matches EVERY document in the collection'
                )
            wants_update = statement.op in _MONGO_UPDATE_OPS
            if wants_update and statement.update is None:
                raise ValueError(f'Statement {name!r} with op {statement.op.value!r} requires an update')
            if not wants_update and statement.update is not None:
                raise ValueError(f'Statement {name!r} with op {statement.op.value!r} does not use an update')
            try:
                compile_template(statement.template())
            except ValueError as e:
                raise ValueError(f'Statement {name!r}: {e}') from e
        return value


class HttpSinkConfig(BaseModel):
    """Configuration for an HTTP output sink.

    Each named instance POSTs a payload to a URL, encoded per ``encoding``.

    The ``encoding`` setting selects the request body format. The body and
    the Content-Type header always derive from it together, so a
    Content-Type in ``headers`` is rejected rather than silently
    contradicting the body.

    SSRF note: the URL is operator-configured (YAML/env), never drawn
    from message content. Validation here protects against typos and
    obvious mistakes (unsupported scheme, missing host) and refuses to
    target cloud metadata endpoints where accidentally pointing the sink
    would leak cloud IAM credentials.
    """

    url: str
    method: str = 'POST'
    timeout_seconds: int = Field(default=30, ge=1)
    headers: dict[str, str] = Field(
        default_factory=dict,
        description='Extra request headers. May carry an Authorization value, so treated as secret as a whole.',
        json_schema_extra={'drakkar_secret': True},
    )
    encoding: Literal['json', 'form', 'multipart'] = 'json'
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

    @model_validator(mode='after')
    def _reject_content_type_header(self) -> 'HttpSinkConfig':
        """Refuse a Content-Type header — ``encoding`` owns it.

        Headers are applied over the encoder's Content-Type, so allowing
        an override would let the declared type contradict the bytes
        actually sent. Multipart additionally needs a generated boundary
        parameter, which a static header can never carry correctly.
        """
        for key in self.headers:
            if key.lower() == 'content-type':
                raise ValueError(
                    f'headers must not set {key!r}; the Content-Type is determined by '
                    f'encoding={self.encoding!r}. Remove the header or change the encoding.'
                )
        return self


class RedisSinkConfig(BaseModel):
    """Configuration for a Redis output sink.

    Each named instance connects to a Redis server and issues one write
    command per payload, or runs an operator-authored Lua script by name.
    """

    url: str = Field(
        default='redis://localhost:6379/0',
        description='Redis connection URL, which may embed a password (redis://:password@host:port/db).',
        json_schema_extra={'drakkar_secret': True},
    )
    key_prefix: str = ''
    scripts: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Operator-authored Lua keyed by name. A RedisPayload with '
            "op='script' and a matching `script` name runs the entry with its "
            '`keys` and `args` passed as KEYS and ARGV. This is the escape '
            'hatch for multi-step or conditional logic, and the only way to '
            'get server-side atomicity — a pipeline is not a transaction. '
            'Parameters are never interpolated into the body, so message '
            'content cannot alter what runs.'
        ),
    )
    ui_url: str = ''

    @field_validator('scripts')
    @classmethod
    def _validate_scripts(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject malformed script config at startup.

        Checks only the name shape and a non-empty body. The Lua is NOT
        parsed: there is no parser available without a server, and validating
        against a live Redis would couple worker startup to Redis
        availability — the same trade-off the Postgres sink settled by not
        calling ``PREPARE``. A broken script fails at delivery and routes
        through ``on_delivery_error``.
        """
        for name, body in value.items():
            if not _REDIS_SCRIPT_NAME_RE.match(name):
                raise ValueError(
                    f'Invalid script name {name!r}: must match '
                    f'{_REDIS_SCRIPT_NAME_RE.pattern} (used as a structured-log field)'
                )
            if not body.strip():
                raise ValueError(f'Script {name!r} has an empty body')
        return value


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

        Useful for startup logging. Only includes types with at least one
        instance. Instance names are SORTED (and custom types render after
        the built-ins, sorted by type name) so the config-summary one-liner
        is byte-identical to the Go backend's, whose maps have no insertion
        order to preserve.
        """
        result: dict[str, list[str]] = {}
        if self.kafka:
            result['kafka'] = sorted(self.kafka)
        if self.postgres:
            result['postgres'] = sorted(self.postgres)
        if self.mongo:
            result['mongo'] = sorted(self.mongo)
        if self.http:
            result['http'] = sorted(self.http)
        if self.redis:
            result['redis'] = sorted(self.redis)
        if self.filesystem:
            result['filesystem'] = sorted(self.filesystem)
        for type_name in sorted(self.custom):
            if self.custom[type_name]:
                result[type_name] = sorted(self.custom[type_name])
        return result


class DLQConfig(BaseModel):
    """Dead letter queue configuration.

    Failed sink deliveries are written to this Kafka topic.
    If `topic` is empty, defaults to `{source_topic}_dlq` at runtime.
    If `brokers` is empty, inherits from `kafka.brokers` — and with them
    `kafka.security` and `kafka.client_config`, on the same
    same-cluster-means-same-credentials rule the Kafka sinks follow.
    """

    topic: str = ''
    brokers: str = ''

    # Only consulted when ``brokers`` is set; see ``KafkaSinkConfig.security``.
    security: KafkaSecurityConfig = Field(default_factory=KafkaSecurityConfig)
    client_config: dict[str, str] = Field(default_factory=dict)

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)

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
            '*PASSWD*',
            '*SECRET*',
            '*TOKEN*',
            '*_KEY',
            '*_DSN',
            '*CREDENTIAL*',
            '*SALT*',
        ],
        description=(
            'Case-insensitive fnmatch patterns against parent env var names. '
            'Matching vars are NOT inherited by subprocesses, even when '
            'env_inherit_parent is True. Deliberately narrower than the '
            'recorder redaction list: a pattern here WITHHOLDS the variable '
            'from your binary, so names with common non-secret uses '
            '(AUTH_SERVICE_URL, CERT_PATH, PRIVATE_SUBNET) are excluded. '
            'Set to [] to fully trust the parent environment.'
        ),
    )
    max_executors: int = Field(default=4, ge=1)
    task_timeout_seconds: int = Field(default=120, ge=1)
    max_stdout_bytes: int = Field(
        default=0,
        ge=0,
        description=(
            'Maximum bytes of stdout retained per task. 0 (the default) = '
            'unlimited. When a process writes more, the retained prefix is '
            'cut at a UTF-8 character boundary, the rest is read and '
            'discarded (the process is never blocked on a full pipe), and '
            'ExecutorResult.stdout_truncated is set. Useful when '
            'subprocesses can emit very large output.'
        ),
    )
    max_stderr_bytes: int = Field(
        default=0,
        ge=0,
        description=(
            'Maximum bytes of stderr retained per task. 0 (the default) = '
            'unlimited. Same semantics as max_stdout_bytes, applied to the '
            'stderr stream; sets ExecutorResult.stderr_truncated on cut.'
        ),
    )
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


class RuntimeHealthConfig(BaseModel):
    """Runtime health monitor: event-loop lag tracking and stall introspection.

    A heartbeat task measures how late the runtime wakes it (event-loop
    lag on this backend); a sampler thread captures stack traces of the
    code blocking the loop whenever the heartbeat goes silent for longer
    than ``stall_seconds``. Current state and lag history surface on the
    debug UI's Runtime tab, as Prometheus metrics, and as flight-recorder
    events (``runtime_health`` transitions/samples, ``runtime_stall``
    with captured stacks).

    The healthy-path cost per tick is one clock read, one comparison and
    one ring-buffer write — introspection (stack capture, task census)
    only runs during a stall or on an explicit debug-UI request.
    """

    enabled: bool = True
    tick_seconds: float = Field(
        default=0.25,
        gt=0.01,
        description=(
            'Heartbeat interval. Lag is measured as how late each tick '
            'fires; the sampler thread also checks heartbeat age at this '
            'interval. Smaller values narrow the attribution blind spot '
            'for short blocks at slightly more (still negligible) wakeups.'
        ),
    )
    warn_lag_seconds: float = Field(
        default=0.1,
        gt=0,
        description=(
            "Lag above this marks the runtime 'degraded'. Recovery to "
            "'healthy' needs several consecutive clean ticks (hysteresis), "
            'so a flapping loop does not spam state transitions.'
        ),
    )
    stall_seconds: float = Field(
        default=1.0,
        gt=0,
        description=(
            "Heartbeat age above this marks the runtime 'stalled' and starts "
            'stack sampling: the sampler thread captures what the runtime '
            'thread is executing until the heartbeat resumes. Each stall '
            'becomes one runtime_stall recorder event with the stacks.'
        ),
    )
    max_stall_stacks: int = Field(
        default=10,
        ge=1,
        description=(
            'Maximum distinct stack traces captured per stall. Repeated '
            'samples of the same location collapse into one entry with a '
            'count; further distinct stacks past the cap are dropped.'
        ),
    )
    sample_interval_seconds: float = Field(
        default=10.0,
        gt=0,
        description=(
            'Interval between runtime_health sample events written to the '
            'flight recorder for cross-restart history. The fine-grained '
            'lag sparkline comes from an in-memory ring buffer instead and '
            'costs no database writes.'
        ),
    )
    history_window_seconds: int = Field(
        default=900,
        ge=60,
        description=(
            'Length of the in-memory lag history ring buffer (one max/avg '
            'aggregate per second) served to the debug UI sparkline.'
        ),
    )


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


# --- UI config (server + recorder + release bundle) ---

# Old debug.* key → new ui.* home. Data-driven so the migration error and
# any future tooling render from one table instead of hand-written prose.
_DEBUG_KEY_MAP: dict[str, str] = {
    'enabled': 'ui.enabled',
    'host': 'ui.host',
    'port': 'ui.port',
    'auth_token': 'ui.auth_token',
    'allowed_ws_origins': 'ui.allowed_ws_origins',
    'debug_url': 'ui.public_url',
    'expose_env_vars': 'ui.expose_env_vars',
    'max_ui_rows': 'ui.max_rows',
    'ws_min_duration_ms': 'ui.ws_min_duration_ms',
    'log_min_duration_ms': 'ui.log_min_duration_ms',
    'prometheus_url': 'ui.prometheus_url',
    'prometheus_rate_interval': 'ui.prometheus_rate_interval',
    'prometheus_worker_label': 'ui.prometheus_worker_label',
    'prometheus_cluster_label': 'ui.prometheus_cluster_label',
    'custom_links': 'ui.custom_links',
    'db_dir': 'ui.recorder.db_dir',
    'store_events': 'ui.recorder.store_events',
    'store_config': 'ui.recorder.store_config',
    'store_state': 'ui.recorder.store_state',
    'state_sync_interval_seconds': 'ui.recorder.state_sync_interval_seconds',
    'rotation_interval_minutes': 'ui.recorder.rotation_interval_minutes',
    'retention_hours': 'ui.recorder.retention_hours',
    'retention_max_events': 'ui.recorder.retention_max_events',
    'store_output': 'ui.recorder.store_output',
    'flush_interval_seconds': 'ui.recorder.flush_interval_seconds',
    'max_buffer': 'ui.recorder.max_buffer',
    'max_flush_retries': 'ui.recorder.max_flush_retries',
    'event_min_duration_ms': 'ui.recorder.event_min_duration_ms',
    'output_min_duration_ms': 'ui.recorder.output_min_duration_ms',
}

# Old flat ui.* fetch key → new ui.release.* home (the pre-merge UI section
# held only bundle-fetch settings at the top level).
_UI_FLAT_KEY_MAP: dict[str, str] = {
    'release_repo': 'ui.release.repo',
    'pinned_version': 'ui.release.pinned_version',
    'cache_dir': 'ui.release.cache_dir',
    'check_update': 'ui.release.check_update',
}


class UIRecorderConfig(BaseModel):
    """Flight-recorder persistence settings — the UI's data store.

    Set ``db_dir: ""`` to run without any SQLite files on disk.

    Granular persistence flags (all require ``db_dir`` to be set):
    - ``store_events``: write processing events to the ``events`` table.
    - ``store_config``: write worker config to ``worker_config`` (enables autodiscovery).
    - ``store_state``: periodically dump counters to ``worker_state``.

    Any combination is valid — e.g. ``store_config=true`` with everything
    else ``false`` gives autodiscovery without event or state logging.
    """

    db_dir: str = '/tmp'
    store_events: bool = True
    store_config: bool = True
    store_state: bool = True
    state_sync_interval_seconds: int = Field(default=10, ge=1)
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
    event_min_duration_ms: int = Field(default=0, ge=0)
    output_min_duration_ms: int = Field(default=500, ge=0)
    # Handler annotations — diagnostic records a handler attaches to a window,
    # message, or task from inside a hook (see drakkar.annotations). They are
    # stored as ordinary rows in the events table, so recorder rotation and
    # retention expire them with everything else.
    #
    # ``0`` disables each byte cap. The two caps guard different resources and
    # are deliberately not one setting: annotation_max_bytes rejects a single
    # unreasonable payload, while annotation_max_bytes_per_call bounds what one
    # hook invocation can add to the DB in total — without the latter, a handler
    # annotating every message of a wide window can exhaust
    # retention_max_events and evict every other event.
    annotations_enabled: bool = True
    annotation_max_bytes: int = Field(default=16_384, ge=0)
    annotation_max_bytes_per_call: int = Field(default=262_144, ge=0)
    # Cap on the payload copy written to the warning log when a record is
    # dropped. Higher than the row itself is pointless; lower is fine. Log
    # lines usually ship to a metered aggregator, so an uncapped copy can cost
    # more than the row it replaced.
    annotation_log_max_bytes: int = Field(default=2048, ge=0)


class UIReleaseConfig(BaseModel):
    """Decoupled drakkar-ui bundle fetching settings.

    The UI ships as its own versioned bundle (the separate drakkar-ui repo,
    published to GitHub Releases) so every backend on a host serves the same
    UI and looks identical. When ``enabled``, the worker resolves that bundle
    through :mod:`drakkar.uihost` (cache → fetch) and serves it in place of
    the built-in server-rendered HTML pages.

    Default-ON with an update check: on startup the worker resolves the
    latest release (or serves the shared cache) and falls back to the
    built-in Jinja pages when nothing is fetchable and the cache is empty —
    a fetch failure is never fatal, so the default is safe offline too.
    """

    enabled: bool = Field(
        default=True,
        description=(
            'Resolve and serve the drakkar-ui bundle. Off pins the built-in '
            'server-rendered pages unconditionally (no fetch, no cache read).'
        ),
    )
    repo: str = Field(
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

    @field_validator('repo')
    @classmethod
    def _validate_repo(cls, v: str) -> str:
        """A non-empty repo must look like a GitHub ``owner/name`` slug."""
        if v and '/' not in v:
            raise ValueError(f'ui.release.repo must be "owner/name", got {v!r}')
        return v


class UIProbeDetailsConfig(BaseModel):
    """Caps for the Message Probe's user-defined details writes.

    Both limits guard a single probe run against a handler that writes
    unbounded diagnostics. The defaults are generous headroom for typical
    handler logic; raise them when a probe legitimately produces more
    (e.g. one table row per record across many large inputs).
    """

    max_writes: int = Field(
        default=10_000,
        ge=1,
        description=(
            'Maximum probe.set/append/update calls recorded per probe run. '
            'The first write past the cap records one ProbeError; further '
            'writes are dropped silently.'
        ),
    )
    max_total_bytes: int = Field(
        default=5_000_000,
        ge=1,
        description=(
            'Maximum total serialized size (bytes) of all probe-details '
            'writes per probe run. Past it, writes are dropped like the '
            'max_writes cap.'
        ),
    )


# --- Timeline tuning: color rules, label roles, history depth -----------
#
# Rule conditions and colors are validated against fixed vocabularies rather
# than left as free-form strings, so a typo in a YAML rule (an unknown op, a
# misspelled field) fails config load instead of silently never matching at
# render time.
TIMELINE_COLOR_NAMES = frozenset({'green', 'red', 'yellow', 'blue', 'gray', 'lightgray', 'purple', 'orange'})
TIMELINE_STRING_FIELDS = frozenset({'status', 'origin', 'client_name'})
TIMELINE_NUMERIC_FIELDS = frozenset(
    {'exit_code', 'duration', 'stdout_size', 'stdout_lines', 'stdin_size', 'stdin_lines', 'partition'}
)
TIMELINE_OPS = frozenset({'eq', 'ne', 'contains', 'prefix', 'gt', 'ge', 'lt', 'le', 'exists', 'missing'})
_TIMELINE_NO_VALUE_OPS = frozenset({'exists', 'missing'})
_TIMELINE_STRING_OPS = frozenset({'contains', 'prefix'})
_TIMELINE_NUMERIC_OPS = frozenset({'gt', 'ge', 'lt', 'le'})
_TIMELINE_HEX_RE = re.compile(r'#[0-9a-fA-F]{6}')


class TimelineRuleCondition(BaseModel):
    """One condition of a timeline color rule: a label or task field compared with an operator."""

    model_config = ConfigDict(extra='forbid')

    label: str = ''
    field: str = ''
    op: str
    value: str | int | float | None = None

    @model_validator(mode='after')
    def _validate_condition(self) -> 'TimelineRuleCondition':
        if bool(self.label) == bool(self.field):
            raise ValueError('timeline condition must set exactly one of label/field')
        if self.op not in TIMELINE_OPS:
            raise ValueError(f"timeline condition op '{self.op}' is not one of {sorted(TIMELINE_OPS)}")
        if self.op in _TIMELINE_NO_VALUE_OPS and self.value is not None:
            raise ValueError(f"timeline condition op '{self.op}' takes no value")
        if self.op not in _TIMELINE_NO_VALUE_OPS and self.value is None:
            raise ValueError(f"timeline condition op '{self.op}' requires a value")
        if self.field:
            if self.field not in TIMELINE_STRING_FIELDS | TIMELINE_NUMERIC_FIELDS:
                raise ValueError(f"timeline condition field '{self.field}' is not a known task field")
            if self.op in _TIMELINE_STRING_OPS and self.field in TIMELINE_NUMERIC_FIELDS:
                raise ValueError(f"op '{self.op}' cannot apply to numeric field '{self.field}'")
            if self.op in _TIMELINE_NUMERIC_OPS and self.field in TIMELINE_STRING_FIELDS:
                raise ValueError(f"op '{self.op}' cannot apply to string field '{self.field}'")
        return self


class TimelineColorRule(BaseModel):
    """A first-match-wins bar-coloring rule: all conditions in `when` must hold."""

    model_config = ConfigDict(extra='forbid')

    name: str = ''
    when: list[TimelineRuleCondition] = Field(min_length=1)
    color: str

    @field_validator('when', mode='before')
    @classmethod
    def _wrap_single_condition(cls, value: object) -> object:
        return [value] if isinstance(value, dict) else value

    @field_validator('color')
    @classmethod
    def _validate_color(cls, value: str) -> str:
        if value in TIMELINE_COLOR_NAMES or _TIMELINE_HEX_RE.fullmatch(value):
            return value
        raise ValueError(f"timeline color '{value}' must be one of {sorted(TIMELINE_COLOR_NAMES)} or '#rrggbb'")


class TimelineLabels(BaseModel):
    """Which task label the UI uses for each special timeline role; empty = role unbound."""

    model_config = ConfigDict(extra='forbid')

    tag: str = ''
    caption: str = ''
    highlight: str = ''
    filter: str = ''
    marker: str = ''


class UITimelineConfig(BaseModel):
    """Timeline history depth, bar color rules, and special label roles."""

    model_config = ConfigDict(extra='forbid')

    history_factor: int = Field(
        default=100,
        ge=1,
        description='Timeline keeps the newest history_factor x executor.max_executors tasks.',
    )
    max_age_minutes: int = Field(
        default=60,
        ge=1,
        le=1440,
        description='Oldest task age the timeline shows, in minutes.',
    )
    color_rules: list[TimelineColorRule] = Field(
        default_factory=list,
        max_length=50,
        description='First-match-wins rules coloring timeline task bars from labels and task fields.',
    )
    labels: TimelineLabels = Field(
        default_factory=TimelineLabels,
        description='Task label keys the UI uses for the tag, caption, highlight, filter, and marker roles.',
    )


class UIConfig(BaseModel):
    """The operator web UI: HTTP server, presentation, and sub-sections.

    One first-class ``ui.*`` section covers the whole surface:

    - top-level keys — the UI server itself (bind address, auth) and
      presentation settings the pages/SPA consume;
    - ``ui.recorder.*`` — the flight-recorder store that feeds the UI
      (:class:`UIRecorderConfig`);
    - ``ui.release.*`` — drakkar-ui bundle fetching
      (:class:`UIReleaseConfig`);
    - ``ui.probe_details.*`` — write caps for the Message Probe's
      user-defined details (:class:`UIProbeDetailsConfig`);
    - ``ui.timeline.*`` — timeline history depth, bar color rules, and
      label roles (:class:`UITimelineConfig`).

    Set ``enabled: false`` to disable the whole UI feature (server,
    recorder persistence, and bundle serving).

    The YAML keys and semantics match the Go backend's ``ui`` config block,
    so ``DK_UI__*`` env overrides behave identically on both backends.
    """

    enabled: bool = True
    host: str = Field(
        default='127.0.0.1',
        description='Bind address for the UI server. Use 0.0.0.0 to expose on all interfaces.',
    )
    port: int = Field(default=8080, ge=1, le=65535)
    auth_token: str = Field(
        default='',
        description=(
            'Bearer token for the UI. **Empty (the default) disables auth** '
            'entirely — every endpoint (including database download, merge, and '
            'message-probe) is reachable without credentials and the WebSocket '
            'live-event stream skips both token and Origin checks. This is a '
            'deliberate opt-in design: no endpoint stops a worker, replays '
            'Kafka messages, mutates sinks, or commits offsets, and Drakkar is '
            'intended for deployment inside a private contour (VPC / internal '
            'cluster / operator-only ingress). Most endpoints are read-only, '
            'but the probe and merge routes are not — close those with '
            '``probe_enabled`` / ``merge_enabled``, which act independently of '
            'this token. A startup warning fires whenever the UI is enabled '
            'without a token so the unauthenticated posture is visible in logs, '
            'naming whichever side-effecting endpoint is still enabled. '
            'When set to a non-empty value, protected HTTP endpoints require '
            'an ``Authorization: Bearer <token>`` header or ``?token=<token>`` '
            'query parameter; WebSocket connections without a valid token are '
            'closed with code 4401, and the Origin header is validated against '
            '``allowed_ws_origins`` (or the request Host header if that list is '
            'empty). Comparison uses ``secrets.compare_digest`` to avoid timing '
            'side-channels. Trailing/leading whitespace is stripped on load to '
            'avoid silent mismatches when YAML accidentally quotes spaces.'
        ),
        json_schema_extra={'drakkar_secret': True},
    )
    allowed_ws_origins: list[str] = Field(
        default_factory=list,
        description=(
            'Explicit allowlist of WebSocket origins. Empty list with non-empty '
            'auth_token defaults to same-origin only; empty list with empty '
            'auth_token = no origin check (dev workflow preserved).'
        ),
    )
    probe_enabled: bool = Field(
        default=True,
        description=(
            'Serve ``POST /api/debug/probe``. This is the one UI endpoint that '
            'runs caller-supplied bytes through the live handler and the real '
            'executor subprocess pool, so it is neither read-only nor free: it '
            'competes with production traffic for executor slots. Set to false '
            'to serve 403 instead — independently of ``auth_token``, so a '
            'deployment that cannot set a token can still close the endpoint, '
            'and a deployment that has one can still close it as defence in '
            'depth. Probes never write sinks, recorder rows, cache entries, or '
            'offsets, so switching it off costs no pipeline behaviour.'
        ),
    )
    merge_enabled: bool = Field(
        default=True,
        description=(
            'Serve ``POST /api/debug/merge``. This is the one UI endpoint that '
            'writes to disk: each call creates a new ``merged-<ts>.db`` in '
            '``ui.recorder.db_dir`` and nothing reclaims it, so repeated calls '
            'grow unbounded. Set to false to serve 403 instead — independently '
            'of ``auth_token``, per the reasoning on ``probe_enabled``.'
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

    @model_validator(mode='before')
    @classmethod
    def _reject_old_flat_release_keys(cls, values: object) -> object:
        """Hard break: the pre-merge flat fetch keys moved under ``ui.release``.

        Detecting them here (instead of letting them vanish as ignored
        extras) prevents the nastiest failure mode of the section merge — a
        config that still says ``ui.release_repo`` silently reverting to the
        default repo.
        """
        if isinstance(values, dict):
            moved = sorted(f'ui.{key} -> {new}' for key, new in _UI_FLAT_KEY_MAP.items() if key in values)
            if moved:
                raise ValueError('old flat ui.* bundle keys were moved under ui.release.*; update: ' + ', '.join(moved))
        return values

    public_url: str = Field(
        default='',
        description=(
            "Externally reachable URL of this worker's UI, used for "
            'cross-worker links in the workers list. Empty derives '
            'http://<ip>:<port> from the bind address.'
        ),
    )
    expose_env_vars: list[str] = Field(default_factory=list)
    max_rows: int = Field(default=5000, ge=100)
    log_min_duration_ms: int = Field(default=500, ge=0)
    ws_min_duration_ms: int = Field(default=500, ge=0)
    prometheus_url: str = ''
    prometheus_rate_interval: str = '5m'
    prometheus_worker_label: str = ''
    prometheus_cluster_label: str = ''
    custom_links: list[dict[str, str]] = Field(default_factory=list)
    link_bases: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Named URL bases for probe-details link templates, e.g. '
            "``{jira: 'https://jira.internal.example.com'}``. A template such as "
            '``{jira}/browse/{value}`` resolves ``{jira}`` from this map, so code '
            'declares the link shape once and each environment supplies its own '
            'hosts. A base referenced by a registered layout but missing here '
            'logs one startup warning and the UI renders plain text for it.'
        ),
    )
    custom_renderers_path: str = Field(
        default='',
        description=(
            'Path to a deployment-provided JS module of custom cell '
            'renderers, served as-is at ``GET /api/v1/ui/renderers.js``. '
            'Empty (the default) disables the feature — the route then '
            '404s and identity reports ``custom_renderers: false``. When '
            'set, the file must exist at startup or the worker fails to '
            'boot; its content is trusted the same as any other backend '
            'config (it runs same-origin in the operator UI).'
        ),
    )
    recorder: UIRecorderConfig = Field(default_factory=UIRecorderConfig)
    release: UIReleaseConfig = Field(default_factory=UIReleaseConfig)
    probe_details: UIProbeDetailsConfig = Field(default_factory=UIProbeDetailsConfig)
    timeline: UITimelineConfig = Field(default_factory=UITimelineConfig)

    @field_validator('link_bases')
    @classmethod
    def _validate_link_bases(cls, value: dict[str, str]) -> dict[str, str]:
        for name, base in value.items():
            if not re.fullmatch(r'[a-z][a-z0-9_]*', name):
                raise ValueError(f"link_bases name '{name}' must be a lower-case identifier ([a-z][a-z0-9_]*)")
            if not base.startswith(('http://', 'https://')):
                raise ValueError(f"link_bases['{name}'] must start with http:// or https://")
        return {name: base.rstrip('/') for name, base in value.items()}


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
    token: str = Field(
        default='',
        description='Bearer token for this client; empty means the anonymous slot (no Authorization header required).',
        json_schema_extra={'drakkar_secret': True},
    )
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
    # Cap on a single POST body (bytes); requests beyond it get a 413
    # ``request_too_large`` envelope before the body is buffered. Same
    # key, default, and behavior as the Go backend.
    max_body_bytes: int = 10 * 1024 * 1024
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
        # A zero/negative body cap would reject every non-empty POST at
        # the body-read gate.
        if self.max_body_bytes <= 0:
            raise ValueError(f'webapp.max_body_bytes must be > 0, got {self.max_body_bytes}')
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

    @model_validator(mode='before')
    @classmethod
    def _reject_retired_debug_section(cls, values: object) -> object:
        """Hard break: the ``debug.*`` section merged into ``ui.*``.

        Fails loudly with the exact old→new key mapping instead of letting
        an unrecognized ``debug`` key be ignored — a stale config would
        otherwise silently run with defaults (wrong port, wrong db_dir).
        Checks the raw input dict (YAML/kwargs path) and the environment
        (``DK_DEBUG__*`` would silently match no field at all).
        """
        if isinstance(values, dict) and 'debug' in values:
            section = cast('dict[str, object]', values)['debug']
            if isinstance(section, dict):
                keys = sorted(str(key) for key in section)
                moved = [f'debug.{key} -> {_DEBUG_KEY_MAP[key]}' for key in keys if key in _DEBUG_KEY_MAP]
                unknown = [f'debug.{key}' for key in keys if key not in _DEBUG_KEY_MAP]
                # Sorted key order keeps this message byte-comparable with the
                # Go backend's twin guard.
                detail = ', '.join(moved + unknown) or 'debug -> ui'
            else:
                detail = 'debug -> ui'
            raise ValueError(
                'the debug.* config section was replaced by the first-class ui.* section; update: ' + detail
            )
        stale_env = sorted(name for name in os.environ if name.startswith('DK_DEBUG__'))
        if stale_env:
            raise ValueError(
                'DK_DEBUG__* environment overrides target the retired debug.* section '
                '(now merged into ui.*); rename: ' + ', '.join(stale_env)
            )
        return values

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
    runtime_health: RuntimeHealthConfig = Field(default_factory=RuntimeHealthConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    webapp: WebAppConfig = Field(default_factory=WebAppConfig)

    def config_summary(self, worker_id: str = '', cluster_name: str = '') -> str:
        """One-line human-readable config summary for startup logging and debug UI.

        Format (Option C — structured-but-readable):
        [worker/cluster] topic=... group=... exec=4w/100win/100poll retries=3/120s ui=on:8080 webapp=on:8090 cache=off metrics=9090 dlq=on sinks=[kf:a,b pg:main] log=INFO

        The ``ui`` token reports the UI server state; the ``ui.release``
        bundle-fetch settings are deliberately excluded (they never affect
        pipeline behavior). Byte-parity with the Go backend is contractual.

        The ``webapp`` token reports the synchronous-ingress server
        (``webapp.host``/``port`` bind; port shown, host omitted like the
        other tokens).
        """
        identity = worker_id or '?'
        if cluster_name:
            identity = f'{identity}/{cluster_name}'

        ex = self.executor
        exec_part = f'{ex.max_executors}w/{ex.window_size}win/{self.kafka.max_poll_records}poll'
        retries_part = f'{ex.max_retries}/{ex.task_timeout_seconds}s'

        ui_part = f'on:{self.ui.port}' if self.ui.enabled else 'off'
        webapp_part = f'on:{self.webapp.port}' if self.webapp.enabled else 'off'

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
            f' ui={ui_part}'
            f' webapp={webapp_part}'
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
