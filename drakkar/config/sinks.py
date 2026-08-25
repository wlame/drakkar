"""Sink configuration models, the circuit breaker, and the DLQ.

One model per sink type, plus the ``SinksConfig`` container that groups
instances by type and the ``DLQConfig`` for the dead-letter producer.

The two imports from ``drakkar.sinks`` are the pure, dependency-free
template compilers — they validate operator-authored SQL/MQL at config
load. See the note in ``drakkar/sinks/__init__.py`` for why that direction
is safe and the reverse one is not.
"""

import re
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field, field_validator, model_validator

from drakkar.kafka_security import KafkaSecurityConfig, validate_client_config
from drakkar.models import MongoOp
from drakkar.sinks.mql import compile_template
from drakkar.sinks.pgsql import compile_named_statement

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

    flush_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        description=(
            'Bound on the producer flush that ends every delivery. Without '
            'one, librdkafka flushes with no deadline and a wedged broker '
            'blocks the call until message.timeout.ms (300s by default) — '
            'and it blocks a producer executor thread while doing it, so a '
            'few stuck deliveries starve every other delivery on the same '
            'producer. On expiry the delivery fails with a transient error, '
            'which is what lets the circuit breaker see the outage. Kept '
            'deliberately generous: this is a last resort, not a latency '
            'target.'
        ),
    )

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
    payload's ``params``, and never by interpolation — see ``drakkar.sinks.mql``
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
    delivery_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        description=(
            'Budget for one sink delivery, including the framework-internal '
            'transient retries. A sink whose server stops answering while the '
            'TCP connection stays open would otherwise block its partition '
            'forever: the circuit breaker only sees a failure when a call '
            'returns. Sinks whose driver supports it also use this value for '
            'their own socket/command timeout, so the driver reports a '
            'specific error before this outer budget expires. Applies to '
            'close() during shutdown as well.'
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

    flush_timeout_seconds: float = Field(
        default=30.0,
        gt=0,
        description=(
            'Bound on the producer flush that ends every DLQ write. Same '
            'rationale as KafkaSinkConfig.flush_timeout_seconds, and it '
            'matters more here: the DLQ is the last resort, so a DLQ write '
            'that blocks for message.timeout.ms stalls the partition it was '
            'meant to rescue. On expiry the write is reported as unconfirmed '
            'and the affected offsets stall.'
        ),
    )

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
