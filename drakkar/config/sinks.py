"""Sink configuration models and the DLQ.

One model per sink type, plus the ``SinksConfig`` container that groups
instances by type and holds the defaults of their policy keys, and the
``DLQConfig`` for the dead-letter producer. A sink of a service kind names a
connection under ``connections.<kind>``; how to reach and log in to the service
stays there, and each sink owns its client or pool.

The two imports from ``drakkar.sinks`` are the pure, dependency-free
template compilers — they validate operator-authored SQL/MQL at config
load. See the note in ``drakkar/sinks/__init__.py`` for why that direction
is safe and the reverse one is not.
"""

import re
from collections.abc import Mapping
from typing import Any, Literal, get_origin
from urllib.parse import urlparse

from pydantic import (
    ConfigDict,
    Field,
    ModelWrapValidatorHandler,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from drakkar.config.base import ConfigModel, yaml_text
from drakkar.config.units import Duration, WholeSeconds
from drakkar.kafka_security import validate_client_config
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


# The failure policy of one sink instance. The keys are shared by every sink
# type; every one but ``depends_on`` has its default in ``sinks.defaults``.
ON_FAILURE_CHOICES = ('dlq', 'skip')
MAX_RETRIES_LIMIT = 10
DEFAULTED_POLICY_KEYS = ('on_failure', 'max_retries', 'delivery_timeout', 'breaker_failures', 'breaker_cooldown')
POLICY_KEYS = (*DEFAULTED_POLICY_KEYS, 'depends_on')

# The same integer rule as the ``max_retries`` field, so the range check sees
# the value the field would hold (``3.0`` and ``'3'`` are 3; ``2.5`` is left to
# the field, whose integer message names the path).
_WHOLE_NUMBER = TypeAdapter(int)

DLQ_PROVIDER_KAFKA = 'kafka'


class SinkDefaults(ConfigModel):
    """The policy keys of a sink instance that does not give them (``sinks.defaults``)."""

    on_failure: Literal['dlq', 'skip'] = Field(
        default='dlq',
        description=(
            "What happens when the retry budget of a delivery is spent: 'dlq' writes the "
            "source message to the DLQ and ends the message; 'skip' drops this sink's "
            'payloads and goes on.'
        ),
    )
    max_retries: int = Field(
        default=2,
        ge=0,
        le=MAX_RETRIES_LIMIT,
        description=(
            'Retries after the first attempt, a whole number from 0 to 10 (2 = 3 attempts). '
            'on_delivery_error returning RETRY spends one; the quick internal retries of an '
            'idempotent write are not counted.'
        ),
    )
    delivery_timeout: Duration = Field(
        default=30.0,
        gt=0,
        description=(
            'Budget for one sink delivery, including the framework-internal transient retries. '
            'A sink whose server stops answering while the TCP connection stays open would otherwise '
            'block its partition forever: the circuit breaker only sees a failure when a call returns. '
            'Sinks whose driver supports it also use this value for their own socket or command '
            'timeout, so the driver reports a specific error first. Applies to close() during shutdown '
            'as well.'
        ),
    )
    breaker_failures: int = Field(
        default=5,
        ge=1,
        description=(
            'Consecutive delivery failures before the circuit trips open: 5 trips on the 5th '
            'consecutive failure. Intermittent failures with any success in between do not accumulate.'
        ),
    )
    breaker_cooldown: Duration = Field(
        default=30.0,
        ge=0.0,
        description=(
            'Time the circuit stays fully open before the next delivery is allowed through as a '
            'half-open probe. While open, no attempt is made on the sink.'
        ),
    )


class SinkInstanceConfig(ConfigModel):
    """The policy keys of one sink instance; a key not given reads ``sinks.defaults``.

    Every sink config class inherits these keys; ``SinksConfig`` resolves them
    through ``SinksConfig.policy_of``.
    """

    on_failure: Literal['dlq', 'skip'] | None = Field(
        default=None,
        description="This sink's on_failure. Not given: sinks.defaults.on_failure.",
    )
    max_retries: int | None = Field(
        default=None,
        ge=0,
        le=MAX_RETRIES_LIMIT,
        description="This sink's max_retries, a whole number from 0 to 10. Not given: sinks.defaults.max_retries.",
    )
    delivery_timeout: Duration | None = Field(
        default=None,
        gt=0,
        description="This sink's delivery_timeout. Not given: sinks.defaults.delivery_timeout.",
    )
    breaker_failures: int | None = Field(
        default=None,
        ge=1,
        description="This sink's breaker_failures. Not given: sinks.defaults.breaker_failures.",
    )
    breaker_cooldown: Duration | None = Field(
        default=None,
        ge=0.0,
        description="This sink's breaker_cooldown. Not given: sinks.defaults.breaker_cooldown.",
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description=(
            "Sinks that must deliver a result before this one starts, each written '<type>/<name>'. "
            'Checked at load: every reference exists, no self-reference, no cycle.'
        ),
    )


class SinkPolicy(ConfigModel):
    """The resolved policy of one sink instance, as the delivery reads it."""

    model_config = ConfigDict(frozen=True)

    on_failure: Literal['dlq', 'skip']
    max_retries: int
    delivery_timeout: float
    breaker_failures: int
    breaker_cooldown: float
    depends_on: tuple[str, ...]

    @classmethod
    def of(cls, instance: SinkInstanceConfig, defaults: SinkDefaults) -> 'SinkPolicy':
        """The policy of ``instance``: a key it gives wins, even one equal to the default."""
        resolved = {key: getattr(instance, key) for key in DEFAULTED_POLICY_KEYS}
        for key, value in resolved.items():
            if value is None:
                resolved[key] = getattr(defaults, key)
        return cls(**resolved, depends_on=tuple(instance.depends_on))


def check_instance_policy(path: str, raw: Mapping[str, Any]) -> None:
    """Refuse a bad ``on_failure`` or ``max_retries`` in the raw leaf at ``path``, with the pinned message.

    A key with no value is not given. A ``max_retries`` that is not a whole
    number is left to the field's integer rule. ``depends_on`` references are checked once every instance
    is known (``SinksConfig.check_dependencies``).
    """
    on_failure = raw.get('on_failure')
    if on_failure is not None and on_failure not in ON_FAILURE_CHOICES:
        raise ValueError(f"{path}.on_failure must be dlq or skip, got '{on_failure}'")
    if raw.get('max_retries') is None:
        return
    try:
        retries = _WHOLE_NUMBER.validate_python(raw['max_retries'])
    except ValidationError:
        return
    if not 0 <= retries <= MAX_RETRIES_LIMIT:
        raise ValueError(f'{path}.max_retries must be a whole number from 0 to {MAX_RETRIES_LIMIT}')


def find_dependency_cycle(graph: Mapping[str, list[str]]) -> list[str] | None:
    """The first cycle of ``graph``, closed by its first member, or ``None``.

    The search visits nodes in sorted order and follows each node's
    dependencies in their declared order, so the cycle it finds is the same
    for any implementation. The cycle is then turned to start at its smallest
    member: ``b -> c -> b``, never ``c -> b -> c``.
    """
    visiting: set[str] = set()
    done: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        visiting.add(node)
        stack.append(node)
        for dependency in graph.get(node, []):
            if dependency in visiting:
                return stack[stack.index(dependency) :]
            if dependency not in done:
                found = visit(dependency)
                if found is not None:
                    return found
        stack.pop()
        visiting.discard(node)
        done.add(node)
        return None

    for node in sorted(graph):
        if node in done:
            continue
        cycle = visit(node)
        if cycle is not None:
            start = cycle.index(min(cycle))
            turned = cycle[start:] + cycle[:start]
            return [*turned, turned[0]]
    return None


class KafkaSinkConfig(SinkInstanceConfig):
    """Configuration for a Kafka output sink: a topic on the cluster that ``connection`` names."""

    connection: str = Field(default='main', description='The Kafka cluster: a name under connections.kafka.')
    topic: str = Field(description='Target Kafka topic for output messages.')
    client_config: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Raw librdkafka properties for this sink's producer, merged after the connection's client_config; "
            'this sink wins on a shared key. Reserved keys are rejected at config load.'
        ),
    )
    flush_timeout: Duration = Field(
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
    ui_url: str = Field(
        default='',
        description='URL to a web UI for this sink (e.g. Kafka-UI), shown as a link in the operator UI.',
    )

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)


class PostgresSinkConfig(SinkInstanceConfig):
    """Configuration for a PostgreSQL output sink.

    Each named instance keeps its own asyncpg pool on the database that
    ``connection`` names.
    """

    connection: str = Field(default='main', description='The database: a name under connections.postgres.')
    pool_min: int = Field(default=2, ge=1, description='Minimum connections in the asyncpg pool.')
    pool_max: int = Field(default=10, ge=1, description='Maximum connections in the asyncpg pool.')
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
    ui_url: str = Field(
        default='',
        description='URL to a database management UI (e.g. pgAdmin), shown as a link in the operator UI.',
    )

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


class MongoStatementConfig(ConfigModel):
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

    collection: str = Field(description='Target MongoDB collection the statement writes to.')
    op: MongoOp = Field(
        description=(
            'Which write operation to perform. Statements allow the update and delete ops only — '
            'an insert is fully expressible as a declarative payload and needs no template.'
        )
    )
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


class MongoSinkConfig(SinkInstanceConfig):
    """Configuration for a MongoDB output sink.

    Each named instance keeps its own PyMongo AsyncMongoClient on the server
    that ``connection`` names.
    """

    connection: str = Field(default='main', description='The server: a name under connections.mongo.')
    database: str = Field(description='Target database name.')
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
    ui_url: str = Field(
        default='',
        description='URL to a MongoDB management UI (e.g. Mongo Express), shown as a link in the operator UI.',
    )

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


class HttpSinkConfig(SinkInstanceConfig):
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

    url: str = Field(
        description=(
            'Target URL for requests. Must be http:// or https:// with a non-empty host; '
            'cloud metadata endpoints are refused at config load.'
        )
    )
    method: str = Field(default='POST', description='HTTP method to use.')
    request_timeout: WholeSeconds = Field(default=30, ge=1, description='Request timeout.')
    headers: dict[str, str] = Field(
        default_factory=dict,
        description='Extra request headers. May carry an Authorization value, so treated as secret as a whole.',
        json_schema_extra={'drakkar_secret': True},
    )
    encoding: Literal['json', 'form', 'multipart'] = Field(
        default='json',
        description=(
            "Request body format: 'json', 'form' (urlencoded), or 'multipart'. "
            'The Content-Type header always derives from it, so headers must not set one.'
        ),
    )
    ui_url: str = Field(
        default='',
        description='URL to a related web UI, shown as a link in the operator UI.',
    )

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


class RedisSinkConfig(SinkInstanceConfig):
    """Configuration for a Redis output sink.

    Each named instance keeps its own client on the server that ``connection``
    names and issues one write command per payload, or runs an operator-authored
    Lua script by name.
    """

    connection: str = Field(default='main', description='The server: a name under connections.redis.')
    key_prefix: str = Field(
        default='',
        description="Prefix prepended to all keys (e.g. 'cache:' produces keys like 'cache:my-key').",
    )
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
    ui_url: str = Field(
        default='',
        description='URL to a Redis management UI (e.g. RedisInsight), shown as a link in the operator UI.',
    )

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


class FileSinkConfig(SinkInstanceConfig):
    """Configuration for a filesystem output sink.

    Writes JSONL lines to files. `base_path` is required — all payload
    paths are resolved relative to it and contained within it.
    """

    base_path: str = Field(
        min_length=1,
        description=(
            'Base directory for all writes. Payload paths resolve relative to it and must stay '
            'contained within it — absolute or traversing paths are rejected.'
        ),
    )
    ui_url: str = Field(
        default='',
        description='URL to a file browser or related UI, shown as a link in the operator UI.',
    )


class SinksConfig(ConfigModel):
    """Container for all configured sink instances, grouped by type.

    Each sink type maps sink names to their configuration.
    Example YAML::

        sinks:
          defaults:
            max_retries: 5
          kafka:
            results:
              topic: "search-results"
          postgres:
            main-db:
              connection: analytics
    """

    defaults: SinkDefaults = Field(default_factory=SinkDefaults)
    kafka: dict[str, KafkaSinkConfig] = Field(default_factory=dict)
    postgres: dict[str, PostgresSinkConfig] = Field(default_factory=dict)
    mongo: dict[str, MongoSinkConfig] = Field(default_factory=dict)
    redis: dict[str, RedisSinkConfig] = Field(default_factory=dict)
    http: dict[str, HttpSinkConfig] = Field(default_factory=dict)
    filesystem: dict[str, FileSinkConfig] = Field(default_factory=dict)

    @model_validator(mode='wrap')
    @classmethod
    def _check_policies(cls, data: Any, handler: ModelWrapValidatorHandler['SinksConfig']) -> 'SinksConfig':
        """Check the policy keys of every instance on the raw input.

        The checks run on the raw input because only here is the instance
        path known, and the messages name it. Instances are checked in sorted
        ``<type>/<name>`` order, ``on_failure`` before ``max_retries``, so the
        first message is the same for any key order; ``sinks.defaults`` comes first.
        """
        if isinstance(data, Mapping):
            defaults = data.get('defaults')
            if isinstance(defaults, Mapping):
                check_instance_policy('sinks.defaults', defaults)
            for _, path, raw in sorted(_raw_instance_leaves(data), key=lambda leaf: leaf[0]):
                check_instance_policy(path, raw)
        return handler(data)

    def _instances(self) -> dict[str, tuple[str, SinkPolicy]]:
        """Every configured instance: its ``<type>/<name>`` reference, its config path and its policy."""
        instances: dict[str, tuple[str, SinkPolicy]] = {}
        for sink_type in SINK_TYPES:
            section: dict[str, SinkInstanceConfig] = getattr(self, sink_type)
            for name, instance in section.items():
                policy = SinkPolicy.of(instance, self.defaults)
                instances[f'{sink_type}/{name}'] = (f'sinks.{sink_type}.{name}', policy)
        return instances

    def policies(self) -> dict[str, SinkPolicy]:
        """The resolved policy of every instance, keyed by its ``<type>/<name>`` reference."""
        return {ref: policy for ref, (_, policy) in self._instances().items()}

    def policy_of(self, sink_type: str, name: str) -> SinkPolicy:
        """The resolved policy of the instance ``name`` of ``sink_type``.

        Raises:
            KeyError: no such instance is configured.
        """
        return self._instances()[f'{sink_type}/{name}'][1]

    def instance_path(self, ref: str) -> str:
        """The config path of the instance ``ref`` (``<type>/<name>``).

        It is ``sinks.<type>.<name>``.

        Raises:
            KeyError: no such instance is configured.
        """
        return self._instances()[ref][0]

    def dependency_graph(self) -> dict[str, list[str]]:
        """Every instance's ``depends_on`` list, keyed by its ``<type>/<name>`` reference, in sorted order."""
        instances = self._instances()
        return {ref: list(instances[ref][1].depends_on) for ref in sorted(instances)}

    def check_dependencies(self) -> None:
        """Refuse an unknown reference, a self-reference or a cycle in ``depends_on``.

        Instances are checked in sorted reference order and each one's
        references in their declared order; the cycle check runs last.

        Raises:
            ValueError: with the pinned message of the first broken rule.
        """
        instances = self._instances()
        for ref in sorted(instances):
            path, policy = instances[ref]
            for dependency in policy.depends_on:
                if dependency == ref:
                    raise ValueError(f'{path}.depends_on: a sink cannot depend on itself')
                if dependency not in instances:
                    raise ValueError(
                        f"{path}.depends_on: '{dependency}' is not a configured sink; write '<type>/<name>'"
                    )
        cycle = find_dependency_cycle(self.dependency_graph())
        if cycle is not None:
            raise ValueError('sinks: depends_on forms a cycle: ' + ' -> '.join(cycle))

    @property
    def is_empty(self) -> bool:
        """True if no sinks of any type are configured."""
        return not any([self.kafka, self.postgres, self.mongo, self.http, self.redis, self.filesystem])

    def summary(self) -> dict[str, list[str]]:
        """Return a dict of sink type → list of instance names.

        Useful for startup logging. Only includes types with at least one
        instance. Instance names are SORTED so the config-summary one-liner
        is reproducible from config whose decoded form carries no
        insertion order.
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
        return result


# The sink types: the ``SinksConfig`` fields that map instance names to a
# sink config.
SINK_TYPES: tuple[str, ...] = tuple(
    name for name, field in SinksConfig.model_fields.items() if get_origin(field.annotation) is dict
)

DEFAULT_SINK_POLICY = SinkPolicy.of(SinkInstanceConfig(), SinkDefaults())


def _raw_instance_leaves(data: Mapping[str, Any]) -> list[tuple[str, str, Mapping[str, Any]]]:
    """``(reference, path, leaf)`` of every instance in raw ``sinks`` input whose leaf is a mapping.

    Anything else is left to the field types, which report it.
    """
    leaves: list[tuple[str, str, Mapping[str, Any]]] = []
    for sink_type in SINK_TYPES:
        section = data.get(sink_type)
        if isinstance(section, Mapping):
            for name, leaf in section.items():
                if isinstance(leaf, Mapping):
                    leaves.append((f'{sink_type}/{name}', f'sinks.{sink_type}.{name}', leaf))
    return leaves


class DLQConfig(ConfigModel):
    """Dead letter queue configuration: failed messages are written to a Kafka topic.

    With ``enabled`` off, or with no topic and no Kafka source, no DLQ producer is
    built (see docs/sources.md). ``provider`` selects the destination, and ``connection``
    names its cluster under ``connections.kafka``.
    """

    enabled: bool = Field(
        default=True,
        description='Write DLQ records. Off, a dlq outcome follows on_send_failure, as with no DLQ sink.',
    )
    provider: Literal['kafka'] = Field(default='kafka', description='Where DLQ records go. kafka is the only value.')
    connection: str = Field(default='main', description='The Kafka cluster: a name under connections.kafka.')
    topic: str = Field(
        default='',
        description=(
            "DLQ Kafka topic name. Empty derives '{sources.kafka.topic}_dlq' when the Kafka source "
            'is enabled; with no Kafka source and no topic there is no DLQ.'
        ),
    )
    client_config: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Raw librdkafka properties for the DLQ producer, merged after the connection's client_config; "
            'the DLQ wins on a shared key. Reserved keys are rejected at config load.'
        ),
    )
    flush_timeout: Duration = Field(
        default=30.0,
        gt=0,
        description=(
            'Bound on the producer flush that ends every DLQ write. Same '
            'rationale as flush_timeout of a Kafka sink, and it '
            'matters more here: the DLQ is the last resort, so a DLQ write '
            'that blocks for message.timeout.ms stalls the partition it was '
            'meant to rescue. On expiry the write is reported as unconfirmed '
            'and the affected offsets stall.'
        ),
    )

    # Strategy when no record can be written after a sink delivery already
    # failed (the write failed, or the DLQ is off) — the payloads have nowhere safe to go:
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
    on_send_failure: Literal['drop', 'stall'] = Field(
        default='drop',
        description=(
            "Strategy when no DLQ record can be written. 'drop' logs CRITICAL, counts the loss, "
            "commits the offset, and keeps the pipeline moving; 'stall' leaves the offset "
            'uncommitted and pauses the partition until restart — no loss, at the cost of '
            'consumer lag.'
        ),
    )

    @field_validator('provider', mode='before')
    @classmethod
    def _require_a_known_provider(cls, value: Any) -> Any:
        if value != DLQ_PROVIDER_KAFKA:
            raise ValueError(f"dlq.provider must be kafka, got '{yaml_text(value)}'")
        return value

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)
