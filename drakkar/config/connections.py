"""Named connections to shared services, declared once and referenced by name.

A consumer that needs a service (a Kafka cluster, a Consul agent, a Redis or
Mongo server, a Postgres database) names a connection instead of repeating the
address and the credentials. ``connections.<type>.<name>`` declares it; a
consumer that names nothing uses ``main``, and the entry must be declared. Many
consumers may name one entry. A different login to one server is a different
connection. What a consumer does with the service stays in the consumer.

The shape rules are checked by one validator on :class:`ConnectionsConfig`,
not by field patterns, because the message must name the connection where the
operator wrote it and its text is part of the config format.
"""

import re
from collections.abc import Mapping

from pydantic import Field, field_validator, model_validator

from drakkar.config.base import ConfigModel
from drakkar.kafka_security import KafkaSecurityConfig, validate_client_config

CONSUL_PREFIX_RE = r'^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$'
CONSUL_DATACENTER_RE = r'^[A-Za-z0-9_.-]*$'
# The rule of a state prefix, as the tail of its load message and of the providers' message for a preset prefix.
CONSUL_PREFIX_RULE = (
    "must be segments of letters, digits, '_', '.' and '-' joined by '/', with no leading or trailing '/', "
    "and no segment may be '.' or '..'"
)
REDIS_PREFIX_RE = r'^[A-Za-z0-9_:.-]*$'
SCHEMA_RE = r'^[a-z_][a-z0-9_]*$'

# Path segments that some HTTP clients remove from a path before they send it and others send as written,
# so a path that holds one is not the same request for every client.
DOT_SEGMENTS = ('.', '..')


def has_dot_segment(path: str) -> bool:
    """Whether a ``/``-separated ``path`` has a segment that is exactly ``.`` or ``..``."""
    return any(segment in DOT_SEGMENTS for segment in path.split('/'))


def is_consul_state_prefix(prefix: str) -> bool:
    """Whether ``prefix`` is segments of the allowed characters joined by ``/``, none of them ``.`` or ``..``."""
    if re.fullmatch(CONSUL_PREFIX_RE, prefix) is None:
        return False
    return not has_dot_segment(prefix)


class KafkaConnectionConfig(ConfigModel):
    """A Kafka cluster: its brokers, transport security, raw client properties and Kafka-UI links."""

    brokers: str = Field(default='localhost:9092', description='Kafka bootstrap servers, comma-separated.')
    security: KafkaSecurityConfig = Field(default_factory=KafkaSecurityConfig)
    client_config: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Raw librdkafka properties merged after the security block, for options the typed fields do '
            'not model, for every client of this connection. Reserved keys (bootstrap.servers, group.id, '
            'enable.auto.commit, partition.assignment.strategy) are rejected at config load.'
        ),
    )
    # Kafka-UI (https://github.com/provectus/kafka-ui) deep links of the source
    # topic. Both fields must be set for the icon next to a <partition:offset> to
    # appear; it opens Kafka-UI filtered on that message in a new tab.
    ui_url: str = Field(
        default='',
        description=(
            'Base URL of a Kafka-UI instance for deep links. Together with ui_cluster_name, renders an icon '
            'next to every partition:offset of the source topic that opens Kafka-UI filtered on that '
            'message; empty disables the links.'
        ),
    )
    ui_cluster_name: str = Field(
        default='',
        description=(
            'Cluster name as configured in Kafka-UI, used in the deep links. '
            'Both this and ui_url must be set for the links to appear.'
        ),
    )

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)

    def client_config_with(self, own: Mapping[str, str]) -> dict[str, str]:
        """The raw properties of one client: this connection's, then the client's own, which win on a shared key."""
        return {**self.client_config, **own}


class ConsulConnectionConfig(ConfigModel):
    """A Consul agent and the prefix of the keys Drakkar itself keeps on it."""

    address: str = Field(
        default='http://127.0.0.1:8500',
        description=(
            'Base URL of the Consul HTTP API: http:// or https://, a host, an optional port and an optional '
            'path; no user info, query or fragment.'
        ),
    )
    token: str = Field(
        default='',
        description='ACL token sent with every request. Empty means no token.',
        json_schema_extra={'drakkar_secret': True},
    )
    datacenter: str = Field(
        default='',
        description=(
            'Consul datacenter to address: letters, digits, "_", "." and "-". Empty means the datacenter of the agent.'
        ),
    )
    state_prefix: str = Field(
        default='drakkar',
        description=(
            'Prefix of the keys Drakkar itself keeps in Consul: segments of letters, digits, "_", "." '
            'and "-" joined by "/", with no leading or trailing "/", and no segment may be "." or "..".'
        ),
    )


class RedisConnectionConfig(ConfigModel):
    """A Redis server and the prefix of the keys Drakkar itself keeps on it."""

    url: str = Field(
        default='redis://localhost:6379/0',
        description='Redis connection URL, typically embedding the password.',
        json_schema_extra={'drakkar_secret': True},
    )
    state_prefix: str = Field(
        default='drakkar:',
        description='Prefix of the keys Drakkar itself keeps in Redis: letters, digits, "_", ":", "." and "-". May be empty.',
    )


class MongoConnectionConfig(ConfigModel):
    """A MongoDB server."""

    uri: str = Field(
        description='MongoDB connection string, typically embedding user and password.',
        json_schema_extra={'drakkar_secret': True},
    )


class PostgresConnectionConfig(ConfigModel):
    """A Postgres database and the schema of the tables Drakkar itself keeps in it."""

    dsn: str = Field(
        description='Postgres connection string (asyncpg-compatible), typically embedding user and password.',
        json_schema_extra={'drakkar_secret': True},
    )
    # ``schema`` is the YAML key, but a field of that name would shadow
    # ``BaseModel.schema``; the alias keeps the key and the attribute is
    # ``schema_name``. Only the key is accepted.
    schema_name: str = Field(
        default='drakkar',
        alias='schema',
        description='Schema of the tables Drakkar itself keeps: a lower-case identifier.',
    )
    manage_schema: bool = Field(
        default=True,
        description='Create the schema and the tables Drakkar keeps when they are missing. Off, they must already exist.',
    )
    pool_min: int = Field(
        default=1, ge=1, description='Minimum connections in the pool of the autodiscovery registry and election.'
    )
    pool_max: int = Field(
        default=4,
        ge=1,
        description='Maximum connections in the pool of the autodiscovery registry and election. Must be >= pool_min.',
    )


class ConnectionsConfig(ConfigModel):
    """Named connections, grouped by service type.

    Example YAML::

        connections:
          kafka:
            main:
              brokers: "kafka:9092"
          postgres:
            main:
              dsn: "postgresql://..."
    """

    kafka: dict[str, KafkaConnectionConfig] = Field(
        default_factory=dict,
        description='Kafka connections by name. A consumer that names no connection uses "main".',
    )
    postgres: dict[str, PostgresConnectionConfig] = Field(
        default_factory=dict,
        description='Postgres connections by name. A consumer that names no connection uses "main".',
    )
    redis: dict[str, RedisConnectionConfig] = Field(
        default_factory=dict,
        description='Redis connections by name. A consumer that names no connection uses "main".',
    )
    mongo: dict[str, MongoConnectionConfig] = Field(
        default_factory=dict,
        description='MongoDB connections by name. A consumer that names no connection uses "main".',
    )
    consul: dict[str, ConsulConnectionConfig] = Field(
        default_factory=dict,
        description='Consul connections by name. A consumer that names no connection uses "main".',
    )

    @model_validator(mode='after')
    def _validate_shapes(self) -> 'ConnectionsConfig':
        """Check the shape of every entry; the first wrong connection by service type and name is reported."""
        for name, consul in sorted(self.consul.items()):
            path = f'connections.consul.{name}'
            if not is_consul_state_prefix(consul.state_prefix):
                raise ValueError(f"{path}.state_prefix '{consul.state_prefix}' {CONSUL_PREFIX_RULE}")
            if not re.fullmatch(CONSUL_DATACENTER_RE, consul.datacenter):
                raise ValueError(
                    f"{path}.datacenter '{consul.datacenter}' may only contain letters, digits, '_', '.' and '-'"
                )
        for name, redis in sorted(self.redis.items()):
            if not re.fullmatch(REDIS_PREFIX_RE, redis.state_prefix):
                raise ValueError(
                    f"connections.redis.{name}.state_prefix '{redis.state_prefix}' may only contain "
                    "letters, digits, '_', ':', '.' and '-'"
                )
        for name, postgres in sorted(self.postgres.items()):
            path = f'connections.postgres.{name}'
            if not re.fullmatch(SCHEMA_RE, postgres.schema_name):
                raise ValueError(
                    f"{path}.schema '{postgres.schema_name}' must be a lower-case identifier ([a-z_][a-z0-9_]*)"
                )
            if postgres.pool_max < postgres.pool_min:
                raise ValueError(
                    f'{path}.pool_max ({postgres.pool_max}) must be >= {path}.pool_min ({postgres.pool_min})'
                )
        return self
