"""Named connections to shared services, declared once and referenced by name.

A feature that needs a service (a Consul agent, a Redis server, a Postgres
database) names a connection instead of repeating the address and the
credentials. ``connections.<type>.<name>`` declares it; a feature that names
nothing uses ``main``. The same connection serves every feature that names it.

The shape rules are checked by one validator on :class:`ConnectionsConfig`,
not by field patterns, because the message must name the connection where the
operator wrote it and its text is part of the config format.
"""

import re

from pydantic import BaseModel, ConfigDict, Field, model_validator

CONSUL_PREFIX_RE = r'^[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*$'
REDIS_PREFIX_RE = r'^[A-Za-z0-9_:.-]*$'
SCHEMA_RE = r'^[a-z_][a-z0-9_]*$'


class ConsulConnectionConfig(BaseModel):
    """A Consul agent and the key prefix a feature may use on it."""

    address: str = Field(
        default='http://127.0.0.1:8500',
        description='Base URL of the Consul HTTP API.',
    )
    token: str = Field(
        default='',
        description='ACL token sent with every request. Empty means no token.',
        json_schema_extra={'drakkar_secret': True},
    )
    datacenter: str = Field(
        default='',
        description='Consul datacenter to address. Empty means the datacenter of the agent.',
    )
    key_prefix: str = Field(
        default='drakkar',
        description=(
            'Key prefix of everything a feature stores in Consul: segments of letters, digits, "_", "." '
            'and "-" joined by "/", with no leading or trailing "/".'
        ),
    )


class RedisConnectionConfig(BaseModel):
    """A Redis server and the key prefix a feature may use on it."""

    url: str = Field(
        default='redis://localhost:6379/0',
        description='Redis connection URL, typically embedding the password.',
        json_schema_extra={'drakkar_secret': True},
    )
    key_prefix: str = Field(
        default='drakkar:',
        description='Prefix of every key a feature stores in Redis: letters, digits, "_", ":", "." and "-". May be empty.',
    )


class PostgresConnectionConfig(BaseModel):
    """A Postgres database and the schema a feature may use in it."""

    # ``schema`` is the YAML key, but a field of that name would shadow
    # ``BaseModel.schema``; the alias keeps the key and the attribute is
    # ``schema_name``. ``populate_by_name`` also accepts the attribute name.
    model_config = ConfigDict(populate_by_name=True)

    dsn: str = Field(
        description='Postgres connection string (asyncpg-compatible), typically embedding user and password.',
        json_schema_extra={'drakkar_secret': True},
    )
    schema_name: str = Field(
        default='drakkar',
        alias='schema',
        description='Schema that holds the tables a feature creates: a lower-case identifier.',
    )
    manage_schema: bool = Field(
        default=True,
        description='Create the schema and the tables of a feature when they are missing. Off, they must already exist.',
    )
    pool_min: int = Field(default=1, ge=1, description='Minimum connections in the pool.')
    pool_max: int = Field(default=4, ge=1, description='Maximum connections in the pool. Must be >= pool_min.')


class ConnectionsConfig(BaseModel):
    """Named connections, grouped by service type.

    Example YAML::

        connections:
          postgres:
            main:
              dsn: "postgresql://..."
    """

    consul: dict[str, ConsulConnectionConfig] = Field(
        default_factory=dict,
        description='Consul connections by name. A feature that names no connection uses "main".',
    )
    redis: dict[str, RedisConnectionConfig] = Field(
        default_factory=dict,
        description='Redis connections by name. A feature that names no connection uses "main".',
    )
    postgres: dict[str, PostgresConnectionConfig] = Field(
        default_factory=dict,
        description='Postgres connections by name. A feature that names no connection uses "main".',
    )

    @model_validator(mode='after')
    def _validate_shapes(self) -> 'ConnectionsConfig':
        """Check prefixes, schemas and pool sizes; the first wrong connection by service type and name is reported."""
        for name, consul in sorted(self.consul.items()):
            if not re.fullmatch(CONSUL_PREFIX_RE, consul.key_prefix):
                raise ValueError(
                    f"connections.consul.{name}.key_prefix '{consul.key_prefix}' must be segments of letters, "
                    "digits, '_', '.' and '-' joined by '/', with no leading or trailing '/'"
                )
        for name, redis in sorted(self.redis.items()):
            if not re.fullmatch(REDIS_PREFIX_RE, redis.key_prefix):
                raise ValueError(
                    f"connections.redis.{name}.key_prefix '{redis.key_prefix}' may only contain "
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
