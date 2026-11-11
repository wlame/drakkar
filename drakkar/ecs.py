"""Elastic Common Schema (ECS) names that the ``ecs`` log format and its config rules share.

Kept apart from :mod:`drakkar.logging`, which imports the config, so the
config can check ``logging.ecs_namespace`` without an import cycle. The names
are pinned by ``tests/fixtures/log_ecs.json``.
"""

import re

ECS_VERSION = '8.11.0'
DEFAULT_ECS_NAMESPACE = 'drakkar'

NAMESPACE_PATTERN = re.compile(r'[a-z_][a-z0-9_]{0,63}')

# The top-level field sets of ECS (and the base fields that are objects or
# words a user could pick), plus ``request_id``, which the format can write at
# the root. A namespace with one of these names would clash with an ECS object,
# and Elasticsearch refuses a document whose field type differs from the
# mapping.
ECS_RESERVED_NAMES = frozenset(
    {
        'agent',
        'as',
        'client',
        'cloud',
        'code_signature',
        'container',
        'data_stream',
        'destination',
        'device',
        'dll',
        'dns',
        'ecs',
        'elf',
        'email',
        'entity',
        'error',
        'event',
        'faas',
        'file',
        'gen_ai',
        'geo',
        'group',
        'hash',
        'host',
        'http',
        'interface',
        'labels',
        'log',
        'macho',
        'message',
        'network',
        'observer',
        'orchestrator',
        'organization',
        'os',
        'package',
        'pe',
        'process',
        'registry',
        'related',
        'request_id',
        'risk',
        'rule',
        'server',
        'service',
        'source',
        'span',
        'tags',
        'threat',
        'tls',
        'trace',
        'transaction',
        'url',
        'user',
        'user_agent',
        'vlan',
        'volume',
        'vulnerability',
        'x509',
    }
)


def ecs_namespace_error(name: str) -> str | None:
    """Return the load message for an unusable ``logging.ecs_namespace``, or None."""
    if not NAMESPACE_PATTERN.fullmatch(name):
        return (
            f"logging.ecs_namespace '{name}' must be a lower-case identifier "
            'of at most 64 characters ([a-z_][a-z0-9_]*)'
        )
    if name in ECS_RESERVED_NAMES:
        return (
            f"logging.ecs_namespace '{name}' is an ECS field name; choose another name, for example your product name"
        )
    return None
