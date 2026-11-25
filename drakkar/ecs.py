"""Elastic Common Schema (ECS) names that the ``ecs`` log format and its config rules share.

Kept apart from :mod:`drakkar.logging`, which imports the config, so the
config can check ``logging.ecs_namespace`` without an import cycle. The names
are pinned by ``tests/fixtures/log_ecs.json``.
"""

import unicodedata

ECS_VERSION = '8.11.0'
DEFAULT_ECS_NAMESPACE = 'drakkar'

NAMESPACE_MAX_CHARACTERS = 255
# How much of a refused name a load message shows.
SHOWN_NAME_CHARACTERS = 64
# Unicode general categories a namespace may not hold: control characters, lone surrogates (they cannot be
# written as UTF-8), and every kind of space. They make a field name that is hard to type and to query.
REFUSED_CATEGORIES = frozenset({'Cc', 'Cs', 'Zs', 'Zl', 'Zp'})

DEFAULT_SERVICE_NAME = 'drakkar'
SERVICE_NAME_MAX_CHARACTERS = 255
# A service name may hold a space between its words, but no control character and no line separator.
SERVICE_NAME_REFUSED_CATEGORIES = frozenset({'Cc', 'Cs', 'Zl', 'Zp'})
SPACE_CATEGORY = 'Zs'

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


def _is_refused_character(character: str) -> bool:
    """Whether ``character`` is a space or a control character, which a namespace may not hold."""
    return unicodedata.category(character) in REFUSED_CATEGORIES


def _shown(name: str) -> str:
    """``name`` for a load message: its first characters, with each refused character written as an escape."""
    cut = name[:SHOWN_NAME_CHARACTERS]
    text = ''.join(repr(character)[1:-1] if _is_refused_character(character) else character for character in cut)
    return text + ('...' if len(name) > SHOWN_NAME_CHARACTERS else '')


def ecs_namespace_error(name: str) -> str | None:
    """Return the load message for an unusable ``logging.ecs_namespace``, or None.

    A name is free text of 1 to 255 characters. Dots are allowed: the name is
    written as one key, and a store that reads a dot as a level sees nested
    objects, so no part between dots may be empty, and the first part may not
    be an ECS field.
    """
    parts = name.split('.')
    has_a_valid_shape = (
        0 < len(name) <= NAMESPACE_MAX_CHARACTERS
        and all(parts)
        and not any(_is_refused_character(character) for character in name)
    )
    if not has_a_valid_shape:
        return (
            f"logging.ecs_namespace '{_shown(name)}' must be 1 to {NAMESPACE_MAX_CHARACTERS} characters "
            'with no space or control character, and no empty part between dots'
        )
    if name in ECS_RESERVED_NAMES:
        return (
            f"logging.ecs_namespace '{name}' is an ECS field name; choose another name, for example your product name"
        )
    if parts[0] in ECS_RESERVED_NAMES:
        return (
            f"logging.ecs_namespace '{name}' is inside the ECS field '{parts[0]}'; "
            'choose another name, for example your product name'
        )
    return None


def service_name_error(name: str) -> str | None:
    """Return the load message for an unusable ``logging.service_name``, or None.

    A name is free text of 1 to 255 characters. A space may stand between
    words; a space at the start or the end is refused, because nobody sees it
    in a log store and a query for the name then finds nothing.
    """
    categories = [unicodedata.category(character) for character in name]
    is_valid = (
        0 < len(name) <= SERVICE_NAME_MAX_CHARACTERS
        and not SERVICE_NAME_REFUSED_CATEGORIES.intersection(categories)
        and SPACE_CATEGORY not in (categories[0], categories[-1])
    )
    if is_valid:
        return None
    return (
        f"logging.service_name '{_shown(name)}' must be 1 to {SERVICE_NAME_MAX_CHARACTERS} characters "
        'with no control character and no space at its start or end'
    )
