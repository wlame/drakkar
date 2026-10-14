"""Worker card, schema 1: what a worker publishes about itself to the registry.

A card is a small JSON object. Its serialized bytes are a contract: sorted
keys, compact separators and raw UTF-8, so every implementation writes the
same bytes for the same card. ``tests/fixtures/fleet_card.json`` pins the
bytes, the rejection reasons and the order in which the checks run.

Checking is split in two steps so a provider can drop unreadable files on
its own while the registry service judges every card the same way, whichever
provider delivered it:

- :func:`decode_card` turns raw bytes into a dict (size, JSON, object);
- :func:`validate_card` applies the schema and field rules to that dict.

:func:`parse_card` runs both.
"""

import copy
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from drakkar.timefmt import format_rfc3339_micro

CARD_SCHEMA = 1
MAX_CARD_BYTES = 4096
CARD_STATES = ('starting', 'running', 'draining')

# The required fields, checked in this order. ``worker`` and ``url`` must not
# be empty; ``cluster`` may be (a worker with no cluster).
REQUIRED_STRING_FIELDS = ('worker', 'cluster', 'url')
NON_EMPTY_FIELDS = frozenset({'worker', 'url'})
# Optional fields that must be strings when present, checked in this order.
OPTIONAL_STRING_FIELDS = ('instance', 'backend', 'version', 'state', 'started_at', 'heartbeat_at')

_UNSAFE_NAME_CHARS = re.compile(r'[^a-zA-Z0-9_-]')


@dataclass(frozen=True)
class WorkerIdentity:
    """The fields of a card that stay the same for the life of a process."""

    worker: str
    instance: str
    cluster: str
    url: str
    backend: str
    version: str
    started_at: datetime


def build_card(
    identity: WorkerIdentity,
    state: str,
    heartbeat_at: datetime,
    features: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the schema-1 card for ``identity`` in ``state``.

    ``features`` is copied, so a later change by the caller does not alter a
    card that is already built.
    """
    return {
        'schema': CARD_SCHEMA,
        'worker': identity.worker,
        'instance': identity.instance,
        'cluster': identity.cluster,
        'backend': identity.backend,
        'version': identity.version,
        'url': identity.url,
        'state': state,
        'started_at': format_rfc3339_micro(identity.started_at),
        'heartbeat_at': format_rfc3339_micro(heartbeat_at),
        'features': copy.deepcopy(features or {}),
    }


def serialize_card(card: dict[str, Any]) -> bytes:
    """Return the contract bytes of ``card``: sorted keys, compact, raw UTF-8, no trailing newline."""
    return json.dumps(card, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def _reject_constant(name: str) -> None:
    """Refuse ``NaN`` and ``Infinity``: they are not JSON, whatever the parser allows."""
    raise ValueError(f'{name} is not JSON')


def decode_card(raw: bytes) -> tuple[dict[str, Any] | None, str]:
    """Turn raw card bytes into a dict, or name why that is impossible.

    Returns ``(card, '')`` or ``(None, reason)`` where reason is
    ``too_large``, ``not_json`` or ``not_object``. Field rules are not
    checked here; see :func:`validate_card`.
    """
    if len(raw) > MAX_CARD_BYTES:
        return None, 'too_large'
    try:
        decoded = json.loads(raw.decode('utf-8'), parse_constant=_reject_constant)
    except ValueError:
        # UnicodeDecodeError and JSONDecodeError are both ValueError.
        return None, 'not_json'
    if not isinstance(decoded, dict):
        return None, 'not_object'
    return decoded, ''


def _is_integer(value: Any) -> bool:
    """True for a JSON integer; ``bool`` is an ``int`` subclass in Python, so exclude it."""
    return isinstance(value, int) and not isinstance(value, bool)


def _schema_reason(card: dict[str, Any]) -> str:
    """The rejection reason of the ``schema`` field, or ``''``."""
    if 'schema' not in card:
        return 'missing_field:schema'
    if not _is_integer(card['schema']):
        return 'bad_type:schema'
    if card['schema'] != CARD_SCHEMA:
        return 'schema_unsupported'
    return ''


def _required_field_reason(card: dict[str, Any], key: str) -> str:
    """The rejection reason of one required string field, or ``''``."""
    if key not in card:
        return f'missing_field:{key}'
    if not isinstance(card[key], str):
        return f'bad_type:{key}'
    if key in NON_EMPTY_FIELDS and not card[key]:
        return f'empty_field:{key}'
    return ''


def validate_card(card: Any) -> str:
    """Return ``''`` for a valid schema-1 card, else the first rejection reason.

    The schema is checked first, because a different schema may have
    different fields. Unknown keys are ignored, and so is an unknown
    ``state`` value.
    """
    if not isinstance(card, dict):
        # A provider plugin handed over something that is not a card object.
        return 'not_object'
    reason = _schema_reason(card)
    if reason:
        return reason
    for key in REQUIRED_STRING_FIELDS:
        reason = _required_field_reason(card, key)
        if reason:
            return reason
    for key in OPTIONAL_STRING_FIELDS:
        if key in card and not isinstance(card[key], str):
            return f'bad_type:{key}'
    if 'features' in card and not isinstance(card['features'], dict):
        return 'bad_type:features'
    return ''


def parse_card(raw: bytes) -> tuple[dict[str, Any] | None, str]:
    """Decode and validate raw card bytes: ``(card, '')`` or ``(None, reason)``."""
    card, reason = decode_card(raw)
    if card is None:
        return None, reason
    reason = validate_card(card)
    if reason:
        return None, reason
    return card, ''


def sanitize_worker_name(name: str) -> str:
    """Return the card file name stem: every character outside ``[a-zA-Z0-9_-]`` becomes ``_``.

    One underscore per character, not per byte, so ``'ünïcode'`` becomes
    ``'_n_code'``.
    """
    return _UNSAFE_NAME_CHARS.sub('_', name)
