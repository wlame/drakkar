"""Parameters of the agent routes and their error messages.

The messages are part of the route contract (``tests/fixtures/agent_contract.json``).
"""

from __future__ import annotations

from collections.abc import Sequence

AGENT_OFF_MESSAGE = 'the agent surface is off; set agent.enabled to true'
TOKEN_REQUIRED_MESSAGE = 'agent token required'
UNKNOWN_PART_MESSAGE = "unknown part '{part}'; known: {known}"
DETAIL_MESSAGE = 'detail must be brief or full'
FORMAT_MESSAGE = 'format must be json or markdown'
NO_SUCH_TOPIC_MESSAGE = "no such topic '{ref}'"
RULE_TIMED_OUT_MESSAGE = 'timed out after 1 s'
NO_RECORDER_STORE_NOTE = 'no recorder store on this worker'

MANUAL_PARTS = ('concepts', 'config', 'metrics', 'events', 'recipes', 'routes', 'rules', 'app')
STATE_PARTS = ('health', 'budget', 'partitions', 'sinks', 'recorder', 'autodiscover', 'recent_failures')
CONFIG_PARTS = (
    'worker',
    'cluster',
    'connections',
    'sources',
    'executor',
    'threads',
    'sinks',
    'dlq',
    'storage',
    'recorder',
    'cache',
    'autodiscover',
    'metrics',
    'logging',
    'runtime_health',
    'api',
    'ui',
    'agent',
    'app',
)

DETAIL_VALUES = ('brief', 'full')
FORMAT_VALUES = ('json', 'markdown')


class ParameterError(ValueError):
    """A route parameter that is not understood; the message is the 400 answer text."""


def parse_parts(raw: str | None, known: Sequence[str]) -> list[str]:
    """Return the asked parts in the order of ``known``; all of them when ``raw`` is absent.

    Raises ``ParameterError`` for the first item that is empty or not in ``known``.
    Repeated items are allowed and kept once.
    """
    if raw is None:
        return list(known)
    asked = raw.split(',')
    for item in asked:
        if item not in known:
            raise ParameterError(UNKNOWN_PART_MESSAGE.format(part=item, known=', '.join(sorted(known))))
    return [part for part in known if part in asked]


def parse_detail(raw: str | None) -> str:
    """Return ``brief`` (the default) or ``full``; raise ``ParameterError`` for another value."""
    if raw is None:
        return 'brief'
    if raw not in DETAIL_VALUES:
        raise ParameterError(DETAIL_MESSAGE)
    return raw


def parse_format(raw: str | None) -> str:
    """Return ``json`` (the default) or ``markdown``; raise ``ParameterError`` for another value."""
    if raw is None:
        return 'json'
    if raw not in FORMAT_VALUES:
        raise ParameterError(FORMAT_MESSAGE)
    return raw
