"""Redis key templates, Lua scripts and client helpers shared by the fleet providers.

The worker registry and the archive election both keep their state in Redis.
This module holds what the two have in common: the key layout, the six Lua
scripts, the encoding of the fragments that the scripts search for, the mapping
of Redis errors to the provider caller's retry rule, and the functions that
open, close and look up the shared client.

The key templates and the scripts are pinned by
``tests/fixtures/fleet_providers.json`` so the emitted bytes are identical for
any implementation. ``{prefix}``, ``{worker}`` and ``{scope}`` in a template
stand for the key prefix of the connection, the sanitized worker name and the
sanitized cluster. None of the three can contain a brace or a glob character
(the config loader and the sanitizers see to that), so a plain replace is safe.

Lua dialect rule: the scripts use only ``redis.call``, concatenation, ``#``,
``string.rep`` and a plain ``string.find``, and a number never becomes a key, a
score, a time to live or a return value by implicit conversion, so they behave
the same in every Lua version that a Redis server or an emulator embeds.

The client does no retrying and has no read time limit of its own. The
resilience block of the provider caller is the only retry and the only clock on
a call: an operation makes exactly ``1 + retries`` attempts, each with its own
limit, and a server that stops answering ends the call at that limit. Only the
connect is bounded by the client, at the fixed ``CONNECT_TIMEOUT_SECONDS``, so
a dial gives up at the same point for any implementation, whatever default its
client library has.

The URL of a connection cannot carry query options (``?...``): the client
applies them after its own settings, and the options of a URL are not the same
for every implementation, so one URL could behave differently in each. For the
same reason the raw path of a ``redis://`` or ``rediss://`` URL must be empty,
``/``, or ``/`` and ASCII digits. The client strips every ``/`` from the path
and reads the rest as a number, so ``/0/extra`` falls back to database 0 and
``/1/2`` becomes database 12. The rule keeps only the paths whose meaning is
plain.
"""

import json
import math
import re
from collections.abc import Awaitable, Callable
from typing import Any, cast
from urllib.parse import urlsplit

from drakkar.config.connections import RedisConnectionConfig
from drakkar.providers.connections import Connections
from drakkar.providers.resilience import TransientError

REDIS_PROVIDER = 'redis'
PREFIX_PLACEHOLDER = '{prefix}'
WORKER_PLACEHOLDER = '{worker}'
SCOPE_PLACEHOLDER = '{scope}'
# The prefix of a connection entry that names none, so a preset client gets the same default.
DEFAULT_KEY_PREFIX: str = RedisConnectionConfig.model_fields['key_prefix'].default

KEY_TEMPLATES: dict[str, str] = {
    'registry_index': '{prefix}fleet:workers',
    'registry_card': '{prefix}fleet:workers:{worker}',
    'election_lock': '{prefix}archive-locks:{scope}',
}

# Sent with EVAL, never EVALSHA: a script cache that a restart empties would need a fallback path.
SCRIPTS: dict[str, str] = {
    # KEYS: card, index. ARGV: card text, time to live in ms, member.
    'publish': '\n'.join(
        (
            "local now = redis.call('TIME')",
            "local score = now[1] .. '.' .. string.rep('0', 6 - #now[2]) .. now[2]",
            "redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])",
            "redis.call('ZADD', KEYS[2], score, ARGV[3])",
            "redis.call('PEXPIRE', KEYS[2], ARGV[2])",
            'return 1',
        )
    ),
    # KEYS: index. ARGV: card key prefix. Reply: seconds, micros, then member, score and card per live card.
    'list': '\n'.join(
        (
            "local now = redis.call('TIME')",
            'local reply = {now[1], now[2]}',
            "local entries = redis.call('ZRANGE', KEYS[1], '0', '-1', 'WITHSCORES')",
            'for i = 1, #entries, 2 do',
            "  local card = redis.call('GET', ARGV[1] .. entries[i])",
            '  if card then',
            '    reply[#reply + 1] = entries[i]',
            '    reply[#reply + 1] = entries[i + 1]',
            '    reply[#reply + 1] = card',
            '  else',
            "    redis.call('ZREM', KEYS[1], entries[i])",
            '  end',
            'end',
            'return reply',
        )
    ),
    # KEYS: card, index. ARGV: instance fragment, member.
    'withdraw': '\n'.join(
        (
            "local card = redis.call('GET', KEYS[1])",
            'if card and string.find(card, ARGV[1], 1, true) then',
            "  redis.call('DEL', KEYS[1])",
            "  redis.call('ZREM', KEYS[2], ARGV[2])",
            '  return 1',
            'end',
            'return 0',
        )
    ),
    # KEYS: lock. ARGV: body, token fragment, lease in ms. The holder keeps its body and gets a new lease.
    'acquire': '\n'.join(
        (
            "local body = redis.call('GET', KEYS[1])",
            'if not body then',
            "  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[3])",
            '  return 1',
            'end',
            'if string.find(body, ARGV[2], 1, true) then',
            "  redis.call('PEXPIRE', KEYS[1], ARGV[3])",
            '  return 1',
            'end',
            'return 0',
        )
    ),
    # KEYS: lock. ARGV: token fragment, lease in ms.
    'refresh': '\n'.join(
        (
            "local body = redis.call('GET', KEYS[1])",
            'if body and string.find(body, ARGV[1], 1, true) then',
            "  redis.call('PEXPIRE', KEYS[1], ARGV[2])",
            '  return 1',
            'end',
            'return 0',
        )
    ),
    # KEYS: lock. ARGV: token fragment.
    'release': '\n'.join(
        (
            "local body = redis.call('GET', KEYS[1])",
            'if body and string.find(body, ARGV[1], 1, true) then',
            "  redis.call('DEL', KEYS[1])",
            '  return 1',
            'end',
            'return 0',
        )
    ),
}

# A server reply is judged by its code alone. These four can pass by themselves
# (a script still running, a dataset still loading, a replica whose primary is
# away, a key request that hit a moving slot); every other code, WRONGPASS and
# NOPERM included, is final.
TRANSIENT_REPLY_CODES = ('BUSY', 'LOADING', 'MASTERDOWN', 'TRYAGAIN')

CONNECT_TIMEOUT_SECONDS = 5.0
RESP_PROTOCOL = 2

# Parse errors of the client can quote the URL, and a piece of a password is a leak.
BAD_URL_MESSAGE = 'redis connection: the url cannot be parsed'
# The client applies the options of a URL query after its own settings, so a query could undo them.
URL_QUERY_MESSAGE = 'redis connection: the url cannot carry query options'
# The schemes whose URL path is the database number (a unix:// path is the socket).
DATABASE_PATH_SCHEMES = ('redis', 'rediss')
# Empty, '/', or '/' and ASCII digits, matched on the raw path. The client strips every '/' and
# reads the rest as a number ('/0/extra' falls back to 0, '/1/2' is 12), so only these paths are plain.
DATABASE_PATH_RE = re.compile(r'(/[0-9]*)?')


def key(name: str, *, prefix: str, worker: str = '', scope: str = '') -> str:
    """Return the key of ``name`` (a key of :data:`KEY_TEMPLATES`) with the three placeholders replaced."""
    return (
        KEY_TEMPLATES[name]
        .replace(PREFIX_PLACEHOLDER, prefix)
        .replace(WORKER_PLACEHOLDER, worker)
        .replace(SCOPE_PLACEHOLDER, scope)
    )


def fragment(field: str, value: str) -> str:
    """Return ``"<field>":<value>`` with the value encoded as the card encodes a string.

    A script searches a stored card or lock body for this text to tell whose it
    is, so the encoding must be the one of ``drakkar.fleet.card.serialize_card``:
    quoted, non-ASCII characters left as they are.
    """
    return f'"{field}":{json.dumps(value, ensure_ascii=False)}'


def milliseconds(seconds: float) -> str:
    """Return ``seconds`` as whole milliseconds in a decimal string: rounded half up, at least 1.

    ``PX`` and ``PEXPIRE`` refuse zero, and the string is what goes on the wire.
    """
    return str(max(1, math.floor(seconds * 1000 + 0.5)))


def is_transient_reply(message: str) -> bool:
    """Tell whether the first word of a server reply is a code that a retry can cure."""
    code, _, _ = message.partition(' ')
    return code in TRANSIENT_REPLY_CODES


def _reply_text(exc: Exception) -> str | None:
    """Return a server reply as the server wrote it, code first, or ``None`` when ``exc`` is not a reply.

    redis-py strips the code from the message of the replies it maps to a class
    and keeps it in ``status_code``; any other reply keeps its text whole.
    """
    from redis.exceptions import ResponseError

    status_code = getattr(exc, 'status_code', None)
    if status_code:
        return f'{status_code} {exc}'
    if isinstance(exc, ResponseError):
        return str(exc)
    return None


def _wrapped(kind: type[Exception], original: Exception) -> Exception:
    """Return ``kind`` with the text of ``original``, which becomes its ``__cause__``."""
    replacement = kind(str(original))
    replacement.__cause__ = original
    return replacement


def classify(exc: Exception) -> Exception:
    """Map a redis-py error to the provider caller's retry rule.

    A server reply is decided by its code before its class is looked at: a
    transient code becomes a ``TransientError``, any other code leaves the error
    as it is (so ``ERR max number of clients reached``, which redis-py raises as
    a ``ConnectionError``, is final). Of the errors that are not replies, a
    client timeout becomes the built-in ``TimeoutError``, an authentication or
    authorization error stays final, and any other connection error becomes a
    ``TransientError``. A wrapped error keeps the original as ``__cause__``;
    anything else is returned unchanged.
    """
    from redis.exceptions import AuthenticationError, AuthorizationError, RedisError
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    if not isinstance(exc, RedisError):
        return exc
    reply = _reply_text(exc)
    if reply is not None:
        return _wrapped(TransientError, exc) if is_transient_reply(reply) else exc
    if isinstance(exc, RedisTimeoutError):
        return _wrapped(TimeoutError, exc)
    if isinstance(exc, AuthenticationError | AuthorizationError):
        return exc
    if isinstance(exc, RedisConnectionError):
        return _wrapped(TransientError, exc)
    return exc


async def open_client(settings: RedisConnectionConfig) -> Any:
    """Return the client of a connection entry; nothing is connected until the first command.

    Client retries are off, reads and writes have no time limit of their own and
    only the connect has one (see the module docstring). The read limit is set to
    ``None`` on purpose: the client library's default is a few seconds. The
    protocol is pinned to RESP2, because the library's default is RESP3, which
    opens every connection with ``HELLO`` (a command that Redis before 6.0 does
    not have), and because every reply then has the shape the scripts and the
    tests expect. A URL with a ``?`` is refused with ``URL_QUERY_MESSAGE`` before
    it is parsed: the client applies the options of a URL query after its own
    settings, so a query such as ``socket_timeout`` would undo the ones set here.
    A URL that does not parse raises ``ValueError(BAD_URL_MESSAGE)``, and so does
    a ``redis://`` or ``rediss://`` URL whose raw path is not empty, ``/`` or
    ``/`` and ASCII digits (see the module docstring). Both errors have no
    cause and quote nothing, because the
    client's own message can quote the URL.
    """
    from redis.asyncio import Redis
    from redis.asyncio.retry import Retry
    from redis.backoff import NoBackoff

    if '?' in settings.url:
        raise ValueError(URL_QUERY_MESSAGE)
    try:
        _check_database_path(settings.url)
        return Redis.from_url(
            settings.url,
            retry=Retry(NoBackoff(), 0),
            socket_connect_timeout=CONNECT_TIMEOUT_SECONDS,
            socket_timeout=None,
            protocol=RESP_PROTOCOL,
        )
    except ValueError:
        raise ValueError(BAD_URL_MESSAGE) from None


def _check_database_path(url: str) -> None:
    """Raise ``ValueError`` when the path of a ``redis://`` or ``rediss://`` URL is not a database number.

    The path may be empty, ``/``, or ``/`` and ASCII digits. ``urlsplit`` keeps
    a ``;`` in the path and does not percent-decode it, so ``/0;x`` and ``/%30``
    are refused too. The caller replaces the error with ``BAD_URL_MESSAGE``, so
    the text here never reaches a log.
    """
    parts = urlsplit(url)
    if parts.scheme in DATABASE_PATH_SCHEMES and DATABASE_PATH_RE.fullmatch(parts.path) is None:
        raise ValueError('the url path is not a database number')


async def close_client(client: Any) -> None:
    """Close the client and the connections of its pool."""
    await client.aclose()


async def run(client: Any, *argv: str) -> Any:
    """Send one command and return its reply.

    A transient error leaves as ``TransientError`` and a client timeout as the
    built-in ``TimeoutError``; any other error leaves as it came (see :func:`classify`).

    Every argument is a ``str``, the counts and the milliseconds included, so
    the command on the wire equals the pinned one exactly.
    """
    try:
        return await client.execute_command(*argv)
    except Exception as exc:
        classified = classify(exc)
        if classified is exc:
            raise
        raise classified from exc


async def resolve_client(
    *,
    connections: Connections | None,
    connection: str,
    preset_client: Any | None,
    preset_prefix: str,
    open_client: Callable[[Any], Awaitable[Any]],
    close_client: Callable[[Any], Awaitable[None]],
    who: str,
) -> tuple[Any, str]:
    """Return the client a provider uses and the key prefix that goes with it.

    A preset client (a test's) is used as it is, with ``preset_prefix``. Otherwise
    the client is the shared one of ``connections.redis.<connection>``, opened by
    ``open_client`` on the first call and closed by ``close_client`` when the
    connections close, with the ``key_prefix`` of that entry. ``who`` names the
    provider in the error when there is neither.
    """
    if preset_client is not None:
        return preset_client, preset_prefix
    if connections is None:
        raise RuntimeError(f'the redis {who} needs the worker connections')
    settings = cast(RedisConnectionConfig, connections.settings(REDIS_PROVIDER, connection))
    client = await connections.acquire(REDIS_PROVIDER, connection, open_client, close_client)
    return client, settings.key_prefix
