"""Consul request corpus and client helpers shared by the fleet providers.

The worker registry and the archive election both keep their state in the KV
store of a Consul agent, each key held by a session whose time to live is the
liveness clock. This module holds what the two have in common: the request
paths, the session names and settings, the session TTL rule, the checks of the
connection's address and token, the mapping of answers and transport errors to
the provider caller's retry rule, the HTTP client, and one function per agent
request (the leader, the sessions, the keys).

The request shapes are pinned by ``tests/fixtures/fleet_providers.json`` so the
bytes on the wire are identical for any implementation: the method, the path,
the query pairs sorted by name and the body of every request.

No Consul client library is used. The providers need a handful of requests of
the agent's HTTP API, and a library brings its own retries, time limits and URL
building, each of which would have to be pinned and turned off. The client does
no retrying and follows no redirect, and only the connect is bounded, at the
fixed ``CONNECT_TIMEOUT_SECONDS``: the resilience block of the provider caller
is the only retry and the only clock on a call.

Nothing is percent-encoded. Every piece that goes into a path or a query is
checked before it gets there: the key prefix and the datacenter by the config
loader, the worker name and the scope by their sanitizers, and a session id
against :data:`SESSION_ID_RE`. So the URL is built by plain concatenation, and
no quoting rule, which differs between URL libraries, ever applies.

A session's TTL is half the limit it stands for, and at least 10 s
(:func:`session_ttl`): a real agent ended a session of a 10 s TTL that nobody
renewed only 20 s after its last renew, at twice its TTL, so half the limit
ends a session by the limit.
"""

import base64
import json
import math
import re
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import httpx

from drakkar.config.connections import (
    CONSUL_PREFIX_RULE,
    ConsulConnectionConfig,
    has_dot_segment,
    is_consul_key_prefix,
)
from drakkar.providers.connections import Connections
from drakkar.providers.resilience import TransientError
from drakkar.providers.urlcheck import PRINTABLE_ASCII_RE, is_portable_host_and_port

CONSUL_PROVIDER = 'consul'
PREFIX_PLACEHOLDER = '{prefix}'
WORKER_PLACEHOLDER = '{worker}'
SCOPE_PLACEHOLDER = '{scope}'
SESSION_PLACEHOLDER = '{session}'
INSTANCE_PLACEHOLDER = '{instance}'
TOKEN_PLACEHOLDER = '{token}'
# The prefix of a connection entry that names none, so a preset client gets the same default.
DEFAULT_KEY_PREFIX: str = ConsulConnectionConfig.model_fields['key_prefix'].default

PATHS: dict[str, str] = {
    'leader': '/v1/status/leader',
    'session_create': '/v1/session/create',
    'session_renew': '/v1/session/renew/{session}',
    'session_destroy': '/v1/session/destroy/{session}',
    'session_info': '/v1/session/info/{session}',
    'registry_card': '/v1/kv/{prefix}/fleet/workers/{worker}',
    'registry_list': '/v1/kv/{prefix}/fleet/workers/',
    'election_lock': '/v1/kv/{prefix}/archive-locks/{scope}',
}
# The head of every KV request path; the rest of the path is the KV key.
KV_PATH_HEAD = '/v1/kv/'

SESSION_NAMES: dict[str, str] = {
    'registry': 'drakkar-{worker}-{instance}',
    'election': 'drakkar/archive-lock/{scope}/{token}',
}

# A session that ends deletes the keys it holds, and another session may take a key at once.
SESSION_SETTINGS: dict[str, str] = {'Behavior': 'delete', 'LockDelay': '0s'}

TOKEN_HEADER = 'X-Consul-Token'
# The agent sets this header to 'true' on an answer from which the token's rights hid results.
FILTERED_HEADER = 'X-Consul-Results-Filtered-By-Acls'
FILTERED_HEADER_VALUE = 'true'

# A rate limit, a server error (a raft apply that failed, a session that vanished under a request)
# and the three answers of a proxy or a server that is not ready. Every other status is final.
TRANSIENT_STATUSES = (429, 500, 502, 503, 504)

CONNECT_TIMEOUT_SECONDS = 5.0
# The characters of a server's error text that an error message keeps.
STATUS_TEXT_LIMIT = 200

SESSION_ID_RE = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
# A worker instance is 16 lowercase hex characters; it ends the session name of a registry card.
INSTANCE_RE = re.compile(r'[0-9a-f]{16}')
INSTANCE_LENGTH = 16
# Consul refuses a session TTL under 10 s with a 500.
MIN_SESSION_TTL_SECONDS = 10

ADDRESS_SCHEMES = ('http', 'https')
# The path of an address, its trailing '/' dropped: nothing, or '/' and a segment, repeated.
# Every class is ASCII and spelled out, never \w, which matches more than ASCII in Python.
ADDRESS_PATH_RE = re.compile(r'(/[A-Za-z0-9._~-]+)*')

# Parse errors could quote the address, so the message is fixed.
BAD_ADDRESS_MESSAGE = 'consul connection: the address cannot be parsed'
# The message never quotes the token.
BAD_TOKEN_MESSAGE = 'consul connection: the token cannot be sent in a header'
NO_LEADER_MESSAGE = 'consul: the cluster has no leader'
BAD_SESSION_ID_MESSAGE = 'consul: the answer carries no valid session id'
CARD_HELD_MESSAGE = 'consul registry: the card key of {worker} is held by a session of another worker'
REFRESH_BEFORE_ACQUIRE_MESSAGE = 'consul election: refresh of a scope this provider has not acquired'
STATUS_MESSAGE = 'consul: {method} {path} answered {status}: {text}'
STATUS_NO_TEXT_MESSAGE = 'consul: {method} {path} answered {status}'
FILTERED_MESSAGE = "consul: {method} {path} answered with results hidden by the token's rights"
UNREADABLE_MESSAGE = 'consul: {method} {path} answered {status} with an answer that cannot be read'
KEY_PREFIX_MESSAGE = "key_prefix '{prefix}' " + CONSUL_PREFIX_RULE


class ConsulError(Exception):
    """A final error of the agent API: a status that a retry cannot cure, or an answer that cannot be used."""


@dataclass(frozen=True)
class ConsulClient:
    """The HTTP client of one connection entry and what every request adds to it.

    ``base`` is the checked address without its trailing ``/``; ``token`` (sent
    in :data:`TOKEN_HEADER` when not empty) and ``datacenter`` (sent as ``dc``
    when not empty) come from the connection entry.
    """

    http: httpx.AsyncClient
    base: str
    # Kept out of the repr, so a client in a log line or a test failure shows no secret.
    token: str = field(repr=False)
    datacenter: str


def path(name: str, *, prefix: str, worker: str = '', scope: str = '', session: str = '') -> str:
    """Return the path of ``name`` (a key of :data:`PATHS`) with the placeholders replaced.

    None of the values can contain a brace (the config loader, the sanitizers
    and :data:`SESSION_ID_RE` see to that), so a plain replace is safe. A
    placeholder of the template with an empty value raises ``ValueError``:
    a card path without a worker would otherwise be the list path.
    """
    template = PATHS[name]
    values = {
        PREFIX_PLACEHOLDER: prefix,
        WORKER_PLACEHOLDER: worker,
        SCOPE_PLACEHOLDER: scope,
        SESSION_PLACEHOLDER: session,
    }
    rendered = template
    for placeholder, value in values.items():
        if placeholder not in template:
            continue
        if not value:
            raise ValueError(f'the consul path {name} needs a value for {placeholder}')
        rendered = rendered.replace(placeholder, value)
    return rendered


def session_ttl(seconds: float) -> str:
    """Return the TTL of a session that stands for a limit of ``seconds``: half of it, rounded up, at least 10 s.

    The agent ends a session about twice its TTL after the last renew, so half
    the limit ends it by the limit (see the module docstring).
    """
    return f'{max(MIN_SESSION_TTL_SECONDS, math.ceil(seconds / 2))}s'


def session_body(name: str, ttl: str) -> str:
    """Return the body of a session create: :data:`SESSION_SETTINGS` with ``Name`` and ``TTL``.

    The encoding is the one of the card and of the lock body (sorted keys,
    compact, non-ASCII characters left as they are). It is written out here
    rather than imported from the recorder, because the recorder imports this
    package and an import back would close a cycle.
    """
    return json.dumps(
        {**SESSION_SETTINGS, 'Name': name, 'TTL': ttl}, sort_keys=True, separators=(',', ':'), ensure_ascii=False
    )


def registry_session_name(member: str, instance: str) -> str:
    """Return the name of the session that holds the card of ``member`` (a sanitized worker name) for ``instance``."""
    return SESSION_NAMES['registry'].replace(WORKER_PLACEHOLDER, member).replace(INSTANCE_PLACEHOLDER, instance)


def election_session_name(scope: str, token: str) -> str:
    """Return the name of the session that holds the lease of ``token`` on ``scope`` (a sanitized cluster name)."""
    return SESSION_NAMES['election'].replace(SCOPE_PLACEHOLDER, scope).replace(TOKEN_PLACEHOLDER, token)


def holder_of(entry: Mapping[str, Any]) -> str | None:
    """Return the id of the session that holds a KV entry, or ``None`` when no session holds it.

    An entry is held while its ``Session`` is a non-empty string. The agent
    leaves the field out of an entry that no session holds; a ``null`` or an
    empty string reads the same, so a reader never has to tell an absent field
    from an empty one.
    """
    session = entry.get('Session')
    return session if isinstance(session, str) and session else None


def is_own_session_name(name: str, member: str) -> bool:
    """Tell whether ``name`` is the session name of a registry card of ``member`` (a sanitized worker name).

    The name is read from the right: its last 16 characters are the instance,
    which must be lowercase hex, and the whole name must be the registry
    template with ``member`` and that instance. So the session of worker
    ``w-1`` is never one of worker ``w``, and an election session (its name has
    a ``/``, which no sanitized name has) is never one of any worker.
    """
    instance = name[-INSTANCE_LENGTH:]
    if INSTANCE_RE.fullmatch(instance) is None:
        return False
    return name == registry_session_name(member, instance)


def base_url(address: str) -> str:
    """Return the base URL every request path is appended to: ``address`` without its trailing ``/``.

    The address is checked by hand, so every implementation judges the same
    pieces whatever its URL parser does. The rules:

    - every character is printable ASCII, and there is no ``@`` (no user
      info), no ``?`` and no ``#``;
    - the scheme is ``http`` or ``https``, in any case, followed by ``//``;
    - the host and the port pass the shared rule
      (:func:`drakkar.providers.urlcheck.is_portable_host_and_port`), the host
      is not empty, and a ``:`` is followed by a port;
    - the path is empty, or segments of letters, digits, ``.``, ``_``, ``~``
      and ``-``, each after one ``/``, with any number of trailing ``/``; no
      segment is exactly ``.`` or ``..`` (a segment such as ``..x`` or
      ``v1.2`` is kept).

    Any other address raises ``ValueError(BAD_ADDRESS_MESSAGE)``, which quotes
    nothing of the address and has no cause.
    """
    if not _is_portable_address(address):
        raise ValueError(BAD_ADDRESS_MESSAGE) from None
    return address.rstrip('/')


def _is_portable_address(address: str) -> bool:
    """Tell whether ``address`` passes the address check of :func:`base_url`."""
    if PRINTABLE_ASCII_RE.fullmatch(address) is None or any(character in address for character in '@?#'):
        return False
    scheme, separator, rest = address.partition('://')
    if not separator or scheme.lower() not in ADDRESS_SCHEMES:
        return False
    host_and_port, slash, tail = rest.partition('/')
    # The shared rule takes an empty host and a ':' with no port; URL parsers read either differently.
    if not host_and_port.partition(':')[0] or host_and_port.endswith(':'):
        return False
    path = (slash + tail).rstrip('/')
    if ADDRESS_PATH_RE.fullmatch(path) is None or has_dot_segment(path):
        return False
    return is_portable_host_and_port(host_and_port)


def check_key_prefix(prefix: str) -> None:
    """Raise ``ValueError(KEY_PREFIX_MESSAGE)`` unless ``prefix`` has the shape a connection entry allows.

    A provider checks a preset prefix with it, since such a prefix skips the
    config loader and goes into the paths as it is.
    """
    if not is_consul_key_prefix(prefix):
        raise ValueError(KEY_PREFIX_MESSAGE.format(prefix=prefix))


def check_token(token: str) -> None:
    """Raise ``ValueError(BAD_TOKEN_MESSAGE)`` unless ``token`` is empty or printable ASCII without a space.

    Any other character cannot travel in an HTTP header the same way for every client.
    """
    if PRINTABLE_ASCII_RE.fullmatch(token) is None:
        raise ValueError(BAD_TOKEN_MESSAGE)


def status_error(method: str, path: str, status: int, body: bytes) -> Exception:
    """Return the error of an answer whose status the request does not expect.

    The text is the first line of the body (up to the first line feed, the body
    read as UTF-8 with a replacement character for a byte that is not), cut to
    :data:`STATUS_TEXT_LIMIT` characters; an empty text gives
    :data:`STATUS_NO_TEXT_MESSAGE`. A status of :data:`TRANSIENT_STATUSES` gives
    a ``TransientError``, any other a :class:`ConsulError`. ``path`` is the
    request path without the query.
    """
    text = body.decode('utf-8', errors='replace').partition('\n')[0][:STATUS_TEXT_LIMIT]
    if text:
        message = STATUS_MESSAGE.format(method=method, path=path, status=status, text=text)
    else:
        message = STATUS_NO_TEXT_MESSAGE.format(method=method, path=path, status=status)
    kind = TransientError if status in TRANSIENT_STATUSES else ConsulError
    return kind(message)


def _wrapped(kind: type[Exception], original: Exception) -> Exception:
    """Return ``kind`` with the text of ``original``, which becomes its ``__cause__``."""
    replacement = kind(str(original))
    replacement.__cause__ = original
    return replacement


def classify(exc: Exception) -> Exception:
    """Map a transport error of the HTTP client to the provider caller's retry rule.

    A time limit of the client becomes the built-in ``TimeoutError``; any other
    transport error (a refused or broken connection, a protocol error) becomes a
    ``TransientError``. A wrapped error keeps the original as ``__cause__``;
    anything else is returned unchanged.
    """
    if isinstance(exc, httpx.TimeoutException):
        return _wrapped(TimeoutError, exc)
    if isinstance(exc, httpx.TransportError):
        return _wrapped(TransientError, exc)
    return exc


async def open_client(
    settings: ConsulConnectionConfig, *, transport: httpx.AsyncBaseTransport | None = None
) -> ConsulClient:
    """Return the client of a connection entry; nothing is connected until the first request.

    The address and the token are checked first (:func:`base_url`,
    :func:`check_token`). The client follows no redirect, and only the connect
    has a time limit (see the module docstring). Proxies and the trust store
    are the HTTP library's defaults, as for the HTTP sink: the proxy
    environment variables and ``SSL_CERT_FILE``/``SSL_CERT_DIR`` apply.
    ``transport`` replaces the network transport (a test's).
    """
    base = base_url(settings.address)
    check_token(settings.token)
    http = httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(None, connect=CONNECT_TIMEOUT_SECONDS),
        transport=transport,
    )
    return ConsulClient(http=http, base=base, token=settings.token, datacenter=settings.datacenter)


async def close_client(client: ConsulClient) -> None:
    """Close the HTTP client and its connections."""
    await client.http.aclose()


async def request(
    client: ConsulClient,
    method: str,
    path: str,
    *,
    query: Sequence[tuple[str, str]] = (),
    body: str | None = None,
    answers: Collection[int] = (200,),
) -> tuple[int, bytes]:
    """Send one request and return its status and body.

    The URL is the base, the path and, when there are pairs, ``?`` and the
    pairs as ``name=value`` joined by ``&``, sorted by name (then value);
    ``dc`` is one of them when the client has a datacenter. The token travels
    in :data:`TOKEN_HEADER` when it is not empty. ``body`` is sent as UTF-8.

    An answer that carries :data:`FILTERED_HEADER` with the value ``true`` (the
    header name in any letter case) raises ``ConsulError(FILTERED_MESSAGE)``,
    whatever its status: the token's rights hid results, and an empty answer
    must not pass for an empty fleet or a session that ended. Then a status
    outside ``answers`` raises :func:`status_error`. A transport error leaves
    as :func:`classify` maps it. A body that the HTTP library cannot decode
    (a ``Content-Encoding`` that does not match the bytes) raises
    ``ConsulError(UNREADABLE_MESSAGE)`` naming the request and its status,
    before the header and the status are judged.
    """
    pairs = [*query, ('dc', client.datacenter)] if client.datacenter else list(query)
    url = client.base + path
    if pairs:
        url += '?' + '&'.join(f'{name}={value}' for name, value in sorted(pairs))
    headers = {TOKEN_HEADER: client.token} if client.token else {}
    content = None if body is None else body.encode('utf-8')
    try:
        # Streamed and then read, so a body that cannot be decoded is caught while the status is known:
        # the library's decoding error carries no response.
        async with client.http.stream(method, url, content=content, headers=headers) as response:
            try:
                await response.aread()
            except httpx.DecodingError as exc:
                status = response.status_code
                raise ConsulError(UNREADABLE_MESSAGE.format(method=method, path=path, status=status)) from exc
    except httpx.TransportError as exc:
        raise classify(exc) from exc
    # Header names are matched in any letter case by the HTTP library.
    if response.headers.get(FILTERED_HEADER) == FILTERED_HEADER_VALUE:
        raise ConsulError(FILTERED_MESSAGE.format(method=method, path=path))
    if response.status_code not in answers:
        raise status_error(method, path, response.status_code, response.content)
    return response.status_code, response.content


def _read_answer[T](answer: bytes, reader: Callable[[Any], T], *, method: str, path: str, status: int) -> T:
    """Return ``reader`` applied to the JSON value of ``answer``.

    An answer that is not JSON, or whose shape ``reader`` refuses (it raises
    ``TypeError``, ``KeyError``, ``IndexError`` or ``ValueError``), raises
    ``ConsulError(UNREADABLE_MESSAGE)`` naming the request, never the parser's
    own error.
    """
    try:
        return reader(json.loads(answer))
    except (ValueError, TypeError, KeyError, IndexError) as exc:
        raise ConsulError(UNREADABLE_MESSAGE.format(method=method, path=path, status=status)) from exc


async def _ask[T](
    client: ConsulClient,
    method: str,
    path: str,
    reader: Callable[[Any], T],
    *,
    query: Sequence[tuple[str, str]] = (),
    body: str | None = None,
) -> T:
    """Send a request that must answer ``200`` and return ``reader`` of its answer (see :func:`_read_answer`)."""
    status, answer = await request(client, method, path, query=query, body=body)
    return _read_answer(answer, reader, method=method, path=path, status=status)


def _string(value: Any) -> str:
    """Return ``value`` when it is a JSON string."""
    if not isinstance(value, str):
        raise TypeError('not a string')
    return value


def _flag(value: Any) -> bool:
    """Return ``value`` when it is a JSON boolean."""
    if not isinstance(value, bool):
        raise TypeError('not a boolean')
    return value


def _objects(value: Any) -> list[dict[str, Any]]:
    """Return ``value`` when it is a JSON list of objects."""
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise TypeError('not a list of objects')
    return value


def _created_id(value: Any) -> Any:
    """Return the ``ID`` of a session create's answer, an object; the id is checked by :func:`_session_id`."""
    if not isinstance(value, dict):
        raise TypeError('not an object')
    return value['ID']


def _first_session_name(value: Any) -> str | None:
    """Return the ``Name`` of the first session of an ``info`` answer, or ``None`` for ``[]``."""
    sessions = _objects(value)
    return _string(sessions[0]['Name']) if sessions else None


def _decoded_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return a KV entry with its base64 ``Value`` decoded to bytes; a null value is ``b''``.

    The entry must name its key with a string ``Key``: a listing reads the
    worker name from it.
    """
    _string(entry['Key'])
    value = entry.get('Value')
    return {**entry, 'Value': b'' if value is None else base64.b64decode(_string(value), validate=True)}


def _entries(value: Any) -> list[dict[str, Any]]:
    """Return the KV entries of a read's answer, each decoded."""
    return [_decoded_entry(entry) for entry in _objects(value)]


def _first_entry(value: Any) -> dict[str, Any]:
    """Return the one KV entry of a single-key read's answer."""
    return _entries(value)[0]


def _session_id(value: object) -> str:
    """Return ``value`` when it is a session id that may enter a path or a query; raise :class:`ConsulError` if not."""
    if not isinstance(value, str) or SESSION_ID_RE.fullmatch(value) is None:
        raise ConsulError(BAD_SESSION_ID_MESSAGE)
    return value


async def check_leader(client: ConsulClient) -> None:
    """Raise ``TransientError(NO_LEADER_MESSAGE)`` unless the agent names a leader of its cluster.

    The leader request needs no ACL right, so it says the agent answers and the
    cluster can write; it says nothing about the rights of the token. An answer
    that is not a JSON string is unreadable (:data:`UNREADABLE_MESSAGE`).
    """
    if not await _ask(client, 'GET', PATHS['leader'], _string):
        raise TransientError(NO_LEADER_MESSAGE)


async def create_session(client: ConsulClient, name: str, limit_seconds: float) -> str:
    """Create a session named ``name`` whose TTL stands for ``limit_seconds``; return its id.

    An answer that is not an object with an ``ID`` is unreadable
    (:data:`UNREADABLE_MESSAGE`); an ``ID`` that does not match
    :data:`SESSION_ID_RE` raises ``ConsulError(BAD_SESSION_ID_MESSAGE)``,
    because the id goes into later paths.
    """
    body = session_body(name, session_ttl(limit_seconds))
    return _session_id(await _ask(client, 'PUT', PATHS['session_create'], _created_id, body=body))


async def renew_session(client: ConsulClient, session: str) -> bool:
    """Renew ``session``; ``False`` when the agent no longer knows it (``404``)."""
    status, _ = await request(
        client, 'PUT', path('session_renew', prefix='', session=_session_id(session)), answers=(200, 404)
    )
    return status == 200


async def destroy_session(client: ConsulClient, session: str) -> None:
    """Destroy ``session``; the keys it holds go with it. A session that is already gone is no error."""
    await request(client, 'PUT', path('session_destroy', prefix='', session=_session_id(session)))


async def session_name(client: ConsulClient, session: str) -> str | None:
    """Return the name of ``session``, or ``None`` when the agent no longer knows it (``200 []``)."""
    return await _ask(client, 'GET', path('session_info', prefix='', session=_session_id(session)), _first_session_name)


async def read_key(client: ConsulClient, key_path: str) -> dict[str, Any] | None:
    """Return the KV entry at ``key_path`` (a path of :data:`PATHS`), or ``None`` when there is none (``404``).

    ``Value`` is decoded to bytes; :func:`holder_of` tells which session holds the key, if any.
    """
    status, answer = await request(client, 'GET', key_path, answers=(200, 404))
    if status == 404:
        return None
    return _read_answer(answer, _first_entry, method='GET', path=key_path, status=status)


async def read_prefix(client: ConsulClient, prefix_path: str) -> list[dict[str, Any]]:
    """Return the KV entries under ``prefix_path`` in key order, decoded as :func:`read_key` decodes one.

    An empty prefix answers ``404``, which is no entry.
    """
    status, answer = await request(client, 'GET', prefix_path, query=(('recurse', 'true'),), answers=(200, 404))
    if status == 404:
        return []
    return _read_answer(answer, _entries, method='GET', path=prefix_path, status=status)


async def acquire_key(client: ConsulClient, key_path: str, session: str, body: str) -> bool:
    """Write ``body`` to the key at ``key_path`` held by ``session``; ``False`` when another session holds it.

    The session that already holds the key gets ``True`` again and replaces the
    value. A session the agent does not know answers ``500``, a ``TransientError``.
    """
    return await _ask(client, 'PUT', key_path, _flag, query=(('acquire', _session_id(session)),), body=body)


async def resolve_client(
    *,
    connections: Connections | None,
    connection: str,
    preset_client: ConsulClient | None,
    preset_prefix: str,
    open_client: Callable[[Any], Awaitable[ConsulClient]],
    close_client: Callable[[ConsulClient], Awaitable[None]],
    who: str,
) -> tuple[ConsulClient, str]:
    """Return the client a provider uses and the key prefix that goes with it.

    A preset client (a test's) is used as it is, with ``preset_prefix``. Otherwise
    the client is the shared one of ``connections.consul.<connection>``, opened by
    ``open_client`` on the first call and closed by ``close_client`` when the
    connections close, with the ``key_prefix`` of that entry. ``who`` names the
    provider in the error when there is neither.
    """
    if preset_client is not None:
        return preset_client, preset_prefix
    if connections is None:
        raise RuntimeError(f'the consul {who} needs the worker connections')
    settings = cast(ConsulConnectionConfig, connections.settings(CONSUL_PROVIDER, connection))
    client = await connections.acquire(CONSUL_PROVIDER, connection, open_client, close_client)
    return client, settings.key_prefix
