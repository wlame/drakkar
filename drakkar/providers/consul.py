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
from collections.abc import Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

import httpx

from drakkar.config.connections import ConsulConnectionConfig
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

SESSION_NAMES: dict[str, str] = {
    'registry': 'drakkar-{worker}-{instance}',
    'election': 'drakkar/archive-lock/{scope}/{token}',
}

# A session that ends deletes the keys it holds, and another session may take a key at once.
SESSION_SETTINGS: dict[str, str] = {'Behavior': 'delete', 'LockDelay': '0s'}

TOKEN_HEADER = 'X-Consul-Token'

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
    and :data:`SESSION_ID_RE` see to that), so a plain replace is safe.
    """
    return (
        PATHS[name]
        .replace(PREFIX_PLACEHOLDER, prefix)
        .replace(WORKER_PLACEHOLDER, worker)
        .replace(SCOPE_PLACEHOLDER, scope)
        .replace(SESSION_PLACEHOLDER, session)
    )


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
    return name == SESSION_NAMES['registry'].replace(WORKER_PLACEHOLDER, member).replace(INSTANCE_PLACEHOLDER, instance)


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
      and ``-``, each after one ``/``, with any number of trailing ``/``.

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
    return (
        is_portable_host_and_port(host_and_port) and ADDRESS_PATH_RE.fullmatch((slash + tail).rstrip('/')) is not None
    )


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

    A status outside ``answers`` raises :func:`status_error`; a transport error
    leaves as :func:`classify` maps it.
    """
    pairs = [*query, ('dc', client.datacenter)] if client.datacenter else list(query)
    url = client.base + path
    if pairs:
        url += '?' + '&'.join(f'{name}={value}' for name, value in sorted(pairs))
    headers = {TOKEN_HEADER: client.token} if client.token else {}
    content = None if body is None else body.encode('utf-8')
    try:
        response = await client.http.request(method, url, content=content, headers=headers)
    except httpx.TransportError as exc:
        raise classify(exc) from exc
    if response.status_code not in answers:
        raise status_error(method, path, response.status_code, response.content)
    return response.status_code, response.content


def _json_or_none(answer: bytes) -> Any:
    """Return the JSON value of ``answer``, or ``None`` when it is not JSON."""
    try:
        return json.loads(answer)
    except ValueError:
        return None


def _session_id(value: object) -> str:
    """Return ``value`` when it is a session id that may enter a path or a query; raise :class:`ConsulError` if not."""
    if not isinstance(value, str) or SESSION_ID_RE.fullmatch(value) is None:
        raise ConsulError(BAD_SESSION_ID_MESSAGE)
    return value


def _decoded_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return a KV entry with its base64 ``Value`` decoded to bytes; a null value is ``b''``."""
    value = entry.get('Value')
    return {**entry, 'Value': b'' if value is None else base64.b64decode(value, validate=True)}


async def check_leader(client: ConsulClient) -> None:
    """Raise ``TransientError(NO_LEADER_MESSAGE)`` unless the agent names a leader of its cluster.

    The leader request needs no ACL right, so it says the agent answers and the
    cluster can write; it says nothing about the rights of the token.
    """
    _, answer = await request(client, 'GET', PATHS['leader'])
    leader = _json_or_none(answer)
    if not isinstance(leader, str) or not leader:
        raise TransientError(NO_LEADER_MESSAGE)


async def create_session(client: ConsulClient, name: str, limit_seconds: float) -> str:
    """Create a session named ``name`` whose TTL stands for ``limit_seconds``; return its id.

    An answer without an id that matches :data:`SESSION_ID_RE` raises
    ``ConsulError(BAD_SESSION_ID_MESSAGE)``: the id goes into later paths.
    """
    _, answer = await request(
        client, 'PUT', PATHS['session_create'], body=session_body(name, session_ttl(limit_seconds))
    )
    created = _json_or_none(answer)
    return _session_id(created.get('ID') if isinstance(created, dict) else None)


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
    _, answer = await request(client, 'GET', path('session_info', prefix='', session=_session_id(session)))
    sessions = json.loads(answer)
    return sessions[0]['Name'] if sessions else None


async def read_key(client: ConsulClient, key_path: str) -> dict[str, Any] | None:
    """Return the KV entry at ``key_path`` (a path of :data:`PATHS`), or ``None`` when there is none (``404``).

    ``Value`` is decoded to bytes; ``Session`` is present only while a session holds the key.
    """
    status, answer = await request(client, 'GET', key_path, answers=(200, 404))
    if status == 404:
        return None
    return _decoded_entry(json.loads(answer)[0])


async def read_prefix(client: ConsulClient, prefix_path: str) -> list[dict[str, Any]]:
    """Return the KV entries under ``prefix_path`` in key order, decoded as :func:`read_key` decodes one.

    An empty prefix answers ``404``, which is no entry.
    """
    status, answer = await request(client, 'GET', prefix_path, query=(('recurse', 'true'),), answers=(200, 404))
    if status == 404:
        return []
    return [_decoded_entry(entry) for entry in json.loads(answer)]


async def acquire_key(client: ConsulClient, key_path: str, session: str, body: str) -> bool:
    """Write ``body`` to the key at ``key_path`` held by ``session``; ``False`` when another session holds it.

    The session that already holds the key gets ``True`` again and replaces the
    value. A session the agent does not know answers ``500``, a ``TransientError``.
    """
    _, answer = await request(client, 'PUT', key_path, query=(('acquire', _session_id(session)),), body=body)
    return json.loads(answer) is True


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
