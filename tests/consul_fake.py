"""In-process Consul agent for the fleet provider tests.

``FakeConsul`` answers the agent requests the providers send, behind an
``httpx.MockTransport``, so no socket is opened. Its answers are the ones a real
agent (Consul 1.21 with ACLs) gave to the same requests:

* ``GET /v1/status/leader`` answers the leader address as a JSON string, ``""``
  without a leader, and needs no token.
* A session create answers ``{"ID": ...}``; a TTL outside 10 s to 24 h answers
  ``500 Invalid Session TTL ...``. Ids are handed out in order:
  ``00000000-0000-4000-8000-000000000001``, ``...0002``, ...
* ``renew`` of an unknown session answers ``404``, ``destroy`` of one
  ``200 true``, ``info`` of one ``200 []``.
* An acquire by the holding session answers ``true`` and replaces the value, by
  another session ``false``, with an unknown session
  ``500 raft apply failed: invalid session "<id>"``.
* A session that ends (destroyed, or not renewed in time) deletes the keys it
  holds when its ``Behavior`` is ``delete``; with the default ``release`` the
  keys stay without a ``Session``.
* A key read answers ``404`` with an empty body when there is no key, and so
  does a recursive read of an empty prefix. ``Value`` is base64, ``null`` for
  an empty value, and ``Session`` is there only while a session holds the key.
* With :attr:`FakeConsul.token` set, ACLs are on and that token has every
  right. The leader request needs no token. For every other request:

  - a token in :attr:`FakeConsul.tokens_without_rights` gets a recursive read
    as ``404`` and a session ``info`` as ``200 []``, both with
    ``X-Consul-Results-Filtered-By-Acls: true`` when they hid something; a
    single-key read and every write answer
    ``403 Permission denied: token with AccessorID '<id>' lacks permission ...``;
  - no token gets the same hidden answers without the header, and ``403`` with
    the anonymous text for a single-key read and every write;
  - any other token answers ``403 ACL not found``.
* A ``dc`` other than ``dc1`` answers ``500 No path to datacenter``.

The fake's clock is its own: it starts at 0 and moves only by
:meth:`FakeConsul.advance`. Sessions end lazily: on each request, every session
whose last create or renew lies ``2 * TTL`` or more in the past ends first,
which is when the real agent ended one. Lock delays are not modelled (the
providers send ``0s``).
"""

import asyncio
import base64
import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from drakkar.providers.consul import CONNECT_TIMEOUT_SECONDS, FILTERED_HEADER, TOKEN_HEADER, ConsulClient

FAKE_BASE = 'http://127.0.0.1:8500'
DATACENTER = 'dc1'
LEADER = '127.0.0.1:8300'
NODE = 'consul-fake-node'
# The AccessorID of every token without the rights; an AccessorID is not the secret.
ACCESSOR_ID = '0b6d1f0e-3c2a-4d8e-9f71-5a2c3e4d6b80'

# Who sent a request that lacks the right, and what kind of request it is, decide the answer
# (recorded on a real agent): HIDE answers as if there were nothing, HIDE_FLAGGED does too and
# sets the filtered header when something was hidden, DENY answers 403.
ANONYMOUS, WITHOUT_RIGHTS = 'anonymous', 'without_rights'
LIST, INFO, READ, WRITE = 'list', 'info', 'read', 'write'
HIDE, HIDE_FLAGGED, DENY = 'hide', 'hide_flagged', 'deny'
REFUSALS: dict[tuple[str, str], str] = {
    (ANONYMOUS, LIST): HIDE,
    (ANONYMOUS, INFO): HIDE,
    (ANONYMOUS, READ): DENY,
    (ANONYMOUS, WRITE): DENY,
    (WITHOUT_RIGHTS, LIST): HIDE_FLAGGED,
    (WITHOUT_RIGHTS, INFO): HIDE_FLAGGED,
    (WITHOUT_RIGHTS, READ): DENY,
    (WITHOUT_RIGHTS, WRITE): DENY,
}
# The answer of a request whose results were all hidden: (status, body).
HIDDEN_ANSWERS: dict[str, tuple[int, bytes]] = {LIST: (404, b''), INFO: (200, b'[]')}
FAILURE_TEXT = 'injected failure'
SESSION_ID_FORMAT = '00000000-0000-4000-8000-{:012d}'
MIN_TTL_SECONDS = 10
MAX_TTL_SECONDS = 86400
NANOSECONDS_PER_SECOND = 1_000_000_000
DEFAULT_BEHAVIOR = 'release'
DELETE_BEHAVIOR = 'delete'
# The fake reads a duration only as whole seconds, the form the providers send.
SECONDS_RE = re.compile(r'([0-9]+)s')
LEADER_PATH = '/v1/status/leader'


@dataclass(frozen=True)
class Seen:
    """One request as the fake received it; ``query`` keeps the order in which the pairs were sent."""

    method: str
    path: str
    query: tuple[tuple[str, str], ...]
    body: str | None
    token: str | None

    def as_request(self) -> list[Any]:
        """Return the request as the fixture writes one: ``[method, path, query pairs, body]``."""
        return [self.method, self.path, [list(pair) for pair in self.query], self.body]


@dataclass
class _Session:
    """A live session of the fake."""

    id: str
    name: str
    ttl: str
    ttl_seconds: int
    behavior: str
    renewed_at: float
    create_index: int

    def as_json(self) -> dict[str, Any]:
        """The session as ``info`` and ``renew`` answer it."""
        return {
            'ID': self.id,
            'Name': self.name,
            'Node': NODE,
            'LockDelay': 0,
            'Behavior': self.behavior,
            'TTL': self.ttl,
            'NodeChecks': ['serfHealth'],
            'ServiceChecks': None,
            'CreateIndex': self.create_index,
            'ModifyIndex': self.create_index,
        }


@dataclass
class _Key:
    """A stored KV key."""

    value: bytes
    session: str | None
    lock_index: int
    create_index: int
    modify_index: int

    def as_json(self, key: str) -> dict[str, Any]:
        """The key as a read answers it."""
        entry: dict[str, Any] = {
            'LockIndex': self.lock_index,
            'Key': key,
            'Flags': 0,
            'Value': base64.b64encode(self.value).decode('ascii') if self.value else None,
        }
        if self.session is not None:
            entry['Session'] = self.session
        entry['CreateIndex'] = self.create_index
        entry['ModifyIndex'] = self.modify_index
        return entry


def _json(status: int, data: Any) -> httpx.Response:
    return httpx.Response(status, content=json.dumps(data).encode(), headers={'Content-Type': 'application/json'})


def _text(status: int, text: str) -> httpx.Response:
    return httpx.Response(status, text=text)


_Handler = Callable[[Seen, re.Match[str]], httpx.Response]
_Route = tuple[str, re.Pattern[str], _Handler, str, str]


class FakeConsul:
    """An in-process Consul agent with the answers of the recorded agent; its own clock starts at 0.

    ``seen`` records every request, also one that fails or hangs. ``fail_next``
    makes the next request answer that status (body ``injected failure``) or
    raise that exception, then is cleared; ``fail_every`` does so for every
    request until it is set back to ``None``. ``hang`` makes the next request
    never answer (until its caller gives up) and is then cleared.
    ``before_answer`` is called with each request before the fake answers it.
    ``fail_after_answer`` lets the next request change the fake's state, then
    raises instead of answering (an answer lost on the way back), and is then
    cleared. ``token`` turns ACLs on (see the module docstring), and
    ``tokens_without_rights`` names the tokens the agent knows but grants nothing.
    """

    def __init__(self) -> None:
        self.seen: list[Seen] = []
        self.token: str | None = None
        self.tokens_without_rights: set[str] = set()
        self.leader = LEADER
        self.fail_next: int | Exception | None = None
        self.fail_every: int | Exception | None = None
        self.hang = False
        self.before_answer: Callable[[Seen], None] | None = None
        self.fail_after_answer: Exception | None = None
        self._now = 0.0
        self._index = 0
        self._next_session = 1
        self._sessions: dict[str, _Session] = {}
        self._keys: dict[str, _Key] = {}
        # (method, path pattern, handler, ACL right, kind of request); the right is checked on the
        # key or on the node. A key read with ``recurse`` is a LIST.
        self._routes: tuple[_Route, ...] = (
            ('PUT', re.compile(r'/v1/session/create'), self._create_session, 'session:write', WRITE),
            ('PUT', re.compile(r'/v1/session/renew/(?P<id>[^/]+)'), self._renew_session, 'session:write', WRITE),
            ('PUT', re.compile(r'/v1/session/destroy/(?P<id>[^/]+)'), self._destroy_session, 'session:write', WRITE),
            ('GET', re.compile(r'/v1/session/info/(?P<id>[^/]+)'), self._session_info, 'session:read', INFO),
            ('GET', re.compile(r'/v1/kv/(?P<key>.+)'), self._read_keys, 'key:read', READ),
            ('PUT', re.compile(r'/v1/kv/(?P<key>.+)'), self._acquire_key, 'key:write', WRITE),
        )

    def transport(self) -> httpx.MockTransport:
        """The transport a client sends its requests through."""
        return httpx.MockTransport(self._handle)

    def advance(self, seconds: float) -> None:
        """Move the fake's clock forward by ``seconds``."""
        self._now += seconds

    def end_session(self, session_id: str) -> None:
        """End ``session_id`` as if its TTL ran out now; its ``Behavior`` applies. An unknown id changes nothing."""
        session = self._sessions.pop(session_id, None)
        if session is None:
            return
        for key, entry in list(self._keys.items()):
            if entry.session != session_id:
                continue
            if session.behavior == DELETE_BEHAVIOR:
                del self._keys[key]
            else:
                entry.session = None
                entry.modify_index = self._next_index()

    def plant_session(self, session_id: str, name: str, ttl_seconds: int) -> None:
        """Add a session no provider of the test created (a predecessor's), renewed now, with ``Behavior: delete``."""
        self._sessions[session_id] = _Session(
            id=session_id,
            name=name,
            ttl=f'{ttl_seconds}s',
            ttl_seconds=ttl_seconds,
            behavior=DELETE_BEHAVIOR,
            renewed_at=self._now,
            create_index=self._next_index(),
        )

    def put_raw(self, key: str, value: bytes, session: str | None = None) -> None:
        """Store ``value`` at the KV key ``key`` (``drakkar/fleet/workers/w``, no request path), held by ``session``."""
        if key.startswith('/'):
            raise ValueError(f'put_raw takes a KV key such as drakkar/fleet/workers/w, not a request path: {key}')
        if session is not None and session not in self._sessions:
            raise KeyError(session)
        index = self._next_index()
        self._keys[key] = _Key(
            value=value,
            session=session,
            lock_index=0 if session is None else 1,
            create_index=index,
            modify_index=index,
        )

    def _next_index(self) -> int:
        self._index += 1
        return self._index

    def _end_expired_sessions(self) -> None:
        for session in list(self._sessions.values()):
            if self._now >= session.renewed_at + 2 * session.ttl_seconds:
                self.end_session(session.id)

    def _record(self, request: httpx.Request) -> Seen:
        target, _, query_text = request.url.raw_path.decode('ascii').partition('?')
        pairs = []
        for pair in query_text.split('&') if query_text else ():
            name, _, value = pair.partition('=')
            pairs.append((name, value))
        seen = Seen(
            method=request.method,
            path=target,
            query=tuple(pairs),
            body=request.content.decode('utf-8') if request.content else None,
            token=request.headers.get(TOKEN_HEADER),
        )
        self.seen.append(seen)
        return seen

    def _take_failure(self) -> int | Exception | None:
        if self.fail_next is not None:
            failure, self.fail_next = self.fail_next, None
            return failure
        return self.fail_every

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        seen = self._record(request)
        if self.before_answer is not None:
            self.before_answer(seen)
        failure = self._take_failure()
        if isinstance(failure, Exception):
            raise failure
        if failure is not None:
            return _text(failure, FAILURE_TEXT)
        if self.hang:
            self.hang = False
            await asyncio.Event().wait()
        self._end_expired_sessions()
        response = self._answer(seen)
        if self.fail_after_answer is not None:
            lost, self.fail_after_answer = self.fail_after_answer, None
            raise lost
        return response

    def _answer(self, seen: Seen) -> httpx.Response:
        if dict(seen.query).get('dc', DATACENTER) != DATACENTER:
            return _text(500, 'No path to datacenter')
        if seen.method == 'GET' and seen.path == LEADER_PATH:
            return _json(200, self.leader)
        for method, pattern, handler, right, kind in self._routes:
            match = pattern.fullmatch(seen.path)
            if seen.method != method or match is None:
                continue
            if self.token is None or seen.token == self.token:
                return handler(seen, match)
            return self._refused(seen, match, handler, right, kind)
        return _text(400, f'fake consul: no answer for {seen.method} {seen.path}')

    def _refused(self, seen: Seen, match: re.Match[str], handler: _Handler, right: str, kind: str) -> httpx.Response:
        """The answer to a request whose token lacks ``right``, by who sent it and the kind of request."""
        if seen.token is not None and seen.token not in self.tokens_without_rights:
            return _text(403, 'ACL not found')
        if kind == READ and 'recurse' in dict(seen.query):
            kind = LIST
        refusal = REFUSALS[(ANONYMOUS if seen.token is None else WITHOUT_RIGHTS, kind)]
        if refusal == DENY:
            return _text(403, self._denied(seen.token, right, match.groupdict().get('key', NODE)))
        status, body = HIDDEN_ANSWERS[kind]
        # Hiding is read-only, so the handler's full answer only tells whether anything was hidden.
        hid_something = handler(seen, match).content not in (b'', b'[]')
        headers = {FILTERED_HEADER: 'true'} if refusal == HIDE_FLAGGED and hid_something else {}
        return httpx.Response(status, content=body, headers=headers)

    @staticmethod
    def _denied(token: str | None, right: str, resource: str) -> str:
        if token is None:
            return (
                f'Permission denied: anonymous token lacks permission \'{right}\' on "{resource}". '
                'The anonymous token is used implicitly when a request does not specify a token.'
            )
        return f"Permission denied: token with AccessorID '{ACCESSOR_ID}' lacks permission '{right}' on \"{resource}\""

    def _create_session(self, seen: Seen, match: re.Match[str]) -> httpx.Response:
        settings = json.loads(seen.body or '{}')
        ttl = settings.get('TTL', '')
        seconds_match = SECONDS_RE.fullmatch(ttl)
        if seconds_match is None:
            return _text(400, f'fake consul: a TTL of whole seconds is expected, got {ttl!r}')
        seconds = int(seconds_match[1])
        if not MIN_TTL_SECONDS <= seconds <= MAX_TTL_SECONDS:
            return _text(
                500, f"Invalid Session TTL '{seconds * NANOSECONDS_PER_SECOND}', must be between [10s=24h0m0s]"
            )
        session_id = SESSION_ID_FORMAT.format(self._next_session)
        self._next_session += 1
        self._sessions[session_id] = _Session(
            id=session_id,
            name=settings.get('Name', ''),
            ttl=ttl,
            ttl_seconds=seconds,
            behavior=settings.get('Behavior', DEFAULT_BEHAVIOR),
            renewed_at=self._now,
            create_index=self._next_index(),
        )
        return _json(200, {'ID': session_id})

    def _renew_session(self, seen: Seen, match: re.Match[str]) -> httpx.Response:
        session = self._sessions.get(match['id'])
        if session is None:
            return _text(404, f"Session id '{match['id']}' not found")
        session.renewed_at = self._now
        return _json(200, [session.as_json()])

    def _destroy_session(self, seen: Seen, match: re.Match[str]) -> httpx.Response:
        self.end_session(match['id'])
        return _json(200, True)

    def _session_info(self, seen: Seen, match: re.Match[str]) -> httpx.Response:
        session = self._sessions.get(match['id'])
        return _json(200, [] if session is None else [session.as_json()])

    def _read_keys(self, seen: Seen, match: re.Match[str]) -> httpx.Response:
        key = match['key']
        if 'recurse' in dict(seen.query):
            entries = [self._keys[name].as_json(name) for name in sorted(self._keys) if name.startswith(key)]
        else:
            entries = [self._keys[key].as_json(key)] if key in self._keys else []
        return _json(200, entries) if entries else httpx.Response(404)

    def _acquire_key(self, seen: Seen, match: re.Match[str]) -> httpx.Response:
        session_id = dict(seen.query).get('acquire')
        if session_id is None:
            return _text(400, 'fake consul: a key write without acquire is not modelled')
        if session_id not in self._sessions:
            return _text(500, f'raft apply failed: invalid session "{session_id}"')
        key = match['key']
        entry = self._keys.get(key)
        if entry is not None and entry.session not in (None, session_id):
            return _json(200, False)
        value = (seen.body or '').encode('utf-8')
        index = self._next_index()
        if entry is None:
            self._keys[key] = _Key(
                value=value, session=session_id, lock_index=1, create_index=index, modify_index=index
            )
            return _json(200, True)
        if entry.session is None:
            entry.lock_index += 1
        entry.value, entry.session, entry.modify_index = value, session_id, index
        return _json(200, True)


def fake_client(fake: FakeConsul, *, datacenter: str = '', token: str = '') -> ConsulClient:
    """Return a client that sends its requests to ``fake``, with the settings ``open_client`` gives a real one."""
    http = httpx.AsyncClient(
        transport=fake.transport(),
        follow_redirects=False,
        timeout=httpx.Timeout(None, connect=CONNECT_TIMEOUT_SECONDS),
    )
    return ConsulClient(http=http, base=FAKE_BASE, token=token, datacenter=datacenter)
