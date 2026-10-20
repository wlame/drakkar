"""Unit tests for the Consul request corpus, the client helpers and the fake agent.

No Consul agent: ``tests/consul_fake.py`` answers through ``httpx.MockTransport``
with the answers a real agent gave to the same requests. The paths, the bodies,
the messages and the worked example are checked against the shared fixture.
"""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import httpcore
import httpx
import pytest

from drakkar.config.connections import ConnectionsConfig, ConsulConnectionConfig
from drakkar.fleet.card import sanitize_worker_name, serialize_card
from drakkar.providers.connections import Connections
from drakkar.providers.consul import (
    BAD_ADDRESS_MESSAGE,
    BAD_SESSION_ID_MESSAGE,
    BAD_TOKEN_MESSAGE,
    CARD_HELD_MESSAGE,
    CONNECT_TIMEOUT_SECONDS,
    CONSUL_PROVIDER,
    DEFAULT_KEY_PREFIX,
    FILTERED_HEADER,
    FILTERED_MESSAGE,
    INSTANCE_PLACEHOLDER,
    NO_LEADER_MESSAGE,
    PATHS,
    PREFIX_PLACEHOLDER,
    REFRESH_BEFORE_ACQUIRE_MESSAGE,
    SCOPE_PLACEHOLDER,
    SESSION_ID_RE,
    SESSION_NAMES,
    SESSION_PLACEHOLDER,
    SESSION_SETTINGS,
    STATUS_MESSAGE,
    STATUS_NO_TEXT_MESSAGE,
    STATUS_TEXT_LIMIT,
    TOKEN_HEADER,
    TOKEN_PLACEHOLDER,
    TRANSIENT_STATUSES,
    UNREADABLE_MESSAGE,
    WORKER_PLACEHOLDER,
    ConsulClient,
    ConsulError,
    acquire_key,
    base_url,
    check_leader,
    check_token,
    classify,
    close_client,
    create_session,
    destroy_session,
    is_own_session_name,
    open_client,
    path,
    read_key,
    read_prefix,
    renew_session,
    request,
    resolve_client,
    session_body,
    session_name,
    session_ttl,
    status_error,
)
from drakkar.providers.resilience import TransientError
from drakkar.recorder.history import serialize_json
from tests.consul_fake import FakeConsul, Seen, fake_client

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['consul']
EXAMPLE = FIXTURE['example']
INPUTS = EXAMPLE['inputs']
REQUESTS = EXAMPLE['requests']
FIRST, SECOND, PREDECESSOR = (INPUTS['sessions'][name] for name in ('first', 'second', 'predecessor'))
MEMBER = sanitize_worker_name(INPUTS['card']['worker'])
CARD_PATH = path('registry_card', prefix=INPUTS['prefix'], worker=MEMBER)
LIST_PATH = path('registry_list', prefix=INPUTS['prefix'])
LOCK_PATH = path('election_lock', prefix=INPUTS['prefix'], scope=INPUTS['scope'])
UNKNOWN_SESSION = '11111111-2222-3333-4444-555555555555'
CONFIGURED_TOKEN = '6a1f3c2e-0d4b-4c8e-9a7f-2b5e8d1c4f90'
WRONG_TOKEN = '0d9e8f7a-6b5c-4d3e-8f2a-1b0c9d8e7f6a'


@pytest.fixture
def fake() -> FakeConsul:
    return FakeConsul()


def registry_session_name(member: str, instance: str) -> str:
    """The session name of a registry card, rendered from the fixture's template."""
    return SESSION_NAMES['registry'].replace(WORKER_PLACEHOLDER, member).replace(INSTANCE_PLACEHOLDER, instance)


def election_session_name(scope: str, token: str) -> str:
    """The session name of an election lease, rendered from the fixture's template."""
    return SESSION_NAMES['election'].replace(SCOPE_PLACEHOLDER, scope).replace(TOKEN_PLACEHOLDER, token)


def example_card_text() -> str:
    return serialize_card(INPUTS['card']).decode()


def example_lock_text() -> str:
    body = {
        'acquired_at': INPUTS['acquired_at'],
        'instance': INPUTS['card']['instance'],
        'provider': CONSUL_PROVIDER,
        'token': INPUTS['token'],
        'worker': INPUTS['card']['worker'],
    }
    return serialize_json(body).decode()


def example_registry_body() -> str:
    name = registry_session_name(MEMBER, INPUTS['card']['instance'])
    return session_body(name, session_ttl(INPUTS['offline_after_seconds']))


def example_election_body() -> str:
    name = election_session_name(INPUTS['scope'], INPUTS['token'])
    return session_body(name, session_ttl(INPUTS['lease_seconds']))


def answering(status: int, content: bytes, headers: dict[str, str] | None = None) -> ConsulClient:
    """A client whose every request gets the same answer."""

    async def answer(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=content, headers=headers)

    return ConsulClient(
        http=httpx.AsyncClient(transport=httpx.MockTransport(answer)),
        base='http://consul.example.test:8500',
        token='',
        datacenter='',
    )


def test_paths_session_names_and_settings_match_the_fixture():
    assert FIXTURE['placeholders'] == {
        'prefix': PREFIX_PLACEHOLDER,
        'worker': WORKER_PLACEHOLDER,
        'scope': SCOPE_PLACEHOLDER,
        'session': SESSION_PLACEHOLDER,
        'instance': INSTANCE_PLACEHOLDER,
        'token': TOKEN_PLACEHOLDER,
    }
    assert FIXTURE['paths'] == PATHS
    assert FIXTURE['session_names'] == SESSION_NAMES
    assert FIXTURE['session_settings'] == SESSION_SETTINGS
    assert FIXTURE['token_header'] == TOKEN_HEADER
    assert FIXTURE['filtered_header'] == FILTERED_HEADER
    assert FIXTURE['session_id_pattern'] == SESSION_ID_RE.pattern


@pytest.mark.parametrize(
    ('name', 'values'),
    [
        ('registry_card', {'prefix': 'drakkar'}),
        ('registry_card', {'worker': 'w'}),
        ('election_lock', {'prefix': 'drakkar'}),
        ('session_renew', {}),
    ],
)
def test_path_refuses_a_placeholder_without_a_value(name, values):
    # Without the check, a card path with no worker would be the list path.
    with pytest.raises(ValueError, match=f'the consul path {name} needs a value for'):
        path(name, **{'prefix': '', **values})


def test_path_needs_no_value_for_a_placeholder_its_template_does_not_have():
    assert path('registry_list', prefix='drakkar') == '/v1/kv/drakkar/fleet/workers/'
    assert path('session_info', prefix='', session=FIRST) == f'/v1/session/info/{FIRST}'
    assert path('leader', prefix='') == '/v1/status/leader'


def test_statuses_timeouts_and_messages_match_the_fixture():
    assert list(TRANSIENT_STATUSES) == FIXTURE['transient_statuses']
    assert FIXTURE['connect_timeout_seconds'] == CONNECT_TIMEOUT_SECONDS
    assert FIXTURE['status_text_limit'] == STATUS_TEXT_LIMIT
    assert FIXTURE['messages'] == {
        'bad_address': BAD_ADDRESS_MESSAGE,
        'bad_token': BAD_TOKEN_MESSAGE,
        'no_leader': NO_LEADER_MESSAGE,
        'bad_session_id': BAD_SESSION_ID_MESSAGE,
        'card_held': CARD_HELD_MESSAGE,
        'refresh_before_acquire': REFRESH_BEFORE_ACQUIRE_MESSAGE,
        'status': STATUS_MESSAGE,
        'status_no_text': STATUS_NO_TEXT_MESSAGE,
        'filtered': FILTERED_MESSAGE,
        'unreadable': UNREADABLE_MESSAGE,
    }


def test_provider_name_and_default_prefix_follow_the_connection_config():
    assert CONSUL_PROVIDER == 'consul'
    assert ConsulConnectionConfig().key_prefix == DEFAULT_KEY_PREFIX


@pytest.mark.parametrize(('seconds', 'expected'), FIXTURE['ttl'])
def test_session_ttl_follows_the_pinned_rule(seconds, expected):
    assert session_ttl(seconds) == expected


def test_session_ttl_rounds_a_fractional_half_up_to_whole_seconds():
    assert session_ttl(20.5) == '11s'
    assert session_ttl(60.0) == '30s'


@pytest.mark.parametrize('case', FIXTURE['own_session_names'], ids=lambda c: c['name'])
def test_is_own_session_name_follows_the_pinned_cases(case):
    assert is_own_session_name(case['name'], case['member']) is case['own']


@pytest.mark.parametrize('case', FIXTURE['address_cases'], ids=lambda c: repr(c['address']))
def test_base_url_follows_the_pinned_cases(case):
    if case['base'] is not None:
        assert base_url(case['address']) == case['base']
        return
    with pytest.raises(ValueError) as info:
        base_url(case['address'])

    assert str(info.value) == BAD_ADDRESS_MESSAGE
    assert info.value.__cause__ is None
    assert 'secret' not in str(info.value)


@pytest.mark.parametrize(
    ('address', 'base'),
    [
        ('http://consul.example.test/..x/', 'http://consul.example.test/..x'),
        ('http://consul.example.test/v1.2', 'http://consul.example.test/v1.2'),
        ('http://consul.example.test/.../api', 'http://consul.example.test/.../api'),
        ('http://consul.example.test/.x', 'http://consul.example.test/.x'),
    ],
)
def test_base_url_keeps_a_segment_with_dots_among_other_characters(address, base):
    # Only a segment that is exactly '.' or '..' is a dot segment, which an HTTP client may remove.
    assert base_url(address) == base


@pytest.mark.parametrize(
    'address', ['http://consul.example.test/..', 'http://consul.example.test/api/./', 'http://[::1]:8500/../']
)
def test_base_url_refuses_a_dot_segment_anywhere_in_the_path(address):
    with pytest.raises(ValueError, match=BAD_ADDRESS_MESSAGE):
        base_url(address)


@pytest.mark.parametrize(('token', 'ok'), FIXTURE['token_cases'])
def test_check_token_follows_the_pinned_cases(token, ok):
    if ok:
        assert check_token(token) is None
        return
    with pytest.raises(ValueError) as info:
        check_token(token)

    assert str(info.value) == BAD_TOKEN_MESSAGE
    assert token not in str(info.value)


def test_example_session_bodies_are_the_session_encoding():
    creates = [
        (name, request)
        for name, sequence in REQUESTS.items()
        for request in sequence
        if request[1] == PATHS['session_create']
    ]

    assert [name for name, _ in creates] == [
        'first_publish',
        'publish_after_the_session_ended',
        'publish_over_the_predecessor',
        'acquire',
    ]
    for name, (_, _, _, body) in creates:
        assert body == (example_election_body() if name == 'acquire' else example_registry_body())


@pytest.mark.parametrize(
    ('name', 'ttl'),
    [
        ('drakkar-search-worker-3-9c41e0b27fd34a1e', '30s'),
        ('drakkar/archive-lock/main/5f0c1d2e3a4b6978', '3600s'),
        ('wörker "1"', '10s'),
    ],
)
def test_session_body_encodes_like_the_contract_encoder(name, ttl):
    # The session body is encoded in this module, not by the recorder's encoder
    # (the recorder imports the providers package); both must give the same bytes.
    body = session_body(name, ttl)

    assert body.encode('utf-8') == serialize_json({**SESSION_SETTINGS, 'Name': name, 'TTL': ttl})


def test_example_card_and_lock_body_are_the_contract_encodings():
    card_puts = [r for sequence in REQUESTS.values() for r in sequence if r[0] == 'PUT' and r[1] == CARD_PATH]
    lock_puts = [r for sequence in REQUESTS.values() for r in sequence if r[0] == 'PUT' and r[1] == LOCK_PATH]

    assert len(card_puts) == 5
    assert len(lock_puts) == 2
    assert {body for _, _, _, body in card_puts} == {example_card_text()}
    assert {body for _, _, _, body in lock_puts} == {example_lock_text()}


def test_example_requests_follow_from_the_helpers():
    dc = [['dc', INPUTS['datacenter']]]

    def create(body: str) -> list:
        return ['PUT', PATHS['session_create'], dc, body]

    def renew(session: str) -> list:
        return ['PUT', path('session_renew', prefix='', session=session), dc, None]

    def destroy(session: str) -> list:
        return ['PUT', path('session_destroy', prefix='', session=session), dc, None]

    def put(key_path: str, session: str, body: str) -> list:
        return ['PUT', key_path, [['acquire', session], *dc], body]

    card, lock = example_card_text(), example_lock_text()
    registry, election = example_registry_body(), example_election_body()

    expected = {
        'start': [['GET', PATHS['leader'], dc, None]],
        'first_publish': [create(registry), put(CARD_PATH, FIRST, card)],
        'next_publish': [renew(FIRST), put(CARD_PATH, FIRST, card)],
        'publish_after_the_session_ended': [renew(FIRST), create(registry), put(CARD_PATH, SECOND, card)],
        'publish_over_the_predecessor': [
            create(registry),
            put(CARD_PATH, FIRST, card),
            ['GET', CARD_PATH, dc, None],
            ['GET', path('session_info', prefix='', session=PREDECESSOR), dc, None],
            destroy(PREDECESSOR),
            put(CARD_PATH, FIRST, card),
        ],
        'list': [['GET', LIST_PATH, [*dc, ['recurse', 'true']], None]],
        'withdraw': [destroy(FIRST)],
        'acquire': [create(election), put(LOCK_PATH, FIRST, lock)],
        'acquire_by_the_holder': [renew(FIRST), put(LOCK_PATH, FIRST, lock)],
        'refresh': [renew(FIRST)],
        'is_held': [['GET', LOCK_PATH, dc, None]],
        'release': [destroy(FIRST)],
    }

    assert expected == REQUESTS
    assert is_own_session_name(INPUTS['predecessor_name'], MEMBER)
    assert base_url(INPUTS['address']) == INPUTS['address']


async def test_helpers_send_the_example_requests(fake):
    client = fake_client(fake, datacenter=INPUTS['datacenter'])
    registry_name = registry_session_name(MEMBER, INPUTS['card']['instance'])

    await check_leader(client)
    session = await create_session(client, registry_name, INPUTS['offline_after_seconds'])
    assert await acquire_key(client, CARD_PATH, session, example_card_text()) is True
    assert await renew_session(client, session) is True
    assert await acquire_key(client, CARD_PATH, session, example_card_text()) is True
    await read_prefix(client, LIST_PATH)
    await destroy_session(client, session)

    assert session == FIRST
    assert [seen.as_request() for seen in fake.seen] == [
        *REQUESTS['start'],
        *REQUESTS['first_publish'],
        *REQUESTS['next_publish'],
        *REQUESTS['list'],
        *REQUESTS['withdraw'],
    ]


async def test_open_client_follows_no_redirect_and_bounds_only_the_connect():
    client = await open_client(ConsulConnectionConfig(address='http://consul.example.test:8500'))

    assert client.http.follow_redirects is False
    assert client.http.timeout.connect == CONNECT_TIMEOUT_SECONDS
    assert client.http.timeout.read is None
    assert client.http.timeout.write is None
    assert client.http.timeout.pool is None
    # Proxies and the trust store follow the environment, as the HTTP sink's client does.
    assert client.http.trust_env is True
    assert (client.base, client.token, client.datacenter) == ('http://consul.example.test:8500', '', '')
    await close_client(client)
    assert client.http.is_closed


async def test_open_client_transports_make_no_retries(monkeypatch):
    # httpx keeps the retry count on each transport's connection pool and has no public
    # accessor for it. A proxy variable makes httpx add a proxy transport per scheme;
    # those must not retry either. Nothing connects, so the proxy host is never looked up.
    monkeypatch.setenv('HTTP_PROXY', 'http://proxy.example.test:3128')
    monkeypatch.setenv('HTTPS_PROXY', 'http://proxy.example.test:3128')
    client = await open_client(ConsulConnectionConfig(address='http://consul.example.test:8500'))

    transports = [client.http._transport, *(t for t in client.http._mounts.values() if t is not None)]

    assert len(transports) == 3
    for transport in transports:
        assert isinstance(transport, httpx.AsyncHTTPTransport)
        pool = transport._pool
        assert isinstance(pool, httpcore.AsyncConnectionPool)
        assert pool._retries == 0
    await close_client(client)


async def test_open_client_keeps_the_token_and_the_datacenter():
    settings = ConsulConnectionConfig(
        address='https://consul.example.test:8501/', token=CONFIGURED_TOKEN, datacenter='dc1'
    )

    client = await open_client(settings)

    assert (client.base, client.token, client.datacenter) == (
        'https://consul.example.test:8501',
        CONFIGURED_TOKEN,
        'dc1',
    )
    await close_client(client)


@pytest.mark.parametrize(
    ('settings', 'message'),
    [
        ({'address': 'http://user:secret@consul.example.test:8500'}, BAD_ADDRESS_MESSAGE),
        ({'token': 'secret token'}, BAD_TOKEN_MESSAGE),
    ],
    ids=['address', 'token'],
)
async def test_open_client_refuses_a_bad_address_or_token(settings, message):
    with pytest.raises(ValueError) as info:
        await open_client(ConsulConnectionConfig(**settings))

    assert str(info.value) == message
    assert 'secret' not in str(info.value)
    assert info.value.__cause__ is None


def test_consul_client_repr_shows_no_token():
    client = ConsulClient(
        http=MagicMock(), base='http://consul.example.test:8500', token=CONFIGURED_TOKEN, datacenter=''
    )

    assert CONFIGURED_TOKEN not in repr(client)


async def test_request_adds_the_datacenter_and_the_token_header_only_when_set(fake):
    await request(fake_client(fake), 'GET', PATHS['leader'])
    await request(fake_client(fake, datacenter='dc1', token=CONFIGURED_TOKEN), 'GET', PATHS['leader'])

    assert fake.seen == [
        Seen(method='GET', path='/v1/status/leader', query=(), body=None, token=None),
        Seen(method='GET', path='/v1/status/leader', query=(('dc', 'dc1'),), body=None, token=CONFIGURED_TOKEN),
    ]


async def test_request_sorts_the_query_pairs(fake):
    client = fake_client(fake, datacenter='dc1')

    await request(client, 'GET', LIST_PATH, query=[('recurse', 'true'), ('acquire', FIRST)], answers=(200, 404))

    # The fake records the pairs in the order they were sent.
    assert fake.seen[0].query == (('acquire', FIRST), ('dc', 'dc1'), ('recurse', 'true'))


async def test_request_sends_the_body_as_utf8_text(fake):
    client = fake_client(fake)
    session = await create_session(client, 'wörker', 20)

    assert await acquire_key(client, CARD_PATH, session, '{"worker":"wörker"}') is True
    assert fake.seen[-1].body == '{"worker":"wörker"}'
    entry = await read_key(client, CARD_PATH)
    assert entry is not None
    assert entry['Value'] == '{"worker":"wörker"}'.encode()


@pytest.mark.parametrize('status', [429, 500, 502, 503, 504])
async def test_a_transient_status_raises_transient_error(status, fake):
    fake.fail_next = status

    with pytest.raises(TransientError) as info:
        await request(fake_client(fake), 'GET', PATHS['leader'])

    assert str(info.value) == f'consul: GET /v1/status/leader answered {status}: injected failure'
    assert not isinstance(info.value, ConsulError)


async def test_a_forbidden_answer_is_final_and_names_no_token(fake):
    fake.token = CONFIGURED_TOKEN
    fake.tokens_without_rights.add(WRONG_TOKEN)
    client = fake_client(fake, token=WRONG_TOKEN)
    # A long key makes the server's text longer than the limit.
    key_path = path('registry_card', prefix='drakkar', worker='w' * 150)

    with pytest.raises(ConsulError) as info:
        await read_key(client, key_path)

    text = str(info.value)
    head = f'consul: GET {key_path} answered 403: '
    assert not isinstance(info.value, TransientError)
    assert CONFIGURED_TOKEN not in text
    assert WRONG_TOKEN not in text
    assert text.startswith(head + 'Permission denied: token with AccessorID ')
    assert len(text) == len(head) + STATUS_TEXT_LIMIT
    assert fake.seen[0].token == WRONG_TOKEN


async def test_an_unknown_token_is_final_and_named_nowhere(fake):
    fake.token = CONFIGURED_TOKEN

    with pytest.raises(ConsulError) as info:
        await read_prefix(fake_client(fake, token=WRONG_TOKEN), LIST_PATH)

    assert str(info.value) == f'consul: GET {LIST_PATH} answered 403: ACL not found'
    assert not isinstance(info.value, TransientError)


async def holder_of_one_card(fake: FakeConsul) -> str:
    """Publish one card with the full-rights token of ``fake``; return its session."""
    client = fake_client(fake, token=CONFIGURED_TOKEN)
    session = await create_session(client, 'drakkar-w-0123456789abcdef', 60)
    await acquire_key(client, CARD_PATH, session, 'card')
    return session


async def test_a_filtered_recursive_read_raises_the_final_error(fake):
    fake.token = CONFIGURED_TOKEN
    fake.tokens_without_rights.add(WRONG_TOKEN)
    await holder_of_one_card(fake)

    with pytest.raises(ConsulError) as info:
        await read_prefix(fake_client(fake, token=WRONG_TOKEN), LIST_PATH)

    assert str(info.value) == f"consul: GET {LIST_PATH} answered with results hidden by the token's rights"
    assert not isinstance(info.value, TransientError)


async def test_a_filtered_session_info_raises_the_final_error(fake):
    fake.token = CONFIGURED_TOKEN
    fake.tokens_without_rights.add(WRONG_TOKEN)
    session = await holder_of_one_card(fake)

    with pytest.raises(ConsulError) as info:
        await session_name(fake_client(fake, token=WRONG_TOKEN), session)

    assert str(info.value) == FILTERED_MESSAGE.format(method='GET', path=f'/v1/session/info/{session}')


@pytest.mark.parametrize('name', [FILTERED_HEADER, FILTERED_HEADER.lower(), FILTERED_HEADER.upper()])
@pytest.mark.parametrize('status', [200, 404, 500])
async def test_the_filtered_header_is_read_in_any_letter_case_and_before_the_status(name, status):
    with pytest.raises(ConsulError) as info:
        await request(answering(status, b'[]', headers={name: 'true'}), 'GET', LIST_PATH, answers=(200, 404))

    assert str(info.value) == FILTERED_MESSAGE.format(method='GET', path=LIST_PATH)


async def test_a_filtered_header_that_is_not_true_changes_nothing():
    client = answering(200, b'[]', headers={FILTERED_HEADER: 'false'})

    assert await request(client, 'GET', LIST_PATH) == (200, b'[]')


async def test_an_anonymous_request_reads_hidden_results_as_absent_ones_like_the_recorded_agent(fake):
    fake.token = CONFIGURED_TOKEN
    session = await holder_of_one_card(fake)
    anonymous = fake_client(fake)

    # The recorded agent hid these results from the anonymous token without the header.
    assert await read_prefix(anonymous, LIST_PATH) == []
    assert await session_name(anonymous, session) is None
    with pytest.raises(ConsulError, match="anonymous token lacks permission 'key:read'"):
        await read_key(anonymous, CARD_PATH)
    with pytest.raises(ConsulError, match="anonymous token lacks permission 'session:write'"):
        await renew_session(anonymous, session)


async def test_a_redirect_is_a_final_status():
    calls = []

    async def answer(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(307, headers={'Location': 'http://elsewhere.example.test/v1/status/leader'})

    client = await open_client(
        ConsulConnectionConfig(address='http://consul.example.test:8500'), transport=httpx.MockTransport(answer)
    )

    with pytest.raises(ConsulError) as info:
        await check_leader(client)

    assert str(info.value) == 'consul: GET /v1/status/leader answered 307'
    assert calls == ['http://consul.example.test:8500/v1/status/leader']
    await close_client(client)


async def test_a_status_error_cuts_the_body_to_its_first_line_and_the_limit():
    head = 'consul: PUT /v1/session/create answered 403'

    def text(body: bytes) -> str:
        return str(status_error('PUT', '/v1/session/create', 403, body))

    assert text(b'a\nb') == f'{head}: a'
    assert text(b'x' * 300) == f'{head}: ' + 'x' * STATUS_TEXT_LIMIT
    assert text(b'') == head
    assert text(b'\nsecond line') == head
    # The limit counts characters, not bytes.
    assert text(('ü' * 300).encode()) == f'{head}: ' + 'ü' * STATUS_TEXT_LIMIT
    # The first line ends at the line feed alone; a carriage return before it stays.
    assert text(b'a\r\nb') == f'{head}: a\r'
    assert text(b'\xff') == f'{head}: �'


def test_status_error_is_transient_only_for_the_transient_statuses():
    assert isinstance(status_error('GET', '/v1/x', 503, b''), TransientError)
    assert isinstance(status_error('GET', '/v1/x', 404, b''), ConsulError)
    assert isinstance(status_error('GET', '/v1/x', 400, b''), ConsulError)


@pytest.mark.parametrize(
    'original', [httpx.ReadTimeout('timed out'), httpx.ConnectTimeout('timed out'), httpx.PoolTimeout('timed out')]
)
async def test_classify_turns_a_timeout_into_the_builtin_timeout(original):
    result = classify(original)

    assert isinstance(result, TimeoutError)
    assert not isinstance(result, TransientError)
    assert str(result) == 'timed out'
    assert result.__cause__ is original


@pytest.mark.parametrize(
    'original', [httpx.ConnectError('refused'), httpx.ReadError('reset'), httpx.RemoteProtocolError('cut')]
)
async def test_classify_wraps_a_connection_error_as_transient(original):
    result = classify(original)

    assert isinstance(result, TransientError)
    assert str(result) == str(original)
    assert result.__cause__ is original


@pytest.mark.parametrize('original', [ValueError('not httpx'), httpx.DecodingError('bad gzip')])
def test_classify_leaves_other_errors_alone(original):
    assert classify(original) is original


@pytest.mark.parametrize(
    ('failure', 'expected'), [(httpx.ConnectError('down'), TransientError), (httpx.ReadTimeout('slow'), TimeoutError)]
)
async def test_request_raises_a_transport_error_classified(fake, failure, expected):
    fake.fail_next = failure

    with pytest.raises(expected) as info:
        await request(fake_client(fake), 'GET', PATHS['leader'])

    assert info.value.__cause__ is failure


async def test_check_leader_passes_with_a_leader(fake):
    assert await check_leader(fake_client(fake)) is None


async def test_check_leader_raises_transient_without_a_leader(fake):
    fake.leader = ''

    with pytest.raises(TransientError) as info:
        await check_leader(fake_client(fake))

    assert str(info.value) == NO_LEADER_MESSAGE


async def test_create_session_sends_the_session_body_and_returns_the_id(fake):
    client = fake_client(fake)

    session = await create_session(client, 'drakkar-w-0123456789abcdef', 61)

    assert session == FIRST
    assert fake.seen[0].body == session_body('drakkar-w-0123456789abcdef', '31s')
    assert await session_name(client, session) == 'drakkar-w-0123456789abcdef'


@pytest.mark.parametrize(
    'answer',
    [
        b'{"ID": "../x"}',
        b'{"ID": "2F2DF444-95C6-E646-5691-7B3B719583E9"}',
        b'{"ID": "2f2df444-95c6-e646-5691-7b3b719583e9\\n"}',
        b'{"ID": 7}',
        b'{"ID": null}',
    ],
)
async def test_create_session_refuses_an_id_that_is_not_a_uuid(answer):
    with pytest.raises(ConsulError) as info:
        await create_session(answering(200, answer), 'drakkar-w-0123456789abcdef', 60)

    assert str(info.value) == BAD_SESSION_ID_MESSAGE


SESSION_INFO_PATH = f'/v1/session/info/{FIRST}'


@pytest.mark.parametrize(
    ('call', 'answer', 'method', 'request_path'),
    [
        (check_leader, b'not json', 'GET', PATHS['leader']),
        (check_leader, b'null', 'GET', PATHS['leader']),
        (check_leader, b'7', 'GET', PATHS['leader']),
        (lambda client: create_session(client, 'x', 20), b'{}', 'PUT', PATHS['session_create']),
        (lambda client: create_session(client, 'x', 20), b'[]', 'PUT', PATHS['session_create']),
        (lambda client: create_session(client, 'x', 20), b'not json', 'PUT', PATHS['session_create']),
        (lambda client: session_name(client, FIRST), b'{"Name": "x"}', 'GET', SESSION_INFO_PATH),
        (lambda client: session_name(client, FIRST), b'[7]', 'GET', SESSION_INFO_PATH),
        (lambda client: session_name(client, FIRST), b'[{"ID": "x"}]', 'GET', SESSION_INFO_PATH),
        (lambda client: session_name(client, FIRST), b'[{"Name": 7}]', 'GET', SESSION_INFO_PATH),
        (lambda client: read_key(client, CARD_PATH), b'[]', 'GET', CARD_PATH),
        (lambda client: read_key(client, CARD_PATH), b'{"Value": null}', 'GET', CARD_PATH),
        (lambda client: read_key(client, CARD_PATH), b'[{"Key": "k", "Value": "%%%"}]', 'GET', CARD_PATH),
        (lambda client: read_key(client, CARD_PATH), b'[{"Key": "k", "Value": "\\u00fc"}]', 'GET', CARD_PATH),
        (lambda client: read_key(client, CARD_PATH), b'[{"Key": "k", "Value": 7}]', 'GET', CARD_PATH),
        (lambda client: read_key(client, CARD_PATH), b'[{"Key": 7, "Value": null}]', 'GET', CARD_PATH),
        (lambda client: read_prefix(client, LIST_PATH), b'[7]', 'GET', LIST_PATH),
        (lambda client: read_prefix(client, LIST_PATH), b'{"Key": "k"}', 'GET', LIST_PATH),
        (lambda client: read_prefix(client, LIST_PATH), b'\xff', 'GET', LIST_PATH),
        (lambda client: read_prefix(client, LIST_PATH), b'[{"Value": null}]', 'GET', LIST_PATH),
        (lambda client: acquire_key(client, CARD_PATH, FIRST, 'x'), b'null', 'PUT', CARD_PATH),
        (lambda client: acquire_key(client, CARD_PATH, FIRST, 'x'), b'"true"', 'PUT', CARD_PATH),
    ],
)
async def test_a_malformed_answer_raises_the_unreadable_error(call, answer, method, request_path):
    with pytest.raises(ConsulError) as info:
        await call(answering(200, answer))

    assert str(info.value) == f'consul: {method} {request_path} answered 200 with an answer that cannot be read'
    assert not isinstance(info.value, TransientError)


@pytest.mark.parametrize(
    'call',
    [
        lambda client: renew_session(client, '../x'),
        lambda client: destroy_session(client, '../x'),
        lambda client: session_name(client, '../x'),
        lambda client: acquire_key(client, CARD_PATH, f'{FIRST}&dc=other', 'x'),
    ],
    ids=['renew', 'destroy', 'info', 'acquire'],
)
async def test_a_session_id_that_is_not_a_uuid_never_enters_a_request(fake, call):
    with pytest.raises(ConsulError, match=BAD_SESSION_ID_MESSAGE):
        await call(fake_client(fake))

    assert fake.seen == []


async def test_read_key_and_read_prefix_decode_each_entry(fake):
    client = fake_client(fake)
    session = await create_session(client, 'drakkar-a-0123456789abcdef', 60)
    await acquire_key(client, '/v1/kv/drakkar/fleet/workers/a', session, 'card a')
    fake.put_raw('drakkar/fleet/workers/b', b'')

    held = await read_key(client, '/v1/kv/drakkar/fleet/workers/a')
    entries = await read_prefix(client, LIST_PATH)

    assert held is not None
    assert (held['Key'], held['Value'], held['Session']) == ('drakkar/fleet/workers/a', b'card a', session)
    assert [entry['Key'] for entry in entries] == ['drakkar/fleet/workers/a', 'drakkar/fleet/workers/b']
    assert [entry['Value'] for entry in entries] == [b'card a', b'']
    # 'Session' is there only while a session holds the key.
    assert 'Session' not in entries[1]
    assert await read_key(client, '/v1/kv/drakkar/fleet/workers/c') is None


async def test_fake_ends_a_session_twice_its_ttl_after_the_last_renew(fake):
    client = fake_client(fake)
    # A limit of 20 s is a 10 s TTL, the TTL the recorded agent ended after 20 s.
    session = await create_session(client, 'drakkar-w-0123456789abcdef', 20)
    await acquire_key(client, CARD_PATH, session, 'card')

    fake.advance(19.9)
    assert await read_key(client, CARD_PATH) is not None
    fake.advance(0.1)
    assert await read_key(client, CARD_PATH) is None
    assert await renew_session(client, session) is False


async def test_fake_starts_the_ttl_again_at_each_renew(fake):
    client = fake_client(fake)
    session = await create_session(client, 'drakkar-w-0123456789abcdef', 20)
    await acquire_key(client, CARD_PATH, session, 'card')
    fake.advance(15)
    assert await renew_session(client, session) is True

    fake.advance(19.9)
    assert await read_key(client, CARD_PATH) is not None
    fake.advance(0.1)
    assert await read_key(client, CARD_PATH) is None


async def test_fake_answers_like_the_recorded_agent(fake):
    client = fake_client(fake)

    assert await request(client, 'GET', PATHS['leader']) == (200, b'"127.0.0.1:8300"')
    assert await request(client, 'GET', f'/v1/session/info/{UNKNOWN_SESSION}') == (200, b'[]')
    assert await session_name(client, UNKNOWN_SESSION) is None
    assert await request(client, 'PUT', f'/v1/session/renew/{UNKNOWN_SESSION}', answers=(404,)) == (
        404,
        f"Session id '{UNKNOWN_SESSION}' not found".encode(),
    )
    assert await renew_session(client, UNKNOWN_SESSION) is False
    assert await request(client, 'PUT', f'/v1/session/destroy/{UNKNOWN_SESSION}') == (200, b'true')
    assert await request(client, 'GET', '/v1/kv/drakkar/nothing/', query=[('recurse', 'true')], answers=(404,)) == (
        404,
        b'',
    )
    assert await read_prefix(client, '/v1/kv/drakkar/nothing/') == []
    with pytest.raises(TransientError) as acquired:
        await acquire_key(client, '/v1/kv/drakkar/fleet/workers/w9', UNKNOWN_SESSION, 'x')
    assert str(acquired.value) == (
        'consul: PUT /v1/kv/drakkar/fleet/workers/w9 answered 500: '
        f'raft apply failed: invalid session "{UNKNOWN_SESSION}"'
    )
    for ttl, nanoseconds in (('5s', '5000000000'), ('86401s', '86401000000000')):
        with pytest.raises(TransientError) as created:
            await request(client, 'PUT', PATHS['session_create'], body=session_body('x', ttl))
        assert str(created.value) == (
            'consul: PUT /v1/session/create answered 500: '
            f"Invalid Session TTL '{nanoseconds}', must be between [10s=24h0m0s]"
        )


async def test_fake_acquire_keeps_the_holder_and_refuses_another_session(fake):
    client = fake_client(fake)
    first = await create_session(client, 'drakkar-w1-0123456789abcdef', 60)
    second = await create_session(client, 'drakkar-w1-fedcba9876543210', 120)

    assert await acquire_key(client, CARD_PATH, first, '{"card":1}') is True
    assert await acquire_key(client, CARD_PATH, first, '{"card":2}') is True
    assert await acquire_key(client, CARD_PATH, second, '{"card":3}') is False

    entry = await read_key(client, CARD_PATH)
    assert entry is not None
    assert (entry['Value'], entry['Session'], entry['LockIndex']) == (b'{"card":2}', first, 1)
    assert second == SECOND


async def test_fake_destroy_of_a_delete_session_frees_its_key_at_once(fake):
    client = fake_client(fake)
    first = await create_session(client, 'drakkar-w1-0123456789abcdef', 60)
    second = await create_session(client, 'drakkar-w1-fedcba9876543210', 60)
    await acquire_key(client, CARD_PATH, first, 'one')

    await destroy_session(client, first)

    assert await read_key(client, CARD_PATH) is None
    assert await renew_session(client, first) is False
    assert await acquire_key(client, CARD_PATH, second, 'two') is True


async def test_fake_ending_a_session_of_the_default_behavior_leaves_its_key_without_a_session(fake):
    client = fake_client(fake)
    _, answer = await request(
        client, 'PUT', PATHS['session_create'], body='{"LockDelay":"0s","Name":"lock","TTL":"10s"}'
    )
    session = json.loads(answer)['ID']
    await acquire_key(client, LOCK_PATH, session, 'body')

    await destroy_session(client, session)

    entry = await read_key(client, LOCK_PATH)
    assert entry is not None
    assert entry['Value'] == b'body'
    assert 'Session' not in entry


async def test_fake_checks_the_token_except_for_the_leader(fake):
    fake.token = CONFIGURED_TOKEN

    await check_leader(fake_client(fake))
    with pytest.raises(ConsulError) as anonymous:
        await create_session(fake_client(fake), 'x', 20)
    assert str(anonymous.value).startswith(
        'consul: PUT /v1/session/create answered 403: '
        'Permission denied: anonymous token lacks permission \'session:write\' on "consul-fake-node".'
    )
    assert await create_session(fake_client(fake, token=CONFIGURED_TOKEN), 'x', 20) == FIRST


async def test_fake_flags_only_answers_that_hid_something_from_a_token_without_rights(fake):
    fake.token = CONFIGURED_TOKEN
    fake.tokens_without_rights.add(WRONG_TOKEN)
    weak = fake_client(fake, token=WRONG_TOKEN)

    assert await request(weak, 'GET', LIST_PATH, query=[('recurse', 'true')], answers=(404,)) == (404, b'')
    assert await session_name(weak, UNKNOWN_SESSION) is None
    with pytest.raises(
        ConsulError, match=r"AccessorID '[0-9a-f-]+' lacks permission 'session:write' on \"consul-fake-node\""
    ):
        await create_session(weak, 'x', 20)
    with pytest.raises(ConsulError, match='lacks permission \'key:write\' on "drakkar/fleet/workers/w"'):
        await acquire_key(weak, path('registry_card', prefix='drakkar', worker='w'), FIRST, 'x')


async def test_fake_answers_the_leader_to_an_unknown_token(fake):
    fake.token = CONFIGURED_TOKEN

    assert await check_leader(fake_client(fake, token=WRONG_TOKEN)) is None


async def test_fake_refuses_a_datacenter_it_does_not_know(fake):
    with pytest.raises(TransientError, match='No path to datacenter'):
        await check_leader(fake_client(fake, datacenter='nope'))


async def test_fake_answers_a_request_it_does_not_model_with_a_400(fake):
    with pytest.raises(ConsulError, match='answered 400: fake consul'):
        await request(fake_client(fake), 'DELETE', CARD_PATH)


async def test_fake_injects_one_failure_or_every_failure(fake):
    client = fake_client(fake)
    fake.fail_next = 503

    with pytest.raises(TransientError):
        await check_leader(client)
    await check_leader(client)

    fake.fail_every = httpx.ConnectError('down')
    for _ in range(2):
        with pytest.raises(TransientError):
            await check_leader(client)
    fake.fail_every = None
    await check_leader(client)
    assert len(fake.seen) == 5


async def test_fake_hangs_only_the_next_request(fake):
    client = fake_client(fake)
    fake.hang = True

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(check_leader(client), timeout=0.05)
    await check_leader(client)
    assert len(fake.seen) == 2


async def test_fake_changes_its_state_before_it_loses_an_answer(fake):
    client = fake_client(fake)
    session = await create_session(client, 'drakkar-w-0123456789abcdef', 60)
    fake.fail_after_answer = httpx.ReadError('lost')

    with pytest.raises(TransientError):
        await acquire_key(client, CARD_PATH, session, 'card')

    entry = await read_key(client, CARD_PATH)
    assert entry is not None
    assert entry['Session'] == session


async def test_fake_calls_before_answer_with_each_request_first(fake):
    client = fake_client(fake)
    session = await create_session(client, 'drakkar-w-0123456789abcdef', 60)

    def end_at_the_acquire(seen: Seen) -> None:
        if seen.path == CARD_PATH:
            fake.end_session(session)

    fake.before_answer = end_at_the_acquire

    with pytest.raises(TransientError, match='invalid session'):
        await acquire_key(client, CARD_PATH, session, 'card')
    # Ending a session that already ended changes nothing.
    fake.end_session(session)


async def test_fake_plants_a_predecessor_and_a_raw_key(fake):
    client = fake_client(fake)
    fake.plant_session(PREDECESSOR, INPUTS['predecessor_name'], 30)
    fake.put_raw('drakkar/fleet/workers/search-worker-3', b'old card', session=PREDECESSOR)

    entry = await read_key(client, CARD_PATH)

    assert entry is not None
    assert (entry['Value'], entry['Session']) == (b'old card', PREDECESSOR)
    assert await session_name(client, PREDECESSOR) == INPUTS['predecessor_name']
    # The planted session takes no id from the ones the fake hands out.
    assert await create_session(client, 'x', 20) == FIRST
    fake.advance(60)
    assert await read_key(client, CARD_PATH) is None


def test_fake_put_raw_refuses_a_request_path_and_an_unknown_session(fake):
    with pytest.raises(ValueError, match='KV key'):
        fake.put_raw(CARD_PATH, b'x')
    with pytest.raises(KeyError):
        fake.put_raw('drakkar/fleet/workers/w', b'x', session=UNKNOWN_SESSION)


def connections_with(*, key_prefix: str) -> Connections:
    """Connections that declare one Consul entry, ``main``, with ``key_prefix``."""
    return Connections(ConnectionsConfig(consul={'main': ConsulConnectionConfig(key_prefix=key_prefix)}))


async def test_resolve_client_shares_one_client_per_connection():
    connections = connections_with(key_prefix='ops/fleet')
    opener = AsyncMock(return_value=object())
    closer = AsyncMock()

    async def resolve() -> tuple[object, str]:
        return await resolve_client(
            connections=connections,
            connection='main',
            preset_client=None,
            preset_prefix='ignored',
            open_client=opener,
            close_client=closer,
            who='registry',
        )

    first, second = await resolve(), await resolve()

    assert first == second
    assert first[1] == 'ops/fleet'
    opener.assert_awaited_once()
    assert opener.await_args is not None
    assert opener.await_args.args[0].key_prefix == 'ops/fleet'
    await connections.close_all()
    closer.assert_awaited_once_with(first[0])


async def test_resolve_client_prefers_the_preset_client(fake):
    connections = MagicMock()
    preset = fake_client(fake)

    result = await resolve_client(
        connections=connections,
        connection='main',
        preset_client=preset,
        preset_prefix='preset',
        open_client=AsyncMock(),
        close_client=AsyncMock(),
        who='registry',
    )

    assert result == (preset, 'preset')
    connections.acquire.assert_not_called()


async def test_resolve_client_without_connections_or_preset_raises():
    with pytest.raises(RuntimeError, match='the consul election needs the worker connections'):
        await resolve_client(
            connections=None,
            connection='main',
            preset_client=None,
            preset_prefix='',
            open_client=AsyncMock(),
            close_client=AsyncMock(),
            who='election',
        )
