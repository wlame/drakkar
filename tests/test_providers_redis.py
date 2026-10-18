"""Unit tests for the Redis key and script corpus and the client helpers.

No Redis server: ``fakeredis`` runs the real command set and the real Lua, so
each script is executed as it is sent. The texts, keys and encodings are
checked against the shared fixture.
"""

import asyncio
import json
import subprocess
import sys
import time
import tomllib
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import fakeredis
import pytest
from redis.exceptions import (
    AuthenticationError,
    AuthorizationError,
    BusyLoadingError,
    DataError,
    MasterDownError,
    NoPermissionError,
    NoScriptError,
    RedisError,
    ResponseError,
    TryAgainError,
)
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from drakkar.config.connections import ConnectionsConfig, RedisConnectionConfig
from drakkar.fleet.card import serialize_card
from drakkar.providers.connections import Connections
from drakkar.providers.redis import (
    BAD_URL_MESSAGE,
    CONNECT_TIMEOUT_SECONDS,
    DEFAULT_KEY_PREFIX,
    KEY_TEMPLATES,
    PREFIX_PLACEHOLDER,
    REDIS_PROVIDER,
    SCOPE_PLACEHOLDER,
    SCRIPTS,
    TRANSIENT_REPLY_CODES,
    URL_QUERY_MESSAGE,
    WORKER_PLACEHOLDER,
    classify,
    close_client,
    fragment,
    is_transient_reply,
    key,
    milliseconds,
    open_client,
    resolve_client,
    run,
)
from drakkar.providers.resilience import TransientError
from drakkar.recorder.history import serialize_json
from tests.redis_fake import FakeClock, RecordingClient, fake_client

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_providers.json').read_text())['redis']
EXAMPLE = FIXTURE['example']

INDEX_KEY = 'drakkar:fleet:workers'
CARD_PREFIX = 'drakkar:fleet:workers:'
LOCK_KEY = 'drakkar:archive-locks:main'
LEASE_MS = 7_200_000


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    return FakeClock(monkeypatch)


@pytest.fixture
def server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


def expand_scripts(command: list[str]) -> list[str]:
    """Replace an ``@name`` argument with the script of that name, as the fixture's commands mean it."""
    return [SCRIPTS[argument[1:]] if argument.startswith('@') else argument for argument in command]


def test_importing_the_corpus_does_not_import_the_redis_client():
    """A worker that names no Redis provider must not pay for the client library."""
    result = subprocess.run(
        [sys.executable, '-c', 'import sys, drakkar.providers.redis; print("redis" in sys.modules)'],
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'False'


def test_placeholders_match_the_fixture():
    assert FIXTURE['placeholders'] == {
        'prefix': PREFIX_PLACEHOLDER,
        'worker': WORKER_PLACEHOLDER,
        'scope': SCOPE_PLACEHOLDER,
    }


def test_key_templates_match_the_fixture():
    assert FIXTURE['keys'] == KEY_TEMPLATES


def test_scripts_match_the_fixture():
    assert FIXTURE['scripts'] == SCRIPTS


def test_transient_codes_and_messages_match_the_fixture():
    assert list(TRANSIENT_REPLY_CODES) == FIXTURE['transient_reply_codes']
    assert FIXTURE['connect_timeout_seconds'] == CONNECT_TIMEOUT_SECONDS
    assert FIXTURE['messages']['bad_url'] == BAD_URL_MESSAGE
    assert FIXTURE['messages']['url_query'] == URL_QUERY_MESSAGE


def test_redis_floor_is_the_release_that_sets_status_code():
    # classify reads RedisError.status_code, which redis-py sets from 7.2 on; older
    # releases strip the reply code from the message and keep no copy of it.
    pyproject = tomllib.loads((Path(__file__).parent.parent / 'pyproject.toml').read_text())

    assert 'redis>=7.2' in pyproject['project']['dependencies']


def test_provider_name_and_default_prefix_follow_the_connection_config():
    assert REDIS_PROVIDER == 'redis'
    assert RedisConnectionConfig().key_prefix == DEFAULT_KEY_PREFIX


@pytest.mark.parametrize('case', FIXTURE['key_examples'], ids=lambda c: c['key'])
def test_key_renders_the_pinned_examples(case):
    assert key(case['name'], prefix=case['prefix'], worker=case['worker'], scope=case['scope']) == case['key']


@pytest.mark.parametrize('case', FIXTURE['fragments'], ids=lambda c: c['fragment'])
def test_fragment_matches_the_pinned_encoding(case):
    assert fragment(case['field'], case['value']) == case['fragment']


@pytest.mark.parametrize(('seconds', 'expected'), FIXTURE['milliseconds'])
def test_milliseconds_follows_the_pinned_rounding(seconds, expected):
    assert milliseconds(seconds) == str(expected)


def test_example_card_text_is_the_canonical_bytes():
    card = EXAMPLE['inputs']['card']

    assert serialize_card(card).decode() == EXAMPLE['commands']['publish'][5]


def test_example_lock_body_is_the_lock_encoding():
    inputs = EXAMPLE['inputs']
    body = {
        'acquired_at': inputs['acquired_at'],
        'instance': inputs['card']['instance'],
        'provider': REDIS_PROVIDER,
        'token': inputs['token'],
        'worker': inputs['card']['worker'],
    }

    assert serialize_json(body).decode() == EXAMPLE['commands']['acquire'][4]


def test_example_commands_follow_from_the_helpers():
    inputs = EXAMPLE['inputs']
    card = inputs['card']
    prefix = inputs['prefix']
    card_key = key('registry_card', prefix=prefix, worker=card['worker'])
    index = key('registry_index', prefix=prefix)
    lock = key('election_lock', prefix=prefix, scope=inputs['scope'])
    instance = fragment('instance', card['instance'])
    token = fragment('token', inputs['token'])
    lease = milliseconds(inputs['lease_seconds'])
    prune = milliseconds(inputs['prune_after_seconds'])
    card_text = serialize_card(card).decode()
    commands = {name: expand_scripts(command) for name, command in EXAMPLE['commands'].items()}

    assert commands['start'] == ['PING']
    assert commands['publish'] == ['EVAL', SCRIPTS['publish'], '2', card_key, index, card_text, prune, card['worker']]
    assert commands['list'] == ['EVAL', SCRIPTS['list'], '1', index, key('registry_card', prefix=prefix, worker='')]
    assert commands['withdraw'] == ['EVAL', SCRIPTS['withdraw'], '2', card_key, index, instance, card['worker']]
    assert commands['acquire'][:4] == ['EVAL', SCRIPTS['acquire'], '1', lock]
    assert commands['acquire'][5:] == [token, lease]
    assert commands['refresh'] == ['EVAL', SCRIPTS['refresh'], '1', lock, token, lease]
    assert commands['is_held'] == ['GET', lock]
    assert commands['release'] == ['EVAL', SCRIPTS['release'], '1', lock, token]


@pytest.mark.parametrize(
    ('message', 'expected'),
    [
        ('BUSY Redis is busy running a script', True),
        ('LOADING Redis is loading', True),
        ('MASTERDOWN Link with MASTER is down', True),
        ('TRYAGAIN Multiple keys request', True),
        ('BUSYGROUP Consumer Group name already exists', False),
        ('WRONGTYPE Operation against a key', False),
        ('NOPERM this user has no permissions', False),
        ('', False),
    ],
)
def test_is_transient_reply_matches_the_first_word(message, expected):
    assert is_transient_reply(message) is expected


# Replies are built as redis-py's parser builds them: parse_error strips a
# mapped code from the message and passes it as status_code.
def test_classify_wraps_a_connection_error_as_transient():
    original = RedisConnectionError('Connection closed by server.')

    result = classify(original)

    assert isinstance(result, TransientError)
    assert str(result) == str(original)
    assert result.__cause__ is original


@pytest.mark.parametrize(
    'exc',
    [
        BusyLoadingError('Redis is loading the dataset in memory', status_code='LOADING'),
        TryAgainError('Multiple keys request during rehashing of slot', status_code='TRYAGAIN'),
        MasterDownError('Link with MASTER is down and replica-serve-stale-data is set to no', status_code='MASTERDOWN'),
        ResponseError('BUSY Redis is busy running a script.'),
    ],
    ids=lambda exc: type(exc).__name__,
)
def test_classify_wraps_the_transient_replies(exc):
    result = classify(exc)

    assert isinstance(result, TransientError)
    assert result.__cause__ is exc


@pytest.mark.parametrize(
    'exc',
    [
        RedisConnectionError('max number of clients reached', status_code='ERR'),
        AuthenticationError('invalid username-password pair', status_code='WRONGPASS'),
        NoPermissionError('this user has no permissions', status_code='NOPERM'),
        ResponseError('WRONGTYPE Operation against a key holding the wrong kind of value'),
        ResponseError('BUSYGROUP Consumer Group name already exists'),
        NoScriptError('No matching script', status_code='NOSCRIPT'),
    ],
    ids=lambda exc: str(exc)[:30],
)
def test_classify_leaves_the_final_replies_alone(exc):
    assert classify(exc) is exc


def test_classify_turns_a_client_timeout_into_the_builtin_timeout():
    original = RedisTimeoutError('Timeout reading from socket')

    result = classify(original)

    assert isinstance(result, TimeoutError)
    assert not isinstance(result, TransientError)
    assert str(result) == str(original)
    assert result.__cause__ is original


@pytest.mark.parametrize('exc', [AuthenticationError('Invalid Username or Password'), AuthorizationError('no access')])
def test_classify_leaves_a_client_side_authentication_error_final(exc):
    assert classify(exc) is exc


@pytest.mark.parametrize('exc', [ValueError('not a redis error'), DataError('Invalid input'), RedisError('plain')])
def test_classify_leaves_other_errors_alone(exc):
    assert classify(exc) is exc


async def test_open_client_disables_client_retries_and_bounds_only_the_connect():
    client = await open_client(RedisConnectionConfig(url='redis://u:secret@h:6379/0'))

    # Look at a connection the pool builds, not at the keyword arguments: an argument
    # that is missing falls back to the library's default (a read limit, RESP3).
    # Making the connection opens no socket, so the host 'h' is never looked up.
    connection = client.connection_pool.make_connection()
    assert connection.retry.get_retries() == 0
    assert connection.socket_connect_timeout == CONNECT_TIMEOUT_SECONDS
    assert connection.socket_timeout is None
    assert connection.protocol == 2
    await close_client(client)


@pytest.mark.parametrize('case', FIXTURE['url_cases'], ids=lambda c: c['url'])
async def test_open_client_follows_the_pinned_url_cases(case):
    settings = RedisConnectionConfig(url=case['url'])

    if case['error'] is None:
        # Nothing connects until the first command, so a good URL opens without a server.
        await close_client(await open_client(settings))
        return
    with pytest.raises(ValueError) as info:
        await open_client(settings)

    assert str(info.value) == FIXTURE['messages'][case['error']]
    assert 'secret' not in str(info.value)
    assert info.value.__cause__ is None


async def test_run_returns_the_reply_of_the_command(server):
    client = RecordingClient(fake_client(server))

    assert await run(client, 'PING') is True
    assert client.calls == [('PING',)]


async def test_run_raises_classified_errors(server):
    client = RecordingClient(fake_client(server))
    original = RedisConnectionError('Connection closed by server.')
    client.fail_next = original

    with pytest.raises(TransientError) as info:
        await run(client, 'PING')

    assert info.value.__cause__ is original


async def test_run_raises_a_final_error_as_it_came(server):
    client = RecordingClient(fake_client(server))
    original = ResponseError('WRONGTYPE Operation against a key holding the wrong kind of value')
    client.fail_next = original

    with pytest.raises(ResponseError) as info:
        await run(client, 'GET', INDEX_KEY)

    assert info.value is original


async def test_publish_script_stamps_the_server_time_and_sets_both_ttls(clock, server):
    clock.set(1_800_000_000.000123)
    client = fake_client(server)
    card_key = f'{CARD_PREFIX}w-1'

    reply = await run(client, 'EVAL', SCRIPTS['publish'], '2', card_key, INDEX_KEY, 'the card', '86400000', 'w-1')

    assert reply == 1
    assert await client.get(card_key) == b'the card'
    assert await client.pttl(card_key) == 86_400_000
    # Redis reads a score back as a double, never as the digits that were sent.
    assert float(await client.zscore(INDEX_KEY, 'w-1')) == pytest.approx(1_800_000_000.000123, abs=1e-6)
    assert await client.pttl(INDEX_KEY) == 86_400_000


async def test_list_script_returns_live_cards_and_drops_expired_members(clock, server):
    client = fake_client(server)
    # The shorter TTL goes first: the index keeps the TTL of the last publish.
    for member, prune_ms in (('w-1', '30000'), ('w-2', '60000')):
        card_key = f'{CARD_PREFIX}{member}'
        await run(client, 'EVAL', SCRIPTS['publish'], '2', card_key, INDEX_KEY, f'card of {member}', prune_ms, member)
    clock.advance(31)

    reply = await run(client, 'EVAL', SCRIPTS['list'], '1', INDEX_KEY, CARD_PREFIX)

    assert [item.decode() for item in reply[:2]] == ['1800000031', '0']
    assert reply[2] == b'w-2'
    assert float(reply[3]) == 1_800_000_000.0
    assert reply[4] == b'card of w-2'
    assert len(reply) == 5
    assert await client.zrange(INDEX_KEY, 0, -1) == [b'w-2']


async def test_withdraw_script_deletes_only_for_the_matching_instance(server):
    client = fake_client(server)
    card_key = f'{CARD_PREFIX}w-1'
    card = serialize_card({'worker': 'w-1', 'instance': 'i-1', 'cluster': 'c'}).decode()
    await run(client, 'EVAL', SCRIPTS['publish'], '2', card_key, INDEX_KEY, card, '60000', 'w-1')

    other = await run(client, 'EVAL', SCRIPTS['withdraw'], '2', card_key, INDEX_KEY, fragment('instance', 'i-2'), 'w-1')

    assert other == 0
    assert await client.get(card_key) == card.encode()
    assert await client.zrange(INDEX_KEY, 0, -1) == [b'w-1']

    own = await run(client, 'EVAL', SCRIPTS['withdraw'], '2', card_key, INDEX_KEY, fragment('instance', 'i-1'), 'w-1')

    assert own == 1
    assert await client.get(card_key) is None
    assert await client.zrange(INDEX_KEY, 0, -1) == []


def lock_body(token: str) -> str:
    """The lock body of a holder, as the election writes it."""
    body = {
        'acquired_at': 'now',
        'instance': 'i-1',
        'provider': REDIS_PROVIDER,
        'token': token,
        'worker': 'w-1',
    }
    return serialize_json(body).decode()


async def acquire(client, token: str, lease_ms: int = LEASE_MS) -> int:
    """Run the acquire script for ``token``."""
    return await run(
        client, 'EVAL', SCRIPTS['acquire'], '1', LOCK_KEY, lock_body(token), fragment('token', token), str(lease_ms)
    )


async def test_acquire_script_takes_a_free_key_reacquires_for_the_holder_and_refuses_others(clock, server):
    client = fake_client(server)

    assert await acquire(client, 'a' * 16) == 1
    assert await client.get(LOCK_KEY) == lock_body('a' * 16).encode()
    assert await client.pttl(LOCK_KEY) == LEASE_MS

    clock.advance(10)
    assert await acquire(client, 'b' * 16) == 0
    assert await client.get(LOCK_KEY) == lock_body('a' * 16).encode()
    assert await client.pttl(LOCK_KEY) == LEASE_MS - 10_000

    # The holder is answered true again; its body stays and its lease starts over.
    assert await acquire(client, 'a' * 16, lease_ms=LEASE_MS + 5_000) == 1
    assert await client.get(LOCK_KEY) == lock_body('a' * 16).encode()
    assert await client.pttl(LOCK_KEY) == LEASE_MS + 5_000

    clock.advance(LEASE_MS / 1000 + 6)
    assert await acquire(client, 'b' * 16) == 1
    assert await client.get(LOCK_KEY) == lock_body('b' * 16).encode()


async def test_refresh_and_release_scripts_act_only_for_the_holder(clock, server):
    client = fake_client(server)
    holder, other = fragment('token', 'a' * 16), fragment('token', 'b' * 16)

    assert await run(client, 'EVAL', SCRIPTS['refresh'], '1', LOCK_KEY, holder, str(LEASE_MS)) == 0
    assert await run(client, 'EVAL', SCRIPTS['release'], '1', LOCK_KEY, holder) == 0

    await acquire(client, 'a' * 16)
    clock.advance(10)

    assert await run(client, 'EVAL', SCRIPTS['refresh'], '1', LOCK_KEY, other, str(LEASE_MS)) == 0
    assert await client.pttl(LOCK_KEY) == LEASE_MS - 10_000
    assert await run(client, 'EVAL', SCRIPTS['release'], '1', LOCK_KEY, other) == 0
    assert await client.exists(LOCK_KEY) == 1

    assert await run(client, 'EVAL', SCRIPTS['refresh'], '1', LOCK_KEY, holder, str(LEASE_MS)) == 1
    assert await client.pttl(LOCK_KEY) == LEASE_MS
    assert await run(client, 'EVAL', SCRIPTS['release'], '1', LOCK_KEY, holder) == 1
    assert await client.exists(LOCK_KEY) == 0
    assert await run(client, 'EVAL', SCRIPTS['release'], '1', LOCK_KEY, holder) == 0


def connections_with(*, key_prefix: str) -> Connections:
    """Connections that declare one Redis entry, ``main``, with ``key_prefix``."""
    return Connections(ConnectionsConfig(redis={'main': RedisConnectionConfig(key_prefix=key_prefix)}))


async def test_resolve_client_shares_one_client_per_connection():
    connections = connections_with(key_prefix='ops:')
    opener = AsyncMock(return_value=object())
    closer = AsyncMock()

    async def resolve() -> tuple[object, str]:
        return await resolve_client(
            connections=connections,
            connection='main',
            preset_client=None,
            preset_prefix='ignored:',
            open_client=opener,
            close_client=closer,
            who='registry',
        )

    first, second = await resolve(), await resolve()

    assert first == second
    assert first[1] == 'ops:'
    opener.assert_awaited_once()
    assert opener.await_args.args[0].key_prefix == 'ops:'


async def test_resolve_client_prefers_the_preset_client():
    connections = MagicMock()
    preset = object()

    result = await resolve_client(
        connections=connections,
        connection='main',
        preset_client=preset,
        preset_prefix='preset:',
        open_client=AsyncMock(),
        close_client=AsyncMock(),
        who='registry',
    )

    assert result == (preset, 'preset:')
    connections.acquire.assert_not_called()


async def test_resolve_client_without_connections_or_preset_raises():
    with pytest.raises(RuntimeError, match='the redis election needs the worker connections'):
        await resolve_client(
            connections=None,
            connection='main',
            preset_client=None,
            preset_prefix='',
            open_client=AsyncMock(),
            close_client=AsyncMock(),
            who='election',
        )


async def test_recording_client_refuses_an_argument_that_is_not_text(server):
    client = RecordingClient(fake_client(server))

    with pytest.raises(TypeError, match='must be str'):
        await client.execute_command('PEXPIRE', 'key', 1000)


async def test_recording_client_injects_one_failure_or_every_failure(server):
    client = RecordingClient(fake_client(server))
    client.fail_next = RedisConnectionError('gone')

    with pytest.raises(RedisConnectionError):
        await client.execute_command('PING')
    assert await client.execute_command('PING') is True

    client.fail_every = RedisConnectionError('down')
    for _ in range(2):
        with pytest.raises(RedisConnectionError):
            await client.execute_command('PING')
    client.fail_every = None
    assert await client.execute_command('PING') is True
    assert client.calls == [('PING',)] * 5


async def test_recording_client_hangs_only_the_next_command(server):
    client = RecordingClient(fake_client(server))
    client.hang = True

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(client.execute_command('PING'), timeout=0.05)
    assert await client.execute_command('PING') is True
    assert client.calls == [('PING',), ('PING',)]


def test_fake_clock_moves_the_wall_clock_for_one_test(clock):
    assert time.time() == 1_800_000_000.0
    clock.advance(2.5)
    assert time.time() == 1_800_000_002.5
    clock.set(5.0)
    assert time.time() == 5.0
