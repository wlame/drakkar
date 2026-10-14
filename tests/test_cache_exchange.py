"""The client helpers of the cache exchange: peer selection, backoff, one page, row checks."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import httpx
import pytest
from prometheus_client import REGISTRY

from drakkar.cache import exchange
from drakkar.cache.exchange import (
    PeerBackoff,
    PeerRequestError,
    PeerTarget,
    changes_url,
    fetch_page,
    keep_rows,
    page_params,
    select_targets,
)
from drakkar.fleet.service import PeerView

MARK = {'ms': 2000, 'key': 'k2'}


def _view(worker: str = 'w-2', *, cluster: str = 'search', online: bool | None = True, **card) -> PeerView:
    card.setdefault('url', f'http://{worker}:8080/')
    card.setdefault('state', 'running')
    card.setdefault('features', {'cache': {'cluster': MARK, 'global': {'ms': 1000, 'key': 'g'}}})
    return PeerView(
        card={'worker': worker, 'cluster': cluster, **card}, age_seconds=1.0, online=online, last_seen_ts=0.0
    )


def _select(views, *, cursors=None, backoff=None, own_cluster: str = 'search') -> list[PeerTarget]:
    return select_targets(
        views,
        own_worker='w-1',
        own_cluster=own_cluster,
        cursors=cursors or {},
        backoff=backoff or PeerBackoff(),
    )


# --- select_targets ---------------------------------------------------------


def test_a_same_cluster_peer_is_asked_for_cluster_and_global_rows():
    (target,) = _select([_view()])

    assert target == PeerTarget(name='w-2', url='http://w-2:8080/', scopes=('cluster', 'global'), mark=(2000, 'k2'))


def test_a_peer_of_another_cluster_is_asked_for_global_rows_only():
    (target,) = _select([_view(cluster='index')])

    assert target.scopes == ('global',)
    assert target.mark == (1000, 'g')


@pytest.mark.parametrize(
    'view',
    [
        _view(online=False),
        _view(state='starting'),
        _view(features={}),
        _view(features={'cache': {}}),
        _view(features={'cache': {'cluster': {'ms': '2000', 'key': 'k2'}}}),
        _view('w-1'),
        _view(url=None),
    ],
    ids=['offline', 'starting', 'no-cache-features', 'no-marks', 'malformed-mark', 'itself', 'no-url'],
)
def test_peers_that_cannot_or_need_not_be_pulled_are_skipped(view):
    assert _select([view]) == []


def test_a_static_peer_card_without_features_is_pulled_without_a_gate():
    # The static provider knows no features: its peers are pulled every
    # cycle, and their 404 (cache off) backs them off.
    view = PeerView(
        card={'schema': 1, 'worker': 'w-2', 'cluster': 'search', 'url': 'http://w-2:8080/'},
        age_seconds=None,
        online=None,
        last_seen_ts=None,
    )

    targets = _select([view], cursors={'w-2': (5, 'k')})

    assert targets == [PeerTarget(name='w-2', url='http://w-2:8080/', scopes=('cluster', 'global'), mark=None)]


def test_a_peer_of_unknown_liveness_is_pulled():
    assert [target.name for target in _select([_view(online=None)])] == ['w-2']


def test_a_peer_whose_marks_are_not_past_the_cursor_is_skipped():
    assert _select([_view()], cursors={'w-2': (2000, 'k2')}) == []
    assert [target.name for target in _select([_view()], cursors={'w-2': (2000, 'k1')})] == ['w-2']


def test_a_cut_mark_still_gates_against_a_full_key_cursor():
    # A card mark carries the first 128 characters of its key; a cursor on
    # the full key sorts after it, so nothing is pulled again.
    long_key = 'k' * 300
    view = _view(features={'cache': {'cluster': {'ms': 2000, 'key': long_key[:128]}}})

    assert _select([view], cursors={'w-2': (2000, long_key)}) == []
    assert [target.name for target in _select([view], cursors={'w-2': (2000, 'a')})] == ['w-2']


def test_a_global_mark_of_another_cluster_peer_is_compared_alone():
    # The peer's newer cluster mark is not ours to pull.
    assert _select([_view(cluster='index')], cursors={'w-2': (1000, 'g')}) == []


# --- PeerBackoff ------------------------------------------------------------


def test_backoff_skips_1_2_4_8_then_16_cycles_and_stays_there():
    backoff = PeerBackoff()

    skips = [backoff.failed('w-2')[1] for _ in range(7)]

    assert skips == [1, 2, 4, 8, 16, 16, 16]


def test_backoff_uses_up_one_cycle_per_skip_and_a_success_resets_it():
    backoff = PeerBackoff()
    backoff.failed('w-2')
    backoff.failed('w-2')

    assert [backoff.skip('w-2') for _ in range(3)] == [True, True, False]
    backoff.succeeded('w-2')
    assert backoff.failed('w-2') == (1, 1)


def test_select_targets_counts_a_backoff_cycle():
    backoff = PeerBackoff()
    backoff.failed('w-2')

    assert _select([_view()], backoff=backoff) == []
    assert [target.name for target in _select([_view()], backoff=backoff)] == ['w-2']


# --- request building -------------------------------------------------------


@pytest.mark.parametrize(
    ('base', 'expected'),
    [
        ('http://10.0.0.2:8080/', 'http://10.0.0.2:8080/api/v1/peer/cache/changes'),
        ('http://10.0.0.2:8080', 'http://10.0.0.2:8080/api/v1/peer/cache/changes'),
        ('https://proxy.internal/worker-2/', 'https://proxy.internal/worker-2/api/v1/peer/cache/changes'),
    ],
)
def test_changes_url_keeps_a_path_prefix(base, expected):
    assert changes_url(base) == expected


@pytest.mark.parametrize(
    ('base', 'expected'),
    [
        ('http://10.0.0.5:8080/?a=1&b=<2>', 'http://10.0.0.5:8080/api/v1/peer/cache/changes'),
        ('http://10.0.0.2:8080/#top', 'http://10.0.0.2:8080/api/v1/peer/cache/changes'),
    ],
)
def test_changes_url_drops_a_query_and_a_fragment(base, expected):
    # ui.public_url is free text: a query or a fragment on a card URL must
    # not end up in front of the path (the shared card fixture has one).
    assert changes_url(base) == expected


def test_page_params_carry_the_cursor_only_when_there_is_one():
    assert page_params(('cluster', 'global'), None, 500) == {'scopes': 'cluster,global', 'limit': '500'}
    assert page_params(('global',), (7, 'k'), 2) == {
        'scopes': 'global',
        'limit': '2',
        'after_ms': '7',
        'after_key': 'k',
    }


# --- fetch_page -------------------------------------------------------------

ROW = {
    'key': 'k',
    'scope': 'cluster',
    'value': '1',
    'size_bytes': 1,
    'created_at_ms': 1,
    'updated_at_ms': 2,
    'expires_at_ms': None,
    'origin_worker_id': 'w-2',
}


def _requests(outcome: str) -> float:
    labels = {'endpoint': 'cache_changes', 'outcome': outcome}
    return REGISTRY.get_sample_value('drakkar_fleet_peer_requests_total', labels) or 0.0


async def _fetch(handler, timeout: float = 5.0) -> tuple[list[dict], bool]:
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        return await fetch_page(
            client, 'http://w-2:8080/api/v1/peer/cache/changes', {'scopes': 'cluster'}, timeout=timeout
        )


async def test_fetch_page_returns_rows_and_more_and_counts_ok():
    before = _requests('ok')

    rows, more = await _fetch(lambda request: httpx.Response(200, json={'rows': [ROW], 'more': True}))

    assert (rows, more) == ([ROW], True)
    assert _requests('ok') == before + 1


def _raise(exc: Exception):
    def handler(request):
        raise exc

    return handler


@pytest.mark.parametrize(
    ('handler', 'outcome'),
    [
        (lambda request: httpx.Response(404, json={'error': 'off'}), 'http_error'),
        (_raise(httpx.ReadTimeout('slow')), 'timeout'),
        (_raise(httpx.ConnectError('refused')), 'transport_error'),
        (lambda request: httpx.Response(200, text='not json'), 'bad_body'),
        (lambda request: httpx.Response(200, json=[]), 'bad_body'),
        (
            lambda request: httpx.Response(200, json={'rows': [{**ROW, 'updated_at_ms': '2'}], 'more': False}),
            'bad_body',
        ),
        (lambda request: httpx.Response(200, json={'rows': [{**ROW, 'size_bytes': True}], 'more': False}), 'bad_body'),
        (lambda request: httpx.Response(200, json={'rows': [], 'more': 'no'}), 'bad_body'),
        (lambda request: httpx.Response(200, text='{"rows": [], "more": false} {}'), 'bad_body'),
        (lambda request: httpx.Response(200, json={'Rows': [], 'MORE': False}), 'bad_body'),
    ],
    ids=[
        'status',
        'timeout',
        'refused',
        'not-json',
        'not-object',
        'string-number',
        'bool-number',
        'bad-more',
        'trailing-data',
        'key-case',
    ],
)
async def test_fetch_page_classifies_every_failure(handler, outcome):
    before = _requests(outcome)

    with pytest.raises(PeerRequestError) as caught:
        await _fetch(handler)

    assert caught.value.outcome == outcome
    assert _requests(outcome) == before + 1


async def test_a_request_cut_by_the_cycle_deadline_is_not_counted():
    async def hang(request: httpx.Request) -> httpx.Response:
        await asyncio.Event().wait()
        raise AssertionError('unreachable')

    counted = {
        outcome: _requests(outcome) for outcome in ('ok', 'http_error', 'timeout', 'transport_error', 'bad_body')
    }

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(_fetch(hang), timeout=0.05)

    assert {outcome: _requests(outcome) for outcome in counted} == counted


@pytest.mark.parametrize(
    'rows',
    [
        [{**ROW, 'updated_at_ms': 2, 'key': 'k'}],
        [{**ROW, 'updated_at_ms': 3, 'key': 'b'}, {**ROW, 'updated_at_ms': 3, 'key': 'a'}],
    ],
    ids=['not-past-the-cursor', 'out-of-order'],
)
async def test_a_page_that_does_not_move_forward_is_a_bad_body(rows):
    # A faulty peer must not keep a pull looping until the cycle deadline.
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={'rows': rows, 'more': True}))
    ) as client:
        with pytest.raises(PeerRequestError) as caught:
            await fetch_page(client, 'http://w-2:8080/x', {}, timeout=5.0, after=(2, 'k'))

    assert caught.value.outcome == 'bad_body'


async def test_a_peer_that_trickles_its_answer_hits_the_total_timeout():
    # The timeout bounds the whole request, body included: a peer that
    # sends one byte at a time cannot hold a slot past it.
    async def trickle():
        for _ in range(40):
            await asyncio.sleep(0.05)
            yield b' '

    before = _requests('timeout')
    started = time.monotonic()

    with pytest.raises(PeerRequestError) as caught:
        await _fetch(lambda request: httpx.Response(200, content=trickle()), timeout=0.3)

    assert caught.value.outcome == 'timeout'
    assert time.monotonic() - started < 1.5
    assert _requests('timeout') == before + 1


async def test_an_answer_over_the_size_cap_is_a_bad_body(monkeypatch):
    monkeypatch.setattr(exchange, 'MAX_PAGE_BYTES', 100)
    big = {'rows': [{**ROW, 'value': 'x' * 200}], 'more': False}

    with pytest.raises(PeerRequestError) as caught:
        await _fetch(lambda request: httpx.Response(200, json=big))

    assert caught.value.outcome == 'bad_body'


CHANGES_FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'cache_changes.json').read_text(encoding='utf-8'))


@pytest.mark.parametrize(
    'case',
    CHANGES_FIXTURE['page']['requests'],
    ids=[case['name'] for case in CHANGES_FIXTURE['page']['requests']],
)
async def test_the_client_reads_every_page_of_the_shared_fixture(case):
    # Every implementation serves these exact answers; the client of every
    # implementation must read them.
    response = case['response']
    after = (int(case['params']['after_ms']), case['params']['after_key']) if 'after_ms' in case['params'] else None

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=response))
    ) as client:
        rows, more = await fetch_page(client, 'http://w-a:8080/x', {}, timeout=5.0, after=after)

    assert (rows, more) == (response['rows'], response['more'])


# --- keep_rows --------------------------------------------------------------


def test_keep_rows_drops_other_scopes_and_relayed_rows():
    target = PeerTarget(name='w-2', url='http://w-2:8080/', scopes=('global',), mark=(1, 'k'))
    rows = [
        {**ROW, 'key': 'g', 'scope': 'global'},
        {**ROW, 'key': 'c', 'scope': 'cluster'},
        {**ROW, 'key': 'l', 'scope': 'local'},
        {**ROW, 'key': 'r', 'scope': 'global', 'origin_worker_id': 'w-3'},
    ]

    kept, rejected = keep_rows(rows, target)

    assert kept == [('g', 'global', '1', 1, 1, 2, None, 'w-2')]
    assert rejected == {'scope': 2, 'origin': 1}
