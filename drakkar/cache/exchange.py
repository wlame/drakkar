"""The client side of the cache exchange: which peers to pull, how, and what to keep.

Every ``fleet.cache_exchange.interval_seconds`` the engine pulls the
``cluster`` and ``global`` rows other workers originated, over
``GET /api/v1/peer/cache/changes``. This module holds the parts that do
not need the engine's database:

- :func:`select_targets` — the peers worth a pull this cycle, from the
  registry's peer views and each peer's card marks;
- :class:`PeerBackoff` — 1, 2, 4 ... 16 skipped cycles after consecutive
  failures of one peer;
- :func:`fetch_page` — one HTTP request, classified into the
  ``drakkar_fleet_peer_requests_total`` outcomes;
- :func:`keep_rows` — the rows of a page this worker accepts.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import httpx

from drakkar import metrics
from drakkar.cache.changes import EXCHANGED_SCOPES, Cursor
from drakkar.fleet.service import PeerView

CHANGES_PATH = 'api/v1/peer/cache/changes'
ENDPOINT = 'cache_changes'
CONNECT_TIMEOUT_SECONDS = 2.0
MAX_SKIP_CYCLES = 16
# The largest page answer read (64 MiB); a larger one is a bad body. It
# bounds what a faulty peer, or a planted card URL, can make a worker hold.
MAX_PAGE_BYTES = 64 * 1024 * 1024

# The fields of a served row, with the types this worker accepts. A row is
# applied as a tuple in this order (the LWW upsert's parameter order).
ROW_FIELDS: tuple[tuple[str, tuple[type, ...]], ...] = (
    ('key', (str,)),
    ('scope', (str,)),
    ('value', (str,)),
    ('size_bytes', (int,)),
    ('created_at_ms', (int,)),
    ('updated_at_ms', (int,)),
    ('expires_at_ms', (int, type(None))),
    ('origin_worker_id', (str,)),
)


@dataclass(frozen=True, slots=True)
class PeerTarget:
    """A peer to pull from this cycle.

    ``mark`` is the newest card mark among the requested scopes: once a
    pull ends with ``more`` false, every row up to it has been served, so
    the cursor may move up to it (rows deleted or expired after the card
    was published would otherwise make every later cycle pull an empty
    page). ``None`` for a card without ``features`` (the ``static``
    provider), which is pulled every cycle.
    """

    name: str
    url: str
    scopes: tuple[str, ...]
    mark: Cursor | None


class PeerRequestError(Exception):
    """One request to a peer failed; ``outcome`` is its metric label."""

    def __init__(self, outcome: str, detail: str) -> None:
        super().__init__(detail)
        self.outcome = outcome


def _card_mark(raw: Any) -> Cursor | None:
    """A ``{"ms": int, "key": str}`` mark from a card, or ``None`` when it is missing or malformed."""
    if not isinstance(raw, Mapping):
        return None
    ms, key = raw.get('ms'), raw.get('key')
    if not isinstance(ms, int) or isinstance(ms, bool) or not isinstance(key, str):
        return None
    return (ms, key)


def scopes_for(peer_cluster: Any, own_cluster: str) -> tuple[str, ...]:
    """``cluster`` and ``global`` from a peer of our own cluster, else ``global`` only."""
    if peer_cluster == own_cluster:
        return EXCHANGED_SCOPES
    return ('global',)


def select_targets(
    views: Iterable[PeerView],
    *,
    own_worker: str,
    own_cluster: str,
    cursors: Mapping[str, Cursor],
    backoff: PeerBackoff,
) -> list[PeerTarget]:
    """The peers worth a pull this cycle.

    A peer is skipped when it is offline (``online`` false; ``None`` means
    unknown and is pulled), still starting, backing off, or when none of
    the scopes asked of it has a card mark past its cursor (a card with a
    ``features`` object but no ``cache`` entry: the peer does not serve the
    exchange). A card with no ``features`` object at all (the ``static``
    provider knows none) is pulled without that gate.
    Each skipped backoff cycle is counted here, so call it once per cycle.
    """
    targets: list[PeerTarget] = []
    for view in views:
        card = view.card
        name, url = card.get('worker'), card.get('url')
        if not isinstance(name, str) or not isinstance(url, str) or not name or name == own_worker:
            continue
        if view.online is False or card.get('state') == 'starting':
            continue
        if backoff.skip(name):
            continue
        scopes = scopes_for(card.get('cluster'), own_cluster)
        features = card.get('features')
        if not isinstance(features, Mapping):
            targets.append(PeerTarget(name=name, url=url, scopes=scopes, mark=None))
            continue
        served = features.get('cache')
        if not isinstance(served, Mapping):
            continue
        marks = [mark for scope in scopes if (mark := _card_mark(served.get(scope))) is not None]
        cursor = cursors.get(name)
        if not marks or (cursor is not None and max(marks) <= cursor):
            continue
        targets.append(PeerTarget(name=name, url=url, scopes=scopes, mark=max(marks)))
    return targets


class PeerBackoff:
    """Skipped cycles per failing peer: 1, 2, 4, 8, then 16 for every further failure; a success resets it."""

    def __init__(self) -> None:
        self._failures: dict[str, int] = {}
        self._skip_left: dict[str, int] = {}

    def skip(self, peer: str) -> bool:
        """True when ``peer`` sits this cycle out; each call uses up one skipped cycle."""
        left = self._skip_left.get(peer, 0)
        if left <= 0:
            return False
        self._skip_left[peer] = left - 1
        return True

    def failed(self, peer: str) -> tuple[int, int]:
        """Count a failure; returns ``(failures in a row, cycles to skip)``."""
        failures = self._failures.get(peer, 0) + 1
        self._failures[peer] = failures
        skip_cycles = min(2 ** (failures - 1), MAX_SKIP_CYCLES)
        self._skip_left[peer] = skip_cycles
        return failures, skip_cycles

    def succeeded(self, peer: str) -> None:
        self._failures.pop(peer, None)
        self._skip_left.pop(peer, None)


def changes_url(base_url: str) -> str:
    """The changes endpoint under a card URL; a path prefix in the URL (a proxy) is kept."""
    return urljoin(base_url if base_url.endswith('/') else base_url + '/', CHANGES_PATH)


def page_params(scopes: tuple[str, ...], after: Cursor | None, limit: int) -> dict[str, str]:
    params = {'scopes': ','.join(scopes), 'limit': str(limit)}
    if after is not None:
        params['after_ms'] = str(after[0])
        params['after_key'] = after[1]
    return params


async def fetch_page(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, str],
    *,
    timeout: float,
    after: Cursor | None = None,
) -> tuple[list[dict], bool]:
    """One page from a peer: ``(rows, more)``. Raises :class:`PeerRequestError` with the outcome label.

    ``timeout`` bounds the whole request, body included, so a peer that
    trickles its answer cannot hold a concurrency slot past it. The body is
    read up to ``MAX_PAGE_BYTES`` and decoded in a thread, off the event
    loop. Every request that ends is counted in
    ``drakkar_fleet_peer_requests_total`` and timed in
    ``drakkar_fleet_peer_request_duration_seconds``. A request the cycle
    deadline cancels is neither: it did not fail and did not succeed.
    The rows must come strictly after ``after`` and in cursor order; a page
    that does not move forward is a bad body, so a faulty peer cannot keep
    a pull looping until the cycle deadline.
    """
    started = time.monotonic()
    outcome: str | None = None
    try:
        try:
            # asyncio.timeout turns its own expiry into TimeoutError; the
            # cycle deadline's cancellation passes through as CancelledError.
            async with asyncio.timeout(timeout):
                body = await _read_page(client, url, params)
        except TimeoutError as exc:
            outcome = 'timeout'
            raise PeerRequestError(outcome, f'no complete answer within {timeout} s') from exc
        except httpx.TimeoutException as exc:
            outcome = 'timeout'
            raise PeerRequestError(outcome, str(exc) or 'timed out') from exc
        except httpx.TransportError as exc:
            outcome = 'transport_error'
            raise PeerRequestError(outcome, str(exc) or type(exc).__name__) from exc
        except PeerRequestError as exc:
            outcome = exc.outcome
            raise
        try:
            page = _parse_page(await asyncio.to_thread(json.loads, body))
            _check_order(page[0], after)
        except ValueError as exc:
            outcome = 'bad_body'
            raise PeerRequestError(outcome, str(exc)) from exc
        outcome = 'ok'
        return page
    finally:
        if outcome is not None:
            metrics.fleet_peer_requests.labels(endpoint=ENDPOINT, outcome=outcome).inc()
            metrics.fleet_peer_request_duration.labels(endpoint=ENDPOINT).observe(time.monotonic() - started)


async def _read_page(client: httpx.AsyncClient, url: str, params: dict[str, str]) -> bytes:
    """The body of a 200 answer, at most ``MAX_PAGE_BYTES``; ``PeerRequestError`` otherwise."""
    async with client.stream('GET', url, params=params) as response:
        if response.status_code != 200:
            raise PeerRequestError('http_error', f'HTTP {response.status_code}')
        chunks: list[bytes] = []
        size = 0
        async for chunk in response.aiter_bytes():
            size += len(chunk)
            if size > MAX_PAGE_BYTES:
                raise PeerRequestError('bad_body', f'the answer is over {MAX_PAGE_BYTES} bytes')
            chunks.append(chunk)
        return b''.join(chunks)


def _check_order(rows: list[dict], after: Cursor | None) -> None:
    """``ValueError`` unless every row is strictly after the previous one, the first after ``after``."""
    previous = after
    for row in rows:
        position = (row['updated_at_ms'], row['key'])
        if previous is not None and position <= previous:
            raise ValueError('the page does not move forward in (updated_at_ms, key) order')
        previous = position


def _parse_page(body: Any) -> tuple[list[dict], bool]:
    """Check the answer shape; ``ValueError`` names the first problem."""
    if not isinstance(body, dict):
        raise ValueError('the answer is not a JSON object')
    rows, more = body.get('rows'), body.get('more')
    if not isinstance(rows, list) or not isinstance(more, bool):
        raise ValueError('the answer has no rows list or no more flag')
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError('a row is not an object')
        for field, types in ROW_FIELDS:
            value = row.get(field)
            if not isinstance(value, types) or isinstance(value, bool):
                raise ValueError(f'a row has no valid {field}')
    return rows, more


def keep_rows(rows: list[dict], target: PeerTarget) -> tuple[list[tuple], dict[str, int]]:
    """The rows this worker applies, as upsert tuples, and the rejected counts by reason.

    Only rows of a scope asked of this peer and originated by it are kept:
    a planted card or a faulty peer cannot inject ``local`` rows, another
    cluster's ``cluster`` rows, or rows relayed from a third worker.
    """
    kept: list[tuple] = []
    rejected: dict[str, int] = {}
    for row in rows:
        if row['scope'] not in target.scopes:
            rejected['scope'] = rejected.get('scope', 0) + 1
        elif row['origin_worker_id'] != target.name:
            rejected['origin'] = rejected.get('origin', 0) + 1
        else:
            kept.append(tuple(row[field] for field, _types in ROW_FIELDS))
    return kept, rejected
