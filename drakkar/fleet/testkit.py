"""Behaviour checks for registry providers.

A provider author runs these from their own test suite; the built-in
providers run them too. Each check is an ``async`` function that raises
``AssertionError`` with a plain message when the provider misbehaves. The
module needs no test framework.

Two groups:

- :func:`check_list_shape` and :func:`check_age_is_none_or_non_negative`
  apply to every provider. They take a provider instance and a context.
- :data:`PUBLISHING_CHECKS` apply to providers that store cards. Each takes
  ``(make_provider, advance, prune_after_seconds)``:

  - ``make_provider()`` returns a new, not yet started provider that shares
    its storage with the others the same check makes (two of them stand for
    two worker processes);
  - ``advance(seconds)`` makes every stored card ``seconds`` older on the
    provider's clock (move a fake clock, or set file times back);
  - ``prune_after_seconds`` is the age after which the provider deletes a
    card.

Example with pytest::

    @pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS)
    async def test_my_provider(check):
        store = MyStore()
        await check(lambda: MyProvider(store), store.advance, 600)
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import structlog

from drakkar.fleet.card import CARD_SCHEMA
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext, RegistryProvider

ProviderMaker = Callable[[], RegistryProvider]
Advance = Callable[[float], Any]

INSTANCE_A = '0123456789abcdef'
INSTANCE_B = 'fedcba9876543210'
# Slack for checks that compare ages: file times and clocks are not exact.
AGE_TOLERANCE_SECONDS = 1.0


def _expect(condition: bool, message: str) -> None:
    """Raise ``AssertionError(message)`` unless ``condition``; unlike ``assert``, never stripped by ``-O``."""
    if not condition:
        raise AssertionError(message)


def make_context(worker: str, instance: str = INSTANCE_A) -> RegistryContext:
    """A context for a provider run by worker ``worker``."""
    return RegistryContext(worker=worker, instance=instance, cluster='kit', logger=structlog.get_logger(), options={})


def make_card(worker: str, instance: str = INSTANCE_A, state: str = 'running') -> dict[str, Any]:
    """A valid schema-1 card for ``worker``."""
    return {
        'schema': CARD_SCHEMA,
        'worker': worker,
        'instance': instance,
        'cluster': 'kit',
        'url': 'http://127.0.0.1:8080/',
        'state': state,
    }


@asynccontextmanager
async def _running(
    make_provider: ProviderMaker, worker: str, instance: str = INSTANCE_A
) -> AsyncIterator[RegistryProvider]:
    provider = make_provider()
    await provider.start(make_context(worker, instance))
    try:
        yield provider
    finally:
        await provider.stop()


def _entries_of(listed: PeerCardList, worker: str) -> PeerCardList:
    return [entry for entry in listed if entry.card.get('worker') == worker]


def _age_of(listed: PeerCardList, worker: str) -> float:
    entries = _entries_of(listed, worker)
    _expect(len(entries) == 1, f'expected one card for {worker!r}, got {len(entries)}')
    age = entries[0].age_seconds
    if age is None:
        raise AssertionError(f'a publishing provider must know the age of the card of {worker!r}')
    return float(age)


# --- every provider ---


async def check_list_shape(provider: RegistryProvider, ctx: RegistryContext) -> None:
    """``list`` returns a list of ``PeerCard`` whose cards are dicts."""
    await provider.start(ctx)
    try:
        listed = await provider.list()
    finally:
        await provider.stop()
    _expect(isinstance(listed, list), f'list() must return a list, got {type(listed).__name__}')
    for entry in listed:
        _expect(isinstance(entry, PeerCard), f'list() entries must be PeerCard, got {type(entry).__name__}')
        _expect(isinstance(entry.card, dict), f'a PeerCard card must be a dict, got {type(entry.card).__name__}')


async def check_age_is_none_or_non_negative(provider: RegistryProvider, ctx: RegistryContext) -> None:
    """Every listed age is ``None`` (unknown) or a number not below zero."""
    await provider.start(ctx)
    try:
        listed = await provider.list()
    finally:
        await provider.stop()
    for entry in listed:
        age = entry.age_seconds
        _expect(
            age is None or (isinstance(age, int | float) and not isinstance(age, bool) and age >= 0),
            f'age_seconds must be None or >= 0, got {age!r}',
        )


# --- providers that store cards ---


async def check_publish_then_list(make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float) -> None:
    """A published card comes back from ``list`` unchanged."""
    card = make_card('kit-publish')
    async with _running(make_provider, 'kit-publish') as provider:
        await provider.publish(card)
        listed = await provider.list()
    entries = _entries_of(listed, 'kit-publish')
    _expect(len(entries) == 1, f'expected one card for kit-publish, got {len(entries)}')
    _expect(entries[0].card == card, f'the listed card differs from the published one: {entries[0].card!r}')


async def check_list_includes_own_card(
    make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float
) -> None:
    """``list`` returns the caller's own card too; the service, not the provider, leaves it out."""
    async with _running(make_provider, 'kit-own') as provider:
        await provider.publish(make_card('kit-own'))
        listed = await provider.list()
    _expect(len(_entries_of(listed, 'kit-own')) == 1, 'the provider did not list its own card')


async def check_two_workers_are_listed(
    make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float
) -> None:
    """Cards of two workers are both listed, whichever provider instance wrote them."""
    async with _running(make_provider, 'kit-first') as first, _running(make_provider, 'kit-second') as second:
        await first.publish(make_card('kit-first'))
        await second.publish(make_card('kit-second'))
        listed = await first.list()
    workers = sorted(entry.card['worker'] for entry in listed)
    _expect(workers == ['kit-first', 'kit-second'], f'expected both workers listed, got {workers}')


async def check_republish_replaces_the_card(
    make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float
) -> None:
    """A second publish by the same worker replaces its card; there is never more than one."""
    async with _running(make_provider, 'kit-states') as provider:
        await provider.publish(make_card('kit-states', state='starting'))
        await provider.publish(make_card('kit-states', state='running'))
        listed = await provider.list()
    entries = _entries_of(listed, 'kit-states')
    _expect(len(entries) == 1, f'expected one card after two publishes, got {len(entries)}')
    _expect(entries[0].card['state'] == 'running', 'the second publish did not replace the first card')


async def check_one_card_per_worker_name(
    make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float
) -> None:
    """Two processes with one worker name share one card: the last publish wins."""
    async with (
        _running(make_provider, 'kit-twin', INSTANCE_A) as first,
        _running(make_provider, 'kit-twin', INSTANCE_B) as second,
    ):
        await first.publish(make_card('kit-twin', INSTANCE_A))
        await second.publish(make_card('kit-twin', INSTANCE_B))
        listed = await first.list()
    entries = _entries_of(listed, 'kit-twin')
    _expect(len(entries) == 1, f'expected one card for a shared worker name, got {len(entries)}')
    _expect(entries[0].card['instance'] == INSTANCE_B, 'the last publish did not win')


async def check_withdraw_own_instance_only(
    make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float
) -> None:
    """``withdraw`` deletes a card only while it still holds the caller's instance."""
    async with (
        _running(make_provider, 'kit-withdraw', INSTANCE_A) as first,
        _running(make_provider, 'kit-withdraw', INSTANCE_B) as second,
    ):
        await first.publish(make_card('kit-withdraw', INSTANCE_A))
        await second.publish(make_card('kit-withdraw', INSTANCE_B))
        await first.withdraw('kit-withdraw', INSTANCE_A)
        after_foreign_withdraw = _entries_of(await second.list(), 'kit-withdraw')
        await second.withdraw('kit-withdraw', INSTANCE_B)
        after_own_withdraw = _entries_of(await second.list(), 'kit-withdraw')
    _expect(len(after_foreign_withdraw) == 1, 'a withdraw by another instance deleted the card')
    _expect(after_own_withdraw == [], 'a withdraw by the owning instance left the card in place')


async def check_age_grows_with_time(make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float) -> None:
    """The age of a card grows as time passes on the provider's clock."""
    step = min(30.0, prune_after_seconds / 4)
    async with _running(make_provider, 'kit-age') as provider:
        await provider.publish(make_card('kit-age'))
        before = _age_of(await provider.list(), 'kit-age')
        await _maybe_await(advance(step))
        after = _age_of(await provider.list(), 'kit-age')
    _expect(before >= 0, f'a fresh card has a negative age: {before}')
    _expect(
        abs(after - before - step) <= AGE_TOLERANCE_SECONDS,
        f'after {step} s the age moved from {before} to {after}',
    )


async def check_prune(make_provider: ProviderMaker, advance: Advance, prune_after_seconds: float) -> None:
    """A card stays listed while it is younger than the prune age, then ``list`` deletes it."""
    half = prune_after_seconds / 2
    async with _running(make_provider, 'kit-alive') as alive, _running(make_provider, 'kit-gone') as gone:
        await gone.publish(make_card('kit-gone'))
        await alive.publish(make_card('kit-alive'))
        await _maybe_await(advance(half))
        await alive.publish(make_card('kit-alive'))
        still_listed = _entries_of(await alive.list(), 'kit-gone')
        await _maybe_await(advance(half + 1))
        await alive.publish(make_card('kit-alive'))
        after_prune = await alive.list()
        listed_again = await alive.list()
    _expect(len(still_listed) == 1, 'a card younger than the prune age was not listed')
    _expect(_entries_of(after_prune, 'kit-gone') == [], 'a card older than the prune age was still listed')
    _expect(_entries_of(listed_again, 'kit-gone') == [], 'a pruned card came back')
    _expect(len(_entries_of(after_prune, 'kit-alive')) == 1, 'pruning removed a card that was just published')


async def _maybe_await(result: Any) -> None:
    """Let ``advance`` be a plain function or a coroutine function."""
    if isinstance(result, Awaitable):
        await result


PUBLISHING_CHECKS: tuple[Callable[[ProviderMaker, Advance, float], Awaitable[None]], ...] = (
    check_publish_then_list,
    check_list_includes_own_card,
    check_two_workers_are_listed,
    check_republish_replaces_the_card,
    check_one_card_per_worker_name,
    check_withdraw_own_instance_only,
    check_age_grows_with_time,
    check_prune,
)
