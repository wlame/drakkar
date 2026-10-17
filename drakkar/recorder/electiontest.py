"""Behaviour checks for election providers.

A provider author runs these from their own test suite; the built-in
``filesystem`` provider runs them too. Each check is an ``async`` function
that raises ``AssertionError`` with a plain message when the provider
misbehaves. The module needs no test framework.

Every check in :data:`ELECTION_CHECKS` takes
``(make_provider, advance, lease_seconds)``:

- ``make_provider()`` returns a new, not yet started provider that shares its
  storage with the others the same check makes (two of them stand for two
  worker processes);
- ``advance(seconds)`` makes every stored lease ``seconds`` older on the
  provider's clock (move a fake clock, or set file times back); it may be a
  plain function or a coroutine function;
- ``lease_seconds`` is the lease length the checks acquire with.

The checks start each provider with :func:`make_context`, whose
``shared_dir`` is empty: a provider that keeps its leases in a folder is
started on a test folder by the author's ``make_provider``.

Example with pytest::

    @pytest.mark.parametrize('check', electiontest.ELECTION_CHECKS)
    async def test_my_provider(check):
        service = MyFakeService()
        await check(lambda: MyElection(service), service.advance, 600)
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import structlog

from drakkar.recorder.election import ElectionContext, ElectionProvider

ElectionMaker = Callable[[], ElectionProvider]
Advance = Callable[[float], Any]

INSTANCE_A = '0123456789abcdef'
INSTANCE_B = 'fedcba9876543210'
TOKEN_A = 'aaaaaaaaaaaaaaaa'
TOKEN_B = 'bbbbbbbbbbbbbbbb'
SCOPE_A = 'kit-scope-a'
SCOPE_B = 'kit-scope-b'


def _expect(condition: bool, message: str) -> None:
    """Raise ``AssertionError(message)`` unless ``condition``; unlike ``assert``, never stripped by ``-O``."""
    if not condition:
        raise AssertionError(message)


def make_context(worker: str, instance: str = INSTANCE_A, shared_dir: str = '') -> ElectionContext:
    """A context for a provider run by worker ``worker``."""
    return ElectionContext(
        worker=worker,
        instance=instance,
        cluster='kit',
        shared_dir=shared_dir,
        logger=structlog.get_logger(),
        options={},
    )


@asynccontextmanager
async def _running(
    make_provider: ElectionMaker, worker: str, instance: str = INSTANCE_A
) -> AsyncIterator[ElectionProvider]:
    provider = make_provider()
    await provider.start(make_context(worker, instance))
    try:
        yield provider
    finally:
        await provider.stop()


async def _maybe_await(result: Any) -> None:
    """Let ``advance`` be a plain function or a coroutine function."""
    if isinstance(result, Awaitable):
        await result


async def check_one_holder_at_a_time(make_provider: ElectionMaker, advance: Advance, lease_seconds: float) -> None:
    """While one token holds a scope, another token cannot acquire it."""
    async with (
        _running(make_provider, 'kit-first', INSTANCE_A) as first,
        _running(make_provider, 'kit-second', INSTANCE_B) as second,
    ):
        first_took = await first.acquire(SCOPE_A, TOKEN_A, lease_seconds)
        second_took = await second.acquire(SCOPE_A, TOKEN_B, lease_seconds)
        first_holds = await first.is_held(SCOPE_A, TOKEN_A)
        second_holds = await second.is_held(SCOPE_A, TOKEN_B)
        await first.release(SCOPE_A, TOKEN_A)
    _expect(first_took is True, f'the first acquire of a free scope answered {first_took!r}')
    _expect(second_took is False, f'a second token acquired a held scope (answer {second_took!r})')
    _expect(first_holds is True, f'the holder is not reported holding its scope ({first_holds!r})')
    _expect(second_holds is False, f'a refused token is reported holding the scope ({second_holds!r})')


async def check_refresh_keeps_the_lease(make_provider: ElectionMaker, advance: Advance, lease_seconds: float) -> None:
    """A lease refreshed before it runs out is still held after more than its length has passed."""
    async with (
        _running(make_provider, 'kit-holder', INSTANCE_A) as holder,
        _running(make_provider, 'kit-taker', INSTANCE_B) as taker,
    ):
        took = await holder.acquire(SCOPE_A, TOKEN_A, lease_seconds)
        await _maybe_await(advance(lease_seconds * 0.75))
        refreshed = await holder.refresh(SCOPE_A, TOKEN_A)
        await _maybe_await(advance(lease_seconds * 0.75))
        taken = await taker.acquire(SCOPE_A, TOKEN_B, lease_seconds)
        still_held = await holder.is_held(SCOPE_A, TOKEN_A)
        await holder.release(SCOPE_A, TOKEN_A)
    _expect(took is True, f'the first acquire of a free scope answered {took!r}')
    _expect(refreshed is True, f'a refresh by the holder answered {refreshed!r}')
    _expect(taken is False, 'another token took over a lease that was refreshed in time')
    _expect(still_held is True, f'a refreshed lease is not held any more ({still_held!r})')


async def check_an_expired_lease_can_be_taken(
    make_provider: ElectionMaker, advance: Advance, lease_seconds: float
) -> None:
    """Once a lease is older than its length without a refresh, another token can acquire the scope."""
    async with (
        _running(make_provider, 'kit-dead', INSTANCE_A) as dead,
        _running(make_provider, 'kit-taker', INSTANCE_B) as taker,
    ):
        took = await dead.acquire(SCOPE_A, TOKEN_A, lease_seconds)
        await _maybe_await(advance(lease_seconds + 1))
        taken = await taker.acquire(SCOPE_A, TOKEN_B, lease_seconds)
        taker_holds = await taker.is_held(SCOPE_A, TOKEN_B)
        await taker.release(SCOPE_A, TOKEN_B)
    _expect(took is True, f'the first acquire of a free scope answered {took!r}')
    _expect(taken is True, f'an expired lease could not be taken over (answer {taken!r})')
    _expect(taker_holds is True, f'the new holder is not reported holding the scope ({taker_holds!r})')


async def check_a_taken_over_holder_is_fenced(
    make_provider: ElectionMaker, advance: Advance, lease_seconds: float
) -> None:
    """After a takeover the old holder is not held, cannot refresh, and its release leaves the new lease alone."""
    async with (
        _running(make_provider, 'kit-stalled', INSTANCE_A) as stalled,
        _running(make_provider, 'kit-taker', INSTANCE_B) as taker,
    ):
        await stalled.acquire(SCOPE_A, TOKEN_A, lease_seconds)
        await _maybe_await(advance(lease_seconds + 1))
        taken = await taker.acquire(SCOPE_A, TOKEN_B, lease_seconds)
        stalled_holds = await stalled.is_held(SCOPE_A, TOKEN_A)
        stalled_refreshed = await stalled.refresh(SCOPE_A, TOKEN_A)
        await stalled.release(SCOPE_A, TOKEN_A)
        taker_holds = await taker.is_held(SCOPE_A, TOKEN_B)
        await taker.release(SCOPE_A, TOKEN_B)
    _expect(taken is True, f'an expired lease could not be taken over (answer {taken!r})')
    _expect(stalled_holds is False, 'a holder that was taken over is still reported holding the scope')
    _expect(stalled_refreshed is False, 'a holder that was taken over could still refresh the lease')
    _expect(taker_holds is True, "the release of a taken-over holder freed the new holder's lease")


async def check_release_only_by_the_holder(
    make_provider: ElectionMaker, advance: Advance, lease_seconds: float
) -> None:
    """A release by another token changes nothing; the holder's release frees the scope for others."""
    async with (
        _running(make_provider, 'kit-holder', INSTANCE_A) as holder,
        _running(make_provider, 'kit-other', INSTANCE_B) as other,
    ):
        await holder.acquire(SCOPE_A, TOKEN_A, lease_seconds)
        await other.release(SCOPE_A, TOKEN_B)
        after_foreign_release = await holder.is_held(SCOPE_A, TOKEN_A)
        await holder.release(SCOPE_A, TOKEN_A)
        after_own_release = await holder.is_held(SCOPE_A, TOKEN_A)
        retaken = await other.acquire(SCOPE_A, TOKEN_B, lease_seconds)
        await other.release(SCOPE_A, TOKEN_B)
    _expect(after_foreign_release is True, "a release by another token freed the holder's lease")
    _expect(after_own_release is False, 'the holder still holds the scope after its own release')
    _expect(retaken is True, f'a released scope could not be acquired again (answer {retaken!r})')


async def check_scopes_are_independent(make_provider: ElectionMaker, advance: Advance, lease_seconds: float) -> None:
    """A lease on one scope neither blocks nor frees a lease on another."""
    async with (
        _running(make_provider, 'kit-first', INSTANCE_A) as first,
        _running(make_provider, 'kit-second', INSTANCE_B) as second,
    ):
        await first.acquire(SCOPE_A, TOKEN_A, lease_seconds)
        second_took_b = await second.acquire(SCOPE_B, TOKEN_B, lease_seconds)
        second_took_a = await second.acquire(SCOPE_A, TOKEN_B, lease_seconds)
        await first.release(SCOPE_A, TOKEN_A)
        second_holds_b = await second.is_held(SCOPE_B, TOKEN_B)
        await second.release(SCOPE_B, TOKEN_B)
    _expect(second_took_b is True, 'a lease on one scope blocked a free scope')
    _expect(second_took_a is False, 'a held scope was acquired by another token')
    _expect(second_holds_b is True, 'releasing one scope freed another')


ELECTION_CHECKS: tuple[Callable[[ElectionMaker, Advance, float], Awaitable[None]], ...] = (
    check_one_holder_at_a_time,
    check_refresh_keeps_the_lease,
    check_an_expired_lease_can_be_taken,
    check_a_taken_over_holder_is_fenced,
    check_release_only_by_the_holder,
    check_scopes_are_independent,
)
