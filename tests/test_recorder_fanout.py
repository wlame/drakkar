"""Unit tests for the recorder's live-stream fan-out.

These exercise :class:`WSFanout` on its own — no database, no recorder, no
event loop except where the deferred-sweep timer needs one.
"""

from __future__ import annotations

import asyncio

import pytest

from drakkar.recorder.fanout import LiveEvent, WSFanout, WSSubscriber


def make_event(name: str = 'task_started', **extra) -> dict:
    return {'event': name, 'ts': 0.0, 'dt': '1970-01-01T00:00:00.000000Z', **extra}


def test_live_event_encodes_once_and_caches():
    event = make_event()
    wrapper = LiveEvent(event)
    first = wrapper.text
    assert wrapper.text is first, 'the encoded text must be cached, not re-encoded'
    assert '"task_started"' in first


def test_subscribe_returns_registered_subscriber():
    fanout = WSFanout(ws_min_duration_ms=0)
    sub = fanout.subscribe()
    assert sub in fanout.subscribers
    assert sub.event_types is None


def test_subscribe_with_allowlist_stores_frozenset():
    fanout = WSFanout(ws_min_duration_ms=0)
    sub = fanout.subscribe(['task_started', 'task_completed'])
    assert sub.event_types == frozenset({'task_started', 'task_completed'})


def test_unsubscribe_removes_subscriber_and_is_idempotent():
    fanout = WSFanout(ws_min_duration_ms=0)
    sub = fanout.subscribe()
    fanout.unsubscribe(sub)
    fanout.unsubscribe(sub)
    assert not fanout.subscribers


def test_broadcast_delivers_to_every_subscriber():
    fanout = WSFanout(ws_min_duration_ms=0)
    first, second = fanout.subscribe(), fanout.subscribe()
    fanout.broadcast(make_event())
    assert first.get_nowait()['event'] == 'task_started'
    assert second.get_nowait()['event'] == 'task_started'


def test_broadcast_shares_one_wrapper_across_subscribers():
    fanout = WSFanout(ws_min_duration_ms=0)
    first, second = fanout.subscribe(), fanout.subscribe()
    fanout.broadcast(make_event())
    assert first.queue.get_nowait() is second.queue.get_nowait(), (
        'the event must be encoded once and shared, not encoded per subscriber'
    )


def test_broadcast_skips_subscriber_that_filtered_the_event_out():
    fanout = WSFanout(ws_min_duration_ms=0)
    sub = fanout.subscribe(['task_completed'])
    fanout.broadcast(make_event('task_started'))
    assert sub.empty()


def test_broadcast_with_no_subscribers_does_nothing():
    fanout = WSFanout(ws_min_duration_ms=0)
    fanout.broadcast(make_event())  # must not raise


def test_broadcast_counts_drops_when_subscriber_queue_is_full():
    fanout = WSFanout(ws_min_duration_ms=0)
    sub = WSSubscriber()
    sub.queue.maxsize = 1
    fanout.subscribers.add(sub)
    fanout.broadcast(make_event())
    fanout.broadcast(make_event())
    assert sub.qsize() == 1
    assert sub.take_dropped() == 1
    assert sub.take_dropped() == 0, 'take_dropped must reset the counter'


def test_broadcast_tolerates_subscribe_during_iteration():
    """The UI thread can subscribe while the main loop fans an event out."""
    fanout = WSFanout(ws_min_duration_ms=0)

    class SubscribingQueue:
        def put_nowait(self, item):
            fanout.subscribers.add(WSSubscriber())

    racer = WSSubscriber()
    racer.queue = SubscribingQueue()  # type: ignore[assignment]
    fanout.subscribers.add(racer)
    fanout.broadcast(make_event())  # must not raise "Set changed size during iteration"


def test_sweep_interval_is_capped_by_the_threshold():
    assert WSFanout(ws_min_duration_ms=10).sweep_interval == pytest.approx(0.01)
    assert WSFanout(ws_min_duration_ms=5000).sweep_interval == pytest.approx(0.1)
    assert WSFanout(ws_min_duration_ms=0).sweep_interval == pytest.approx(0.001)


async def test_defer_holds_the_event_until_the_deadline():
    fanout = WSFanout(ws_min_duration_ms=10)
    sub = fanout.subscribe()
    fanout.defer('t1', make_event(), hold_seconds=0.01)
    assert sub.empty(), 'the start event must not be broadcast immediately'
    await asyncio.sleep(0.1)
    assert sub.get_nowait()['event'] == 'task_started'
    assert not fanout.deferred


async def test_take_deferred_suppresses_the_pending_broadcast():
    fanout = WSFanout(ws_min_duration_ms=10)
    sub = fanout.subscribe()
    fanout.defer('t1', make_event(), hold_seconds=0.01)
    assert fanout.take_deferred('t1') is not None
    assert fanout.take_deferred('t1') is None, 'a taken entry must not come back'
    await asyncio.sleep(0.1)
    assert sub.empty(), 'a task that finished early must never reach the live stream'


async def test_one_sweep_timer_serves_every_deferred_entry():
    fanout = WSFanout(ws_min_duration_ms=1000)
    for i in range(200):
        fanout.defer(f't{i}', make_event(), hold_seconds=10.0)
    handle = fanout.sweep_handle
    assert handle is not None
    fanout.defer('t200', make_event(), hold_seconds=10.0)
    assert fanout.sweep_handle is handle, 'deferring must not arm a second timer'


async def test_sweep_stops_at_the_first_entry_that_is_not_due():
    fanout = WSFanout(ws_min_duration_ms=10)
    sub = fanout.subscribe()
    fanout.defer('due', make_event('a'), hold_seconds=-1.0)
    fanout.defer('later', make_event('b'), hold_seconds=60.0)
    await asyncio.sleep(0.05)
    assert sub.get_nowait()['event'] == 'a'
    assert sub.empty()
    assert set(fanout.deferred) == {'later'}


async def test_sweep_rearms_only_while_entries_remain():
    fanout = WSFanout(ws_min_duration_ms=10)
    fanout.defer('t1', make_event(), hold_seconds=0.01)
    await asyncio.sleep(0.1)
    assert not fanout.deferred
    assert fanout.sweep_handle is None, 'an idle fan-out must hold no timer'


async def test_close_disarms_the_timer_and_drops_deferred_entries():
    fanout = WSFanout(ws_min_duration_ms=10)
    sub = fanout.subscribe()
    fanout.defer('t1', make_event(), hold_seconds=0.01)
    fanout.close()
    assert fanout.sweep_handle is None
    assert not fanout.deferred
    await asyncio.sleep(0.1)
    assert sub.empty()
