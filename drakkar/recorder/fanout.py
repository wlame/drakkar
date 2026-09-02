"""Live-stream fan-out for the flight recorder.

Owns everything between "an event was recorded" and "a browser sees it":
the subscriber set, the per-subscriber queues, the one-encode-per-event
wrapper, and the deferred ``task_started`` events that are held back until
a task proves it is slow enough to be worth showing.

Split out of :mod:`drakkar.recorder.core` so the fan-out can be reviewed
and tested without a database.
"""

from __future__ import annotations

import asyncio
import json
import queue
import time
from collections.abc import Collection
from dataclasses import dataclass, field

# Bound separately from the ``import queue`` above: WSSubscriber has a field
# NAMED ``queue``, which shadows the module inside the class body and makes
# the annotation there unresolvable. Method bodies are unaffected (class
# attributes are not in their lookup chain), so ``queue.Empty`` and
# ``queue.Full`` still read from the module.
from queue import Queue

from drakkar.metrics import recorder_ws_dropped_events
from drakkar.recorder.helpers import encode_ws_event, strip_ws_omitted

# Upper bound on the deferred-start sweep period. One sweep expires every
# task_started event whose ws_min_duration_ms threshold has passed, instead of
# one timer per task — the difference between ~10 and ~1200 timer operations
# per second on the main loop for a handler that fans one message out to a
# thousand tasks. The actual period is also capped by the threshold itself
# (see ``WSFanout.sweep_interval``).
DEFERRED_SWEEP_INTERVAL_SECONDS = 0.1

# Per-subscriber queue depth. Reached only when a client cannot keep up;
# beyond it events are dropped for that client alone so the recording hot
# path never blocks on a slow browser.
WS_SUBSCRIBER_QUEUE_SIZE = 10_000


class LiveEvent:
    """One recorded event on its way to the live stream, encoded on demand.

    A single instance is shared by every subscriber that wants the event, so
    the JSON encode happens **once no matter how many browsers are open**.
    Previously each ``/ws`` coroutine ran its own ``json.dumps`` over the
    same dict, making serialization cost scale with tab count.

    Encoding is deferred to first read rather than done at fan-out, which
    keeps it on the UI-server thread instead of the main loop — the
    pipeline pays only for one small object allocation per event.

    Two coroutines on the UI loop can reach :attr:`text` before either
    stores its result; there is no ``await`` between the check and the
    assignment, so the worst case is a duplicated encode producing an
    identical string. Not worth a lock.
    """

    __slots__ = ('_text', 'event')

    def __init__(self, event: dict) -> None:
        # Stripped here rather than at encode time. This wrapper sits in every
        # interested subscriber's queue — up to ``WS_SUBSCRIBER_QUEUE_SIZE``
        # deep — until the UI thread drains it, so holding the raw event would
        # pin the whole captured stdout and stderr of every queued task. One
        # throttled browser tab on a slow link could keep gigabytes alive that
        # way, on top of the recorder buffer, and take the worker out with it.
        # The fields are omitted from the wire anyway (WS_OMITTED_FIELDS), so
        # nothing downstream loses anything; the database buffer still holds
        # the original dict with the output intact.
        self.event = strip_ws_omitted(event)
        self._text: str | None = None

    @property
    def text(self) -> str:
        """The event as the JSON text sent over the wire."""
        if self._text is None:
            self._text = encode_ws_event(self.event)
        return self._text


@dataclass(eq=False)
class WSSubscriber:
    """One live ``/ws`` client's slot in the recorder's fan-out.

    ``eq=False`` keeps identity hashing, so instances can live in a ``set``
    and be removed by identity on unsubscribe.

    ``event_types``
        Optional allowlist of ``event`` names. ``None`` streams everything.
        Set by the client so a page pays only for the events it renders.

    ``queue``
        Thread-safe hand-off from the main loop (which records) to the UI
        server thread (which sends), carrying :class:`LiveEvent` wrappers.
        Prefer the accessors below over touching it directly.

    ``dropped``
        Events discarded because ``queue`` was full. The ``/ws`` route
        reports and clears this, letting the browser resync deliberately
        instead of drifting out of sync with no signal. Incremented from the
        main loop and cleared from the UI thread; a lost increment across
        that boundary would at worst delay one resync by a frame, so the
        plain ``int`` needs no lock.
    """

    event_types: frozenset[str] | None = None
    queue: Queue = field(default_factory=lambda: Queue(maxsize=WS_SUBSCRIBER_QUEUE_SIZE))
    dropped: int = 0

    def get_nowait(self) -> dict:
        """Pop the next event as the dict the client receives.

        Decoded from the wire text, not handed back from the recorder's
        own dict — so what this returns is exactly what the browser sees,
        including the omission of captured subprocess output. Callers that
        stream (the ``/ws`` route) use :meth:`drain_encoded` and never pay
        for the decode.
        """
        return json.loads(self.queue.get_nowait().text)

    def drain_encoded(self, limit: int) -> list[str]:
        """Pop up to ``limit`` events as pre-encoded JSON text.

        The streaming path: it never materializes the dicts, and the
        encoded text is shared with every other subscriber holding the same
        :class:`LiveEvent`.
        """
        out: list[str] = []
        try:
            while len(out) < limit:
                out.append(self.queue.get_nowait().text)
        except queue.Empty:
            pass
        return out

    def empty(self) -> bool:
        return self.queue.empty()

    def qsize(self) -> int:
        return self.queue.qsize()

    def take_dropped(self) -> int:
        """Return the drop count since the last call and reset it to zero.

        Subtracts rather than zeroing so an increment landing between the
        read and the write (from the main loop) is not lost.
        """
        n = self.dropped
        if n:
            self.dropped -= n
        return n


class WSFanout:
    """Subscriber registry + deferred-start bookkeeping for the live stream.

    Every method runs on the main (pipeline) loop except
    :meth:`subscribe` / :meth:`unsubscribe`, which the ``/ws`` route calls
    from the UI-server thread — see :meth:`broadcast` for why that is safe.
    """

    def __init__(self, ws_min_duration_ms: float) -> None:
        self.subscribers: set[WSSubscriber] = set()
        # Deferred WS broadcasts: task_started events are held for
        # ws_min_duration_ms before being sent to WebSocket. If the task
        # completes before the threshold, neither start nor completion is
        # sent (fast task, invisible to live UI). Exception: failed tasks
        # always go to WS regardless of duration.
        #
        # Values are ``(event, monotonic deadline)``. One periodic sweep
        # expires them all — see ``_sweep``. The earlier design created one
        # ``loop.call_later`` per task and cancelled it on completion, which a
        # fan-out handler turns into thousands of timer-heap operations per
        # second on the MAIN loop. CPython only purges cancelled TimerHandles
        # once they exceed half a heap of more than 100 entries, so at that
        # rate the heap churns continuously.
        self.deferred: dict[str, tuple[dict, float]] = {}
        # The single outstanding sweep timer, or None when nothing is
        # pending. Scheduled on demand and re-armed only while entries
        # remain, so an idle worker holds no timer at all.
        self.sweep_handle: asyncio.TimerHandle | None = None
        # Sweep period. Expiry is checked in batches rather than per task, so
        # a start event can be broadcast up to this late. That is acceptable
        # for a "this task is still running" signal, and the threshold itself
        # caps the period so a small ws_min_duration_ms is not swamped by it.
        self.sweep_interval = min(
            DEFERRED_SWEEP_INTERVAL_SECONDS,
            max(ws_min_duration_ms / 1000.0, 0.001),
        )

    # --- subscriber registry (called from the UI-server thread) ---

    def subscribe(self, event_types: Collection[str] | None = None) -> WSSubscriber:
        """Subscribe to the live event stream.

        ``event_types`` is an optional allowlist of ``event`` names. A
        subscriber that names the events it renders never pays for the
        rest: the filter is applied at fan-out, before encoding, so an
        unwanted event costs one set-membership test instead of a JSON
        encode, a queue slot and a WebSocket frame. ``None`` means "every
        event" — the back-compatible default for clients that predate the
        parameter.

        The distinction matters under fan-out-heavy workloads. A handler
        that turns one message into a thousand tasks emits a thousand
        ``task_complete`` events per message; a page that renders only the
        executor timeline should not receive any of them.
        """
        sub = WSSubscriber(event_types=frozenset(event_types) if event_types is not None else None)
        self.subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: WSSubscriber) -> None:
        """Unsubscribe from live event stream."""
        self.subscribers.discard(sub)

    # --- fan-out (called from the main loop) ---

    def broadcast(self, event: dict) -> None:
        """Push ``event`` to every interested subscriber, dropping when full.

        Iterates a **snapshot** of the subscriber set, not the set itself.
        ``subscribe``/``unsubscribe`` run on the UI-server thread (inside the
        ``/ws`` route) while this runs on the main loop, so iterating the live
        set raises ``RuntimeError: Set changed size during iteration`` the
        moment a browser tab opens or closes mid-fan-out. That exception would
        surface on the message-processing path — ``_record`` is called from
        ``PartitionProcessor.enqueue``, which has no ``except`` — and kill the
        worker. ``list(set)`` completes in C without executing bytecode, so no
        thread switch can occur inside it — the CPython-idiomatic
        equivalent of holding a lock across the snapshot.

        Every interested subscriber receives the **same** :class:`LiveEvent`
        wrapper, so the event is JSON-encoded once regardless of how many
        browsers are connected, and the encode itself happens later on the
        UI thread rather than here on the pipeline loop.

        Subscribers that filtered the event out are skipped before the
        wrapper is even allocated, so a page that renders only the executor
        timeline costs nothing for the thousand ``task_complete`` events a
        fan-out-heavy message produces.

        A subscriber whose queue is full has events dropped, as before, but
        the loss is now **counted**. The ``/ws`` route reports the count to
        the browser so it can resync instead of silently drifting.
        """
        if not self.subscribers:
            return
        name = event.get('event')
        wrapper: LiveEvent | None = None
        for sub in list(self.subscribers):
            if sub.event_types is not None and name not in sub.event_types:
                continue
            if wrapper is None:
                wrapper = LiveEvent(event)
            try:
                sub.queue.put_nowait(wrapper)
            except queue.Full:
                sub.dropped += 1
                recorder_ws_dropped_events.inc()

    # --- deferred task_started events ---

    def defer(self, task_id: str, event: dict, hold_seconds: float) -> None:
        """Hold ``event`` back for ``hold_seconds`` unless the task finishes."""
        self.deferred[task_id] = (event, time.monotonic() + hold_seconds)
        self._schedule_sweep()

    def take_deferred(self, task_id: str) -> dict | None:
        """Remove and return the held start event for ``task_id``, if any."""
        entry = self.deferred.pop(task_id, None)
        return entry[0] if entry is not None else None

    def _schedule_sweep(self) -> None:
        """Arm the sweep timer unless one is already outstanding."""
        if self.sweep_handle is not None:
            return
        loop = asyncio.get_running_loop()
        self.sweep_handle = loop.call_later(self.sweep_interval, self._sweep)

    def _sweep(self) -> None:
        """Broadcast every deferred start event whose threshold has passed.

        Cost is proportional to the number of EXPIRED entries, not to the
        number pending: ``ws_min_duration_ms`` is fixed for the process, so
        every entry gets the same offset and insertion order is deadline
        order. The loop therefore stops at the first entry that is not due.
        (``record_task_completed`` removes entries from the middle, which
        leaves the relative order of the rest intact.)

        Entries are collected before deletion — mutating a dict while
        iterating it raises.
        """
        self.sweep_handle = None
        now = time.monotonic()
        due: list[tuple[str, dict]] = []
        for task_id, (event, deadline) in self.deferred.items():
            if deadline > now:
                break
            due.append((task_id, event))
        for task_id, event in due:
            del self.deferred[task_id]
            self.broadcast(event)
        if self.deferred:
            self._schedule_sweep()

    def close(self) -> None:
        """Disarm the sweep timer and drop everything still deferred.

        Called from the recorder's ``stop()``: a live TimerHandle would keep
        firing against a stopped recorder, and the held start events have
        nowhere left to go.
        """
        if self.sweep_handle is not None:
            self.sweep_handle.cancel()
            self.sweep_handle = None
        self.deferred.clear()
