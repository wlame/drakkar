"""Thread-pool offload for CPU-bound work inside handler hooks.

Why this exists
---------------
Every handler hook runs on the worker's single event loop. A hook that
spends seconds in pure-Python computation — deeply nested loops deriving
task parameters in ``arrange()``, result crunching in
``on_message_complete()`` — freezes the whole worker for that time:
Kafka polling, executor completions, sink flushes, the debug UI, all of
it. The runtime health monitor (:mod:`drakkar.runtimehealth`) *detects*
that state and captures the blocking stack; this module is the *remedy*.

``await self.offload(fn, *args)`` runs ``fn`` on a small dedicated
``ThreadPoolExecutor`` and returns its result. Total processing time
does not improve — under the GIL, pure-Python work is serialized with
everything else no matter which thread runs it — but the event loop
stays responsive: instead of one 25-second stall, the loop sees
millisecond-scale GIL handoffs and every other partition, sink, and
server keeps making progress.

Deliberately NOT the executor pool
----------------------------------
``ExecutorTask`` semantics are subprocess-shaped (source offsets,
priority gate, retries, sink flow, per-task events). The CPU work this
module serves happens *inside* a hook, before or after tasks exist, and
its result is needed inline — so it is a call-site await, not a
schedulable task. Keeping it out of :class:`~drakkar.executor.ExecutorPool`
keeps that scheduler and every stats surface built on it untouched.

What the offloaded function may do
----------------------------------
It is a plain synchronous function on a non-loop thread:

- It must not ``await`` (it is not a coroutine) and must not touch
  loop-bound framework objects.
- ``self.cache.peek`` / ``set`` / ``delete`` / ``in`` ARE allowed —
  :class:`~drakkar.cache.memory.Cache` guards its memory state with an
  internal lock precisely for this. The async ``cache.get`` (DB
  fallback) stays loop-only: warm the keys you need with ``await
  self.cache.get(...)`` *before* offloading, then ``peek`` inside.
- ``self.annotate(...)`` works: the hook's context is copied into the
  thread (``contextvars.copy_context``) and the recorder's record path
  is thread-tolerant (in-memory append, thread-safe metrics).

Cancellation contract
---------------------
Cancelling the awaiting side (rebalance revoking a partition, worker
shutdown) cancels a *queued* computation outright; a *running* one
cannot be interrupted — Python threads are not killable — so the thread
finishes its current function and the result is discarded. Long
offloaded functions that want to cooperate can check a flag the handler
manages itself.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import structlog

from drakkar import metrics
from drakkar.hookctx import current_hook_context

if TYPE_CHECKING:
    from collections.abc import Callable

    from drakkar.config import OffloadConfig
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()


@runtime_checkable
class OffloaderLike(Protocol):
    """Structural interface behind ``BaseDrakkarHandler.offload``.

    Mirrors :class:`~drakkar.cache.protocol.CacheLike` /
    :class:`~drakkar.annotations.AnnotatorLike`: both :class:`OffloadPool`
    and :class:`InlineOffloader` satisfy it without inheriting, and tests
    can substitute a recording fake.
    """

    async def run(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Execute ``fn(*args, **kwargs)`` off the event loop and return its result."""
        ...


class InlineOffloader:
    """Poolless stand-in used before startup and in unit tests.

    Same execution semantics as the real pool — the function still runs
    on a worker thread with the caller's context copied in
    (``asyncio.to_thread`` does both) — but no shared pool, no metrics,
    no recorder events. Handlers can therefore call ``self.offload(...)``
    unconditionally, exactly like ``self.cache`` / ``self.annotate``
    with their stubs in place.
    """

    async def run(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Run ``fn`` in the default executor via ``asyncio.to_thread``."""
        # functools.partial so keyword arguments can be forwarded — the
        # convention from the project style for run_sync-alikes.
        return await asyncio.to_thread(functools.partial(fn, *args, **kwargs))


def resolve_max_threads(configured: int, executor_pool_max: int) -> int:
    """The effective offload pool size for ``offload.max_threads``.

    An explicit positive value wins untouched. 0 (the config default) sizes
    the pool from the executor pool — ``ceil(executor.max_executors / 4)`` with a
    floor of 2 — so a bigger subprocess fleet gets proportionally more
    offload headroom without every deployment tuning a second knob.
    """
    if configured > 0:
        return configured
    return max(2, -(-executor_pool_max // 4))


class OffloadPool:
    """Bounded thread pool executing handler-offloaded computations.

    One instance per worker, shared by all hooks and partitions. Bounded
    on purpose: a cap gives queueing (visible in
    ``drakkar_offload_queued``) instead of unbounded GIL thrash when many
    partitions offload at once, and the queue depth is the operator's
    tuning signal for ``offload.max_threads``.
    """

    def __init__(
        self,
        config: OffloadConfig,
        *,
        recorder: EventRecorder | None = None,
        executor_pool_max: int = 0,
    ) -> None:
        """Create the pool.

        Args:
            config: sizing settings (``max_threads``; 0 = auto).
            recorder: flight recorder for per-call ``offload`` events, or
                ``None`` when the UI/recorder is disabled — the pool then
                feeds Prometheus only.
            executor_pool_max: the executor pool size the auto default
                scales from (see :func:`resolve_max_threads`); ignored
                when ``max_threads`` is explicit.
        """
        resolved = resolve_max_threads(config.max_threads, executor_pool_max)
        self._executor = ThreadPoolExecutor(
            max_workers=resolved,
            thread_name_prefix='drakkar-offload',
        )
        self._recorder = recorder
        self._max_threads = resolved
        # Own counters rather than reading the Prometheus gauges back:
        # the live-overview endpoint needs plain ints, and gauge internals
        # are private API. The counters are the source of truth and the
        # gauges mirror them. Guarded by a lock because transitions happen
        # on pool threads and on the loop thread (cancel path) — ``+=``
        # on an int is not atomic across threads.
        self._count_lock = threading.Lock()
        self._queued_count = 0
        self._running_count = 0

    @property
    def max_threads(self) -> int:
        """Effective pool size (``offload.max_threads``, auto-resolved when 0)."""
        return self._max_threads

    def snapshot(self) -> dict[str, int]:
        """Current pool state for the live-overview endpoint.

        Key-presence of this object in the overview payload is the UI's
        feature flag: a worker with no offload pool omits it entirely.
        """
        with self._count_lock:
            return {
                'running': self._running_count,
                'queued': self._queued_count,
                'max_threads': self._max_threads,
            }

    def _transition(self, *, queued_delta: int = 0, running_delta: int = 0) -> None:
        """Apply one state transition to the counters and mirror the gauges."""
        with self._count_lock:
            self._queued_count += queued_delta
            self._running_count += running_delta
            metrics.offload_queued.set(self._queued_count)
            metrics.offload_running.set(self._running_count)

    async def run(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        """Execute ``fn(*args, **kwargs)`` on the pool and await the result.

        Exceptions raised by ``fn`` propagate to the awaiting hook
        unchanged (after being recorded). See the module docstring for
        the cancellation contract.
        """
        # Copy the caller's context so hook coordinates (hookctx) and
        # structlog bindings survive into the thread — plain
        # ``ThreadPoolExecutor.submit`` would run ``fn`` context-free and
        # ``self.annotate()`` inside it would drop with reason
        # ``no_context``.
        ctx = contextvars.copy_context()
        call = functools.partial(ctx.run, functools.partial(fn, *args, **kwargs))

        # ``timing`` is written by the worker thread and read by this
        # coroutine only *after* the future completes (or is cancelled),
        # so no lock is needed: the future's completion is the
        # happens-before edge.
        timing: dict[str, float] = {}

        def _invoke() -> Any:
            # First line on the pool thread: queued -> running.
            self._transition(queued_delta=-1, running_delta=1)
            timing['started'] = time.monotonic()
            try:
                return call()
            finally:
                timing['finished'] = time.monotonic()
                self._transition(running_delta=-1)

        def _on_done(done_future: Future[Any]) -> None:
            # Runs for every terminal state. The one bookkeeping hole
            # ``_invoke`` cannot cover: a future cancelled while still
            # queued never runs ``_invoke``, so its ``queued`` increment
            # must be paid back here. ``Future.cancel()`` succeeds only
            # for never-started callables, so this branch and ``_invoke``
            # are exactly complementary — no leak, no double-decrement.
            if done_future.cancelled():
                self._transition(queued_delta=-1)

        submitted = time.monotonic()
        self._transition(queued_delta=1)
        # Submit directly (not loop.run_in_executor) so the
        # ``concurrent.futures.Future`` is in hand for the done-callback
        # above. ``asyncio.wrap_future`` provides the same awaitable +
        # cancellation bridging run_in_executor would.
        try:
            future = self._executor.submit(_invoke)
        except RuntimeError:
            # Executor already shut down (offload() racing worker stop).
            # ``_invoke`` never runs, so pay the increment back here.
            self._transition(queued_delta=-1)
            raise
        future.add_done_callback(_on_done)

        status = 'ok'
        error = ''
        try:
            return await asyncio.wrap_future(future)
        except asyncio.CancelledError:
            # Same class as concurrent.futures.CancelledError since 3.8,
            # so this covers both the queued-and-cancelled and the
            # awaiting-task-cancelled paths.
            status = 'cancelled'
            raise
        except Exception as exc:
            status = 'error'
            error = str(exc)
            raise
        finally:
            # Metrics + recorder from the awaiting side, on the loop, with
            # the hook context still bound — keeps the recorder call sites
            # single-threaded and the event anchored like an annotation.
            started = timing.get('started')
            finished = timing.get('finished')
            duration = (finished - started) if (started is not None and finished is not None) else 0.0
            queued_wait = (started - submitted) if started is not None else 0.0
            hook_ctx = current_hook_context()
            hook = hook_ctx.hook if hook_ctx is not None else 'none'
            if status != 'cancelled':
                metrics.offload_duration.labels(hook=hook).observe(duration)
            if self._recorder is not None:
                fn_name = getattr(fn, '__qualname__', None) or repr(fn)
                if hook_ctx is not None:
                    self._recorder.record_offload(
                        hook=hook,
                        partition=hook_ctx.partition,
                        function=fn_name,
                        duration=duration,
                        queued=queued_wait,
                        status=status,
                        error=error,
                        window_id=hook_ctx.window_id,
                        offsets=hook_ctx.offsets,
                        offset=hook_ctx.offset,
                        task_id=hook_ctx.task_id,
                    )
                else:
                    # offload() outside a framework-invoked hook (on_ready,
                    # a @periodic method): still worth a row, anchored to
                    # nothing — mirrors how periodic_run events live
                    # outside the message trace.
                    self._recorder.record_offload(
                        hook=hook,
                        partition=None,
                        function=fn_name,
                        duration=duration,
                        queued=queued_wait,
                        status=status,
                        error=error,
                    )

    def shutdown(self) -> None:
        """Stop the pool without waiting for a running computation.

        ``cancel_futures=True`` drops queued calls (their awaiting side
        sees ``CancelledError``); a computation already on a thread runs
        to completion in the background — Python threads cannot be
        interrupted — and its result is discarded. ``wait=False`` keeps
        worker shutdown from blocking behind a long crunch; the
        interpreter still joins the thread at exit.
        """
        self._executor.shutdown(wait=False, cancel_futures=True)
