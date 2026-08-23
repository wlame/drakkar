"""Subprocess executor pool for Drakkar framework."""

from __future__ import annotations

import asyncio
import fnmatch
import heapq
import itertools
import os
import signal
import sys
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.metrics import (
    executor_priority_fn_errors,
    executor_queue_wait,
    output_truncated,
    tasks_precomputed,
)
from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

# POSIX systems let us spawn a child in its own session/process group so we
# can signal the entire descendant tree on timeout (see ``_kill_process_tree``
# below). Windows has no ``os.killpg`` / session semantics — fall back to the
# parent-only kill on that platform.
_IS_POSIX = sys.platform != 'win32'

# Chunk size for the capped-capture pipe readers. 64 KiB matches the typical
# OS pipe buffer, so a full buffer drains in one read.
_READ_CHUNK_BYTES = 65536

# Cap on reaping a process after SIGKILL. A process that responds to SIGKILL
# at all dies in milliseconds; anything still alive after this is wedged in
# the kernel (uninterruptible sleep on a hung mount or failing disk) and
# waiting longer gains nothing. Bounded because this sits on the shutdown
# path, where an unbounded wait hangs the whole worker.
_KILL_REAP_TIMEOUT_SECONDS = 5.0


def _trim_incomplete_utf8(data: bytes) -> bytes:
    """Strip a trailing incomplete-but-valid UTF-8 sequence (at most 3 bytes).

    Applied only to truncated captures: a byte-exact cut can split a
    multi-byte character, and the dangling prefix decodes differently across
    backends (Python emits one replacement char per maximal subpart, Go one
    per byte). Removing the incomplete tail keeps the decoded text identical
    on both. Genuinely invalid tails (an 0xF8-0xFF lead, a stray
    continuation byte) are left in place — ``decode(errors='replace')``
    already renders those identically on both backends.
    """
    for back in range(1, min(3, len(data)) + 1):
        byte = data[-back]
        if byte < 0x80:
            # ASCII tail — nothing dangling.
            return data
        if byte >= 0xC0:
            # Lead byte: how long should its sequence be?
            if byte < 0xE0:
                expected = 2
            elif byte < 0xF0:
                expected = 3
            elif byte < 0xF8:
                expected = 4
            else:
                # Invalid lead byte — leave it for decode-replace.
                return data
            if expected > back:
                # Fewer continuation bytes present than promised — the cut
                # split this character. Strip the whole partial sequence.
                return data[:-back]
            return data
        # 0x80-0xBF: continuation byte — keep scanning backwards.
    return data


# ``priority_fn`` callable signature. The return value is anything ``heapq``
# can compare with ``__lt__`` — typically an int (Kafka offset), a tuple
# (e.g. ``(partition, offset)`` for partition-aware ordering), or any
# user-defined sortable value. Smaller values get scheduled first.
PriorityFn = Callable[[ExecutorTask], Any]


def default_priority(task: ExecutorTask) -> int:
    """Default task-priority key: smallest source offset.

    Earlier messages drain from the wait queue first. This keeps the
    ``MessageTracker`` / ``OffsetTracker`` state in front of the watermark
    small — the slowest task in a fan-out no longer anchors the whole
    message in memory.

    Returns ``0`` for tasks with no ``source_offsets`` (synthetic tasks,
    tests). Such tasks all share the same priority and degrade to the
    gate's stable FIFO tiebreak — matching the semaphore's pre-priority
    behaviour for those callers.
    """
    return min(task.source_offsets) if task.source_offsets else 0


class PriorityGate:
    """Async slot-allocator that selects the next waiter from a min-heap.

    Drop-in replacement for :class:`asyncio.Semaphore` with the same
    ``acquire`` / ``release`` surface, except that contended ``acquire``
    callers wake up in priority order rather than FIFO arrival order.
    The recorder of who-goes-next is a heap of ``(priority, seq, future)``
    triples; ``release`` pops the smallest priority and resolves that
    future. Equal-priority callers are tiebroken by ``seq``, a monotone
    counter that preserves insertion order — so within a priority band
    we still behave like a FIFO semaphore.

    Fast path: when a slot is free AND no one is waiting, ``acquire``
    decrements the counter and returns immediately. Same overhead as
    a Semaphore; the heap is never touched.

    Cancellation: callers awaiting their future may be cancelled while
    sitting in the heap. We use *lazy deletion* — the cancelled future
    stays in the heap until ``release`` pops it, at which point it is
    skipped (its slot pass-through goes to the next live waiter).
    Eager removal would require an O(N) heap walk on every cancel; the
    lazy variant is O(1) amortised against eventual ``release`` calls.

    Cancellation race: if ``release`` resolved a future BUT the awaiter
    was cancelled before consuming the slot, the slot would otherwise
    leak. ``acquire``'s ``except CancelledError`` checks for that case
    (``fut.done() and not fut.cancelled()``) and calls ``_release_slot``
    to give the slot back so it isn't lost.

    The gate is single-loop only — ``loop.create_future()`` binds each
    waiter's future to the loop the awaiter is running on, just like
    Semaphore. The same ``ExecutorPool`` invariant applies (one pool
    per worker, one event loop per pool).
    """

    def __init__(self, max_slots: int) -> None:
        if max_slots < 1:
            raise ValueError(f'max_slots must be >= 1, got {max_slots}')
        self._max = max_slots
        self._available = max_slots
        # Heap entries: (priority, seq, future). ``seq`` is the per-instance
        # monotone counter that tiebreaks equal-priority waiters AND
        # prevents heapq from ever comparing two ``Future`` objects (which
        # raises TypeError because Future has no ``__lt__``).
        self._heap: list[tuple[Any, int, asyncio.Future]] = []
        self._seq = itertools.count()

    @property
    def available(self) -> int:
        """Free slots, in [0, max_slots]. Drops to zero under saturation."""
        return self._available

    @property
    def waiters(self) -> int:
        """Approximate count of pending acquirers.

        Includes cancellation tombstones that haven't been popped yet
        — this is an upper bound, not a precise count. For a precise
        gauge we would walk the heap and skip ``fut.cancelled()``,
        which is O(N); the upper bound is fine for the metric's purpose
        ("are we backed up").
        """
        return len(self._heap)

    async def acquire(self, priority: Any = 0) -> None:
        """Acquire a slot, possibly waiting in priority order.

        ``priority`` may be any heapq-comparable value (int, tuple, etc.).
        Smaller values are served first.
        """
        # Fast path: a slot is free and the heap is empty. This happens
        # whenever the pool is undersubscribed and is the cheapest path
        # — same overhead as a plain Semaphore.acquire() would have.
        if self._available > 0 and not self._heap:
            self._available -= 1
            return

        # Slow path: queue up in the priority heap and wait.
        # get_running_loop(): we are always inside a coroutine here, and
        # get_event_loop() is deprecated in that context since 3.10.
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        seq = next(self._seq)
        heapq.heappush(self._heap, (priority, seq, fut))

        try:
            await fut
        except asyncio.CancelledError:
            # Cancellation race: if the future already received its slot
            # via ``set_result`` (we were the next-best waiter and a
            # release fired) but the await raised before we consumed it,
            # the slot would otherwise leak. Hand it back so the next
            # waiter — or the next acquire's fast path — can pick it up.
            #
            # If the future is still pending or already cancelled, we
            # never received the slot. The heap still contains our
            # entry as a tombstone; the next ``release`` will pop and
            # skip it.
            if fut.done() and not fut.cancelled():
                self._release_slot()
            raise

    def release(self) -> None:
        """Release one slot; wake the highest-priority pending waiter, if any."""
        self._release_slot()

    def _release_slot(self) -> None:
        """Internal slot release. Skips cancellation tombstones lazily."""
        while self._heap:
            _, _, fut = heapq.heappop(self._heap)
            if fut.cancelled():
                # Tombstone — the awaiter was cancelled. Try the next.
                continue
            # Live waiter: hand them the slot. ``set_result`` is safe
            # here because the future is neither cancelled nor done
            # (the heap only ever holds futures we created and that
            # have not yet been resolved).
            fut.set_result(None)
            return
        # Heap is empty (or only had tombstones). Bump the available
        # counter so the next acquire's fast path can pick it up.
        self._available += 1


class ExecutorPool:
    """Manages concurrent subprocess execution with semaphore-based limiting.

    Uses asyncio.create_subprocess_exec (not shell) for safe subprocess execution.
    Arguments are passed as a list, preventing shell injection.

    The binary to execute is resolved per-task: ``ExecutorTask.binary_path``
    takes precedence over the pool-level ``binary_path`` (from config).
    Either one can be ``None`` as long as the other is set. If neither is
    provided, the task fails with a clear ``ExecutorTaskError``.
    This allows handlers to run different binaries per message by setting
    ``binary_path`` on the task returned from ``arrange()``.
    """

    def __init__(
        self,
        binary_path: str | None,
        max_executors: int,
        task_timeout_seconds: int,
        env: dict[str, str] | None = None,
        inherit_parent_env: bool = True,
        inherit_deny_patterns: list[str] | None = None,
        priority_fn: PriorityFn | None = None,
        max_stdout_bytes: int = 0,
        max_stderr_bytes: int = 0,
    ) -> None:
        """Configure the pool.

        Args:
            priority_fn: optional callable that returns a sortable
                priority key for an :class:`ExecutorTask` waiting for a
                slot. When the pool is saturated, callers wake up in
                ascending priority order (smallest first). ``None``
                (default) uses :func:`default_priority`, which keys on
                the task's smallest source offset — earlier Kafka
                messages drain first so ``on_message_complete``
                fires sooner, ``_message_trackers`` clears faster, and
                the ``OffsetTracker`` watermark advances. The wiring
                from the handler's ``task_priority`` method to this
                kwarg lives in :class:`drakkar.app.DrakkarApp`.
            max_stdout_bytes: 0 (the default) = unlimited; positive
                values cap how many bytes of stdout are retained per
                task.
            max_stderr_bytes: 0 (the default) = unlimited; positive
                values cap how many bytes of stderr are retained per
                task.
        """
        self._binary_path = binary_path
        self._max_executors = max_executors
        self._task_timeout = task_timeout_seconds
        self._config_env = env or {}
        self._inherit_parent_env = inherit_parent_env
        # Patterns are compared case-insensitively against env var names.
        self._inherit_deny_patterns = list(inherit_deny_patterns or [])
        # Upper-cased once at construction: the matcher previously re-cased
        # every (constant) pattern for every env var, for every task.
        self._denied_patterns_upper = [p.upper() for p in self._inherit_deny_patterns]
        # Filled on first use by _filtered_parent_env(); see its docstring.
        self._filtered_parent_env_cache: dict[str, str] | None = None
        # Priority gate replaces ``asyncio.Semaphore``. Same acquire/release
        # contract; contended waiters wake in priority order rather than
        # FIFO. See ``PriorityGate`` for the design.
        self._gate = PriorityGate(max_executors)
        self._priority_fn: PriorityFn = priority_fn or default_priority
        self._max_stdout_bytes = max_stdout_bytes
        self._max_stderr_bytes = max_stderr_bytes
        self._active_count = 0
        self._waiting_count = 0
        self._available_slots: list[int] = list(range(max_executors))
        heapq.heapify(self._available_slots)

    @property
    def active_count(self) -> int:
        return self._active_count

    @property
    def waiting_count(self) -> int:
        return self._waiting_count

    @property
    def max_executors(self) -> int:
        return self._max_executors

    async def execute(
        self,
        task: ExecutorTask,
        recorder: EventRecorder | None = None,
        partition_id: int = 0,
    ) -> ExecutorResult:
        """Execute a single task, respecting the concurrency semaphore.

        Records task_started AFTER acquiring the semaphore slot, so the
        timestamp reflects actual execution start, not queue entry time.

        Cancellation safety: every counter and pool-slot increment is paired
        with a decrement in a `finally` so that ``asyncio.CancelledError``
        (raised during shutdown or rebalance) cannot leak slots. A leak here
        would permanently shrink effective pool capacity.

        Fast track — precomputed tasks: if ``task.precomputed`` is set, the
        handler has supplied the outcome itself (cache hit, lookup-table
        answer, deterministic shortcut). We skip the semaphore AND the
        subprocess entirely, synthesise an ExecutorResult, record synthetic
        events marked ``precomputed=true``, and treat non-zero exit codes
        the same as a real subprocess failure (raise ExecutorTaskError so
        on_error fires). This path intentionally does NOT contribute to
        pool-utilisation metrics or the executor_duration histogram — it
        does no pool work.
        """
        if task.precomputed is not None:
            return self._execute_precomputed(task, recorder, partition_id)

        # Compute the priority key BEFORE entering the gate. ``priority_fn``
        # is operator-supplied (typically ``handler.task_priority``); a buggy
        # override must not stall a task, so wrap the call in
        # ``_compute_priority`` which logs + ticks a metric and falls back
        # to the default on raise.
        priority = self._compute_priority(task)
        self._waiting_count += 1
        waiting_decremented = False
        try:
            # `async with` would also work, but we need to guarantee
            # waiting_count rollback when cancellation fires before acquire.
            # The wait is timed: it is the "pool is the bottleneck" signal —
            # wall time a task spent queued before any work could begin.
            wait_started = time.monotonic()
            await self._gate.acquire(priority)
            queue_wait = time.monotonic() - wait_started
            executor_queue_wait.observe(queue_wait)
            try:
                # Transition from "waiting" to "active". These two lines run
                # with no `await` between them, so they are atomic w.r.t.
                # cancellation on a single event loop.
                self._waiting_count -= 1
                waiting_decremented = True
                self._active_count += 1
                slot = heapq.heappop(self._available_slots)
                try:
                    if recorder:
                        recorder.record_task_started(
                            task,
                            partition_id,
                            pool_active=self._active_count,
                            pool_waiting=self._waiting_count,
                            slot=slot,
                            queue_wait_ms=round(queue_wait * 1000, 1),
                        )
                    return await self._run_subprocess(task)
                finally:
                    heapq.heappush(self._available_slots, slot)
                    self._active_count -= 1
            finally:
                self._gate.release()
        finally:
            if not waiting_decremented:
                # Cancelled before (or while) acquiring — the increment above
                # was never paired with the inner decrement. Rollback now.
                self._waiting_count -= 1

    def _compute_priority(self, task: ExecutorTask) -> Any:
        """Evaluate ``priority_fn(task)`` with a graceful fallback.

        A user-supplied ``priority_fn`` may raise (logic bug, missing
        attribute on a custom task subclass, etc.). Letting that
        propagate would convert a buggy override into a hard task
        failure — far worse than the original FIFO behaviour we are
        replacing. Instead we tick
        ``drakkar_executor_priority_fn_errors_total`` so operators see
        the rate, log a warning with the failing task id, and fall back
        to ``default_priority`` so the task is still scheduled.
        """
        try:
            return self._priority_fn(task)
        except Exception as exc:
            executor_priority_fn_errors.inc()
            logger.warning(
                'priority_fn_failed',
                category='executor',
                task_id=task.task_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return default_priority(task)

    def _execute_precomputed(
        self,
        task: ExecutorTask,
        recorder: EventRecorder | None,
        partition_id: int,
    ) -> ExecutorResult:
        """Synthesise an ExecutorResult from ``task.precomputed`` without
        running a subprocess.

        Does not acquire the semaphore or a slot — a precomputed task does
        no pool work so counting it against pool capacity would misreport
        utilisation. Records synthetic task_started/task_completed events
        with ``precomputed=true`` metadata so the timeline is coherent and
        operators can filter precomputed outcomes in the debug UI.
        """
        assert task.precomputed is not None
        pre = task.precomputed
        result = ExecutorResult(
            exit_code=pre.exit_code,
            stdout=pre.stdout,
            stderr=pre.stderr,
            duration_seconds=round(pre.duration_seconds, 3),
            task=task,
            pid=None,
        )
        tasks_precomputed.inc()
        if recorder:
            recorder.record_task_started(
                task,
                partition_id,
                pool_active=self._active_count,
                pool_waiting=self._waiting_count,
                slot=-1,  # sentinel: no slot used
                precomputed=True,
            )
            recorder.record_task_completed(
                result,
                partition_id,
                pool_active=self._active_count,
                pool_waiting=self._waiting_count,
                precomputed=True,
            )
        if result.exit_code != 0:
            # Same on_error semantics as a real subprocess failure — the
            # framework must not distinguish "real failure" from
            # "precomputed failure" to the handler.
            raise ExecutorTaskError(
                error=ExecutorError(
                    task=task,
                    kind='nonzero_exit',
                    exit_code=result.exit_code,
                    stderr=result.stderr,
                    pid=None,
                ),
                result=result,
            )
        return result

    def _resolve_binary(self, task: ExecutorTask) -> str:
        binary = task.binary_path or self._binary_path
        if not binary:
            msg = (
                'No binary_path configured: neither executor config nor '
                'ExecutorTask.binary_path provides a path to the executable.'
            )
            raise ExecutorTaskError(
                error=ExecutorError(task=task, kind='launch_failure', exception=msg),
                result=ExecutorResult(
                    exit_code=-1,
                    stdout='',
                    stderr=msg,
                    duration_seconds=0.0,
                    task=task,
                ),
            )
        return binary

    def _build_env(self, task: ExecutorTask) -> dict[str, str] | None:
        """Build merged environment for subprocess.

        Precedence: (filtered) parent env → ExecutorConfig.env → ExecutorTask.env.

        Parent-env inheritance is filtered through ``inherit_deny_patterns``
        to avoid leaking framework-internal config (DK_*) and common
        secrets (passwords, tokens, DSNs, keys) to the executor subprocess.

        Returns None (= inherit parent env verbatim) ONLY in the rare case
        of: ``inherit_parent_env`` is True, no deny patterns are configured,
        and no custom env is configured. Any filtering or custom env forces
        an explicit dict so the subprocess sees exactly what we intend.
        """
        has_custom_env = bool(self._config_env or task.env)
        has_deny = bool(self._inherit_deny_patterns)

        if self._inherit_parent_env and not has_custom_env and not has_deny:
            # Trivial case: pass through parent env with no transformation.
            return None

        merged: dict[str, str] = {}
        if self._inherit_parent_env:
            merged.update(self._filtered_parent_env())
        # Custom env always wins — operator/handler chose these explicitly.
        merged.update(self._config_env)
        merged.update(task.env)
        return merged

    def _filtered_parent_env(self) -> dict[str, str]:
        """The parent environment minus deny-listed names, computed once.

        The result depends only on ``os.environ`` and the deny patterns, and
        neither changes once the worker is running — but it used to be
        recomputed for every task, at roughly 80us of event-loop time each on
        a typical environment. That cost is unavoidable by configuration: the
        "inherit verbatim" fast path above needs an EMPTY deny list, and the
        default ships several patterns, so real deployments never reach it.

        Computed lazily on first use rather than in ``__init__`` so variables
        set by startup hooks are still picked up. The consequence is that
        mutating ``os.environ`` after the first task no longer affects later
        subprocesses — use ``executor.env`` or ``ExecutorTask.env`` for values
        that need to vary per task. The Go backend caches the same way.
        """
        if self._filtered_parent_env_cache is None:
            self._filtered_parent_env_cache = {
                key: val for key, val in os.environ.items() if not self._is_env_key_denied(key)
            }
        return self._filtered_parent_env_cache

    def _is_env_key_denied(self, key: str) -> bool:
        """Case-insensitive glob match against any deny pattern."""
        key_upper = key.upper()
        return any(fnmatch.fnmatchcase(key_upper, p.upper()) for p in self._denied_patterns_upper)

    async def _communicate_capped(
        self,
        proc: asyncio.subprocess.Process,
        stdin_bytes: bytes | None,
    ) -> tuple[bytes, int, bytes, int]:
        """``communicate()`` replacement that caps per-stream retention.

        Reads both pipes to EOF in chunks, retaining at most the configured
        cap per stream (0 = unlimited) and counting every byte. Reading
        NEVER stops at the cap: a reader that stopped would let the OS pipe
        buffer fill and block the child, turning a chatty process into a
        spurious timeout kill. Returns ``(stdout_raw, stdout_total,
        stderr_raw, stderr_total)`` — the caller derives the truncation
        flags and applies the UTF-8-safe trim.
        """

        async def feed_stdin(data: bytes) -> None:
            # Mirrors communicate()'s stdin handling: write, drain, close —
            # swallowing the two errors a child that exits before reading
            # all of its stdin produces.
            assert proc.stdin is not None
            try:
                proc.stdin.write(data)
                await proc.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                proc.stdin.close()

        async def read_capped(stream: asyncio.StreamReader, cap: int) -> tuple[bytes, int]:
            retained = bytearray()
            total = 0
            while chunk := await stream.read(_READ_CHUNK_BYTES):
                total += len(chunk)
                if cap == 0:
                    retained.extend(chunk)
                elif len(retained) < cap:
                    retained.extend(chunk[: cap - len(retained)])
            return bytes(retained), total

        assert proc.stdout is not None and proc.stderr is not None
        if proc.stdin is not None:
            (stdout_raw, stdout_total), (stderr_raw, stderr_total), _ = await asyncio.gather(
                read_capped(proc.stdout, self._max_stdout_bytes),
                read_capped(proc.stderr, self._max_stderr_bytes),
                feed_stdin(stdin_bytes or b''),
            )
        else:
            (stdout_raw, stdout_total), (stderr_raw, stderr_total) = await asyncio.gather(
                read_capped(proc.stdout, self._max_stdout_bytes),
                read_capped(proc.stderr, self._max_stderr_bytes),
            )
        await proc.wait()
        return stdout_raw, stdout_total, stderr_raw, stderr_total

    async def _run_subprocess(self, task: ExecutorTask) -> ExecutorResult:
        binary = self._resolve_binary(task)
        start = time.monotonic()
        proc = None
        subprocess_env = self._build_env(task)
        try:
            # Args passed as a list — no shell injection risk.
            # start_new_session=True on POSIX gives the child its own session and
            # process group. That lets the timeout path reap the whole descendant
            # tree via os.killpg (see _kill_process_tree). Without this, a binary
            # that spawns background grandchildren would leak them when the
            # parent is killed. Windows has no session concept; the kwarg is
            # simply omitted there (and _kill_process_tree falls back to
            # proc.kill).
            stdin_pipe = asyncio.subprocess.PIPE if task.stdin is not None else None
            # ``start_new_session`` only on POSIX — Windows has no session
            # concept. Use ``Any`` for the kwarg dict so the type checker
            # doesn't try to widen ``bool`` to ``int`` to match
            # ``create_subprocess_exec``'s positional-arg signature.
            platform_kwargs: dict[str, Any] = {'start_new_session': True} if _IS_POSIX else {}
            # Timed separately from the task body: fork/exec runs on the event
            # loop, so on a congested worker this measures the parent's own
            # contribution to task wall time — the number that tells "slow
            # child" apart from "slow orchestrator".
            spawn_started = time.monotonic()
            proc = await asyncio.create_subprocess_exec(
                binary,
                *task.args,
                stdin=stdin_pipe,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=subprocess_env,
                **platform_kwargs,
            )
            spawn_seconds = time.monotonic() - spawn_started
            stdin_bytes = task.stdin.encode() if task.stdin is not None else None
            if self._max_stdout_bytes == 0 and self._max_stderr_bytes == 0:
                # Unlimited capture: communicate() handles stdin write,
                # dual-pipe reads, and process wait in one call.
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    proc.communicate(input=stdin_bytes),
                    timeout=self._task_timeout,
                )
                stdout_truncated = stderr_truncated = False
            else:
                stdout_bytes, stdout_total, stderr_bytes, stderr_total = await asyncio.wait_for(
                    self._communicate_capped(proc, stdin_bytes),
                    timeout=self._task_timeout,
                )
                stdout_truncated = 0 < self._max_stdout_bytes < stdout_total
                stderr_truncated = 0 < self._max_stderr_bytes < stderr_total
                if stdout_truncated:
                    stdout_bytes = _trim_incomplete_utf8(stdout_bytes)
                if stderr_truncated:
                    stderr_bytes = _trim_incomplete_utf8(stderr_bytes)

                for stream_name, truncated, cap, total in (
                    ('stdout', stdout_truncated, self._max_stdout_bytes, stdout_total),
                    ('stderr', stderr_truncated, self._max_stderr_bytes, stderr_total),
                ):
                    if truncated:
                        output_truncated.labels(stream=stream_name).inc()
                        logger.warning(
                            'executor_output_truncated',
                            category='executor',
                            task_id=task.task_id,
                            stream=stream_name,
                            cap_bytes=cap,
                            total_bytes=total,
                        )
            duration = time.monotonic() - start

            pid = proc.pid
            # After `communicate()` returns normally the process has exited and
            # returncode is set. If it isn't (unexpected), treat the task as
            # failed rather than silently masking None as success with `or 0` —
            # that pattern previously let abnormal terminations look like exit
            # code 0 and advance offset commits past a broken task.
            exit_code = proc.returncode if proc.returncode is not None else -1
            result = ExecutorResult(
                exit_code=exit_code,
                stdout=stdout_bytes.decode(errors='replace') if stdout_bytes else '',
                stderr=stderr_bytes.decode(errors='replace') if stderr_bytes else '',
                duration_seconds=round(duration, 3),
                spawn_seconds=round(spawn_seconds, 4),
                task=task,
                pid=pid,
                stdout_truncated=stdout_truncated,
                stderr_truncated=stderr_truncated,
            )

            if result.exit_code != 0:
                raise ExecutorTaskError(
                    error=ExecutorError(
                        task=task,
                        kind='nonzero_exit',
                        exit_code=result.exit_code,
                        stderr=result.stderr,
                        pid=pid,
                    ),
                    result=result,
                )

            # No per-task log line here on purpose. At the throughput this
            # pool is built for it would fire ~1,200 times a second, and
            # structlog's async variants copy the context and hop through the
            # default thread pool BEFORE the level filter runs — so the cost
            # is paid even with debug logging off, and it competes with the
            # ``to_thread`` users (dbstats, archive, FileSink) for the same
            # executor. The flight recorder already records ``task_completed``
            # with more detail than this line carried.
            return result

        except TimeoutError:
            duration = time.monotonic() - start
            timeout_pid = proc.pid if proc else None
            raise ExecutorTaskError(  # noqa: B904
                error=ExecutorError(
                    task=task,
                    kind='timeout',
                    stderr='task timed out',
                    exception=f'Timeout after {self._task_timeout}s',
                    pid=timeout_pid,
                ),
                result=ExecutorResult(
                    exit_code=-1,
                    stdout='',
                    stderr='task timed out',
                    duration_seconds=round(duration, 3),
                    task=task,
                    pid=timeout_pid,
                ),
            )

        except OSError as e:
            duration = time.monotonic() - start
            raise ExecutorTaskError(  # noqa: B904
                error=ExecutorError(
                    task=task,
                    kind='launch_failure',
                    exception=str(e),
                ),
                result=ExecutorResult(
                    exit_code=-1,
                    stdout='',
                    stderr=str(e),
                    duration_seconds=round(duration, 3),
                    task=task,
                ),
            )

        finally:
            if proc and proc.returncode is None:
                await self._kill_process_tree(proc)

    @staticmethod
    async def _kill_process_tree(proc: asyncio.subprocess.Process) -> None:
        """Kill a subprocess and all of its descendants.

        On POSIX, the child was spawned with ``start_new_session=True`` so its
        PID is also its process-group ID. ``os.killpg`` then signals every
        process in that group — parent and all (grand)children — in one call.
        This is how we prevent the classic "I killed the shell but its
        backgrounded jobs are still running" leak on timeout.

        Race handling: the child may exit between ``getpgid`` and ``killpg``
        (or between the signal and ``wait``), in which case the kernel raises
        ``ProcessLookupError``. That is benign — the process is already gone,
        which is what we wanted — so we log at debug and continue.

        Windows has no ``killpg``; we fall back to the standard ``proc.kill``
        which terminates only the direct child. Grandchild-cleanup on Windows
        would need job objects and is out of scope.
        """
        if _IS_POSIX:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                # Child exited between our getpgid/killpg calls — that's fine.
                # Sync, not ``await logger.adebug``: the async variants hop
                # through the default thread pool before the level filter
                # runs, so they cost a thread round trip even when debug
                # logging is off.
                logger.debug(
                    'executor_killpg_race_noop',
                    category='executor',
                    pid=proc.pid,
                )
        else:
            # Windows path: single-process kill, no process group concept.
            proc.kill()
        # proc.wait() is safe to call even if the child is already gone — it
        # just returns the cached returncode. Bounded: a process in
        # uninterruptible sleep survives SIGKILL and would otherwise hang
        # shutdown forever.
        try:
            await asyncio.wait_for(proc.wait(), timeout=_KILL_REAP_TIMEOUT_SECONDS)
        except TimeoutError:
            logger.warning(
                'executor_kill_reap_timeout',
                category='executor',
                pid=proc.pid,
                timeout_seconds=_KILL_REAP_TIMEOUT_SECONDS,
                hint='process survived SIGKILL — likely blocked in uninterruptible I/O; '
                'it is left unreaped so shutdown can proceed',
            )


class ExecutorTaskError(Exception):
    """Raised when an executor task fails."""

    def __init__(self, error: ExecutorError, result: ExecutorResult) -> None:
        self.error = error
        self.result = result
        super().__init__(f'Task {error.task.task_id} failed: {error.stderr or error.exception}')
