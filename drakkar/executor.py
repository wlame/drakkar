"""Subprocess executor pool for Drakkar framework."""

from __future__ import annotations

import asyncio
import fnmatch
import heapq
import os
import signal
import sys
import time
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.metrics import tasks_precomputed
from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

# POSIX systems let us spawn a child in its own session/process group so we
# can signal the entire descendant tree on timeout (see ``_kill_process_tree``
# below). Windows has no ``os.killpg`` / session semantics — fall back to the
# parent-only kill on that platform.
_IS_POSIX = sys.platform != 'win32'


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
    ) -> None:
        self._binary_path = binary_path
        self._max_executors = max_executors
        self._task_timeout = task_timeout_seconds
        self._config_env = env or {}
        self._inherit_parent_env = inherit_parent_env
        # Patterns are compared case-insensitively against env var names.
        self._inherit_deny_patterns = list(inherit_deny_patterns or [])
        self._semaphore = asyncio.Semaphore(max_executors)
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

        self._waiting_count += 1
        waiting_decremented = False
        try:
            # `async with` would also work, but we need to guarantee
            # waiting_count rollback when cancellation fires before acquire.
            await self._semaphore.acquire()
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
                        )
                    return await self._run_subprocess(task)
                finally:
                    heapq.heappush(self._available_slots, slot)
                    self._active_count -= 1
            finally:
                self._semaphore.release()
        finally:
            if not waiting_decremented:
                # Cancelled before (or while) acquiring — the increment above
                # was never paired with the inner decrement. Rollback now.
                self._waiting_count -= 1

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
                error=ExecutorError(task=task, exception=msg),
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
        to avoid leaking framework-internal config (DRAKKAR_*) and common
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
            for key, val in os.environ.items():
                if not self._is_env_key_denied(key):
                    merged[key] = val
        # Custom env always wins — operator/handler chose these explicitly.
        merged.update(self._config_env)
        merged.update(task.env)
        return merged

    def _is_env_key_denied(self, key: str) -> bool:
        """Case-insensitive glob match against any deny pattern."""
        key_upper = key.upper()
        return any(fnmatch.fnmatchcase(key_upper, p.upper()) for p in self._inherit_deny_patterns)

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
            proc = await asyncio.create_subprocess_exec(
                binary,
                *task.args,
                stdin=stdin_pipe,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=subprocess_env,
                **platform_kwargs,
            )
            stdin_bytes = task.stdin.encode() if task.stdin is not None else None
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(input=stdin_bytes),
                timeout=self._task_timeout,
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
                task=task,
                pid=pid,
            )

            if result.exit_code != 0:
                raise ExecutorTaskError(
                    error=ExecutorError(
                        task=task,
                        exit_code=result.exit_code,
                        stderr=result.stderr,
                        pid=pid,
                    ),
                    result=result,
                )

            await logger.adebug(
                'executor_task_completed',
                category='executor',
                task_id=task.task_id,
                duration=result.duration_seconds,
                exit_code=result.exit_code,
            )
            return result

        except TimeoutError:
            duration = time.monotonic() - start
            timeout_pid = proc.pid if proc else None
            raise ExecutorTaskError(  # noqa: B904
                error=ExecutorError(
                    task=task,
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
                await logger.adebug(
                    'executor_killpg_race_noop',
                    category='executor',
                    pid=proc.pid,
                )
        else:
            # Windows path: single-process kill, no process group concept.
            proc.kill()
        # proc.wait() is safe to call even if the child is already gone — it
        # just returns the cached returncode. Always await so we don't leave
        # a zombie or a dangling Transport.
        await proc.wait()


class ExecutorTaskError(Exception):
    """Raised when an executor task fails."""

    def __init__(self, error: ExecutorError, result: ExecutorResult) -> None:
        self.error = error
        self.result = result
        super().__init__(f'Task {error.task.task_id} failed: {error.stderr or error.exception}')
