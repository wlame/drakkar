"""Subprocess executor pool for Drakkar framework."""

from __future__ import annotations

import asyncio
import heapq
import os
import time
from typing import TYPE_CHECKING

import structlog

from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()


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
    ) -> None:
        self._binary_path = binary_path
        self._max_executors = max_executors
        self._task_timeout = task_timeout_seconds
        self._config_env = env or {}
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
        """
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
        """Build merged environment for subprocess: parent env + config env + task env.

        Returns None (inherit parent env) when no custom env vars are configured.
        Task env overrides config env on key conflict.
        """
        if not self._config_env and not task.env:
            return None
        merged = dict(os.environ)
        merged.update(self._config_env)
        merged.update(task.env)
        return merged

    async def _run_subprocess(self, task: ExecutorTask) -> ExecutorResult:
        binary = self._resolve_binary(task)
        start = time.monotonic()
        proc = None
        subprocess_env = self._build_env(task)
        try:
            # create_subprocess_exec passes args as list — no shell injection risk
            proc = await asyncio.create_subprocess_exec(
                binary,
                *task.args,
                stdin=asyncio.subprocess.PIPE if task.stdin is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=subprocess_env,
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
                proc.kill()
                await proc.wait()


class ExecutorTaskError(Exception):
    """Raised when an executor task fails."""

    def __init__(self, error: ExecutorError, result: ExecutorResult) -> None:
        self.error = error
        self.result = result
        super().__init__(f'Task {error.task.task_id} failed: {error.stderr or error.exception}')
