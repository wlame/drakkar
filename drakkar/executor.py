"""Subprocess executor pool for Drakkar framework."""

from __future__ import annotations

import asyncio
import heapq
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

    def __init__(self, binary_path: str | None, max_executors: int, task_timeout_seconds: int) -> None:
        self._binary_path = binary_path
        self._max_executors = max_executors
        self._task_timeout = task_timeout_seconds
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
        """
        self._waiting_count += 1
        async with self._semaphore:
            self._waiting_count -= 1
            self._active_count += 1
            slot = heapq.heappop(self._available_slots)
            if recorder:
                recorder.record_task_started(
                    task,
                    partition_id,
                    pool_active=self._active_count,
                    pool_waiting=self._waiting_count,
                    slot=slot,
                )
            try:
                return await self._run_subprocess(task)
            finally:
                heapq.heappush(self._available_slots, slot)
                self._active_count -= 1

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

    async def _run_subprocess(self, task: ExecutorTask) -> ExecutorResult:
        binary = self._resolve_binary(task)
        start = time.monotonic()
        proc = None
        try:
            # create_subprocess_exec passes args as list — no shell injection risk
            proc = await asyncio.create_subprocess_exec(
                binary,
                *task.args,
                stdin=asyncio.subprocess.PIPE if task.stdin is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdin_bytes = task.stdin.encode() if task.stdin is not None else None
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(input=stdin_bytes),
                timeout=self._task_timeout,
            )
            duration = time.monotonic() - start

            pid = proc.pid
            result = ExecutorResult(
                exit_code=proc.returncode or 0,
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
