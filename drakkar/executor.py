"""Subprocess executor pool for Drakkar framework."""

from __future__ import annotations

import asyncio
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
    """

    def __init__(self, binary_path: str, max_workers: int, task_timeout_seconds: int):
        self._binary_path = binary_path
        self._max_workers = max_workers
        self._task_timeout = task_timeout_seconds
        self._semaphore = asyncio.Semaphore(max_workers)
        self._active_count = 0

    @property
    def active_count(self) -> int:
        return self._active_count

    @property
    def max_workers(self) -> int:
        return self._max_workers

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
        async with self._semaphore:
            self._active_count += 1
            if recorder:
                recorder.record_task_started(task, partition_id)
            try:
                return await self._run_subprocess(task)
            finally:
                self._active_count -= 1

    async def _run_subprocess(self, task: ExecutorTask) -> ExecutorResult:
        start = time.monotonic()
        proc = None
        try:
            # create_subprocess_exec passes args as list — no shell injection risk
            proc = await asyncio.create_subprocess_exec(
                self._binary_path,
                *task.args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                proc.communicate(),
                timeout=self._task_timeout,
            )
            duration = time.monotonic() - start

            pid = proc.pid
            result = ExecutorResult(
                task_id=task.task_id,
                exit_code=proc.returncode or 0,
                stdout=stdout_bytes.decode(errors="replace") if stdout_bytes else "",
                stderr=stderr_bytes.decode(errors="replace") if stderr_bytes else "",
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
                "executor_task_completed",
                category="executor",
                task_id=task.task_id,
                duration=result.duration_seconds,
                exit_code=result.exit_code,
            )
            return result

        except asyncio.TimeoutError:
            duration = time.monotonic() - start
            timeout_pid = proc.pid if proc else None
            raise ExecutorTaskError(
                error=ExecutorError(
                    task=task,
                    stderr="task timed out",
                    exception=f"Timeout after {self._task_timeout}s",
                    pid=timeout_pid,
                ),
                result=ExecutorResult(
                    task_id=task.task_id,
                    exit_code=-1,
                    stdout="",
                    stderr="task timed out",
                    duration_seconds=round(duration, 3),
                    task=task,
                    pid=timeout_pid,
                ),
            )

        except OSError as e:
            duration = time.monotonic() - start
            raise ExecutorTaskError(
                error=ExecutorError(
                    task=task,
                    exception=str(e),
                ),
                result=ExecutorResult(
                    task_id=task.task_id,
                    exit_code=-1,
                    stdout="",
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

    def __init__(self, error: ExecutorError, result: ExecutorResult):
        self.error = error
        self.result = result
        super().__init__(f"Task {error.task.task_id} failed: {error.stderr or error.exception}")
