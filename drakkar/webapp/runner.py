"""Webapp request runner — executes one HTTP request through the Drakkar pipeline.

The runner lives on the main pipeline loop (T1). The webapp's FastAPI
route handler runs on the webapp loop (T2) and bridges to T1 via
:func:`drakkar.concurrency.dispatch_to_loop`. Once the dispatched
coroutine resumes here, it owns the pipeline-side work for that request:

1. synthesise a virtual ``SourceMessage`` (``partition=-1``, monotone
   per-runner ``offset``) so the rest of the pipeline doesn't need to
   know the request didn't come from Kafka;
2. invoke ``handler.arrange_http_request`` to translate the parsed
   ``HttpRequestT`` into a list of ``ExecutorTask`` records — each task
   gets stamped with ``origin='http'``, ``client_name``, and
   ``request_id`` before submission;
3. submit the tasks via the same ``ExecutorPool`` that drains Kafka
   work, gather results, build a synthetic ``MessageGroup``;
4. invoke ``handler.on_http_request_complete`` to produce the user's
   ``HttpResponseT``;
5. assemble a ``WebReport`` (the framework-level response envelope) and
   return it for the route handler to JSON-encode.

User-hook exceptions are caught and re-raised as
:class:`WebappHandlerError` so the route handler can map them to a
flat 500 body. The original traceback is captured on the exception so
the route handler / logger can record it without leaking it into the
response body.

Task 6a does NOT implement timeout, cancellation, the concurrency
semaphore, or the sinks-enabled path. Those land in Tasks 6b and 6c.
The signatures here are shaped to accept those features without
restructuring (e.g., ``ctx.cancelled`` is allocated up front so 6b can
add ``is_set()`` checks at the documented points).
"""

from __future__ import annotations

import asyncio
import time
import traceback
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.config import WebAppConfig
from drakkar.models import (
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    PendingContext,
    SourceMessage,
)
from drakkar.webapp.models import (
    CacheStats,
    StageTiming,
    TaskReport,
    TaskSummary,
    WebReport,
    WebRequestContext,
)

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()


class WebappHandlerError(Exception):
    """Raised when a user-supplied HTTP hook raises.

    Carries enough information for the route handler to log + record the
    failure server-side and emit a flat 500 body to the caller. The
    framework deliberately never includes the traceback in the response
    body — operators read full tracebacks via the recorder/debug UI.

    Attributes
    ----------
    where:
        Hook that raised — ``"arrange_http_request"`` or
        ``"on_http_request_complete"``. Used in logs and error reports
        so operators can pinpoint the failing override.
    original_exc:
        The exception object the hook raised. Held so the route handler
        can include the type/message in structured logs (still NOT in
        the response body).
    traceback_str:
        Pre-formatted traceback string captured at raise time. Stored
        because the route handler logs the failure asynchronously and
        the original ``__traceback__`` may have been cleared by then.
    """

    def __init__(
        self,
        where: str,
        original_exc: Exception,
        traceback_str: str,
    ) -> None:
        super().__init__(f'webapp handler hook {where!r} raised: {original_exc!r}')
        self.where = where
        self.original_exc = original_exc
        self.traceback_str = traceback_str


class WebappRunner:
    """Owns the per-request fan-out on the main pipeline loop.

    One ``WebappRunner`` instance per ``WebApp`` — the runner holds a
    ``_request_seq`` counter so synthetic ``SourceMessage.offset``
    values are monotone within the worker process. This mirrors how
    Kafka offsets are monotone within a partition; the synthetic
    ``partition=-1`` keeps HTTP-origin messages out of the regular
    partition trackers in the recorder/debug UI without needing a
    separate code path.

    The runner does not own any thread or loop — its ``run`` coroutine
    executes on whichever loop the caller awaits it on. The public
    contract is "always called via :func:`dispatch_to_loop` from the
    webapp loop", so in production ``run`` resumes on T1.
    """

    def __init__(self, drakkar_app: DrakkarApp, config: WebAppConfig) -> None:
        self._app = drakkar_app
        self._config = config
        # Monotone per-runner counter; increments before each request so
        # the first synthetic offset is 1, not 0. ``int`` not ``itertools``
        # because we want a plain field for tests to assert against.
        self._request_seq: int = 0

    async def run(self, ctx: WebRequestContext) -> WebReport:
        """Execute one HTTP request and return the assembled framework report.

        High-level flow (see module docstring for the rationale):

        1. allocate ``ctx.cancelled`` on this loop (so a Task 6b timeout
           on T2 binds the Event to T1, the loop that will await it);
        2. assign a synthetic offset and build the source message;
        3. call ``arrange_http_request`` on the user hook, stamp tasks;
        4. submit tasks via the executor pool and gather results;
        5. build a synthetic ``MessageGroup`` and call
           ``on_http_request_complete`` to produce the user response;
        6. assemble and return the ``WebReport``.
        """
        # ``cancelled`` is allocated on the loop that awaits it (T1).
        # Task 6b uses this to short-circuit late side effects when T2's
        # ``wait_for`` already 504'd; Task 6a leaves the field unused but
        # the allocation is cheap and keeps the contract uniform.
        if ctx.cancelled is None:
            ctx.cancelled = asyncio.Event()

        self._request_seq += 1
        offset = self._request_seq

        # Wall-clock start of the pipeline-side work — used for the
        # WebReport ``finished_at`` and ``duration_ms``. ``ctx.started_at``
        # remains as set by T2 (when the request landed on the webapp).
        timeline: list[StageTiming] = []
        run_started_monotonic = time.monotonic()

        # Synthetic SourceMessage. ``partition=-1`` keeps HTTP-origin
        # messages cleanly distinct from any real Kafka partition. The
        # value field requires bytes; we serialise the parsed Pydantic
        # request body rather than re-reading the raw POST body so the
        # bytes match what arrange_http_request sees in ``ctx.request``.
        request_body_bytes = self._serialize_request_body(ctx.request)
        # SourceMessage.timestamp expects milliseconds-since-epoch (Kafka
        # convention). Convert the UTC ``datetime`` from ctx to ms.
        ts_ms = int(ctx.started_at.timestamp() * 1000)
        source_message = SourceMessage(
            topic='__webapp__',
            partition=-1,
            offset=offset,
            key=ctx.client_name.encode('utf-8'),
            value=request_body_bytes,
            timestamp=ts_ms,
        )

        # ----- Stage: arrange_http_request -----
        arrange_started = time.monotonic()
        # Empty PendingContext for HTTP-origin requests — there are no
        # in-flight tasks to dedupe against on a per-request synthetic
        # group. (The Kafka path passes its partition's pending tasks
        # for cross-message dedup, which doesn't apply here.)
        pending_ctx = PendingContext(pending_tasks=[], pending_task_ids=set())

        try:
            tasks = await self._app._handler.arrange_http_request(ctx.request, pending_ctx)
        except Exception as exc:
            # Wrap and re-raise so the route handler can emit a flat 500
            # body. Capture the traceback string up front because
            # ``__traceback__`` may be cleared by the time the route
            # handler logs the failure.
            tb = traceback.format_exc()
            await logger.aerror(
                'webapp_arrange_http_request_failed',
                category='webapp',
                request_id=ctx.request_id,
                client=ctx.client_name,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise WebappHandlerError(
                where='arrange_http_request',
                original_exc=exc,
                traceback_str=tb,
            ) from exc

        # Stamp every task with the HTTP origin markers. The plan note
        # confirms ``ExecutorTask`` is a mutable Pydantic model (no
        # ``model_config['frozen']``), so direct assignment is the
        # simplest correct option. ``model_copy(update=...)`` would
        # produce a fresh object the user no longer holds a reference
        # to, breaking ``arrange_http_request`` overrides that stash
        # task references in ``self`` for tracking.
        for task in tasks:
            task.origin = 'http'
            task.client_name = ctx.client_name
            task.request_id = ctx.request_id

        timeline.append(StageTiming(stage='arrange', duration_ms=(time.monotonic() - arrange_started) * 1000.0))

        # ----- Stage: execute -----
        execute_started = time.monotonic()
        # The executor pool's ``execute`` coroutine is the same entry
        # point the partition processor uses (drakkar/partition.py:437).
        # We submit all tasks concurrently via ``asyncio.gather`` so
        # arrange_http_request can fan out without authoring its own
        # concurrency control. ``return_exceptions=True`` keeps a
        # single failing task from cancelling siblings — failure
        # bookkeeping (success/failed counts) is computed below.
        results = await self._submit_tasks(tasks)
        timeline.append(StageTiming(stage='execute', duration_ms=(time.monotonic() - execute_started) * 1000.0))

        # Split gathered outcomes into successes/failures for the
        # synthetic MessageGroup and the WebReport summary. The
        # executor pool raises ``ExecutorTaskError`` on a non-zero exit;
        # we treat any exception as a terminal failure for the request.
        successful_results: list[ExecutorResult] = []
        failed_count = 0
        for outcome in results:
            if isinstance(outcome, ExecutorResult):
                successful_results.append(outcome)
            else:
                failed_count += 1

        # ----- Stage: synthetic MessageGroup + on_http_request_complete -----
        # Per the task brief: pass origin/client_name/request_id
        # EXPLICITLY — the SourceMessage doesn't carry those fields.
        group = MessageGroup(
            source_message=source_message,
            tasks=list(tasks),
            results=successful_results,
            errors=[],  # Task 6a does not surface terminal failures into errors[]
            started_at=run_started_monotonic,
            finished_at=time.monotonic(),
            origin='http',
            client_name=ctx.client_name,
            request_id=ctx.request_id,
        )

        complete_started = time.monotonic()
        try:
            response = await self._app._handler.on_http_request_complete(group)
        except Exception as exc:
            tb = traceback.format_exc()
            await logger.aerror(
                'webapp_on_http_request_complete_failed',
                category='webapp',
                request_id=ctx.request_id,
                client=ctx.client_name,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise WebappHandlerError(
                where='on_http_request_complete',
                original_exc=exc,
                traceback_str=tb,
            ) from exc
        timeline.append(
            StageTiming(
                stage='on_http_request_complete',
                duration_ms=(time.monotonic() - complete_started) * 1000.0,
            )
        )

        # ----- Assemble WebReport -----
        finished_at = datetime.now(UTC)
        duration_ms = (finished_at - ctx.started_at).total_seconds() * 1000.0

        # ``WebReport.result`` is typed as ``Any | None`` so a Pydantic
        # response model can be embedded directly — FastAPI's JSON encoder
        # walks Pydantic objects via ``model_dump`` automatically when we
        # return the WebReport with ``model_dump(mode='json')``.
        report = WebReport(
            request_id=ctx.request_id,
            client=ctx.client_name,
            started_at=ctx.started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            status='ok',
            result=response,
            tasks=[self._task_report(r) for r in successful_results],
            task_summary=TaskSummary(
                total=len(tasks),
                success=len(successful_results),
                failed=failed_count,
            ),
            # Task 6a does not thread per-request cache stats through the
            # runner — the cache exposes only Prometheus counters today,
            # and there is no per-request observation hook. CacheStats
            # default zeros keep the response shape stable for clients;
            # Task 6c / Task 7 may revisit if a per-request hook lands.
            cache=CacheStats(),
            sinks=None,  # Task 6c fills this on the sinks_enabled path.
            timeline=timeline,
        )
        return report

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _serialize_request_body(request: Any) -> bytes:
        """Encode the parsed request body as bytes for ``SourceMessage.value``.

        ``SourceMessage.value`` requires ``bytes`` (it carries the raw
        Kafka payload in the regular path). We serialise via Pydantic's
        ``model_dump_json`` when the body is a ``BaseModel``; fall back
        to ``str(...).encode()`` for the rare test path that hands in a
        non-Pydantic object so synthetic-context tests don't have to
        wrap their fixtures in a model.
        """
        # Imported lazily to keep the module-level imports tight and to
        # avoid a hard dependency on Pydantic for tests that pass plain
        # bytes / dicts as the request body.
        from pydantic import BaseModel

        if isinstance(request, BaseModel):
            return request.model_dump_json().encode('utf-8')
        if isinstance(request, bytes | bytearray):
            return bytes(request)
        # Last-resort fallback. Tests can hand in plain strings / dicts
        # for the synthetic path; production always sees a BaseModel.
        return str(request).encode('utf-8')

    async def _submit_tasks(self, tasks: list[ExecutorTask]) -> list[Any]:
        """Run ``tasks`` through the executor pool and gather outcomes.

        Mirrors the partition processor's submission pattern but without
        the per-task ``on_task_complete`` hook (the webapp path
        aggregates via ``on_http_request_complete`` instead) and without
        the retry / replacement-chain machinery (a webapp request lives
        for one round-trip; on_error replacements live in 6b/6c if they
        ever land for the HTTP path).

        Uses ``return_exceptions=True`` so a single subprocess failure
        does not cancel sibling tasks — the resulting list is split into
        successes and failures by the caller.
        """
        if not tasks:
            return []
        if self._app._executor_pool is None:
            # Defensive — webapp.enabled=true requires a running pool.
            # Surfacing this as a clear runtime error beats an
            # AttributeError later in the partition path.
            raise RuntimeError(
                'executor pool is not initialised; webapp.run cannot submit tasks before AppLifecycle starts the pool'
            )
        pool = self._app._executor_pool
        # ``partition_id=-1`` matches the synthetic SourceMessage.partition
        # so recorder rows for HTTP-origin tasks all key on -1. The
        # recorder treats partition_id as opaque.
        coros = [pool.execute(task, self._app._recorder, partition_id=-1) for task in tasks]
        return await asyncio.gather(*coros, return_exceptions=True)

    @staticmethod
    def _task_report(result: ExecutorResult) -> TaskReport:
        """Build a compact ``TaskReport`` from an executor result.

        Excludes stdout/stderr by design — operators read those via the
        recorder/debug UI. The plan's "Response shape" section
        documents this rule explicitly.
        """
        return TaskReport(
            task_id=result.task.task_id,
            exit_code=result.exit_code,
            duration_ms=result.duration_seconds * 1000.0,
            retries=0,  # Retries land in Task 6b/6c on the on_error path.
        )
