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

Cooperative cancellation wraps the user-facing side effects (sink
delivery and ``on_http_request_complete``). When the route handler on
T2 hits its
``request_timeout`` budget, it (a) sets ``ctx.cancelled`` via
``call_soon_threadsafe`` so the flag is set on T1's loop, and (b)
cancels the cross-thread ``concurrent.futures.Future`` so any awaits
on T1 that are still running raise ``CancelledError``. The runner
checks ``ctx.cancelled.is_set()`` at the two safe boundaries — after
``asyncio.gather`` returns and immediately before invoking
``on_http_request_complete`` — and raises ``CancelledError`` from
either point so the user-side response work is skipped.

v1 caveat: subprocesses already running on T1 cannot be SIGTERM'd —
they finish naturally and their results are simply discarded after
the cancellation gate trips. Documented as a known limitation; v2
may add SIGTERM-on-cancel for in-flight subprocesses.

The optional sinks-enabled path sits between the two cancellation
checks. When ``config.deliver_to_sinks`` is True the runner
calls ``handler.on_message_complete(group)`` and, if it returns a
``CollectResult``, delivers it in one ``SinkManager.deliver_all`` call
with the same declared policies and ``depends_on`` order as a Kafka
message result; a delivery that ends in the DLQ writes the raw request
body as the record. The cancellation flag is checked before each sink
group starts, so a late timeout stops the groups that have not started.
Per-type outcomes (attempted / delivered / dlq / errors) are captured
into a ``SinkDeliverySummary`` on the ``WebReport`` so operators can see
exactly what was delivered without re-correlating against sink-level
metrics.
"""

from __future__ import annotations

import asyncio
import time
import traceback
from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar.budget import BudgetAccount, output_bytes, text_bytes
from drakkar.config import WebAppConfig
from drakkar.executor import ExecutorTaskError
from drakkar.handler import task_run_seconds_hook
from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.metrics import webapp_dropped_after_timeout, webapp_inflight
from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    PendingContext,
    SourceMessage,
)
from drakkar.reported_run import RunSecondsHook, reported_run_seconds
from drakkar.sinks.manager import (
    DeliveryErrorCallback,
    DeliveryOutcome,
    GroupGate,
    MessageContext,
    SinkKey,
    sink_ref,
)
from drakkar.webapp.models import (
    CacheStats,
    SinkDeliverySummary,
    SinkResult,
    StageTiming,
    TaskReport,
    TaskSummary,
    WebReport,
    WebRequestContext,
)

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp
    from drakkar.executor import ExecutorPool

logger = structlog.get_logger()

# The payload fields of a ``CollectResult``, in the order the summary lists the types.
_PAYLOAD_FIELDS = ('kafka', 'postgres', 'mongo', 'http', 'redis', 'files')

# The summary error that says how a sink's delivery ended when it neither
# delivered nor reached the DLQ and no skip dropped it: it did not start,
# or it failed with no DLQ record written.
_OUTCOME_ERROR_TEXT = {
    DeliveryOutcome.NOT_STARTED: 'not started',
    DeliveryOutcome.FAILED: 'failed, no DLQ record',
}


def received_ms(ctx: WebRequestContext) -> int:
    """When the request was received, in ms since the epoch (the Kafka timestamp convention)."""
    return int(ctx.started_at.timestamp() * 1000)


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
        # The handler's optional run-time hook, or None when it keeps the
        # default (asked once: the answer is a property of the handler class).
        self._task_run_seconds = task_run_seconds_hook(drakkar_app._handler)

    async def run(self, ctx: WebRequestContext) -> WebReport:
        """Execute one HTTP request and return the assembled framework report.

        High-level flow (see module docstring for the rationale):

        1. allocate ``ctx.cancelled`` on this loop (so a timeout on T2
           binds the Event to T1, the loop that will await it);
        2. assign a synthetic offset and build the source message;
        3. call ``arrange_http_request`` on the user hook, stamp tasks;
        4. submit tasks via the executor pool and gather results;
        5. build a synthetic ``MessageGroup`` and call
           ``on_http_request_complete`` to produce the user response;
        6. assemble and return the ``WebReport``.
        """
        # ``cancelled`` is allocated on the loop that awaits it (T1).
        # The cancellation gates below use it to short-circuit late side
        # effects when T2's ``wait_for`` already 504'd.
        if ctx.cancelled is None:
            ctx.cancelled = asyncio.Event()

        # Inflight gauge — incremented on entry, decremented in the
        # ``finally`` below so error / cancellation paths still release
        # the count. Operators alert on a runaway gauge to spot bugs that
        # leak the increment (e.g., an early ``raise`` before this point).
        webapp_inflight.inc()
        # The request's share of the pending budget: its tasks while they
        # run, and the output of their results until the request ends. HTTP
        # requests never wait for room; they only count.
        budget = self._app._pending_budget
        account = BudgetAccount(budget) if budget is not None else None
        try:
            return await self._run_body(ctx, account)
        finally:
            if account is not None:
                account.close()
            webapp_inflight.dec()

    async def _run_body(self, ctx: WebRequestContext, account: BudgetAccount | None = None) -> WebReport:
        """The actual pipeline-side work; wrapped by ``run`` for inflight."""
        self._request_seq += 1
        offset = self._request_seq

        # Recorder: mark this request entering the runner. The recorder is
        # present when ``ui.enabled`` or ``recorder.enabled`` is on. Helper
        # is sync so we don't need to await; the recorder buffers the
        # event and the periodic flush loop persists it.
        recorder = self._app._recorder
        if recorder is not None:
            recorder.record_webapp_request_received(ctx)

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
        ts_ms = received_ms(ctx)
        source_message = SourceMessage(
            topic='__webapp__',
            partition=-1,
            offset=offset,
            key=ctx.client_name.encode('utf-8'),
            value=request_body_bytes,
            timestamp=ts_ms,
            # Kafka-path invariant parity: deserialize_message sets payload
            # to the parsed input model before hooks fire; the webapp path
            # does the same with the already-parsed request so
            # on_http_request_complete never needs to re-parse ``value``.
            payload=ctx.request,
        )

        # ----- Stage: arrange_http_request -----
        arrange_started = time.monotonic()
        # Empty PendingContext for HTTP-origin requests — there are no
        # in-flight tasks to dedupe against on a per-request synthetic
        # group. (The Kafka path passes its partition's pending tasks
        # for cross-message dedup, which doesn't apply here.)
        pending_ctx = PendingContext(pending_tasks=[], pending_task_ids=set())

        # ``partition=-1`` matches the synthetic SourceMessage above, so any
        # annotation emitted from this hook lands on the same virtual
        # partition its events do and stays out of the real partitions.
        hook_token = bind_hook_context(
            hook='arrange_http_request',
            partition=-1,
            offset=offset,
            offsets=(offset,),
        )
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
        finally:
            clear_hook_context(hook_token)

        # Stamp every task with the HTTP origin markers.
        # ``ExecutorTask`` is a mutable Pydantic model (no
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
        # single failing task from cancelling siblings — outcome
        # classification (successes / group errors) happens below.
        results = await self._submit_tasks(tasks, account)
        timeline.append(StageTiming(stage='execute', duration_ms=(time.monotonic() - execute_started) * 1000.0))

        # Classify gathered outcomes. Terminal task failures surface into
        # the group's errors as the SAME ExecutorError the pool built —
        # exactly what the Kafka path appends per terminal task
        # (partition.py) — so shared hook code behaves identically on both
        # paths. There is no retry machinery over HTTP: a failure is
        # terminal on its first attempt.
        successful_results: list[ExecutorResult] = []
        group_errors: list[ExecutorError] = []
        for outcome in results:
            if isinstance(outcome, ExecutorResult):
                successful_results.append(outcome)
            elif isinstance(outcome, ExecutorError):
                group_errors.append(outcome)
            elif isinstance(outcome, asyncio.CancelledError):
                # Shutdown/cancellation must never masquerade as a task
                # failure — abort the request, like the cancellation gates.
                raise outcome
            else:
                # Any other BaseException (KeyboardInterrupt, SystemExit):
                # never convert interpreter-level signals into task errors.
                raise outcome

        # ----- Cancellation gate (post-execute) -----
        # T2's ``wait_for`` may have timed out while we were inside the
        # executor pool. ``ctx.cancelled`` is set via
        # ``loop.call_soon_threadsafe`` so it lands on this loop (T1).
        # Bail out before building the synthetic ``MessageGroup`` and
        # invoking the user's response hook — those side effects are
        # wasted work once T2 has already returned a 504 to the caller.
        if ctx.cancelled is not None and ctx.cancelled.is_set():
            await logger.ainfo(
                'webapp_request_dropped_after_timeout',
                category='webapp',
                request_id=ctx.request_id,
                client=ctx.client_name,
                stage='post_execute',
            )
            webapp_dropped_after_timeout.labels(client=ctx.client_name).inc()
            if recorder is not None:
                recorder.record_webapp_request_dropped_after_timeout(ctx)
            raise asyncio.CancelledError('webapp request cancelled after task execution; T2 already 504d')

        # ----- Stage: synthetic MessageGroup + on_http_request_complete -----
        # Per the task brief: pass origin/client_name/request_id
        # EXPLICITLY — the SourceMessage doesn't carry those fields.
        group = MessageGroup(
            source_message=source_message,
            tasks=list(tasks),
            results=successful_results,
            errors=group_errors,
            started_at=run_started_monotonic,
            finished_at=time.monotonic(),
            origin='http',
            client_name=ctx.client_name,
            request_id=ctx.request_id,
        )

        # ----- Stage: optional sinks delivery -----
        # Only fires on the opt-in ``deliver_to_sinks=True`` path. Mirrors
        # the Kafka pipeline's behaviour: ``on_message_complete`` may
        # return ``None`` (no rollup payloads) — we treat that as "no
        # sinks-side work" and leave ``WebReport.sinks`` as ``None``.
        # When a CollectResult comes back, we deliver it through the
        # SinkManager and capture per-sink outcomes into the summary.
        sinks_summary: SinkDeliverySummary | None = None
        if self._config.deliver_to_sinks:
            sinks_started = time.monotonic()
            sinks_summary = await self._deliver_sinks(ctx=ctx, group=group)
            timeline.append(
                StageTiming(
                    stage='sinks',
                    duration_ms=(time.monotonic() - sinks_started) * 1000.0,
                )
            )

        # Second cancellation gate: late timeouts may land between the
        # post-execute gate above and the user's response hook. Skipping
        # ``on_http_request_complete`` keeps user-side response work off
        # the critical path once the caller has already received a 504.
        if ctx.cancelled is not None and ctx.cancelled.is_set():
            await logger.ainfo(
                'webapp_request_dropped_after_timeout',
                category='webapp',
                request_id=ctx.request_id,
                client=ctx.client_name,
                stage='pre_on_http_request_complete',
            )
            webapp_dropped_after_timeout.labels(client=ctx.client_name).inc()
            if recorder is not None:
                recorder.record_webapp_request_dropped_after_timeout(ctx)
            raise asyncio.CancelledError('webapp request cancelled before on_http_request_complete; T2 already 504d')

        complete_started = time.monotonic()
        complete_token = bind_hook_context(
            hook='on_http_request_complete',
            partition=-1,
            offset=offset,
            offsets=(offset,),
        )
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
        finally:
            clear_hook_context(complete_token)
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
                failed=len(group_errors),
            ),
            # Per-request cache stats are not threaded through the
            # runner — the cache exposes only Prometheus counters today,
            # and there is no per-request observation hook. CacheStats
            # default zeros keep the response shape stable for clients.
            cache=CacheStats(),
            # Populated by ``_deliver_sinks`` only when ``deliver_to_sinks``
            # is True AND ``on_message_complete`` returned a non-empty
            # CollectResult; ``None`` otherwise (consistent with the
            # documented response shape).
            sinks=sinks_summary,
            timeline=timeline,
        )
        # Recorder: persist the successful completion as a single row
        # alongside the per-task rows already produced by the executor
        # path. Operators querying ``event = 'webapp_request_completed'``
        # see one row per successful HTTP request without joining to
        # webapp_request_received. Status mirrors the route's outcome
        # label so the same value flows into both the recorder and
        # ``drakkar_webapp_requests_total``.
        if recorder is not None:
            recorder.record_webapp_request_completed(
                ctx,
                status='ok',
                duration_ms=duration_ms,
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

    async def _submit_tasks(self, tasks: list[ExecutorTask], account: BudgetAccount | None = None) -> list[Any]:
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
            # Defensive — sources.http.enabled=true requires a running pool.
            # Surfacing this as a clear runtime error beats an
            # AttributeError later in the partition path.
            raise RuntimeError(
                'executor pool is not initialised; webapp.run cannot submit tasks before AppLifecycle starts the pool'
            )
        pool = self._app._executor_pool
        # Per task object (``id``, not task_id): [runs not ended, stdin bytes
        # charged]. ``arrange_http_request`` may list one object twice, and its
        # stdin must outlive the first run.
        holds: dict[int, list[int]] = {}
        for task in tasks:
            holds.setdefault(id(task), [0, 0])[0] += 1
        coros = [self._execute_and_record(pool, task, holds, account) for task in tasks]
        return await asyncio.gather(*coros, return_exceptions=True)

    async def _execute_and_record(
        self,
        pool: ExecutorPool,
        task: ExecutorTask,
        holds: dict[int, list[int]],
        account: BudgetAccount | None = None,
    ) -> ExecutorResult | ExecutorError:
        """Run one task, record its terminal event as soon as it ends, and release its stdin.

        There is no retry over HTTP, but the request may list one task object
        more than once (``holds`` counts its runs that have not ended and the
        stdin they charged). At the last run of the object ``task.stdin`` is
        set to None and the stdin charged by all its runs is released, so no
        run starts without its input; ``on_http_request_complete`` sees the
        task without its stdin. The output of the outcome stays charged to
        ``account`` until the request ends.

        The pool records only ``task_started``; the terminal
        ``task_completed`` / ``task_failed`` row is the caller's job (the
        Kafka path writes it in the partition processor). Recording here,
        per task rather than after the gather, keeps each timeline bar
        ending when its own subprocess ended.

        Returns the result on success, or the ``ExecutorError`` for a
        terminal failure. Cancellation propagates unrecorded, as on the
        Kafka path.
        """
        recorder = self._app._recorder
        # A precomputed task never enters the pool; the pool records both
        # its events itself, so a second terminal row here would duplicate it.
        records_outcome = recorder is not None and task.precomputed is None
        # ``partition_id=-1`` matches the synthetic SourceMessage.partition
        # so recorder rows for HTTP-origin tasks all key on -1. The
        # recorder treats partition_id as opaque.
        held = text_bytes(task.stdin)
        hold = holds[id(task)]
        hold[1] += held
        if account is not None:
            account.charge(1, held)
        try:
            outcome = await self._execute_task(
                pool,
                task,
                recorder,
                records_outcome=records_outcome,
                run_seconds_hook=self._task_run_seconds,
            )
        finally:
            hold[0] -= 1
            if hold[0] > 0:
                if account is not None:
                    account.release(1, 0)
            else:
                task.stdin = None
                if account is not None:
                    account.release(1, hold[1])
        if account is not None:
            account.charge(0, output_bytes(outcome))
        return outcome

    @staticmethod
    async def _execute_task(
        pool: ExecutorPool,
        task: ExecutorTask,
        recorder: Any,
        *,
        records_outcome: bool,
        run_seconds_hook: RunSecondsHook | None = None,
    ) -> ExecutorResult | ExecutorError:
        """Run one task through the pool; write its terminal row when ``records_outcome``.

        ``run_seconds_hook`` is the handler's ``task_run_seconds`` override,
        asked for the row's ``reported_run_seconds``.
        """
        try:
            result = await pool.execute(task, recorder, partition_id=-1)
        except ExecutorTaskError as exc:
            if records_outcome:
                recorder.record_task_failed(
                    task,
                    exc.error,
                    -1,
                    pool_active=pool.active_count,
                    pool_waiting=pool.waiting_count,
                    duration_seconds=exc.result.duration_seconds,
                    result=exc.result,
                    reported_run_seconds=reported_run_seconds(run_seconds_hook, exc.result),
                )
            return exc.error
        except Exception as exc:
            # Unexpected (non-executor) failure: keep it visible in the
            # group as an internal error rather than reducing it to a count.
            error = ExecutorError(task=task, kind='internal', exception=f'{type(exc).__name__}: {exc}')
            if records_outcome:
                recorder.record_task_failed(
                    task,
                    error,
                    -1,
                    pool_active=pool.active_count,
                    pool_waiting=pool.waiting_count,
                )
            return error
        if records_outcome:
            recorder.record_task_completed(
                result,
                -1,
                pool_active=pool.active_count,
                pool_waiting=pool.waiting_count,
                reported_run_seconds=reported_run_seconds(run_seconds_hook, result),
            )
        return result

    @staticmethod
    def _task_report(result: ExecutorResult) -> TaskReport:
        """Build a compact ``TaskReport`` from an executor result.

        Excludes stdout/stderr by design — operators read those via the
        recorder/debug UI (see the response-shape reference in
        ``docs/webapp.md``).
        """
        return TaskReport(
            task_id=result.task.task_id,
            exit_code=result.exit_code,
            duration_ms=result.duration_seconds * 1000.0,
            retries=0,  # Retry tracking is not threaded through the webapp path.
        )

    # ------------------------------------------------------------------
    # Sinks integration
    # ------------------------------------------------------------------

    async def _deliver_sinks(
        self,
        ctx: WebRequestContext,
        group: MessageGroup,
    ) -> SinkDeliverySummary | None:
        """Run the optional ``on_message_complete`` → ``SinkManager`` sequence.

        Only invoked when ``config.deliver_to_sinks`` is True. Returns
        ``None`` when ``on_message_complete`` returns ``None`` (no
        rollup payloads — same convention as the Kafka pipeline) so the
        ``WebReport.sinks`` field stays ``null`` for that case.

        When a ``CollectResult`` comes back, every sink of it is delivered
        in one :meth:`SinkManager.deliver_all` call, as a Kafka message
        result is: each sink by its declared ``on_failure`` and
        ``max_retries``, groups in parallel unless ``depends_on`` orders
        them. The request is one source message whose DLQ record carries
        the raw request body; once a delivery writes it, the sinks that
        have not started do not start. The per-type
        :class:`SinkDeliverySummary` is built from the outcome of each
        sink.

        Validation:
            Each sink type is validated on its own; a type whose payloads
            name no configured sink is recorded in its summary and left
            out, and the other types are still delivered.

        Cancellation between groups:
            Before each group starts, ``ctx.cancelled`` is checked. If set,
            the runner logs ``webapp_request_dropped_after_timeout`` with
            ``stage='during_sinks'`` once and raises ``CancelledError``.
            Groups already in flight finish; nothing delivered is rolled
            back — sink deliveries are not transactional and the client
            already received a 504.

        ``on_message_complete`` raising:
            Wrapped into :class:`WebappHandlerError` so the route
            handler can map it to a flat 500 body, matching the
            ``arrange_http_request`` / ``on_http_request_complete``
            failure pattern.
        """
        # Wrap user-hook exceptions in WebappHandlerError so the route
        # handler can emit a flat 500 — same pattern as
        # arrange_http_request / on_http_request_complete.
        try:
            collect_result = await self._app._handler.on_message_complete(group)
        except Exception as exc:
            tb = traceback.format_exc()
            await logger.aerror(
                'webapp_on_message_complete_failed',
                category='webapp',
                request_id=ctx.request_id,
                client=ctx.client_name,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise WebappHandlerError(
                where='on_message_complete',
                original_exc=exc,
                traceback_str=tb,
            ) from exc

        # No payloads → no sink writes. Mirrors the Kafka path: a None
        # return from on_message_complete means "nothing to ship". We
        # leave the WebReport.sinks field at None so clients can
        # distinguish "sinks disabled" from "sinks ran with empty
        # output" by introspecting the request config separately.
        if collect_result is None or not collect_result.has_outputs:
            return None

        manager = self._app._sink_manager
        summary = SinkDeliverySummary()
        valid: list[CollectResult] = []
        # The summary type and the payload count of every sink of the request.
        summary_type_of: dict[SinkKey, str] = {}
        payload_count: Counter[SinkKey] = Counter()
        for summary_type, sub_result in self._split_collect_by_type(collect_result):
            sink_result = SinkResult(attempted=self._count_payloads(sub_result))
            summary.by_type[summary_type] = sink_result
            try:
                manager.validate_collect(sub_result)
            except Exception as exc:
                # Recorded, not raised: the response can still be built and
                # return 200; the summary explains the gap.
                sink_result.errors.append(f'validation_error: {exc!r}')
                await logger.awarning(
                    'webapp_sink_validation_failed',
                    category='webapp',
                    request_id=ctx.request_id,
                    client=ctx.client_name,
                    sink_type=summary_type,
                    error=str(exc),
                )
                continue
            valid.append(sub_result)
            for key in self._sink_keys(summary_type, sub_result):
                summary_type_of[key] = summary_type
                payload_count[key] += 1
        if not valid:
            return summary

        try:
            outcomes = await manager.deliver_all(
                self._merge(valid),
                on_delivery_error=self._make_on_delivery_error_capturer(summary, summary_type_of),
                partition_id=-1,
                message_context=MessageContext.for_request(
                    body=ctx.raw_body,
                    request_id=ctx.request_id,
                    received_ms=received_ms(ctx),
                ),
                before_group_start=self._make_cancellation_gate(ctx),
            )
        except Exception as exc:
            # A raise (a DLQ record that could not be written under
            # ``dlq.on_send_failure: stall``, or a manager bug) carries no
            # outcomes. The request is not failed for it: the response was
            # built, and a 200 with sink errors tells the client more than
            # a 500.
            for summary_type in set(summary_type_of.values()):
                summary.by_type[summary_type].errors.append(f'deliver_all_raised: {exc!r}')
            await logger.aerror(
                'webapp_sink_deliver_all_failed',
                category='webapp',
                request_id=ctx.request_id,
                client=ctx.client_name,
                error=str(exc),
            )
            return summary

        for key, outcome in outcomes.items():
            sink_result = summary.by_type[summary_type_of[key]]
            if outcome == DeliveryOutcome.DELIVERED:
                sink_result.delivered += payload_count[key]
            elif outcome == DeliveryOutcome.DLQ:
                sink_result.dlq += payload_count[key]
            elif outcome in _OUTCOME_ERROR_TEXT:
                # The hook entries name failed attempts, not how the delivery ended.
                sink_result.errors.append(f'{sink_ref(key)}: {_OUTCOME_ERROR_TEXT[outcome]}')
        return summary

    def _sink_keys(self, summary_type: str, sub_result: CollectResult) -> list[SinkKey]:
        """The sink key of each payload of a validated one-type sub-result, in payload order."""
        manager = self._app._sink_manager
        keys: list[SinkKey] = []
        for field in _PAYLOAD_FIELDS:
            for payload in getattr(sub_result, field):
                sink = manager.resolve_sink(summary_type, payload.sink)
                keys.append((sink.sink_type, sink.name))
        return keys

    def _make_cancellation_gate(self, ctx: WebRequestContext) -> GroupGate:
        """Build the check that stops the delivery between groups once T2 gave up on the request.

        Groups start in parallel, so several can reach the check at once;
        the drop is logged, counted and recorded once per request.
        """
        app = self._app
        dropped = False

        async def _gate(key: SinkKey) -> None:
            nonlocal dropped
            if ctx.cancelled is None or not ctx.cancelled.is_set():
                return
            if not dropped:
                dropped = True
                await logger.ainfo(
                    'webapp_request_dropped_after_timeout',
                    category='webapp',
                    request_id=ctx.request_id,
                    client=ctx.client_name,
                    stage='during_sinks',
                    next_sink_type=key[0],
                    next_sink=sink_ref(key),
                )
                webapp_dropped_after_timeout.labels(client=ctx.client_name).inc()
                if app._recorder is not None:
                    app._recorder.record_webapp_request_dropped_after_timeout(ctx)
            raise asyncio.CancelledError(
                f'webapp request cancelled before {sink_ref(key)!r} sink delivery; T2 already 504d'
            )

        return _gate

    @staticmethod
    def _split_collect_by_type(result: CollectResult) -> list[tuple[str, CollectResult]]:
        """Partition a ``CollectResult`` into per-sink-type sub-results.

        Returns a list of ``(sink_type, sub_result)`` tuples. The list
        order is deterministic — built-in sink types in the documented
        order (kafka, postgres, mongo, http, redis, files). Empty fields are omitted so the
        caller iterates only over types that actually carry payloads.

        Each sub-result is a fresh ``CollectResult`` containing payloads
        for exactly one type, so each type is validated and counted on
        its own before the valid ones are merged into one delivery.
        """
        groups: list[tuple[str, CollectResult]] = []
        if result.kafka:
            groups.append(('kafka', CollectResult(kafka=list(result.kafka))))
        if result.postgres:
            groups.append(('postgres', CollectResult(postgres=list(result.postgres))))
        if result.mongo:
            groups.append(('mongo', CollectResult(mongo=list(result.mongo))))
        if result.http:
            groups.append(('http', CollectResult(http=list(result.http))))
        if result.redis:
            groups.append(('redis', CollectResult(redis=list(result.redis))))
        if result.files:
            groups.append(('filesystem', CollectResult(files=list(result.files))))
        return groups

    @staticmethod
    def _count_payloads(result: CollectResult) -> int:
        """Total payload count across every sink-typed field of ``result``."""
        return len(WebappRunner._payloads(result))

    @staticmethod
    def _payloads(result: CollectResult) -> list[BaseModel]:
        """Every payload of ``result``, field by field, each field in payload order."""
        return [payload for field in _PAYLOAD_FIELDS for payload in getattr(result, field)]

    @staticmethod
    def _merge(results: list[CollectResult]) -> CollectResult:
        """One ``CollectResult`` holding the payloads of ``results``, each field in payload order."""
        return CollectResult(
            **{
                field: [payload for result in results for payload in getattr(result, field)]
                for field in _PAYLOAD_FIELDS
            }
        )

    def _make_on_delivery_error_capturer(
        self,
        summary: SinkDeliverySummary,
        summary_type_of: dict[SinkKey, str],
    ) -> DeliveryErrorCallback:
        """Build the ``on_delivery_error`` callback that also lists each failed attempt in the summary.

        It appends ``<type>/<name>: <error>`` to the errors of the sink's
        summary type (the error text is already redacted by the
        SinkManager), then asks the handler's ``on_delivery_error``. The
        SinkManager acts on the answer: it retries, skips, or writes the
        DLQ record; a hook that raises is logged there and the sink's
        declared ``on_failure`` applies.
        """
        app = self._app

        async def _capturing_on_delivery_error(error: DeliveryError) -> DeliveryAction:
            summary_type = summary_type_of[(error.sink_type, error.sink_name)]
            summary.by_type[summary_type].errors.append(f'{error.sink_type}/{error.sink_name}: {error.error}')
            bind_contextvars(hook='on_delivery_error', sink_type=error.sink_type, sink_name=error.sink_name)
            try:
                return await app._handler.on_delivery_error(error)
            finally:
                unbind_contextvars('hook', 'sink_type', 'sink_name')

        return _capturing_on_delivery_error
