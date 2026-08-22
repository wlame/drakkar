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
``request_timeout_seconds`` budget, it (a) sets ``ctx.cancelled`` via
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
checks. When ``config.sinks_enabled`` is True the runner
calls ``handler.on_message_complete(group)`` and, if it returns a
``CollectResult``, delivers each sink-type batch sequentially through
``SinkManager.deliver_all`` so the cancellation flag can short-circuit
the next batch on a late timeout. Per-sink outcomes (attempted /
delivered / dlq / errors) are captured into a ``SinkDeliverySummary``
on the ``WebReport`` so operators can see exactly what was delivered
without re-correlating against sink-level metrics.
"""

from __future__ import annotations

import asyncio
import time
import traceback
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.config import WebAppConfig
from drakkar.executor import ExecutorTaskError
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

logger = structlog.get_logger()

# Type alias for the SinkManager's on_delivery_error callback. Mirrors
# :data:`drakkar.sinks.manager.DeliveryErrorCallback` but is duplicated
# here so the runner module does not depend on a private re-export from
# the sink manager. The signature is fixed by ``SinkManager.deliver_all``:
# an async callable taking a ``DeliveryError`` and returning a
# ``DeliveryAction``.
OnDeliveryErrorCallback = Callable[[DeliveryError], Awaitable[DeliveryAction]]


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
        try:
            return await self._run_body(ctx)
        finally:
            webapp_inflight.dec()

    async def _run_body(self, ctx: WebRequestContext) -> WebReport:
        """The actual pipeline-side work; wrapped by ``run`` for inflight."""
        self._request_seq += 1
        offset = self._request_seq

        # Recorder: mark this request entering the runner. Opt-in —
        # recorder is only present when ``debug.enabled=true``. Helper
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
        results = await self._submit_tasks(tasks)
        timeline.append(StageTiming(stage='execute', duration_ms=(time.monotonic() - execute_started) * 1000.0))

        # Classify gathered outcomes. Terminal task failures surface into
        # the group's errors as the SAME ExecutorError the pool built —
        # exactly what the Kafka path appends per terminal task
        # (partition.py) — so shared hook code behaves identically on both
        # paths. There is no retry machinery over HTTP: a failure is
        # terminal on its first attempt.
        successful_results: list[ExecutorResult] = []
        group_errors: list[ExecutorError] = []
        for task, outcome in zip(tasks, results, strict=True):
            if isinstance(outcome, ExecutorResult):
                successful_results.append(outcome)
            elif isinstance(outcome, ExecutorTaskError):
                group_errors.append(outcome.error)
            elif isinstance(outcome, asyncio.CancelledError):
                # Shutdown/cancellation must never masquerade as a task
                # failure — abort the request, like the cancellation gates.
                raise outcome
            elif isinstance(outcome, Exception):
                # Unexpected (non-executor) failure: keep it visible in the
                # group rather than reducing it to a bare count.
                group_errors.append(
                    ExecutorError(task=task, kind='internal', exception=f'{type(outcome).__name__}: {outcome}')
                )
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
        # Only fires on the opt-in ``sinks_enabled=True`` path. Mirrors
        # the Kafka pipeline's behaviour: ``on_message_complete`` may
        # return ``None`` (no rollup payloads) — we treat that as "no
        # sinks-side work" and leave ``WebReport.sinks`` as ``None``.
        # When a CollectResult comes back, we deliver it through the
        # SinkManager and capture per-sink outcomes into the summary.
        sinks_summary: SinkDeliverySummary | None = None
        if self._config.sinks_enabled:
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
            # Populated by ``_deliver_sinks`` only when ``sinks_enabled``
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

        Only invoked when ``config.sinks_enabled`` is True. Returns
        ``None`` when ``on_message_complete`` returns ``None`` (no
        rollup payloads — same convention as the Kafka pipeline) so the
        ``WebReport.sinks`` field stays ``null`` for that case.

        When a ``CollectResult`` comes back, the runner splits it by
        sink type (``kafka`` / ``postgres`` / ``mongo`` / ``http`` /
        ``redis`` / ``filesystem`` / plugin sinks via ``custom``) and
        delivers each batch sequentially through
        :meth:`SinkManager.deliver_all`. Sequential dispatch (rather than
        parallel) is the divergence from the Kafka path: it lets us
        check ``ctx.cancelled`` between sink-type batches so a late T2
        timeout can short-circuit the rest of the delivery without
        committing wasted work to downstreams.

        Per-sink delivery outcomes are captured into a
        :class:`SinkDeliverySummary` via a wrapped ``on_delivery_error``
        callback that mirrors :meth:`DrakkarApp._handle_collect`'s DLQ
        routing — the user's ``handler.on_delivery_error`` decides the
        action (DLQ / RETRY / SKIP); when DLQ wins and a DLQ sink is
        configured, the failed payloads are forwarded there. Either
        way, the failure is recorded in the response summary so the
        client sees what happened.

        Cancellation between batches:
            ``ctx.cancelled.is_set()`` is checked before each sink-type
            batch. If set, the runner logs
            ``webapp_request_dropped_after_timeout`` with
            ``stage='during_sinks'`` and raises ``CancelledError``.
            Already-delivered batches are not rolled back — sink
            deliveries are not transactional and the user has already
            received a 504 client-side.

        ``on_message_complete`` raising:
            Wrapped into :class:`WebappHandlerError` so the route
            handler can map it to a flat 500 body, matching the
            ``arrange_http_request`` / ``on_http_request_complete``
            failure pattern.
        """
        # Step 1: invoke on_message_complete to gather sink payloads.
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

        # Step 2: split CollectResult by sink type so we can dispatch
        # batches sequentially with cancellation checks between them.
        # ``custom`` payloads are grouped under the synthetic key
        # ``'custom'`` because plugin sinks span arbitrary sink types
        # whose names are runtime-resolved per payload — splitting them
        # further would require resolving each plugin sink upfront.
        per_type: list[tuple[str, CollectResult]] = self._split_collect_by_type(collect_result)

        # Step 3: build the per-sink summary scaffold. Initialise each
        # sink type with attempted=N (count of payloads) and
        # delivered=N — the wrapped on_delivery_error decrements
        # delivered and increments dlq when a batch fails.
        summary = SinkDeliverySummary()
        for sink_type, sub_result in per_type:
            attempted = self._count_payloads(sub_result)
            summary.by_type[sink_type] = SinkResult(
                attempted=attempted,
                delivered=attempted,
                dlq=0,
                errors=[],
            )

        # Step 4: dispatch each sink-type batch sequentially.
        for sink_type, sub_result in per_type:
            # Late-cancellation gate. T2 may have timed out while we
            # were inside an earlier sink's deliver call; bail out
            # before the next batch so we don't burn another
            # round-trip on a request the client has already given up
            # on.
            if ctx.cancelled is not None and ctx.cancelled.is_set():
                await logger.ainfo(
                    'webapp_request_dropped_after_timeout',
                    category='webapp',
                    request_id=ctx.request_id,
                    client=ctx.client_name,
                    stage='during_sinks',
                    next_sink_type=sink_type,
                )
                webapp_dropped_after_timeout.labels(client=ctx.client_name).inc()
                if self._app._recorder is not None:
                    self._app._recorder.record_webapp_request_dropped_after_timeout(ctx)
                raise asyncio.CancelledError(
                    f'webapp request cancelled before {sink_type!r} sink delivery; T2 already 504d'
                )

            # Validate the sub-result against the SinkManager (mirrors
            # DrakkarApp._handle_collect). Misconfigured sinks should
            # surface as a clear server-side error; we don't fail the
            # whole request, we just record the validation failure in
            # the per-sink summary and skip delivery for that type.
            sink_result = summary.by_type[sink_type]
            try:
                self._app._sink_manager.validate_collect(sub_result)
            except Exception as exc:
                # Validation failures are recorded but do not raise:
                # the user-side response was built and the request can
                # still return 200; the sinks summary explains the gap.
                sink_result.delivered = 0
                sink_result.errors.append(f'validation_error: {exc!r}')
                await logger.awarning(
                    'webapp_sink_validation_failed',
                    category='webapp',
                    request_id=ctx.request_id,
                    client=ctx.client_name,
                    sink_type=sink_type,
                    error=str(exc),
                )
                continue

            # Wrapped on_delivery_error mirrors DrakkarApp._handle_collect:
            # delegates to the user hook, then routes DLQ-action results
            # to the configured DLQ sink (when present). The wrapper
            # additionally captures per-sink outcomes into the summary
            # so the response body reflects what actually happened.
            on_delivery_error = self._make_on_delivery_error_capturer(
                sink_result=sink_result,
            )

            try:
                await self._app._sink_manager.deliver_all(
                    sub_result,
                    on_delivery_error=on_delivery_error,
                    partition_id=-1,
                )
            except Exception as exc:
                # Catastrophic failure inside SinkManager (rare —
                # SinkManager normally routes failures through
                # on_delivery_error). We record it in the summary but
                # don't fail the request: the user's HTTP response was
                # already built, and a 200-with-sink-errors gives the
                # client more useful information than a 500.
                sink_result.delivered = 0
                sink_result.errors.append(f'deliver_all_raised: {exc!r}')
                await logger.aerror(
                    'webapp_sink_deliver_all_failed',
                    category='webapp',
                    request_id=ctx.request_id,
                    client=ctx.client_name,
                    sink_type=sink_type,
                    error=str(exc),
                )

        return summary

    @staticmethod
    def _split_collect_by_type(result: CollectResult) -> list[tuple[str, CollectResult]]:
        """Partition a ``CollectResult`` into per-sink-type sub-results.

        Returns a list of ``(sink_type, sub_result)`` tuples. The list
        order is deterministic — built-in sink types in the documented
        order (kafka, postgres, mongo, http, redis, files), then
        ``custom`` for plugin sinks. Empty fields are omitted so the
        caller iterates only over types that actually carry payloads.

        Each sub-result is a fresh ``CollectResult`` containing payloads
        for exactly one type — that's what lets ``deliver_all`` run on
        a single batch at a time with cancellation checks between.
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
        if result.custom:
            # Plugin sinks all share the synthetic ``'custom'`` key in
            # the response summary because each plugin sink can have
            # its own ``sink_type`` (resolved at runtime per payload).
            # Operators read finer-grained per-sink stats from the
            # SinkManager's existing ``get_all_stats`` API.
            groups.append(('custom', CollectResult(custom=list(result.custom))))
        return groups

    @staticmethod
    def _count_payloads(result: CollectResult) -> int:
        """Total payload count across every sink-typed field of ``result``."""
        return (
            len(result.kafka)
            + len(result.postgres)
            + len(result.mongo)
            + len(result.http)
            + len(result.redis)
            + len(result.files)
            + len(result.custom)
        )

    def _make_on_delivery_error_capturer(
        self,
        sink_result: SinkResult,
    ) -> OnDeliveryErrorCallback:
        """Build a wrapped ``on_delivery_error`` that captures into ``sink_result``.

        The wrapper:
          1. delegates to the user's ``handler.on_delivery_error`` to
             learn the desired :class:`DeliveryAction` (DLQ / RETRY /
             SKIP);
          2. appends a redacted error message to ``sink_result.errors``
             so the response summary surfaces what went wrong;
          3. when DLQ wins and a DLQ sink is configured (mirrors
             :meth:`DrakkarApp._handle_collect`), forwards the failed
             payloads to the DLQ and increments ``sink_result.dlq``;
          4. decrements ``sink_result.delivered`` by the failed payload
             count so ``attempted = delivered + dlq + skipped``
             relationship holds at the end (modulo SKIP; see below).

        SKIP semantics: SKIP drops payloads silently from the
        downstream, so we count them as not-delivered (decrement
        ``delivered``) but do NOT add to ``dlq``. They show up implicitly
        in ``attempted - delivered - dlq``. RETRY exhausting its budget
        eventually flows back through here as a final DLQ/SKIP, so the
        summary remains consistent.
        """
        app = self._app

        async def _capturing_on_delivery_error(error: DeliveryError) -> DeliveryAction:
            # Append a compact error string. The raw error message is
            # already redacted by the SinkManager (see
            # :func:`drakkar.utils.redact_url`) so we forward it
            # directly. We prepend the sink type so the operator can
            # tell at a glance which downstream blew up.
            sink_result.errors.append(f'{error.sink_type}/{error.sink_name}: {error.error}')

            # Delegate to the user hook to learn the action. ``app`` is
            # captured by closure so tests can swap it out without
            # reaching into the runner.
            try:
                action = await app._handler.on_delivery_error(error)
            except Exception as exc:
                # User hook raising is itself an error — log it and
                # default to DLQ (the safest fallback). Mirrors the
                # SinkManager's existing tolerance for handler bugs.
                await logger.aerror(
                    'webapp_on_delivery_error_failed',
                    category='webapp',
                    sink_type=error.sink_type,
                    sink_name=error.sink_name,
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                action = DeliveryAction.DLQ

            failed_count = len(error.payloads)

            if action == DeliveryAction.DLQ:
                # Match DrakkarApp._handle_collect: the DLQ send happens
                # only when a DLQ sink is configured. Without it, the
                # SinkManager logs ``sink_delivery_failed_to_dlq`` and
                # the payloads are effectively dropped — same behaviour
                # the Kafka path exhibits in pre-DLQ deployments.
                if app._dlq_sink is not None:
                    try:
                        sent = await app._dlq_sink.send(error, partition_id=-1)
                    except Exception as exc:
                        # DLQ send raising is rare but should not crash
                        # the request — record and continue.
                        sent = False
                        sink_result.errors.append(f'dlq_send_failed: {exc!r}')
                        await logger.aerror(
                            'webapp_sink_dlq_send_failed',
                            category='webapp',
                            sink_type=error.sink_type,
                            error=str(exc),
                        )
                    if not sent and not any(e.startswith('dlq_send_failed') for e in sink_result.errors):
                        # send() reported failure without raising — surface
                        # it in the response so the HTTP caller knows the
                        # payloads were not persisted anywhere. The webapp
                        # path has no offsets to stall; the request-level
                        # error report IS the recovery signal.
                        sink_result.errors.append('dlq_send_failed: DLQ write not confirmed')
                sink_result.dlq += failed_count
                sink_result.delivered = max(0, sink_result.delivered - failed_count)
            elif action == DeliveryAction.SKIP:
                # SKIP drops the payloads from the downstream entirely.
                # We decrement ``delivered`` so the response shape
                # matches reality but do NOT count toward DLQ — that
                # column is reserved for routed-to-DLQ outcomes.
                sink_result.delivered = max(0, sink_result.delivered - failed_count)
            else:  # DeliveryAction.RETRY
                # The SinkManager will retry the call; we don't update
                # the summary yet because the retry's terminal outcome
                # will surface back through this same callback (or the
                # success path implicit in deliver_all returning).
                pass

            return action

        return _capturing_on_delivery_error
