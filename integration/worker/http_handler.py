"""The ripgrep pipeline behind the HTTP source only.

Same ``arrange`` logic as ``RipgrepHandler`` — the HTTP hook wraps the
request into one synthetic source message and reuses it — but sink
delivery is reduced to one Kafka topic, because the HTTP-only worker
config declares no Postgres, Mongo, Redis, webhook or file sink.

Why a subclass and not composition around a ``RipgrepHandler`` instance:
the framework wires a handler's runtime dependencies onto the object it
was handed — ``cache``, the offload pool, the annotator, the timeline
emitter and the loaded ``app_config`` are all set per-instance on THAT
handler, and ``app_config_model`` / ``app_env_prefix`` /
``probe_details_model`` / ``ui_pages`` / the ``@dk.periodic`` methods are
read off THAT handler's class. A wrapped inner handler would see none of
it, so every one of those would have to be forwarded by hand and kept in
sync forever. Inheriting puts the whole ripgrep pipeline and the
framework's wiring on one object, and this module only restates what
actually differs: the four HTTP-path hooks.

Re-declaring ``dk.BaseDrakkarHandler[...]`` as a second base is what
re-parameterises the generic — ``RipgrepHandler`` fixes slots 3 and 4 to
RankRequest/RankResponse, and the webapp bootstrap reads those slots to
build the POST route's request and response models.
"""

import time

from handler import RipgrepHandler, _match_count
from models import SearchAggregate, SearchRequest, SearchResponse, SearchResult, TaskMatches

import drakkar as dk

# The single Kafka sink instance the HTTP-only worker declares. Anything
# else in a CollectResult would fail the sink manager's startup-time
# validation, which is deliberate: an unroutable payload is a
# configuration error, not something to discover in production.
MIRRORED_RESULTS_SINK = 'mirrored_results'

# Synthetic source-message coordinates for an HTTP request. partition=-1
# matches what the framework's own webapp runner stamps on its synthetic
# message, so HTTP-origin work stays cleanly distinct from any real Kafka
# partition in labels, annotations and the Live timeline.
WEBAPP_TOPIC = '__webapp__'
WEBAPP_PARTITION = -1


class HttpSearchHandler(
    RipgrepHandler,
    dk.BaseDrakkarHandler[SearchRequest, SearchResult, SearchRequest, SearchResponse],
):
    """``RipgrepHandler`` re-hosted for an HTTP-only worker.

    Inherits the whole Kafka-side pipeline — ``arrange`` with its
    fan-out, fan-in and precomputed cache fast-track, the periodic tasks,
    the probe tab, the error and delivery policies — and changes only
    what the HTTP-only deployment makes different:

    - the HTTP request model is ``SearchRequest`` itself rather than
      ``RankRequest``, so a POST carries the same patterns x file_paths
      fan-out a Kafka message would;
    - ``arrange_http_request`` wraps that request into one synthetic
      source message and runs the inherited ``arrange`` over it;
    - ``on_task_complete`` keeps the inherited side effects (cache
      population, metrics, logging) but delivers nothing, because this
      worker has no per-task sinks;
    - ``on_message_complete`` mirrors one aggregate record to the single
      configured Kafka sink.
    """

    def __init__(self) -> None:
        super().__init__()
        # Mirrors the framework webapp runner's own ``_request_seq``: it
        # numbers each request's synthetic source message from 1 and
        # increments once per request, right before arrange_http_request.
        # Tracking it here deliberately keeps the offset this handler
        # stamps equal to the one the runner stamps, so the annotations
        # arrange() emits land on the same virtual offset as the rest of
        # the request's trace. Drift between the two counters would not
        # break processing — it would only anchor the demo's annotations
        # on an offset no other event in the trace uses.
        self._request_seq = 0

    async def arrange_http_request(
        self,
        req: SearchRequest,
        pending: dk.PendingContext,
    ) -> list[dk.ExecutorTask]:
        """Turn one POSTed SearchRequest into the same tasks Kafka would.

        The request is wrapped in a synthetic ``SourceMessage`` and handed
        to the inherited ``arrange`` as a one-message window, so the HTTP
        path exercises the identical planning code — including the
        offloaded scan plan and the two-tier cache lookup — rather than a
        parallel implementation that could drift from it.

        The inherited ``arrange`` also sleeps 50-500ms to simulate IO-bound
        preparation; on this path that latency lands on the blocked HTTP
        caller, which is intended — the harness scenario wants the webapp
        timings to reflect the real pipeline's cost.
        """
        # One increment per request, matching the runner's. Safe without a
        # lock: the hook runs on the single event loop and there is no
        # await between the read and the write.
        self._request_seq += 1
        message = dk.SourceMessage(
            topic=WEBAPP_TOPIC,
            partition=WEBAPP_PARTITION,
            offset=self._request_seq,
            key=req.request_id.encode(),
            value=req.model_dump_json().encode(),
            timestamp=int(time.time() * 1000),
            # Kafka-path parity: arrange() reads the parsed model off
            # ``payload``, never off ``value``.
            payload=req,
        )
        return await self.arrange([message], pending)

    async def on_task_complete(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
        """Keep the inherited per-task work reachable, but deliver nothing.

        The HTTP runner never calls this hook — it aggregates through
        ``on_http_request_complete`` instead (see ``_submit_tasks`` in
        ``drakkar/webapp/runner.py``), so on this worker the only caller
        is the Debug UI message probe, which replays the whole
        arrange → execute → collect sequence. Delegating rather than
        returning ``None`` outright keeps that replay faithful, and keeps
        the hook correct if the Kafka source is ever turned back on for
        this handler.

        A consequence worth knowing when reading the harness: because the
        hook does not run on the live HTTP path, this worker never writes
        to the framework cache itself. Its precomputed fast-track is
        primed only by rows peer-sync pulls from the Kafka workers' cache
        files.

        The ``CollectResult`` is discarded either way: it targets
        Postgres, Mongo and Redis sinks this worker does not configure,
        and the per-task detail already goes back to the caller in the
        HTTP response.
        """
        await super().on_task_complete(result)
        return None

    async def on_message_complete(self, group: dk.MessageGroup) -> dk.CollectResult | None:
        """Mirror one aggregate record to the only configured sink.

        Same ``SearchAggregate`` shape the Kafka worker emits, so a
        consumer can read the HTTP worker's topic with the same schema —
        but routed to this worker's lone Kafka sink instead of the
        Kafka worker's priority topic plus its Postgres, Redis, webhook
        and file fan-out.
        """
        if group.source_message.payload is None or group.is_empty:
            # arrange_http_request produced no tasks — nothing to mirror.
            return None

        summary: SearchAggregate = self.build_summary(group, [_match_count(r) for r in group.results])
        return dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=summary,
                    key=summary.request_id.encode(),
                    sink=MIRRORED_RESULTS_SINK,
                ),
            ],
        )

    async def on_http_request_complete(self, group: dk.MessageGroup) -> SearchResponse:
        """Answer the POST with the per-task breakdown of its fan-out.

        One ``TaskMatches`` row per terminal success, naming the
        (pattern, file_path) pair the task covered — the caller's view of
        what the synchronous request actually ran. Failed tasks are
        counted in ``failed`` but contribute no row, because they produced
        no match output to report.
        """
        # A group whose payload never parsed carries no request id to echo.
        req: SearchRequest | None = group.source_message.payload
        return SearchResponse(
            request_id=req.request_id if req is not None else '',
            tasks=len(group.tasks),
            succeeded=group.succeeded,
            failed=group.failed,
            matches=[
                TaskMatches(
                    task_id=r.task.task_id,
                    pattern=r.task.metadata.get('pattern', ''),
                    file_path=r.task.metadata.get('file_path', ''),
                    match_count=_match_count(r),
                )
                for r in group.results
            ],
        )
