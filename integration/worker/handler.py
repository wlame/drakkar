"""Ripgrep search handler — demonstrates all Drakkar framework features.

Shows how to:
- Use typed handler with Pydantic input/output models
- FAN-OUT: one SearchRequest -> patterns x file_paths subprocess tasks
- Per-task routing (on_task_complete) + per-message aggregation
  (on_message_complete with MessageGroup)
- Route results to different sinks based on business logic
- Add custom Prometheus metrics
- Use async structured logging in hooks
- Handle executor failures with on_error() and retries
- Handle sink delivery failures with on_delivery_error() and DLQ
- Simulate random executor failures via --fail flag
- Use @periodic for recurring background tasks (stats, health checks)
"""

import asyncio
import os
import random

import structlog
from metrics import (
    delivery_retries_total,
    periodic_stats_runs_total,
    search_errors_total,
    search_match_count,
)
from models import (
    SearchAggregate,
    SearchNotification,
    SearchRequest,
    SearchResult,
    SearchSummary,
)

import drakkar as dk

logger = structlog.get_logger()

# Fail rate for simulated executor failures (passed as --fail=X to CLI)
FAIL_RATE = '0.05'


class RipgrepHandler(dk.BaseDrakkarHandler[SearchRequest, SearchResult]):
    """Searches source files using ripgrep with FAN-OUT, FAN-IN, and a
    precomputed-result fast-track driven by the framework cache.

    FAN-OUT: one SearchRequest with N patterns x M file_paths produces up
    to N*M subprocess tasks (before dedup). Every task for one message
    stamps its source_offsets with that message's offset.

    FAN-IN: when multiple messages IN THE SAME WINDOW request the same
    (pattern, file_path) pair, arrange() combines them into ONE task
    whose source_offsets lists EVERY contributing message. The framework
    reports that single task's result to every corresponding
    MessageGroup. Look for the "fan_in_count" label in the debug UI.

    PRECOMPUTED FAST-TRACK: the handler memoizes recent successful
    ripgrep stdout in the framework-provided ``self.cache`` (keyed by a
    string built from ``pattern``, ``file_path``, and ``repeat``). On the
    next arrange() window:

      - ``self.cache.peek(cache_key)`` gives a synchronous memory hit —
        no event-loop work, safe inside the tight arrange() loop.
      - If memory missed, ``await self.cache.get(cache_key)`` checks the
        local SQLite file (where the periodic flush has persisted
        earlier writes), and also reaches cross-worker values that
        peer-sync has pulled into our DB.

    A cache hit becomes a PrecomputedResult attached to the task — the
    framework skips the subprocess, synthesises an ExecutorResult, and
    feeds it through on_task_complete exactly like a real run. Cache
    hits show up in the debug UI with ``source=cache`` in task labels,
    ``metadata.precomputed=true`` in recorder events, and increment the
    ``drakkar_tasks_precomputed_total`` counter. Framework-level cache
    metrics (``drakkar_cache_hits_total`` etc.) give the hit-rate view
    previously carried by hand-rolled counters on this handler.

    Per-task (on_task_complete, one call per subprocess outcome OR cache
    hit — handler can't tell the difference):
      - Kafka "results" topic: full SearchResult
      - Postgres archive_results_db: compact per-task row
      - MongoDB: full document archive
      - Redis: cached per-(request, pattern, file) summary (1h TTL)

    Per-request (on_message_complete, one call per SearchRequest after
    all its tasks finish):
      - Kafka "priority_match_notifications" topic: ONE SearchAggregate
      - Postgres hot_recent_matches_db: if total_matches > 20
      - HTTP webhook: if total_matches > 20 (one alert per request)
      - Filesystem JSONL: if total_matches > 50
    """

    def message_label(self, msg: dk.SourceMessage) -> str:
        if msg.payload:
            req: SearchRequest = msg.payload
            fan_out = len(req.patterns) * len(req.file_paths)
            return f'{msg.partition}:{msg.offset} [{req.request_id[:8]}] {fan_out}-task fan-out'
        return f'{msg.partition}:{msg.offset}'

    async def on_startup(self, config: dk.DrakkarConfig) -> dk.DrakkarConfig:
        await logger.ainfo(
            'handler_startup',
            category='handler',
            input_model=self.input_model.__name__ if self.input_model else None,
            output_model=self.output_model.__name__ if self.output_model else None,
            binary=config.executor.binary_path,
            max_executors=config.executor.max_executors,
            fail_rate=FAIL_RATE,
        )
        return config

    async def on_ready(self, config: dk.DrakkarConfig, db_pool: object) -> None:
        # Nothing handler-local to bootstrap for caching any more —
        # ``self.cache`` is wired by the framework before the first hook
        # fires (either a real Cache when ``config.cache.enabled=true``
        # or a NoOpCache stub when disabled). Handler code can call the
        # cache unconditionally.
        self.total_collected = 0

    # -- Periodic tasks -------------------------------------------------

    @dk.periodic(seconds=10)
    async def log_stats(self):
        """Log pipeline stats every 10 seconds. Demonstrates a recurring
        background task that accesses handler state set during processing.

        Cache hit-rate is intentionally NOT logged here any more: the
        framework emits ``drakkar_cache_hits_total`` / ``_misses_total``
        counters, so the same information is available in Prometheus /
        Grafana without the handler having to hand-roll counters.
        """
        periodic_stats_runs_total.inc()
        await logger.ainfo(
            'periodic_stats',
            category='periodic',
            total_collected=self.total_collected,
        )
        await asyncio.sleep(0.8)  # emulate some async work

    @dk.periodic(seconds=30, on_error='stop')
    async def health_check(self):
        """Verify /tmp/search-corpus exists (executor needs it).
        Demonstrates on_error='stop' — if the corpus disappears, this
        task logs an error and stops rather than spamming every 30s."""
        if not os.path.isdir('/tmp/search-corpus'):
            raise RuntimeError('Search corpus directory missing: /tmp/search-corpus')
        await logger.ainfo('health_check_ok', category='periodic')

    # -------------------------------------------------------------------

    async def arrange(
        self,
        messages: list[dk.SourceMessage],
        pending: dk.PendingContext,
    ) -> list[dk.ExecutorTask]:
        """Build the task set for this window, with BOTH fan-out AND fan-in:

        - Fan-out: one message xN patterns xM file_paths → N*M tasks
          (tasks share that message's source_offset).
        - Fan-IN: if two messages in the SAME WINDOW both request the same
          (pattern, file_path) pair — not rare given the producer hot set —
          run the subprocess ONCE and stamp its source_offsets with every
          message that asked. Both messages' MessageGroups receive the
          same ExecutorResult. Saves duplicate work; demonstrates
          framework-level dedup.
        """
        # simulate slow IO-bound preparation (e.g. DB lookup, HTTP call)
        await asyncio.sleep(random.uniform(0.05, 0.5))

        # Bucket every (pattern, file_path) pair across ALL messages in the
        # window. Key = (pattern, file_path); value = list of contributing
        # messages (with their request_ids for metadata).
        by_key: dict[tuple[str, str], list[dk.SourceMessage]] = {}
        for msg in messages:
            req: SearchRequest = msg.payload
            if req is None:
                continue
            for pattern in req.patterns:
                for file_path in req.file_paths:
                    by_key.setdefault((pattern, file_path), []).append(msg)

        tasks = []
        for (pattern, file_path), contributing_msgs in by_key.items():
            task_id = dk.make_task_id('rg')
            if task_id in pending.pending_task_ids:
                continue
            # Representative repeat: max(repeat) so the subprocess does
            # at least as much work as anyone asked for (a merge policy).
            merged_repeat = max((m.payload.repeat for m in contributing_msgs), default=1)
            request_ids = [m.payload.request_id for m in contributing_msgs]
            offsets = [m.offset for m in contributing_msgs]

            # PRECOMPUTED FAST-TRACK — consult the framework cache before
            # scheduling a subprocess. A cache hit becomes a PrecomputedResult
            # attached to the task; the framework will skip the subprocess
            # entirely and feed the cached stdout to on_task_complete. The
            # debug UI marks these tasks with ``source=cache`` and the event
            # recorder sets ``metadata.precomputed=true``.
            #
            # The cache key is a pipe-delimited string (the framework cache
            # stores strings, not tuples). ``match|...`` prefix namespaces
            # these entries so other features of this handler can cohabit
            # the same cache DB without key collisions in the future.
            cache_key = f'match|{pattern}|{file_path}|{merged_repeat}'

            # Two-tier lookup: peek() is synchronous and hits the in-memory
            # dict only — ideal for the tight arrange() loop. If memory
            # misses, fall back to await self.cache.get() which checks
            # the local SQLite file (populated by the background flush
            # loop and by peer-sync pulls from other workers). This is
            # how we now see values persisted across worker restarts
            # AND values produced by peers — neither of which the
            # hand-rolled dict cache could do.
            cached_stdout = self.cache.peek(cache_key)
            if cached_stdout is None:
                cached_stdout = await self.cache.get(cache_key)
            if cached_stdout is not None:
                tasks.append(
                    dk.ExecutorTask(
                        task_id=task_id,
                        # args is empty — no subprocess will run. Kept visible
                        # in metadata for debugging what would have run.
                        metadata={
                            'request_id': request_ids[0],
                            'fan_in_request_ids': request_ids,
                            'pattern': pattern,
                            'file_path': file_path,
                            'repeat': merged_repeat,
                            'would_have_run': [str(merged_repeat), pattern, file_path],
                        },
                        labels={
                            'source': 'cache',
                            'fan_in_count': str(len(contributing_msgs)),
                            'pattern': pattern,
                            'file': file_path,
                        },
                        source_offsets=offsets,
                        precomputed=dk.PrecomputedResult(
                            stdout=cached_stdout,
                            # Small non-zero duration reflects the cache lookup
                            # itself so the UI histogram isn't cluttered with
                            # zeros; also makes the "hit" visible on timelines.
                            duration_seconds=0.0005,
                        ),
                    ),
                )
                continue

            # Cache miss: build a normal subprocess task.
            tasks.append(
                dk.ExecutorTask(
                    task_id=task_id,
                    args=[str(merged_repeat), pattern, file_path, f'--fail={FAIL_RATE}'],
                    metadata={
                        # First contributor's request_id is "primary" — used
                        # as the Kafka key for per-task output. The full
                        # list is in fan_in_request_ids for downstream debug.
                        'request_id': request_ids[0],
                        'fan_in_request_ids': request_ids,
                        'pattern': pattern,
                        'file_path': file_path,
                        'repeat': merged_repeat,
                    },
                    labels={
                        'source': 'subprocess',
                        # Label shows fan-in count so the debug UI makes it
                        # visible at a glance (e.g. "2-way fan-in").
                        'fan_in_count': str(len(contributing_msgs)),
                        'pattern': pattern,
                        'file': file_path,
                    },
                    env={
                        'REQUEST_ID': request_ids[0],
                    },
                    # THE FAN-IN: this single task is tied to every source
                    # message that requested this (pattern, file_path) pair.
                    # The framework reports its terminal outcome to every
                    # corresponding MessageGroup.
                    source_offsets=offsets,
                )
            )
        return tasks

    async def on_task_complete(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
        """Per-TASK delivery — one call per (pattern, file_path) subprocess.

        Emits fine-grained per-task records: full detail to Kafka/Mongo,
        a scalar summary to the Postgres archive (one row per task), and
        a cached summary to Redis. Request-level aggregation happens
        later in on_message_complete.

        Also populates the framework cache with this task's stdout so
        subsequent arrange() windows fast-track the same (pattern,
        file_path, repeat) combination. We only store genuinely-run
        subprocess results (``result.pid is not None``) — precomputed
        cache hits already sit in the cache, so re-writing them would
        spin needlessly through the flush loop.
        """
        self.total_collected += 1
        # simulate post-processing (e.g. parsing, enrichment)
        await asyncio.sleep(random.uniform(0.001, 0.005))

        matches = [line for line in result.stdout.strip().split('\n') if line]
        meta = result.task.metadata

        # Persist into the framework cache for subsequent fast-track
        # hits. ``scope=CLUSTER`` shares across all workers in the same
        # cluster (cluster_name) — the periodic peer-sync loop pulls
        # recent rows from peer ``-cache.db`` files and LWW-merges. This
        # demonstrates cross-worker cache sharing in the integration
        # scenario; switch to LOCAL to keep per-worker caches isolated,
        # or GLOBAL to share across clusters. TTL cleans old entries
        # out of the on-disk DB automatically.
        if result.pid is not None:
            cache_key = f'match|{meta["pattern"]}|{meta["file_path"]}|{meta["repeat"]}'
            self.cache.set(
                cache_key,
                result.stdout,
                ttl=3600,
                scope=dk.CacheScope.CLUSTER,
            )

        # build typed output models
        output = SearchResult(
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            file_path=meta['file_path'],
            repeat=meta['repeat'],
            match_count=len(matches),
            duration_seconds=result.duration_seconds,
            matches=matches[:50],
        )

        summary = SearchSummary(
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            match_count=len(matches),
            duration_seconds=result.duration_seconds,
        )

        # custom Prometheus metric
        search_match_count.observe(len(matches))

        # async structured logging — one event per (pattern, file) task
        await logger.ainfo(
            'task_completed',
            category='handler',
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            file_path=meta['file_path'],
            match_count=len(matches),
            duration=round(result.duration_seconds, 3),
        )

        # Per-task detail sinks. Request-level rollup happens in
        # on_message_complete; we keep per-task records for traceability
        # (each subprocess outcome is individually addressable).
        return dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=output,
                    key=meta['request_id'].encode(),
                    sink='results',
                ),
            ],
            postgres=[
                dk.PostgresPayload(
                    table='search_results',
                    data=summary,
                    sink='archive_results_db',
                ),
            ],
            mongo=[dk.MongoPayload(collection='search_archive', data=output)],
            redis=[
                dk.RedisPayload(
                    key=f'search:{meta["request_id"]}:{meta["pattern"]}:{meta["file_path"]}',
                    data=summary,
                    ttl=3600,
                ),
            ],
        )

    async def on_message_complete(self, group: dk.MessageGroup) -> dk.CollectResult | None:
        """Per-REQUEST aggregation — fires once after ALL fan-out tasks finish.

        ``group`` contains:
          - source_message: the original Kafka message
          - tasks: every task scheduled (including replaced ones)
          - results: terminal successes
          - errors: terminal failures (SKIP or retries exhausted)
          - replaced: computed count of replaced-originals in the history

        The aggregation below rolls the per-task counts + match totals
        into one ``SearchAggregate`` record per request, then routes it
        to a priority Kafka topic and (conditionally) to the hot
        Postgres DB, a webhook, and a file — using MESSAGE-level
        thresholds rather than per-task.
        """
        req: SearchRequest | None = group.source_message.payload
        if req is None or group.is_empty:
            # arrange() produced no tasks for this message (poison
            # message or deliberately filtered). Nothing to aggregate.
            return None

        # Sum per-task match_counts. result.stdout holds the task's match
        # lines; we re-parse them here because on_task_complete's output
        # isn't shared across hooks (each hook is independent). In a real
        # handler you'd cache the parsed data on self for efficiency, or
        # emit a compact intermediate via on_task_complete.
        def _match_count(r: dk.ExecutorResult) -> int:
            return sum(1 for line in r.stdout.strip().split('\n') if line)

        match_counts = [_match_count(r) for r in group.results]
        total_matches = sum(match_counts)
        max_matches = max(match_counts) if match_counts else 0

        aggregate = SearchAggregate(
            request_id=req.request_id,
            partition=group.source_message.partition,
            offset=group.source_message.offset,
            total_tasks=group.total,
            succeeded_tasks=group.succeeded,
            failed_tasks=group.failed,
            replaced_tasks=group.replaced,
            total_matches=total_matches,
            max_matches=max_matches,
            duration_seconds=round(group.duration_seconds, 3),
        )

        await logger.ainfo(
            'request_aggregated',
            category='handler',
            request_id=req.request_id,
            total_tasks=aggregate.total_tasks,
            succeeded=aggregate.succeeded_tasks,
            failed=aggregate.failed_tasks,
            total_matches=aggregate.total_matches,
            duration=aggregate.duration_seconds,
        )

        # Always: one aggregate record per request to the priority
        # Kafka topic. Downstream analytics consumers use this stream
        # instead of the per-task "results" topic.
        sinks = dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=aggregate,
                    key=req.request_id.encode(),
                    sink='priority_match_notifications',
                ),
            ],
        )

        # Conditional: a "hot" Postgres row for requests with significant
        # match volume — kept small and fast-queryable for dashboards.
        if aggregate.total_matches > 20:
            sinks.postgres.append(
                dk.PostgresPayload(
                    table='hot_recent_matches',
                    data=aggregate,
                    sink='hot_recent_matches_db',
                ),
            )

            # Fire a single webhook per HIGH-match REQUEST (previously was
            # per-task; the request-level threshold is a better signal).
            notification = SearchNotification(
                request_id=req.request_id,
                pattern=','.join(req.patterns),
                match_count=aggregate.total_matches,
                message=(f'Request matched {aggregate.total_matches} lines across {aggregate.succeeded_tasks} tasks'),
            )
            sinks.http.append(dk.HttpPayload(data=notification))

        # Conditional: JSONL file log for very high-match requests.
        if aggregate.total_matches > 50:
            sinks.files.append(
                dk.FilePayload(path='/tmp/high-match-requests.jsonl', data=aggregate),
            )

        return sinks

    async def on_error(self, task: dk.ExecutorTask, error: dk.ExecutorError) -> str:
        error_type = 'timeout' if error.exception and 'Timeout' in error.exception else 'exit_code'
        search_errors_total.labels(error_type=error_type).inc()

        await logger.awarning(
            'search_failed',
            category='handler',
            request_id=task.metadata.get('request_id', '?'),
            task_id=task.task_id,
            exit_code=error.exit_code,
            error_type=error_type,
        )

        # retry simulated failures, skip everything else
        if error.exit_code == 1 and error.stderr and 'SIMULATED FAILURE' in error.stderr:
            await logger.ainfo('retrying_simulated_failure', category='handler', task_id=task.task_id)
            return dk.ErrorAction.RETRY
        return dk.ErrorAction.SKIP

    async def on_delivery_error(self, error: dk.DeliveryError) -> dk.DeliveryAction:
        delivery_retries_total.labels(sink_type=error.sink_type).inc()

        await logger.awarning(
            'delivery_failed',
            category='handler',
            sink_name=error.sink_name,
            sink_type=error.sink_type,
            error=error.error,
            payload_count=len(error.payloads),
        )

        # retry HTTP/Redis failures (transient), DLQ for everything else
        if error.sink_type in ('http', 'redis'):
            return dk.DeliveryAction.RETRY
        return dk.DeliveryAction.DLQ

    async def on_assign(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_assigned', category='handler', partitions=partitions)

    async def on_revoke(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_revoked', category='handler', partitions=partitions)
