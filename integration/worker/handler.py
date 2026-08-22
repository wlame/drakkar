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
- OFFLOAD: run the CPU/syscall-bound window planning (nested loops +
  file stats + cache probes) on the offload pool via self.offload(),
  keeping the event loop responsive (docs/offload.md)
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
    AppConfig,
    CacheLookupRow,
    MatchAnalysisRow,
    PatternRankRow,
    PatternStatsParams,
    RankRequest,
    RankResponse,
    RequestKey,
    RequestNotified,
    RequestSummary,
    RipgrepProbeDetails,
    SearchAggregate,
    SearchNotification,
    SearchRequest,
    SearchResult,
    SearchSummary,
    SinkDecisionRow,
)

import drakkar as dk
from drakkar import probe
from drakkar.probe import Column
from drakkar.uipages import AnnotationsSource, EventsSource, MetricsSource, Page, TasksSource, Widget

logger = structlog.get_logger()

# Fail rate for simulated executor failures (passed as --fail=X to CLI)
FAIL_RATE = '0.05'

# Webapp tasks always pick from this small pattern list — keeps the demo
# deterministic and decoupled from the Kafka producer's pattern set. Two
# patterns mean every ``RankRequest`` produces two HTTP-origin executor
# tasks, which is enough to demonstrate fan-out and the 4-param generic
# wiring without flooding the executor pool.
HTTP_PATTERNS = ['main', 'config']
HTTP_FILE_PATH = '/tmp/search-corpus'

# ui.timeline demo: byte thresholds for the human-readable file_size label,
# largest first so the first unit the count clears wins the suffix.
_TIMELINE_SIZE_UNITS = (('M', 1024 * 1024), ('K', 1024))


def _human_file_size(num_bytes: int) -> str:
    """Render a byte count as a short human string: plain bytes below 1024,
    one decimal with a K/M suffix at or above it (e.g. '512', '12.4K')."""
    for suffix, unit in _TIMELINE_SIZE_UNITS:
        if num_bytes >= unit:
            return f'{num_bytes / unit:.1f}{suffix}'
    return str(num_bytes)


def _scan_target_stats(file_path: str) -> tuple[int, int]:
    """Best-effort (size_bytes, line_count) for a scan target. A directory
    or any other unreadable path returns (0, 0) rather than raising —
    arrange() must not fail a whole window over one label input."""
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
    except OSError:
        return 0, 0
    return len(content), content.count(b'\n')


def _scan_target_module(file_path: str) -> str:
    """Directory owning the scan target: its own name when the target IS a
    directory (e.g. '/project/drakkar' scanned recursively), otherwise its
    parent directory's name (e.g. '/project/drakkar/app.py' -> 'drakkar')."""
    if os.path.isdir(file_path):
        return os.path.basename(file_path.rstrip('/')) or file_path
    return os.path.basename(os.path.dirname(file_path)) or os.path.basename(file_path)


def _scan_target_labels(file_path: str) -> dict[str, str]:
    """The five ui.timeline demo labels describing the scan target itself —
    base name, owning directory, size (human + exact bytes), and line
    count — feeding the color_rules and tag/caption/highlight/filter roles
    in drakkar.yaml's ui.timeline block."""
    size_bytes, lines = _scan_target_stats(file_path)
    return {
        'file_name': os.path.basename(file_path),
        'module': _scan_target_module(file_path),
        'file_size': _human_file_size(size_bytes),
        'file_size_bytes': str(size_bytes),
        'lines': str(lines),
    }


class RipgrepHandler(
    dk.BaseDrakkarHandler[SearchRequest, SearchResult, RankRequest, RankResponse],
):
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
        thread-safe, so it runs inside the OFFLOADED planning function
        (see ``_build_scan_plan``).
      - If memory missed, ``await self.cache.get(cache_key)`` checks the
        local SQLite file (where the periodic flush has persisted
        earlier writes), and also reaches cross-worker values that
        peer-sync has pulled into our DB. ``get`` is a loop-only
        coroutine, so this tier runs in ``arrange()`` after the offload
        returns.

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

    # User-defined app config — see docs/app-config.md. The framework
    # loads AppConfig from drakkar.yaml's app: section (plus RGAPP_* env
    # overrides — docker-compose raises worker-2's threshold that way)
    # and exposes it as self.app_config before any hook runs.
    app_config_model = AppConfig
    app_env_prefix = 'RGAPP_'

    # User-defined Message Probe tab — see docs/probe-user-details.md.
    # The probe.set/append/update calls sprinkled through this handler
    # are near-zero-cost no-ops outside a probe — but any computation
    # feeding them (e.g. the derived stats in on_task_complete) still
    # runs in production. It's wired straight into the real pipeline
    # rather than gated behind a flag because that computation is cheap.
    probe_details_model = RipgrepProbeDetails

    # Declared dashboard page — see docs/ui-pages.md. Adds a "Scan
    # activity" nav entry at /p/scan-activity, reading data the framework
    # already records (no new endpoint, no extra handler bookkeeping).
    ui_pages = [
        Page(
            slug='scan-activity',
            title='Scan activity',
            widgets=[
                Widget(
                    title='Recent tasks',
                    view='table',
                    source=TasksSource(limit=100),
                    columns={
                        'task_id': Column(),
                        'status': Column(
                            badge_colors={'completed': 'green', 'failed': 'red', 'running': 'blue', '*': 'gray'}
                        ),
                        'exec_duration': Column(hint='Execution time in seconds'),
                        'exit_code': Column(),
                    },
                ),
                Widget(
                    title='Executor tasks total',
                    view='stat',
                    source=MetricsSource(metric='drakkar_executor_tasks_total'),
                    format='number',
                ),
                Widget(
                    title='Task failures',
                    view='table',
                    source=EventsSource(event_types=['task_failed'], limit=100),
                    columns={
                        'task_id': Column(hint='Failed task {value}'),
                        'exit_code': Column(),
                        'partition': Column(),
                        'origin': Column(),
                    },
                ),
                Widget(
                    title='Fan-in bucketing',
                    view='table',
                    source=AnnotationsSource(kind_prefix='fan_in_bucketing', limit=100),
                    columns=['messages', 'distinct_pairs', 'collapsed_duplicates'],
                ),
            ],
        ),
    ]

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
            # self.app_config is already loaded here — the framework wires
            # it before on_startup. Threshold only; the api_key stays out
            # of logs (SecretStr would mask it anyway).
            priority_match_threshold=self.app_config.priority_match_threshold
            if isinstance(self.app_config, AppConfig)
            else None,
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

    def _build_scan_plan(
        self,
        messages: list[dk.SourceMessage],
    ) -> tuple[dict[tuple[str, str], list[dk.SourceMessage]], list[dict]]:
        """Synchronous window planning — runs on the offload pool, not the loop.

        Everything CPU- or syscall-bound about arranging a window lives
        here: the nested patterns x file_paths bucketing, the per-target
        stat/read syscalls behind ``_scan_target_labels``, and the
        memory-tier cache probe. On real workloads this is the part that
        grows with window size and can hold the event loop for seconds;
        under ``self.offload()`` it occupies one pool thread instead,
        while Kafka polling, executor completions, and sink flushes keep
        running (docs/offload.md).

        Cache rules inside an offloaded function: the sync ops are
        thread-safe, so ``self.cache.peek`` is fine here; the async
        ``self.cache.get`` (SQLite fallback) is loop-only and therefore
        runs in ``arrange()`` AFTER this returns.
        """
        # Bucket every (pattern, file_path) pair across ALL messages in
        # the window. Key = (pattern, file_path); value = list of
        # contributing messages (their request_ids feed task metadata).
        by_key: dict[tuple[str, str], list[dk.SourceMessage]] = {}
        for msg in messages:
            req: SearchRequest = msg.payload
            if req is None:
                continue
            for pattern in req.patterns:
                for file_path in req.file_paths:
                    by_key.setdefault((pattern, file_path), []).append(msg)

        # One stat/read pass per DISTINCT file rather than one per pair —
        # and all of them on this single pool thread, replacing the
        # per-pair asyncio.to_thread hop the pre-offload version paid.
        labels_by_file = {file_path: _scan_target_labels(file_path) for _, file_path in by_key}

        plan: list[dict] = []
        for (pattern, file_path), contributing_msgs in by_key.items():
            # Representative repeat: max(repeat) so the subprocess does
            # at least as much work as anyone asked for (a merge policy).
            merged_repeat = max((m.payload.repeat for m in contributing_msgs), default=1)
            # The cache key is a pipe-delimited string (the framework
            # cache stores strings, not tuples). ``match|...`` prefix
            # namespaces these entries so other features of this handler
            # can cohabit the same cache DB without key collisions.
            cache_key = f'match|{pattern}|{file_path}|{merged_repeat}'
            plan.append(
                {
                    'pattern': pattern,
                    'file_path': file_path,
                    'merged_repeat': merged_repeat,
                    'request_ids': [m.payload.request_id for m in contributing_msgs],
                    'offsets': [m.offset for m in contributing_msgs],
                    'fan_in': len(contributing_msgs),
                    'scan_labels': labels_by_file[file_path],
                    'cache_key': cache_key,
                    # Memory-tier probe — thread-safe sync op. ``None``
                    # here is not yet a miss: arrange() still asks the
                    # SQLite tier via the loop-only async get().
                    'cached_stdout': self.cache.peek(cache_key),
                }
            )
        return by_key, plan

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

        # ui.timeline demo: every task built in this arrange() window shares
        # one "request" label — <partition>:<first-offset> of the window's
        # first message — so the timeline's marker role draws one pin per
        # Kafka batch boundary rather than one per task.
        request_label = f'{messages[0].partition}:{messages[0].offset}'

        # OFFLOAD: the whole CPU/syscall-bound planning pass — nested-loop
        # bucketing, per-file stat/read syscalls, memory-tier cache probes —
        # runs on the offload pool in ONE call. The event loop stays free
        # for the other partitions, the executor, and the sinks while it
        # computes. Shows up as an `offload` event on this window's trace
        # and in the drakkar_offload_* metrics (docs/offload.md).
        by_key, plan = await self.offload(self._build_scan_plan, messages)

        # Window-scoped annotation: what the bucketing decided for this whole
        # arrange() call. This is the "why does this window have fewer tasks
        # than messages x patterns x files" answer, visible on every message
        # in the window rather than reconstructed from task counts.
        collapsed = sum(len(m) - 1 for m in by_key.values() if len(m) > 1)

        # Probe-details example: the window-level shape, in the same
        # words the fan_in_bucketing annotation above already carries —
        # here it lands in the User-defined tab instead of an
        # annotation, free outside a probe.
        probe.set(
            window_shape=(
                f'{len(messages)} messages → {len(by_key)} distinct (pattern, file) pairs, '
                f'{collapsed} duplicates collapsed'
            )
        )
        probe.update(
            'stage_counters', messages=len(messages), distinct_pairs=len(by_key), collapsed_duplicates=collapsed
        )

        self.annotate(
            None,
            'fan_in_bucketing',
            {
                'messages': len(messages),
                'distinct_pairs': len(by_key),
                'collapsed_duplicates': collapsed,
                'pairs': [f'{pattern}|{path}' for pattern, path in by_key],
            },
        )

        # Message-scoped: which pairs each message contributed, and which of
        # them it shares with a sibling. Answers "what did THIS message ask
        # for, and did it get its own subprocess or someone else's".
        for msg in messages:
            mine = [
                {'pair': f'{pattern}|{path}', 'shared_with': len(msgs) - 1}
                for (pattern, path), msgs in by_key.items()
                if msg in msgs
            ]
            if mine:
                self.annotate(msg, 'requested_pairs', {'pairs': mine})

        tasks = []
        # Probe-details example: probe.update() merges keys rather than
        # adding to them, so per-window totals are tallied locally and
        # written once after the loop instead of once per pair.
        cache_tally = {
            'cache_hits_memory': 0,
            'cache_hits_sqlite': 0,
            'cache_misses': 0,
            'subprocess_tasks': 0,
            'precomputed_tasks': 0,
        }
        for entry in plan:
            task_id = dk.make_task_id('rg')
            if task_id in pending.pending_task_ids:
                continue
            pattern = entry['pattern']
            file_path = entry['file_path']
            merged_repeat = entry['merged_repeat']
            request_ids = entry['request_ids']
            offsets = entry['offsets']
            scan_labels = entry['scan_labels']
            cache_key = entry['cache_key']

            # PRECOMPUTED FAST-TRACK — consult the framework cache before
            # scheduling a subprocess. A cache hit becomes a PrecomputedResult
            # attached to the task; the framework will skip the subprocess
            # entirely and feed the cached stdout to on_task_complete. The
            # debug UI marks these tasks with ``source=cache`` and the event
            # recorder sets ``metadata.precomputed=true``.
            #
            # Two-tier lookup, split across the offload boundary: the
            # memory tier was already probed with the thread-safe peek()
            # inside _build_scan_plan; here — back on the loop, where
            # coroutines are allowed — a memory miss falls through to
            # await self.cache.get(), which checks the local SQLite file
            # (populated by the background flush loop and by peer-sync
            # pulls from other workers). This is how we see values
            # persisted across worker restarts AND values produced by
            # peers — neither of which a hand-rolled dict cache could do.
            cached_stdout = entry['cached_stdout']
            tier = 'memory'
            if cached_stdout is None:
                cached_stdout = await self.cache.get(cache_key)
                tier = 'sqlite' if cached_stdout is not None else 'miss'
            hit = cached_stdout is not None

            # Probe-details example: record what the two-tier lookup
            # decided for this pair before branching on it.
            probe.append(
                'cache_lookups',
                CacheLookupRow(
                    cache_key=cache_key,
                    tier=tier,
                    decision='precomputed' if hit else 'subprocess',
                    fan_in=entry['fan_in'],
                    outcome='hit' if hit else 'miss',
                ),
            )
            if tier == 'memory':
                cache_tally['cache_hits_memory'] += 1
            elif tier == 'sqlite':
                cache_tally['cache_hits_sqlite'] += 1
            else:
                cache_tally['cache_misses'] += 1
            cache_tally['precomputed_tasks' if hit else 'subprocess_tasks'] += 1

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
                        # Marker env var so the cache-hit nature of this task
                        # is visible in the debug UI's per-task env section
                        # and in the recorder. No subprocess actually runs,
                        # so the value is purely for traceability — operators
                        # who land on a "CLI: []" task can confirm at a glance
                        # that it was a cache short-circuit, not an empty
                        # subprocess invocation.
                        env={'PRECOMPUTED_RESULT': 'taken-from-cache'},
                        labels={
                            'source': 'cache',
                            'fan_in_count': str(entry['fan_in']),
                            'pattern': pattern,
                            'file': file_path,
                            'request': request_label,
                            **scan_labels,
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
                # Task-scoped: the subprocess did NOT run, and the reason is
                # not recoverable from the task's own fields — args is empty
                # either way. This is what tells an operator the difference
                # between a cache short-circuit and a misbuilt task.
                self.annotate(
                    tasks[-1],
                    'precomputed_from_cache',
                    {
                        'cache_key': cache_key,
                        'would_have_run': [str(merged_repeat), pattern, file_path],
                        'fan_in_request_ids': request_ids,
                    },
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
                        'fan_in_count': str(entry['fan_in']),
                        'pattern': pattern,
                        'file': file_path,
                        'request': request_label,
                        **scan_labels,
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

        # Probe-details example: one merge with this window's totals,
        # rather than one probe.update() call per pair.
        probe.update('stage_counters', **cache_tally)
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

        # Probe-details example: the recording (probe.append) is a no-op
        # outside a probe; the derived stats themselves are one cheap
        # pass over the already-materialized matches list. run-rg.sh
        # invokes rg with --no-filename, so lines carry no path prefix —
        # distinct_lines counts repeated content instead (the runner
        # repeats searches, so duplicate lines are expected).
        distinct_lines = len(set(matches))
        longest_line = max((len(line) for line in matches), default=0)
        # Synthetic-but-plausible popup-only fields (see docs/ui-enrichment.md
        # detail panels): scan_meta is invented diagnostic junk derived from
        # real values where convenient, sample_lines is a small preview of
        # the real matches list.
        scan_meta = {
            'encoding': 'utf-8',
            'shard': len(meta['file_path']) % 4,
            'attempt': 1,
            'worker_note': f'handled by {os.environ.get("WORKER_ID", "worker-?")}',
        }
        sample_lines = [
            {'line_no': i + 1, 'text': line[:80], 'score': round(len(line) / (longest_line or 1), 2)}
            for i, line in enumerate(matches[:3])
        ]
        probe.append(
            'match_analysis',
            MatchAnalysisRow(
                pattern=meta['pattern'],
                file=meta['file_path'],
                matches=len(matches),
                distinct_lines=distinct_lines,
                longest_line=longest_line,
                duration_ms=round(result.duration_seconds * 1000, 2),
                source='cache' if result.pid is None else 'subprocess',
                bytes_scanned=len(result.stdout.encode('utf-8')),
                scan_meta=scan_meta,
                sample_lines=sample_lines,
            ),
        )

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

        # Probe-details example: this request's matches, broken down by
        # pattern and ranked — the same data total_matches rolls up,
        # kept at the pattern granularity instead of collapsed to a sum.
        # Zipped against the match_counts just computed above rather
        # than re-parsing each result's stdout a second time.
        pattern_totals: dict[str, int] = {}
        for r, count in zip(group.results, match_counts, strict=True):
            pattern_totals[r.task.metadata['pattern']] = pattern_totals.get(r.task.metadata['pattern'], 0) + count
        ranked_patterns = sorted(pattern_totals.items(), key=lambda kv: kv[1], reverse=True)
        for rank, (pattern, pattern_matches) in enumerate(ranked_patterns, start=1):
            probe.append(
                'pattern_ranking',
                PatternRankRow(
                    rank=rank,
                    pattern=pattern,
                    matches=pattern_matches,
                    share_pct=round(100 * pattern_matches / total_matches, 1) if total_matches else 0.0,
                ),
            )

        # Probe-details example: a request-level verdict badge, using the
        # same thresholds already computed below for the hot-row/webhook
        # and file-log sink decisions.
        if total_matches == 0:
            verdict = 'clean'
        elif total_matches > 50:
            verdict = 'noisy'
        else:
            verdict = 'matched'
        probe.set(scan_verdict=verdict)

        # Probe-details example: a custom-renderer scalar. patternChip() in
        # custom-renderers.js turns this small JSON payload into a styled
        # chip instead of any built-in view.
        if ranked_patterns:
            top_pattern, top_matches = ranked_patterns[0]
            probe.set(top_pattern_chip={'pattern': top_pattern, 'matches': top_matches})

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
        # Probe-details example: this one always fires, alongside the
        # conditional decisions recorded further down.
        probe.append(
            'sink_decisions',
            SinkDecisionRow(
                sink='kafka',
                destination='priority_match_notifications',
                fired='yes',
                reason='every request gets one aggregate record',
            ),
        )

        # ---- write operations beyond INSERT ------------------------------
        #
        # Everything above this point is "append a record". A pipeline that
        # maintains state needs more, and each payload below shows one of
        # the operations that exist for it.

        # UPSERT: one row per request, keyed on request_id.
        #
        # Delivery is at-least-once, so a redelivered request would duplicate
        # this row under a plain INSERT. `update_columns` deliberately omits
        # `notified`, so a redelivery cannot un-send a webhook that already
        # went out — the columns an upsert overwrites are a choice, not
        # automatically "all of them".
        sinks.postgres.append(
            dk.PostgresPayload(
                op=dk.PostgresOp.UPSERT,
                table='request_summaries',
                data=RequestSummary(
                    request_id=req.request_id,
                    total_matches=aggregate.total_matches,
                    succeeded_tasks=aggregate.succeeded_tasks,
                    failed_tasks=aggregate.failed_tasks,
                    duration_seconds=aggregate.duration_seconds,
                ),
                conflict=['request_id'],
                update_columns=['total_matches', 'succeeded_tasks', 'failed_tasks', 'duration_seconds'],
                sink='archive_results_db',
            ),
        )
        probe.append(
            'sink_decisions',
            SinkDecisionRow(
                sink='postgres',
                destination='request_summaries',
                fired='yes',
                reason='every request upserts its summary row',
            ),
        )

        # STATEMENT: running totals per pattern. The new value depends on the
        # old one, which no declarative operation can express, so the SQL
        # lives in configuration under a name and the payload supplies only
        # bound parameters.
        for pattern in req.patterns:
            sinks.postgres.append(
                dk.PostgresPayload(
                    op=dk.PostgresOp.STATEMENT,
                    statement='bump_pattern_stats',
                    params=PatternStatsParams(pattern=pattern, matches=aggregate.total_matches),
                    sink='archive_results_db',
                ),
            )

        # Redis, beyond the per-task SET in on_task_complete.
        #
        # HSET writes several fields of one hash in one command; its values
        # are a plain mapping rather than a model, because hash fields are
        # frequently dynamic keys. EXPIRE is separate because HSET carries no
        # TTL of its own.
        sinks.redis.extend(
            [
                dk.RedisPayload(
                    op=dk.RedisOp.HSET,
                    key=f'request:{req.request_id}',
                    fields={
                        'total_matches': aggregate.total_matches,
                        'succeeded': aggregate.succeeded_tasks,
                        'failed': aggregate.failed_tasks,
                    },
                    sink='hot_match_cache',
                ),
                dk.RedisPayload(
                    op=dk.RedisOp.EXPIRE,
                    key=f'request:{req.request_id}',
                    ttl=3600,
                    sink='hot_match_cache',
                ),
                # INCRBY accumulates, which is why a batch containing one is
                # never fast-retried: re-running it would count twice.
                dk.RedisPayload(
                    op=dk.RedisOp.INCRBY,
                    key='matches:total',
                    amount=aggregate.total_matches,
                    sink='hot_match_cache',
                ),
                # ZADD sets a score rather than incrementing it, so it
                # converges on a retry. The mapping is member -> score.
                dk.RedisPayload(
                    op=dk.RedisOp.ZADD,
                    key='leaderboard:requests',
                    members={req.request_id: float(aggregate.total_matches)},
                    sink='hot_match_cache',
                ),
                # SCRIPT: append to a capped recent-requests list. As two
                # commands this could interleave with another worker between
                # the push and the trim; inside a script it cannot.
                dk.RedisPayload(
                    op=dk.RedisOp.SCRIPT,
                    script='push_and_cap',
                    keys=['recent:requests'],
                    args=[req.request_id, 50],
                    sink='hot_match_cache',
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
            probe.append(
                'sink_decisions',
                SinkDecisionRow(
                    sink='postgres',
                    destination='hot_recent_matches_db',
                    fired='yes',
                    reason=f'total_matches {aggregate.total_matches} > 20',
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
            probe.append(
                'sink_decisions',
                SinkDecisionRow(
                    sink='http',
                    destination='webhook',
                    fired='yes',
                    reason=f'total_matches {aggregate.total_matches} > 20',
                ),
            )

            # UPDATE: record that the webhook went out. The predicate is
            # required and may never be empty — an empty one would rewrite
            # every row in the table.
            #
            # This payload is appended AFTER the upsert above, and execution
            # order always equals payload order, so it can never run first
            # and be overwritten by it.
            sinks.postgres.append(
                dk.PostgresPayload(
                    op=dk.PostgresOp.UPDATE,
                    table='request_summaries',
                    data=RequestNotified(),
                    where=RequestKey(request_id=req.request_id),
                    sink='archive_results_db',
                ),
            )
        else:
            # Probe-details example: the "no" half of the same decision,
            # so the User-defined tab always shows both conditional
            # sinks below the 20-match threshold, not just the fired ones.
            probe.append(
                'sink_decisions',
                SinkDecisionRow(
                    sink='postgres',
                    destination='hot_recent_matches_db',
                    fired='no',
                    reason=f'total_matches {aggregate.total_matches} ≤ 20',
                ),
            )
            probe.append(
                'sink_decisions',
                SinkDecisionRow(
                    sink='http',
                    destination='webhook',
                    fired='no',
                    reason=f'total_matches {aggregate.total_matches} ≤ 20',
                ),
            )

        # Conditional: JSONL file log for very high-match requests.
        if aggregate.total_matches > 50:
            sinks.files.append(
                dk.FilePayload(path='/tmp/high-match-requests.jsonl', data=aggregate),
            )
            probe.append(
                'sink_decisions',
                SinkDecisionRow(
                    sink='files',
                    destination='/tmp/high-match-requests.jsonl',
                    fired='yes',
                    reason=f'total_matches {aggregate.total_matches} > 50',
                ),
            )
        else:
            probe.append(
                'sink_decisions',
                SinkDecisionRow(
                    sink='files',
                    destination='/tmp/high-match-requests.jsonl',
                    fired='no',
                    reason=f'total_matches {aggregate.total_matches} ≤ 50',
                ),
            )

        # Probe-details example: the thresholds this window's decisions
        # were made against, next to what was actually observed.
        probe.set(
            thresholds={
                'hot_row_and_webhook': 20,
                'file_log': 50,
                'observed_total_matches': total_matches,
                'observed_max_matches': max_matches,
            }
        )

        return sinks

    async def on_error(self, task: dk.ExecutorTask, error: dk.ExecutorError) -> str:
        error_type = 'timeout' if error.kind == 'timeout' else 'exit_code'
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

    # ------------------------------------------------------------------
    # Webapp pipeline (only invoked when ``webapp.enabled=True`` — by default
    # only worker-1 has the webapp turned on; worker-2 / worker-3 never call
    # these hooks and their existence is ignored at runtime).
    # ------------------------------------------------------------------

    async def arrange_http_request(
        self,
        req: RankRequest,
        pending: dk.PendingContext,
    ) -> list[dk.ExecutorTask]:
        """Translate a single HTTP RankRequest into 1-2 executor tasks.

        Every HTTP request fans out into ``len(HTTP_PATTERNS)`` ripgrep tasks
        against ``HTTP_FILE_PATH``. Tasks carry priority/client metadata so
        ``task_priority`` (below) can prioritise them ahead of Kafka tasks
        AND respect a ``priority_class`` override coming from the request
        path — both axes of the priority demo are exercised.
        """

        await logger.ainfo(
            'http_arrange',
            category='handler',
            request_id=req.request_id,
            score=req.score,
            patterns=HTTP_PATTERNS,
        )

        # Use ``score`` to deterministically vary the repeat count: small
        # scores produce fast tasks, large scores produce slow ones. This
        # makes the priority-scheduling demo visible in /live: a slow Kafka
        # task in flight + an HTTP task arriving should cause the HTTP task
        # to be dequeued ahead of any pending Kafka task at the gate.
        repeat = max(1, min(5, req.score % 5 + 1))

        tasks: list[dk.ExecutorTask] = []
        for pattern in HTTP_PATTERNS:
            task_id = dk.make_task_id('http-rg')
            if task_id in pending.pending_task_ids:
                continue
            tasks.append(
                dk.ExecutorTask(
                    task_id=task_id,
                    args=[str(repeat), pattern, HTTP_FILE_PATH, f'--fail={FAIL_RATE}'],
                    metadata={
                        'request_id': req.request_id,
                        'pattern': pattern,
                        'file_path': HTTP_FILE_PATH,
                        'repeat': repeat,
                        # Read by ``task_priority``: HTTP tasks tagged
                        # ``priority_class='web'`` jump the queue. Tasks
                        # without the tag fall back to the default key.
                        'priority_class': 'web',
                        'client': 'http-client',
                    },
                    labels={
                        'source': 'http',
                        'pattern': pattern,
                        'file': HTTP_FILE_PATH,
                    },
                ),
            )
        return tasks

    async def on_http_request_complete(self, group: dk.MessageGroup) -> RankResponse:
        """Build the HTTP response from the gathered task outputs.

        Counts match lines across all completed tasks and folds them into a
        single ``RankResponse``. ``client_hint`` echoes the priority class
        assigned by ``arrange_http_request`` so the caller can confirm its
        request was processed on the fast lane.
        """

        if group.is_empty:
            return RankResponse(
                request_id=group.request_id or '',
                result=0,
                client_hint='empty',
            )

        total_matches = 0
        for r in group.results:
            total_matches += sum(1 for line in r.stdout.strip().split('\n') if line)

        await logger.ainfo(
            'http_completed',
            category='handler',
            request_id=group.request_id,
            client_name=group.client_name,
            succeeded=group.succeeded,
            failed=group.failed,
            total_matches=total_matches,
        )

        return RankResponse(
            request_id=group.request_id or '',
            result=total_matches,
            client_hint='web-priority',
            succeeded_tasks=group.succeeded,
            failed_tasks=group.failed,
        )

    def task_priority(self, task: dk.ExecutorTask) -> tuple[int, int]:
        """Order tasks at the executor gate.

        Returns a ``(priority_class, tiebreak)`` tuple where smaller
        priority_class dequeues first. The framework auto-stamps
        ``task.origin='http'`` for tasks returned by
        ``arrange_http_request``, so we can route on it directly.

        Two axes are demonstrated:

        - ``origin='http'`` tasks always lead Kafka tasks, even when a Kafka
          task has been waiting longer (the synchronous client is blocked
          on the response, so it gets the fast lane).
        - Within Kafka tasks, smaller offsets dequeue first — matching the
          framework default and keeping ``OffsetTracker`` memory bounded.
        """

        if task.origin == 'http':
            return (0, 0)
        return (1, min(task.source_offsets) if task.source_offsets else 0)

    async def on_assign(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_assigned', category='handler', partitions=partitions)

    async def on_revoke(self, partitions: list[int]) -> None:
        await logger.ainfo('partitions_revoked', category='handler', partitions=partitions)
