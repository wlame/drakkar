"""Prometheus metrics for Drakkar framework."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
from prometheus_client.metrics import MetricWrapperBase
from prometheus_client.registry import REGISTRY

if TYPE_CHECKING:
    from drakkar.config import MetricsConfig

# --- Worker identity ---

worker_info = Info(
    'drakkar_worker',
    'Worker instance identity and version',
)

# --- Messages ---

messages_consumed = Counter(
    'drakkar_messages_consumed_total',
    'Total messages consumed from source topic',
    ['partition'],
)

# --- Executor ---

executor_tasks = Counter(
    'drakkar_executor_tasks_total',
    'Total executor tasks by status',
    ['status'],
)

executor_duration = Histogram(
    'drakkar_executor_duration_seconds',
    'Executor task duration in seconds',
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

executor_pool_active = Gauge(
    'drakkar_executor_pool_active',
    'Currently active executor tasks',
)

executor_timeouts = Counter(
    'drakkar_executor_timeouts_total',
    'Total executor tasks that timed out',
)

task_retries = Counter(
    'drakkar_task_retries_total',
    'Total executor tasks retried after failure',
)

tasks_precomputed = Counter(
    'drakkar_tasks_precomputed_total',
    (
        'Total tasks whose result was supplied by the handler via '
        'ExecutorTask.precomputed, skipping subprocess execution. '
        'The framework is agnostic to the reason (cache hit, lookup, etc.).'
    ),
)

# --- Windows/batches ---

batch_duration = Histogram(
    'drakkar_batch_duration_seconds',
    'Window/batch processing duration in seconds',
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
)

# --- Partition state ---

partition_queue_size = Gauge(
    'drakkar_partition_queue_size',
    'Number of messages waiting in partition queue',
    ['partition'],
)

offset_lag = Gauge(
    'drakkar_offset_lag',
    'Number of pending (uncommitted) offsets per partition',
    ['partition'],
)

backpressure_active = Gauge(
    'drakkar_backpressure_active',
    'Whether consumer is paused due to backpressure (1=paused, 0=flowing)',
)

total_queued = Gauge(
    'drakkar_total_queued',
    'Total messages buffered in partition queues plus in-flight tasks',
)

assigned_partitions = Gauge(
    'drakkar_assigned_partitions',
    'Number of partitions currently assigned to this worker',
)

executor_idle_waste = Counter(
    'drakkar_executor_idle_slot_seconds_total',
    'Accumulated slot-seconds of executor idle time while messages are waiting in queues. '
    'If 2 slots are idle for 3 seconds while messages are queued, this increments by 6.',
)

consumer_idle = Counter(
    'drakkar_consumer_idle_seconds_total',
    'Accumulated seconds with no messages available from Kafka and nothing in queues. '
    'Measures time the worker has genuinely nothing to do (consumer lag is zero). '
    'Does not count time when the consumer is paused due to backpressure.',
)

# --- Consumer errors ---

consumer_errors = Counter(
    'drakkar_consumer_errors_total',
    'Total Kafka consumer poll errors',
)

# --- Offset commits ---

offsets_committed = Counter(
    'drakkar_offsets_committed_total',
    'Total offset commit operations',
    ['partition'],
)

# --- Rebalancing ---

rebalance_events = Counter(
    'drakkar_rebalance_events_total',
    'Total Kafka rebalance events',
    ['type'],
)

# --- Sinks ---

sink_deliver_duration = Histogram(
    'drakkar_sink_deliver_duration_seconds',
    'Duration of sink delivery operations',
    ['sink_type', 'sink_name'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
)

sink_deliver_errors = Counter(
    'drakkar_sink_deliver_errors_total',
    'Total sink delivery failures',
    ['sink_type', 'sink_name'],
)

sink_payloads_delivered = Counter(
    'drakkar_sink_payloads_delivered_total',
    'Total payloads delivered to sinks',
    ['sink_type', 'sink_name'],
)

sink_delivery_retries = Counter(
    'drakkar_sink_delivery_retries_total',
    'Total sink delivery retry attempts',
    ['sink_type', 'sink_name'],
)

sink_deliveries_skipped = Counter(
    'drakkar_sink_deliveries_skipped_total',
    'Total sink deliveries skipped via on_delivery_error handler',
    ['sink_type', 'sink_name'],
)

sink_dlq_messages = Counter(
    'drakkar_sink_dlq_messages_total',
    'Total messages sent to the dead letter queue',
)

dlq_send_failures = Counter(
    'drakkar_dlq_send_failures_total',
    'Total failed attempts to send messages to the dead letter queue',
)

# Per-sink circuit breaker observability. The gauge maps to
# three discrete circuit states so a single time-series makes the state
# transitions visible on any Grafana line chart:
#   0.0 — closed   (normal operation)
#   0.5 — half_open (one probe delivery in flight; success closes, failure
#                    reopens)
#   1.0 — open     (cooldown in progress; all deliveries route to DLQ
#                    without attempting the sink)
# The trip counter ticks once per closed→open transition so operators can
# compute trip frequency and alert on ``rate(..._total[5m]) > threshold``.
sink_circuit_open = Gauge(
    'drakkar_sink_circuit_open',
    ('Sink circuit breaker state: 0.0=closed (normal), 0.5=half_open (probing), 1.0=open (tripped — routing to DLQ)'),
    ['sink_type', 'sink_name'],
)

sink_circuit_trips = Counter(
    'drakkar_sink_circuit_trips_total',
    # Ticks on ANY transition into the open state — the initial closed→open
    # trip at the failure threshold AND half_open→open reopenings after a
    # failed probe. That way a flapping circuit shows up as a rate on this
    # counter, not as a single trip plus silent reopens.
    'Total transitions into the open state (initial trips + failed-probe reopens) per sink',
    ['sink_type', 'sink_name'],
)

# --- Handler hooks ---

handler_duration = Histogram(
    'drakkar_handler_duration_seconds',
    'Duration of user handler hook execution',
    ['hook'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30),
)


# --- Cache ---
#
# The cache metrics suite covers the full lifecycle of a cached entry —
# read (hits/misses), write (sets/deletes/evictions), DB persistence
# (flush entries), cross-worker replication (sync fetched/upserted/errors),
# and space reclaim (cleanup). Memory gauges track the live in-memory state
# and are maintained as running sums (never walked on scrape).

# Read-path counters. Every call into ``Cache.get`` lands in exactly one of
# ``hits{source=...}`` or ``misses`` — operators can compute hit rate as
# ``sum(hits) / (sum(hits) + misses)`` and inspect which source (memory
# fast-path vs SQLite fallback) is dominant for a given workload.
#
# The ``source`` label on hits distinguishes:
#   source='memory' — the value was served from the in-memory ``_memory`` dict
#   source='db'     — memory miss fell through to the reader SQLite connection
#                    and the row was revived + warmed back into memory
#
# Separate label values (rather than two counters) keeps the `hit rate` math
# a straightforward sum and matches the Prometheus convention for partition
# labels on a shared semantic.
cache_hits = Counter(
    'drakkar_cache_hits_total',
    'Total cache reads served without reaching the DB fallback miss branch',
    ['source'],
)
cache_misses = Counter(
    'drakkar_cache_misses_total',
    'Total cache reads that returned None (key absent / expired in both memory and DB)',
)

# Write-path counters. ``writes{scope}`` ticks on every ``Cache.set`` (one
# counter per ``CacheScope`` value). ``deletes`` ticks on every
# ``Cache.delete`` call regardless of whether the key was present — the
# counter measures user intent, not row-affected count, matching the
# "throughput not state transitions" convention used by flush / sync.
cache_writes = Counter(
    'drakkar_cache_writes_total',
    'Total cache writes (Cache.set) labelled by entry scope',
    ['scope'],
)
cache_deletes = Counter(
    'drakkar_cache_deletes_total',
    'Total cache deletes (Cache.delete calls)',
)

# LRU eviction counter. Increments once per entry popped from memory when
# ``max_memory_entries`` is exceeded — operators watch this to spot
# cap-undersizing or hot-key churn.
cache_evictions = Counter(
    'drakkar_cache_evictions_total',
    'Total entries evicted from the in-memory cache dict due to the LRU cap',
)

# Memory gauges — "live" view of what's sitting in the Cache's in-memory
# dict right now. Both are maintained as **running sums**: set/delete/evict
# each adjusts the counter by one (or by size_bytes for the bytes gauge).
# We never walk the dict on scrape — doing so would turn a zero-cost
# scrape into an O(N) scan, which defeats the whole point of
# ``bytes_in_memory`` as a cheap observability signal.
cache_entries_in_memory = Gauge(
    'drakkar_cache_entries_in_memory',
    'Current number of entries held in the in-memory cache dict',
)
cache_bytes_in_memory = Gauge(
    'drakkar_cache_bytes_in_memory',
    'Sum of size_bytes across entries held in the in-memory cache dict',
)

# Flush-loop counter reporting op-by-op activity (UPSERTs and deletes) drained
# from the dirty map per flush.
#
# Labels:
#   op='set'     — rows UPSERTed via ``LWW_UPSERT_SQL``
#   op='delete'  — rows removed via local-only DELETE
#
# The counter measures the number of op intents drained from ``_dirty``
# per flush, not the number of rows actually changed — a SET whose LWW
# lost to a newer row still counts, because the flush "did the work" of
# attempting the UPSERT. Treat the counter as "flush throughput per op
# type", not "rows successfully modified".
cache_flush_entries = Counter(
    'drakkar_cache_flush_entries_total',
    'Total cache entries drained from the dirty map to SQLite per op type',
    ['op'],
)

# Cleanup-loop counter. The cleanup loop deletes every row whose
# ``expires_at_ms < now_ms``; this counter advances by the number of rows
# actually removed per cycle. Operators use it to spot TTL mis-sizing —
# a sudden spike means many entries are expiring together.
cache_cleanup_removed = Counter(
    'drakkar_cache_cleanup_removed_total',
    'Total cache entries removed from SQLite by the cleanup loop',
)

# Peer-sync counters for the UPSERT apply step. Both are labelled by peer
# worker name so operators can spot uneven sync throughput or a single
# misbehaving peer.
#
#   fetched  — rows read from the peer's cache DB via the scoped SELECT.
#   upserted — rows the engine *attempted* to UPSERT into the local DB.
#
# Under our conventions ``upserted`` increments once per fetched row
# regardless of whether the LWW guard accepted it — the counter measures
# pipeline throughput, not DB state transitions, matching
# ``cache_flush_entries``. This way operators can see sync cost even on
# idle clusters where LWW rejects most rows.
cache_sync_entries_fetched = Counter(
    'drakkar_cache_sync_entries_fetched_total',
    'Total cache entries pulled from a peer cache DB by the sync loop',
    ['peer'],
)
cache_sync_entries_upserted = Counter(
    'drakkar_cache_sync_entries_upserted_total',
    'Total cache entries the sync loop attempted to UPSERT into the local DB',
    ['peer'],
)

# Peer-sync error counter. Labelled by peer so operators can alert on a
# specific misbehaving worker (corrupt DB, missing file, read timeout, etc.)
# without parsing logs. One increment per failed per-peer cycle; the sync
# loop itself keeps running so one bad peer cannot break the whole worker.
cache_sync_errors = Counter(
    'drakkar_cache_sync_errors_total',
    'Total per-peer failures during the cache peer-sync cycle',
    ['peer'],
)

# Per-cycle deadline counter. Complements the per-peer
# ``cache_sync_errors`` counter: that one ticks on a single peer failing,
# this one ticks when the whole cycle (all peers combined) blew past the
# wall-clock cap. Unlabelled — the deadline is a worker-level property so
# per-peer labels would add cardinality without analytical value. Any
# nonzero rate means the worker has more peers than ``interval_seconds``
# can comfortably accommodate, or one peer's DB is pathologically slow.
cache_peer_sync_timeouts = Counter(
    'drakkar_cache_peer_sync_timeouts_total',
    (
        'Peer-sync cycles that hit the per-cycle deadline before completing. '
        'Any nonzero rate indicates a slow peer or large peer count relative '
        'to interval_seconds.'
    ),
)

# DB-size gauges refreshed by the cleanup loop. Counting DB rows on every
# ``set``/``get`` would defeat the running-sum design used for the in-memory
# gauges, so the DB view is updated at cleanup cadence (default 60s). Since
# cleanup is where rows leave the DB, this is a natural point to refresh.
cache_entries_in_db = Gauge(
    'drakkar_cache_entries_in_db',
    'Current number of entries persisted in the local cache SQLite DB',
)
cache_bytes_in_db = Gauge(
    'drakkar_cache_bytes_in_db',
    'Sum of size_bytes across entries persisted in the local cache SQLite DB',
)


def cache_gauge_snapshot() -> dict[str, int]:
    """Return the current integer values of all four cache gauges.

    Provides a stable, library-internals-free interface for callers that
    need to read gauge values (notably the debug server's
    ``/api/debug/cache/stats`` endpoint). Iterates the gauge's sample
    collection via prometheus_client's public ``collect()`` API rather
    than reaching into ``_value``, so a prometheus_client upgrade that
    changes the private representation will not silently break callers.

    The four gauges are maintained by the cache engine:
      - ``entries_in_memory`` / ``bytes_in_memory`` — live mutations
        on set/delete/evict/cleanup.
      - ``entries_in_db`` / ``bytes_in_db`` — refreshed once per cleanup
        cycle via ``SELECT COUNT(*), SUM(size_bytes)``.

    Returns a dict with integer-valued keys; int-casting matches the
    historical shape of the stats endpoint.
    """

    def _value_of(gauge: Gauge) -> int:
        # A Gauge with no labels has exactly one sample per collect()
        # iteration; read the first sample's value. This is the public
        # prometheus_client contract, not an internal detail.
        for metric in gauge.collect():
            for sample in metric.samples:
                return int(sample.value)
        return 0

    return {
        'entries_in_memory': _value_of(cache_entries_in_memory),
        'bytes_in_memory': _value_of(cache_bytes_in_memory),
        'entries_in_db': _value_of(cache_entries_in_db),
        'bytes_in_db': _value_of(cache_bytes_in_db),
    }


# --- Recorder ---
#
# Observability for the flight recorder's in-memory flush buffer. The
# recorder buffers events in a ``deque(maxlen=N)`` and flushes them to
# SQLite periodically. Under contention (slow disk, high event volume)
# the buffer can fill and start silently evicting the oldest entries —
# these three metrics surface that pathway so operators can alert on it.
#
#   recorder_buffer_size          Current depth of the flush buffer.
#                                 Stable near zero under healthy flushing.
#   recorder_dropped_events_total Events silently dropped on overflow.
#                                 Any nonzero value is a data-loss signal.
#   recorder_flush_duration_seconds Wall-clock time spent in one flush
#                                   (executemany + commit). The histogram
#                                   buckets are tuned for sub-second flushes
#                                   with outliers up to 5s.
recorder_buffer_size = Gauge(
    'drakkar_recorder_buffer_size',
    (
        'Current number of events waiting in the recorder flush buffer. '
        'Stabilizes near zero under healthy flushing; grows during contention.'
    ),
)

recorder_dropped_events = Counter(
    'drakkar_recorder_dropped_events_total',
    (
        'Total events silently dropped when the recorder buffer overflowed '
        'due to flush stalls. Any nonzero value indicates the recorder is '
        'losing observability data.'
    ),
)

recorder_flush_duration = Histogram(
    'drakkar_recorder_flush_duration_seconds',
    'Duration of the recorder flush body (executemany + commit) in seconds',
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


# --- Periodic tasks ---

periodic_task_runs = Counter(
    'drakkar_periodic_task_runs_total',
    'Total periodic task executions by name and outcome',
    ['name', 'status'],
)

periodic_task_duration = Histogram(
    'drakkar_periodic_task_duration_seconds',
    'Duration of periodic task executions',
    ['name'],
    buckets=(0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60),
)


def start_metrics_server(config: MetricsConfig) -> None:
    """Start the Prometheus metrics HTTP server if enabled."""
    if config.enabled:
        start_http_server(config.port)


# --- Framework metric names (for classifying framework vs user) ---

FRAMEWORK_PREFIXES = ('drakkar_',)


def discover_handler_metrics(handler: object) -> dict[str, MetricWrapperBase]:
    """Discover prometheus metrics declared as class attributes on a handler.

    Scans the handler's class hierarchy (MRO) for attributes that are
    instances of prometheus_client MetricWrapperBase (Counter, Gauge,
    Histogram, Summary, Info). Returns a dict of attribute_name → metric.
    """
    metrics: dict[str, MetricWrapperBase] = {}
    for cls in type(handler).__mro__:
        for attr_name, attr_value in vars(cls).items():
            if attr_name.startswith('_'):
                continue
            if isinstance(attr_value, MetricWrapperBase) and attr_name not in metrics:
                metrics[attr_name] = attr_value
    return metrics


def collect_all_metrics() -> list[dict[str, Any]]:
    """Collect a snapshot of all registered Prometheus metrics.

    Returns a list of dicts with name, type, help, source (framework/user),
    and current samples (label sets + values). Histograms are summarized
    as count+sum instead of listing every bucket.
    """
    result: list[dict[str, Any]] = []
    for metric_family in REGISTRY.collect():
        # skip internal python/process/gc metrics
        if metric_family.name.startswith(('python_', 'process_', 'gc_')):
            continue

        is_framework = any(metric_family.name.startswith(p) for p in FRAMEWORK_PREFIXES)
        source = 'framework' if is_framework else 'user'

        samples: list[dict[str, Any]] = []
        for sample in metric_family.samples:
            # skip _created and _bucket samples for compactness
            if sample.name.endswith('_created'):
                continue
            if sample.name.endswith('_bucket'):
                continue
            samples.append(
                {
                    'name': sample.name,
                    'labels': dict(sample.labels) if sample.labels else {},
                    'value': sample.value,
                }
            )

        result.append(
            {
                'name': metric_family.name,
                'type': metric_family.type,
                'help': metric_family.documentation,
                'source': source,
                'samples': samples,
            }
        )

    result.sort(key=lambda m: (0 if m['source'] == 'framework' else 1, m['name']))
    return result
