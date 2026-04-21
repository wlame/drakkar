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

# --- Handler hooks ---

handler_duration = Histogram(
    'drakkar_handler_duration_seconds',
    'Duration of user handler hook execution',
    ['hook'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30),
)


# --- Cache ---
#
# The full cache metrics suite (hits/misses/writes/flush/sync/cleanup gauges)
# lands in Task 14. This counter is introduced here because the in-memory
# Cache API (Task 4) drives LRU eviction and we want a first-class metric
# for it from day one — otherwise the caller has no visibility into whether
# their `max_memory_entries` cap is being hit.

cache_evictions = Counter(
    'drakkar_cache_evictions_total',
    'Total entries evicted from the in-memory cache dict due to the LRU cap',
)

# Flush-loop counter introduced alongside Task 7. The full metrics wiring
# pass (Task 14) adds hits/misses/writes/sync/cleanup — but the flush
# counters land here now so the flush path can report op-by-op activity
# from the first cycle it runs.
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

# Cleanup-loop counter introduced alongside Task 10. The cleanup loop deletes
# every row whose ``expires_at_ms < now_ms``; this counter advances by the
# number of rows actually removed per cycle. Operators use it to spot TTL
# mis-sizing — a sudden spike means many entries are expiring together.
cache_cleanup_removed = Counter(
    'drakkar_cache_cleanup_removed_total',
    'Total cache entries removed from SQLite by the cleanup loop',
)

# Peer-sync counters introduced alongside Task 12 (UPSERT apply step). Both
# are labelled by peer worker name so operators can spot uneven sync
# throughput or a single misbehaving peer.
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

# Peer-sync error counter introduced alongside Task 13. Labelled by peer so
# operators can alert on a specific misbehaving worker (corrupt DB, missing
# file, read timeout, etc.) without parsing logs. One increment per failed
# per-peer cycle; the sync loop itself keeps running so one bad peer cannot
# break the whole worker.
cache_sync_errors = Counter(
    'drakkar_cache_sync_errors_total',
    'Total per-peer failures during the cache peer-sync cycle',
    ['peer'],
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
