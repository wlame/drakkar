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

# --- Handler hooks ---

handler_duration = Histogram(
    'drakkar_handler_duration_seconds',
    'Duration of user handler hook execution',
    ['hook'],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30),
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
            samples.append({
                'name': sample.name,
                'labels': dict(sample.labels) if sample.labels else {},
                'value': sample.value,
            })

        result.append({
            'name': metric_family.name,
            'type': metric_family.type,
            'help': metric_family.documentation,
            'source': source,
            'samples': samples,
        })

    result.sort(key=lambda m: (0 if m['source'] == 'framework' else 1, m['name']))
    return result
