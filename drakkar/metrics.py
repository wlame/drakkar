"""Prometheus metrics for Drakkar framework."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from drakkar.config import MetricsConfig

messages_consumed = Counter(
    "drakkar_messages_consumed_total",
    "Total messages consumed from source topic",
    ["partition"],
)

messages_produced = Counter(
    "drakkar_messages_produced_total",
    "Total messages produced to target topic",
)

executor_tasks = Counter(
    "drakkar_executor_tasks_total",
    "Total executor tasks by status",
    ["status"],
)

executor_duration = Histogram(
    "drakkar_executor_duration_seconds",
    "Executor task duration in seconds",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

batch_duration = Histogram(
    "drakkar_batch_duration_seconds",
    "Window/batch processing duration in seconds",
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600),
)

executor_pool_active = Gauge(
    "drakkar_executor_pool_active",
    "Currently active executor tasks",
)

partition_queue_size = Gauge(
    "drakkar_partition_queue_size",
    "Number of messages waiting in partition queue",
    ["partition"],
)

offset_lag = Gauge(
    "drakkar_offset_lag",
    "Number of pending (uncommitted) offsets per partition",
    ["partition"],
)


def start_metrics_server(config: MetricsConfig) -> None:
    """Start the Prometheus metrics HTTP server if enabled."""
    if config.enabled:
        start_http_server(config.port)
