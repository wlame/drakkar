"""Prometheus metrics for Drakkar framework."""

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

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

messages_produced = Counter(
    'drakkar_messages_produced_total',
    'Total messages produced to target topic',
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

# --- Consumer/Producer errors ---

consumer_errors = Counter(
    'drakkar_consumer_errors_total',
    'Total Kafka consumer poll errors',
)

producer_errors = Counter(
    'drakkar_producer_errors_total',
    'Total Kafka producer delivery failures',
)

produce_duration = Histogram(
    'drakkar_produce_duration_seconds',
    'Time to produce a single message to Kafka',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
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

# --- Database ---

db_rows_written = Counter(
    'drakkar_db_rows_written_total',
    'Total rows written to PostgreSQL',
)

db_write_duration = Histogram(
    'drakkar_db_write_duration_seconds',
    'Duration of database write operations',
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
)

db_errors = Counter(
    'drakkar_db_errors_total',
    'Total database write errors',
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
