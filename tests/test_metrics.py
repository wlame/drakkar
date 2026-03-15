"""Tests for Drakkar Prometheus metrics."""

from unittest.mock import patch

from prometheus_client import CollectorRegistry

from drakkar.config import MetricsConfig
from drakkar.metrics import (
    batch_duration,
    executor_duration,
    executor_pool_active,
    executor_tasks,
    messages_consumed,
    messages_produced,
    offset_lag,
    partition_queue_size,
    start_metrics_server,
)


def test_messages_consumed_counter():
    before = messages_consumed.labels(partition="0")._value.get()
    messages_consumed.labels(partition="0").inc()
    after = messages_consumed.labels(partition="0")._value.get()
    assert after == before + 1


def test_messages_produced_counter():
    before = messages_produced._value.get()
    messages_produced.inc()
    after = messages_produced._value.get()
    assert after == before + 1


def test_executor_tasks_counter():
    executor_tasks.labels(status="started").inc()
    executor_tasks.labels(status="completed").inc()
    executor_tasks.labels(status="failed").inc()


def test_executor_duration_histogram():
    executor_duration.observe(1.5)
    executor_duration.observe(0.3)


def test_batch_duration_histogram():
    batch_duration.observe(5.0)


def test_executor_pool_active_gauge():
    executor_pool_active.set(10)
    assert executor_pool_active._value.get() == 10
    executor_pool_active.set(0)
    assert executor_pool_active._value.get() == 0


def test_partition_queue_size_gauge():
    partition_queue_size.labels(partition="3").set(42)
    assert partition_queue_size.labels(partition="3")._value.get() == 42


def test_offset_lag_gauge():
    offset_lag.labels(partition="5").set(100)
    assert offset_lag.labels(partition="5")._value.get() == 100


def test_start_metrics_server_enabled():
    config = MetricsConfig(enabled=True, port=19090)
    with patch("drakkar.metrics.start_http_server") as mock_start:
        start_metrics_server(config)
        mock_start.assert_called_once_with(19090)


def test_start_metrics_server_disabled():
    config = MetricsConfig(enabled=False, port=19090)
    with patch("drakkar.metrics.start_http_server") as mock_start:
        start_metrics_server(config)
        mock_start.assert_not_called()
