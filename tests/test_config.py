"""Tests for Drakkar configuration loading."""

import os
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    PostgresConfig,
    load_config,
)


def test_kafka_config_defaults():
    cfg = KafkaConfig()
    assert cfg.brokers == "localhost:9092"
    assert cfg.consumer_group == "drakkar-workers"
    assert cfg.max_poll_records == 100
    assert cfg.max_poll_interval_ms == 300_000


def test_executor_config_requires_binary_path():
    with pytest.raises(ValidationError):
        ExecutorConfig()  # type: ignore[call-arg]


def test_executor_config_defaults():
    cfg = ExecutorConfig(binary_path="/usr/bin/echo")
    assert cfg.max_workers == 4
    assert cfg.task_timeout_seconds == 120
    assert cfg.window_size == 50


def test_executor_config_rejects_empty_binary_path():
    with pytest.raises(ValidationError):
        ExecutorConfig(binary_path="")


def test_executor_config_rejects_zero_workers():
    with pytest.raises(ValidationError):
        ExecutorConfig(binary_path="/bin/echo", max_workers=0)


def test_metrics_config_rejects_invalid_port():
    with pytest.raises(ValidationError):
        MetricsConfig(port=0)

    with pytest.raises(ValidationError):
        MetricsConfig(port=99999)


def test_postgres_config_defaults():
    cfg = PostgresConfig()
    assert "localhost" in cfg.dsn
    assert cfg.pool_min == 2
    assert cfg.pool_max == 10


def test_metrics_config_defaults():
    cfg = MetricsConfig()
    assert cfg.enabled is True
    assert cfg.port == 9090


def test_logging_config_defaults():
    cfg = LoggingConfig()
    assert cfg.level == "INFO"
    assert cfg.format == "json"


def test_logging_config_valid_formats():
    assert LoggingConfig(format="json").format == "json"
    assert LoggingConfig(format="console").format == "console"


def test_logging_config_invalid_format():
    with pytest.raises(ValidationError):
        LoggingConfig(format="xml")


def test_load_config_from_yaml(config_yaml_file: Path):
    cfg = load_config(config_yaml_file)
    assert cfg.kafka.brokers == "kafka1:9092,kafka2:9092"
    assert cfg.executor.binary_path == "/usr/local/bin/processor"
    assert cfg.executor.max_workers == 40
    assert cfg.postgres.dsn == "postgresql://user:pass@db:5432/app"
    assert cfg.metrics.port == 9091
    assert cfg.logging.level == "DEBUG"
    assert cfg.logging.format == "console"


def test_load_config_minimal_yaml(minimal_config_yaml_file: Path):
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.executor.binary_path == "/usr/bin/echo"
    assert cfg.kafka.brokers == "localhost:9092"
    assert cfg.metrics.enabled is True


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        load_config("/nonexistent/path/config.yaml")


def test_load_config_from_env_var(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DRAKKAR_CONFIG", str(minimal_config_yaml_file))
    cfg = load_config()
    assert cfg.executor.binary_path == "/usr/bin/echo"


def test_load_config_env_override(minimal_config_yaml_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DRAKKAR_KAFKA__BROKERS", "override:9092")
    cfg = load_config(minimal_config_yaml_file)
    assert cfg.kafka.brokers == "override:9092"


def test_load_config_no_path_no_env_requires_executor(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("DRAKKAR_CONFIG", raising=False)
    monkeypatch.setenv("DRAKKAR_EXECUTOR__BINARY_PATH", "/usr/bin/test")
    cfg = load_config()
    assert cfg.executor.binary_path == "/usr/bin/test"


def test_load_config_empty_yaml(tmp_path: Path):
    config_path = tmp_path / "empty.yaml"
    config_path.write_text("")
    with pytest.raises(ValidationError):
        load_config(config_path)


def test_drakkar_config_env_nested_delimiter(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DRAKKAR_EXECUTOR__BINARY_PATH", "/usr/bin/test")
    monkeypatch.setenv("DRAKKAR_EXECUTOR__MAX_WORKERS", "16")
    monkeypatch.setenv("DRAKKAR_KAFKA__SOURCE_TOPIC", "my-topic")
    cfg = DrakkarConfig()
    assert cfg.executor.binary_path == "/usr/bin/test"
    assert cfg.executor.max_workers == 16
    assert cfg.kafka.source_topic == "my-topic"


def test_config_serialization(config_yaml_file: Path):
    cfg = load_config(config_yaml_file)
    data = cfg.model_dump()
    assert data["kafka"]["brokers"] == "kafka1:9092,kafka2:9092"
    assert data["executor"]["max_workers"] == 40
