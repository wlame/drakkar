"""Configuration loading for Drakkar framework."""

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    """Kafka connection and consumer/producer settings."""

    brokers: str = "localhost:9092"
    source_topic: str = "input-events"
    target_topic: str = "output-results"
    consumer_group: str = "drakkar-workers"
    max_poll_records: int = 100
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 3_000


class ExecutorConfig(BaseModel):
    """Subprocess executor pool settings."""

    binary_path: str
    max_workers: int = 4
    task_timeout_seconds: int = 120
    window_size: int = 50


class PostgresConfig(BaseModel):
    """PostgreSQL connection settings."""

    dsn: str = "postgresql://localhost:5432/drakkar"
    pool_min: int = 2
    pool_max: int = 10


class MetricsConfig(BaseModel):
    """Prometheus metrics settings."""

    enabled: bool = True
    port: int = 9090


class LoggingConfig(BaseModel):
    """Structured logging settings."""

    level: str = "INFO"
    format: str = Field(default="json", pattern="^(json|console)$")


class DebugConfig(BaseModel):
    """Debug flight recorder and web UI settings."""

    enabled: bool = True
    port: int = 8080
    db_path: str = "/tmp/drakkar-debug.db"
    retention_hours: int = 24
    retention_max_events: int = 100_000
    store_output: bool = True
    flush_interval_seconds: int = 5


class DrakkarConfig(BaseSettings):
    """Root configuration for a Drakkar worker."""

    model_config = SettingsConfigDict(
        env_prefix="DRAKKAR_",
        env_nested_delimiter="__",
    )

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    executor: ExecutorConfig
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    debug: DebugConfig = Field(default_factory=DebugConfig)


def load_config(config_path: str | Path | None = None) -> DrakkarConfig:
    """Load configuration from YAML file and environment variables.

    YAML file path is resolved in order:
    1. Explicit config_path argument
    2. DRAKKAR_CONFIG environment variable
    3. Raises ValueError if neither is provided

    Environment variables override YAML values. Use DRAKKAR_ prefix
    with __ for nesting (e.g., DRAKKAR_KAFKA__BROKERS).
    """
    if config_path is None:
        config_path = os.environ.get("DRAKKAR_CONFIG")

    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path) as f:
            yaml_data = yaml.safe_load(f) or {}

        return DrakkarConfig(**yaml_data)

    return DrakkarConfig()
