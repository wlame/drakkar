"""Configuration loading for Drakkar framework."""

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseModel):
    """Kafka connection and consumer/producer settings."""

    brokers: str = 'localhost:9092'
    source_topic: str = 'input-events'
    target_topic: str = 'output-results'
    consumer_group: str = 'drakkar-workers'
    max_poll_records: int = 100
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 3_000


class ExecutorConfig(BaseModel):
    """Subprocess executor pool settings."""

    binary_path: str = Field(..., min_length=1)
    max_workers: int = Field(default=4, ge=1)
    task_timeout_seconds: int = Field(default=120, ge=1)
    window_size: int = Field(default=100, ge=1)


class PostgresConfig(BaseModel):
    """PostgreSQL connection settings."""

    dsn: str = 'postgresql://localhost:5432/drakkar'
    pool_min: int = Field(default=2, ge=1)
    pool_max: int = Field(default=10, ge=1)


class MetricsConfig(BaseModel):
    """Prometheus metrics settings."""

    enabled: bool = True
    port: int = Field(default=9090, ge=1, le=65535)


class LoggingConfig(BaseModel):
    """Structured logging settings."""

    level: str = 'INFO'
    format: str = Field(default='json', pattern='^(json|console)$')


class DebugConfig(BaseModel):
    """Debug flight recorder and web UI settings."""

    enabled: bool = True
    port: int = Field(default=8080, ge=1, le=65535)
    db_path: str = '/tmp/drakkar-debug.db'
    retention_hours: int = Field(default=24, ge=1)
    retention_max_events: int = Field(default=100_000, ge=100)
    store_output: bool = True
    flush_interval_seconds: int = Field(default=5, ge=1)
    max_ui_rows: int = Field(default=5000, ge=100)


class DrakkarConfig(BaseSettings):
    """Root configuration for a Drakkar worker."""

    model_config = SettingsConfigDict(
        env_prefix='DRAKKAR_',
        env_nested_delimiter='__',
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
        config_path = os.environ.get('DRAKKAR_CONFIG')

    if config_path is not None:
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f'Config file not found: {path}')

        with open(path) as f:
            yaml_data = yaml.safe_load(f) or {}

        # pydantic-settings ignores env vars for nested models when init
        # kwargs are passed. Fix: extract DRAKKAR_* env vars, parse them
        # into nested structure, and deep-merge on top of YAML.
        env_overrides = _parse_env_overrides('DRAKKAR_', '__')
        merged = _deep_merge(yaml_data, env_overrides)
        return DrakkarConfig(**merged)

    return DrakkarConfig()


def _parse_env_overrides(prefix: str, delimiter: str) -> dict:
    """Extract env vars with prefix, split by delimiter into nested dict."""
    result: dict = {}
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        # skip the config file path env var itself
        if key == f'{prefix}CONFIG':
            continue
        parts = key[len(prefix) :].lower().split(delimiter)
        d = result
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = value
    return result


def _deep_merge(base: dict, override: dict) -> dict:
    """Deep-merge override on top of base. Override wins for leaf values."""
    result = dict(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result
