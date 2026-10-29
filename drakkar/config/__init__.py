"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DK_ prefix with __ for nesting (e.g., DK_KAFKA__BROKERS).

The models are split per domain across this package — ``kafka``, ``sinks``,
``sources``, ``runtime``, ``ui``, ``cache``, ``storage``, ``periodic``, ``fleet``, ``connections``, ``webapp``
— and assembled by ``root``. ``base`` holds their base class, ``units`` the types
for values with units, and ``strict`` the check for unknown keys.
Everything is re-exported here, so ``from drakkar.config import X`` remains
the one import path regardless of which module defines ``X``.
"""

from drakkar.config.base import ConfigModel
from drakkar.config.cache import CacheConfig
from drakkar.config.connections import (
    ConnectionsConfig,
    ConsulConnectionConfig,
    PostgresConnectionConfig,
    RedisConnectionConfig,
)
from drakkar.config.fleet import (
    CONNECTION_PROVIDERS,
    CacheExchangeConfig,
    FleetConfig,
    FleetRegistryConfig,
)
from drakkar.config.kafka import KafkaConfig
from drakkar.config.periodic import ElectionConfig, PeriodicConfig
from drakkar.config.resilience import BreakerConfig, ResilienceConfig, validate_resilience
from drakkar.config.root import DrakkarConfig, _parse_env_overrides, load_config, load_yaml_document
from drakkar.config.runtime import (
    ExecutorConfig,
    IOConfig,
    LoggingConfig,
    MetricsConfig,
    OffloadConfig,
    RuntimeHealthConfig,
)
from drakkar.config.sinks import (
    CircuitBreakerConfig,
    DLQConfig,
    FileSinkConfig,
    HttpSinkConfig,
    KafkaSinkConfig,
    MongoSinkConfig,
    MongoStatementConfig,
    PostgresSinkConfig,
    RedisSinkConfig,
    SinkInstanceConfig,
    SinkPolicy,
    SinksConfig,
)
from drakkar.config.sources import KafkaSourceConfig, SourcesConfig
from drakkar.config.storage import (
    DEFAULT_ARCHIVE_RETENTION_DAYS,
    HistoryArchiveConfig,
    HistoryConfig,
    SharedConfig,
    StorageConfig,
)
from drakkar.config.strict import UnknownKey, deep_merge, find_unknown_keys, unknown_keys_message
from drakkar.config.ui import (
    BUILTIN_LINK_BASES,
    TIMELINE_COLOR_NAMES,
    TIMELINE_NUMERIC_FIELDS,
    TIMELINE_OPS,
    TIMELINE_STRING_FIELDS,
    DocsAnchor,
    DocsAnchorMatch,
    TimelineColorRule,
    TimelineEventType,
    TimelineLabels,
    TimelineRuleCondition,
    UIConfig,
    UIConsumePauseConfig,
    UIDocsConfig,
    UIProbeDetailsConfig,
    UIRecorderConfig,
    UIRecorderMemoryConfig,
    UIRecorderPostgresConfig,
    UIReleaseConfig,
    UITimelineConfig,
    UITraceConfig,
)
from drakkar.config.units import (
    ByteSize,
    Duration,
    WholeDays,
    WholeHours,
    WholeSeconds,
    format_duration,
    format_size,
    parse_duration,
    parse_size,
)
from drakkar.config.webapp import WebAppConfig, WebClientConfig

__all__ = [
    'BUILTIN_LINK_BASES',
    'CONNECTION_PROVIDERS',
    'DEFAULT_ARCHIVE_RETENTION_DAYS',
    'TIMELINE_COLOR_NAMES',
    'TIMELINE_NUMERIC_FIELDS',
    'TIMELINE_OPS',
    'TIMELINE_STRING_FIELDS',
    'BreakerConfig',
    'ByteSize',
    'CacheConfig',
    'CacheExchangeConfig',
    'CircuitBreakerConfig',
    'ConfigModel',
    'ConnectionsConfig',
    'ConsulConnectionConfig',
    'DLQConfig',
    'DocsAnchor',
    'DocsAnchorMatch',
    'DrakkarConfig',
    'Duration',
    'ElectionConfig',
    'ExecutorConfig',
    'FileSinkConfig',
    'FleetConfig',
    'FleetRegistryConfig',
    'HistoryArchiveConfig',
    'HistoryConfig',
    'HttpSinkConfig',
    'IOConfig',
    'KafkaConfig',
    'KafkaSinkConfig',
    'KafkaSourceConfig',
    'LoggingConfig',
    'MetricsConfig',
    'MongoSinkConfig',
    'MongoStatementConfig',
    'OffloadConfig',
    'PeriodicConfig',
    'PostgresConnectionConfig',
    'PostgresSinkConfig',
    'RedisConnectionConfig',
    'RedisSinkConfig',
    'ResilienceConfig',
    'RuntimeHealthConfig',
    'SharedConfig',
    'SinkInstanceConfig',
    'SinkPolicy',
    'SinksConfig',
    'SourcesConfig',
    'StorageConfig',
    'TimelineColorRule',
    'TimelineEventType',
    'TimelineLabels',
    'TimelineRuleCondition',
    'UIConfig',
    'UIConsumePauseConfig',
    'UIDocsConfig',
    'UIProbeDetailsConfig',
    'UIRecorderConfig',
    'UIRecorderMemoryConfig',
    'UIRecorderPostgresConfig',
    'UIReleaseConfig',
    'UITimelineConfig',
    'UITraceConfig',
    'UnknownKey',
    'WebAppConfig',
    'WebClientConfig',
    'WholeDays',
    'WholeHours',
    'WholeSeconds',
    '_parse_env_overrides',
    'deep_merge',
    'find_unknown_keys',
    'format_duration',
    'format_size',
    'load_config',
    'load_yaml_document',
    'parse_duration',
    'parse_size',
    'unknown_keys_message',
    'validate_resilience',
]
