"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DK_ prefix with __ for nesting (e.g., DK_KAFKA__BROKERS).

The models are split per domain across this package — ``kafka``, ``sinks``,
``sources``, ``runtime``, ``ui``, ``cache``, ``storage``, ``periodic``, ``fleet``, ``connections``, ``webapp``
— and assembled by ``root``.
Everything is re-exported here, so ``from drakkar.config import X`` remains
the one import path regardless of which module defines ``X``.
"""

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
    ConsulRegistryConfig,
    FilesystemRegistryConfig,
    FleetConfig,
    FleetRegistryConfig,
    PostgresRegistryConfig,
    RedisRegistryConfig,
    StaticPeerConfig,
    StaticRegistryConfig,
)
from drakkar.config.kafka import KafkaConfig
from drakkar.config.periodic import ElectionConfig, PeriodicConfig, PeriodicTaskOverride
from drakkar.config.resilience import BreakerConfig, ResilienceConfig, validate_resilience
from drakkar.config.root import DrakkarConfig, _deep_merge, _parse_env_overrides, load_config
from drakkar.config.runtime import (
    ExecutorConfig,
    IOConfig,
    LoggingConfig,
    MetricsConfig,
    OffloadConfig,
    RuntimeHealthConfig,
    ThroughputConfig,
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
    UIReleaseConfig,
    UITimelineConfig,
    UITraceConfig,
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
    'CacheConfig',
    'CacheExchangeConfig',
    'CircuitBreakerConfig',
    'ConnectionsConfig',
    'ConsulConnectionConfig',
    'ConsulRegistryConfig',
    'DLQConfig',
    'DocsAnchor',
    'DocsAnchorMatch',
    'DrakkarConfig',
    'ElectionConfig',
    'ExecutorConfig',
    'FileSinkConfig',
    'FilesystemRegistryConfig',
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
    'PeriodicTaskOverride',
    'PostgresConnectionConfig',
    'PostgresRegistryConfig',
    'PostgresSinkConfig',
    'RedisConnectionConfig',
    'RedisRegistryConfig',
    'RedisSinkConfig',
    'ResilienceConfig',
    'RuntimeHealthConfig',
    'SharedConfig',
    'SinksConfig',
    'SourcesConfig',
    'StaticPeerConfig',
    'StaticRegistryConfig',
    'StorageConfig',
    'ThroughputConfig',
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
    'UIReleaseConfig',
    'UITimelineConfig',
    'UITraceConfig',
    'WebAppConfig',
    'WebClientConfig',
    '_deep_merge',
    '_parse_env_overrides',
    'load_config',
    'validate_resilience',
]
