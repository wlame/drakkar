"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DK_ prefix with __ for nesting (e.g., DK_EXECUTOR__MAX_EXECUTORS).

The models are split per domain across this package — ``sinks``,
``sources``, ``runtime``, ``api``, ``ui``, ``recorder``, ``cache``, ``storage``, ``autodiscover``, ``connections``, ``webapp``
— and assembled by ``root``. ``base`` holds their base class, ``units`` the types
for values with units, and ``strict`` the check for unknown keys.
Everything is re-exported here, so ``from drakkar.config import X`` remains
the one import path regardless of which module defines ``X``.
"""

from drakkar.config.api import APIConfig
from drakkar.config.autodiscover import AUTODISCOVER_PROVIDERS, CONNECTION_PROVIDERS, AutodiscoverConfig
from drakkar.config.base import ConfigModel
from drakkar.config.cache import CacheConfig, CacheExchangeConfig
from drakkar.config.connections import (
    ConnectionsConfig,
    ConsulConnectionConfig,
    KafkaConnectionConfig,
    MongoConnectionConfig,
    PostgresConnectionConfig,
    RedisConnectionConfig,
)
from drakkar.config.recorder import (
    DEFAULT_ARCHIVE_RETENTION_DAYS,
    RecorderAnnotationsConfig,
    RecorderArchiveConfig,
    RecorderCaptureConfig,
    RecorderConfig,
    RecorderMemoryConfig,
    RecorderPostgresConfig,
    RecorderSearchConfig,
    RecorderSqliteConfig,
)
from drakkar.config.resilience import ResilienceConfig, validate_resilience
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
    DLQConfig,
    FileSinkConfig,
    HttpSinkConfig,
    KafkaSinkConfig,
    MongoSinkConfig,
    MongoStatementConfig,
    PostgresSinkConfig,
    RedisSinkConfig,
    SinkDefaults,
    SinkInstanceConfig,
    SinkPolicy,
    SinksConfig,
)
from drakkar.config.sources import KafkaSourceConfig, SourcesConfig, StartupAlignConfig
from drakkar.config.storage import StorageConfig
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
    UIProbeConfig,
    UIPrometheusConfig,
    UIReleaseConfig,
    UITimelineConfig,
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
    'AUTODISCOVER_PROVIDERS',
    'BUILTIN_LINK_BASES',
    'CONNECTION_PROVIDERS',
    'DEFAULT_ARCHIVE_RETENTION_DAYS',
    'TIMELINE_COLOR_NAMES',
    'TIMELINE_NUMERIC_FIELDS',
    'TIMELINE_OPS',
    'TIMELINE_STRING_FIELDS',
    'APIConfig',
    'AutodiscoverConfig',
    'ByteSize',
    'CacheConfig',
    'CacheExchangeConfig',
    'ConfigModel',
    'ConnectionsConfig',
    'ConsulConnectionConfig',
    'DLQConfig',
    'DocsAnchor',
    'DocsAnchorMatch',
    'DrakkarConfig',
    'Duration',
    'ExecutorConfig',
    'FileSinkConfig',
    'HttpSinkConfig',
    'IOConfig',
    'KafkaConnectionConfig',
    'KafkaSinkConfig',
    'KafkaSourceConfig',
    'LoggingConfig',
    'MetricsConfig',
    'MongoConnectionConfig',
    'MongoSinkConfig',
    'MongoStatementConfig',
    'OffloadConfig',
    'PostgresConnectionConfig',
    'PostgresSinkConfig',
    'RecorderAnnotationsConfig',
    'RecorderArchiveConfig',
    'RecorderCaptureConfig',
    'RecorderConfig',
    'RecorderMemoryConfig',
    'RecorderPostgresConfig',
    'RecorderSearchConfig',
    'RecorderSqliteConfig',
    'RedisConnectionConfig',
    'RedisSinkConfig',
    'ResilienceConfig',
    'RuntimeHealthConfig',
    'SinkDefaults',
    'SinkInstanceConfig',
    'SinkPolicy',
    'SinksConfig',
    'SourcesConfig',
    'StartupAlignConfig',
    'StorageConfig',
    'TimelineColorRule',
    'TimelineEventType',
    'TimelineLabels',
    'TimelineRuleCondition',
    'UIConfig',
    'UIConsumePauseConfig',
    'UIDocsConfig',
    'UIProbeConfig',
    'UIPrometheusConfig',
    'UIReleaseConfig',
    'UITimelineConfig',
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
