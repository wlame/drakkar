"""Configuration loading for Drakkar framework.

Supports YAML files with environment variable overrides.
Use DK_ prefix with __ for nesting (e.g., DK_KAFKA__BROKERS).

The models are split per domain across this package — ``kafka``, ``sinks``,
``runtime``, ``ui``, ``cache``, ``webapp`` — and assembled by ``root``.
Everything is re-exported here, so ``from drakkar.config import X`` remains
the one import path regardless of which module defines ``X``.
"""

from drakkar.config.cache import CacheConfig, CachePeerSyncConfig
from drakkar.config.kafka import KafkaConfig
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
from drakkar.config.ui import (
    TIMELINE_COLOR_NAMES,
    TIMELINE_NUMERIC_FIELDS,
    TIMELINE_OPS,
    TIMELINE_STRING_FIELDS,
    TimelineColorRule,
    TimelineLabels,
    TimelineRuleCondition,
    UIConfig,
    UIConsumePauseConfig,
    UIProbeDetailsConfig,
    UIRecorderConfig,
    UIReleaseConfig,
    UITimelineConfig,
)
from drakkar.config.webapp import WebAppConfig, WebClientConfig

__all__ = [
    'TIMELINE_COLOR_NAMES',
    'TIMELINE_NUMERIC_FIELDS',
    'TIMELINE_OPS',
    'TIMELINE_STRING_FIELDS',
    'CacheConfig',
    'CachePeerSyncConfig',
    'CircuitBreakerConfig',
    'DLQConfig',
    'DrakkarConfig',
    'ExecutorConfig',
    'FileSinkConfig',
    'HttpSinkConfig',
    'IOConfig',
    'KafkaConfig',
    'KafkaSinkConfig',
    'LoggingConfig',
    'MetricsConfig',
    'MongoSinkConfig',
    'MongoStatementConfig',
    'OffloadConfig',
    'PostgresSinkConfig',
    'RedisSinkConfig',
    'RuntimeHealthConfig',
    'SinksConfig',
    'ThroughputConfig',
    'TimelineColorRule',
    'TimelineLabels',
    'TimelineRuleCondition',
    'UIConfig',
    'UIConsumePauseConfig',
    'UIProbeDetailsConfig',
    'UIRecorderConfig',
    'UIReleaseConfig',
    'UITimelineConfig',
    'WebAppConfig',
    'WebClientConfig',
    '_deep_merge',
    '_parse_env_overrides',
    'load_config',
]
