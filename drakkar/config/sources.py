"""Input sources: the Kafka consumer and the HTTP ingress, as peers.

Each source is optional and enabled explicitly. A block is validated only
when it is enabled; a disabled block reports its problems through
``validation_errors()`` so startup can warn without blocking.
"""

from typing import Literal

from pydantic import BaseModel, Field, model_validator

from drakkar.config.webapp import WebAppConfig


class KafkaSourceConfig(BaseModel):
    """The Kafka consumer as an input source (``sources.kafka``).

    Connection settings (brokers, security) live in ``kafka:``; this block
    holds what only the consumer needs.
    """

    enabled: bool = Field(
        default=False,
        description='Consume the Kafka topic. Off, no consumer group is joined and arrange() is never called.',
    )
    topic: str = Field(default='input-events', description='Kafka topic to consume messages from.')
    consumer_group: str = Field(
        default='drakkar-workers',
        description='Consumer group ID. Workers sharing a group split the source partitions between them.',
    )
    max_poll_records: int = Field(
        default=100,
        description='Maximum messages returned per poll batch. Higher improves throughput; lower reduces latency.',
    )
    max_poll_interval_ms: int = Field(
        default=300_000,
        description=(
            'Maximum time (ms) between poll calls before Kafka considers the consumer dead '
            'and triggers a rebalance. Increase for long-running tasks.'
        ),
    )
    session_timeout_ms: int = Field(
        default=45_000,
        description=(
            'Session timeout (ms) for group membership. Without a heartbeat within this window '
            'the broker removes the consumer from the group.'
        ),
    )
    heartbeat_interval_ms: int = Field(
        default=3_000,
        description='Interval (ms) between heartbeats to the broker. Keep below session_timeout_ms / 3.',
    )
    on_parse_error: Literal['skip', 'dlq', 'raise'] = Field(
        default='skip',
        description=(
            "What to do with a message whose value fails input_model parsing. 'skip' passes it to "
            "arrange() with payload=None and msg.parse_error set; 'dlq' excludes it from arrange(), "
            'writes a ParseFailurePayload to the DLQ topic, and commits once the write is confirmed; '
            "'raise' fails fast — a MessageParseError stops the partition processor."
        ),
    )
    startup_align_enabled: bool = Field(
        default=True,
        description=(
            'Delay the first Kafka subscribe until a shared wall-clock boundary, so a '
            'rolling-deploy fleet converges on one rebalance instead of N. Disable for '
            'snappy single-worker dev runs.'
        ),
    )
    startup_min_wait_seconds: float = Field(
        default=4.0,
        ge=0.0,
        description='Minimum seconds to sleep before aligning — a buffer for slow init (DB connects, cache warm-up).',
    )
    startup_align_interval_seconds: int = Field(
        default=10,
        ge=1,
        description=(
            'Alignment interval in seconds. Workers wake at the next Unix-epoch multiple — '
            'the default 10 aligns on :00/:10/:20/:30/:40/:50 of every minute.'
        ),
    )

    def validation_errors(self) -> list[str]:
        """Problems that block this source when it is enabled. Pure; used by the model validator and by startup warnings."""
        errors: list[str] = []
        if not self.topic.strip():
            errors.append('sources.kafka.topic must be a non-empty string')
        if not self.consumer_group.strip():
            errors.append('sources.kafka.consumer_group must be a non-empty string')
        return errors

    @model_validator(mode='after')
    def _validate_when_enabled(self) -> 'KafkaSourceConfig':
        if self.enabled:
            errors = self.validation_errors()
            if errors:
                raise ValueError('; '.join(errors))
        return self


class SourcesConfig(BaseModel):
    """The ``sources:`` section.

    At least one source must be enabled — enforced by ``DrakkarConfig``,
    not here, so a bare ``SourcesConfig()`` (both sources off) stays
    constructible on its own, e.g. for tests and for reading a partial
    ``sources.*`` block before it is known whether the block will end up
    nested in a full root config.
    """

    kafka: KafkaSourceConfig = Field(default_factory=KafkaSourceConfig)
    http: WebAppConfig = Field(default_factory=WebAppConfig)

    @property
    def enabled_names(self) -> list[str]:
        """Enabled source names in the fixed start order (kafka, then http)."""
        return [name for name in ('kafka', 'http') if getattr(self, name).enabled]
