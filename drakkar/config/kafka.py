"""Kafka source (consumer) configuration."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from drakkar.kafka_security import KafkaSecurityConfig, validate_client_config


class KafkaConfig(BaseModel):
    """Kafka connection and consumer settings."""

    brokers: str = Field(
        default='localhost:9092',
        description=(
            'Kafka bootstrap servers, comma-separated for multiple brokers. '
            'Also the fallback for sink and DLQ brokers left empty.'
        ),
    )
    source_topic: str = Field(
        default='input-events',
        description='Kafka topic to consume messages from.',
    )
    consumer_group: str = Field(
        default='drakkar-workers',
        description='Consumer group ID. Workers sharing a group split the source partitions between them.',
    )

    # Transport security for the consumer. Also inherited by Kafka sinks and
    # the DLQ producer whose own ``brokers`` field is empty (same cluster =>
    # same credentials); see ``KafkaSinkConfig.security``.
    security: KafkaSecurityConfig = Field(default_factory=KafkaSecurityConfig)

    # Raw librdkafka properties merged after ``security``, for options the
    # typed block does not model. Reserved keys are rejected at load time —
    # see ``drakkar.kafka_security.RESERVED_CLIENT_KEYS``.
    client_config: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Raw librdkafka properties merged after the security block, for options '
            'the typed fields do not model. Reserved keys (bootstrap.servers, group.id, '
            'enable.auto.commit, partition.assignment.strategy) are rejected at config load.'
        ),
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

    # Policy for source messages whose value fails input_model parsing:
    #   - 'skip'  — message flows to arrange() with payload=None (and
    #               msg.parse_error set); the handler decides what to do.
    #               Matches pre-policy behavior, now logged + counted.
    #   - 'dlq'   — message is excluded from arrange(), a ParseFailurePayload
    #               is written to the DLQ topic, and the offset commits once
    #               the DLQ write is confirmed. A failed DLQ write stalls the
    #               offset (redelivery on restart) instead of losing data.
    #   - 'raise' — fail fast: a MessageParseError propagates and stops the
    #               partition processor. Use when a parse failure means the
    #               deployment is broken (schema mismatch) and processing
    #               must not continue past it.
    on_parse_error: Literal['skip', 'dlq', 'raise'] = Field(
        default='skip',
        description=(
            "What to do with a message whose value fails input_model parsing. 'skip' passes it to "
            "arrange() with payload=None and msg.parse_error set; 'dlq' excludes it from arrange(), "
            'writes a ParseFailurePayload to the DLQ topic, and commits once the write is confirmed; '
            "'raise' fails fast — a MessageParseError stops the partition processor."
        ),
    )

    # Kafka-UI (https://github.com/provectus/kafka-ui) deep-link config.
    # When both fields are set, the debug UI renders a small Kafka icon
    # next to every <partition:offset> display; the icon opens Kafka-UI
    # filtered on (source_topic, partition, offset) in a new tab.
    # Both must be set for the icon to appear; empty values disable the
    # feature silently.
    ui_url: str = Field(
        default='',
        description=(
            'Base URL of a Kafka-UI instance for deep links. Together with ui_cluster_name, '
            'renders an icon next to every partition:offset that opens Kafka-UI filtered on '
            'that message; empty disables the links.'
        ),
    )
    ui_cluster_name: str = Field(
        default='',
        description=(
            'Cluster name as configured in Kafka-UI, used in the deep links. '
            'Both this and ui_url must be set for the links to appear.'
        ),
    )

    # Staggered startup: delay the Kafka subscribe until the next wall-clock
    # alignment boundary. During a rolling deploy, workers come up one at
    # a time and each triggers a Kafka consumer-group rebalance — which
    # stalls consumption on all other workers. Aligning startup to shared
    # boundaries lets a fleet converge on a single rebalance instead of N.
    #
    # Sequence: wait ``startup_min_wait_seconds`` (buffer for slow init),
    # then sleep until the next wall-clock instant whose Unix-epoch seconds
    # are a multiple of ``startup_align_interval_seconds`` (default :00,
    # :10, :20, :30, :40, :50 of every minute in UTC — which maps 1:1 to
    # local wall-clock seconds since timezone offsets are whole-minute).
    # Disable with ``startup_align_enabled=false`` for snappy dev iteration.
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

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)
