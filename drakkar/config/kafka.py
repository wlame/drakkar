"""Kafka source (consumer) configuration."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from drakkar.kafka_security import KafkaSecurityConfig, validate_client_config


class KafkaConfig(BaseModel):
    """Kafka connection and consumer settings."""

    brokers: str = 'localhost:9092'
    source_topic: str = 'input-events'
    consumer_group: str = 'drakkar-workers'

    # Transport security for the consumer. Also inherited by Kafka sinks and
    # the DLQ producer whose own ``brokers`` field is empty (same cluster =>
    # same credentials); see ``KafkaSinkConfig.security``.
    security: KafkaSecurityConfig = Field(default_factory=KafkaSecurityConfig)

    # Raw librdkafka properties merged after ``security``, for options the
    # typed block does not model. Reserved keys are rejected at load time —
    # see ``drakkar.kafka_security.RESERVED_CLIENT_KEYS``.
    client_config: dict[str, str] = Field(default_factory=dict)

    max_poll_records: int = 100
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 3_000

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
    on_parse_error: Literal['skip', 'dlq', 'raise'] = 'skip'

    # Kafka-UI (https://github.com/provectus/kafka-ui) deep-link config.
    # When both fields are set, the debug UI renders a small Kafka icon
    # next to every <partition:offset> display; the icon opens Kafka-UI
    # filtered on (source_topic, partition, offset) in a new tab.
    # Both must be set for the icon to appear; empty values disable the
    # feature silently.
    ui_url: str = ''
    ui_cluster_name: str = ''

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
    startup_align_enabled: bool = True
    startup_min_wait_seconds: float = Field(default=4.0, ge=0.0)
    startup_align_interval_seconds: int = Field(default=10, ge=1)

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)
