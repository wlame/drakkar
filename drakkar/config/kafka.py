"""Kafka cluster connection configuration, shared by the Kafka source, Kafka sinks and the DLQ."""

from pydantic import BaseModel, Field, field_validator

from drakkar.kafka_security import KafkaSecurityConfig, validate_client_config


class KafkaConfig(BaseModel):
    """Kafka connection settings: brokers, transport security, raw client properties, Kafka-UI links."""

    brokers: str = Field(
        default='localhost:9092',
        description=(
            'Kafka bootstrap servers, comma-separated. Used by the Kafka source, and the '
            'fallback for sink and DLQ brokers left empty.'
        ),
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

    # Kafka-UI (https://github.com/provectus/kafka-ui) deep-link config.
    # When both fields are set, the debug UI renders a small Kafka icon
    # next to every <partition:offset> display; the icon opens Kafka-UI
    # filtered on (the message's topic, partition, offset) in a new tab.
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

    @field_validator('client_config')
    @classmethod
    def _reject_reserved_client_keys(cls, v: dict[str, str]) -> dict[str, str]:
        return validate_client_config(v)
