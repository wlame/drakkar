"""Input sources: the Kafka consumer and the HTTP ingress, as peers.

Each source is optional and enabled explicitly. A block is validated only
when it is enabled; a disabled block reports its problems through
``validation_errors()`` so startup can warn without blocking.
"""

from typing import Literal

from pydantic import Field, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.units import Duration, WholeSeconds, seconds_text
from drakkar.config.webapp import WebAppConfig


class StartupAlignConfig(ConfigModel):
    """Delay the first Kafka subscribe until a shared wall-clock boundary (``sources.kafka.startup_align``)."""

    enabled: bool = Field(
        default=True,
        description=(
            'Delay the first Kafka subscribe until a shared wall-clock boundary, so a '
            'rolling-deploy fleet converges on one rebalance instead of N. Disable for '
            'snappy single-worker dev runs.'
        ),
    )
    min_wait: Duration = Field(
        default=4.0,
        description='Minimum time to sleep before aligning: a buffer for slow init (DB connects, cache warm-up).',
    )
    interval: WholeSeconds = Field(
        default=10,
        description=(
            'Alignment interval. Workers wake at the next Unix-epoch multiple: '
            'the default 10s aligns on :00/:10/:20/:30/:40/:50 of every minute.'
        ),
    )


class KafkaDetachConfig(ConfigModel):
    """Leave the consumer group for a bounded time and join it again (``sources.kafka.detach``).

    A lease slot in the autodiscover store limits how many workers of one group
    are away at once.
    """

    enabled: bool = Field(
        default=False,
        description=(
            'Let this worker leave its consumer group for a bounded time and join it again without a '
            'restart. Needs autodiscover: the budget of detached workers is kept in its store.'
        ),
    )
    max_detached: int = Field(
        default=1,
        ge=1,
        le=100,
        description=(
            'How many workers of one consumer group may be detached at once, from 1 to 100. '
            'Workers of one group should use the same value.'
        ),
    )
    min_attached: int = Field(
        default=1,
        ge=1,
        le=1000,
        description=(
            'How many workers of one consumer group must stay attached, from 1 to 1000. '
            'Workers of one group should use the same value.'
        ),
    )
    default_duration: Duration = Field(
        default=3600.0,
        description='How long a detach lasts when the request gives no duration; from 10s to max_duration.',
    )
    max_duration: Duration = Field(
        default=86400.0,
        description='The longest detach a request may ask for, from 10s to 7d.',
    )
    cooldown: Duration = Field(
        default=300.0,
        description=(
            'The shortest time between the return of a detach, handler or operator, and the next '
            'handler detach, from 0 (none) to 1d. Every attach after a detach starts the time; it binds '
            'handler detaches only, and an operator detach does not wait for it.'
        ),
    )
    lease: WholeSeconds = Field(
        default=60,
        description=(
            'The lease of the slot in the autodiscover store, from 40s to 1d. A detached worker '
            'attaches again when its lease was not confirmed for this long.'
        ),
    )

    def check_durations(self) -> None:
        """Refuse a default duration that a request could not ask for.

        The root config calls this after the range check of each value, at load
        and again after ``on_startup``, so both give the same message.

        Raises:
            ValueError: ``default_duration`` is above ``max_duration``.
        """
        if self.default_duration > self.max_duration:
            raise ValueError(
                f'sources.kafka.detach.default_duration ({seconds_text(self.default_duration)}) must be <= '
                f'sources.kafka.detach.max_duration ({seconds_text(self.max_duration)})'
            )


class KafkaSourceConfig(ConfigModel):
    """The Kafka consumer as an input source (``sources.kafka``).

    ``connection`` names the cluster under ``connections.kafka``; this block
    holds what only the consumer needs.
    """

    enabled: bool = Field(
        default=False,
        description='Consume the Kafka topic. Off, no consumer group is joined and arrange() is never called.',
    )
    connection: str = Field(default='main', description='The Kafka cluster to read: a name under connections.kafka.')
    topic: str = Field(default='input-events', description='Kafka topic to consume messages from.')
    consumer_group: str = Field(
        default='drakkar-workers',
        description='Consumer group ID. Workers sharing a group split the source partitions between them.',
    )
    max_poll_records: int = Field(
        default=100,
        description='Maximum messages returned per poll batch. Higher improves throughput; lower reduces latency.',
    )
    max_queued_messages: int = Field(
        default=1000,
        ge=1,
        description=(
            'Kafka fetching pauses when this many polled messages wait in partition queues, and '
            'resumes at half of it. Must be at least max_poll_records. Messages of a dead partition '
            'do not count; a dead or stalled partition stays paused.'
        ),
    )
    max_poll_interval: Duration = Field(
        default=300.0,
        description=(
            'Maximum time between poll calls before Kafka considers the consumer dead '
            'and triggers a rebalance. Increase for long-running tasks.'
        ),
    )
    session_timeout: Duration = Field(
        default=45.0,
        description=(
            'Session timeout for group membership. Without a heartbeat within this window '
            'the broker removes the consumer from the group.'
        ),
    )
    heartbeat_interval: Duration = Field(
        default=3.0,
        description='Interval between heartbeats to the broker. Keep below session_timeout / 3.',
    )
    on_parse_error: Literal['skip', 'dlq', 'raise'] = Field(
        default='skip',
        description=(
            "What to do with a message whose value fails input_model parsing. 'skip' passes it to "
            "arrange() with payload=None and msg.parse_error set; 'dlq' excludes it from arrange(), "
            'writes the message to the DLQ topic as a record with the dk-stage: parse header, and commits '
            'once the write is confirmed; '
            "'raise' fails fast — a MessageParseError stops the partition processor."
        ),
    )
    startup_align: StartupAlignConfig = Field(default_factory=StartupAlignConfig)
    detach: KafkaDetachConfig = Field(default_factory=KafkaDetachConfig)

    def validation_errors(self) -> list[str]:
        """Problems that block this source when it is enabled. Pure; used by the model validator and by startup warnings."""
        errors: list[str] = []
        if not self.topic.strip():
            errors.append('sources.kafka.topic must be a non-empty string')
        if not self.consumer_group.strip():
            errors.append('sources.kafka.consumer_group must be a non-empty string')
        if self.max_queued_messages < self.max_poll_records:
            errors.append(
                f'sources.kafka.max_queued_messages ({self.max_queued_messages}) must be >= '
                f'sources.kafka.max_poll_records ({self.max_poll_records})'
            )
        return errors

    @model_validator(mode='after')
    def _validate_when_enabled(self) -> 'KafkaSourceConfig':
        if self.enabled:
            errors = self.validation_errors()
            if errors:
                raise ValueError('; '.join(errors))
        return self


class SourcesConfig(ConfigModel):
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

    @property
    def delivers_to_sinks(self) -> bool:
        """Whether results of an enabled source go to the sinks.

        The Kafka source always delivers to sinks. The HTTP source does so
        only with ``deliver_to_sinks``; otherwise the handler's response is
        the whole result, and a worker with only that source needs no sink.
        """
        return self.kafka.enabled or (self.http.enabled and self.http.deliver_to_sinks)
