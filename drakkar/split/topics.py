"""The start check of the two internal topics of the split feature, and the real broker queries it uses."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource

from drakkar.config import DrakkarConfig
from drakkar.config.connections import KafkaConnectionConfig
from drakkar.config.units import format_duration
from drakkar.kafka_security import merge_client_config
from drakkar.models import ConfigurationError

ADMIN_TIMEOUT_SECONDS = 10.0
RETENTION_KEY = 'retention.ms'
NO_RETENTION_LIMIT_MS = -1
MS_PER_SECOND = 1000


def _missing_topic_message(topic: str) -> str:
    return f"split: the topic '{topic}' does not exist; create it before the worker starts"


def _partition_count_message(results_topic: str, results_count: int, source_topic: str, source_count: int) -> str:
    return (
        f"split: the results topic '{results_topic}' has {results_count} partitions "
        f"and the source topic '{source_topic}' has {source_count}; they must be equal"
    )


def _retention_warning(topic: str, retention_ms: int, job_timeout_seconds: int) -> str:
    return (
        f"split: the retention of the topic '{topic}' ({format_duration(retention_ms)}) "
        f'is shorter than split.job_timeout ({format_duration(job_timeout_seconds * MS_PER_SECOND)})'
    )


def _unreadable_retention_warning(topic: str, error: Exception) -> str:
    return f"split: the retention of the topic '{topic}' could not be read: {error}"


async def check_split_topics(
    config: DrakkarConfig,
    *,
    metadata: Callable[[], Awaitable[Mapping[str, int]]],
    retention: Callable[[str], Awaitable[int | None]],
) -> list[str]:
    """Check that the parts and results topics exist and fit the source topic.

    ``metadata`` gives the partition count of each topic that exists; ``retention`` gives the
    ``retention.ms`` of one topic, ``-1`` or ``None`` when there is no limit. Raises
    ``ConfigurationError`` for a missing topic or a results topic with another partition count than
    the source topic. Returns the warnings: a retention shorter than ``split.job_timeout``, and one for
    each topic whose retention could not be read (that never raises).
    """
    source_topic = config.sources.kafka.topic
    parts_topic = config.resolved_split_parts_topic
    results_topic = config.resolved_split_results_topic
    counts = await metadata()
    for topic in (parts_topic, results_topic, source_topic):
        if topic not in counts:
            raise ConfigurationError(_missing_topic_message(topic))
    if counts[results_topic] != counts[source_topic]:
        raise ConfigurationError(
            _partition_count_message(results_topic, counts[results_topic], source_topic, counts[source_topic])
        )
    return await _retention_warnings(config, (parts_topic, results_topic), retention)


async def _retention_warnings(
    config: DrakkarConfig, topics: tuple[str, ...], retention: Callable[[str], Awaitable[int | None]]
) -> list[str]:
    job_timeout = config.split.job_timeout
    if job_timeout == 0:
        return []
    warnings = []
    for topic in topics:
        try:
            retention_ms = await retention(topic)
        except (KafkaException, TimeoutError) as exc:
            # Often the worker may not describe topic configs: this check only warns, so it never stops the start.
            warnings.append(_unreadable_retention_warning(topic, exc))
            continue
        if retention_ms is None or retention_ms == NO_RETENTION_LIMIT_MS:
            continue
        if retention_ms < job_timeout * MS_PER_SECOND:
            warnings.append(_retention_warning(topic, retention_ms, job_timeout))
    return warnings


def _admin_config(connection: KafkaConnectionConfig) -> dict[str, Any]:
    return merge_client_config({'bootstrap.servers': connection.brokers}, connection.security, connection.client_config)


def topic_partition_counts(
    connection: KafkaConnectionConfig, *, admin_factory: Callable[[dict], Any] = AdminClient
) -> Callable[[], Awaitable[Mapping[str, int]]]:
    """A ``metadata`` callable for ``check_split_topics``: the blocking client call runs on a thread."""

    def read() -> dict[str, int]:
        admin = admin_factory(_admin_config(connection))
        topics = admin.list_topics(timeout=ADMIN_TIMEOUT_SECONDS).topics
        return {name: len(topic.partitions) for name, topic in topics.items() if topic.error is None}

    async def counts() -> Mapping[str, int]:
        return await asyncio.to_thread(read)

    return counts


def topic_retention_ms(
    connection: KafkaConnectionConfig, *, admin_factory: Callable[[dict], Any] = AdminClient
) -> Callable[[str], Awaitable[int | None]]:
    """A ``retention`` callable for ``check_split_topics``: the blocking client call runs on a thread.

    A read the broker refuses raises ``KafkaException``; ``check_split_topics`` turns it into a warning.
    """

    def read(topic: str) -> int | None:
        admin = admin_factory(_admin_config(connection))
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        entries = admin.describe_configs([resource])[resource].result(timeout=ADMIN_TIMEOUT_SECONDS)
        entry = entries.get(RETENTION_KEY)
        return int(entry.value) if entry is not None and entry.value is not None else None

    async def retention(topic: str) -> int | None:
        return await asyncio.to_thread(read, topic)

    return retention
