"""The start checks of the internal topics of the split feature, and the real broker queries they use."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from confluent_kafka.admin import AdminClient, ConfigResource

from drakkar.config import DrakkarConfig
from drakkar.config.connections import KafkaConnectionConfig
from drakkar.config.units import format_duration
from drakkar.kafka_security import merge_client_config
from drakkar.models import ConfigurationError

ADMIN_TIMEOUT_SECONDS = 10.0
RETENTION_KEY = 'retention.ms'
CLEANUP_POLICY_KEY = 'cleanup.policy'
COMPACT_POLICY = 'compact'
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


def _status_retention_warning(topic: str, retention_ms: int, result_retention_seconds: int) -> str:
    return (
        f"split.http.status_topic '{topic}' keeps records for {format_duration(retention_ms)}, "
        f'less than split.http.result_retention ({format_duration(result_retention_seconds * MS_PER_SECOND)})'
    )


def _compact_policy_warning(topic: str, policy: str) -> str:
    return (
        f"split.http.status_topic '{topic}' has cleanup.policy={policy}; use delete: "
        'compaction can keep an older status of a job and drop the end state'
    )


def _unreadable_policy_warning(topic: str, error: Exception) -> str:
    return f"split: the cleanup.policy of the topic '{topic}' could not be read: {error}"


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
        except Exception as exc:
            # Any failed read, a malformed value included, is a warning. Often the worker may not describe topic configs: this check only warns, so it never stops the start.
            warnings.append(_unreadable_retention_warning(topic, exc))
            continue
        if retention_ms is None or retention_ms == NO_RETENTION_LIMIT_MS:
            continue
        if retention_ms < job_timeout * MS_PER_SECOND:
            warnings.append(_retention_warning(topic, retention_ms, job_timeout))
    return warnings


async def check_status_topic(
    topic: str,
    *,
    result_retention: int,
    metadata: Callable[[], Awaitable[Mapping[str, int]]],
    retention: Callable[[str], Awaitable[int | None]],
    cleanup_policy: Callable[[str], Awaitable[str | None]],
) -> list[str]:
    """Check the status topic of HTTP split jobs; any partition count is fine.

    Raises ``ConfigurationError`` when the topic does not exist. Returns the
    warnings: a retention shorter than ``result_retention`` (seconds), a
    cleanup policy with compaction, and one for each setting that could not
    be read (that never raises).
    """
    if topic not in await metadata():
        raise ConfigurationError(_missing_topic_message(topic))
    warnings = []
    try:
        retention_ms = await retention(topic)
    except Exception as exc:
        # As for the other topics: the worker may not describe topic configs, and this check only warns.
        warnings.append(_unreadable_retention_warning(topic, exc))
    else:
        limited = retention_ms is not None and retention_ms != NO_RETENTION_LIMIT_MS
        if limited and retention_ms < result_retention * MS_PER_SECOND:
            warnings.append(_status_retention_warning(topic, retention_ms, result_retention))
    try:
        policy = await cleanup_policy(topic)
    except Exception as exc:
        warnings.append(_unreadable_policy_warning(topic, exc))
    else:
        if policy is not None and COMPACT_POLICY in (part.strip() for part in policy.split(',')):
            warnings.append(_compact_policy_warning(topic, policy))
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


def _topic_config_value(connection: KafkaConnectionConfig, admin_factory: Callable[[dict], Any], topic: str, key: str):
    """The value of one config entry of ``topic``, or None when the broker names none; a blocking call."""
    admin = admin_factory(_admin_config(connection))
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
    entries = admin.describe_configs([resource])[resource].result(timeout=ADMIN_TIMEOUT_SECONDS)
    entry = entries.get(key)
    return entry.value if entry is not None else None


def topic_retention_ms(
    connection: KafkaConnectionConfig, *, admin_factory: Callable[[dict], Any] = AdminClient
) -> Callable[[str], Awaitable[int | None]]:
    """A ``retention`` callable for ``check_split_topics``: the blocking client call runs on a thread.

    A read the broker refuses raises ``KafkaException`` and a malformed value (also one below -1) ``ValueError``;
    ``check_split_topics`` turns any failed read into a warning.
    """

    def read(topic: str) -> int | None:
        value = _topic_config_value(connection, admin_factory, topic, RETENTION_KEY)
        if value is None:
            return None
        retention_ms = int(value)
        # A broker refuses a value below -1, so such a value is malformed: a read that failed.
        if retention_ms < NO_RETENTION_LIMIT_MS:
            raise ValueError(f'{RETENTION_KEY} is below {NO_RETENTION_LIMIT_MS}: {retention_ms}')
        return retention_ms

    async def retention(topic: str) -> int | None:
        return await asyncio.to_thread(read, topic)

    return retention


def topic_cleanup_policy(
    connection: KafkaConnectionConfig, *, admin_factory: Callable[[dict], Any] = AdminClient
) -> Callable[[str], Awaitable[str | None]]:
    """A ``cleanup_policy`` callable for ``check_status_topic``: the blocking client call runs on a thread.

    A read the broker refuses raises ``KafkaException``; ``check_status_topic`` turns it into a warning.
    """

    async def cleanup_policy(topic: str) -> str | None:
        return await asyncio.to_thread(_topic_config_value, connection, admin_factory, topic, CLEANUP_POLICY_KEY)

    return cleanup_policy
