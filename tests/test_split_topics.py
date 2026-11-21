"""The start check of the split topics and the broker queries behind it."""

from concurrent.futures import Future
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from drakkar.config import DrakkarConfig
from drakkar.config.connections import KafkaConnectionConfig
from drakkar.models import ConfigurationError
from drakkar.split.topics import check_split_topics, topic_partition_counts, topic_retention_ms
from tests.conftest import kafka_connections

PARTS = 'items_g_split_parts'
RESULTS = 'items_g_split_results'
HOUR_MS = 3_600_000


def make_config(**split) -> DrakkarConfig:
    return DrakkarConfig.model_validate(
        {
            'connections': kafka_connections(),
            'sources': {'kafka': {'enabled': True, 'topic': 'items', 'consumer_group': 'g'}},
            'ui': {'release': {'enabled': False}},
            'split': {'enabled': True, **split},
        }
    )


def fixed(counts: dict[str, int], retentions: dict[str, int | None] | None = None):
    async def metadata():
        return counts

    async def retention(topic: str):
        return (retentions or {}).get(topic)

    return {'metadata': metadata, 'retention': retention}


async def test_matching_topics_pass_with_no_warning():
    assert await check_split_topics(make_config(), **fixed({'items': 4, PARTS: 8, RESULTS: 4})) == []


@pytest.mark.parametrize('missing', [PARTS, RESULTS, 'items'])
async def test_missing_topic_fails(missing):
    counts = {name: 4 for name in ('items', PARTS, RESULTS) if name != missing}
    with pytest.raises(ConfigurationError) as raised:
        await check_split_topics(make_config(), **fixed(counts))
    assert str(raised.value) == f"split: the topic '{missing}' does not exist; create it before the worker starts"


async def test_results_topic_with_another_partition_count_fails():
    with pytest.raises(ConfigurationError) as raised:
        await check_split_topics(make_config(), **fixed({'items': 4, PARTS: 4, RESULTS: 3}))
    assert str(raised.value) == (
        f"split: the results topic '{RESULTS}' has 3 partitions and the source topic 'items' has 4; they must be equal"
    )


async def test_short_retention_gives_a_warning_for_each_topic():
    warnings = await check_split_topics(
        make_config(job_timeout='24h'),
        **fixed({'items': 1, PARTS: 1, RESULTS: 1}, {PARTS: HOUR_MS, RESULTS: 25 * HOUR_MS}),
    )
    assert warnings == [f"split: the retention of the topic '{PARTS}' (1h) is shorter than split.job_timeout (1d)"]


@pytest.mark.parametrize('retention', [-1, None])
async def test_no_limit_gives_no_warning(retention):
    counts = {'items': 1, PARTS: 1, RESULTS: 1}
    assert await check_split_topics(make_config(), **fixed(counts, {PARTS: retention, RESULTS: retention})) == []


async def test_no_job_timeout_gives_no_warning():
    config = make_config(job_timeout='0', idle_timeout='1h')
    counts = {'items': 1, PARTS: 1, RESULTS: 1}
    assert await check_split_topics(config, **fixed(counts, {PARTS: 1, RESULTS: 1})) == []


class FakeAdmin:
    """The admin client calls that the real queries make."""

    def __init__(self, config: dict) -> None:
        self.config = config

    def list_topics(self, timeout: float):
        topics = {
            'a': SimpleNamespace(error=None, partitions={0: 1, 1: 1}),
            'broken': SimpleNamespace(error='x', partitions={}),
        }
        return SimpleNamespace(topics=topics)

    def describe_configs(self, resources):
        future: Future = Future()
        topic = resources[0].name
        if topic == 'denied':
            future.set_exception(KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)))
        else:
            entries = (
                {}
                if topic == 'bare'
                else {'retention.ms': SimpleNamespace(value='abc' if topic == 'malformed' else '60000')}
            )
            future.set_result(entries)
        return {resources[0]: future}


async def test_partition_counts_come_from_the_topics_that_have_no_error():
    connection = KafkaConnectionConfig(brokers='broker:9092')
    counts = await topic_partition_counts(connection, admin_factory=FakeAdmin)()
    assert counts == {'a': 2}


async def test_retention_is_read_as_milliseconds_and_none_when_unset():
    read = topic_retention_ms(KafkaConnectionConfig(), admin_factory=FakeAdmin)
    assert await read('x') == 60000
    assert await read('bare') is None


async def test_retention_read_that_the_broker_refuses_raises_the_client_error():
    read = topic_retention_ms(KafkaConnectionConfig(), admin_factory=FakeAdmin)
    with pytest.raises(KafkaException):
        await read('denied')


@pytest.mark.parametrize(
    'error', [KafkaException('no right to describe configs'), TimeoutError('slow'), ValueError('invalid literal')]
)
async def test_unreadable_retention_is_a_warning_and_does_not_stop_the_start(error):
    async def metadata():
        return {'items': 1, PARTS: 1, RESULTS: 1}

    async def retention(topic: str):
        if topic == PARTS:
            raise error
        return HOUR_MS

    warnings = await check_split_topics(make_config(job_timeout='24h'), metadata=metadata, retention=retention)
    assert warnings == [
        f"split: the retention of the topic '{PARTS}' could not be read: {error}",
        f"split: the retention of the topic '{RESULTS}' (1h) is shorter than split.job_timeout (1d)",
    ]


async def test_malformed_retention_value_from_the_real_query_is_a_warning():
    read = topic_retention_ms(KafkaConnectionConfig(), admin_factory=FakeAdmin)

    async def metadata():
        return {'items': 1, PARTS: 1, RESULTS: 1}

    async def retention(topic: str):
        return await read('malformed' if topic == PARTS else topic)

    warnings = await check_split_topics(make_config(job_timeout='24h'), metadata=metadata, retention=retention)
    assert warnings[0].startswith(f"split: the retention of the topic '{PARTS}' could not be read: ")
