"""Tests for Drakkar Kafka consumer wrapper."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from confluent_kafka import OFFSET_INVALID, KafkaError, TopicPartition
from structlog.testing import capture_logs

from drakkar.config import KafkaConfig, KafkaSourceConfig
from drakkar.consumer import CONSUMER_MAX_WORKERS, LAG_QUERY_TIMEOUT_SECONDS, KafkaConsumer
from drakkar.metrics import consumer_errors


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(brokers='localhost:9092')


@pytest.fixture
def kafka_source_config() -> KafkaSourceConfig:
    return KafkaSourceConfig(
        enabled=True,
        topic='test-source',
        consumer_group='test-group',
        max_poll_records=10,
    )


def make_mock_message(topic='test-source', partition=0, offset=0, key=b'k', value=b'v', timestamp=1000):
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = key
    msg.value.return_value = value
    msg.timestamp.return_value = (1, timestamp)
    return msg


def make_error_message(error_code):
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = error_code
    msg.error.return_value = err
    return msg


@patch('drakkar.consumer.AIOConsumer')
def test_consumer_creation(mock_cls, kafka_config, kafka_source_config):
    _consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    mock_cls.assert_called_once()
    call_args = mock_cls.call_args[0][0]
    assert call_args['bootstrap.servers'] == 'localhost:9092'
    assert call_args['group.id'] == 'test-group'
    assert call_args['enable.auto.commit'] is False
    assert call_args['partition.assignment.strategy'] == 'cooperative-sticky'


@patch('drakkar.consumer.AIOConsumer')
async def test_consumer_subscribe(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    await consumer.subscribe()

    mock_inner.subscribe.assert_called_once()
    call_args = mock_inner.subscribe.call_args
    assert call_args[0][0] == ['test-source']


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_returns_messages(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_mock_message(partition=0, offset=10, value=b'msg1'),
        make_mock_message(partition=1, offset=20, value=b'msg2'),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    messages = await consumer.poll_batch(max_messages=10, timeout=0.1)

    assert len(messages) == 2
    assert messages[0].partition == 0
    assert messages[0].offset == 10
    assert messages[0].value == b'msg1'
    assert messages[1].partition == 1


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_empty(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_inner.consume.return_value = []
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    messages = await consumer.poll_batch(timeout=0.1)
    assert messages == []


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_skips_partition_eof(mock_cls, kafka_config, kafka_source_config):
    from confluent_kafka import KafkaError

    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_error_message(KafkaError._PARTITION_EOF),
        make_mock_message(partition=0, offset=5, value=b'valid'),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    messages = await consumer.poll_batch(timeout=0.1)
    assert len(messages) == 1
    assert messages[0].value == b'valid'


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_logs_errors(mock_cls, kafka_config, kafka_source_config):
    from confluent_kafka import KafkaError

    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_error_message(KafkaError._ALL_BROKERS_DOWN),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    with capture_logs() as cap:
        messages = await consumer.poll_batch(timeout=0.1)

    assert messages == []
    warnings = [e for e in cap if e['event'] == 'consumer_error']
    assert len(warnings) == 1
    assert warnings[0]['log_level'] == 'warning'


@patch('drakkar.consumer.AIOConsumer')
async def test_commit_offsets(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    await consumer.commit({0: 100, 1: 200})

    mock_inner.commit.assert_called_once()
    call_kwargs = mock_inner.commit.call_args[1]
    assert len(call_kwargs['offsets']) == 2
    assert call_kwargs['asynchronous'] is False


@patch('drakkar.consumer.AIOConsumer')
async def test_on_assign_callback(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    assigned = []
    consumer = KafkaConsumer(
        connection=kafka_config, source=kafka_source_config, on_assign=lambda parts: assigned.extend(parts)
    )
    await consumer.subscribe()

    # get the async callback registered with subscribe
    call_kwargs = mock_inner.subscribe.call_args[1]
    assign_cb = call_kwargs['on_assign']
    from confluent_kafka import TopicPartition

    await assign_cb(mock_inner, [TopicPartition('test-source', 0), TopicPartition('test-source', 1)])
    assert assigned == [0, 1]


@patch('drakkar.consumer.AIOConsumer')
async def test_on_revoke_callback(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    revoked = []

    # The revoke callback is awaited: the rebalance must not complete until
    # the worker has drained and committed the revoked partitions.
    async def on_revoke(parts):
        revoked.extend(parts)

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config, on_revoke=on_revoke)
    await consumer.subscribe()

    call_kwargs = mock_inner.subscribe.call_args[1]
    revoke_cb = call_kwargs['on_revoke']
    from confluent_kafka import TopicPartition

    await revoke_cb(mock_inner, [TopicPartition('test-source', 3)])
    assert revoked == [3]


@patch('drakkar.consumer.AIOConsumer')
async def test_pause_partitions(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    await consumer.pause([0, 3])

    mock_inner.pause.assert_called_once()
    tps = mock_inner.pause.call_args[0][0]
    assert len(tps) == 2
    assert tps[0].partition == 0
    assert tps[1].partition == 3


@patch('drakkar.consumer.AIOConsumer')
async def test_resume_partitions(mock_cls, kafka_config, kafka_source_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    await consumer.resume([1, 5])

    mock_inner.resume.assert_called_once()
    tps = mock_inner.resume.call_args[0][0]
    assert len(tps) == 2


@patch('drakkar.consumer.AIOConsumer')
async def test_get_total_lag(mock_cls, kafka_config, kafka_source_config):
    from confluent_kafka import TopicPartition

    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    # committed returns TopicPartitions with offsets
    tp0 = TopicPartition(kafka_source_config.topic, 0, 90)
    tp1 = TopicPartition(kafka_source_config.topic, 1, 80)
    mock_inner.committed.return_value = [tp0, tp1]

    # watermarks: (low, high)
    async def fake_watermarks(tp, *, timeout=None):
        assert timeout == LAG_QUERY_TIMEOUT_SECONDS, 'watermark query must carry the lag timeout'
        if tp.partition == 0:
            return (0, 100)  # lag = 100 - 90 = 10
        return (0, 95)  # lag = 95 - 80 = 15

    mock_inner.get_watermark_offsets.side_effect = fake_watermarks

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    total = await consumer.get_total_lag([0, 1])
    assert total == 25  # 10 + 15


@patch('drakkar.consumer.AIOConsumer')
async def test_get_total_lag_empty_partitions(mock_cls, kafka_config, kafka_source_config):
    """get_total_lag returns 0 immediately when partition list is empty."""
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    total = await consumer.get_total_lag([])
    assert total == 0
    mock_inner.committed.assert_not_called()


@patch('drakkar.consumer.AIOConsumer')
async def test_lag_metadata_rpcs_carry_an_explicit_timeout(mock_cls, kafka_config, kafka_source_config):
    """Both lag RPCs must pass a timeout — librdkafka blocks forever without one.

    These calls share one small executor pool with ``consume`` and ``commit``.
    An untimed query against an unreachable partition leader parks a pool
    thread indefinitely; once the pool is exhausted the poll loop queues
    behind it and the worker silently stops consuming while still reporting
    ready. The timeout is what bounds that, so it is pinned here.
    """
    from confluent_kafka import TopicPartition

    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner
    mock_inner.committed.return_value = [TopicPartition(kafka_source_config.topic, 0, 5)]

    async def fake_watermarks(tp, *, timeout=None):
        assert timeout == LAG_QUERY_TIMEOUT_SECONDS
        return (0, 10)

    mock_inner.get_watermark_offsets.side_effect = fake_watermarks

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    await consumer.get_total_lag([0])
    await consumer.get_partition_lag([0])

    for call in mock_inner.committed.call_args_list:
        assert call.kwargs.get('timeout') == LAG_QUERY_TIMEOUT_SECONDS


@patch('drakkar.consumer.AIOConsumer')
async def test_consumer_pool_is_larger_than_the_two_thread_default(mock_cls, kafka_config, kafka_source_config):
    """AIOConsumer's 2-thread default is shared by every consumer operation.

    The UI's watermark fan-out submits one work item per assigned partition,
    so the default leaves ``consume``/``commit`` waiting behind UI traffic.
    """
    mock_cls.return_value = AsyncMock()
    KafkaConsumer(connection=kafka_config, source=kafka_source_config)

    assert mock_cls.call_args.kwargs['max_workers'] == CONSUMER_MAX_WORKERS
    assert CONSUMER_MAX_WORKERS > 2


@patch('drakkar.consumer.AIOConsumer')
async def test_get_total_lag_watermark_exception(mock_cls, kafka_config, kafka_source_config):
    """Watermark exception for one partition yields 0 lag for that partition only."""
    from confluent_kafka import TopicPartition

    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    tp0 = TopicPartition(kafka_source_config.topic, 0, 50)
    tp1 = TopicPartition(kafka_source_config.topic, 1, 80)
    mock_inner.committed.return_value = [tp0, tp1]

    async def fake_watermarks(tp, *, timeout=None):
        assert timeout == LAG_QUERY_TIMEOUT_SECONDS, 'watermark query must carry the lag timeout'
        if tp.partition == 0:
            raise RuntimeError('timeout')
        return (0, 100)  # partition 1 lag = 100 - 80 = 20

    mock_inner.get_watermark_offsets.side_effect = fake_watermarks

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    total = await consumer.get_total_lag([0, 1])
    assert total == 20  # partition 0 = 0 (exception), partition 1 = 20


@patch('drakkar.consumer.AIOConsumer')
async def test_get_partition_lag(mock_cls, kafka_config, kafka_source_config):
    """get_partition_lag returns per-partition committed, high_watermark, lag."""
    from confluent_kafka import TopicPartition

    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    async def fake_watermarks(tp, *, timeout=None):
        assert timeout == LAG_QUERY_TIMEOUT_SECONDS, 'watermark query must carry the lag timeout'
        if tp.partition == 0:
            return (0, 100)
        return (0, 200)

    mock_inner.get_watermark_offsets.side_effect = fake_watermarks

    tp0_committed = TopicPartition(kafka_source_config.topic, 0, 90)
    tp1_committed = TopicPartition(kafka_source_config.topic, 1, 150)
    mock_inner.committed.return_value = [tp0_committed, tp1_committed]

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    result = await consumer.get_partition_lag([0, 1])

    assert result[0] == {'committed': 90, 'high_watermark': 100, 'lag': 10}
    assert result[1] == {'committed': 150, 'high_watermark': 200, 'lag': 50}


@patch('drakkar.consumer.AIOConsumer')
async def test_get_partition_lag_watermark_failure_returns_zero_lag(mock_cls, kafka_config, kafka_source_config):
    """A failed watermark query yields zero lag; nothing known about the commit reads as None."""
    mock_inner = AsyncMock()
    mock_inner.get_watermark_offsets.side_effect = RuntimeError('connection lost')
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)
    result = await consumer.get_partition_lag([0])

    assert result[0] == {'committed': None, 'high_watermark': 0, 'lag': 0}


LOW_WATERMARK = 10
HIGH_WATERMARK = 100


def make_lag_consumer(mock_cls, connection, source, committed_rows):
    """A KafkaConsumer whose broker reports the module watermarks and the given committed() rows."""
    mock_inner = AsyncMock()
    mock_inner.get_watermark_offsets.return_value = (LOW_WATERMARK, HIGH_WATERMARK)
    mock_inner.committed.return_value = committed_rows
    mock_cls.return_value = mock_inner
    return KafkaConsumer(connection=connection, source=source)


def committed_row_with_error(partition):
    """A committed() row that carries a per-partition error.

    ``TopicPartition.error`` is read-only from Python, so this stand-in has
    the attributes the lag code reads.
    """
    return SimpleNamespace(
        partition=partition,
        offset=OFFSET_INVALID,
        error=KafkaError(KafkaError.REQUEST_TIMED_OUT),
    )


@pytest.mark.parametrize(
    ('broker_offset', 'own_commit', 'expected_committed', 'expected_lag'),
    [
        pytest.param(90, None, 90, 10, id='broker-only'),
        pytest.param(OFFSET_INVALID, 95, 95, 5, id='broker-missing-uses-own-commit'),
        pytest.param(90, 95, 95, 5, id='larger-of-both'),
        pytest.param(0, None, 0, HIGH_WATERMARK, id='broker-zero-is-a-real-offset'),
        pytest.param(OFFSET_INVALID, None, None, HIGH_WATERMARK - LOW_WATERMARK, id='nothing-known'),
    ],
)
@patch('drakkar.consumer.AIOConsumer')
async def test_get_partition_lag_resolves_committed_offset(
    mock_cls, kafka_config, kafka_source_config, broker_offset, own_commit, expected_committed, expected_lag
):
    """An offset the broker does not report never reads as committed=0 and a full backlog."""
    consumer = make_lag_consumer(
        mock_cls, kafka_config, kafka_source_config, [TopicPartition(kafka_source_config.topic, 0, broker_offset)]
    )
    if own_commit is not None:
        await consumer.commit({0: own_commit})

    result = await consumer.get_partition_lag([0])

    assert result[0] == {'committed': expected_committed, 'high_watermark': HIGH_WATERMARK, 'lag': expected_lag}


@pytest.mark.parametrize(
    ('reset_policy', 'expected_lag'),
    [
        ('earliest', HIGH_WATERMARK - LOW_WATERMARK),
        ('smallest', HIGH_WATERMARK - LOW_WATERMARK),
        ('beginning', HIGH_WATERMARK - LOW_WATERMARK),
        ('error', HIGH_WATERMARK - LOW_WATERMARK),
        ('latest', 0),
        ('largest', 0),
        ('end', 0),
        ('Latest', 0),
    ],
)
@patch('drakkar.consumer.AIOConsumer')
async def test_get_partition_lag_without_committed_offset_counts_from_reset_position(
    mock_cls, kafka_source_config, reset_policy, expected_lag
):
    """With no committed offset, lag counts from where auto.offset.reset starts reading."""
    connection = KafkaConfig(brokers='localhost:9092', client_config={'auto.offset.reset': reset_policy})
    consumer = make_lag_consumer(
        mock_cls, connection, kafka_source_config, [TopicPartition(kafka_source_config.topic, 0, OFFSET_INVALID)]
    )

    result = await consumer.get_partition_lag([0])

    assert result[0] == {'committed': None, 'high_watermark': HIGH_WATERMARK, 'lag': expected_lag}


@patch('drakkar.consumer.AIOConsumer')
async def test_get_partition_lag_with_per_partition_error_logs_and_counts_it(
    mock_cls, kafka_config, kafka_source_config
):
    """A per-partition committed() error is visible, and the partition falls back to its own commit."""
    consumer = make_lag_consumer(
        mock_cls,
        kafka_config,
        kafka_source_config,
        [TopicPartition(kafka_source_config.topic, 0, 90), committed_row_with_error(1)],
    )
    await consumer.commit({1: 95})
    errors_before = consumer_errors._value.get()  # type: ignore[attr-defined]

    with capture_logs() as cap:
        result = await consumer.get_partition_lag([0, 1])

    failures = [e for e in cap if e['event'] == 'lag_query_failed']
    assert [(f['op'], f['partition']) for f in failures] == [('committed', 1)]
    assert consumer_errors._value.get() == errors_before + 1  # type: ignore[attr-defined]
    assert result[0] == {'committed': 90, 'high_watermark': HIGH_WATERMARK, 'lag': 10}
    assert result[1] == {'committed': 95, 'high_watermark': HIGH_WATERMARK, 'lag': 5}


@patch('drakkar.consumer.AIOConsumer')
async def test_get_total_lag_when_committed_query_fails_equals_partition_lag_sum(
    mock_cls, kafka_config, kafka_source_config
):
    """A failed committed() call falls back per partition, and the total is the per-partition sum."""
    consumer = make_lag_consumer(mock_cls, kafka_config, kafka_source_config, [])
    mock_cls.return_value.committed.side_effect = RuntimeError('broker unreachable')
    await consumer.commit({0: 95})

    partition_lag = await consumer.get_partition_lag([0, 1])
    total = await consumer.get_total_lag([0, 1])

    assert partition_lag[0] == {'committed': 95, 'high_watermark': HIGH_WATERMARK, 'lag': 5}
    assert partition_lag[1] == {
        'committed': None,
        'high_watermark': HIGH_WATERMARK,
        'lag': HIGH_WATERMARK - LOW_WATERMARK,
    }
    assert total == partition_lag[0]['lag'] + partition_lag[1]['lag']


@patch('drakkar.consumer.AIOConsumer')
async def test_assign_forgets_commit_from_an_earlier_assignment(mock_cls, kafka_config, kafka_source_config):
    """A re-assigned partition does not reuse this worker's commit from before."""
    consumer = make_lag_consumer(
        mock_cls, kafka_config, kafka_source_config, [TopicPartition(kafka_source_config.topic, 0, OFFSET_INVALID)]
    )
    await consumer.commit({0: 95})
    await consumer.subscribe()
    assign_cb = mock_cls.return_value.subscribe.call_args.kwargs['on_assign']

    await assign_cb(mock_cls.return_value, [TopicPartition(kafka_source_config.topic, 0)])
    result = await consumer.get_partition_lag([0])

    assert result[0]['committed'] is None


@patch('drakkar.consumer.AIOConsumer')
async def test_failed_commit_is_not_used_for_lag(mock_cls, kafka_config, kafka_source_config):
    """Only a commit the broker accepted can stand in for a missing broker answer."""
    consumer = make_lag_consumer(
        mock_cls, kafka_config, kafka_source_config, [TopicPartition(kafka_source_config.topic, 0, OFFSET_INVALID)]
    )
    mock_cls.return_value.commit.side_effect = RuntimeError('commit rejected')

    with pytest.raises(RuntimeError, match='commit rejected'):
        await consumer.commit({0: 95})
    result = await consumer.get_partition_lag([0])

    assert result[0]['committed'] is None


@patch('drakkar.consumer.AIOConsumer')
async def test_on_assign_without_callback(mock_cls, kafka_config, kafka_source_config):
    """_handle_assign with no callback still logs and increments metrics."""
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)  # no on_assign callback
    await consumer.subscribe()

    call_kwargs = mock_inner.subscribe.call_args[1]
    assign_cb = call_kwargs['on_assign']
    from confluent_kafka import TopicPartition

    # should not raise even without callback
    await assign_cb(mock_inner, [TopicPartition('test-source', 0)])


@patch('drakkar.consumer.AIOConsumer')
async def test_on_revoke_without_callback(mock_cls, kafka_config, kafka_source_config):
    """_handle_revoke with no callback still logs and increments metrics."""
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(connection=kafka_config, source=kafka_source_config)  # no on_revoke callback
    await consumer.subscribe()

    call_kwargs = mock_inner.subscribe.call_args[1]
    revoke_cb = call_kwargs['on_revoke']
    from confluent_kafka import TopicPartition

    # should not raise even without callback
    await revoke_cb(mock_inner, [TopicPartition('test-source', 2)])


@patch('drakkar.consumer.AIOConsumer')
def test_consumer_reads_group_and_topic_from_source_config(mock_cls):
    consumer = KafkaConsumer(
        connection=KafkaConfig(brokers='b:9092'),
        source=KafkaSourceConfig(enabled=True, topic='t', consumer_group='g', max_poll_records=7),
    )
    assert consumer.source_topic == 't'
    assert consumer.consumer_group == 'g'
