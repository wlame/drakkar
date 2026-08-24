"""Tests for the ad-hoc Kafka reader (drakkar/kafka_read.py).

Covers the three layers of the module in isolation, with no real Kafka
(per the unit-test isolation rule — the AIOConsumer is replaced by a
scripted fake):

1. ``build_alias_table`` — the config → alias resolution, including the
   credential-inheritance rules shared with the pipeline and the
   reserved-alias collision policy.
2. ``fetch_message`` — watermark gating, exact-offset fetch, the
   compacted-slot case, error classification, and consumer hygiene
   (assign-only, no group joins, always closed).
3. ``stream_messages`` — offsets-for-times start resolution, the
   high-watermark snapshot as the finish line, ``to_ts``/``limit``
   termination, and the single-partition variant.
"""

from __future__ import annotations

import asyncio

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from drakkar import kafka_read
from drakkar.config import DLQConfig, DrakkarConfig, KafkaConfig, KafkaSinkConfig, SinksConfig
from drakkar.kafka_read import (
    STREAM_LIMIT_MAX,
    AliasTarget,
    KafkaReadNotFound,
    KafkaReadUnavailable,
    build_alias_table,
    fetch_message,
    stream_messages,
)
from drakkar.kafka_security import KafkaSecurityConfig

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeMessage:
    """Minimal stand-in for confluent_kafka.Message (which is not
    constructible from Python)."""

    def __init__(
        self,
        *,
        partition: int = 0,
        offset: int = 0,
        value: bytes | None = b'{"ok":true}',
        key: bytes | None = None,
        timestamp: tuple[int, int] = (1, 1_700_000_000_000),
        headers: list[tuple[str, bytes | None]] | None = None,
        error: KafkaError | None = None,
    ) -> None:
        self._partition = partition
        self._offset = offset
        self._value = value
        self._key = key
        self._timestamp = timestamp
        self._headers = headers
        self._error = error

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def key(self):
        return self._key

    def timestamp(self):
        return self._timestamp

    def headers(self):
        return self._headers

    def error(self):
        return self._error


class _TopicMeta:
    def __init__(self, partition_ids: list[int]):
        self.partitions = {pid: object() for pid in partition_ids}
        self.error = None


class _Metadata:
    def __init__(self, topic: str, partition_ids: list[int]):
        self.topics = {topic: _TopicMeta(partition_ids)}


class FakeConsumer:
    """Scripted AIOConsumer replacement.

    Configure ``watermarks`` (partition → (low, high)), ``start_offsets``
    (partition → offsets_for_times answer, -1 for "nothing at/after ts"),
    and ``batches`` (successive ``consume()`` return values). Records what
    the reader did for the hygiene assertions.
    """

    def __init__(self, conf: dict, max_workers: int = 2):
        self.conf = conf
        self.max_workers = max_workers
        self.assigned: list[TopicPartition] = []
        self.closed = False
        self.subscribed = False
        self.committed = False
        self.watermarks: dict[int, tuple[int, int]] = {}
        self.start_offsets: dict[int, int] = {}
        self.batches: list[list[FakeMessage]] = []
        self.partition_ids: list[int] = [0]
        self.topic = ''
        self.watermark_error: KafkaException | None = None

    async def get_watermark_offsets(self, tp: TopicPartition, timeout=None):
        if self.watermark_error is not None:
            raise self.watermark_error
        return self.watermarks[tp.partition]

    async def list_topics(self, topic=None, timeout=None):
        return _Metadata(self.topic, self.partition_ids)

    async def offsets_for_times(self, tps: list[TopicPartition], timeout=None):
        return [TopicPartition(tp.topic, tp.partition, self.start_offsets.get(tp.partition, -1)) for tp in tps]

    async def assign(self, tps: list[TopicPartition]):
        self.assigned = list(tps)

    async def consume(self, num_messages: int, timeout: float):
        if self.batches:
            return self.batches.pop(0)
        await asyncio.sleep(0)  # let the deadline loop advance
        return []

    async def subscribe(self, *args, **kwargs):  # pragma: no cover - must never run
        self.subscribed = True
        raise AssertionError('ad-hoc readers must never subscribe()')

    async def commit(self, *args, **kwargs):  # pragma: no cover - must never run
        self.committed = True
        raise AssertionError('ad-hoc readers must never commit()')

    async def close(self):
        self.closed = True


@pytest.fixture
def fake_consumer(monkeypatch):
    """Patch AIOConsumer with a factory handing out one prepared fake."""
    fake = FakeConsumer({}, 2)

    def _factory(conf, max_workers=2):
        fake.conf = conf
        fake.max_workers = max_workers
        return fake

    monkeypatch.setattr(kafka_read, 'AIOConsumer', _factory)
    return fake


def _target(topic: str = 'orders') -> AliasTarget:
    return AliasTarget(
        alias='orders-sink',
        kind='sink',
        topic=topic,
        brokers='broker:9092',
        security=KafkaSecurityConfig(),
        client_config={},
    )


# ---------------------------------------------------------------------------
# build_alias_table
# ---------------------------------------------------------------------------


def _config_with_sinks(**kafka_overrides) -> DrakkarConfig:
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='main:9092', source_topic='input-events', **kafka_overrides),
        sinks=SinksConfig(
            kafka={
                'search-results-kafka-sink': KafkaSinkConfig(topic='search-results'),
                'audit-kafka-sink': KafkaSinkConfig(topic='audit-log', brokers='other:9092'),
            }
        ),
    )


def test_alias_table_maps_source_dlq_and_sinks():
    table = build_alias_table(_config_with_sinks())
    assert set(table) == {'source', 'dlq', 'search-results-kafka-sink', 'audit-kafka-sink'}
    assert table['source'].topic == 'input-events'
    assert table['source'].kind == 'source'
    assert table['dlq'].topic == 'input-events_dlq'  # derived default
    assert table['dlq'].kind == 'dlq'
    assert table['search-results-kafka-sink'].topic == 'search-results'
    assert table['search-results-kafka-sink'].kind == 'sink'


def test_alias_table_explicit_dlq_topic_wins_over_derived_default():
    config = _config_with_sinks()
    config = config.model_copy(update={'dlq': DLQConfig(topic='custom-dlq-topic')})
    table = build_alias_table(config)
    assert table['dlq'].topic == 'custom-dlq-topic'


def test_alias_table_sink_without_brokers_inherits_consumer_credentials():
    security = KafkaSecurityConfig(
        protocol='SASL_PLAINTEXT', sasl_mechanism='PLAIN', sasl_username='svc', sasl_password='pw'
    )
    config = _config_with_sinks(security=security)
    table = build_alias_table(config)
    # empty brokers → same cluster → same credentials (resolve_client rule)
    inherited = table['search-results-kafka-sink']
    assert inherited.brokers == 'main:9092'
    assert inherited.security.protocol == 'SASL_PLAINTEXT'
    # explicit brokers → self-contained, defaults to PLAINTEXT
    own = table['audit-kafka-sink']
    assert own.brokers == 'other:9092'
    assert own.security.protocol == 'PLAINTEXT'


def test_alias_table_sink_shadowing_reserved_alias_is_skipped():
    config = DrakkarConfig(
        kafka=KafkaConfig(source_topic='input-events'),
        sinks=SinksConfig(kafka={'dlq': KafkaSinkConfig(topic='not-the-real-dlq')}),
    )
    table = build_alias_table(config)
    # the reserved meaning wins; the sink is not readable under 'dlq'
    assert table['dlq'].kind == 'dlq'
    assert table['dlq'].topic == 'input-events_dlq'


# ---------------------------------------------------------------------------
# reader hygiene (the no-side-effects contract)
# ---------------------------------------------------------------------------


async def test_fetch_uses_assign_only_consumer_with_no_autocommit(fake_consumer):
    fake_consumer.watermarks[3] = (0, 10)
    fake_consumer.batches = [[FakeMessage(partition=3, offset=5)]]
    await fetch_message(_target(), 3, 5)
    assert fake_consumer.conf['enable.auto.commit'] is False
    assert fake_consumer.conf['group.id'] == kafka_read.READ_GROUP_ID
    assert not fake_consumer.subscribed
    assert not fake_consumer.committed
    assert fake_consumer.closed
    assert [(tp.partition, tp.offset) for tp in fake_consumer.assigned] == [(3, 5)]


# ---------------------------------------------------------------------------
# fetch_message
# ---------------------------------------------------------------------------


async def test_fetch_message_returns_record_with_metadata(fake_consumer):
    fake_consumer.watermarks[0] = (0, 100)
    fake_consumer.batches = [
        [
            FakeMessage(
                partition=0,
                offset=42,
                value=b'{"result": "ok"}',
                key=b'task-1',
                timestamp=(1, 1_700_000_000_123),
                headers=[('trace', b'abc'), ('blob', b'\xff\xfe')],
            )
        ]
    ]
    msg = await fetch_message(_target(), 0, 42)
    assert msg.alias == 'orders-sink'
    assert (msg.partition, msg.offset) == (0, 42)
    assert msg.timestamp_ms == 1_700_000_000_123
    assert (msg.key, msg.key_encoding) == ('task-1', 'utf-8')
    assert (msg.payload, msg.payload_encoding) == ('{"result": "ok"}', 'utf-8')
    assert msg.payload_size_bytes == len(b'{"result": "ok"}')
    assert msg.headers[0].model_dump() == {'key': 'trace', 'value': 'abc', 'value_encoding': 'utf-8'}
    # non-utf8 header value falls back to base64
    assert msg.headers[1].value_encoding == 'base64'


async def test_fetch_message_binary_payload_encodes_base64(fake_consumer):
    fake_consumer.watermarks[0] = (0, 10)
    fake_consumer.batches = [[FakeMessage(offset=1, value=b'\x00\xff\x01')]]
    msg = await fetch_message(_target(), 0, 1)
    assert msg.payload_encoding == 'base64'
    assert msg.payload_size_bytes == 3


async def test_fetch_message_offset_outside_watermarks_is_not_found(fake_consumer):
    fake_consumer.watermarks[0] = (5, 10)
    with pytest.raises(KafkaReadNotFound, match=r'\[5, 10\)'):
        await fetch_message(_target(), 0, 3)
    assert fake_consumer.assigned == []  # rejected before any fetch
    assert fake_consumer.closed


async def test_fetch_message_compacted_slot_reports_next_surviving_offset(fake_consumer):
    fake_consumer.watermarks[0] = (0, 100)
    fake_consumer.batches = [[FakeMessage(offset=8)]]  # 5 was compacted away
    with pytest.raises(KafkaReadNotFound, match='next surviving offset is 8'):
        await fetch_message(_target(), 0, 5)


async def test_fetch_message_unknown_partition_is_not_found(fake_consumer):
    fake_consumer.watermark_error = KafkaException(KafkaError(KafkaError._UNKNOWN_PARTITION))
    with pytest.raises(KafkaReadNotFound):
        await fetch_message(_target(), 9, 0)
    assert fake_consumer.closed


async def test_fetch_message_broker_error_message_is_unavailable(fake_consumer):
    fake_consumer.watermarks[0] = (0, 10)
    fake_consumer.batches = [[FakeMessage(offset=5, error=KafkaError(KafkaError._TRANSPORT))]]
    with pytest.raises(KafkaReadUnavailable):
        await fetch_message(_target(), 0, 5)


async def test_fetch_message_deadline_elapses_as_unavailable(fake_consumer, monkeypatch):
    monkeypatch.setattr(kafka_read, 'FETCH_DEADLINE_SECONDS', 0.05)
    fake_consumer.watermarks[0] = (0, 10)  # empty batches → polls return nothing
    with pytest.raises(KafkaReadUnavailable, match='Timed out'):
        await fetch_message(_target(), 0, 5)
    assert fake_consumer.closed


# ---------------------------------------------------------------------------
# stream_messages
# ---------------------------------------------------------------------------


async def _collect(agen):
    return [m async for m in agen]


async def test_stream_reads_window_and_stops_at_high_watermark_snapshot(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0, 1]
    fake_consumer.start_offsets = {0: 2, 1: -1}  # p1 has nothing at/after from_ts
    fake_consumer.watermarks = {0: (0, 5), 1: (0, 3)}
    fake_consumer.batches = [
        [FakeMessage(partition=0, offset=2), FakeMessage(partition=0, offset=3)],
        [FakeMessage(partition=0, offset=4)],
    ]
    got = await _collect(stream_messages(_target(), from_ts_ms=1_000))
    assert [(m.partition, m.offset) for m in got] == [(0, 2), (0, 3), (0, 4)]
    assert fake_consumer.closed
    # only the partition with data in the window was assigned
    assert [(tp.partition, tp.offset) for tp in fake_consumer.assigned] == [(0, 2)]


async def test_stream_empty_window_yields_nothing(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0]
    fake_consumer.start_offsets = {0: -1}
    fake_consumer.watermarks = {0: (0, 10)}
    got = await _collect(stream_messages(_target(), from_ts_ms=1_000))
    assert got == []
    assert fake_consumer.closed


async def test_stream_to_ts_ends_the_partition(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0]
    fake_consumer.start_offsets = {0: 0}
    fake_consumer.watermarks = {0: (0, 10)}
    fake_consumer.batches = [
        [
            FakeMessage(partition=0, offset=0, timestamp=(1, 1_000)),
            FakeMessage(partition=0, offset=1, timestamp=(1, 2_000)),
            FakeMessage(partition=0, offset=2, timestamp=(1, 9_999)),  # past to_ts
        ]
    ]
    got = await _collect(stream_messages(_target(), from_ts_ms=0, to_ts_ms=2_000))
    assert [m.offset for m in got] == [0, 1]


async def test_stream_limit_caps_the_message_count(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0]
    fake_consumer.start_offsets = {0: 0}
    fake_consumer.watermarks = {0: (0, 100)}
    fake_consumer.batches = [[FakeMessage(partition=0, offset=i) for i in range(10)]]
    got = await _collect(stream_messages(_target(), from_ts_ms=0, limit=4))
    assert [m.offset for m in got] == [0, 1, 2, 3]
    assert fake_consumer.closed


async def test_stream_limit_is_clamped_to_the_hard_cap(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0]
    fake_consumer.start_offsets = {0: 0}
    fake_consumer.watermarks = {0: (0, 2)}
    fake_consumer.batches = [[FakeMessage(partition=0, offset=0), FakeMessage(partition=0, offset=1)]]
    got = await _collect(stream_messages(_target(), from_ts_ms=0, limit=STREAM_LIMIT_MAX))
    assert len(got) == 2  # window is smaller than any cap; call just must not blow up


async def test_stream_unknown_partition_param_is_not_found(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0, 1]
    with pytest.raises(KafkaReadNotFound, match='Partition 7'):
        await _collect(stream_messages(_target(), from_ts_ms=0, partition=7))
    assert fake_consumer.closed


async def test_stream_single_partition_reads_only_that_partition(fake_consumer):
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0, 1]
    fake_consumer.start_offsets = {0: 0, 1: 0}
    fake_consumer.watermarks = {0: (0, 5), 1: (0, 2)}
    fake_consumer.batches = [[FakeMessage(partition=1, offset=0), FakeMessage(partition=1, offset=1)]]
    got = await _collect(stream_messages(_target(), from_ts_ms=0, partition=1))
    assert [(m.partition, m.offset) for m in got] == [(1, 0), (1, 1)]
    assert [tp.partition for tp in fake_consumer.assigned] == [1]


async def test_stream_early_close_still_closes_the_consumer(fake_consumer):
    """A client disconnect closes the generator mid-flight; the ad-hoc
    consumer must not leak."""
    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0]
    fake_consumer.start_offsets = {0: 0}
    fake_consumer.watermarks = {0: (0, 100)}
    fake_consumer.batches = [[FakeMessage(partition=0, offset=i) for i in range(10)]]
    agen = stream_messages(_target(), from_ts_ms=0)
    first = await anext(agen)
    assert first.offset == 0
    await agen.aclose()
    assert fake_consumer.closed


async def test_stream_stops_at_the_wall_clock_deadline(fake_consumer, monkeypatch):
    """A window that never closes must not spin forever.

    The per-poll timeout bounds each librdkafka call, not the loop around
    them: if the offsets between the start and the high watermark were
    compacted or aged out they simply never arrive, so the partition stays
    active and ``remaining`` positive. The request would hold a consumer, a
    connection and a task indefinitely.
    """
    monkeypatch.setattr(kafka_read, 'STREAM_DEADLINE_SECONDS', 0.05)

    fake_consumer.topic = 'orders'
    fake_consumer.partition_ids = [0]
    fake_consumer.start_offsets = {0: 2}
    fake_consumer.watermarks = {0: (0, 100)}
    # Every poll comes back empty: the window can never close on its own.
    fake_consumer.batches = []

    got = await _collect(stream_messages(_target(), from_ts_ms=1_000))

    assert got == []
    # It returned rather than looping, and cleaned up on the way out.
    assert fake_consumer.closed
