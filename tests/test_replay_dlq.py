"""Tests for scripts/replay_dlq.py — the DLQ replay reference tool.

The script lives outside the ``drakkar`` package (under ``scripts/``), so we
load it once via ``importlib`` and share the module across tests. The DLQ
reader needs a live broker, so tests replace ``read_dlq_entries`` with a plain
async iterator and the producer with a fake of ``confluent_kafka.Producer``.
"""

import importlib.util
import re
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from drakkar.sinks.dlq import DLQEntry

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'replay_dlq.py'


@pytest.fixture(scope='module')
def replay_module():
    """Import scripts/replay_dlq.py as a module so tests can call its functions.

    ``importlib.util.spec_from_file_location`` is the canonical way to pull a
    script that isn't on ``sys.path`` into the test process without packaging
    it. The module is cached at module scope so we pay the import cost once.
    """
    spec = importlib.util.spec_from_file_location('replay_dlq', SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules['replay_dlq'] = module
    spec.loader.exec_module(module)
    return module


def _dlq_entry(
    value: str | bytes,
    offset: int = 0,
    *,
    stage: str | None = 'sink',
    key: bytes | None = None,
    source_headers: list[tuple[str, bytes | None]] | None = None,
    partition: int = 0,
) -> DLQEntry:
    """A DLQ record as ``read_dlq_entries`` yields it: source headers, then the dk- headers."""
    dk_headers: list[tuple[str, bytes | None]] = [('dk-record-version', b'1')]
    if stage is not None:
        dk_headers.append(('dk-stage', stage.encode()))
    dk_headers.append(('dk-error', b'broker down'))
    return DLQEntry(
        key=key,
        value=value if isinstance(value, bytes) else value.encode(),
        headers=[*(source_headers or []), *dk_headers],
        partition=partition,
        offset=offset,
    )


async def _async_iter(items):
    """Yield items from a plain list as an async iterator, in place of ``read_dlq_entries``."""
    for item in items:
        yield item


def _ok_message():
    message = MagicMock()
    message.error.return_value = None
    return message


class _FakeProducer:
    """Stands in for ``confluent_kafka.Producer``.

    ``produce()`` keeps the delivery callback and ``flush()`` serves it, as
    librdkafka does: the callback runs on the thread that flushes.
    ``report`` is the ``(error, message)`` pair handed to the callback;
    ``serves_reports=False`` models a broker that never answers;
    ``fail_on_call`` makes the Nth ``produce()`` raise (1-based).
    """

    def __init__(self, *, report=None, serves_reports=True, fail_on_call=None):
        self.report = report if report is not None else (None, _ok_message())
        self.serves_reports = serves_reports
        self.fail_on_call = fail_on_call
        self.produced: list[dict] = []
        self.flush_timeouts: list[float] = []
        self.pending_callbacks: list = []
        self.threads: list[tuple[int, str]] = []
        self.calls = 0

    def _note_thread(self):
        self.threads.append((threading.get_ident(), threading.current_thread().name))

    def produce(self, **kwargs):
        self._note_thread()
        self.calls += 1
        if self.fail_on_call is not None and self.calls == self.fail_on_call:
            raise BufferError(f'broker down on call {self.fail_on_call}')
        self.produced.append(kwargs)
        self.pending_callbacks.append(kwargs['on_delivery'])

    def flush(self, timeout):
        self._note_thread()
        self.flush_timeouts.append(timeout)
        if self.serves_reports:
            for callback in self.pending_callbacks:
                callback(*self.report)
            self.pending_callbacks.clear()
        return len(self.pending_callbacks)


async def _run(replay_module, entries, producer, **overrides):
    """Run the replay loop over ``entries`` with defaults that publish everything."""
    kwargs = {
        'dlq_topic': 'dlq',
        'dlq_brokers': 'localhost:9092',
        'target_topic': 'target',
        'target_brokers': 'localhost:9092',
        'limit': None,
        'filter_pattern': '',
        'stage': None,
        'dry_run': False,
        'producer': producer,
    }
    kwargs.update(overrides)
    with patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)):
        return await replay_module.run(**kwargs)


def _write_config(tmp_path) -> str:
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'connections:\n'
        '  kafka:\n'
        '    main:\n'
        '      brokers: "localhost:9092"\n'
        'sources:\n'
        '  kafka:\n'
        '    enabled: true\n'
        '    topic: "requests"\n'
        '    consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: "requests_dlq"\n'
    )
    return str(config_file)


# =============================================================================
# What a replay writes
# =============================================================================


async def test_replay_publishes_key_value_and_source_headers(replay_module):
    """Each record is republished as the original message: its key, its value and its own headers."""
    entries = [
        _dlq_entry(
            b'\xff raw value',
            offset=10,
            key=b'order-1',
            source_headers=[('x-trace', b'abc'), ('x-empty', None), ('schema-id', b'7')],
        ),
        _dlq_entry('{"request_id": "r2"}', offset=11),
    ]
    producer = _FakeProducer()

    stats = await _run(replay_module, entries, producer)

    assert [call['topic'] for call in producer.produced] == ['target', 'target']
    assert producer.produced[0]['key'] == b'order-1'
    assert producer.produced[0]['value'] == b'\xff raw value'
    assert producer.produced[0]['headers'] == [('x-trace', b'abc'), ('x-empty', None), ('schema-id', b'7')]
    assert producer.produced[1]['key'] is None
    assert producer.produced[1]['value'] == b'{"request_id": "r2"}'
    assert producer.produced[1]['headers'] == []
    assert (stats.read, stats.published, stats.filtered_out) == (2, 2, 0)
    assert stats.last_successful == (0, 11)


def test_source_headers_drop_every_dk_header(replay_module):
    """A replayed record is an ordinary message again: the failure facts stay in the DLQ."""
    entry = _dlq_entry(
        'v',
        source_headers=[('x-trace', b'1'), ('dk-custom', b'old'), ('trace-dk-id', b'2')],
    )

    assert replay_module.source_headers(entry) == [('x-trace', b'1'), ('trace-dk-id', b'2')]


async def test_replay_produces_on_its_own_thread_not_the_loop(replay_module):
    """The blocking produce and flush run on one thread of the script's own, never on the loop."""
    loop_thread = threading.get_ident()
    producer = _FakeProducer()

    await _run(replay_module, [_dlq_entry('a', offset=1), _dlq_entry('b', offset=2)], producer)

    idents = {ident for ident, _name in producer.threads}
    assert loop_thread not in idents
    assert len(idents) == 1
    assert all(name.startswith('replay') for _ident, name in producer.threads)


async def test_replay_flushes_each_record_with_the_bound_and_once_more_at_the_end(replay_module):
    producer = _FakeProducer()

    await _run(replay_module, [_dlq_entry('a', offset=1)], producer, flush_timeout_seconds=7.5)

    # One flush for the record, one final flush when the replay ends.
    assert producer.flush_timeouts == [7.5, 7.5]


def _old_format_entry(offset: int) -> DLQEntry:
    """A record as an older worker wrote it: a JSON document of payloads, with no headers."""
    return DLQEntry(
        key=None,
        value=b'{"original_payloads": ["{\\"id\\": 1}"], "sink_name": "results", "sink_type": "kafka"}',
        headers=[],
        partition=0,
        offset=offset,
    )


async def test_old_format_record_is_skipped_and_the_record_next_to_it_is_replayed(replay_module):
    """A record without dk-record-version is never published, so the old JSON document never reaches the topic."""
    producer = _FakeProducer()
    entries = [_old_format_entry(offset=1), _dlq_entry('new-record', offset=2)]

    stats = await _run(replay_module, entries, producer)

    assert [call['value'] for call in producer.produced] == [b'new-record']
    assert (stats.read, stats.published, stats.skipped_old_format, stats.filtered_out) == (2, 1, 1, 0)


async def test_old_format_record_is_skipped_before_the_filters(replay_module):
    """An old record that would match the text filter is still skipped as old, not counted as filtered."""
    producer = _FakeProducer()

    stats = await _run(replay_module, [_old_format_entry(offset=1)], producer, filter_pattern='results')

    assert producer.produced == []
    assert (stats.skipped_old_format, stats.filtered_out) == (1, 0)


async def test_main_reports_old_format_records_in_a_dry_run(replay_module, tmp_path, capsys):
    entries = [_old_format_entry(offset=1), _old_format_entry(offset=2), _dlq_entry('new', offset=3)]

    with patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)):
        exit_code = await replay_module.main(['--dlq-config', _write_config(tmp_path), '--dry-run'])

    assert exit_code == 0
    stderr = capsys.readouterr().err
    assert 'summary: read=3 published=0 would_publish=1 filtered_out=0 skipped_old_format=2 errors=0' in stderr


# =============================================================================
# --dry-run
# =============================================================================


async def test_dry_run_skips_producer(replay_module):
    """With dry_run=True the reader iterates fully but nothing is produced.

    The ``published`` counter reflects only actual writes to the target
    topic (zero in dry-run). The ``would_publish`` counter tracks the
    volume estimate so operators can size a real run from dry-run output.
    """
    entries = [_dlq_entry('r1', offset=1), _dlq_entry('r2', offset=2)]

    stats = await _run(replay_module, entries, None, dry_run=True)

    assert stats.read == 2
    assert stats.published == 0
    assert stats.would_publish == 2


# =============================================================================
# --filter and --stage
# =============================================================================


async def test_filter_excludes_non_matching_entries(replay_module):
    """When the filter text appears in no value, nothing is published."""
    entries = [_dlq_entry('{"request_id": "apple"}', offset=1), _dlq_entry('{"request_id": "banana"}', offset=2)]
    producer = _FakeProducer()

    stats = await _run(replay_module, entries, producer, filter_pattern='nonexistent-needle')

    assert producer.produced == []
    assert (stats.read, stats.published, stats.filtered_out) == (2, 0, 2)


async def test_filter_matches_subset(replay_module):
    """Filter matches only entries containing the text — others are skipped."""
    entries = [
        _dlq_entry('{"request_id": "apple"}', offset=1),
        _dlq_entry('{"request_id": "banana"}', offset=2),
        _dlq_entry('{"request_id": "application"}', offset=3),
    ]
    producer = _FakeProducer()

    stats = await _run(replay_module, entries, producer, filter_pattern='app')

    assert [call['value'] for call in producer.produced] == [
        b'{"request_id": "apple"}',
        b'{"request_id": "application"}',
    ]
    assert (stats.read, stats.published, stats.filtered_out) == (3, 2, 1)


def test_entry_matches_filter_empty_pattern(replay_module):
    """Empty filter matches every entry (default CLI behavior)."""
    assert replay_module._entry_matches_filter(_dlq_entry('{"id": "x"}'), '') is True


def test_entry_matches_filter_on_the_value_text(replay_module):
    """The filter matches the value read as UTF-8; bytes that are not UTF-8 do not stop the match."""
    entry = _dlq_entry(b'\xff{"city": "caf\xc3\xa9"}')

    assert replay_module._entry_matches_filter(entry, 'café') is True
    assert replay_module._entry_matches_filter(entry, 'missing') is False


@pytest.mark.parametrize(
    ('stage', 'expected_values'),
    [
        ('parse', [b'parse-1']),
        ('execute', [b'execute-1']),
        ('sink', [b'sink-1', b'sink-2']),
        (None, [b'parse-1', b'sink-1', b'execute-1', b'sink-2', b'no-stage']),
    ],
)
async def test_stage_filter_matches_the_dk_stage_header(replay_module, stage, expected_values):
    """``--stage`` keeps the records of one failing stage; a record without ``dk-stage`` matches none."""
    entries = [
        _dlq_entry('parse-1', offset=1, stage='parse'),
        _dlq_entry('sink-1', offset=2, stage='sink'),
        _dlq_entry('execute-1', offset=3, stage='execute'),
        _dlq_entry('sink-2', offset=4, stage='sink'),
        _dlq_entry('no-stage', offset=5, stage=None),
    ]
    producer = _FakeProducer()

    stats = await _run(replay_module, entries, producer, stage=stage)

    assert [call['value'] for call in producer.produced] == expected_values
    assert stats.filtered_out == len(entries) - len(expected_values)


async def test_stage_and_text_filters_both_apply(replay_module):
    entries = [
        _dlq_entry('order-1', offset=1, stage='parse'),
        _dlq_entry('order-2', offset=2, stage='sink'),
        _dlq_entry('refund-3', offset=3, stage='sink'),
    ]
    producer = _FakeProducer()

    stats = await _run(replay_module, entries, producer, stage='sink', filter_pattern='order')

    assert [call['value'] for call in producer.produced] == [b'order-2']
    assert stats.filtered_out == 2


# =============================================================================
# --limit
# =============================================================================


async def test_limit_is_forwarded_to_the_reader(replay_module):
    """The script relies on the reader to stop after ``limit`` records; it must forward the value."""
    captured = {}

    def _fake_reader(*, topic, brokers, limit=None, **kwargs):  # matches the real signature
        captured['limit'] = limit
        return _async_iter([_dlq_entry('r1', offset=1)])

    producer = _FakeProducer()
    with patch.object(replay_module, 'read_dlq_entries', side_effect=_fake_reader):
        stats = await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=1,
            filter_pattern='',
            stage=None,
            dry_run=False,
            producer=producer,
        )

    assert captured['limit'] == 1
    assert (stats.read, stats.published) == (1, 1)


# =============================================================================
# Failures: the DLQ coordinates of the failed and the last good record
# =============================================================================


async def test_produce_failure_raises_and_logs_the_dlq_coordinates(replay_module, capsys):
    """When produce() raises on record 2, run() re-raises and stderr names the failed and the last good record."""
    entries = [
        _dlq_entry('r1', offset=10, partition=3),
        _dlq_entry('r2', offset=11, partition=3),
        _dlq_entry('r3', offset=12, partition=3),
    ]
    producer = _FakeProducer(fail_on_call=2)

    with pytest.raises(BufferError, match='broker down'):
        await _run(replay_module, entries, producer)

    stderr = capsys.readouterr().err
    assert 'publish failed at dlq_partition=3 dlq_offset=11' in stderr
    assert 'last successful dlq_partition=3 dlq_offset=10' in stderr


async def test_delivery_report_error_fails_the_record(replay_module, capsys):
    producer = _FakeProducer(report=('Broker: topic authorization failed', None))

    with pytest.raises(RuntimeError, match='topic authorization failed'):
        await _run(replay_module, [_dlq_entry('r1', offset=5)], producer)

    assert 'publish failed at dlq_partition=0 dlq_offset=5' in capsys.readouterr().err


async def test_message_error_in_the_report_fails_the_record(replay_module):
    message = MagicMock()
    message.error.return_value = 'Broker: message too large'
    producer = _FakeProducer(report=(None, message))

    with pytest.raises(RuntimeError, match='message too large'):
        await _run(replay_module, [_dlq_entry('r1', offset=5)], producer)


async def test_missing_delivery_report_fails_the_record(replay_module):
    """A broker that never confirms the write within the flush bound fails the record."""
    producer = _FakeProducer(serves_reports=False)

    with pytest.raises(RuntimeError, match=re.escape('not confirmed within 0.01s')):
        await _run(replay_module, [_dlq_entry('r1', offset=5)], producer, flush_timeout_seconds=0.01)


async def test_main_returns_exit_code_1_on_produce_failure(replay_module, tmp_path, capsys):
    """Top-level ``main`` builds a sync producer and turns a produce failure into exit code 1."""
    entries = [_dlq_entry('r1', offset=10), _dlq_entry('r2', offset=11)]
    fake_producer_cls = MagicMock(return_value=_FakeProducer(fail_on_call=2))

    with (
        patch.object(replay_module, 'Producer', fake_producer_cls),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)),
    ):
        exit_code = await replay_module.main(['--dlq-config', _write_config(tmp_path), '--target-topic', 'target'])

    assert exit_code == 1
    fake_producer_cls.assert_called_once_with({'bootstrap.servers': 'localhost:9092'})
    stderr = capsys.readouterr().err
    # The summary line is printed even on error so the operator sees progress.
    assert 'summary: read=2 published=1 would_publish=0 filtered_out=0 skipped_old_format=0 errors=1' in stderr


async def test_main_passes_the_stage_filter(replay_module, tmp_path, capsys):
    entries = [_dlq_entry('p', offset=1, stage='parse'), _dlq_entry('s', offset=2, stage='sink')]
    producer = _FakeProducer()

    with (
        patch.object(replay_module, 'Producer', MagicMock(return_value=producer)),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)),
    ):
        exit_code = await replay_module.main(
            ['--dlq-config', _write_config(tmp_path), '--target-topic=target', '--stage=parse']
        )

    assert exit_code == 0
    assert [call['value'] for call in producer.produced] == [b'p']
    stderr = capsys.readouterr().err
    assert "stage='parse'" in stderr
    assert 'summary: read=2 published=1 would_publish=0 filtered_out=1 skipped_old_format=0 errors=0' in stderr


# =============================================================================
# argparse wiring and config resolution
# =============================================================================


def test_arg_parser_requires_dlq_config(replay_module):
    """--dlq-config is a required flag."""
    parser = replay_module._build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_arg_parser_defaults(replay_module):
    """Verify default values match the documented CLI contract."""
    parser = replay_module._build_arg_parser()
    args = parser.parse_args(['--dlq-config', 'x.yaml'])
    assert args.dlq_config == 'x.yaml'
    assert args.target_topic is None
    assert args.target_brokers is None
    assert args.dry_run is False
    assert args.limit is None
    assert args.filter == ''
    assert args.stage is None


def test_arg_parser_refuses_an_unknown_stage(replay_module, capsys):
    parser = replay_module._build_arg_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['--dlq-config', 'x.yaml', '--stage=delivery'])
    assert "invalid choice: 'delivery'" in capsys.readouterr().err


def test_resolve_dlq_coordinates_uses_derived_topic(replay_module, tmp_path):
    """When dlq.topic is empty, topic is derived as {sources.kafka.topic}_dlq."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'connections:\n'
        '  kafka:\n'
        '    main:\n'
        '      brokers: "kafka:9092"\n'
        'sources:\n'
        '  kafka:\n'
        '    enabled: true\n'
        '    topic: "search-requests"\n'
        '    consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: ""\n'
    )

    dlq = replay_module._resolve_dlq_coordinates(str(config_file))
    assert dlq.topic == 'search-requests_dlq'
    assert dlq.brokers == 'kafka:9092'


def test_resolve_dlq_coordinates_explicit_overrides(replay_module, tmp_path):
    """dlq.topic wins over the derived name, and dlq.connection names the cluster."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'connections:\n'
        '  kafka:\n'
        '    main:\n'
        '      brokers: "kafka:9092"\n'
        '    other:\n'
        '      brokers: "other-cluster:9092"\n'
        'sources:\n'
        '  kafka:\n'
        '    enabled: true\n'
        '    topic: "requests"\n'
        '    consumer_group: "grp"\n'
        'dlq:\n'
        '  connection: other\n'
        '  topic: "custom-dlq"\n'
    )

    dlq = replay_module._resolve_dlq_coordinates(str(config_file))
    assert dlq.topic == 'custom-dlq'
    assert dlq.brokers == 'other-cluster:9092'


def test_resolve_dlq_coordinates_raises_when_topic_cannot_be_derived(replay_module, tmp_path):
    """No dlq.topic and a disabled Kafka source leaves resolved_dlq_topic empty — surface that plainly."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text('sources:\n  http:\n    enabled: true\ndlq:\n  topic: ""\n')

    with pytest.raises(
        ValueError,
        match=re.escape(
            'cannot derive DLQ topic: dlq.enabled is off, or dlq.topic is empty and the Kafka source is disabled'
        ),
    ):
        replay_module._resolve_dlq_coordinates(str(config_file))


async def test_main_rejects_missing_target_topic(replay_module, tmp_path, capsys):
    """Without --dry-run, --target-topic is required (exit 2)."""
    exit_code = await replay_module.main(['--dlq-config', _write_config(tmp_path)])
    assert exit_code == 2
    assert '--target-topic is required' in capsys.readouterr().err


async def test_main_reports_missing_config_file(replay_module, capsys):
    """A nonexistent config path exits 2 with a clear error."""
    exit_code = await replay_module.main(['--dlq-config', '/nonexistent/path.yaml'])
    assert exit_code == 2
    assert 'config error' in capsys.readouterr().err


async def test_main_dry_run_happy_path(replay_module, tmp_path):
    """Dry-run end-to-end path returns 0 and does not construct a producer."""
    fake_producer_cls = MagicMock()
    with (
        patch.object(replay_module, 'Producer', fake_producer_cls),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter([_dlq_entry('{"r": 1}')])),
    ):
        exit_code = await replay_module.main(['--dlq-config', _write_config(tmp_path), '--dry-run'])

    assert exit_code == 0
    fake_producer_cls.assert_not_called()


_SECURED_DLQ_CONFIG = (
    'connections:\n'
    '  kafka:\n'
    '    main:\n'
    '      brokers: "secured:9093"\n'
    '      client_config:\n'
    '        socket.timeout.ms: "5000"\n'
    '        client.id: "from-connection"\n'
    '      security:\n'
    '        protocol: SASL_SSL\n'
    '        sasl_mechanism: PLAIN\n'
    '        sasl_username: replayer\n'
    '        sasl_password: s3cret\n'
    'sources:\n'
    '  kafka:\n'
    '    enabled: true\n'
    '    topic: "requests"\n'
    '    consumer_group: "grp"\n'
    'dlq:\n'
    '  topic: "requests_dlq"\n'
    '  client_config:\n'
    '    socket.timeout.ms: "9000"\n'
)


def _write_secured_config(tmp_path) -> str:
    config_file = tmp_path / 'secured.yaml'
    config_file.write_text(_SECURED_DLQ_CONFIG)
    return str(config_file)


async def test_main_reads_and_produces_with_the_dlq_connection_settings(replay_module, tmp_path):
    """The reader and the producer both get the connection's security and client config, the DLQ's own on top."""
    fake_producer_cls = MagicMock()
    with (
        patch.object(replay_module, 'Producer', fake_producer_cls),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter([])) as reader,
    ):
        exit_code = await replay_module.main(
            ['--dlq-config', _write_secured_config(tmp_path), '--target-topic=replayed']
        )

    assert exit_code == 0
    reader_kwargs = reader.call_args.kwargs
    assert reader_kwargs['security'].protocol == 'SASL_SSL'
    assert reader_kwargs['client_config'] == {'socket.timeout.ms': '9000', 'client.id': 'from-connection'}
    producer_config = fake_producer_cls.call_args.args[0]
    assert producer_config['bootstrap.servers'] == 'secured:9093'
    assert producer_config['security.protocol'] == 'SASL_SSL'
    assert producer_config['sasl.username'] == 'replayer'
    assert producer_config['socket.timeout.ms'] == '9000'
    assert producer_config['client.id'] == 'from-connection'


async def test_main_sends_no_dlq_credentials_to_another_target_cluster(replay_module, tmp_path):
    """A producer aimed at another cluster by --target-brokers gets the brokers alone, never the DLQ's credentials."""
    fake_producer_cls = MagicMock()
    with (
        patch.object(replay_module, 'Producer', fake_producer_cls),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter([])),
    ):
        exit_code = await replay_module.main(
            [
                '--dlq-config',
                _write_secured_config(tmp_path),
                '--target-topic=replayed',
                '--target-brokers=elsewhere:9092',
            ]
        )

    assert exit_code == 0
    assert fake_producer_cls.call_args.args[0] == {'bootstrap.servers': 'elsewhere:9092'}
