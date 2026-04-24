"""Tests for scripts/replay_dlq.py — the DLQ replay reference tool.

The script lives outside the ``drakkar`` package (under ``scripts/``), so we
load it once via ``importlib`` and share the module across tests. The script
is dependency-free beyond what drakkar already requires, so no extra fixtures
are needed besides the standard mock patterns used elsewhere in the suite.
"""

import asyncio
import importlib.util
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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


def _dlq_entry(payload_json: str, offset: int = 0) -> dict:
    """Build a minimal DLQ entry matching the format produced by DLQMessage.serialize().

    Only the fields the script actually reads are included; extra keys the
    real framework writes (sink_name, timestamp, etc.) are irrelevant to the
    replay logic.
    """
    return {
        'original_payloads': [payload_json],
        'sink_name': 'results',
        'sink_type': 'kafka',
        'error': 'BrokerDown',
        'timestamp': 1_700_000_000.0,
        'partition': 0,
        'attempt_count': 1,
        '_kafka_offset': offset,
        '_kafka_partition': 0,
    }


async def _async_iter(items):
    """Yield items from a plain list as an async iterator.

    Used to stand in for ``read_dlq_entries`` in tests — the real reader
    depends on a live Kafka broker which we don't have in unit tests.
    """
    for item in items:
        yield item


def _mock_resolved_future():
    """Return a resolved future whose result passes AIOProducer's success checks."""
    f = asyncio.get_event_loop().create_future()
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = 'target'
    msg.partition.return_value = 0
    msg.offset.return_value = 99
    f.set_result(msg)
    return f


def _mock_producer(*, fail_on_call: int | None = None):
    """Build an AsyncMock producer that succeeds (or fails on the Nth produce call).

    When ``fail_on_call`` is an integer, the producer's ``produce`` method
    raises ``RuntimeError`` on that 1-based call count. Useful for the
    mid-run failure test.
    """
    producer = AsyncMock()
    producer.flush.return_value = 0
    call_count = {'n': 0}

    async def _produce(**kwargs):
        call_count['n'] += 1
        if fail_on_call is not None and call_count['n'] == fail_on_call:
            raise RuntimeError(f'broker down on call {fail_on_call}')
        return _mock_resolved_future()

    producer.produce.side_effect = _produce
    return producer


# =============================================================================
# Test 1: basic replay — 3 entries → 3 produce calls to the target topic
# =============================================================================


async def test_replay_publishes_all_entries(replay_module):
    """Feed 3 DLQ entries; verify produce() is called 3 times with correct args."""
    entries = [
        _dlq_entry('{"request_id": "r1"}', offset=10),
        _dlq_entry('{"request_id": "r2"}', offset=11),
        _dlq_entry('{"request_id": "r3"}', offset=12),
    ]
    producer = _mock_producer()

    with patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)):
        stats = await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=None,
            filter_pattern='',
            dry_run=False,
            producer=producer,
        )

    assert producer.produce.call_count == 3
    assert stats.read == 3
    assert stats.published == 3
    assert stats.filtered_out == 0
    assert stats.last_successful_offset == 12
    # Each call targets the configured target topic with the preserved payload bytes.
    for i, call in enumerate(producer.produce.call_args_list, start=1):
        assert call.kwargs['topic'] == 'target'
        assert call.kwargs['value'] == f'{{"request_id": "r{i}"}}'.encode()


# =============================================================================
# Test 2: --dry-run — reader is drained but no publishes happen
# =============================================================================


async def test_dry_run_skips_producer(replay_module):
    """With dry_run=True the reader iterates fully but produce() is never called.

    The ``published`` counter reflects only actual writes to the target
    topic (zero in dry-run). The ``would_publish`` counter tracks the
    volume estimate so operators can size a real run from dry-run output.
    """
    entries = [
        _dlq_entry('{"request_id": "r1"}', offset=1),
        _dlq_entry('{"request_id": "r2"}', offset=2),
    ]
    # Passing producer=None + dry_run=True matches the CLI's behavior — no
    # producer is constructed at all, so nothing to call.
    with patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)):
        stats = await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=None,
            filter_pattern='',
            dry_run=True,
            producer=None,
        )

    assert stats.read == 2
    # ``published`` is zero in dry-run — nothing was actually produced.
    assert stats.published == 0
    # ``would_publish`` tracks the count that WOULD have been sent.
    assert stats.would_publish == 2


# =============================================================================
# Test 3: --filter with non-matching pattern → zero publishes
# =============================================================================


async def test_filter_excludes_non_matching_entries(replay_module):
    """When the filter substring doesn't appear in any payload, zero publishes occur."""
    entries = [
        _dlq_entry('{"request_id": "apple"}', offset=1),
        _dlq_entry('{"request_id": "banana"}', offset=2),
    ]
    producer = _mock_producer()

    with patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)):
        stats = await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=None,
            filter_pattern='nonexistent-needle',
            dry_run=False,
            producer=producer,
        )

    producer.produce.assert_not_called()
    assert stats.read == 2
    assert stats.published == 0
    assert stats.filtered_out == 2


async def test_filter_matches_subset(replay_module):
    """Filter matches only entries containing the substring — others are skipped."""
    entries = [
        _dlq_entry('{"request_id": "apple"}', offset=1),
        _dlq_entry('{"request_id": "banana"}', offset=2),
        _dlq_entry('{"request_id": "application"}', offset=3),
    ]
    producer = _mock_producer()

    with patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)):
        stats = await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=None,
            filter_pattern='app',
            dry_run=False,
            producer=producer,
        )

    assert producer.produce.call_count == 2
    assert stats.read == 3
    assert stats.published == 2
    assert stats.filtered_out == 1


# =============================================================================
# Test 4: --limit=1 — only one entry is read
# =============================================================================


async def test_limit_stops_after_n(replay_module):
    """limit=1 passes through to the reader generator, which yields only 1 entry.

    The script relies on the reader honoring ``limit`` (the real
    ``read_dlq_entries`` implementation does). Our mock passes the full list
    through — but the script's read loop increments stats.read per yielded
    entry, so verifying the count here would not prove the limit path.
    Instead, we verify that the ``limit`` argument is forwarded verbatim to
    the reader, which is the script's responsibility.
    """
    entries = [
        _dlq_entry('{"request_id": "r1"}', offset=1),
    ]
    producer = _mock_producer()

    captured = {}

    def _fake_reader(*, topic, brokers, limit=None, **kwargs):  # matches real signature
        captured['limit'] = limit
        return _async_iter(entries)

    with patch.object(replay_module, 'read_dlq_entries', side_effect=_fake_reader):
        stats = await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=1,
            filter_pattern='',
            dry_run=False,
            producer=producer,
        )

    assert captured['limit'] == 1
    assert stats.read == 1
    assert stats.published == 1


# =============================================================================
# Test 5: mid-run produce failure — exit code 1, last-offset logged, partial stats
# =============================================================================


async def test_produce_failure_raises_and_logs_offset(replay_module, capsys):
    """When produce() raises on entry 2, run() re-raises and stderr logs the last
    successful offset so the operator can resume.
    """
    entries = [
        _dlq_entry('{"request_id": "r1"}', offset=10),
        _dlq_entry('{"request_id": "r2"}', offset=11),
        _dlq_entry('{"request_id": "r3"}', offset=12),
    ]
    producer = _mock_producer(fail_on_call=2)

    with (
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)),
        pytest.raises(RuntimeError, match='broker down'),
    ):
        await replay_module.run(
            dlq_topic='dlq',
            dlq_brokers='localhost:9092',
            target_topic='target',
            target_brokers='localhost:9092',
            limit=None,
            filter_pattern='',
            dry_run=False,
            producer=producer,
        )

    stderr = capsys.readouterr().err
    assert 'publish failed' in stderr
    assert 'dlq_offset=10' in stderr  # last successful offset before the failure


async def test_main_returns_exit_code_1_on_produce_failure(replay_module, tmp_path, capsys):
    """Top-level ``main`` translates a produce failure into exit code 1."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'kafka:\n'
        '  brokers: "localhost:9092"\n'
        '  source_topic: "requests"\n'
        '  consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: "requests_dlq"\n'
        '  brokers: "localhost:9092"\n'
    )

    entries = [
        _dlq_entry('{"request_id": "r1"}', offset=10),
        _dlq_entry('{"request_id": "r2"}', offset=11),
    ]

    fake_producer_cls = MagicMock()
    fake_producer_cls.return_value = _mock_producer(fail_on_call=2)

    with (
        patch.object(replay_module, 'AIOProducer', fake_producer_cls),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)),
    ):
        exit_code = await replay_module.main(
            [
                '--dlq-config',
                str(config_file),
                '--target-topic',
                'target',
            ]
        )

    assert exit_code == 1
    stderr = capsys.readouterr().err
    # The summary line is printed even on error so the operator sees progress.
    assert 'summary:' in stderr


# =============================================================================
# Additional coverage — argparse wiring and filter edge cases
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


def test_entry_matches_filter_empty_pattern(replay_module):
    """Empty filter matches every entry (default CLI behavior)."""
    entry = _dlq_entry('{"id": "x"}')
    assert replay_module._entry_matches_filter(entry, '') is True


def test_entry_matches_filter_bytes_payload(replay_module):
    """Bytes payloads are handled — some older DLQ records may have bytes."""
    entry = {'original_payloads': [b'{"id": "raw-bytes"}']}
    assert replay_module._entry_matches_filter(entry, 'raw-bytes') is True
    assert replay_module._entry_matches_filter(entry, 'missing') is False


def test_entry_matches_filter_no_payloads(replay_module):
    """Entry with empty original_payloads never matches a non-empty filter."""
    entry = {'original_payloads': []}
    assert replay_module._entry_matches_filter(entry, 'x') is False


def test_extract_payload_values_handles_mixed_types(replay_module):
    """_extract_payload_values tolerates strings, bytes, and objects."""
    entry = {
        'original_payloads': [
            '{"a": 1}',
            b'{"b": 2}',
            {'c': 3},  # fallback branch — serializes via json.dumps
        ]
    }
    values = replay_module._extract_payload_values(entry)
    assert values[0] == b'{"a": 1}'
    assert values[1] == b'{"b": 2}'
    assert b'"c": 3' in values[2]


def test_resolve_dlq_coordinates_uses_derived_topic(replay_module, tmp_path):
    """When dlq.topic is empty, topic is derived as {source_topic}_dlq."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'kafka:\n'
        '  brokers: "kafka:9092"\n'
        '  source_topic: "search-requests"\n'
        '  consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: ""\n'
        '  brokers: ""\n'
    )

    topic, brokers = replay_module._resolve_dlq_coordinates(str(config_file))
    assert topic == 'search-requests_dlq'
    assert brokers == 'kafka:9092'


def test_resolve_dlq_coordinates_explicit_overrides(replay_module, tmp_path):
    """Explicit dlq.topic / dlq.brokers take precedence over kafka.*."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'kafka:\n'
        '  brokers: "kafka:9092"\n'
        '  source_topic: "requests"\n'
        '  consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: "custom-dlq"\n'
        '  brokers: "other-cluster:9092"\n'
    )

    topic, brokers = replay_module._resolve_dlq_coordinates(str(config_file))
    assert topic == 'custom-dlq'
    assert brokers == 'other-cluster:9092'


async def test_main_rejects_missing_target_topic(replay_module, tmp_path, capsys):
    """Without --dry-run, --target-topic is required (exit 2)."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'kafka:\n'
        '  brokers: "kafka:9092"\n'
        '  source_topic: "requests"\n'
        '  consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: "dlq"\n'
        '  brokers: "kafka:9092"\n'
    )

    exit_code = await replay_module.main(['--dlq-config', str(config_file)])
    assert exit_code == 2
    stderr = capsys.readouterr().err
    assert '--target-topic is required' in stderr


async def test_main_reports_missing_config_file(replay_module, capsys):
    """A nonexistent config path exits 2 with a clear error."""
    exit_code = await replay_module.main(['--dlq-config', '/nonexistent/path.yaml'])
    assert exit_code == 2
    stderr = capsys.readouterr().err
    assert 'config error' in stderr


async def test_main_dry_run_happy_path(replay_module, tmp_path, capsys):
    """Dry-run end-to-end path returns 0 and does not construct a producer."""
    config_file = tmp_path / 'config.yaml'
    config_file.write_text(
        'kafka:\n'
        '  brokers: "kafka:9092"\n'
        '  source_topic: "requests"\n'
        '  consumer_group: "grp"\n'
        'dlq:\n'
        '  topic: "dlq"\n'
        '  brokers: "kafka:9092"\n'
    )

    entries = [_dlq_entry('{"r": 1}')]

    fake_producer_cls = MagicMock()
    with (
        patch.object(replay_module, 'AIOProducer', fake_producer_cls),
        patch.object(replay_module, 'read_dlq_entries', return_value=_async_iter(entries)),
    ):
        exit_code = await replay_module.main(
            [
                '--dlq-config',
                str(config_file),
                '--dry-run',
            ]
        )

    assert exit_code == 0
    # No producer should have been constructed in dry-run mode.
    fake_producer_cls.assert_not_called()
