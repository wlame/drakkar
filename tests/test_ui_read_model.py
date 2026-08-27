"""The UI read model: SQL builders and row aggregation, without a web server.

These used to be closures inside `create_live_router`, reachable only
through HTTP. They are plain functions in `drakkar/recorder/queries.py`
now, so the timeline's retry grouping and the task-state fold can be
checked against known rows — and the aggregation lives in one place, not
own queries against.
"""

from __future__ import annotations

import json

import pytest

from drakkar.recorder.queries import (
    WEBAPP_RATE_TILES,
    build_task_detail,
    consumed_timestamps_query,
    count_by_topic,
    end_to_end_seconds,
    event_count_query,
    events_query,
    group_task_states,
    group_timeline_tasks,
    hook_events_query,
    index_consumed_timestamps,
    parse_json_object,
    recent_tasks_query,
    sink_breakdown_query,
    task_state_query,
)
from drakkar.recorder.queries import (
    base_task_id as parse_base,
)


def started(task_id='t1', ts=100.0, **extra) -> dict:
    return {'event': 'task_started', 'task_id': task_id, 'ts': ts, **extra}


def completed(task_id='t1', ts=101.0, duration=1.0, **extra) -> dict:
    return {'event': 'task_completed', 'task_id': task_id, 'ts': ts, 'duration': duration, **extra}


def failed(task_id='t1', ts=101.0, duration=1.0, **extra) -> dict:
    return {'event': 'task_failed', 'task_id': task_id, 'ts': ts, 'duration': duration, **extra}


class TestSqlBuilders:
    """Parameters are always bound, never interpolated — the values come
    from HTTP query strings and request bodies."""

    def test_events_query_without_filters(self):
        sql, params = events_query(limit=50)

        assert 'WHERE' not in sql
        assert sql.endswith('ORDER BY id DESC LIMIT ?')
        assert params == [50]

    def test_events_query_binds_every_filter_value(self):
        sql, params = events_query(partitions=[0, 3], event_types=['consumed'], after_id=17, limit=10)

        assert 'partition IN (?,?)' in sql
        assert 'event IN (?)' in sql
        assert 'id > ?' in sql
        assert params == [0, 3, 'consumed', 17, 10]

    def test_events_query_ignores_a_zero_after_id(self):
        _sql, params = events_query(after_id=0, limit=5)

        assert params == [5]

    def test_recent_tasks_query_takes_the_newest_then_re_sorts(self):
        """The retry grouping depends on chronological order, but the cap
        must keep the NEWEST rows — hence DESC inside, ASC outside."""
        sql, params = recent_tasks_query(since=1000.0, event_limit=300)

        inner = sql.index('ORDER BY ts DESC')
        outer = sql.index('ORDER BY ts ASC')
        assert inner < outer
        assert params == [1000.0, 300]

    def test_recent_tasks_query_never_selects_captured_output(self):
        """stdout/stderr hold subprocess output no timeline renders; pulling
        them made the response size track output volume, not task count."""
        sql, _params = recent_tasks_query(since=0.0, event_limit=1)

        assert 'stdout,' not in sql
        assert 'stderr' not in sql
        assert 'stdout_size' in sql

    def test_task_state_query_binds_one_placeholder_per_id(self):
        sql, params = task_state_query(['a', 'b', 'c'])

        assert 'task_id IN (?,?,?)' in sql
        assert params == ['a', 'b', 'c']

    def test_hook_events_query_binds_the_event_name(self):
        sql, params = hook_events_query(event_name='task_complete', limit=25)

        assert 'event = ?' in sql
        assert params == ['task_complete', 25]

    def test_sink_breakdown_query_groups_by_topic(self):
        sql, params = sink_breakdown_query(partition=2, offsets=[10, 11])

        assert 'GROUP BY output_topic' in sql
        assert 'offset IN (?,?)' in sql
        assert params == [2, 10, 11]


class TestParseJsonObject:
    @pytest.mark.parametrize('raw', [None, '', 'not json', '[1, 2]', '"a string"', '42'])
    def test_anything_that_is_not_an_object_reads_as_empty(self, raw):
        """A truncated or hand-edited row must not blank a whole page."""
        assert parse_json_object(raw) == {}

    def test_an_object_decodes(self):
        assert parse_json_object('{"slot": 3}') == {'slot': 3}


class TestGroupTimelineTasks:
    def test_a_completed_task_becomes_one_row(self):
        rows, trimmed = group_timeline_tasks(
            [started(), completed()],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        (row,) = rows
        assert row['task_id'] == 't1'
        assert row['status'] == 'completed'
        assert row['start_ts'] == 100.0
        assert row['end_ts'] == 101.0
        assert trimmed is False

    def test_a_task_with_no_completion_is_still_running(self):
        (row,), _ = group_timeline_tasks([started()], ws_min_duration_seconds=0.0, limit=10)

        assert row['status'] == 'running'
        assert row['end_ts'] is None

    def test_a_retry_archives_the_earlier_attempt_under_a_composite_key(self):
        rows, _ = group_timeline_tasks(
            [started(ts=100.0), failed(ts=101.0), started(ts=102.0), completed(ts=103.0)],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        by_id = {row['task_id']: row for row in rows}
        assert set(by_id) == {'t1', 't1:r100.0'}
        assert by_id['t1:r100.0']['status'] == 'failed'
        # The latest attempt keeps the plain id so live WebSocket events
        # still match the row the UI is drawing.
        assert by_id['t1']['status'] == 'completed'

    def test_an_abandoned_attempt_is_closed_when_the_retry_starts(self):
        """Without this it would draw as a bar that never ends."""
        rows, _ = group_timeline_tasks(
            [started(ts=100.0), started(ts=105.0)],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        archived = next(row for row in rows if row['task_id'] == 't1:r100.0')
        assert archived['end_ts'] == 105.0
        assert archived['status'] == 'failed'

    def test_a_completion_without_a_start_is_ignored(self):
        """Its start event fell outside the query window."""
        rows, _ = group_timeline_tasks([completed()], ws_min_duration_seconds=0.0, limit=10)

        assert rows == []

    def test_an_event_without_a_task_id_is_ignored(self):
        rows, _ = group_timeline_tasks(
            [{'event': 'consumed', 'ts': 1.0}, started()],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        assert len(rows) == 1

    def test_a_fast_completed_task_is_hidden(self):
        """Matches what the live WebSocket stream suppresses — a task
        nobody saw start must not appear finishing."""
        rows, _ = group_timeline_tasks(
            [started(), completed(duration=0.01)],
            ws_min_duration_seconds=0.1,
            limit=10,
        )

        assert rows == []

    def test_a_fast_failed_task_is_still_shown(self):
        rows, _ = group_timeline_tasks(
            [started(), failed(duration=0.01)],
            ws_min_duration_seconds=0.1,
            limit=10,
        )

        assert len(rows) == 1

    def test_a_running_task_is_kept_whatever_the_threshold(self):
        rows, _ = group_timeline_tasks([started()], ws_min_duration_seconds=10.0, limit=10)

        assert len(rows) == 1

    def test_the_newest_rows_survive_the_limit_and_trimming_is_reported(self):
        events = []
        for index in range(5):
            events += [started(f't{index}', ts=100.0 + index), completed(f't{index}', ts=100.5 + index)]

        rows, trimmed = group_timeline_tasks(events, ws_min_duration_seconds=0.0, limit=2)

        assert trimmed is True
        assert [row['task_id'] for row in rows] == ['t3', 't4']

    def test_slot_and_env_come_from_the_start_metadata(self):
        (row,), _ = group_timeline_tasks(
            [started(metadata=json.dumps({'slot': 4, 'env': {'A': '1'}}))],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        assert row['slot'] == 4
        assert row['env'] == {'A': '1'}

    def test_cost_and_speed_come_from_the_completion_metadata(self):
        (row,), _ = group_timeline_tasks(
            [started(), completed(metadata=json.dumps({'cost': 2.0, 'speed': 5.0}))],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        assert (row['cost'], row['speed']) == (2.0, 5.0)

    def test_a_completion_without_speed_adds_no_throughput_keys(self):
        (row,), _ = group_timeline_tasks(
            [started(), completed(metadata=json.dumps({'other': 1}))],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        assert 'cost' not in row and 'speed' not in row

    def test_origin_defaults_to_kafka_on_an_older_row(self):
        (row,), _ = group_timeline_tasks([started()], ws_min_duration_seconds=0.0, limit=10)

        assert row['origin'] == 'kafka'

    def test_webapp_columns_are_carried_through(self):
        (row,), _ = group_timeline_tasks(
            [started(origin='http', client_name='tenant-a', request_id='req-1')],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        assert (row['origin'], row['client_name'], row['request_id']) == ('http', 'tenant-a', 'req-1')

    def test_unparseable_metadata_does_not_lose_the_row(self):
        (row,), _ = group_timeline_tasks(
            [started(metadata='{truncated', labels='also broken')],
            ws_min_duration_seconds=0.0,
            limit=10,
        )

        assert row['slot'] is None
        assert row['labels'] is None


class TestGroupTaskStates:
    def test_an_unfinished_task_reports_running(self):
        states = group_task_states([started()])

        assert states['t1']['status'] == 'running'
        assert states['t1']['end_ts'] is None

    def test_a_retry_collapses_to_the_latest_outcome(self):
        """Unlike the timeline, this answers "what is the state of this
        task", not "draw me every attempt"."""
        states = group_task_states(
            [started(ts=100.0), failed(ts=101.0), started(ts=102.0), completed(ts=103.0, exit_code=0)]
        )

        assert set(states) == {'t1'}
        assert states['t1']['status'] == 'completed'
        assert states['t1']['exit_code'] == 0

    def test_source_offsets_and_labels_come_from_the_start_row(self):
        states = group_task_states(
            [started(metadata=json.dumps({'source_offsets': [7, 8]}), labels=json.dumps({'tenant': 'a'}))]
        )

        assert states['t1']['source_offsets'] == [7, 8]
        assert states['t1']['labels'] == {'tenant': 'a'}

    def test_a_completion_with_no_start_still_reports_its_outcome(self):
        states = group_task_states([completed(exit_code=0)])

        assert states['t1']['status'] == 'completed'
        assert states['t1']['start_ts'] is None

    def test_tasks_are_keyed_independently(self):
        states = group_task_states([started('a'), started('b'), completed('b')])

        assert states['a']['status'] == 'running'
        assert states['b']['status'] == 'completed'

    def test_no_events_yields_no_entries(self):
        assert group_task_states([]) == {}


class TestCountByTopic:
    def test_counts_land_under_their_topic(self):
        assert count_by_topic([('orders-out', 3), ('audit', 1)]) == {'orders-out': 3, 'audit': 1}

    def test_a_row_with_no_topic_is_counted_not_dropped(self):
        """A produced event predating per-sink attribution still has to
        make the totals add up."""
        assert count_by_topic([(None, 2)]) == {'(unknown)': 2}

    def test_no_rows_is_an_empty_map(self):
        assert count_by_topic([]) == {}


class TestBaseTaskId:
    def test_a_retry_composite_key_resolves_to_the_recorded_id(self):
        """The timeline links to composite keys, but the recorder only ever
        wrote the base id."""
        assert parse_base('t-abc:r1234567.89') == 't-abc'

    def test_a_plain_id_is_unchanged(self):
        assert parse_base('t-abc') == 't-abc'

    def test_only_the_first_suffix_is_stripped(self):
        assert parse_base('t-abc:r1:r2') == 't-abc'


class TestBuildTaskDetail:
    def test_the_requested_id_is_echoed_even_for_a_retry_key(self):
        detail = build_task_detail('t1:r100.0', [started()])

        assert detail['task_id'] == 't1:r100.0'

    def test_duration_prefers_the_recorded_value(self):
        detail = build_task_detail('t1', [started(ts=100.0), completed(ts=110.0, duration=3.0)])

        assert detail['duration'] == 3.0

    def test_duration_falls_back_to_the_span_between_start_and_finish(self):
        """An older completion row may carry no duration column."""
        detail = build_task_detail('t1', [started(ts=100.0), completed(ts=110.0, duration=None)])

        assert detail['duration'] == 10.0

    def test_a_task_that_never_finished_has_no_duration(self):
        detail = build_task_detail('t1', [started()])

        assert detail['duration'] is None
        assert detail['exit_code'] is None

    def test_a_failure_is_reported_with_its_exit_code(self):
        detail = build_task_detail('t1', [started(), failed(exit_code=2)])

        assert detail['failed'] is not None
        assert detail['exit_code'] == 2

    def test_start_metadata_supplies_offsets_env_and_labels(self):
        detail = build_task_detail(
            't1',
            [
                started(
                    metadata=json.dumps({'source_offsets': [4], 'env': {'A': '1'}}),
                    labels=json.dumps({'tenant': 'a'}),
                    args=json.dumps(['--flag']),
                    partition=2,
                )
            ],
        )

        assert detail['source_offsets'] == [4]
        assert detail['task_env'] == {'A': '1'}
        assert detail['labels'] == {'tenant': 'a'}
        assert detail['args'] == ['--flag']
        assert detail['partition'] == 2

    def test_unparseable_args_are_shown_raw_rather_than_dropped(self):
        detail = build_task_detail('t1', [started(args='--not-json')])

        assert detail['args'] == '--not-json'

    def test_an_http_task_carries_its_client_and_request(self):
        detail = build_task_detail('t1', [started(origin='http', client_name='tenant-a', request_id='req-1')])

        assert (detail['origin'], detail['client_name'], detail['request_id']) == ('http', 'tenant-a', 'req-1')

    def test_a_kafka_task_reports_no_webapp_bodies(self):
        detail = build_task_detail(
            't1',
            [started(), {'event': 'webapp_request_received', 'ts': 1.0, 'metadata': json.dumps({'body': {'a': 1}})}],
        )

        assert detail['webapp_request_body'] is None, 'only HTTP-origin tasks have request bodies'

    def test_a_captured_request_body_is_surfaced(self):
        detail = build_task_detail(
            't1',
            [
                started(origin='http'),
                {'event': 'webapp_request_received', 'ts': 1.0, 'metadata': json.dumps({'body': {'a': 1}})},
                {'event': 'webapp_request_completed', 'ts': 2.0, 'metadata': json.dumps({'response': {'ok': True}})},
            ],
        )

        assert detail['webapp_request_body'] == {'a': 1}
        assert detail['webapp_response_body'] == {'ok': True}

    def test_a_size_only_capture_says_the_body_was_not_recorded(self):
        detail = build_task_detail(
            't1',
            [
                started(origin='http'),
                {'event': 'webapp_request_received', 'ts': 1.0, 'metadata': json.dumps({'body_bytes': 8192})},
            ],
        )

        assert detail['webapp_request_body'] == {'body_bytes': 8192, 'recorded': False}

    def test_no_events_still_returns_the_documented_shape(self):
        detail = build_task_detail('t-missing', [])

        assert detail['task_id'] == 't-missing'
        assert detail['started'] is None
        assert detail['events'] == []
        assert detail['origin'] == 'kafka'


class TestConsumedPairing:
    def test_the_query_filters_both_dimensions_separately(self):
        """SQLite has no row-value IN, so the result is a superset that
        index_consumed_timestamps narrows by exact key."""
        sql, params = consumed_timestamps_query([(0, 10), (1, 20)])

        assert 'partition IN (?,?)' in sql
        assert 'offset IN (?,?)' in sql
        assert params == [0, 1, 10, 20]

    def test_rows_group_by_exact_key(self):
        index = index_consumed_timestamps([(0, 10, 100.0), (0, 10, 200.0), (1, 20, 150.0)])

        assert index[(0, 10)] == [100.0, 200.0]
        assert index[(1, 20)] == [150.0]

    def test_end_to_end_uses_the_most_recent_consume_before_completion(self):
        """A message redelivered after a rebalance has several consumes."""
        assert end_to_end_seconds([100.0, 200.0], completed_ts=250.0) == 50.0

    def test_a_consume_after_the_completion_is_ignored(self):
        """It belongs to a redelivery that has not finished — pairing with
        it would report a negative duration."""
        assert end_to_end_seconds([300.0], completed_ts=250.0) is None

    def test_no_consume_row_means_unknown(self):
        assert end_to_end_seconds(None, completed_ts=1.0) is None
        assert end_to_end_seconds([], completed_ts=1.0) is None


class TestWebappRateTiles:
    def test_every_tile_names_at_least_one_event(self):
        for key, event_names in WEBAPP_RATE_TILES:
            assert key.endswith('_60s')
            assert event_names

    def test_no_event_is_counted_in_two_tiles(self):
        """Double-counting would make the dashboard's totals exceed the
        requests actually served."""
        counted = [name for _key, names in WEBAPP_RATE_TILES for name in names]

        assert len(counted) == len(set(counted))

    def test_the_count_query_binds_every_event_name_and_the_cutoff(self):
        sql, params = event_count_query(event_names=('a', 'b'), since=99.0)

        assert 'COUNT(*)' in sql
        assert 'event IN (?,?)' in sql
        assert params == ['a', 'b', 99.0]


class TestEventsOriginFilter:
    """The History page's origin radio — the SPA sends it, so it must work."""

    def test_origin_is_bound_not_interpolated(self):
        sql, params = events_query(origin='http', limit=10)

        assert 'origin = ?' in sql
        assert params == ['http', 10]

    def test_origin_combines_with_the_other_filters(self):
        sql, params = events_query(partitions=[1], event_types=['consumed'], origin='kafka', after_id=5, limit=10)

        assert sql.count('?') == len(params)
        assert params == [1, 'consumed', 'kafka', 5, 10]

    def test_no_origin_adds_no_condition(self):
        sql, params = events_query(origin=None, limit=10)

        assert 'origin' not in sql
        assert params == [10]
