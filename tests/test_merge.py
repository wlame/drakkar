"""Tests for Drakkar debug database merge logic."""

import json
import os
import sqlite3
import time

import pytest

from drakkar.merge import (
    DbStats,
    MergeResult,
    merge_databases,
    scan_db,
    scan_directory,
)
from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE


def _create_source_db(
    path,
    worker_name='worker-1',
    cluster_name=None,
    events=None,
    state=None,
    skip_events_table=False,
    skip_config_table=False,
    skip_state_table=False,
):
    """Create a source debug database with optional tables and data."""
    db = sqlite3.connect(str(path))

    if not skip_config_table:
        db.executescript(SCHEMA_WORKER_CONFIG)
        db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url,
                kafka_brokers, source_topic, consumer_group, binary_path,
                max_workers, task_timeout_seconds, max_retries, window_size,
                sinks_json, env_vars_json, created_at)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                worker_name,
                cluster_name,
                '10.0.0.1',
                8080,
                None,
                'kafka:9092',
                'search-requests',
                'test-group',
                '/usr/bin/rg',
                4,
                120,
                3,
                10,
                json.dumps({'kafka': ['results']}),
                json.dumps({'WORKER_ID': worker_name}),
                time.time(),
            ],
        )

    if not skip_events_table:
        db.executescript(SCHEMA_EVENTS)
        for ev in events or []:
            db.execute(
                """INSERT INTO events (ts, event, partition, offset, task_id, args,
                   stdout_size, stdout, stderr, exit_code, duration, output_topic, metadata, pid)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    ev.get('ts', time.time()),
                    ev.get('event', 'consumed'),
                    ev.get('partition', 0),
                    ev.get('offset'),
                    ev.get('task_id'),
                    ev.get('args'),
                    ev.get('stdout_size', 0),
                    ev.get('stdout'),
                    ev.get('stderr'),
                    ev.get('exit_code'),
                    ev.get('duration'),
                    ev.get('output_topic'),
                    ev.get('metadata'),
                    ev.get('pid'),
                ],
            )

    if not skip_state_table:
        db.executescript(SCHEMA_WORKER_STATE)
        if state:
            db.execute(
                """INSERT INTO worker_state
                   (id, uptime_seconds, assigned_partitions, partition_count,
                    pool_active, pool_max, total_queued,
                    consumed_count, completed_count, failed_count,
                    produced_count, committed_count, paused, updated_at)
                   VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    state.get('uptime_seconds', 100),
                    json.dumps(state.get('assigned_partitions', [0, 1])),
                    state.get('partition_count', 2),
                    state.get('pool_active', 1),
                    state.get('pool_max', 4),
                    state.get('total_queued', 0),
                    state.get('consumed_count', 50),
                    state.get('completed_count', 45),
                    state.get('failed_count', 2),
                    state.get('produced_count', 45),
                    state.get('committed_count', 40),
                    state.get('paused', 0),
                    state.get('updated_at', time.time()),
                ],
            )

    db.commit()
    db.close()


# ─── scan_db ─────────────────────────────────────────────────


def test_scan_db_reads_worker_config(tmp_path):
    db_path = tmp_path / 'w1.db'
    _create_source_db(db_path, worker_name='worker-1', cluster_name='main')

    stats = scan_db(str(db_path))

    assert stats.worker_name == 'worker-1'
    assert stats.cluster_name == 'main'
    assert stats.has_config is True
    assert stats.filename == 'w1.db'


def test_scan_db_counts_events(tmp_path):
    db_path = tmp_path / 'w1.db'
    events = [
        {'ts': 1000.0, 'event': 'consumed', 'partition': 0, 'offset': 1},
        {'ts': 1001.0, 'event': 'consumed', 'partition': 0, 'offset': 2},
        {'ts': 1002.0, 'event': 'task_completed', 'partition': 0, 'task_id': 't1'},
    ]
    _create_source_db(db_path, events=events)

    stats = scan_db(str(db_path))

    assert stats.event_count == 3
    assert stats.event_counts == {'consumed': 2, 'task_completed': 1}
    assert stats.first_event_ts == 1000.0
    assert stats.last_event_ts == 1002.0
    assert stats.has_events is True


def test_scan_db_no_events_table(tmp_path):
    db_path = tmp_path / 'w1.db'
    _create_source_db(db_path, skip_events_table=True)

    stats = scan_db(str(db_path))

    assert stats.has_events is False
    assert stats.event_count == 0
    assert stats.has_config is True


def test_scan_db_no_config_table(tmp_path):
    db_path = tmp_path / 'w1.db'
    _create_source_db(db_path, skip_config_table=True)

    stats = scan_db(str(db_path))

    assert stats.has_config is False
    assert stats.worker_name == ''
    assert stats.has_events is True


def test_scan_db_reports_file_size(tmp_path):
    db_path = tmp_path / 'w1.db'
    _create_source_db(db_path)

    stats = scan_db(str(db_path))

    assert stats.size_bytes > 0
    assert stats.size_bytes == os.path.getsize(str(db_path))


def test_scan_db_handles_corrupt_file(tmp_path):
    db_path = tmp_path / 'bad.db'
    db_path.write_text('not a database')

    stats = scan_db(str(db_path))

    assert stats.event_count == 0
    assert stats.worker_name == ''


# ─── scan_directory ───────────────────────────────────────────


def test_scan_directory_lists_db_files(tmp_path):
    _create_source_db(tmp_path / 'w1-2026-03-24__10_00_00.db', worker_name='w1')
    _create_source_db(tmp_path / 'w2-2026-03-24__10_00_00.db', worker_name='w2')

    results = scan_directory(str(tmp_path))

    assert len(results) == 2
    names = [r.worker_name for r in results]
    assert 'w1' in names
    assert 'w2' in names


def test_scan_directory_skips_symlinks(tmp_path):
    db_path = tmp_path / 'w1-2026-03-24__10_00_00.db'
    _create_source_db(db_path, worker_name='w1')
    link = tmp_path / 'w1-live.db'
    os.symlink(db_path.name, str(link))

    results = scan_directory(str(tmp_path))

    assert len(results) == 1
    assert results[0].filename == 'w1-2026-03-24__10_00_00.db'


def test_scan_directory_skips_non_db_files(tmp_path):
    _create_source_db(tmp_path / 'w1.db', worker_name='w1')
    (tmp_path / 'readme.txt').write_text('hello')
    (tmp_path / 'data.json').write_text('{}')

    results = scan_directory(str(tmp_path))

    assert len(results) == 1


def test_scan_directory_empty(tmp_path):
    results = scan_directory(str(tmp_path))
    assert results == []


def test_scan_directory_nonexistent():
    results = scan_directory('/no/such/dir')
    assert results == []


def test_scan_directory_sorted_by_filename(tmp_path):
    _create_source_db(tmp_path / 'c.db')
    _create_source_db(tmp_path / 'a.db')
    _create_source_db(tmp_path / 'b.db')

    results = scan_directory(str(tmp_path))

    assert [r.filename for r in results] == ['a.db', 'b.db', 'c.db']


# ─── merge_databases — basic ─────────────────────────────────


def test_merge_creates_output_file(tmp_path):
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, worker_name='worker-1')
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert os.path.isfile(output)
    assert result.output_path == output


def test_merge_single_db_workers_table(tmp_path):
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, worker_name='worker-1', cluster_name='main')
    output = str(tmp_path / 'merged.db')

    merge_databases([str(db1)], output)

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    rows = merged.execute('SELECT * FROM workers').fetchall()
    assert len(rows) == 1
    assert rows[0]['worker_name'] == 'worker-1'
    assert rows[0]['cluster_name'] == 'main'
    assert rows[0]['source_file'] == 'w1.db'
    assert rows[0]['kafka_brokers'] == 'kafka:9092'
    merged.close()


def test_merge_single_db_events(tmp_path):
    db1 = tmp_path / 'w1.db'
    events = [
        {'ts': 1000.0, 'event': 'consumed', 'partition': 0, 'offset': 1},
        {'ts': 1001.0, 'event': 'task_started', 'partition': 0, 'task_id': 't1'},
        {'ts': 1002.0, 'event': 'task_completed', 'partition': 0, 'task_id': 't1', 'duration': 1.5},
    ]
    _create_source_db(db1, worker_name='worker-1', events=events)
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.event_count == 3
    assert result.worker_count == 1

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    rows = merged.execute('SELECT * FROM events ORDER BY ts').fetchall()
    assert len(rows) == 3
    assert rows[0]['event'] == 'consumed'
    assert rows[2]['duration'] == 1.5
    # all events reference the same worker
    worker_ids = {r['worker_id'] for r in rows}
    assert len(worker_ids) == 1
    merged.close()


def test_merge_single_db_state(tmp_path):
    db1 = tmp_path / 'w1.db'
    state = {'consumed_count': 100, 'completed_count': 90, 'failed_count': 5}
    _create_source_db(db1, worker_name='worker-1', state=state)
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.state_count == 1

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    rows = merged.execute('SELECT * FROM worker_states').fetchall()
    assert len(rows) == 1
    assert rows[0]['consumed_count'] == 100
    assert rows[0]['completed_count'] == 90
    assert rows[0]['failed_count'] == 5
    merged.close()


# ─── merge_databases — multi-worker ──────────────────────────


def test_merge_two_workers_interleaved_events(tmp_path):
    """Events from two workers are merged in timestamp order."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(
        db1,
        worker_name='worker-1',
        events=[
            {'ts': 1000.0, 'event': 'consumed', 'partition': 0, 'offset': 1},
            {'ts': 1002.0, 'event': 'task_completed', 'partition': 0, 'task_id': 't1'},
            {'ts': 1004.0, 'event': 'committed', 'partition': 0, 'offset': 1},
        ],
    )
    _create_source_db(
        db2,
        worker_name='worker-2',
        events=[
            {'ts': 1001.0, 'event': 'consumed', 'partition': 1, 'offset': 10},
            {'ts': 1003.0, 'event': 'task_completed', 'partition': 1, 'task_id': 't2'},
        ],
    )
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.event_count == 5
    assert result.worker_count == 2

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    rows = merged.execute('SELECT * FROM events ORDER BY id').fetchall()

    # events should be in timestamp order (1000, 1001, 1002, 1003, 1004)
    timestamps = [r['ts'] for r in rows]
    assert timestamps == sorted(timestamps)

    # verify worker_id foreign keys are correct
    workers = {r['id']: r['worker_name'] for r in merged.execute('SELECT * FROM workers')}
    for row in rows:
        wname = workers[row['worker_id']]
        if row['partition'] == 0:
            assert wname == 'worker-1'
        elif row['partition'] == 1:
            assert wname == 'worker-2'
    merged.close()


def test_merge_two_workers_separate_states(tmp_path):
    """Each worker's state snapshot is preserved with correct worker_id."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(
        db1,
        worker_name='worker-1',
        state={'consumed_count': 100, 'completed_count': 90},
    )
    _create_source_db(
        db2,
        worker_name='worker-2',
        state={'consumed_count': 200, 'completed_count': 180},
    )
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.state_count == 2

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    states = merged.execute(
        'SELECT ws.*, w.worker_name FROM worker_states ws JOIN workers w ON ws.worker_id = w.id'
    ).fetchall()
    by_name = {s['worker_name']: s for s in states}
    assert by_name['worker-1']['consumed_count'] == 100
    assert by_name['worker-2']['consumed_count'] == 200
    merged.close()


def test_merge_workers_table_has_all_config(tmp_path):
    """Workers table preserves full config from each source DB."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(db1, worker_name='worker-1', cluster_name='alpha')
    _create_source_db(db2, worker_name='worker-2', cluster_name='beta')
    output = str(tmp_path / 'merged.db')

    merge_databases([str(db1), str(db2)], output)

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    workers = merged.execute('SELECT * FROM workers ORDER BY worker_name').fetchall()
    assert len(workers) == 2
    assert workers[0]['worker_name'] == 'worker-1'
    assert workers[0]['cluster_name'] == 'alpha'
    assert workers[0]['source_file'] == 'w1.db'
    assert workers[1]['worker_name'] == 'worker-2'
    assert workers[1]['cluster_name'] == 'beta'
    merged.close()


# ─── merge_databases — cluster detection ─────────────────────


def test_merge_same_cluster_detected(tmp_path):
    """When all DBs share the same cluster, result.cluster_name is set."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(db1, worker_name='w1', cluster_name='main cluster')
    _create_source_db(db2, worker_name='w2', cluster_name='main cluster')
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.cluster_name == 'main cluster'


def test_merge_mixed_clusters_no_cluster(tmp_path):
    """When DBs come from different clusters, result.cluster_name is empty."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(db1, worker_name='w1', cluster_name='main')
    _create_source_db(db2, worker_name='w2', cluster_name='fast')
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.cluster_name == ''


def test_merge_some_without_cluster_no_cluster(tmp_path):
    """If any DB lacks a cluster, result.cluster_name is empty."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(db1, worker_name='w1', cluster_name='main')
    _create_source_db(db2, worker_name='w2', cluster_name=None)
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.cluster_name == ''


def test_merge_single_db_with_cluster(tmp_path):
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, worker_name='w1', cluster_name='prod')
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.cluster_name == 'prod'


# ─── merge_databases — missing tables ────────────────────────


def test_merge_db_without_events_table(tmp_path):
    """DB with only worker_config (no events) still contributes a worker row."""
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, worker_name='config-only', skip_events_table=True)
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.worker_count == 1
    assert result.event_count == 0

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    workers = merged.execute('SELECT * FROM workers').fetchall()
    assert workers[0]['worker_name'] == 'config-only'
    merged.close()


def test_merge_db_without_config_creates_placeholder(tmp_path):
    """DB with no worker_config gets a placeholder worker row from filename."""
    db1 = tmp_path / 'orphan-worker.db'
    _create_source_db(
        db1,
        skip_config_table=True,
        events=[{'ts': 1000.0, 'event': 'consumed', 'partition': 0}],
    )
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.worker_count == 1
    assert result.event_count == 1

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    workers = merged.execute('SELECT * FROM workers').fetchall()
    assert workers[0]['worker_name'] == 'orphan-worker'
    assert workers[0]['source_file'] == 'orphan-worker.db'

    events = merged.execute('SELECT * FROM events').fetchall()
    assert events[0]['worker_id'] == workers[0]['id']
    merged.close()


def test_merge_db_without_state_table(tmp_path):
    """DB with no worker_state — no state row added."""
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, skip_state_table=True)
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.state_count == 0

    merged = sqlite3.connect(output)
    rows = merged.execute('SELECT COUNT(*) FROM worker_states').fetchone()
    assert rows[0] == 0
    merged.close()


def test_merge_mixed_tables(tmp_path):
    """Merge DBs where one has full tables and another has only events."""
    db1 = tmp_path / 'full.db'
    db2 = tmp_path / 'events-only.db'
    _create_source_db(
        db1,
        worker_name='full-worker',
        cluster_name='main',
        events=[{'ts': 1000.0, 'event': 'consumed'}],
        state={'consumed_count': 50},
    )
    _create_source_db(
        db2,
        skip_config_table=True,
        skip_state_table=True,
        events=[{'ts': 1001.0, 'event': 'consumed'}],
    )
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.worker_count == 2
    assert result.event_count == 2
    assert result.state_count == 1


# ─── merge_databases — foreign key integrity ─────────────────


def test_merge_event_worker_fk_valid(tmp_path):
    """Every event's worker_id references a valid workers row."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(
        db1,
        worker_name='w1',
        events=[
            {'ts': 1000.0, 'event': 'consumed'},
            {'ts': 1001.0, 'event': 'task_completed'},
        ],
    )
    _create_source_db(
        db2,
        worker_name='w2',
        events=[
            {'ts': 1000.5, 'event': 'consumed'},
        ],
    )
    output = str(tmp_path / 'merged.db')
    merge_databases([str(db1), str(db2)], output)

    merged = sqlite3.connect(output)
    merged.execute('PRAGMA foreign_keys=ON')
    # this query would fail if FK is violated
    orphans = merged.execute(
        'SELECT COUNT(*) FROM events WHERE worker_id NOT IN (SELECT id FROM workers)'
    ).fetchone()
    assert orphans[0] == 0
    merged.close()


def test_merge_state_worker_fk_valid(tmp_path):
    """Every worker_state's worker_id references a valid workers row."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(db1, worker_name='w1', state={'consumed_count': 10})
    _create_source_db(db2, worker_name='w2', state={'consumed_count': 20})
    output = str(tmp_path / 'merged.db')
    merge_databases([str(db1), str(db2)], output)

    merged = sqlite3.connect(output)
    orphans = merged.execute(
        'SELECT COUNT(*) FROM worker_states WHERE worker_id NOT IN (SELECT id FROM workers)'
    ).fetchone()
    assert orphans[0] == 0
    merged.close()


def test_merge_fk_join_events_to_workers(tmp_path):
    """Can join events to workers and get correct worker_name per event."""
    db1 = tmp_path / 'w1.db'
    db2 = tmp_path / 'w2.db'
    _create_source_db(
        db1,
        worker_name='alpha',
        events=[{'ts': 1000.0, 'event': 'consumed', 'partition': 0}],
    )
    _create_source_db(
        db2,
        worker_name='beta',
        events=[{'ts': 1001.0, 'event': 'consumed', 'partition': 1}],
    )
    output = str(tmp_path / 'merged.db')
    merge_databases([str(db1), str(db2)], output)

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    rows = merged.execute(
        'SELECT e.*, w.worker_name FROM events e JOIN workers w ON e.worker_id = w.id ORDER BY e.ts'
    ).fetchall()
    assert rows[0]['worker_name'] == 'alpha'
    assert rows[0]['partition'] == 0
    assert rows[1]['worker_name'] == 'beta'
    assert rows[1]['partition'] == 1
    merged.close()


# ─── merge_databases — indexes ────────────────────────────────


def test_merge_creates_indexes(tmp_path):
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1)
    output = str(tmp_path / 'merged.db')
    merge_databases([str(db1)], output)

    merged = sqlite3.connect(output)
    indexes = {
        row[0]
        for row in merged.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
    }
    expected = {
        'idx_events_ts',
        'idx_events_event',
        'idx_events_task_id',
        'idx_events_partition',
        'idx_events_worker',
        'idx_events_worker_ts',
        'idx_states_worker',
        'idx_states_updated',
    }
    assert expected.issubset(indexes)
    merged.close()


# ─── merge_databases — overwrites existing output ────────────


def test_merge_overwrites_existing_output(tmp_path):
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, worker_name='new-worker')
    output = str(tmp_path / 'merged.db')

    # create a stale output file
    old = sqlite3.connect(output)
    old.execute('CREATE TABLE stale (x TEXT)')
    old.close()

    merge_databases([str(db1)], output)

    merged = sqlite3.connect(output)
    tables = {
        row[0] for row in merged.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert 'stale' not in tables
    assert 'workers' in tables
    merged.close()


# ─── merge_databases — event ordering across many workers ────


def test_merge_three_workers_global_order(tmp_path):
    """Events from three workers are merged in correct global timestamp order."""
    dbs = []
    for i, (name, base_ts) in enumerate([('w1', 1000), ('w2', 999), ('w3', 1001)]):
        p = tmp_path / f'{name}.db'
        events = [
            {'ts': base_ts + j * 0.5, 'event': 'consumed', 'partition': i, 'offset': j}
            for j in range(5)
        ]
        _create_source_db(p, worker_name=name, events=events)
        dbs.append(str(p))

    output = str(tmp_path / 'merged.db')
    result = merge_databases(dbs, output)

    assert result.event_count == 15
    assert result.worker_count == 3

    merged = sqlite3.connect(output)
    timestamps = [r[0] for r in merged.execute('SELECT ts FROM events ORDER BY id')]
    assert timestamps == sorted(timestamps)
    merged.close()


# ─── merge_databases — result metadata ────────────────────────


def test_merge_result_source_files(tmp_path):
    db1 = tmp_path / 'worker-1.db'
    db2 = tmp_path / 'worker-2.db'
    _create_source_db(db1, worker_name='w1')
    _create_source_db(db2, worker_name='w2')
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1), str(db2)], output)

    assert result.source_files == ['worker-1.db', 'worker-2.db']


# ─── merge_databases — empty inputs ──────────────────────────


def test_merge_empty_db_list(tmp_path):
    output = str(tmp_path / 'merged.db')
    result = merge_databases([], output)

    assert result.worker_count == 0
    assert result.event_count == 0
    assert os.path.isfile(output)


def test_merge_db_with_no_events(tmp_path):
    """DB that has events table but no rows."""
    db1 = tmp_path / 'empty.db'
    _create_source_db(db1, worker_name='empty-worker', events=[])
    output = str(tmp_path / 'merged.db')

    result = merge_databases([str(db1)], output)

    assert result.worker_count == 1
    assert result.event_count == 0


# ─── merge_databases — event data fidelity ────────────────────


def test_merge_preserves_all_event_columns(tmp_path):
    """All event columns are correctly transferred to the merged DB."""
    db1 = tmp_path / 'w1.db'
    events = [
        {
            'ts': 1000.0,
            'event': 'task_completed',
            'partition': 3,
            'offset': 42,
            'task_id': 'task-abc',
            'args': '["--input", "foo"]',
            'stdout_size': 256,
            'stdout': 'output text',
            'stderr': 'warning msg',
            'exit_code': 0,
            'duration': 2.5,
            'output_topic': 'results',
            'metadata': '{"slot": 2}',
            'pid': 12345,
        },
    ]
    _create_source_db(db1, worker_name='w1', events=events)
    output = str(tmp_path / 'merged.db')
    merge_databases([str(db1)], output)

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    row = merged.execute('SELECT * FROM events').fetchone()
    assert row['ts'] == 1000.0
    assert row['event'] == 'task_completed'
    assert row['partition'] == 3
    assert row['offset'] == 42
    assert row['task_id'] == 'task-abc'
    assert row['args'] == '["--input", "foo"]'
    assert row['stdout_size'] == 256
    assert row['stdout'] == 'output text'
    assert row['stderr'] == 'warning msg'
    assert row['exit_code'] == 0
    assert row['duration'] == 2.5
    assert row['output_topic'] == 'results'
    assert row['metadata'] == '{"slot": 2}'
    assert row['pid'] == 12345
    merged.close()


def test_merge_preserves_worker_config_fields(tmp_path):
    """All worker_config fields are correctly transferred to workers table."""
    db1 = tmp_path / 'w1.db'
    _create_source_db(db1, worker_name='w1', cluster_name='prod')
    output = str(tmp_path / 'merged.db')
    merge_databases([str(db1)], output)

    merged = sqlite3.connect(output)
    merged.row_factory = sqlite3.Row
    w = merged.execute('SELECT * FROM workers').fetchone()
    assert w['worker_name'] == 'w1'
    assert w['cluster_name'] == 'prod'
    assert w['ip_address'] == '10.0.0.1'
    assert w['debug_port'] == 8080
    assert w['kafka_brokers'] == 'kafka:9092'
    assert w['source_topic'] == 'search-requests'
    assert w['consumer_group'] == 'test-group'
    assert w['binary_path'] == '/usr/bin/rg'
    assert w['max_workers'] == 4
    assert w['task_timeout_seconds'] == 120
    assert w['max_retries'] == 3
    assert w['window_size'] == 10
    sinks = json.loads(w['sinks_json'])
    assert 'kafka' in sinks
    merged.close()
