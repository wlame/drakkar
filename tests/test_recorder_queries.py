"""Unit tests for the recorder's read side.

Every test here builds an :class:`EventQueries` over a plain SQLite file —
no :class:`EventRecorder`, no UI server, no background loops. That is the
point of the split: the queries and the cross-worker sweeps can be checked
against known rows without starting a worker.
"""

from __future__ import annotations

import time

import aiosqlite
import pytest

from drakkar.config import UIConfig
from drakkar.recorder import queries as queries_module
from drakkar.recorder.queries import CROSS_TRACE_MAX_FILES, EventQueries, QueryContext, _ScanBudget
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE

WORKER = 'query-test-worker'
CLUSTER = 'query-test-cluster'


async def make_db(path, *, worker_name=WORKER, cluster_name=CLUSTER, events=(), state_updated_at=None):
    """Create a recorder-shaped SQLite file with the given rows."""
    async with aiosqlite.connect(str(path)) as db:
        await db.executescript(SCHEMA_EVENTS)
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.executescript(SCHEMA_WORKER_STATE)
        await db.execute(
            'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) '
            'VALUES (1, ?, ?, 0.0, ?)',
            (worker_name, cluster_name, '1970-01-01T00:00:00.000000Z'),
        )
        for row in events:
            event = {'dt': '1970-01-01T00:00:00.000000Z', **row}
            columns = ', '.join(event)
            marks = ', '.join('?' * len(event))
            await db.execute(f'INSERT INTO events ({columns}) VALUES ({marks})', tuple(event.values()))
        if state_updated_at is not None:
            await db.execute(
                'INSERT INTO worker_state (updated_at, updated_at_dt) VALUES (?, ?)',
                (state_updated_at, '1970-01-01T00:00:00.000000Z'),
            )
        await db.commit()
    return str(path)


@pytest.fixture
def ui_config(tmp_path):
    return UIConfig(
        recorder={'db_dir': str(tmp_path), 'store_events': True, 'store_config': True},
        release={'enabled': False},
    )


class Ctx:
    """Builds a QueryContext over one open connection, counting flushes."""

    def __init__(self, config, db, db_path, *, worker_name=WORKER, cluster_name=CLUSTER):
        self.flushes = 0
        self.context = QueryContext(
            config=config,
            worker_name=worker_name,
            cluster_name=cluster_name,
            reader=lambda: db,
            db_path=lambda: db_path,
            flush=self._flush,
        )

    async def _flush(self) -> None:
        self.flushes += 1


@pytest.fixture
async def local(tmp_path, ui_config):
    """An EventQueries over a database with a small, known event history."""
    path = await make_db(
        tmp_path / f'{WORKER}-20260101-000000.db',
        events=[
            {'ts': 100.0, 'event': 'consumed', 'partition': 0, 'offset': 10, 'origin': 'kafka'},
            {'ts': 101.0, 'event': 'task_started', 'partition': 0, 'offset': 10, 'task_id': 't1', 'origin': 'kafka'},
            {'ts': 102.0, 'event': 'task_completed', 'partition': 0, 'offset': 10, 'task_id': 't1', 'origin': 'kafka'},
            {'ts': 103.0, 'event': 'task_failed', 'partition': 1, 'offset': 20, 'task_id': 't2', 'origin': 'http'},
            {'ts': 104.0, 'event': 'committed', 'partition': 0, 'offset': 10, 'origin': 'kafka'},
        ],
    )
    async with aiosqlite.connect(path) as db:
        ctx = Ctx(ui_config, db, path)
        yield EventQueries(ctx.context), ctx


class TestGetEvents:
    async def test_returns_newest_first(self, local):
        queries, _ = local

        events = await queries.get_events(limit=10)

        assert [e['event'] for e in events] == [
            'committed',
            'task_failed',
            'task_completed',
            'task_started',
            'consumed',
        ]

    async def test_filters_by_partition_type_origin_and_since(self, local):
        queries, _ = local

        assert {e['event'] for e in await queries.get_events(partition=1)} == {'task_failed'}
        assert {e['event'] for e in await queries.get_events(event_type='consumed')} == {'consumed'}
        assert {e['event'] for e in await queries.get_events(origin='http')} == {'task_failed'}
        assert len(await queries.get_events(since=103.0)) == 2

    async def test_limit_and_offset_page_the_result(self, local):
        queries, _ = local

        first = await queries.get_events(limit=2)
        second = await queries.get_events(limit=2, offset=2)

        assert len(first) == len(second) == 2
        assert {e['id'] for e in first}.isdisjoint({e['id'] for e in second})

    async def test_returns_nothing_when_events_are_not_stored(self, tmp_path, ui_config, local):
        queries, _ = local
        ui_config.recorder.store_events = False

        assert await queries.get_events() == []

    async def test_returns_nothing_without_a_reader(self, ui_config):
        ctx = QueryContext(
            config=ui_config,
            worker_name=WORKER,
            cluster_name=CLUSTER,
            reader=lambda: None,
            db_path=lambda: '',
            flush=_noop_flush,
        )

        assert await EventQueries(ctx).get_events() == []

    async def test_does_not_flush_for_a_plain_listing(self, local):
        """``get_events`` backs the polled history page — flushing there would
        force a write on every poll."""
        queries, ctx = local

        await queries.get_events()

        assert ctx.flushes == 0


async def _noop_flush() -> None:
    return None


class TestTraces:
    async def test_get_trace_returns_the_whole_message_lifecycle(self, local):
        queries, _ = local

        events = await queries.get_trace(partition=0, msg_offset=10)

        assert [e['event'] for e in events] == ['consumed', 'task_started', 'task_completed', 'committed']

    async def test_get_trace_flushes_first(self, local):
        """An operator tracing a message must see events recorded a moment ago."""
        queries, ctx = local

        await queries.get_trace(partition=0, msg_offset=10)

        assert ctx.flushes == 1

    async def test_get_task_events_are_chronological(self, local):
        queries, _ = local

        events = await queries.get_task_events('t1')

        assert [e['event'] for e in events] == ['task_started', 'task_completed']

    async def test_partition_summary_counts_per_partition(self, local):
        queries, _ = local

        summary = {row['partition']: row for row in await queries.get_partition_summary()}

        assert summary[0]['consumed_count'] == 1
        assert summary[0]['completed_count'] == 1
        assert summary[0]['last_committed_offset'] == 10
        assert summary[1]['failed_count'] == 1


class TestCrossWorkerSweep:
    async def test_local_hit_never_touches_peer_databases(self, local, tmp_path):
        queries, _ = local
        # A peer that would answer too — if the local DB is consulted first,
        # it is never opened.
        await make_db(tmp_path / 'peer-20260101-000000.db', worker_name='peer')

        events = await queries.cross_trace(partition=0, msg_offset=10)

        assert {e['worker_name'] for e in events} == {WORKER}

    async def test_falls_back_to_a_peers_live_database(self, tmp_path, ui_config):
        peer_path = await make_db(
            tmp_path / 'peer-20260101-000000.db',
            worker_name='peer',
            events=[
                {'ts': 1.0, 'event': 'consumed', 'partition': 7, 'offset': 70},
                {'ts': 2.0, 'event': 'committed', 'partition': 7, 'offset': 70},
            ],
        )
        (tmp_path / 'peer-live.db').symlink_to(peer_path)
        empty = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')

        async with aiosqlite.connect(empty) as db:
            queries = EventQueries(Ctx(ui_config, db, empty).context)
            events = await queries.cross_trace(partition=7, msg_offset=70)

        assert [e['event'] for e in events] == ['consumed', 'committed']
        assert {e['worker_name'] for e in events} == {'peer'}

    async def test_peer_of_another_cluster_is_skipped(self, tmp_path, ui_config):
        peer_path = await make_db(
            tmp_path / 'peer-20260101-000000.db',
            worker_name='peer',
            cluster_name='some-other-cluster',
            events=[{'ts': 1.0, 'event': 'consumed', 'partition': 7, 'offset': 70}],
        )
        (tmp_path / 'peer-live.db').symlink_to(peer_path)
        empty = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')

        async with aiosqlite.connect(empty) as db:
            queries = EventQueries(Ctx(ui_config, db, empty).context)
            events = await queries.cross_trace(partition=7, msg_offset=70)

        assert events == []

    async def test_unreadable_peer_does_not_abort_the_sweep(self, tmp_path, ui_config):
        broken = tmp_path / 'broken-20260101-000000.db'
        broken.write_bytes(b'this is not a database')
        (tmp_path / 'broken-live.db').symlink_to(broken)
        good_path = await make_db(
            tmp_path / 'good-20260101-000000.db',
            worker_name='good',
            events=[{'ts': 1.0, 'event': 'consumed', 'partition': 7, 'offset': 70}],
        )
        (tmp_path / 'good-live.db').symlink_to(good_path)
        empty = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')

        async with aiosqlite.connect(empty) as db:
            queries = EventQueries(Ctx(ui_config, db, empty).context)
            events = await queries.cross_trace(partition=7, msg_offset=70)

        assert {e['worker_name'] for e in events} == {'good'}

    async def test_rotated_files_are_searched_after_live_peers(self, tmp_path, ui_config):
        await make_db(
            tmp_path / 'archived-20251231-235959.db',
            worker_name='archived',
            events=[{'ts': 1.0, 'event': 'consumed', 'partition': 9, 'offset': 90}],
        )
        empty = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')

        async with aiosqlite.connect(empty) as db:
            queries = EventQueries(Ctx(ui_config, db, empty).context)
            events = await queries.cross_trace(partition=9, msg_offset=90)

        assert {e['worker_name'] for e in events} == {'archived'}

    async def test_miss_returns_empty(self, local):
        queries, _ = local

        assert await queries.cross_trace(partition=99, msg_offset=99) == []


class TestDiscoverWorkers:
    async def test_lists_peers_with_liveness(self, tmp_path, ui_config, monkeypatch):
        import time

        now = time.time()
        fresh = await make_db(
            tmp_path / 'fresh-20260101-000000.db',
            worker_name='fresh',
            state_updated_at=now,
        )
        stale = await make_db(
            tmp_path / 'stale-20260101-000000.db',
            worker_name='stale',
            state_updated_at=now - 3600,
        )
        (tmp_path / 'fresh-live.db').symlink_to(fresh)
        (tmp_path / 'stale-live.db').symlink_to(stale)
        own = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')

        async with aiosqlite.connect(own) as db:
            queries = EventQueries(Ctx(ui_config, db, own).context)
            workers = {w['worker_name']: w for w in await queries.discover_workers()}

        assert workers['fresh']['online'] is True
        assert workers['stale']['online'] is False, 'a crashed worker leaves its symlink behind'
        assert WORKER not in workers, 'a worker must not discover itself'

    async def test_falls_back_to_the_newest_event_when_state_is_off(self, tmp_path, ui_config):
        import time

        peer = await make_db(
            tmp_path / 'eventsonly-20260101-000000.db',
            worker_name='eventsonly',
            events=[{'ts': time.time(), 'event': 'consumed', 'partition': 0, 'offset': 1}],
        )
        (tmp_path / 'eventsonly-live.db').symlink_to(peer)
        own = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')

        async with aiosqlite.connect(own) as db:
            queries = EventQueries(Ctx(ui_config, db, own).context)
            (worker,) = await queries.discover_workers()

        assert worker['worker_name'] == 'eventsonly'
        assert worker['online'] is True

    async def test_returns_nothing_when_config_is_not_stored(self, tmp_path, ui_config):
        own = await make_db(tmp_path / f'{WORKER}-20260101-000000.db')
        ui_config.recorder.store_config = False

        async with aiosqlite.connect(own) as db:
            queries = EventQueries(Ctx(ui_config, db, own).context)

            assert await queries.discover_workers() == []


class TestScanBudget:
    """The bound on a cross-worker sweep (file count + wall clock)."""

    def test_scan_budget_stops_at_file_limit(self):
        """A cross-trace sweep must not open unbounded database files.

        A miss — the common case when an operator pastes an offset that is not in
        this cluster — walks every candidate database in db_dir, which defaults to
        /tmp and accumulates live plus rotated files from every co-located worker.
        The sweep runs on the MAIN loop, so an unbounded scan stalls Kafka polling,
        and refreshing the page repeats it.
        """
        budget = _ScanBudget()
        for i in range(CROSS_TRACE_MAX_FILES):
            assert budget.allow(), f'budget exhausted early, at file {i}'
        assert not budget.truncated, 'reported truncation while within limits'
        assert not budget.allow(), 'allowed more than CROSS_TRACE_MAX_FILES'
        assert budget.truncated, 'hitting the file limit must be recorded'

    def test_scan_budget_stops_at_deadline(self):
        """The wall-clock bound protects against slow shared or network storage,
        where the file count alone is a poor proxy for cost."""
        budget = _ScanBudget()
        # Expire the deadline directly rather than sleeping out the whole budget.
        budget._deadline = time.monotonic() - 1
        assert not budget.allow(), 'an expired budget must not allow further work'
        assert budget.truncated, 'deadline expiry must be recorded as truncation'

    def test_scan_budget_reports_truncation_once(self, monkeypatch):
        """Truncation must be visible: 'we stopped looking' and 'it is not there'
        are very different answers during an incident."""
        logged: list[tuple] = []
        monkeypatch.setattr(
            queries_module.logger,
            'warning',
            lambda event, **kw: logged.append((event, kw)),
        )

        quiet = _ScanBudget()
        quiet.report('cross_trace')
        assert logged == [], 'a sweep within budget must not warn'

        hit = _ScanBudget()
        hit.truncated = True
        hit.report('cross_trace')
        assert len(logged) == 1
        assert logged[0][0] == 'cross_trace_scan_truncated'
        assert logged[0][1]['op'] == 'cross_trace'
