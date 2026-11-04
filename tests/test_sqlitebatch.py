"""Keyset batches of a SQLite table, bounded by rows and by text bytes."""

import sqlite3

import pytest

from drakkar.sqlitebatch import BATCH_BYTES, BATCH_ROWS, fetch_rows, keyset_batches, row_bytes

SQL = 'SELECT id, body FROM t WHERE id > ? ORDER BY id LIMIT ?'


@pytest.fixture
def conn():
    db = sqlite3.connect(':memory:')
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)')
    yield db
    db.close()


def _fill(db: sqlite3.Connection, count: int, body: str = 'b') -> None:
    db.executemany('INSERT INTO t (id, body) VALUES (?, ?)', [(i, body) for i in range(1, count + 1)])


def test_defaults_are_5000_rows_and_8_mib():
    assert (BATCH_ROWS, BATCH_BYTES) == (5000, 8 * 1024 * 1024)


def test_row_bytes_counts_text_and_blob_lengths_plus_a_cost_per_value():
    assert row_bytes([1, 'abc', b'xy', None]) == 4 * 8 + 3 + 2


@pytest.mark.parametrize(('count', 'expected'), [(0, []), (5, [2, 2, 1]), (6, [2, 2, 2])])
def test_keyset_batches_cover_every_row_once(conn, count, expected):
    _fill(conn, count)

    batches = list(keyset_batches(conn, SQL, batch_rows=2))

    assert [len(batch) for batch in batches] == expected
    assert [row[0] for batch in batches for row in batch] == list(range(1, count + 1))


def test_keyset_batches_end_a_batch_at_the_byte_bound_and_go_on(conn):
    _fill(conn, 5, body='x' * 100)

    batches = list(keyset_batches(conn, SQL, batch_rows=10, max_bytes=250))

    assert [len(batch) for batch in batches] == [3, 2]


def test_fetch_rows_says_whether_the_bytes_cut_the_batch(conn):
    _fill(conn, 3, body='x' * 100)

    assert fetch_rows(conn, SQL, (0, 10), 1000) == ([(1, 'x' * 100), (2, 'x' * 100), (3, 'x' * 100)], False)
    rows, cut = fetch_rows(conn, SQL, (0, 10), 100)
    assert (len(rows), cut) == (1, True)


def test_fetch_rows_raises_a_sqlite_error_as_it_is(conn):
    with pytest.raises(sqlite3.OperationalError):
        fetch_rows(conn, 'SELECT * FROM missing', (), 10)
