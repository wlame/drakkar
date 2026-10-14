"""The archive lock file: one archiver per cluster history folder.

Protocol: exclusive create; a stale lock (older than lock_stale_seconds on
the file server's clock) is taken over once; the holder refreshes the
lock's mtime; a holder checks the lock still holds its token right before
each publish (fencing); release deletes only its own lock.
"""

import json
import os
import re
import stat
import time

import pytest
from structlog.testing import capture_logs

from drakkar.recorder import archive_lock
from drakkar.recorder.archive_lock import LOCK_NAME, ArchiveLock

STALE = 7200.0


def _lock(folder, worker='w-1', instance='1111111111111111', **kwargs) -> ArchiveLock:
    return ArchiveLock(str(folder), worker=worker, instance=instance, stale_seconds=STALE, **kwargs)


def _age_lock(folder, seconds: float) -> None:
    """Make the lock look older on the file server's clock."""
    path = folder / LOCK_NAME
    old = time.time() - seconds
    os.utime(path, (old, old))


def test_acquire_creates_an_owner_only_lock_with_its_body(tmp_path):
    lock = _lock(tmp_path)

    assert lock.acquire() is True

    path = tmp_path / LOCK_NAME
    body = json.loads(path.read_bytes())
    assert body['worker'] == 'w-1'
    assert body['instance'] == '1111111111111111'
    assert re.fullmatch(r'[0-9a-f]{16}', body['token'])
    assert re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}Z', body['acquired_at'])
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    lock.release()


def test_a_second_archiver_is_refused_while_the_lock_is_fresh(tmp_path):
    first = _lock(tmp_path)
    second = _lock(tmp_path, worker='w-2', instance='2222222222222222')
    assert first.acquire()

    assert second.acquire() is False

    assert first.is_ours()
    first.release()


def test_a_stale_lock_is_taken_over_once_and_logged(tmp_path):
    dead = _lock(tmp_path, worker='w-dead', instance='dddddddddddddddd')
    assert dead.acquire()
    dead.stop_refreshing()
    _age_lock(tmp_path, STALE + 60)
    taker = _lock(tmp_path, worker='w-2', instance='2222222222222222')

    with capture_logs() as logs:
        assert taker.acquire() is True

    assert taker.is_ours()
    assert not dead.is_ours()
    (event,) = [entry for entry in logs if entry['event'] == 'recorder_archive_lock_taken_over']
    assert event['previous_worker'] == 'w-dead'
    assert not [p for p in tmp_path.iterdir() if p.name.startswith('.archive.lock.stale.')]
    taker.release()


def test_the_age_check_leaves_no_clock_marker(tmp_path):
    first = _lock(tmp_path)
    assert first.acquire()

    _lock(tmp_path, worker='w-2', instance='2222222222222222').acquire()

    assert sorted(p.name for p in tmp_path.iterdir()) == [LOCK_NAME]
    first.release()


def test_release_deletes_only_its_own_lock(tmp_path):
    dead = _lock(tmp_path, worker='w-dead', instance='dddddddddddddddd')
    assert dead.acquire()
    dead.stop_refreshing()
    _age_lock(tmp_path, STALE + 60)
    taker = _lock(tmp_path, worker='w-2', instance='2222222222222222')
    assert taker.acquire()

    dead.release()

    assert (tmp_path / LOCK_NAME).exists()
    assert taker.is_ours()
    taker.release()
    assert not (tmp_path / LOCK_NAME).exists()


def test_refresh_moves_the_lock_mtime_while_held(tmp_path):
    lock = _lock(tmp_path, refresh_seconds=0.05)
    assert lock.acquire()
    _age_lock(tmp_path, 1000)
    before = (tmp_path / LOCK_NAME).stat().st_mtime

    deadline = time.time() + 2
    while (tmp_path / LOCK_NAME).stat().st_mtime == before and time.time() < deadline:
        time.sleep(0.02)

    assert (tmp_path / LOCK_NAME).stat().st_mtime > before
    lock.release()


def test_a_lock_that_is_not_a_plain_file_is_never_taken_over(tmp_path):
    (tmp_path / LOCK_NAME).mkdir()

    assert _lock(tmp_path).acquire() is False


def test_is_ours_is_false_when_the_lock_is_gone(tmp_path):
    lock = _lock(tmp_path)
    assert lock.acquire()
    (tmp_path / LOCK_NAME).unlink()

    assert lock.is_ours() is False
    lock.release()


@pytest.mark.parametrize('body', [b'{not json', b'[]', b'{"token": 5}'])
def test_an_unreadable_lock_body_is_not_ours(tmp_path, body):
    lock = _lock(tmp_path)
    assert lock.acquire()
    (tmp_path / LOCK_NAME).write_bytes(body)

    assert lock.is_ours() is False
    lock.release()


# --- contested takeovers ---
#
# Each test steps in between the taker's age check and its rename (or its
# put-back) to replay what another worker does at that moment.


def _stale_dead_lock(folder) -> None:
    dead = _lock(folder, worker='w-dead', instance='dddddddddddddddd')
    assert dead.acquire()
    dead.stop_refreshing()
    _age_lock(folder, STALE + 60)


def test_a_taker_that_loses_the_rename_race_stands_down(tmp_path, monkeypatch):
    _stale_dead_lock(tmp_path)
    taker = _lock(tmp_path, worker='w-2', instance='2222222222222222')
    real_rename = archive_lock._rename_aside

    def another_taker_moves_it_first(source, target):
        real_rename(source, tmp_path / '.archive.lock.stale.ffffffffffffffff')
        return real_rename(source, target)

    monkeypatch.setattr(archive_lock, '_rename_aside', another_taker_moves_it_first)

    assert taker.acquire() is False
    assert not (tmp_path / LOCK_NAME).exists()


def test_a_fresh_lock_moved_aside_by_a_racing_taker_is_put_back(tmp_path, monkeypatch):
    _stale_dead_lock(tmp_path)
    holder = _lock(tmp_path, worker='w-2', instance='2222222222222222')
    taker = _lock(tmp_path, worker='w-3', instance='3333333333333333')
    real_rename = archive_lock._rename_aside

    def holder_takes_over_in_between(source, target):
        # After the taker saw the stale lock, another worker took it over
        # and created a fresh one; the taker now moves THAT one aside.
        monkeypatch.setattr(archive_lock, '_rename_aside', real_rename)
        assert holder.acquire()
        return real_rename(source, target)

    monkeypatch.setattr(archive_lock, '_rename_aside', holder_takes_over_in_between)

    assert taker.acquire() is False
    assert holder.is_ours()
    assert sorted(p.name for p in tmp_path.iterdir()) == [LOCK_NAME]
    holder.release()


def test_a_put_back_that_loses_to_a_third_creator_fences_the_holder(tmp_path, monkeypatch):
    _stale_dead_lock(tmp_path)
    holder = _lock(tmp_path, worker='w-2', instance='2222222222222222')
    taker = _lock(tmp_path, worker='w-3', instance='3333333333333333')
    third = _lock(tmp_path, worker='w-4', instance='4444444444444444')
    real_rename = archive_lock._rename_aside
    real_link = archive_lock._link_back

    def holder_takes_over_in_between(source, target):
        monkeypatch.setattr(archive_lock, '_rename_aside', real_rename)
        assert holder.acquire()
        return real_rename(source, target)

    def third_creates_before_the_put_back(source, target):
        assert third.acquire()
        return real_link(source, target)

    monkeypatch.setattr(archive_lock, '_rename_aside', holder_takes_over_in_between)
    monkeypatch.setattr(archive_lock, '_link_back', third_creates_before_the_put_back)

    with capture_logs() as logs:
        assert taker.acquire() is False

    assert not holder.is_ours()
    assert third.is_ours()
    assert [e['event'] for e in logs if e['event'] == 'recorder_archive_lock_put_back_failed'] == [
        'recorder_archive_lock_put_back_failed'
    ]
    assert sorted(p.name for p in tmp_path.iterdir()) == [LOCK_NAME]
    third.release()
