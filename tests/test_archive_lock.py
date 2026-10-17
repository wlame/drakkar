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
import threading
import time
from datetime import datetime
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from drakkar.recorder import archive_lock
from drakkar.recorder.archive_lock import LOCK_NAME, ArchiveLock

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'history_meta.json').read_text(encoding='utf-8'))

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


def test_the_body_names_the_provider(tmp_path):
    expected = FIXTURE['lock']
    lock = _lock(
        tmp_path,
        worker=expected['worker'],
        instance=expected['instance'],
        token=expected['token'],
        auto_refresh=False,
        now=lambda: datetime.fromisoformat(expected['acquired_at']),
    )

    assert lock.acquire() is True

    raw = (tmp_path / LOCK_NAME).read_bytes()
    assert json.loads(raw)['provider'] == 'filesystem'
    assert raw == FIXTURE['lock_bytes'].encode('utf-8')
    lock.release()


def _refresh_threads() -> set[threading.Thread]:
    return {thread for thread in threading.enumerate() if thread.name == 'drakkar-archive-lock'}


def test_auto_refresh_false_starts_no_thread(tmp_path):
    lock = _lock(tmp_path, auto_refresh=False, refresh_seconds=0.01)
    before = _refresh_threads()

    assert lock.acquire() is True

    assert _refresh_threads() - before == set()
    lock.release()


def test_a_given_token_is_the_lock_token(tmp_path):
    lock = _lock(tmp_path, token='0123456789abcdef', auto_refresh=False)

    assert lock.acquire() is True

    assert lock.token == '0123456789abcdef'
    assert json.loads((tmp_path / LOCK_NAME).read_bytes())['token'] == '0123456789abcdef'
    lock.release()


def test_refresh_answers_whether_the_lock_is_still_ours(tmp_path):
    lock = _lock(tmp_path, auto_refresh=False)
    assert lock.acquire()

    assert lock.refresh() is True
    (tmp_path / LOCK_NAME).unlink()
    assert lock.refresh() is False


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
    assert event['provider'] == 'filesystem'
    assert not [p for p in tmp_path.iterdir() if p.name.startswith('.archive.lock.stale.')]
    taker.release()


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


def test_a_lock_planted_as_a_symlink_is_not_ours(tmp_path):
    lock = _lock(tmp_path, auto_refresh=False)
    assert lock.acquire()
    target = tmp_path / 'elsewhere.lock'
    (tmp_path / LOCK_NAME).rename(target)
    (tmp_path / LOCK_NAME).symlink_to(target)

    assert lock.is_ours() is False
    assert lock.refresh() is False


def _failing_lock_reads(monkeypatch, folder: Path) -> None:
    """Make every read-only open of the lock file in ``folder`` fail with an I/O error, like a mount hiccup."""
    real_open = os.open
    lock_path = str(folder / LOCK_NAME)

    def open_with_io_error(path, flags, *args, **kwargs):
        if os.fspath(path) == lock_path and not flags & os.O_CREAT:
            raise OSError(5, 'Input/output error', lock_path)
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, 'open', open_with_io_error)


def test_refresh_raises_when_the_lock_cannot_be_read(tmp_path, monkeypatch):
    # An I/O error says nothing about who holds the lock: it must not read
    # as another holder (False), so the caller sees a failed call instead.
    lock = _lock(tmp_path, auto_refresh=False)
    assert lock.acquire()
    _failing_lock_reads(monkeypatch, tmp_path)

    with pytest.raises(OSError, match='Input/output error'):
        lock.refresh()
    with pytest.raises(OSError, match='Input/output error'):
        lock.is_ours()


def test_refresh_answers_false_for_another_token(tmp_path):
    holder = _lock(tmp_path, token='aaaaaaaaaaaaaaaa', auto_refresh=False)
    other = _lock(tmp_path, token='bbbbbbbbbbbbbbbb', auto_refresh=False)
    assert holder.acquire()

    assert other.refresh() is False
    assert holder.refresh() is True
    holder.release()


def test_the_auto_refresh_thread_survives_a_lock_read_error(tmp_path, monkeypatch):
    lock = _lock(tmp_path, refresh_seconds=0.01)
    before = _refresh_threads()
    assert lock.acquire()
    (refresher,) = _refresh_threads() - before
    _failing_lock_reads(monkeypatch, tmp_path)
    # Several refresh rounds hit the error.
    time.sleep(0.1)

    assert refresher.is_alive()
    monkeypatch.undo()
    lock.release()
    refresher.join(timeout=2)
    assert not refresher.is_alive()


def test_a_stale_lock_whose_body_cannot_be_read_is_still_taken_over(tmp_path, monkeypatch):
    dead = _lock(tmp_path, worker='w-dead', instance='dddddddddddddddd', auto_refresh=False)
    assert dead.acquire()
    _age_lock(tmp_path, STALE + 60)
    taker = _lock(tmp_path, worker='w-2', instance='2222222222222222', auto_refresh=False)
    _failing_lock_reads(monkeypatch, tmp_path)

    with capture_logs() as logs:
        assert taker.acquire() is True

    (event,) = [entry for entry in logs if entry['event'] == 'recorder_archive_lock_taken_over']
    assert event['previous_worker'] is None
    monkeypatch.undo()
    assert taker.is_ours()
    taker.release()


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
    # The holder ends fenced and never releases, so it gets no refresh thread to leak.
    holder = _lock(tmp_path, worker='w-2', instance='2222222222222222', auto_refresh=False)
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
