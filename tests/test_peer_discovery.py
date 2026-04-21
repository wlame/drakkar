"""Tests for the shared peer discovery helper.

The helper powers both the recorder's own `discover_workers()` and the
upcoming cache peer-sync loop. Both scan a shared `db_dir` for live
symlinks following the `<worker>{suffix}` convention (e.g. `-live.db`,
`-cache.db`) and return resolved target paths.
"""

import os
from pathlib import Path

import pytest

from drakkar.peer_discovery import discover_peer_dbs

# --- helpers -----------------------------------------------------------------


async def _collect(db_dir: str, suffix: str, self_worker_name: str) -> list[tuple[str, str]]:
    """Drain the async iterator into a list so tests can easily assert on it."""
    results = []
    async for pair in discover_peer_dbs(db_dir, suffix, self_worker_name):
        results.append(pair)
    return results


def _make_symlink(db_dir: Path, worker_name: str, suffix: str, target_name: str) -> Path:
    """Create a live-symlink `<worker><suffix>` pointing at `target_name`.

    The target file is NOT created by this helper — callers do so explicitly
    so tests can simulate broken symlinks.
    """
    link = db_dir / f'{worker_name}{suffix}'
    os.symlink(target_name, link)
    return link


# --- tests -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_finds_symlinks_matching_suffix(tmp_path):
    """Scan returns all live-symlinks matching the given suffix."""
    # two workers with valid -live.db files + symlinks
    target_a = tmp_path / 'worker-a-2026-04-21__10_00_00.db'
    target_a.write_text('fake db a')
    target_b = tmp_path / 'worker-b-2026-04-21__10_05_00.db'
    target_b.write_text('fake db b')
    _make_symlink(tmp_path, 'worker-a', '-live.db', target_a.name)
    _make_symlink(tmp_path, 'worker-b', '-live.db', target_b.name)

    results = await _collect(str(tmp_path), '-live.db', self_worker_name='me')

    names = sorted(name for name, _ in results)
    assert names == ['worker-a', 'worker-b']
    # resolved target paths must point at the actual DB files
    paths = {name: path for name, path in results}
    assert paths['worker-a'] == str(target_a.resolve())
    assert paths['worker-b'] == str(target_b.resolve())


@pytest.mark.asyncio
async def test_excludes_self_worker(tmp_path):
    """The scan must skip our own symlink so we don't peer with ourselves."""
    target_self = tmp_path / 'me-2026-04-21__10_00_00.db'
    target_self.write_text('fake self db')
    target_other = tmp_path / 'other-2026-04-21__10_00_00.db'
    target_other.write_text('fake other db')
    _make_symlink(tmp_path, 'me', '-live.db', target_self.name)
    _make_symlink(tmp_path, 'other', '-live.db', target_other.name)

    results = await _collect(str(tmp_path), '-live.db', self_worker_name='me')

    names = [name for name, _ in results]
    assert names == ['other']


@pytest.mark.asyncio
async def test_skips_broken_symlink(tmp_path):
    """Symlink whose target file does not exist is silently skipped."""
    # broken symlink — target never created
    _make_symlink(tmp_path, 'ghost-worker', '-live.db', 'nonexistent-file.db')

    # one healthy peer alongside
    target_ok = tmp_path / 'ok-worker-2026-04-21__10_00_00.db'
    target_ok.write_text('fake db')
    _make_symlink(tmp_path, 'ok-worker', '-live.db', target_ok.name)

    results = await _collect(str(tmp_path), '-live.db', self_worker_name='me')

    names = [name for name, _ in results]
    assert names == ['ok-worker']


@pytest.mark.asyncio
async def test_missing_db_dir_returns_empty(tmp_path):
    """Non-existent db_dir does not raise; yields nothing."""
    ghost_dir = tmp_path / 'does-not-exist'
    results = await _collect(str(ghost_dir), '-live.db', self_worker_name='me')
    assert results == []


@pytest.mark.asyncio
async def test_empty_db_dir_returns_empty(tmp_path):
    """Empty existing db_dir yields nothing."""
    results = await _collect(str(tmp_path), '-live.db', self_worker_name='me')
    assert results == []


@pytest.mark.asyncio
async def test_empty_string_db_dir_returns_empty():
    """Empty-string db_dir (un-configured) yields nothing, no raise."""
    results = await _collect('', '-live.db', self_worker_name='me')
    assert results == []


@pytest.mark.asyncio
async def test_ignores_non_symlink_files(tmp_path):
    """A regular file whose name matches the suffix is not a peer symlink."""
    # plain file, not a symlink — mimics an accidentally-named file
    plain = tmp_path / 'fake-worker-live.db'
    plain.write_text('not a symlink')

    # one real peer alongside
    target_ok = tmp_path / 'real-worker-2026-04-21__10_00_00.db'
    target_ok.write_text('fake db')
    _make_symlink(tmp_path, 'real-worker', '-live.db', target_ok.name)

    results = await _collect(str(tmp_path), '-live.db', self_worker_name='me')

    names = [name for name, _ in results]
    assert names == ['real-worker']


@pytest.mark.asyncio
async def test_different_suffix_filters_correctly(tmp_path):
    """Passing a different suffix (e.g. `-cache.db`) only matches that family."""
    # a recorder live link + a cache live link — suffix selects which to return
    live_target = tmp_path / 'w-live-target.db'
    live_target.write_text('x')
    cache_target = tmp_path / 'w-cache-target.db'
    cache_target.write_text('y')
    _make_symlink(tmp_path, 'w', '-live.db', live_target.name)
    _make_symlink(tmp_path, 'w', '-cache.db', cache_target.name)

    # ask only for -cache.db
    results = await _collect(str(tmp_path), '-cache.db', self_worker_name='me')

    assert len(results) == 1
    name, path = results[0]
    assert name == 'w'
    assert path == str(cache_target.resolve())
