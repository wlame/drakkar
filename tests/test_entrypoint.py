"""Tests for the integration worker entrypoint.sh — DB path construction per worker.

The entrypoint sets DRAKKAR_DEBUG__DB_PATH to a per-worker base path.
Timestamping and rotation are handled by the recorder (see test_recorder.py).
"""

import os
import subprocess
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).resolve().parent.parent / 'integration' / 'worker' / 'entrypoint.sh'


def _run_entrypoint(shared_dir: Path, worker_id: str = 'worker-1') -> str:
    """Run entrypoint.sh with a patched SHARED_DIR and return stdout."""
    script = ENTRYPOINT.read_text().replace(
        'DRAKKAR_DEBUG__DB_PATH="/shared/',
        f'DRAKKAR_DEBUG__DB_PATH="{shared_dir}/',
    )
    # Replace exec "$@" with env dump so we can inspect the exported var
    script = script.replace('exec "$@"', 'echo "DB_PATH=$DRAKKAR_DEBUG__DB_PATH"')
    env = {
        'PATH': os.environ['PATH'],
        'WORKER_ID': worker_id,
    }
    result = subprocess.run(['bash', '-c', script], env=env, capture_output=True, text=True)
    assert result.returncode == 0, f'entrypoint failed: {result.stderr}'
    return result.stdout.strip()


def _get_db_path(output: str) -> str:
    """Extract DB_PATH value from entrypoint output."""
    for line in output.split('\n'):
        if line.startswith('DB_PATH='):
            return line.split('DB_PATH=', 1)[1]
    pytest.fail(f'DB_PATH not found in output: {output}')


# --- Path construction ---


def test_entrypoint_includes_worker_name_in_path(tmp_path):
    output = _run_entrypoint(tmp_path, worker_id='worker-1')
    db_path = _get_db_path(output)
    assert 'worker-1' in db_path


def test_entrypoint_different_workers_get_different_paths(tmp_path):
    path1 = _get_db_path(_run_entrypoint(tmp_path, worker_id='worker-1'))
    path2 = _get_db_path(_run_entrypoint(tmp_path, worker_id='worker-2'))
    path3 = _get_db_path(_run_entrypoint(tmp_path, worker_id='worker-3'))

    assert path1 != path2 != path3
    assert 'worker-1' in path1
    assert 'worker-2' in path2
    assert 'worker-3' in path3


def test_entrypoint_path_is_in_shared_dir(tmp_path):
    output = _run_entrypoint(tmp_path, worker_id='worker-1')
    db_path = _get_db_path(output)
    assert db_path.startswith(str(tmp_path))


def test_entrypoint_path_ends_with_db_extension(tmp_path):
    output = _run_entrypoint(tmp_path, worker_id='worker-1')
    db_path = _get_db_path(output)
    assert db_path.endswith('.db')


def test_entrypoint_path_is_base_path_without_timestamp(tmp_path):
    """The entrypoint sets a base path — no timestamp suffix.

    The recorder's _make_db_path() is responsible for adding timestamps.
    """
    output = _run_entrypoint(tmp_path, worker_id='worker-1')
    db_path = _get_db_path(output)
    filename = Path(db_path).name
    assert filename == 'drakkar-debug-worker-1.db'


def test_entrypoint_defaults_worker_name(tmp_path):
    """Without WORKER_ID, falls back to 'worker'."""
    script = ENTRYPOINT.read_text().replace(
        'DRAKKAR_DEBUG__DB_PATH="/shared/',
        f'DRAKKAR_DEBUG__DB_PATH="{tmp_path}/',
    )
    script = script.replace('exec "$@"', 'echo "DB_PATH=$DRAKKAR_DEBUG__DB_PATH"')
    # No WORKER_ID in env
    env = {'PATH': os.environ['PATH']}
    result = subprocess.run(['bash', '-c', script], env=env, capture_output=True, text=True)
    assert result.returncode == 0
    db_path = _get_db_path(result.stdout.strip())
    assert 'drakkar-debug-worker.db' in db_path


# --- Integration with recorder's naming ---


def test_recorder_make_db_path_adds_timestamp_to_base():
    """Verify the recorder correctly timestamps the base path the entrypoint produces."""
    from drakkar.recorder import _make_db_path

    base = '/shared/drakkar-debug-worker-1.db'
    result = _make_db_path(base)
    assert result.startswith('/shared/drakkar-debug-worker-1-')
    assert result.endswith('.db')
    # Should contain a date-time pattern
    filename = Path(result).stem
    assert '__' in filename  # _make_db_path uses YYYY-MM-DD__HH_MM_SS


def test_recorder_list_db_files_finds_worker_specific_files(tmp_path):
    """Verify the recorder's glob finds files for the right worker only."""
    from drakkar.recorder import _list_db_files

    # Simulate files from two workers
    (tmp_path / 'drakkar-debug-worker-1-2026-03-23__10_00_00.db').touch()
    (tmp_path / 'drakkar-debug-worker-1-2026-03-23__11_00_00.db').touch()
    (tmp_path / 'drakkar-debug-worker-2-2026-03-23__10_00_00.db').touch()

    w1_files = _list_db_files(str(tmp_path / 'drakkar-debug-worker-1.db'))
    w2_files = _list_db_files(str(tmp_path / 'drakkar-debug-worker-2.db'))

    assert len(w1_files) == 2
    assert all('worker-1' in f for f in w1_files)
    assert len(w2_files) == 1
    assert 'worker-2' in w2_files[0]
