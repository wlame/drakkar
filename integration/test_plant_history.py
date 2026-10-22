"""Unit tests for the harness's planted-history generator.

Writes into a temporary folder only: no container, no network. Not part of
the main ``tests/`` suite; they pin that the generator's files are what an
archive pass accepts: sealed recorder files with meta files, all in one
window that is already due.
"""

import os
import time

import pytest
from plant_history import main, plant

from drakkar.merge import merge_databases
from drakkar.recorder.archive import archive_file_name, assign_windows
from drakkar.recorder.history import history_dir, list_sealed, meta_path, read_meta

CLUSTER = 'main cluster'
HOUR = 3600


def test_planted_history_is_one_due_window(tmp_path):
    result = plant(shared_dir=str(tmp_path), cluster=CLUSTER, files=3, events_per_file=5, hours_ago=3)

    folder = history_dir(str(tmp_path), CLUSTER)
    paths = list_sealed(folder)
    assert len(paths) == 3
    for path in paths:
        meta = read_meta(meta_path(path))
        assert meta is not None
        assert meta['cluster'] == CLUSTER
        assert meta['tables']['events'] == 5
        assert path.endswith('.db')
        assert meta['worker'] == os.path.basename(path).rsplit('-', 3)[0]
        assert meta['worker'].startswith('planted-')

    windows = assign_windows(paths, window_seconds=HOUR, due_after_seconds=HOUR, now=time.time())
    assert len(windows) == 1
    window = windows[0]
    assert sorted(candidate.path for candidate in window.files) == sorted(paths)
    assert (window.start, window.end) == (result.window_start, result.window_end)
    assert result.archive == archive_file_name(CLUSTER, window.start, window.end)
    assert result.sources == tuple(sorted(paths))
    assert [entry.name for entry in tmp_path.iterdir() if entry.name.startswith('.')] == []


def test_plant_prints_the_planted_line(tmp_path, capsys):
    exit_code = main(
        [f'--shared-dir={tmp_path}', f'--cluster={CLUSTER}', '--files=2', '--events-per-file=1', '--hours-ago=3']
    )

    assert exit_code == 0
    line = capsys.readouterr().out.strip()
    fields = dict(part.split('=', 1) for part in line.split()[1:])
    assert line.startswith('PLANTED ')
    assert fields['scope'] == 'main_cluster'
    assert fields['files'] == '2'
    start, end = int(fields['window_start']), int(fields['window_end'])
    assert end - start == HOUR
    assert fields['archive'] == archive_file_name(CLUSTER, start, end)


def test_plant_refuses_a_window_whose_archive_exists(tmp_path, capsys):
    result = plant(shared_dir=str(tmp_path), cluster=CLUSTER, files=1, events_per_file=1, hours_ago=3)
    folder = history_dir(str(tmp_path), CLUSTER)
    (tmp_path / 'history' / 'main_cluster' / result.archive).write_bytes(b'')
    before = list_sealed(folder)

    exit_code = main([f'--shared-dir={tmp_path}', f'--cluster={CLUSTER}', '--files=1', '--hours-ago=3'])

    assert exit_code == 1
    assert result.archive in capsys.readouterr().err
    assert list_sealed(folder) == before


@pytest.mark.parametrize('argument', ['--files=0', '--events-per-file=0', '--hours-ago=1'])
def test_plant_refuses_bad_sizes(tmp_path, argument):
    with pytest.raises(SystemExit) as raised:
        main([f'--shared-dir={tmp_path}', argument])

    assert raised.value.code == 2
    assert list_sealed(history_dir(str(tmp_path), CLUSTER)) == []


def test_planted_files_are_all_read_by_the_archive_merge(tmp_path):
    result = plant(shared_dir=str(tmp_path), cluster=CLUSTER, files=2, events_per_file=3, hours_ago=3)

    merged = merge_databases(list(result.sources), str(tmp_path / 'merged.db'))

    assert sorted(merged.merged_files) == sorted(result.sources)
    assert merged.event_count == 6
    assert merged.cluster_name == CLUSTER
