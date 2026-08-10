"""Unit tests for the integration worker's ui.timeline label helpers.

Pure functions only (no Kafka, no subprocess, no framework wiring), so
these run standalone with plain pytest — no fixtures, no isolation
concerns. Not part of the main ``tests/`` suite (which covers the
``drakkar`` package itself): this mirrors ``handler_test.go`` in the Go
worker so the two languages' label derivation stays provably identical.
"""

import pytest
from handler import _human_file_size, _scan_target_module


@pytest.mark.parametrize(
    ('file_path', 'expected'),
    [
        # Root: os.path.isdir('/') is always true, and the directory
        # branch's fallback (`file_path.rstrip('/') or file_path`) — '/'
        # stripped of trailing slashes is '', which is falsy — returns the
        # path itself rather than ''.
        pytest.param('/', '/', id='root path'),
        # Bare relative name, no directory component: os.path.dirname
        # returns '', so os.path.basename('') is also '' (falsy), and the
        # helper falls back to the target's own base name rather than ''.
        pytest.param('app.py', 'app.py', id='bare filename with no parent directory'),
        # Ordinary file: the module is the immediate parent directory.
        pytest.param('/project/drakkar/app.py', 'drakkar', id='file with a parent directory'),
        # Ordinary nested file two directories deep: same rule, deeper path.
        pytest.param('/project/tests/test_app.py', 'tests', id='file two directories deep'),
    ],
)
def test_scan_target_module_returns_expected_directory_name(file_path: str, expected: str) -> None:
    assert _scan_target_module(file_path) == expected


@pytest.mark.parametrize(
    ('num_bytes', 'expected'),
    [
        (0, '0'),
        (512, '512'),
        (1023, '1023'),
        (1024, '1.0K'),
        (12698, '12.4K'),  # 12698 / 1024 = 12.400...
        (1024 * 1024, '1.0M'),
        (3 * 1024 * 1024, '3.0M'),
    ],
)
def test_human_file_size_formats_bytes_with_k_and_m_suffixes(num_bytes: int, expected: str) -> None:
    assert _human_file_size(num_bytes) == expected
