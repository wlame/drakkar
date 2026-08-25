"""Tests for the ``drakkar-ui`` cache-management CLI.

The CLI mirrors the Go backend's ``cmd/drakkar-ui`` byte-for-byte — same
subcommands (``where`` / ``fetch`` / ``update``), flags, output shapes, and
exit codes (0 ok, 1 runtime error, 2 usage error) — so cache-management
commands are interchangeable between the two backends. Network tests run
against the same ``StubGitHub`` the resolver tests use.
"""

import io

from drakkar.uihost.cli import run
from tests.test_uihost import BUNDLE_FILES, make_tar_gz, seed_cache
from tests.test_uihost import stub_github as stub_github


def invoke(*argv: str) -> tuple[int, str, str]:
    stdout, stderr = io.StringIO(), io.StringIO()
    code = run(list(argv), stdout, stderr)
    return code, stdout.getvalue(), stderr.getvalue()


# --- usage surface ---------------------------------------------------------


def test_no_arguments_prints_usage_to_stderr():
    code, out, err = invoke()
    assert code == 2
    assert out == ''
    assert 'Usage:' in err and 'drakkar-ui where' in err


def test_unknown_command_is_a_usage_error():
    code, _, err = invoke('frobnicate')
    assert code == 2
    assert 'unknown command "frobnicate"' in err


def test_help_prints_usage_to_stdout():
    code, out, _ = invoke('--help')
    assert code == 0
    assert 'drakkar-ui update' in out


def test_fetch_requires_version():
    code, _, err = invoke('fetch')
    assert code == 2
    assert 'fetch requires --version=<tag>' in err


# --- where (offline cache introspection) ------------------------------------


def test_where_empty_cache_reports_nothing(tmp_path):
    code, out, _ = invoke('where', f'--cache-dir={tmp_path}')
    assert code == 0
    assert f'cache root:     {tmp_path}' in out
    assert 'pinned version: (none)' in out
    assert 'would serve:    nothing' in out
    assert 'newest cached:' not in out


def test_where_unpinned_reports_newest_cached(tmp_path):
    seed_cache(tmp_path, 'v1.0.0')
    seed_cache(tmp_path, 'v1.2.0')
    code, out, _ = invoke('where', f'--cache-dir={tmp_path / "cache"}')
    assert code == 0
    assert 'newest cached:  v1.2.0' in out
    assert f'version dir:    {tmp_path / "cache" / "v1.2.0"}' in out
    assert 'would serve:    cache' in out


def test_where_pinned_and_cached(tmp_path):
    seed_cache(tmp_path, 'v1.0.0')
    code, out, _ = invoke('where', f'--cache-dir={tmp_path / "cache"}', '--version=v1.0.0')
    assert code == 0
    assert 'pinned version: v1.0.0' in out
    assert 'cached:         true' in out  # lowercase — matches the Go CLI
    assert 'would serve:    cache' in out


def test_where_pinned_but_not_cached(tmp_path):
    code, out, _ = invoke('where', f'--cache-dir={tmp_path}', '--version=v9.9.9')
    assert code == 0
    assert 'cached:         false' in out
    assert 'would serve:    nothing' in out


# --- fetch / update (network, against the stub) ------------------------------


def test_fetch_downloads_specific_version(stub_github, tmp_path):
    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.0.0', make_tar_gz(BUNDLE_FILES))
    code, out, _ = invoke('fetch', '--version=v1.0.0', f'--cache-dir={tmp_path}', f'--api-base={stub_github.base_url}')
    assert code == 0
    assert f'fetched v1.0.0 into {tmp_path / "v1.0.0"}' in out
    assert (tmp_path / 'v1.0.0' / 'index.html').is_file()


def test_update_fetches_latest(stub_github, tmp_path):
    stub_github.add_direct_release('wlame/drakkar-ui', 'v1.2.0', make_tar_gz(BUNDLE_FILES), latest=True)
    code, out, _ = invoke('update', f'--cache-dir={tmp_path}', f'--api-base={stub_github.base_url}')
    assert code == 0
    assert f'updated to v1.2.0 in {tmp_path / "v1.2.0"}' in out
    assert (tmp_path / 'v1.2.0' / 'index.html').is_file()


def test_fetch_missing_release_is_a_runtime_error(stub_github, tmp_path):
    code, out, err = invoke(
        'fetch', '--version=v0.0.9', f'--cache-dir={tmp_path}', f'--api-base={stub_github.base_url}'
    )
    assert code == 1
    assert out == ''
    assert err.startswith('drakkar-ui: ')


def test_update_with_empty_repo_is_a_runtime_error(tmp_path):
    code, _, err = invoke('update', '--repo=', f'--cache-dir={tmp_path}')
    assert code == 1
    assert 'no release repo configured' in err
