"""``drakkar-ui`` — manage the decoupled drakkar-ui static bundle.

A thin CLI over :mod:`drakkar.uihost` — the same engine the worker uses at
startup (headless-first parity). It fetches a versioned UI release from
GitHub into the local cache, updates to the latest release, and reports
where the cache lives and what would be served.

The command mirrors the Go backend's ``cmd/drakkar-ui`` byte-for-byte —
same subcommands, flags, output shapes, and exit codes (0 ok, 1 runtime
error, 2 usage error) — and both operate on the same per-user cache, so on
a mixed host either backend's command manages the bundle for both.

Usage::

    drakkar-ui where  [--repo=owner/name] [--version=vX.Y.Z] [--cache-dir=DIR]
    drakkar-ui fetch   --version=vX.Y.Z [--repo=owner/name] [--cache-dir=DIR]
    drakkar-ui update [--repo=owner/name] [--cache-dir=DIR]

The GitHub API base can be overridden with ``--api-base`` for GitHub
Enterprise. A ``GITHUB_TOKEN`` in the environment is used for higher rate
limits / private repos.
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import IO, NoReturn

from drakkar.config import UIReleaseConfig
from drakkar.uihost import (
    GITHUB_API_BASE,
    fetch_latest,
    fetch_version,
    inspect_cache,
)

# The canonical drakkar-ui release repo.
DEFAULT_REPO = 'wlame/drakkar-ui'

# Bounds a single fetch/update network round-trip (matches the Go CLI).
FETCH_TIMEOUT_SECONDS = 60.0

USAGE_TEXT = f"""drakkar-ui — manage the decoupled drakkar-ui static bundle.

Usage:
  drakkar-ui where  [flags]            report the cache location + what would be served
  drakkar-ui fetch   --version=vX [flags]   download a specific UI release into the cache
  drakkar-ui update [flags]            download the latest UI release into the cache

Flags (all subcommands):
  --repo=owner/name   GitHub repo publishing UI bundles (default: {DEFAULT_REPO})
  --version=vX.Y.Z    release tag (required for fetch; selects the row 'where' reports)
  --cache-dir=DIR     bundle cache root (default: per-user cache dir ~/.cache/drakkar/ui)
  --api-base=URL      GitHub API base URL (for GitHub Enterprise)

Environment:
  GITHUB_TOKEN        used for higher rate limits / private repos
"""


class _UsageError(Exception):
    """Raised for malformed flags so ``run`` can exit 2 deterministically."""


class _Parser(argparse.ArgumentParser):
    """ArgumentParser that raises instead of calling ``sys.exit``.

    The CLI writes to explicit stdout/stderr handles (testability, mirroring
    the Go CLI's ``run(argv, stdout, stderr) int`` shape), so argparse's
    default print-and-exit behavior would bypass them.
    """

    def error(self, message: str) -> NoReturn:
        raise _UsageError(message)


def _parse_common(name: str, argv: list[str]) -> argparse.Namespace:
    parser = _Parser(prog=f'drakkar-ui {name}', add_help=False)
    parser.add_argument('--repo', default=DEFAULT_REPO)
    parser.add_argument('--version', default='')
    parser.add_argument('--cache-dir', dest='cache_dir', default='')
    parser.add_argument('--api-base', dest='api_base', default='')
    return parser.parse_args(argv)


def _release_config(opts: argparse.Namespace) -> UIReleaseConfig:
    return UIReleaseConfig(
        repo=opts.repo,
        pinned_version=opts.version,
        cache_dir=opts.cache_dir,
    )


def _api_base(opts: argparse.Namespace) -> str:
    return opts.api_base or GITHUB_API_BASE


def _run_where(argv: list[str], stdout: IO[str]) -> int:
    opts = _parse_common('where', argv)
    status = inspect_cache(_release_config(opts))
    print(f'cache root:     {status.cache_root}', file=stdout)
    if not status.pinned_version:
        print('pinned version: (none)', file=stdout)
        # With no pin, the resolver falls back to the newest cached bundle
        # before the embedded placeholder — report which one that would be.
        if status.fallback_version:
            print(f'newest cached:  {status.fallback_version}', file=stdout)
            print(f'version dir:    {status.fallback_dir}', file=stdout)
    else:
        print(f'pinned version: {status.pinned_version}', file=stdout)
        print(f'version dir:    {status.pinned_dir}', file=stdout)
        # Lowercase booleans keep the report byte-identical to the Go CLI.
        print(f'cached:         {str(status.pinned_cached).lower()}', file=stdout)
    print(f'would serve:    {status.source}', file=stdout)
    return 0


def _run_fetch(argv: list[str], stdout: IO[str], stderr: IO[str]) -> int:
    opts = _parse_common('fetch', argv)
    if not opts.version:
        print('drakkar-ui: fetch requires --version=<tag> (use `update` for the latest)', file=stderr)
        return 2
    deadline = time.monotonic() + FETCH_TIMEOUT_SECONDS
    got = fetch_version(_release_config(opts), opts.version, api_base=_api_base(opts), deadline=deadline)
    print(f'fetched {got.version} into {got.dir}', file=stdout)
    return 0


def _run_update(argv: list[str], stdout: IO[str]) -> int:
    opts = _parse_common('update', argv)
    deadline = time.monotonic() + FETCH_TIMEOUT_SECONDS
    got = fetch_latest(_release_config(opts), api_base=_api_base(opts), deadline=deadline)
    print(f'updated to {got.version} in {got.dir}', file=stdout)
    return 0


def run(argv: list[str], stdout: IO[str], stderr: IO[str]) -> int:
    """The testable CLI body: dispatch on the subcommand, return the exit code."""
    if not argv:
        print(USAGE_TEXT, file=stderr, end='')
        return 2
    command, rest = argv[0], argv[1:]
    try:
        if command == 'where':
            return _run_where(rest, stdout)
        if command == 'fetch':
            return _run_fetch(rest, stdout, stderr)
        if command == 'update':
            return _run_update(rest, stdout)
        if command in ('-h', '--help', 'help'):
            print(USAGE_TEXT, file=stdout, end='')
            return 0
        print(f'drakkar-ui: unknown command "{command}"\n', file=stderr)
        print(USAGE_TEXT, file=stderr, end='')
        return 2
    except _UsageError as exc:
        print(f'drakkar-ui: {exc}', file=stderr)
        return 2
    except Exception as exc:  # runtime failure (network, extraction, config)
        print(f'drakkar-ui: {exc}', file=stderr)
        return 1


def main() -> None:
    """Console-script entry point (``[project.scripts]`` in pyproject)."""
    sys.exit(run(sys.argv[1:], sys.stdout, sys.stderr))
