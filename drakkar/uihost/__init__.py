"""Serve the drakkar web UI as static files, decoupled from the backend.

The UI ships as its own versioned bundle (the separate drakkar-ui repo,
published to GitHub Releases) so every backend on a host serves the same UI
and looks identical. This package is the Python port of the Go backend's
``internal/uihost`` — both resolve bundles into the same per-user cache
directory so a host running mixed backends downloads each release once.

Resolution order (graceful degradation, never fatal):

1. an update check (when ``check_update``) resolves the latest release tag;
   on any failure the pinned version is kept;
2. a previously cached bundle for the resolved version
   (``~/.cache/drakkar/ui/<ver>``) — release tags are immutable, so a cached
   version is never re-downloaded;
3. otherwise fetch that version from GitHub Releases into the cache; on a
   fetch failure a stale cached copy of that version still serves;
4. with no resolvable version at all, the newest previously cached version
   (semver order, lexicographic fallback) serves.

A fetch failure is never fatal — the worker still starts, and serves
whatever the cache holds. There is deliberately no bundle baked into the
package: one download, at any point in the worker's life, fills a cache
that every later start (and every co-located worker of either backend)
reads. When nothing can be resolved the worker runs API-only and the UI
server explains how to supply a bundle — see
:mod:`drakkar.uiserver.routes_spa`.
"""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import structlog

from drakkar.config import UIReleaseConfig
from drakkar.uihost.fetch import (
    GITHUB_API_BASE,
    GITHUB_DOWNLOAD_BASE,
    FetchError,
    dir_has_index,
    fetch_latest_version,
    fetch_release,
)

__all__ = [
    'GITHUB_API_BASE',
    'UI_RESOLVE_TIMEOUT_SECONDS',
    'FetchError',
    'Fetched',
    'ResolvedBundle',
    'Source',
    'Status',
    'cache_root',
    'default_cache_root',
    'dir_has_index',
    'fetch_latest',
    'fetch_latest_version',
    'fetch_release',
    'fetch_version',
    'inspect_cache',
    'newest_cached_version',
    'resolve',
]

logger = structlog.get_logger()

# Identifies which bundle ``resolve`` selected — surfaced in logs.
Source = Literal['cache', 'fetched']

# Bounds the whole startup-time resolution (update check + fetch), matching
# the Go backend's uiResolveTimeout. Resolution failures degrade to a cached
# a cached bundle; they never delay worker startup past this budget.
UI_RESOLVE_TIMEOUT_SECONDS = 30.0

# The drakkar-ui release baked into the package (``just embed-ui vX.Y.Z``
# refreshes it) so SPA mode works fully offline out of the box — mirrors
# Go's go:embed fallback bundle. The bundled release tag lives in the
# VERSION file the embed recipe writes alongside index.html.


# Cached bundle directories are named after release tags (``v1.2.0``).
_SEMVER_TAG_RE = re.compile(r'^v(\d+)\.(\d+)\.(\d+)$')


@dataclass(frozen=True)
class ResolvedBundle:
    """The UI bundle ``resolve`` selected: directory, provenance, version.

    ``version`` is the release tag the directory holds (``v1.2.0``), or
    ``None`` when no tag is known — surfaced by the identity endpoint so
    operators can see which UI a backend actually serves.
    """

    root: Path
    source: Source
    version: str | None = None


def default_cache_root() -> Path:
    """Per-user bundle cache root: ``$XDG_CACHE_HOME/drakkar/ui`` or ``~/.cache/drakkar/ui``.

    This is exactly the directory Go's ``os.UserCacheDir()/drakkar/ui``
    produces on Linux, so the Python and Go backends share one cache.
    """
    xdg = os.environ.get('XDG_CACHE_HOME', '')
    base = Path(xdg) if xdg else Path.home() / '.cache'
    return base / 'drakkar' / 'ui'


def cache_root(config: UIReleaseConfig) -> Path:
    """The configured cache root, defaulting to the per-user cache dir."""
    if config.cache_dir:
        return Path(config.cache_dir)
    return default_cache_root()


# dir_has_index lives in fetch.py (the installer needs it too); re-exported
# here as part of the module's public surface.


def newest_cached_version(root: Path) -> str | None:
    """The newest usable cached version under ``root``, or ``None``.

    "Newest" orders ``v<maj>.<min>.<patch>`` tags by semver; any other
    directory names sort below semver tags, lexicographically among
    themselves. In-flight ``.incoming`` extraction dirs are skipped.
    """
    try:
        candidates = [
            entry.name
            for entry in root.iterdir()
            if entry.is_dir() and not entry.name.endswith('.incoming') and dir_has_index(entry)
        ]
    except OSError:
        return None
    if not candidates:
        return None
    return max(candidates, key=_version_sort_key)


def _version_sort_key(name: str) -> tuple[int, int, int, int, str]:
    """Sort key ranking semver tags above (and among) non-semver names."""
    match = _SEMVER_TAG_RE.match(name)
    if match:
        return (1, int(match.group(1)), int(match.group(2)), int(match.group(3)), name)
    return (0, 0, 0, 0, name)


def resolve(
    config: UIReleaseConfig,
    *,
    api_base: str = GITHUB_API_BASE,
    download_base: str | None = None,
    timeout: float = UI_RESOLVE_TIMEOUT_SECONDS,
) -> ResolvedBundle | None:
    """Select the UI bundle directory to serve.

    Never raises for fetch/cache problems — it degrades along the resolution
    order in the module docstring and logs a structured event for each step.
    Returns ``None`` when nothing could be resolved — no cache, and no
    reachable release source. The worker then runs API-only; see
    :mod:`drakkar.uiserver.routes_spa`.

    ``api_base`` overrides the GitHub API base (GitHub Enterprise, or a stub
    server in tests); when it is overridden and ``download_base`` is not,
    the same server handles the plain-web routes too, mirroring the Go
    backend's single BaseURL override. ``timeout`` bounds the whole
    resolution — update check plus download — as an absolute wall-clock
    budget.
    """
    if download_base is None:
        download_base = api_base if api_base != GITHUB_API_BASE else GITHUB_DOWNLOAD_BASE
    deadline = time.monotonic() + timeout
    version = config.pinned_version

    # An update check resolves the latest tag first; on any failure we keep
    # the pinned version (the fetch below still tries, then we degrade).
    if config.check_update and config.repo:
        try:
            version = fetch_latest_version(api_base, config.repo, download_base=download_base, deadline=deadline)
            logger.info('ui_update_check', category='ui', latest=version)
        except Exception as exc:
            logger.warning('ui_update_check_failed', category='ui', error=str(exc))

    root = cache_root(config)

    if version:
        bundle_dir = root / version
        # 1. Cache hit for the resolved version. Release tags are immutable,
        # so a cached copy never needs re-downloading — even on an update
        # check that resolved "latest" to an already-cached tag.
        if dir_has_index(bundle_dir):
            logger.info('ui_served_from_cache', category='ui', version=version, dir=str(bundle_dir))
            return ResolvedBundle(root=bundle_dir, source='cache', version=version)
        # 2. Fetch the version into the cache.
        if config.repo:
            try:
                fetch_release(
                    api_base, config.repo, version, bundle_dir, download_base=download_base, deadline=deadline
                )
                logger.info('ui_fetched', category='ui', version=version, dir=str(bundle_dir))
                return ResolvedBundle(root=bundle_dir, source='fetched', version=version)
            except Exception as exc:
                logger.warning('ui_fetch_failed', category='ui', version=version, error=str(exc))
            # Fetch failed but a cached copy may have appeared meanwhile
            # (another worker sharing the cache can fill it concurrently).
            if dir_has_index(bundle_dir):
                logger.info('ui_served_from_stale_cache', category='ui', version=version)
                return ResolvedBundle(root=bundle_dir, source='cache', version=version)
        # 3. The pinned version, when the update check resolved a newer tag
        # that could not be fetched: a cached pin is contract-guaranteed and
        # beats every further fallback.
        if config.pinned_version and config.pinned_version != version:
            pinned_dir = root / config.pinned_version
            if dir_has_index(pinned_dir):
                logger.info('ui_served_from_cache', category='ui', version=config.pinned_version, dir=str(pinned_dir))
                return ResolvedBundle(root=pinned_dir, source='cache', version=config.pinned_version)

    # 4. Newest cached bundle — for UNPINNED workers only: with no pin there
    # is no contract guarantee to protect, so the last-fetched release
    # serves both when no version was resolvable (offline restart) and when
    # the resolved latest failed to download. A worker whose PIN cannot be
    # fetched deliberately does NOT fall back to a different cached version.
    if not config.pinned_version:
        newest = newest_cached_version(root)
        if newest is not None:
            logger.info('ui_served_from_cache', category='ui', version=newest, dir=str(root / newest))
            return ResolvedBundle(root=root / newest, source='cache', version=newest)

    # Nothing resolvable and nothing cached. Not fatal: the worker serves
    # its API, probes and WebSocket, and the UI server answers page
    # requests with a 503 naming the ways to supply a bundle.
    logger.warning(
        'ui_bundle_unavailable',
        category='ui',
        cache_root=str(root),
        repo=config.repo,
        hint='no cached bundle and no reachable release source; the worker runs API-only',
    )
    return None


# ---------------------------------------------------------------------------
# Introspection + fetch API the drakkar-ui CLI is a thin projection of (the
# headless-first command surface, mirroring the Go backend's uihost helpers).
# ``resolve`` stays the single function the UI server calls at startup; these
# expose the individual steps — inspect the cache, fetch a pinned version,
# fetch the latest — so the CLI's where / fetch / update subcommands never
# reimplement the engine.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Status:
    """What bundle would be served for a config, from cache state only (no network).

    Backs the ``drakkar-ui where`` output; field-for-field identical to the
    Go backend's ``uihost.Status``.
    """

    cache_root: Path
    pinned_version: str
    pinned_dir: Path | None
    pinned_cached: bool
    fallback_version: str | None
    fallback_dir: Path | None
    #: What would serve today, or ``None`` when the cache is empty and
    #: the worker would run API-only.
    source: Source | None


@dataclass(frozen=True)
class Fetched:
    """The result of downloading a bundle into the cache."""

    version: str
    dir: Path


def inspect_cache(config: UIReleaseConfig) -> Status:
    """Report the offline cache state for ``config`` without any network access."""
    root = cache_root(config)
    if config.pinned_version:
        pinned_dir = root / config.pinned_version
        pinned_cached = dir_has_index(pinned_dir)
        return Status(
            cache_root=root,
            pinned_version=config.pinned_version,
            pinned_dir=pinned_dir,
            pinned_cached=pinned_cached,
            fallback_version=None,
            fallback_dir=None,
            source='cache' if pinned_cached else None,
        )
    # No pinned version: resolve (offline) would serve the newest cached
    # bundle, or nothing at all — report the same.
    newest = newest_cached_version(root)
    return Status(
        cache_root=root,
        pinned_version='',
        pinned_dir=None,
        pinned_cached=False,
        fallback_version=newest,
        fallback_dir=root / newest if newest else None,
        source='cache' if newest else None,
    )


def fetch_version(
    config: UIReleaseConfig,
    version: str,
    *,
    api_base: str = GITHUB_API_BASE,
    download_base: str | None = None,
    deadline: float | None = None,
) -> Fetched:
    """Download ``config.repo``'s release tagged ``version`` into the cache.

    An already-cached valid copy is left in place (release tags are
    immutable, so re-downloading the same tag can never change content).
    Error copy matches the Go CLI so the two backends' commands report
    interchangeably.
    """
    if not config.repo:
        raise FetchError('no release repo configured (set --repo)')
    if not version:
        raise FetchError('no version specified (set --version)')
    if download_base is None:
        download_base = api_base if api_base != GITHUB_API_BASE else GITHUB_DOWNLOAD_BASE
    dest_dir = cache_root(config) / version
    fetch_release(api_base, config.repo, version, dest_dir, download_base=download_base, deadline=deadline)
    return Fetched(version=version, dir=dest_dir)


def fetch_latest(
    config: UIReleaseConfig,
    *,
    api_base: str = GITHUB_API_BASE,
    download_base: str | None = None,
    deadline: float | None = None,
) -> Fetched:
    """Resolve the latest release tag for ``config.repo`` and fetch it."""
    if not config.repo:
        raise FetchError('no release repo configured (set --repo)')
    if download_base is None:
        download_base = api_base if api_base != GITHUB_API_BASE else GITHUB_DOWNLOAD_BASE
    latest = fetch_latest_version(api_base, config.repo, download_base=download_base, deadline=deadline)
    return fetch_version(config, latest, api_base=api_base, download_base=download_base, deadline=deadline)
