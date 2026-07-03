"""GitHub Releases fetch + safe archive extraction for UI bundles.

A UI release asset is a gzipped tarball whose files sit at the archive root
(``index.html`` at the top level) — the drakkar-ui release workflow produces
that shape. This module mirrors the Go backend's ``internal/uihost/fetch.go``
so both backends fetch, validate, and cache bundles identically.
"""

from __future__ import annotations

import json
import os
import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from typing import IO, Any

import httpx

# Default GitHub API base. Overridable per call so tests (and GitHub
# Enterprise deployments) can point the engine at another server.
GITHUB_API_BASE = 'https://api.github.com'

# Caps total extracted size to defuse a decompression bomb in a
# malicious/corrupt release asset (a UI bundle is well under this).
MAX_BUNDLE_BYTES = 100 * 1024 * 1024  # 100 MiB
# Caps the downloaded tarball size.
MAX_ASSET_BYTES = 50 * 1024 * 1024  # 50 MiB
# Caps the release-metadata JSON response.
_MAX_RELEASE_JSON_BYTES = 1 * 1024 * 1024  # 1 MiB

# Fallback per-request timeout when the caller supplies no deadline.
_DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0


class FetchError(Exception):
    """A UI bundle could not be fetched, validated, or extracted."""


def fetch_latest_version(api_base: str, repo: str, *, deadline: float | None = None) -> str:
    """Return the latest release tag for the ``owner/name`` repo.

    ``deadline`` is an absolute ``time.monotonic()`` instant bounding the
    request; ``None`` uses a per-request default timeout.
    """
    release = _get_release(f'{api_base}/repos/{repo}/releases/latest', deadline=deadline)
    tag = release.get('tag_name') or ''
    if not isinstance(tag, str) or not tag:
        raise FetchError('latest release has no tag_name')
    return tag


def fetch_release(api_base: str, repo: str, version: str, dest_dir: Path, *, deadline: float | None = None) -> None:
    """Download repo's release tagged ``version`` and extract it into ``dest_dir``.

    Extraction is atomic: the tarball unpacks into a sibling per-process
    ``<dest_dir>.<pid>.incoming`` directory which is swapped into place only
    after it passes structural validation (``index.html`` at the bundle
    root), so a partially written cache is never served.
    """
    release = _get_release(f'{api_base}/repos/{repo}/releases/tags/{version}', deadline=deadline)
    assets = release.get('assets')
    asset = pick_bundle_asset(assets if isinstance(assets, list) else [])
    if asset is None:
        raise FetchError(f'release {version} has no .tar.gz bundle asset')
    download_url = asset.get('browser_download_url')
    if not isinstance(download_url, str) or not download_url:
        raise FetchError(f'release {version} bundle asset has no download URL')

    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    # Per-process staging name: concurrent workers fetching the same missing
    # version into the shared cache must never delete or interleave each
    # other's in-flight extraction. The '.incoming' suffix is what
    # newest_cached_version (both backends) filters on.
    incoming = dest_dir.with_name(f'{dest_dir.name}.{os.getpid()}.incoming')
    shutil.rmtree(incoming, ignore_errors=True)
    try:
        # Buffer the tarball to a temp file so the download cap applies
        # before any extraction work starts.
        with tempfile.TemporaryFile() as tarball:
            _download(download_url, tarball, deadline=deadline)
            tarball.seek(0)
            _extract_tar_gz(tarball, incoming)
        if not (incoming / 'index.html').is_file():
            raise FetchError(f'release {version} bundle has no index.html at its root')
    except BaseException:
        shutil.rmtree(incoming, ignore_errors=True)
        raise
    # Swap into place. rmtree + rename is not perfectly atomic, but the
    # window is tiny and a concurrent reader sees either the old or new tree.
    shutil.rmtree(dest_dir, ignore_errors=True)
    incoming.replace(dest_dir)


def pick_bundle_asset(assets: list[Any]) -> dict[str, Any] | None:
    """Select the UI bundle asset — the first ``.tar.gz`` / ``.tgz`` (case-insensitive)."""
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        name = str(asset.get('name', '')).lower()
        if name.endswith(('.tar.gz', '.tgz')):
            return asset
    return None


def _auth_headers() -> dict[str, str]:
    """Optional ``GITHUB_TOKEN`` bearer auth (env only) for rate limits / private repos."""
    token = os.environ.get('GITHUB_TOKEN', '')
    if token:
        return {'Authorization': f'Bearer {token}'}
    return {}


def _remaining(deadline: float | None) -> float:
    """Seconds left until ``deadline``; raises once the budget is spent."""
    if deadline is None:
        return _DEFAULT_REQUEST_TIMEOUT_SECONDS
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise FetchError('UI resolve deadline exceeded')
    return remaining


def _get_release(url: str, *, deadline: float | None) -> dict[str, Any]:
    """GET and decode one GitHub release object."""
    headers = {'Accept': 'application/vnd.github+json', **_auth_headers()}
    try:
        with httpx.Client(timeout=_remaining(deadline), follow_redirects=True) as client:
            resp = client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        raise FetchError(f'github releases API {url}: {exc}') from exc
    if resp.status_code != 200:
        raise FetchError(f'github releases API {url}: status {resp.status_code}')
    if len(resp.content) > _MAX_RELEASE_JSON_BYTES:
        raise FetchError(f'release JSON exceeds {_MAX_RELEASE_JSON_BYTES} bytes')
    try:
        release = json.loads(resp.content)
    except json.JSONDecodeError as exc:
        raise FetchError(f'decoding release JSON: {exc}') from exc
    if not isinstance(release, dict):
        raise FetchError('release JSON is not an object')
    return release


def _download(url: str, out: IO[bytes], *, deadline: float | None) -> None:
    """Stream the asset into ``out``, enforcing the download cap and deadline.

    httpx drops the ``Authorization`` header on cross-origin redirects, so
    following GitHub's asset redirect to its CDN never leaks the token.
    """
    try:
        with (
            httpx.Client(timeout=_remaining(deadline), follow_redirects=True) as client,
            client.stream('GET', url, headers=_auth_headers()) as resp,
        ):
            if resp.status_code != 200:
                raise FetchError(f'downloading {url}: status {resp.status_code}')
            written = 0
            for chunk in resp.iter_bytes():
                if deadline is not None and time.monotonic() > deadline:
                    raise FetchError('UI resolve deadline exceeded during download')
                written += len(chunk)
                if written > MAX_ASSET_BYTES:
                    raise FetchError(f'bundle asset exceeds {MAX_ASSET_BYTES} bytes')
                out.write(chunk)
    except httpx.HTTPError as exc:
        raise FetchError(f'downloading {url}: {exc}') from exc


def _extract_tar_gz(fileobj: IO[bytes], dest_dir: Path) -> None:
    """Unpack a gzipped tar stream into ``dest_dir``.

    Rejects path traversal (zip-slip), caps total extracted bytes
    (decompression bomb), and skips any non-regular, non-directory entry
    (symlinks, devices, fifos) for safety — a UI bundle never needs them.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_root = dest_dir.resolve()
    written = 0
    try:
        with tarfile.open(fileobj=fileobj, mode='r:gz') as tar:
            for member in tar:
                target = _safe_join(dest_root, member.name)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                elif member.isreg():
                    if member.size > MAX_BUNDLE_BYTES - written:
                        raise FetchError(f'bundle exceeds {MAX_BUNDLE_BYTES} bytes')
                    target.parent.mkdir(parents=True, exist_ok=True)
                    extracted = tar.extractfile(member)
                    if extracted is None:
                        continue
                    with extracted, open(target, 'wb') as dst:
                        shutil.copyfileobj(extracted, dst)
                    written += member.size
                # else: skip symlinks, devices, fifos.
    except tarfile.TarError as exc:
        raise FetchError(f'reading tar: {exc}') from exc


def _safe_join(dest_root: Path, name: str) -> Path:
    """Join ``name`` under ``dest_root``, rejecting ``../`` traversal in entry names.

    ``dest_root`` must already be resolved. Because extraction never creates
    symlinks, resolving the joined path here is a pure lexical normalization
    of the archive entry name — no attacker-controlled link can redirect it.
    """
    target = (dest_root / name).resolve()
    if target != dest_root and dest_root not in target.parents:
        raise FetchError(f'unsafe path in archive: {name!r}')
    return target
