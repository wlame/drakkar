"""GitHub Releases fetch + safe archive extraction for UI bundles.

A UI release asset is a gzipped tarball whose files sit at the archive root
(``index.html`` at the top level) — the drakkar-ui release workflow produces
that shape. This module fetches, validates and caches those bundles.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import tarfile
import tempfile
import time
from pathlib import Path
from typing import IO, Any
from urllib.parse import unquote

import httpx

# Default GitHub API base. Overridable per call so tests (and GitHub
# Enterprise deployments) can point the engine at another server.
GITHUB_API_BASE = 'https://api.github.com'

# Plain-web GitHub host serving release assets and the /releases/latest
# redirect. Unlike api.github.com it is not subject to the anonymous REST
# rate limit (60 req/h per IP), so it is the PRIMARY fetch path for public
# repos; the API remains the fallback (needed for private repos via
# GITHUB_TOKEN and for releases whose asset name deviates from the
# convention).
GITHUB_DOWNLOAD_BASE = 'https://github.com'

# Caps total extracted size to defuse a decompression bomb in a
# malicious/corrupt release asset (a UI bundle is well under this).
MAX_BUNDLE_BYTES = 100 * 1024 * 1024  # 100 MiB
# Caps the downloaded tarball size.
MAX_ASSET_BYTES = 50 * 1024 * 1024  # 50 MiB
# Caps the release-metadata JSON response.
_MAX_RELEASE_JSON_BYTES = 1 * 1024 * 1024  # 1 MiB

# Fallback per-request timeout when the caller supplies no deadline.
_DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0

# A sha256 hex digest — the first token of the optional ``.sha256``
# checksum sidecar published next to the bundle asset.
_HEX64_RE = re.compile(r'[0-9a-fA-F]{64}')


class FetchError(Exception):
    """A UI bundle could not be fetched, validated, or extracted."""


def dir_has_index(directory: Path) -> bool:
    """Whether ``directory`` looks like a usable UI bundle (has ``index.html``)."""
    return (directory / 'index.html').is_file()


def fetch_latest_version(
    api_base: str, repo: str, *, download_base: str = GITHUB_DOWNLOAD_BASE, deadline: float | None = None
) -> str:
    """Return the latest release tag for the ``owner/name`` repo.

    The github.com ``/releases/latest`` redirect is tried first (no API rate
    limit); the REST API is the fallback. ``deadline`` is an absolute
    ``time.monotonic()`` instant bounding each request; ``None`` uses a
    per-request default timeout.
    """
    try:
        return _latest_tag_via_redirect(download_base, repo, deadline=deadline)
    except FetchError as redirect_exc:
        try:
            release = _get_release(f'{api_base}/repos/{repo}/releases/latest', deadline=deadline)
        except FetchError as api_exc:
            raise FetchError(f'{redirect_exc}; {api_exc}') from api_exc
        tag = release.get('tag_name') or ''
        if not isinstance(tag, str) or not tag:
            raise FetchError('latest release has no tag_name') from redirect_exc
        return tag


def _latest_tag_via_redirect(download_base: str, repo: str, *, deadline: float | None) -> str:
    """Resolve the latest release tag WITHOUT the REST API.

    ``GET {download_base}/{repo}/releases/latest`` answers with a redirect
    whose Location ends in ``/releases/tag/<tag>``. A repo with no releases
    serves its releases index without a redirect — reported as an error so
    the caller can fall back to the API.
    """
    url = f'{download_base}/{repo}/releases/latest'
    try:
        with httpx.Client(timeout=_remaining(deadline), follow_redirects=False) as client:
            resp = client.get(url)
    except httpx.HTTPError as exc:
        raise FetchError(f'latest-release redirect {url}: {exc}') from exc
    location = resp.headers.get('location', '')
    marker = '/releases/tag/'
    idx = location.rfind(marker)
    if not (300 <= resp.status_code < 400) or idx < 0:
        raise FetchError(f'no latest-release redirect from {url} (status {resp.status_code})')
    tag = unquote(location[idx + len(marker) :].rstrip('/'))
    if not tag:
        raise FetchError('empty tag in latest-release redirect')
    return tag


def bundle_asset_name(version: str) -> str:
    """Conventional release-asset filename the drakkar-ui workflow produces.

    Deterministic from the tag — which is what lets the direct (API-free)
    download URL be constructed without asset discovery.
    """
    return f'drakkar-ui-{version}.tar.gz'


def fetch_release(
    api_base: str,
    repo: str,
    version: str,
    dest_dir: Path,
    *,
    download_base: str = GITHUB_DOWNLOAD_BASE,
    deadline: float | None = None,
) -> None:
    """Download repo's release tagged ``version`` and extract it into ``dest_dir``.

    The direct ``{download_base}/{repo}/releases/download/{tag}/{asset}`` URL
    is tried first (plain github.com — no API rate limit); asset discovery
    through the REST API is the fallback. Extraction is atomic: the tarball
    unpacks into a sibling ``<dest_dir>.<token>.incoming`` staging directory
    which is swapped into place only after it passes structural validation
    (``index.html`` at the bundle root), so a partially written cache is
    never served.
    """
    dest_dir.parent.mkdir(parents=True, exist_ok=True)
    # The staging dir carries a random token so concurrent workers fetching
    # the same missing version into a shared cache never delete or interleave
    # each other's in-flight extraction. A pid would NOT be unique here:
    # containerized workers are all pid 1, and the cache is shared across
    # containers by design. The '.incoming' suffix is what
    # newest_cached_version filters on.
    incoming = dest_dir.with_name(f'{dest_dir.name}.{secrets.token_hex(4)}.incoming')
    direct_url = f'{download_base}/{repo}/releases/download/{version}/{bundle_asset_name(version)}'
    try:
        # Buffer the tarball to a temp file so the download cap applies
        # before any extraction work starts.
        with tempfile.TemporaryFile() as tarball:
            try:
                _download(direct_url, tarball, deadline=deadline)
            except FetchError as direct_exc:
                _download_via_api(api_base, repo, version, tarball, direct_exc, deadline=deadline)
            _verify_checksum(tarball, f'{direct_url}.sha256', version, deadline=deadline)
            tarball.seek(0)
            _extract_tar_gz(tarball, incoming)
        if not (incoming / 'index.html').is_file():
            raise FetchError(f'release {version} bundle has no index.html at its root')
    except BaseException:
        shutil.rmtree(incoming, ignore_errors=True)
        raise
    _install_bundle(incoming, dest_dir)


def _install_bundle(incoming: Path, dest_dir: Path) -> None:
    """Move a validated staging dir into place.

    Release tags are immutable, so when several workers race to install the
    same version the content is identical — the invariant here is that a
    VALID ``dest_dir`` is never deleted or replaced, only an invalid/partial
    one: whoever installs first wins, and everyone else discards their
    staging copy and reports success.
    """
    if dir_has_index(dest_dir):
        shutil.rmtree(incoming, ignore_errors=True)  # another worker already installed it
        return
    try:
        incoming.rename(dest_dir)
        return
    except OSError:
        pass
    # Rename failed — either dest_dir appeared concurrently (valid → we lost
    # the race, fine) or an invalid leftover occupies the name: move it aside
    # (never delete in place — it could become valid mid-check) and retry once.
    if dir_has_index(dest_dir):
        shutil.rmtree(incoming, ignore_errors=True)
        return
    trash = dest_dir.with_name(f'{dest_dir.name}.{secrets.token_hex(4)}.incoming')
    try:
        dest_dir.rename(trash)
    except FileNotFoundError:
        pass
    except OSError:
        shutil.rmtree(incoming, ignore_errors=True)
        raise
    shutil.rmtree(trash, ignore_errors=True)
    try:
        incoming.rename(dest_dir)
    except OSError:
        shutil.rmtree(incoming, ignore_errors=True)
        if dir_has_index(dest_dir):
            return  # lost the final race to a valid install
        raise


def _verify_checksum(tarball: IO[bytes], checksum_url: str, version: str, *, deadline: float | None) -> None:
    """Verify the tarball against the release's optional ``.sha256`` sidecar.

    The drakkar-ui release workflow publishes
    ``drakkar-ui-<tag>.tar.gz.sha256`` alongside the bundle. Releases that
    predate the sidecar have no such asset, so an UNAVAILABLE sidecar
    skips verification (the download is already HTTPS-authenticated to
    GitHub). A PRESENT sidecar is binding: a malformed one, or a digest
    that does not match the downloaded bytes, aborts the install before
    any extraction work.
    """
    try:
        with tempfile.TemporaryFile() as sumfile:
            _download(checksum_url, sumfile, deadline=deadline)
            sumfile.seek(0)
            raw = sumfile.read(512)
    except FetchError:
        return  # no sidecar published for this release — skip verification
    fields = raw.decode('ascii', errors='replace').split()
    if not fields or not _HEX64_RE.fullmatch(fields[0]):
        raise FetchError(f'release {version} checksum asset is malformed')
    expected = fields[0].lower()
    digest = hashlib.sha256()
    tarball.seek(0)
    while chunk := tarball.read(1 << 16):
        digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected:
        raise FetchError(f'release {version} bundle checksum mismatch: expected {expected}, got {actual}')


def _download_via_api(
    api_base: str, repo: str, version: str, tarball: IO[bytes], direct_exc: FetchError, *, deadline: float | None
) -> None:
    """Fallback bundle download: discover the asset through the REST API."""
    release = _get_release(f'{api_base}/repos/{repo}/releases/tags/{version}', deadline=deadline)
    assets = release.get('assets')
    asset = pick_bundle_asset(assets if isinstance(assets, list) else [])
    if asset is None:
        raise FetchError(f'release {version} has no .tar.gz bundle asset') from direct_exc
    download_url = asset.get('browser_download_url')
    if not isinstance(download_url, str) or not download_url:
        raise FetchError(f'release {version} bundle asset has no download URL') from direct_exc
    # Drop any bytes the failed direct attempt already buffered.
    tarball.seek(0)
    tarball.truncate()
    _download(download_url, tarball, deadline=deadline)


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
