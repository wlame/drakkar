"""Packaging metadata the published artifacts must carry.

These are cheap file-level assertions rather than a build: running
``uv build`` inside the unit suite would be slow and would need network on a
cold cache. The build itself is exercised by the release workflow; what is
easy to get wrong — and silently — is the metadata that decides whether the
license text is *in* the artifact at all, so that is what is pinned here.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text())
LICENSE = REPO_ROOT / 'LICENSE'


def test_license_file_exists_at_repo_root() -> None:
    """A ``license = "MIT"`` field is metadata, not a grant of permission."""
    assert LICENSE.is_file(), 'LICENSE missing at the repo root'


def test_license_is_the_mit_text_naming_the_copyright_holder() -> None:
    text = LICENSE.read_text()
    assert text.startswith('MIT License')
    assert re.search(r'Copyright \(c\) \d{4} wlame', text), 'no copyright line naming the holder'
    # The two clauses that carry the actual grant and the disclaimer.
    assert 'Permission is hereby granted, free of charge' in text
    assert 'THE SOFTWARE IS PROVIDED "AS IS"' in text


def test_declared_license_matches_the_file() -> None:
    assert PYPROJECT['project']['license'] == 'MIT'


def test_license_file_is_declared_for_the_build_backend() -> None:
    """``license-files`` is what puts the text into the sdist and wheel.

    Without it hatchling ships the metadata field and no text, so PyPI
    shows "MIT" with nothing behind it.
    """
    declared = PYPROJECT['project']['license-files']
    assert 'LICENSE' in declared, f'LICENSE not in license-files: {declared}'


@pytest.mark.parametrize('document', ['README.md', 'CONTRIBUTING.md'])
def test_license_section_links_to_the_file(document: str) -> None:
    """A reader should reach the terms from the document, not just the word."""
    path = REPO_ROOT / document
    if not path.is_file():
        pytest.skip(f'{document} not present')
    text = path.read_text()
    if '## License' not in text:
        pytest.skip(f'{document} has no License section')
    section = text[text.index('## License') :]
    assert 'LICENSE' in section, f'{document} License section does not link to the LICENSE file'
