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


# Tools that only ever run during development. None of them belongs in a
# published extra: `pip install py-drakkar[dev]` should not exist, because a
# consumer of the library has no use for the test runner or the linter.
DEV_ONLY_TOOLS = frozenset(
    {'pytest', 'pytest-asyncio', 'pytest-cov', 'ruff', 'ty', 'mkdocs', 'mkdocs-material', 'jsonschema'}
)


def _requirement_names(specifiers: list[str]) -> set[str]:
    """Strip version specifiers and extras from a dependency list."""
    return {re.split(r'[<>=!~\[; ]', spec)[0].strip().lower() for spec in specifiers}


def test_published_extras_carry_nothing_dev_only() -> None:
    """Extras are installable by end users; dependency-groups are not."""
    for name, specifiers in PYPROJECT['project'].get('optional-dependencies', {}).items():
        leaked = _requirement_names(specifiers) & DEV_ONLY_TOOLS
        assert not leaked, f'extra {name!r} publishes dev-only tools: {sorted(leaked)}'


def test_perf_is_the_only_published_extra() -> None:
    """One runtime opt-in (orjson); everything else is a dependency group."""
    assert set(PYPROJECT['project'].get('optional-dependencies', {})) == {'perf'}


def test_dev_tooling_is_not_duplicated_between_extras_and_groups() -> None:
    """Two lists of the same tools have to be kept equal by hand."""
    extras = set()
    for specifiers in PYPROJECT['project'].get('optional-dependencies', {}).values():
        extras |= _requirement_names(specifiers)
    groups = _requirement_names(PYPROJECT.get('dependency-groups', {}).get('dev', []))
    overlap = extras & groups
    # orjson is legitimately in both: the `perf` extra is a user-facing
    # runtime opt-in, and the dev group pins it so the development
    # environment always resolves it.
    assert overlap <= {'orjson'}, f'duplicated between extras and dependency-groups: {sorted(overlap)}'


def test_dev_group_carries_every_tool_the_recipes_need() -> None:
    """`uv run` installs the dev group by default — it must be complete."""
    group = _requirement_names(PYPROJECT['dependency-groups']['dev'])
    missing = DEV_ONLY_TOOLS - group
    assert not missing, f'dev dependency-group is missing: {sorted(missing)}'


def test_justfile_does_not_select_a_dev_extra() -> None:
    """A recipe passing --extra=dev re-syncs the venv and evicts group-only packages."""
    justfile = (REPO_ROOT / 'justfile').read_text()
    offenders = [line.strip() for line in justfile.splitlines() if '--extra=dev' in line]
    assert not offenders, 'recipes still select the removed dev extra: ' + '; '.join(offenders)


def test_agents_md_directory_map_matches_the_tree() -> None:
    """AGENTS.md is a committed deliverable an agent reads before the code.

    A map naming a directory that no longer exists (``templates/`` survived
    its v1.19 deletion here) sends a reader looking for something that is
    not there.
    """
    agents = (REPO_ROOT / 'AGENTS.md').read_text()
    block = re.search(r'^drakkar/\n(.*?)^```', agents, re.S | re.M)
    assert block, 'AGENTS.md has no drakkar/ directory map'
    listed = re.findall(r'^  ([a-z_]+(?:\.py)?/?)', block.group(1), re.M)
    assert listed, 'directory map parsed as empty — did its format change?'
    missing = [entry for entry in listed if not (REPO_ROOT / 'drakkar' / entry.rstrip('/')).exists()]
    assert not missing, f'AGENTS.md maps paths that do not exist: {missing}'
