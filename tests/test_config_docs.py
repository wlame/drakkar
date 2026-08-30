"""Validation tests for ui.docs (UIDocsConfig / DocsAnchor)."""

import pytest
from pydantic import ValidationError

from drakkar.config import DocsAnchor, UIDocsConfig


def _anchor(**over) -> dict:
    base = {'match': {'label': 'module'}, 'path': 'architecture/scanners/#modules'}
    base.update(over)
    return base


def test_docs_defaults_are_off():
    cfg = UIDocsConfig()
    assert (cfg.site_dir, cfg.title, cfg.anchors) == ('', 'Docs', [])


def test_anchor_match_requires_exactly_one_selector():
    with pytest.raises(ValidationError, match='exactly one'):
        DocsAnchor.model_validate(_anchor(match={'label': 'module', 'sink': 'archive_results_db'}))
    with pytest.raises(ValidationError, match='exactly one'):
        DocsAnchor.model_validate(_anchor(match={}))


def test_anchor_value_only_with_label():
    with pytest.raises(ValidationError, match='value'):
        DocsAnchor.model_validate(_anchor(match={'sink': 'archive_results_db', 'value': 'x'}))
    ok = DocsAnchor.model_validate(_anchor(match={'label': 'module', 'value': 'vendor'}))
    assert ok.match.value == 'vendor'


@pytest.mark.parametrize('bad_path', ['../escape.md', '/absolute.md', 'https://example.com/page', 'a/../../b'])
def test_anchor_path_rejects_traversal_absolute_and_scheme(bad_path):
    with pytest.raises(ValidationError, match='path'):
        DocsAnchor.model_validate(_anchor(path=bad_path))


def test_anchor_path_accepts_relative_with_fragment():
    assert DocsAnchor.model_validate(_anchor()).path == 'architecture/scanners/#modules'


def test_more_than_200_anchors_fail():
    anchors = [_anchor(match={'label': f'k{i}'}) for i in range(201)]
    with pytest.raises(ValidationError):
        UIDocsConfig.model_validate({'anchors': anchors})


def test_extra_keys_forbidden():
    with pytest.raises(ValidationError):
        UIDocsConfig.model_validate({'site_dir': '', 'unknown': 1})
