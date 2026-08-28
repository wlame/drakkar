"""Validation tests for ui.timeline.events (TimelineEventType)."""

import pytest
from pydantic import ValidationError

from drakkar.config import TimelineEventType, UITimelineConfig


def _decl(**overrides) -> dict:
    base = {'name': 'deploy_marker', 'kind': 'marker', 'color': 'purple'}
    base.update(overrides)
    return base


def test_events_default_is_empty_list():
    assert UITimelineConfig().events == []


def test_minimal_marker_declaration_fills_defaults():
    t = TimelineEventType.model_validate(_decl())
    assert (t.line, t.label, t.enabled, t.show, t.link, t.action) == ('solid', '', True, True, '', 'none')


def test_range_and_flag_reject_line_style():
    with pytest.raises(ValidationError, match='line'):
        TimelineEventType.model_validate(_decl(kind='range', line='dotted'))


def test_invalid_name_shape_fails():
    with pytest.raises(ValidationError, match='name'):
        TimelineEventType.model_validate(_decl(name='Deploy-Marker'))


def test_unknown_color_fails():
    with pytest.raises(ValidationError, match='color'):
        TimelineEventType.model_validate(_decl(color='mauve'))


def test_hex_color_accepted():
    assert TimelineEventType.model_validate(_decl(color='#1f2a44')).color == '#1f2a44'


def test_action_link_requires_link_template():
    with pytest.raises(ValidationError, match='link'):
        TimelineEventType.model_validate(_decl(action='link'))


def test_link_without_link_action_fails():
    with pytest.raises(ValidationError, match='link'):
        TimelineEventType.model_validate(_decl(link='https://x/{ts_ms}'))


def test_duplicate_names_fail_at_the_list():
    with pytest.raises(ValidationError, match='duplicate'):
        UITimelineConfig.model_validate({'events': [_decl(), _decl()]})


def test_more_than_fifty_declarations_fail():
    events = [_decl(name=f'ev_{i}') for i in range(51)]
    with pytest.raises(ValidationError):
        UITimelineConfig.model_validate({'events': events})


def test_extra_key_forbidden():
    with pytest.raises(ValidationError):
        TimelineEventType.model_validate(_decl(style='bold'))
