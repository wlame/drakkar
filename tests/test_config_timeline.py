"""Tests for the ``ui.timeline`` config models.

Covers defaults, the single-mapping-``when`` normalization, condition/op/field
validation, color validation, and the ``extra='forbid'`` guard on every model
in the block.
"""

import pytest
from pydantic import ValidationError

from drakkar.config import (
    TimelineColorRule,
    TimelineLabels,
    TimelineRuleCondition,
    UITimelineConfig,
)


def test_timeline_defaults():
    timeline = UITimelineConfig()
    assert timeline.history_factor == 100
    assert timeline.max_age_minutes == 60
    assert timeline.color_rules == []
    assert timeline.labels == TimelineLabels()
    assert timeline.labels.tag == ''
    assert timeline.labels.caption == ''
    assert timeline.labels.highlight == ''
    assert timeline.labels.filter == ''
    assert timeline.labels.marker == ''


def test_rule_single_mapping_when_normalizes_to_list():
    rule = TimelineColorRule(when={'label': 'env', 'op': 'eq', 'value': 'prod'}, color='green')
    assert isinstance(rule.when, list)
    assert len(rule.when) == 1
    assert rule.when[0].label == 'env'


def test_rule_list_when_with_two_conditions():
    rule = TimelineColorRule(
        when=[
            {'label': 'env', 'op': 'eq', 'value': 'prod'},
            {'field': 'status', 'op': 'eq', 'value': 'failed'},
        ],
        color='red',
    )
    assert len(rule.when) == 2


def test_condition_unknown_op_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='status', op='between', value='x')


def test_condition_unknown_field_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='not_a_field', op='eq', value='x')


def test_rule_unknown_color_name_fails():
    with pytest.raises(ValidationError):
        TimelineColorRule(when={'label': 'env', 'op': 'eq', 'value': 'prod'}, color='mauve')


def test_rule_bad_hex_color_fails():
    with pytest.raises(ValidationError):
        TimelineColorRule(when={'label': 'env', 'op': 'eq', 'value': 'prod'}, color='#12345')


def test_rule_valid_hex_color_passes():
    rule = TimelineColorRule(when={'label': 'env', 'op': 'eq', 'value': 'prod'}, color='#A1b2c3')
    assert rule.color == '#A1b2c3'


def test_condition_contains_on_numeric_field_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='stdout_size', op='contains', value='x')


def test_condition_gt_on_string_field_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='status', op='gt', value=1)


def test_condition_gt_on_label_passes():
    condition = TimelineRuleCondition(label='priority', op='gt', value=1)
    assert condition.label == 'priority'


def test_condition_exists_with_value_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='status', op='exists', value='x')


def test_condition_eq_without_value_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='status', op='eq')


def test_condition_both_label_and_field_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(label='env', field='status', op='eq', value='x')


def test_condition_neither_label_nor_field_fails():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(op='eq', value='x')


def test_rule_empty_when_list_fails():
    with pytest.raises(ValidationError):
        TimelineColorRule(when=[], color='green')


def test_timeline_51_rules_fails():
    rule = {'when': {'label': 'env', 'op': 'eq', 'value': 'prod'}, 'color': 'green'}
    with pytest.raises(ValidationError):
        UITimelineConfig(color_rules=[rule] * 51)


def test_timeline_50_rules_passes():
    rule = {'when': {'label': 'env', 'op': 'eq', 'value': 'prod'}, 'color': 'green'}
    timeline = UITimelineConfig(color_rules=[rule] * 50)
    assert len(timeline.color_rules) == 50


def test_condition_extra_key_forbidden():
    with pytest.raises(ValidationError):
        TimelineRuleCondition(field='status', op='eq', value='x', extra_key='nope')


def test_rule_extra_key_forbidden():
    with pytest.raises(ValidationError):
        TimelineColorRule(
            when={'label': 'env', 'op': 'eq', 'value': 'prod'},
            color='green',
            extra_key='nope',
        )


def test_labels_extra_key_forbidden():
    with pytest.raises(ValidationError):
        TimelineLabels(tag='x', extra_key='nope')


def test_timeline_block_extra_key_forbidden():
    with pytest.raises(ValidationError):
        UITimelineConfig(extra_key='nope')


def test_timeline_history_factor_zero_fails():
    with pytest.raises(ValidationError):
        UITimelineConfig(history_factor=0)


def test_timeline_max_age_minutes_over_cap_fails():
    with pytest.raises(ValidationError):
        UITimelineConfig(max_age_minutes=1441)
