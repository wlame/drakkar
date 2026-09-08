"""Config schema for the two input sources (sources.kafka, sources.http)."""

import pytest
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, KafkaConfig, KafkaSourceConfig, SourcesConfig, WebAppConfig


def _config(**overrides) -> DrakkarConfig:
    base = {'ui': {'release': {'enabled': False}}, 'executor': {'binary_path': '/usr/bin/echo'}}
    base.update(overrides)
    return DrakkarConfig(**base)


def test_sources_both_disabled_is_a_config_error():
    with pytest.raises(
        ValidationError, match=r'no input source enabled: set sources.kafka.enabled or sources.http.enabled'
    ):
        _config(sources={})


def test_sources_default_flags_are_false():
    sources = SourcesConfig()
    assert sources.kafka.enabled is False
    assert sources.http.enabled is False
    assert sources.enabled_names == []


def test_kafka_source_defaults_match_previous_consumer_defaults():
    src = KafkaSourceConfig()
    assert src.topic == 'input-events'
    assert src.consumer_group == 'drakkar-workers'
    assert src.max_poll_records == 100
    assert src.max_poll_interval_ms == 300_000
    assert src.session_timeout_ms == 45_000
    assert src.heartbeat_interval_ms == 3_000
    assert src.on_parse_error == 'skip'
    assert src.startup_align_enabled is True


def test_kafka_connection_has_no_consumer_fields():
    assert set(KafkaConfig.model_fields) == {'brokers', 'security', 'client_config', 'ui_url', 'ui_cluster_name'}


def test_enabled_kafka_source_requires_topic_and_group():
    with pytest.raises(ValidationError, match=r'sources.kafka.topic must be a non-empty string'):
        _config(sources={'kafka': {'enabled': True, 'topic': ''}})
    with pytest.raises(ValidationError, match=r'sources.kafka.consumer_group must be a non-empty string'):
        _config(sources={'kafka': {'enabled': True, 'consumer_group': ' '}})


def test_disabled_kafka_source_with_bad_values_loads_and_reports_errors():
    cfg = _config(sources={'kafka': {'enabled': False, 'topic': ''}, 'http': {'enabled': True}})
    assert cfg.sources.kafka.validation_errors() == ['sources.kafka.topic must be a non-empty string']


def test_disabled_http_source_with_bad_values_loads_and_reports_errors():
    cfg = _config(sources={'kafka': {'enabled': True}, 'http': {'enabled': False, 'path': 'nope'}})
    assert cfg.sources.http.validation_errors() == [
        "sources.http.path must start with '/' and have a non-empty route, got 'nope'"
    ]


def test_enabled_http_source_with_bad_values_fails():
    with pytest.raises(ValidationError, match=r"sources.http.path must start with '/'"):
        _config(sources={'http': {'enabled': True, 'path': 'nope'}})


def test_http_source_enabled_names_order_is_kafka_then_http():
    cfg = _config(sources={'kafka': {'enabled': True}, 'http': {'enabled': True}})
    assert cfg.sources.enabled_names == ['kafka', 'http']


def test_webapp_config_field_set_unchanged():
    assert set(WebAppConfig.model_fields) == {
        'enabled',
        'host',
        'port',
        'path',
        'sinks_enabled',
        'request_timeout_seconds',
        'max_concurrent',
        'max_body_bytes',
        'clients',
    }


def test_root_config_has_no_webapp_field():
    assert 'webapp' not in DrakkarConfig.model_fields
    assert 'sources' in DrakkarConfig.model_fields


def test_top_level_webapp_key_rejected_with_migration_message():
    with pytest.raises(ValidationError, match=r'webapp: moved to sources\.http \(see docs/sources\.md\)'):
        _config(webapp={'enabled': True})


def _summary_config(**overrides) -> DrakkarConfig:
    base = {
        'ui': {'release': {'enabled': False}, 'port': 8081},
        'executor': {
            'binary_path': '/usr/bin/echo',
            'max_executors': 4,
            'window_size': 10,
            'max_retries': 3,
            'task_timeout_seconds': 120,
        },
        'sinks': {
            'kafka': {'a': {'topic': 't'}, 'b': {'topic': 'u'}},
            'postgres': {'main': {'dsn': 'postgresql://u:p@h/d'}},
        },
        'metrics': {'port': 9090},
        'logging': {'level': 'INFO'},
    }
    base.update(overrides)
    return DrakkarConfig(**base)


def test_summary_kafka_only():
    cfg = _summary_config(
        sources={
            'kafka': {'enabled': True, 'topic': 'search-requests', 'consumer_group': 'grp', 'max_poll_records': 50}
        }
    )
    assert cfg.config_summary('worker-1', 'analytics-prod') == (
        '[worker-1/analytics-prod] sources=[kafka:search-requests/grp/50poll] exec=4w/10win retries=3/120s '
        'ui=on:8081 cache=off metrics=9090 dlq=on sinks=[kf:a,b pg:main] log=INFO'
    )


def test_summary_http_only_dlq_off():
    cfg = _summary_config(sources={'http': {'enabled': True, 'port': 8091}})
    assert cfg.config_summary('w', '') == (
        '[w] sources=[http:8091] exec=4w/10win retries=3/120s ui=on:8081 cache=off metrics=9090 dlq=off '
        'sinks=[kf:a,b pg:main] log=INFO'
    )


def test_summary_mixed_with_explicit_dlq_topic():
    cfg = _summary_config(
        sources={
            'kafka': {'enabled': True, 'topic': 'in', 'consumer_group': 'g'},
            'http': {'enabled': True, 'port': 8091},
        },
        dlq={'topic': 'my-dlq'},
    )
    assert cfg.config_summary('w', '') == (
        '[w] sources=[kafka:in/g/100poll http:8091] exec=4w/10win retries=3/120s ui=on:8081 cache=off '
        'metrics=9090 dlq=my-dlq sinks=[kf:a,b pg:main] log=INFO'
    )


def test_summary_http_only_with_explicit_dlq_topic_shows_topic():
    cfg = _summary_config(sources={'http': {'enabled': True}}, dlq={'topic': 'my-dlq'})
    assert cfg.config_summary('w', '') == (
        '[w] sources=[http:8090] exec=4w/10win retries=3/120s ui=on:8081 cache=off metrics=9090 '
        'dlq=my-dlq sinks=[kf:a,b pg:main] log=INFO'
    )
    assert cfg.dlq_enabled is True
    assert cfg.resolved_dlq_topic == 'my-dlq'


def test_resolved_dlq_topic_derives_from_kafka_source_topic():
    cfg = _summary_config(sources={'kafka': {'enabled': True, 'topic': 'events'}})
    assert cfg.resolved_dlq_topic == 'events_dlq'
    assert cfg.dlq_enabled is True


def test_dlq_off_when_kafka_source_off_and_no_topic():
    cfg = _summary_config(sources={'http': {'enabled': True}})
    assert cfg.dlq_enabled is False
    assert cfg.resolved_dlq_topic == ''


def test_resolved_consumer_group_is_the_configured_group_when_kafka_is_on():
    cfg = _summary_config(sources={'kafka': {'enabled': True, 'consumer_group': 'search-workers'}})
    assert cfg.resolved_consumer_group == 'search-workers'


def test_resolved_consumer_group_is_empty_when_kafka_is_off():
    # The default group name would otherwise put an HTTP-only worker in
    # every group-scoped log filter and dashboard query.
    cfg = _summary_config(sources={'http': {'enabled': True}})
    assert cfg.sources.kafka.consumer_group == 'drakkar-workers'
    assert cfg.resolved_consumer_group == ''
