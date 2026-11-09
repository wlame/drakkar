"""The ``agent`` config section: defaults, env, the API rule and its order, the summary token and the start warning."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.app_security import warn_if_agent_unauthenticated
from drakkar.config import AgentConfig, DrakkarConfig, load_config
from tests.conftest import kafka_connections
from tests.test_sources_validation import KafkaOnlyHandler

AGENT_NEEDS_API = 'agent.enabled needs api.enabled: the agent routes are served by the API server'
METADATA = json.loads(
    (Path(__file__).parent.parent / 'drakkar' / 'uiserver' / 'config-metadata.json').read_text(encoding='utf-8')
)


def _config(**sections) -> DrakkarConfig:
    return DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        **sections,
    )


def test_agent_defaults_are_off_and_empty():
    config = _config()

    assert (config.agent.enabled, config.agent.auth_token, config.agent.include_payloads) == (False, '', False)


def test_agent_token_is_stripped_on_load():
    assert AgentConfig(auth_token='  secret  ').auth_token == 'secret'


def test_agent_section_refuses_unknown_keys():
    with pytest.raises(ValidationError):
        AgentConfig(token='x')


def test_agent_env_overrides_load(tmp_path, monkeypatch):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(
        'connections: {kafka: {main: {}}}\nsources: {kafka: {enabled: true}}\n',
        encoding='utf-8',
    )
    monkeypatch.setenv('DK_AGENT__ENABLED', 'true')
    monkeypatch.setenv('DK_AGENT__AUTH_TOKEN', 'abc')
    monkeypatch.setenv('DK_AGENT__INCLUDE_PAYLOADS', 'true')

    config = load_config(path)

    assert (config.agent.enabled, config.agent.auth_token, config.agent.include_payloads) == (True, 'abc', True)


def test_agent_without_api_fails_the_load():
    with pytest.raises(ValidationError, match=AGENT_NEEDS_API):
        _config(api={'enabled': False}, ui={'enabled': False}, agent={'enabled': True})


def test_agent_with_api_loads_and_shows_in_the_summary():
    config = _config(agent={'enabled': True})

    assert ' ui=on agent=on ' in config.config_summary()


def test_a_hook_that_turns_the_agent_on_without_the_api_gets_the_load_message():
    config = _config(api={'enabled': False}, ui={'enabled': False}, recorder={'enabled': False})
    app = DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')
    config.agent.enabled = True

    with pytest.raises(ValueError, match=AGENT_NEEDS_API):
        app._lifecycle._check_config_after_startup()


def test_the_auth_token_is_marked_secret_in_the_metadata():
    token_entry = next(
        entry for group in METADATA['groups'] for entry in group['entries'] if entry['path'] == 'agent.auth_token'
    )

    assert token_entry['secret'] is True


@pytest.mark.parametrize(
    ('agent', 'expected_events'),
    [
        ({'enabled': True}, 1),
        ({'enabled': True, 'auth_token': 'x'}, 0),
        ({'enabled': False}, 0),
    ],
)
def test_unauthenticated_warning_only_for_an_enabled_surface_with_no_token(agent, expected_events):
    config = _config(agent=agent)

    with capture_logs() as records:
        warn_if_agent_unauthenticated(config)

    events = [record for record in records if record['event'] == 'agent_surface_unauthenticated']
    assert len(events) == expected_events
    if events:
        assert events[0]['log_level'] == 'warning'
        assert events[0]['category'] == 'agent'
        assert 'agent.auth_token' in events[0]['hint']
