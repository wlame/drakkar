"""The provider lookup every provider-backed feature shares: built-ins plus a feature's entry-point group.

The registry's own lookup rules are tested through its wrappers in
``tests/test_fleet_plugins.py``; these tests use the election group.
"""

import pytest
from structlog.testing import capture_logs

from drakkar.providers import lookup
from drakkar.providers.lookup import UnknownProviderError, resolve_factory
from drakkar.recorder.election import BUILTIN_ELECTIONS, ELECTION_ENTRY_POINT_GROUP

ELECTION_PATH = 'history.archive.election.provider'


class _BrokenEntryPoint:
    """An installed plugin whose module cannot be imported."""

    name = 'consul'
    value = 'plugin_pkg:consul'

    def load(self) -> object:
        raise ImportError('no module named consul_api')


@pytest.fixture
def no_election_plugins(monkeypatch):
    monkeypatch.setattr(lookup, 'entry_points', lambda *, group: [])


def test_resolve_factory_finds_the_builtin_election(no_election_plugins):
    factory = resolve_factory(ELECTION_ENTRY_POINT_GROUP, BUILTIN_ELECTIONS, 'filesystem', config_path=ELECTION_PATH)

    assert factory is BUILTIN_ELECTIONS['filesystem']


def test_resolve_factory_names_the_election_path(no_election_plugins):
    with pytest.raises(UnknownProviderError) as raised:
        resolve_factory(ELECTION_ENTRY_POINT_GROUP, BUILTIN_ELECTIONS, 'nope', config_path=ELECTION_PATH)

    assert (
        str(raised.value)
        == "history.archive.election.provider 'nope' is not known; known providers: filesystem, postgres"
    )


def test_the_election_entry_point_group_name():
    assert ELECTION_ENTRY_POINT_GROUP == 'drakkar.election_providers'


def test_a_broken_election_plugin_is_logged_with_its_group(monkeypatch):
    monkeypatch.setattr(lookup, 'entry_points', lambda *, group: [_BrokenEntryPoint()])

    with capture_logs() as logs:
        factories = lookup.discover_plugin_factories(ELECTION_ENTRY_POINT_GROUP)

    assert factories == {}
    (failure,) = [entry for entry in logs if entry['event'] == 'provider_entry_point_load_failed']
    assert (failure['group'], failure['name'], failure['value'], failure['category']) == (
        'drakkar.election_providers',
        'consul',
        'plugin_pkg:consul',
        'fleet',
    )
    assert 'consul_api' in failure['error']
