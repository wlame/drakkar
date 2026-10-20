"""Registry provider lookup: built-ins plus ``drakkar.registry_providers`` entry points.

The lookup itself lives in ``drakkar.providers.lookup``, shared by every
provider-backed feature, so the tests replace the entry points it reads.
"""

import pytest
from structlog.testing import capture_logs

from drakkar.fleet.plugins import ENTRY_POINT_GROUP, UnknownRegistryProviderError, resolve_provider_factory
from drakkar.fleet.providers import BUILTIN_PROVIDERS
from drakkar.providers import lookup
from drakkar.providers.lookup import UnknownProviderError


class _EntryPoint:
    """The two attributes of ``importlib.metadata.EntryPoint`` the lookup uses, plus ``load``."""

    def __init__(self, name: str, target: object = None, error: Exception | None = None) -> None:
        self.name = name
        self.value = f'plugin_pkg:{name}'
        self._target = target
        self._error = error

    def load(self) -> object:
        if self._error is not None:
            raise self._error
        return self._target


def _plugin_factory(options):
    return ('example-plugin', options)


@pytest.fixture
def installed(monkeypatch):
    """Replace the installed entry points of the provider group with the given list."""

    def install(*entry_points: _EntryPoint) -> None:
        def fake_entry_points(*, group: str):
            assert group == ENTRY_POINT_GROUP
            return list(entry_points)

        monkeypatch.setattr(lookup, 'entry_points', fake_entry_points)

    return install


def test_entry_point_group_name():
    assert ENTRY_POINT_GROUP == 'drakkar.registry_providers'


def test_a_builtin_name_resolves_to_its_factory(installed):
    installed()

    assert resolve_provider_factory('filesystem') is BUILTIN_PROVIDERS['filesystem']


def test_a_plugin_name_resolves_to_the_entry_point_factory(installed):
    installed(_EntryPoint('example-plugin', _plugin_factory))

    factory = resolve_provider_factory('example-plugin')

    assert factory({'address': 'x'}) == ('example-plugin', {'address': 'x'})


def test_a_plugin_replaces_a_builtin_of_the_same_name(installed):
    installed(_EntryPoint('filesystem', _plugin_factory))

    assert resolve_provider_factory('filesystem') is _plugin_factory


def test_a_plugin_that_fails_to_load_is_logged_and_skipped(installed):
    installed(
        _EntryPoint('broken', error=ImportError('no module named example_api')),
        _EntryPoint('example-plugin', _plugin_factory),
    )

    with capture_logs() as logs:
        factory = resolve_provider_factory('example-plugin')

    assert factory is _plugin_factory
    failures = [entry for entry in logs if entry['event'] == 'provider_entry_point_load_failed']
    assert len(failures) == 1
    assert failures[0]['name'] == 'broken'
    assert 'example_api' in failures[0]['error']


def test_a_plugin_that_is_not_callable_is_logged_and_skipped(installed):
    installed(_EntryPoint('example-plugin', target='not a factory'))

    with capture_logs() as logs, pytest.raises(UnknownRegistryProviderError):
        resolve_provider_factory('example-plugin')

    assert [entry['name'] for entry in logs if entry['event'] == 'provider_entry_point_invalid'] == ['example-plugin']


def test_an_unreadable_entry_point_table_is_logged_and_builtins_still_resolve(monkeypatch):
    def broken_entry_points(*, group: str):
        raise RuntimeError('metadata table unreadable')

    monkeypatch.setattr(lookup, 'entry_points', broken_entry_points)

    with capture_logs() as logs:
        factory = resolve_provider_factory('static')

    assert factory is BUILTIN_PROVIDERS['static']
    assert [entry['event'] for entry in logs] == ['provider_entry_point_scan_failed']


def test_an_unknown_name_fails_and_lists_the_known_providers(installed):
    installed(_EntryPoint('example-plugin', _plugin_factory))

    with pytest.raises(UnknownRegistryProviderError) as raised:
        resolve_provider_factory('etcd')

    assert str(raised.value) == (
        "fleet.registry.provider 'etcd' is not known; known providers: consul, example-plugin, filesystem, none, postgres, redis, static"
    )
    assert isinstance(raised.value, UnknownProviderError)
