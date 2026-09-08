"""The source registry: which sources exist, and which ones get built."""

from unittest.mock import MagicMock

from drakkar.config import DrakkarConfig
from drakkar.sources.http import HttpSource
from drakkar.sources.kafka import KafkaSource
from drakkar.sources.registry import SOURCE_FACTORIES, build_sources


def _app(sources: dict) -> MagicMock:
    app = MagicMock()
    app._config = DrakkarConfig(
        ui={'release': {'enabled': False}}, executor={'binary_path': '/usr/bin/echo'}, sources=sources
    )
    return app


def test_factories_table_order():
    assert list(SOURCE_FACTORIES) == ['kafka', 'http']


def test_build_only_enabled_sources_in_order():
    sources = build_sources(_app({'kafka': {'enabled': True}, 'http': {'enabled': True}}))
    assert [type(s) for s in sources] == [KafkaSource, HttpSource]
    assert build_sources(_app({'http': {'enabled': True}}))[0].name == 'http'
    assert [s.name for s in build_sources(_app({'kafka': {'enabled': True}}))] == ['kafka']
