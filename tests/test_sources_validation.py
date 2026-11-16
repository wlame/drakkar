"""Handler validation against the enabled sources."""

import pytest
from pydantic import BaseModel

import drakkar as dk
from drakkar.config import SourcesConfig
from drakkar.models import ConfigurationError
from drakkar.sources.validation import validate_handler_for_sources


class In(BaseModel):
    x: int


class Out(BaseModel):
    y: int


class KafkaOnlyHandler(dk.BaseDrakkarHandler[In, Out]):
    async def arrange(self, messages, pending):
        return []


class HttpOnlyHandler(dk.BaseDrakkarHandler[In, Out, In, Out]):
    async def arrange_http_request(self, req, pending):
        return []

    async def on_http_request_complete(self, group):
        return Out(y=1)


class BothHandler(HttpOnlyHandler):
    async def arrange(self, messages, pending):
        return []


class NeitherHandler(dk.BaseDrakkarHandler[In, Out]):
    pass


def _sources(kafka: bool, http: bool) -> SourcesConfig:
    return SourcesConfig(kafka={'enabled': kafka}, http={'enabled': http})


@pytest.mark.parametrize(
    ('handler_cls', 'kafka', 'http', 'ok'),
    [
        (KafkaOnlyHandler, True, False, True),
        (KafkaOnlyHandler, False, True, False),
        (KafkaOnlyHandler, True, True, False),
        (HttpOnlyHandler, True, False, False),
        (HttpOnlyHandler, False, True, True),
        (HttpOnlyHandler, True, True, False),
        (BothHandler, True, False, True),
        (BothHandler, False, True, True),
        (BothHandler, True, True, True),
        (NeitherHandler, True, False, False),
        (NeitherHandler, False, True, False),
        (NeitherHandler, True, True, False),
    ],
)
def test_validate_handler_for_sources(handler_cls, kafka, http, ok):
    if ok:
        validate_handler_for_sources(handler_cls(), _sources(kafka, http))
    else:
        with pytest.raises(ConfigurationError):
            validate_handler_for_sources(handler_cls(), _sources(kafka, http))


def test_kafka_error_names_arrange():
    with pytest.raises(
        ConfigurationError,
        match=r'sources.kafka.enabled=true but NeitherHandler does not override arrange\(\) or arrange_stream\(\)',
    ):
        validate_handler_for_sources(NeitherHandler(), _sources(True, False))


def test_http_error_names_hooks():
    with pytest.raises(ConfigurationError, match=r'sources.http.enabled=true but KafkaOnlyHandler did not declare'):
        validate_handler_for_sources(KafkaOnlyHandler(), _sources(False, True))


class StreamHandler(dk.BaseDrakkarHandler[In, Out]):
    group_labels = ('directory', 'file_size')

    async def arrange_stream(self, message):
        yield []


def test_stream_handler_with_valid_group_labels_passes():
    validate_handler_for_sources(StreamHandler(), _sources(True, False))


def test_stream_handler_with_list_group_labels_passes():
    class ListLabels(StreamHandler):
        group_labels = ['directory']  # noqa: RUF012  (a list is a valid sequence of names)

    validate_handler_for_sources(ListLabels(), _sources(True, False))


def test_kafka_error_names_both_arrange_hooks():
    with pytest.raises(
        ConfigurationError,
        match=(
            r'^sources\.kafka\.enabled=true but NeitherHandler does not override arrange\(\) or arrange_stream\(\) '
            r'— implement one of them or disable sources\.kafka$'
        ),
    ):
        validate_handler_for_sources(NeitherHandler(), _sources(True, False))


def test_handler_with_both_arrange_hooks_fails():
    class TwoArranges(StreamHandler):
        async def arrange(self, messages, pending):
            return []

    with pytest.raises(
        ConfigurationError,
        match=r'^TwoArranges overrides both arrange\(\) and arrange_stream\(\); a handler has one of them$',
    ):
        validate_handler_for_sources(TwoArranges(), _sources(True, False))


def test_stream_handler_with_window_hook_fails():
    class StreamWithWindow(StreamHandler):
        async def on_window_complete(self, results, source_messages):
            return None

    with pytest.raises(
        ConfigurationError,
        match=(
            r'^StreamWithWindow overrides arrange_stream\(\) and on_window_complete\(\); a stream handler has no '
            r'windows, use on_group_complete\(\) or on_message_complete\(\)$'
        ),
    ):
        validate_handler_for_sources(StreamWithWindow(), _sources(True, False))


def test_group_hook_without_stream_fails():
    class GroupHookOnly(KafkaOnlyHandler):
        async def on_group_complete(self, group, results, errors):
            return None

    with pytest.raises(
        ConfigurationError,
        match=(
            r'^GroupHookOnly overrides on_group_complete\(\) without arrange_stream\(\); '
            r'groups exist only for a stream handler$'
        ),
    ):
        validate_handler_for_sources(GroupHookOnly(), _sources(True, False))


def test_group_labels_without_stream_fails():
    class LabelsOnly(KafkaOnlyHandler):
        group_labels = ('directory',)

    with pytest.raises(
        ConfigurationError,
        match=r'^LabelsOnly sets group_labels without arrange_stream\(\); groups exist only for a stream handler$',
    ):
        validate_handler_for_sources(LabelsOnly(), _sources(True, False))


@pytest.mark.parametrize('labels', [('a', 'a'), ('',), 'abc', (1,), None, b'ab'])
def test_invalid_group_labels_fail(labels):
    class BadLabels(StreamHandler):
        group_labels = labels

    with pytest.raises(
        ConfigurationError,
        match=r'^BadLabels\.group_labels must be a sequence of unique, non-empty label names$',
    ):
        validate_handler_for_sources(BadLabels(), _sources(True, False))


def test_stream_checks_run_with_only_the_http_source():
    class HttpWithGroupHook(HttpOnlyHandler):
        async def on_group_complete(self, group, results, errors):
            return None

    with pytest.raises(ConfigurationError, match=r'overrides on_group_complete\(\) without arrange_stream\(\)'):
        validate_handler_for_sources(HttpWithGroupHook(), _sources(False, True))
