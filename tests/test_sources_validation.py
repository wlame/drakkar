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
        ConfigurationError, match=r'sources.kafka.enabled=true but NeitherHandler does not override arrange'
    ):
        validate_handler_for_sources(NeitherHandler(), _sources(True, False))


def test_http_error_names_hooks():
    with pytest.raises(ConfigurationError, match=r'sources.http.enabled=true but KafkaOnlyHandler did not declare'):
        validate_handler_for_sources(KafkaOnlyHandler(), _sources(False, True))
