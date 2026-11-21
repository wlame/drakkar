"""The handler API of the split feature: defaults, startup checks and exports."""

import pytest
from pydantic import BaseModel

import drakkar as dk
from drakkar.config import SourcesConfig
from drakkar.handler import _handler_splits
from drakkar.models import ConfigurationError, SourceMessage
from drakkar.sources.validation import validate_handler_for_sources
from drakkar.split.models import SplitJob


class In(BaseModel):
    x: int


class Out(BaseModel):
    y: int


class Summary(BaseModel):
    n: int


class PlainHandler(dk.BaseDrakkarHandler[In, Out]):
    async def arrange(self, messages, pending):
        return []


class SplitHandler(PlainHandler):
    async def split(self, message):
        yield message.payload


class SyncSplitHandler(PlainHandler):
    def split(self, message):
        return iter([])


class CoroutineSplitHandler(PlainHandler):
    async def split(self, message):
        return None


class PartDataOnly(PlainHandler):
    async def part_data(self, group):
        return None


class AggregateOnly(PlainHandler):
    async def aggregate(self, job):
        return None


class JobTimeoutOnly(PlainHandler):
    async def on_job_timeout(self, job):
        return None


class ModelOnly(PlainHandler):
    part_data_model = Summary


class AllSplitHooks(SplitHandler):
    part_data_model = Summary

    async def part_data(self, group):
        return None

    async def aggregate(self, job):
        return None

    async def on_job_timeout(self, job):
        return None


def _sources() -> SourcesConfig:
    return SourcesConfig(kafka={'enabled': True})


def _job(failed_parts: int) -> SplitJob:
    message = SourceMessage(topic='t', partition=0, offset=1, value=b'{}', timestamp=0)

    async def _no_results():
        return
        yield

    return SplitJob(
        source_message=message,
        job_id='j',
        attempt=1,
        local=True,
        total_parts=1,
        received_parts=1,
        failed_parts=failed_parts,
        dlq_parts=0,
        tasks_succeeded=0,
        tasks_failed=0,
        tasks_replaced=0,
        deliveries={},
        timed_out='',
        opened_at=0.0,
        duration_seconds=0.0,
        missing=list,
        results_source=_no_results,
    )


@pytest.mark.parametrize(
    ('handler_cls', 'enabled', 'message'),
    [
        (
            PlainHandler,
            True,
            'split.enabled=true but PlainHandler does not override split() — '
            'implement it or set split.enabled to false',
        ),
        (
            PartDataOnly,
            True,
            'PartDataOnly overrides part_data() without split(); it is called only for a handler that splits',
        ),
        (
            AggregateOnly,
            True,
            'AggregateOnly overrides aggregate() without split(); it is called only for a handler that splits',
        ),
        (
            JobTimeoutOnly,
            True,
            'JobTimeoutOnly overrides on_job_timeout() without split(); it is called only for a handler that splits',
        ),
        (
            ModelOnly,
            True,
            'ModelOnly sets part_data_model without split(); it is read only for a handler that splits',
        ),
        (SyncSplitHandler, True, 'SyncSplitHandler.split must be an async generator function'),
        (CoroutineSplitHandler, True, 'CoroutineSplitHandler.split must be an async generator function'),
        (ModelOnly, True, 'ModelOnly sets part_data_model without split()'),
    ],
)
def test_split_startup_check_refuses_with_message(handler_cls, enabled, message):
    with pytest.raises(ConfigurationError) as excinfo:
        validate_handler_for_sources(handler_cls(), _sources(), split_enabled=enabled)
    assert message in str(excinfo.value)


@pytest.mark.parametrize(
    ('handler_cls', 'enabled'),
    [
        (PlainHandler, False),
        (SplitHandler, True),
        (SplitHandler, False),
        (AllSplitHooks, True),
        # With the feature off no split hook is checked: these names may be the handler's own.
        (PartDataOnly, False),
        (AggregateOnly, False),
        (JobTimeoutOnly, False),
        (ModelOnly, False),
        (SyncSplitHandler, False),
        (CoroutineSplitHandler, False),
    ],
)
def test_split_startup_check_passes(handler_cls, enabled):
    validate_handler_for_sources(handler_cls(), _sources(), split_enabled=enabled)


def test_handler_splits_detects_override_on_class():
    assert _handler_splits(SplitHandler())
    assert not _handler_splits(PlainHandler())


async def test_default_aggregate_sends_failed_job_to_dlq():
    assert await PlainHandler().aggregate(_job(failed_parts=1)) == dk.JobAction.DLQ


async def test_default_aggregate_returns_none_without_failed_parts():
    assert await PlainHandler().aggregate(_job(failed_parts=0)) is None


async def test_default_on_job_timeout_sends_job_to_dlq():
    assert await PlainHandler().on_job_timeout(_job(failed_parts=0)) == dk.JobAction.DLQ


async def test_default_part_data_returns_none():
    assert await PlainHandler().part_data(None) is None


def test_default_split_raises_not_implemented():
    with pytest.raises(NotImplementedError, match='split\\(\\) is optional'):
        PlainHandler().split(None)


@pytest.mark.parametrize(
    'name',
    [
        'JobAction',
        'SplitJob',
        'SplitPart',
        'PartResult',
        'PartDLQ',
        'SinkCounts',
        'SplitResultsUnavailable',
        'SplitResultsExpired',
    ],
)
def test_split_names_are_exported(name):
    assert name in dk.__all__
    assert hasattr(dk, name)
