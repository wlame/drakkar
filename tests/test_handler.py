"""Tests for Drakkar handler protocol and base handler."""

import json
import os

import pytest
from pydantic import BaseModel

from drakkar.config import DrakkarConfig, ExecutorConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
)


@pytest.fixture
def handler() -> BaseDrakkarHandler:
    return BaseDrakkarHandler()


async def test_base_handler_arrange_raises(
    handler: BaseDrakkarHandler,
    source_message: SourceMessage,
):
    with pytest.raises(NotImplementedError, match='arrange'):
        await handler.arrange([source_message], PendingContext())


async def test_base_handler_collect_returns_none(
    handler: BaseDrakkarHandler,
    executor_result: ExecutorResult,
):
    result = await handler.collect(executor_result)
    assert result is None


async def test_base_handler_on_window_complete_returns_none(
    handler: BaseDrakkarHandler,
    executor_result: ExecutorResult,
    source_message: SourceMessage,
):
    result = await handler.on_window_complete([executor_result], [source_message])
    assert result is None


async def test_base_handler_on_error_returns_skip(
    handler: BaseDrakkarHandler,
    executor_task: ExecutorTask,
    executor_error: ExecutorError,
):
    action = await handler.on_error(executor_task, executor_error)
    assert action == ErrorAction.SKIP


async def test_base_handler_on_assign_is_noop(handler: BaseDrakkarHandler):
    await handler.on_assign([0, 1, 2])


async def test_base_handler_on_revoke_is_noop(handler: BaseDrakkarHandler):
    await handler.on_revoke([0, 1, 2])


async def test_base_handler_on_startup_returns_config_unchanged(handler: BaseDrakkarHandler):
    config = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/echo'))
    result = await handler.on_startup(config)
    assert result is config


async def test_base_handler_on_ready_is_noop(handler: BaseDrakkarHandler):
    config = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/echo'))
    await handler.on_ready(config, None)  # should not raise


async def test_on_ready_receives_db_pool():
    initialized = {}

    class InitHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        async def on_ready(self, config, db_pool):
            initialized['pool'] = db_pool
            initialized['config'] = config

    handler = InitHandler()
    config = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/echo'))
    fake_pool = object()
    await handler.on_ready(config, fake_pool)

    assert initialized['pool'] is fake_pool
    assert initialized['config'] is config


async def test_on_startup_can_modify_config():
    class TuningHandler(BaseDrakkarHandler):
        async def on_startup(self, config):
            import os

            cpu_count = os.cpu_count() or 4
            return config.model_copy(
                update={
                    'executor': config.executor.model_copy(
                        update={
                            'max_workers': cpu_count,
                        }
                    ),
                }
            )

        async def arrange(self, messages, pending):
            return []

    handler = TuningHandler()
    config = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/echo', max_workers=1),
    )
    result = await handler.on_startup(config)
    assert result.executor.max_workers == (os.cpu_count() or 4)
    assert result.executor.binary_path == '/bin/echo'


async def test_custom_handler_overrides():
    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id='custom-1',
                    args=['--test'],
                    source_offsets=[m.offset for m in messages],
                )
            ]

    handler = MyHandler()
    msg = SourceMessage(topic='t', partition=0, offset=5, value=b'x', timestamp=0)
    tasks = await handler.arrange([msg], PendingContext())
    assert len(tasks) == 1
    assert tasks[0].task_id == 'custom-1'
    assert tasks[0].source_offsets == [5]


# --- Generic typed handler ---


class SearchRequest(BaseModel):
    pattern: str
    file_path: str
    repeat: int = 1


class SearchResult(BaseModel):
    pattern: str
    match_count: int
    matches: list[str] = []


async def test_generic_handler_extracts_input_model():
    class MyHandler(BaseDrakkarHandler[SearchRequest, SearchResult]):
        async def arrange(self, messages, pending):
            return []

    handler = MyHandler()
    assert handler.input_model is SearchRequest
    assert handler.output_model is SearchResult


async def test_generic_handler_deserializes_payload():
    class MyHandler(BaseDrakkarHandler[SearchRequest, SearchResult]):
        async def arrange(self, messages, pending):
            return []

    handler = MyHandler()
    msg = SourceMessage(
        topic='t',
        partition=0,
        offset=0,
        timestamp=0,
        value=json.dumps({'pattern': 'error', 'file_path': '/tmp/f.txt', 'repeat': 5}).encode(),
    )
    handler.deserialize_message(msg)
    assert msg.payload is not None
    assert isinstance(msg.payload, SearchRequest)
    assert msg.payload.pattern == 'error'
    assert msg.payload.repeat == 5


async def test_generic_handler_payload_default_on_bad_json():
    class MyHandler(BaseDrakkarHandler[SearchRequest, SearchResult]):
        async def arrange(self, messages, pending):
            return []

    handler = MyHandler()
    msg = SourceMessage(
        topic='t',
        partition=0,
        offset=0,
        timestamp=0,
        value=b'not json',
    )
    handler.deserialize_message(msg)
    assert msg.payload is None


async def test_non_generic_handler_skips_deserialization():
    class PlainHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

    handler = PlainHandler()
    assert handler.input_model is None
    assert handler.output_model is None

    msg = SourceMessage(
        topic='t',
        partition=0,
        offset=0,
        timestamp=0,
        value=b'{"x": 1}',
    )
    handler.deserialize_message(msg)
    assert msg.payload is None  # no deserialization without input_model


async def test_output_message_from_model():
    result = SearchResult(pattern='error', match_count=3, matches=['a', 'b', 'c'])
    msg = OutputMessage.from_model(result, key=b'key-1')
    assert msg.key == b'key-1'
    parsed = json.loads(msg.value)
    assert parsed['pattern'] == 'error'
    assert parsed['match_count'] == 3
    assert parsed['matches'] == ['a', 'b', 'c']
