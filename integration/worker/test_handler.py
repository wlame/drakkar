"""Unit tests for the integration worker's handlers.

The ui.timeline label helpers are pure functions (no Kafka, no
subprocess, no framework wiring); the HttpSearchHandler cases run the
real ``arrange`` against the class-level framework stubs — a NoOpCache,
the inline offloader and no-op annotators — so they too need no fixtures
and no containers. Not part of the main ``tests/`` suite (which covers
the ``drakkar`` package itself): these pin the harness worker's own
behaviour so any implementation of it stays provably identical.
"""

from pathlib import Path

import pytest
from handler import _human_file_size, _scan_target_module
from http_handler import MIRRORED_RESULTS_SINK, HttpSearchHandler
from models import SearchRequest, SearchResponse

import drakkar as dk
from drakkar.config import load_config

# integration/worker/test_handler.py -> integration/worker -> integration -> repo root
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _empty_pending() -> dk.PendingContext:
    """The pending context the webapp runner hands an HTTP request: empty,
    because a synthetic per-request group has no in-flight siblings."""
    return dk.PendingContext(pending_tasks=[], pending_task_ids=set())


def _result(task: dk.ExecutorTask, stdout: str) -> dk.ExecutorResult:
    """A terminal success carrying ``stdout``, with the fields the hooks read."""
    return dk.ExecutorResult(
        task_id=task.task_id,
        exit_code=0,
        stdout=stdout,
        stderr='',
        duration_seconds=0.01,
        task=task,
        pid=4242,
    )


def _group(tasks: list[dk.ExecutorTask], results: list[dk.ExecutorResult], req: SearchRequest) -> dk.MessageGroup:
    """A completed HTTP-origin MessageGroup, shaped like the webapp runner's."""
    return dk.MessageGroup(
        source_message=dk.SourceMessage(
            topic='__webapp__',
            partition=-1,
            # First request of the process, matching what both the
            # framework runner and the handler stamp.
            offset=1,
            key=req.request_id.encode(),
            value=req.model_dump_json().encode(),
            timestamp=0,
            payload=req,
        ),
        tasks=tasks,
        results=results,
        errors=[],
        started_at=0.0,
        finished_at=1.5,
        origin='http',
        request_id='framework-assigned-id',
    )


@pytest.mark.parametrize(
    ('file_path', 'expected'),
    [
        # Root: os.path.isdir('/') is always true, and the directory
        # branch's fallback (`file_path.rstrip('/') or file_path`) — '/'
        # stripped of trailing slashes is '', which is falsy — returns the
        # path itself rather than ''.
        pytest.param('/', '/', id='root path'),
        # Bare relative name, no directory component: os.path.dirname
        # returns '', so os.path.basename('') is also '' (falsy), and the
        # helper falls back to the target's own base name rather than ''.
        pytest.param('app.py', 'app.py', id='bare filename with no parent directory'),
        # Ordinary file: the module is the immediate parent directory.
        pytest.param('/project/drakkar/app.py', 'drakkar', id='file with a parent directory'),
        # Ordinary nested file two directories deep: same rule, deeper path.
        pytest.param('/project/tests/test_app.py', 'tests', id='file two directories deep'),
    ],
)
def test_scan_target_module_returns_expected_directory_name(file_path: str, expected: str) -> None:
    assert _scan_target_module(file_path) == expected


@pytest.mark.parametrize(
    ('num_bytes', 'expected'),
    [
        (0, '0'),
        (512, '512'),
        (1023, '1023'),
        (1024, '1.0K'),
        (12698, '12.4K'),  # 12698 / 1024 = 12.400...
        (1024 * 1024, '1.0M'),
        (3 * 1024 * 1024, '3.0M'),
    ],
)
def test_human_file_size_formats_bytes_with_k_and_m_suffixes(num_bytes: int, expected: str) -> None:
    assert _human_file_size(num_bytes) == expected


def test_http_search_handler_declares_search_request_and_response_http_types() -> None:
    """The 3rd/4th generic slots are what the webapp bootstrap reads to build
    the POST route, so re-parameterising them is the whole point of the
    subclass — RipgrepHandler's RankRequest/RankResponse must not leak through."""
    assert HttpSearchHandler.http_request_model is SearchRequest
    assert HttpSearchHandler.http_response_model is SearchResponse


async def test_http_search_handler_arranges_like_ripgrep() -> None:
    """One POSTed request reaches the inherited arrange() and fans out to one
    task per (pattern, file_path) pair, carrying the caller's request_id."""
    handler = HttpSearchHandler()
    req = SearchRequest(
        request_id='req-000001',
        patterns=['import'],
        file_paths=['/project/drakkar'],
        repeat=1,
    )

    tasks = await handler.arrange_http_request(req, _empty_pending())

    assert len(tasks) == 1
    assert tasks[0].metadata['request_id'] == 'req-000001'
    assert tasks[0].metadata['pattern'] == 'import'
    assert tasks[0].metadata['file_path'] == '/project/drakkar'


async def test_http_search_handler_fans_out_over_patterns_and_file_paths() -> None:
    """Fan-out is the inherited Cartesian product, not a single task."""
    handler = HttpSearchHandler()
    req = SearchRequest(
        request_id='req-000002',
        patterns=['import', 'class'],
        file_paths=['/project/drakkar', '/project/tests'],
        repeat=2,
    )

    tasks = await handler.arrange_http_request(req, _empty_pending())

    assert len(tasks) == 4
    assert {(t.metadata['pattern'], t.metadata['file_path']) for t in tasks} == {
        ('import', '/project/drakkar'),
        ('import', '/project/tests'),
        ('class', '/project/drakkar'),
        ('class', '/project/tests'),
    }


async def test_http_search_handler_on_message_complete_targets_only_the_kafka_mirror() -> None:
    """The HTTP-only worker configures exactly one sink; a payload for any
    other would fail the sink manager's validation at delivery time."""
    handler = HttpSearchHandler()
    req = SearchRequest(request_id='req-000003', patterns=['import'], file_paths=['/project/drakkar'])
    tasks = await handler.arrange_http_request(req, _empty_pending())
    results = [_result(tasks[0], 'one match\ntwo matches\n')]

    collected = await handler.on_message_complete(_group(tasks, results, req))

    assert collected is not None
    assert not collected.postgres and not collected.mongo and not collected.redis
    assert not collected.http and not collected.files
    assert len(collected.kafka) == 1
    assert collected.kafka[0].sink == MIRRORED_RESULTS_SINK
    assert collected.kafka[0].key == b'req-000003'
    assert collected.kafka[0].data.total_matches == 2
    assert collected.kafka[0].data.succeeded_tasks == 1


async def test_http_search_handler_on_message_complete_skips_an_empty_group() -> None:
    """A request that produced no tasks has nothing to mirror."""
    handler = HttpSearchHandler()
    req = SearchRequest(request_id='req-000004', patterns=['import'], file_paths=['/project/drakkar'])

    assert await handler.on_message_complete(_group([], [], req)) is None


async def test_http_search_handler_response_reports_one_row_per_succeeded_task() -> None:
    """The POST answer breaks the fan-out down per task, counting blank lines
    out of the match totals the way the Kafka path does."""
    handler = HttpSearchHandler()
    req = SearchRequest(request_id='req-000005', patterns=['import', 'class'], file_paths=['/project/drakkar'])
    tasks = await handler.arrange_http_request(req, _empty_pending())
    results = [_result(tasks[0], 'a\n\nb\n'), _result(tasks[1], '')]

    response = await handler.on_http_request_complete(_group(tasks, results, req))

    assert response.request_id == 'req-000005'
    assert response.tasks == 2
    assert response.succeeded == 2
    assert response.failed == 0
    assert [m.match_count for m in response.matches] == [2, 0]
    assert {m.pattern for m in response.matches} == {'import', 'class'}
    assert [m.task_id for m in response.matches] == [tasks[0].task_id, tasks[1].task_id]


@pytest.mark.parametrize(
    ('relative_path', 'expected_prefix', 'expect_dlq_off'),
    [
        pytest.param(
            'integration/worker/drakkar.yaml',
            'sources=[kafka:search-requests/drakkar-integration/50poll]',
            False,
            id='worker',
        ),
        pytest.param(
            'integration/fast-worker/drakkar.yaml',
            'sources=[kafka:search-requests/drakkar-fast/50poll]',
            False,
            id='fast-worker',
        ),
        pytest.param(
            'integration/http-worker/drakkar.yaml',
            'sources=[http:8092]',
            True,
            id='http-worker',
        ),
    ],
)
def test_load_config_integration_yaml_uses_sources_layout(
    relative_path: str,
    expected_prefix: str,
    expect_dlq_off: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The three worker fixtures load under the ``sources:`` layout and
    report the expected ``sources=[...]`` token — pinning what any other
    implementation of this config format must also produce.

    ``load_config`` alone never fetches the UI bundle, but the env override
    is set anyway so this stays hermetic if that ever changes.
    """
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')

    config = load_config(str(_REPO_ROOT / relative_path))
    summary = config.config_summary('w', 'c')

    assert expected_prefix in summary
    if expect_dlq_off:
        assert 'dlq=off' in summary


async def test_http_search_handler_numbers_synthetic_offsets_from_one() -> None:
    """The handler's synthetic offset must track the framework runner's own
    per-request sequence (``_request_seq``, which starts at 1 and increments
    once per request), so arrange()'s annotations anchor on the same virtual
    offset as the rest of that request's trace."""
    handler = HttpSearchHandler()
    req = SearchRequest(request_id='req-000006', patterns=['import'], file_paths=['/project/drakkar'])

    first = await handler.arrange_http_request(req, _empty_pending())
    second = await handler.arrange_http_request(req, _empty_pending())

    assert first[0].source_offsets == [1]
    assert second[0].source_offsets == [2]
    # The window label arrange() stamps on every task carries the same offset.
    assert first[0].labels['request'] == '-1:1'
    assert second[0].labels['request'] == '-1:2'
