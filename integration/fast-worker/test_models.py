"""Unit tests of the fast harness worker's input model.

The module is loaded by its path under its own name: the main harness
worker also has a ``models`` module, and both test files run in one pytest
session (``just test-harness``).
"""

import importlib.util
from pathlib import Path

import pytest
from pydantic import ValidationError

_MODELS_PATH = Path(__file__).with_name('models.py')
_SPEC = importlib.util.spec_from_file_location('fast_worker_models', _MODELS_PATH)
assert _SPEC is not None and _SPEC.loader is not None
fast_models = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(fast_models)
CountRequest = fast_models.CountRequest


@pytest.mark.parametrize(
    ('message', 'expected_paths'),
    [
        pytest.param(
            '{"request_id": "req-000001", "patterns": ["error"], "file_paths": ["/a", "/b"], "repeat": 2}',
            ['/a', '/b'],
            id='producer_list',
        ),
        pytest.param(
            '{"request_id": "req-000002", "pattern": "x", "file_path": "/c"}',
            ['/c'],
            id='scenario_single_path',
        ),
        pytest.param(
            '{"request_id": "req-000003", "file_path": "/a", "file_paths": ["/b", "/a"]}',
            ['/a', '/b'],
            id='both_in_order_without_repeats',
        ),
    ],
)
def test_count_request_paths_reads_both_shapes(message: str, expected_paths: list[str]) -> None:
    request = CountRequest.model_validate_json(message)

    assert request.paths() == expected_paths


def test_count_request_without_a_path_fails_the_parse() -> None:
    with pytest.raises(ValidationError, match='needs file_path or file_paths'):
        CountRequest.model_validate_json('{"request_id": "req-000004", "patterns": ["error"]}')
