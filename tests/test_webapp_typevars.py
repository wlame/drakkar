"""Tests for the 4-param generic ``BaseDrakkarHandler`` (PEP 696 defaults).

Covers the type-arg-extraction code path that powers automatic
serialisation/deserialisation of webapp HTTP requests:

* a 2-param subclass (legacy form) keeps working — HTTP slots resolve to
  ``None``;
* a 4-param subclass (webapp opt-in) exposes the HTTP request/response
  models on the subclass;
* a 3-param subclass (only ``HttpRequestT`` filled, ``HttpResponseT`` left
  defaulted) — PEP 696 fills the missing slot with ``None``;
* a no-param subclass leaves all four slots ``None``.
"""

from pydantic import BaseModel

from drakkar.handler import BaseDrakkarHandler, _extract_type_args


class InputModel(BaseModel):
    """Stand-in Kafka-input Pydantic model for the tests below."""

    a: int = 0


class OutputModel(BaseModel):
    """Stand-in Kafka-output Pydantic model."""

    b: int = 0


class HttpReq(BaseModel):
    """Stand-in webapp HTTP-request Pydantic model."""

    pattern: str = ''


class HttpResp(BaseModel):
    """Stand-in webapp HTTP-response Pydantic model."""

    matches: int = 0


def test_two_param_subclass_resolves_http_slots_to_none():
    """Legacy 2-param subclass — HTTP slots default to None (PEP 696)."""

    class MyHandler(BaseDrakkarHandler[InputModel, OutputModel]):
        async def arrange(self, messages, pending):
            return []

    input_t, output_t, http_req_t, http_resp_t = _extract_type_args(MyHandler)
    assert input_t is InputModel
    assert output_t is OutputModel
    assert http_req_t is None
    assert http_resp_t is None
    # Class attributes follow the same rule.
    assert MyHandler.input_model is InputModel
    assert MyHandler.output_model is OutputModel
    assert MyHandler.http_request_model is None
    assert MyHandler.http_response_model is None


def test_four_param_subclass_exposes_http_models():
    """Webapp opt-in: 4-param subclass populates http_request/response_model."""

    class MyHandler(BaseDrakkarHandler[InputModel, OutputModel, HttpReq, HttpResp]):
        async def arrange(self, messages, pending):
            return []

    input_t, output_t, http_req_t, http_resp_t = _extract_type_args(MyHandler)
    assert input_t is InputModel
    assert output_t is OutputModel
    assert http_req_t is HttpReq
    assert http_resp_t is HttpResp
    # Class attributes are mirrored from the resolved type args.
    assert MyHandler.http_request_model is HttpReq
    assert MyHandler.http_response_model is HttpResp


def test_three_param_subclass_leaves_response_slot_none():
    """PEP 696 default fills omitted slots with None — verified at runtime.

    A user who declares only ``HttpRequestT`` (and leaves ``HttpResponseT`` to
    its default) gets a class with ``http_response_model = None``. This is a
    nonsensical configuration for a real webapp handler but the framework
    must handle it gracefully — the webapp bootstrap is responsible
    for fail-fasting at app startup, not the type-extraction code.
    """

    class MyHandler(BaseDrakkarHandler[InputModel, OutputModel, HttpReq]):
        async def arrange(self, messages, pending):
            return []

    input_t, output_t, http_req_t, http_resp_t = _extract_type_args(MyHandler)
    assert input_t is InputModel
    assert output_t is OutputModel
    assert http_req_t is HttpReq
    assert http_resp_t is None
    assert MyHandler.http_response_model is None


def test_no_param_subclass_leaves_all_slots_none():
    """No type params at all — all four resolved slots are None."""

    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

    assert _extract_type_args(MyHandler) == (None, None, None, None)
    assert MyHandler.input_model is None
    assert MyHandler.output_model is None
    assert MyHandler.http_request_model is None
    assert MyHandler.http_response_model is None
