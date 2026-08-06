"""Registration + startup fail-fast for probe details models."""

import pytest
from pydantic import BaseModel

from drakkar import probe_field
from drakkar.handler import BaseDrakkarHandler
from drakkar.probe import ProbeDetailsConfigError


def test_handler_probe_details_model_defaults_to_none():
    class Bare(BaseDrakkarHandler):
        pass

    assert Bare.probe_details_model is None


def test_app_init_rejects_invalid_details_model():
    # Validation is wired in DrakkarApp.__init__ next to validate_webapp_handler.
    # We call the same seam the app calls, with an invalid model, and assert
    # the config error propagates (unit-isolated: no full app construction).
    from drakkar.probe import build_layout

    class Invalid(BaseModel):
        plain: int = 0  # unannotated field → startup rejection

    class WithInvalid(BaseDrakkarHandler):
        probe_details_model = Invalid

    with pytest.raises(ProbeDetailsConfigError):
        build_layout(WithInvalid.probe_details_model)


def test_probe_field_exported_from_package_root():
    import drakkar

    assert drakkar.probe_field is probe_field
