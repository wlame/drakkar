"""Handler validation for the HTTP input source (``sources.http``)."""

from typing import Any

from drakkar.models import ConfigurationError


def validate_webapp_handler(handler: Any) -> None:
    """Fail fast when ``sources.http.enabled`` but the handler can't serve HTTP.

    Called from ``DrakkarApp.__init__`` — construction time, so a
    misconfiguration fails before anything binds — and again defensively
    when the webapp server is built. Two requirements:

    - both HTTP hooks are overridden (the ``BaseDrakkarHandler`` defaults
      only raise ``NotImplementedError`` at request time), and
    - the 3rd/4th generic slots carry concrete Pydantic models
      (``http_request_model`` / ``http_response_model``).
    """
    from drakkar.handler import BaseDrakkarHandler

    handler_cls = type(handler)
    # Types first: the 4-slot generic is the primary opt-in mechanism and
    # its message names the offending class — the most useful pointer for
    # a handler that has no webapp support at all.
    request_model = getattr(handler, 'http_request_model', None)
    response_model = getattr(handler, 'http_response_model', None)
    if request_model is None or response_model is None:
        cls_name = handler_cls.__name__
        raise ConfigurationError(
            f'sources.http.enabled=true but {cls_name} did not declare '
            f'HttpRequestT/HttpResponseT — extend '
            f'BaseDrakkarHandler[InputT, OutputT, HttpRequestT, '
            f'HttpResponseT] with concrete Pydantic models in slots '
            f'3 and 4. See docs/webapp.md for an example.'
        )
    base = BaseDrakkarHandler
    hooks_overridden = (
        getattr(handler_cls, 'arrange_http_request', base.arrange_http_request) is not base.arrange_http_request
        and getattr(handler_cls, 'on_http_request_complete', base.on_http_request_complete)
        is not base.on_http_request_complete
    )
    if not hooks_overridden:
        raise ConfigurationError(
            'sources.http.enabled=true but the handler does not override '
            'arrange_http_request + on_http_request_complete — implement both '
            'hooks or disable sources.http'
        )
