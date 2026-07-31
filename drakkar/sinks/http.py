"""HTTP sink — POSTs payloads to an HTTP endpoint.

Wraps httpx.AsyncClient. Each HttpPayload's data field is encoded per the
sink's ``encoding`` setting (json, form, or multipart); the encoder
returns both the body and the Content-Type that describes it.
"""

import time

import httpx
import structlog

from drakkar.config import HttpSinkConfig
from drakkar.http_encoding import encode_body
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import HttpPayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


class HttpSink(BaseSink[HttpPayload]):
    """Sends payloads to an HTTP endpoint with configurable body encoding.

    Each HttpPayload is encoded per the config's ``encoding`` setting:
        - json: application/json (default)
        - form: application/x-www-form-urlencoded
        - multipart: multipart/form-data

    Non-2xx responses raise httpx.HTTPStatusError so the framework
    can route the failure through on_delivery_error.
    """

    sink_type = 'http'

    # HTTP POST/PUT/etc. requests can have observable side effects on the
    # receiver (a webhook endpoint creating records, sending notifications,
    # charging a payment, etc.). Without an ``Idempotency-Key`` header the
    # downstream honors, retrying a request that succeeded on the server
    # but failed to return a response would double-submit. We default to
    # ``idempotent=False`` so the SinkManager makes a single delivery
    # attempt and delegates to ``on_delivery_error``. Users whose webhook
    # receiver supports an idempotency key (Stripe, Shopify, custom APIs)
    # can subclass ``HttpSink``, inject the key via ``config.headers``,
    # and set ``idempotent = True`` to opt into automatic retry.
    idempotent = False

    def __init__(self, name: str, config: HttpSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        """Create the httpx async client with configured timeout and headers."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._config.timeout_seconds),
            headers=dict(self._config.headers),
        )
        await logger.ainfo(
            'http_sink_connected',
            category='sink',
            sink_name=self._name,
            url=self._config.url,
            method=self._config.method,
        )

    def _request_headers(self, content_type: str) -> dict[str, str]:
        """Build per-request headers with the encoder's Content-Type winning.

        Config validation rejects a Content-Type in ``headers``, but a caller
        can build a config with ``model_construct`` and never validate it, so
        the strip happens here too. The comparison is case-insensitive
        because HTTP header names are, while a dict merge is not — leaving a
        differently-cased key in place would put two Content-Type lines on
        the wire instead of letting the encoder's value win.

        The configured headers are re-applied per request (rather than relying
        on the client defaults set in ``connect``) so ``deliver`` produces a
        complete header set on its own.
        """
        headers = {k: v for k, v in self._config.headers.items() if k.lower() != 'content-type'}
        headers['Content-Type'] = content_type
        return headers

    async def deliver(self, payloads: list[HttpPayload]) -> None:
        """Send each payload to the configured URL with encoded body.

        Raises httpx.HTTPStatusError on non-2xx responses.
        """
        if not payloads or not self._client:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            for payload in payloads:
                body, content_type = encode_body(payload.data, self._config.encoding)
                response = await self._client.request(
                    method=self._config.method,
                    url=self._config.url,
                    content=body,
                    headers=self._request_headers(content_type),
                )
                response.raise_for_status()

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    async def close(self) -> None:
        """Close the httpx client."""
        if self._client:
            try:
                await self._client.aclose()
            except Exception as e:
                await logger.awarning(
                    'http_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._client = None
