"""HTTP sink — POSTs JSON payloads to an HTTP endpoint.

Wraps httpx.AsyncClient. Each HttpPayload's data field is serialized
via model_dump_json() and sent as the request body with Content-Type:
application/json.
"""

import time

import httpx
import structlog

from drakkar.config import HttpSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import HttpPayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


class HttpSink(BaseSink[HttpPayload]):
    """Sends JSON payloads to an HTTP endpoint.

    Each HttpPayload is serialized:
        - body = payload.data.model_dump_json()
        - Content-Type: application/json

    Non-2xx responses raise httpx.HTTPStatusError so the framework
    can route the failure through on_delivery_error.
    """

    sink_type = 'http'

    def __init__(self, name: str, config: HttpSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client: httpx.AsyncClient | None = None

    async def connect(self) -> None:
        """Create the httpx async client with configured timeout and headers."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._config.timeout_seconds),
            headers={
                'Content-Type': 'application/json',
                **self._config.headers,
            },
        )
        await logger.ainfo(
            'http_sink_connected',
            category='sink',
            sink_name=self._name,
            url=self._config.url,
            method=self._config.method,
        )

    async def deliver(self, payloads: list[HttpPayload]) -> None:
        """Send each payload as a JSON request to the configured URL.

        Raises httpx.HTTPStatusError on non-2xx responses.
        """
        if not payloads or not self._client:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            for payload in payloads:
                body = payload.data.model_dump_json()
                response = await self._client.request(
                    method=self._config.method,
                    url=self._config.url,
                    content=body,
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
