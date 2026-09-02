"""Kafka sink — produces messages to a Kafka topic.

Uses confluent_kafka's AIOProducer for native asyncio integration.
Serializes each KafkaPayload's data field via model_dump_json().encode()
and produces it with the payload's key to the configured topic.
"""

import asyncio
import time
from collections.abc import Mapping

import structlog
from confluent_kafka.aio import AIOProducer

from drakkar.config import KafkaSinkConfig
from drakkar.kafka_security import KafkaSecurityConfig, merge_client_config, resolve_client
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import KafkaPayload
from drakkar.sinks.base import BaseSink
from drakkar.sinks.kafka_delivery import (
    ABANDONED_FUTURE_GRACE_SECONDS,
    future_outcome,
    settle_budget,
)
from drakkar.utils import redact_url

logger = structlog.get_logger()


class KafkaSink(BaseSink[KafkaPayload]):
    """Produces messages to a Kafka topic.

    Each KafkaPayload is serialized:
        - value = payload.data.model_dump_json().encode()
        - key = payload.key (passthrough bytes)

    If the sink config has empty brokers, falls back to the
    shared kafka.brokers from the main Kafka config.
    """

    sink_type = 'kafka'

    # The default ``AIOProducer`` configuration does NOT enable
    # ``enable.idempotence=true`` (see ``connect()`` below). Without the
    # broker-side deduplication that flag unlocks, a retried produce can
    # land the same logical message twice under broker-failover / timeout
    # scenarios. We therefore keep ``idempotent=False`` here as the safe
    # default. Operators who configure the underlying producer with
    # ``enable.idempotence=true`` + a stable message key + ``acks=all``
    # can subclass ``KafkaSink`` and flip this flag to opt into automatic
    # transient-error retry by the SinkManager.
    idempotent = False

    def __init__(
        self,
        name: str,
        config: KafkaSinkConfig,
        brokers_fallback: str = '',
        security_fallback: KafkaSecurityConfig | None = None,
        client_config_fallback: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        # An empty ``brokers`` means "the consumer's cluster", so the
        # consumer's credentials come with it — see ``resolve_client``.
        resolved = resolve_client(
            config.brokers,
            config.security,
            config.client_config,
            fallback_brokers=brokers_fallback,
            fallback_security=security_fallback or KafkaSecurityConfig(),
            fallback_client_config=client_config_fallback or {},
        )
        self._brokers = resolved.brokers
        self._security = resolved.security
        self._client_config = resolved.client_config
        self._producer: AIOProducer | None = None

    @property
    def topic(self) -> str:
        """The Kafka topic this sink produces to."""
        return self._config.topic

    async def connect(self) -> None:
        """Create the AIOProducer connection."""
        self._producer = AIOProducer(
            merge_client_config({'bootstrap.servers': self._brokers}, self._security, self._client_config)
        )
        await logger.ainfo(
            'kafka_sink_connected',
            category='sink',
            sink_name=self._name,
            topic=self._config.topic,
            brokers=redact_url(self._brokers),
            security=self._security.describe(),
        )

    async def deliver(self, payloads: list[KafkaPayload]) -> None:
        """Produce all payloads to the Kafka topic.

        Submits all messages, flushes to push them into flight,
        then awaits all delivery futures and verifies broker acknowledgement.
        """
        if not payloads:
            return
        # ``deliver`` must raise on failure (BaseSink contract) — silently
        # returning here would let the offset commit past lost payloads.
        if self._producer is None:
            raise RuntimeError(f'KafkaSink {self._name!r} is not connected — call connect() before deliver()')

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        futures = []
        futures_collected = False
        try:
            for payload in payloads:
                value = payload.data.model_dump_json().encode()
                f = await self._producer.produce(
                    topic=self._config.topic,
                    key=payload.key,
                    value=value,
                )
                futures.append(f)

            # The flush stays. Unlike the postgres/mongo/redis sinks — which
            # used to make one round-trip PER PAYLOAD and are now batched —
            # this sink already sends the whole batch with a single flush;
            # there is no per-payload round-trip to remove. Dropping it in
            # favour of awaiting the futures alone would be a regression,
            # not an optimisation: AIOProducer buffers internally and only
            # hands messages to librdkafka once the buffer reaches
            # ``batch_size`` (default 1000) or its ``buffer_timeout``
            # (default 1.0s) expires. A Drakkar batch is normally far
            # smaller than 1000 (window_size defaults to 100), so every
            # delivery would wait out that one-second inactivity timer
            # before its futures resolved — and ``deliver`` blocks on them.
            # Bounded on purpose. ``flush()`` with no argument becomes
            # librdkafka's ``flush(-1)``: it blocks until every in-flight
            # message resolves, which against a wedged broker means
            # ``message.timeout.ms`` (300s by default). Worse, the wait
            # occupies one of the AIOProducer's executor threads, so a
            # handful of stuck deliveries starve every other delivery on the
            # same producer. The outer delivery timeout cannot rescue this:
            # cancelling the await does not stop the thread.
            deadline = time.monotonic() + self._config.flush_timeout_seconds
            remaining = await self._producer.flush(self._config.flush_timeout_seconds)

            # ``remaining`` is NOT the verdict for this batch. librdkafka's
            # flush is producer-wide and this producer is shared by every
            # partition loop, so the count includes messages some other
            # partition is still waiting on. One topic-partition without a
            # leader would otherwise fail every concurrently-delivered batch
            # on the worker — re-producing groups the broker already
            # acknowledged (duplicates on the topic) or shipping them to the
            # DLQ for a replay that duplicates them again. The batch is
            # judged by its own delivery futures below; the count only
            # explains a timeout in the log.
            _settled, pending = await asyncio.wait(futures, timeout=settle_budget(deadline))
            if pending:
                # TimeoutError, not RuntimeError: the manager classifies it
                # as transient, so the breaker sees the outage and an
                # idempotent sink gets its fast-retry.
                raise TimeoutError(
                    f'Kafka delivery not acknowledged within {self._config.flush_timeout_seconds}s: '
                    f'{len(pending)} of {len(futures)} message(s) in this batch are still unconfirmed '
                    f'(producer-wide queue: {remaining}, topic={self._config.topic!r}, sink={self._name!r})'
                )
            # Every future is done; ``.result()`` re-raises a produce-side
            # exception here exactly as the previous ``gather`` did.
            results = [f.result() for f in futures]
            futures_collected = True
            for i, result in enumerate(results):
                if result is None:
                    raise RuntimeError(
                        f'Kafka delivery future resolved to None for message {i} '
                        f'(topic={self._config.topic!r}, sink={self._name!r})'
                    )
                if hasattr(result, 'error') and result.error() is not None:
                    raise RuntimeError(
                        f'Kafka delivery error for message {i}: {result.error()} '
                        f'(topic={self._config.topic!r}, sink={self._name!r})'
                    )

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise
        finally:
            if not futures_collected and futures:
                # An exception escaped before the gather (e.g. flush raised
                # with messages still in flight). Collect the outstanding
                # delivery futures so broker-level errors surface here
                # instead of being silently abandoned — their disposition
                # would otherwise depend on producer-close internals.
                # Bounded wait: futures of permanently-undeliverable
                # messages may never resolve.
                # ``asyncio.wait``, not ``wait_for(gather(...))``: a timeout
                # there cancels the delivery futures, and the producer thread
                # then sets a result on a cancelled future when the report
                # finally arrives. A short grace is enough — these futures
                # already had the full flush budget.
                done, pending = await asyncio.wait(futures, timeout=ABANDONED_FUTURE_GRACE_SECONDS)
                if pending:
                    await logger.awarning(
                        'kafka_sink_abandoned_futures_timeout',
                        category='sink',
                        sink_name=self._name,
                        topic=self._config.topic,
                        future_count=len(pending),
                    )
                leftover = [future_outcome(f) for f in done]
                failed = [
                    r
                    for r in leftover
                    if isinstance(r, BaseException) or (hasattr(r, 'error') and r.error() is not None)
                ]
                if failed:
                    await logger.awarning(
                        'kafka_sink_inflight_delivery_errors',
                        category='sink',
                        sink_name=self._name,
                        topic=self._config.topic,
                        failed_count=len(failed),
                        first_error=str(failed[0]),
                    )

    async def close(self) -> None:
        """Flush pending messages and close the producer."""
        if self._producer is not None:
            try:
                await self._producer.close()
            except Exception as e:
                await logger.awarning(
                    'kafka_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._producer = None
