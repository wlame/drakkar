"""Shared delivery-verdict helpers for the Kafka-backed sinks.

``AIOProducer.flush(timeout)`` is librdkafka's **producer-wide** flush: it
returns how many messages are still queued on the whole producer, not on the
batch that asked for it. One producer serves every partition loop, so that
count says nothing about whether *this* batch was acknowledged. Both Kafka
sinks therefore keep the flush as the nudge that hands buffered messages to
librdkafka, and take their verdict from their own delivery futures instead.
These helpers hold the timing arithmetic that makes the two-step wait share a
single budget.
"""

import asyncio
import time

# Smallest wait handed to the delivery futures after the flush. A flush that
# burned the entire budget must still let futures whose delivery report is
# already queued on the loop resolve, otherwise an acknowledged batch would be
# reported as a timeout.
MIN_SETTLE_SECONDS = 0.1

# Grace for collecting delivery futures the main path abandoned. They already
# had the whole flush budget, so this only drains reports that landed since.
ABANDONED_FUTURE_GRACE_SECONDS = 0.5


def settle_budget(deadline: float) -> float:
    """Return what is left of ``deadline``, never less than the minimum grace.

    ``deadline`` is a ``time.monotonic()`` stamp taken before the flush, so
    the flush and the wait on the delivery futures share one bound instead of
    each getting a full ``flush_timeout_seconds``.
    """
    return max(deadline - time.monotonic(), MIN_SETTLE_SECONDS)


def future_outcome(future: asyncio.Future) -> object:
    """Return a settled future's exception or its result, never raising."""
    if future.cancelled():
        return asyncio.CancelledError()
    return future.exception() or future.result()
