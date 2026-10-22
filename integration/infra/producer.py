"""Integration test producer with realistic load patterns.

Load phases (sizes are fractions of TOTAL_MESSAGES, see PHASE_TABLE; the
sizes below are those of 5000 messages):
  0. Drip:       10 msgs at 0.2/sec
  1. Warm-up:    500 msgs at steady 15/sec
  2. Pause:      15 second silence
  3. Burst:      200 msgs as fast as possible
  4. Steady:     2000 msgs at 20/sec
  5. Pause:      10 second silence
  6. Burst:      300 msgs as fast as possible
  7. Cool-down:  remaining msgs at 10/sec
  8. Flood:      every id again as fast as possible

Messages include a "repeat" field (how many times to run rg):
  - 97% of messages: repeat 1-15 (normal, seconds)
  - 3% of messages:  repeat 150-250 (slow outliers, minutes)
REPEAT_SCALE multiplies both ranges.

Request ids are DETERMINISTIC (``req-000001`` ... ``req-<TOTAL>``) rather
than random UUIDs, so ``verify_delivery.py`` can reconstruct exactly what
was produced from the message count alone and check the sinks against it.
The flood phase re-sends the same id range: same volume and the same
consumer lag as before, but now every request is genuinely redelivered,
which is what the at-least-once assertions need to be meaningful.
"""

import json
import os
import random
import time
import urllib.error
import urllib.request
from typing import NamedTuple

from confluent_kafka import Producer

BROKERS = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
TOPIC = os.environ.get('SOURCE_TOPIC', 'search-requests')
TOTAL_MESSAGES = int(os.environ.get('TOTAL_MESSAGES', '5000'))
# Multiplies both ranges of the per-message `repeat`, so a quick profile
# does less work per task and still sees every phase drain.
REPEAT_SCALE = float(os.environ.get('REPEAT_SCALE', '1.0'))

# Kept byte-identical in integration/verify_delivery.py, which rebuilds the
# produced id set from TOTAL_MESSAGES alone. A unit test pins the two.
REQUEST_ID_FORMAT = 'req-{:06d}'

# A small fraction of messages are ALSO POSTed to the HTTP-only worker, in
# addition to the Kafka send below — never instead of it, so
# verify_delivery.py's Kafka-derived accounting is unaffected. It is a demo
# of the HTTP input source; nothing verifies these requests automatically,
# the maintainer compares the two sink topics by hand.
HTTP_MIRROR_RATIO = float(os.environ.get('HTTP_MIRROR_RATIO', '0.01'))
HTTP_MIRROR_URL = os.environ.get('HTTP_MIRROR_URL', 'http://http-worker:8092/process')
HTTP_MIRROR_TOKEN = os.environ.get('HTTP_MIRROR_TOKEN', '')
HTTP_MIRROR_TIMEOUT_SECONDS = 5.0

# search requests pool — patterns x paths
SEARCH_REQUESTS = [
    {'pattern': r'error', 'file_path': '/project/drakkar/app.py'},
    {'pattern': r'warning', 'file_path': '/project/drakkar/partition.py'},
    {'pattern': r'async def', 'file_path': '/project/drakkar/executor.py'},
    {'pattern': r'import', 'file_path': '/project/drakkar/consumer.py'},
    {'pattern': r'await', 'file_path': '/project/drakkar/producer.py'},
    {'pattern': r'class\s+\w+', 'file_path': '/project/drakkar/models.py'},
    {'pattern': r'def\s+\w+', 'file_path': '/project/drakkar/handler.py'},
    {'pattern': r'return', 'file_path': '/project/drakkar/offsets.py'},
    {'pattern': r'self\._', 'file_path': '/project/drakkar/config.py'},
    {'pattern': r'logger', 'file_path': '/project/drakkar/logging.py'},
    {'pattern': r'metrics', 'file_path': '/project/drakkar/metrics.py'},
    {'pattern': r'async with', 'file_path': '/project/drakkar/db.py'},
    {'pattern': r'Exception', 'file_path': '/project/drakkar/recorder.py'},
    {'pattern': r'request', 'file_path': '/project/drakkar/debug_server.py'},
    {'pattern': r'assert', 'file_path': '/project/tests/test_partition.py'},
    {'pattern': r'pytest', 'file_path': '/project/tests/test_app.py'},
    {'pattern': r'fixture', 'file_path': '/project/tests/conftest.py'},
    {'pattern': r'mock', 'file_path': '/project/tests/test_consumer.py'},
    {'pattern': r'async def test_', 'file_path': '/project/tests/test_metrics.py'},
    {'pattern': r'def test_', 'file_path': '/project/tests/test_models.py'},
    {'pattern': r'import', 'file_path': '/project/drakkar'},
    {'pattern': r'asyncio', 'file_path': '/project/drakkar'},
    {'pattern': r'pydantic', 'file_path': '/project/drakkar'},
    {'pattern': r'structlog', 'file_path': '/project/drakkar'},
    {'pattern': r'json', 'file_path': '/project/drakkar'},
    {'pattern': r'time\.', 'file_path': '/project/drakkar'},
    {'pattern': r'TODO|FIXME|HACK', 'file_path': '/project'},
    {'pattern': r'def __init__', 'file_path': '/project/drakkar'},
    {'pattern': r'raise\s+\w+', 'file_path': '/project/drakkar'},
    {'pattern': r'\basync\b', 'file_path': '/project/drakkar'},
    {'pattern': r'partition', 'file_path': '/project/drakkar/partition.py'},
    {'pattern': r'offset', 'file_path': '/project/drakkar/offsets.py'},
    {'pattern': r'flush', 'file_path': '/project/drakkar/recorder.py'},
    {'pattern': r'template', 'file_path': '/project/drakkar/debug_server.py'},
    {'pattern': r'consumer', 'file_path': '/project/drakkar/consumer.py'},
    {'pattern': r'producer', 'file_path': '/project/drakkar/producer.py'},
    {'pattern': r'window', 'file_path': '/project/drakkar/partition.py'},
    {'pattern': r'commit', 'file_path': '/project/drakkar/app.py'},
    {'pattern': r'signal', 'file_path': '/project/drakkar/app.py'},
    {'pattern': r'Histogram|Counter|Gauge', 'file_path': '/project/drakkar/metrics.py'},
    {'pattern': r'SourceMessage|ExecutorTask', 'file_path': '/project/drakkar/models.py'},
    {'pattern': r'INSERT INTO', 'file_path': '/project/drakkar/db.py'},
    {'pattern': r'SELECT', 'file_path': '/project/drakkar/recorder.py'},
    {'pattern': r'CREATE TABLE', 'file_path': '/project/drakkar/recorder.py'},
    {'pattern': r'render|html', 'file_path': '/project/drakkar/templates'},
    {'pattern': r'tailwind', 'file_path': '/project/drakkar/templates'},
    {'pattern': r'test_.*error', 'file_path': '/project/tests'},
    {'pattern': r'conftest', 'file_path': '/project/tests'},
    {'pattern': r'ExecutorPool', 'file_path': '/project/drakkar/executor.py'},
    {'pattern': r'BaseDrakkarHandler', 'file_path': '/project/drakkar/handler.py'},
]


NORMAL_REPEAT_RANGE = (1, 15)
OUTLIER_REPEAT_RANGE = (150, 250)
OUTLIER_SHARE = 0.03


def repeat_ranges(scale: float) -> tuple[tuple[int, int], tuple[int, int]]:
    """The (normal, outlier) `repeat` ranges multiplied by `scale`, each bound at least 1."""
    if scale <= 0:
        raise ValueError(f'REPEAT_SCALE must be greater than 0, got {scale}')

    def scaled(bounds: tuple[int, int]) -> tuple[int, int]:
        low, high = bounds
        return max(1, round(low * scale)), max(1, round(high * scale))

    return scaled(NORMAL_REPEAT_RANGE), scaled(OUTLIER_REPEAT_RANGE)


def make_repeat(scale: float = 1.0) -> int:
    """97% normal (1-15), 3% slow outlier (150-250), both ranges multiplied by `scale`."""
    normal, outlier = repeat_ranges(scale)
    if random.random() < OUTLIER_SHARE:
        return random.randint(*outlier)
    return random.randint(*normal)


class Phase(NamedTuple):
    """One load phase: how many ids, at what rate, and the silence after it."""

    label: str
    title: str
    size: int
    rate: float | None  # messages per second; None is a burst at max speed
    pause_seconds: int  # silence after the phase; 0 for none


class PhaseSpec(NamedTuple):
    """A row of PHASE_TABLE: a phase whose size is a fraction of the total."""

    label: str
    title: str
    fraction: float | None  # None takes whatever is left (the cool-down)
    rate: float | None
    pause_seconds: int


# Fractions of TOTAL_MESSAGES. At 5000 they give 500, 200, 2000 and 300
# messages; the cool-down takes the rest. The drip is not in the table:
# it is always min(DRIP_MESSAGES, total).
DRIP_MESSAGES = 10
DRIP_RATE = 0.2
PHASE_TABLE = (
    PhaseSpec('warmup', 'warm-up', 0.10, 15, 15),
    PhaseSpec('burst1', 'burst', 0.04, None, 0),
    PhaseSpec('steady', 'steady', 0.40, 20, 10),
    PhaseSpec('burst2', 'burst', 0.06, None, 0),
    PhaseSpec('cooldown', 'cool-down', None, 10, 0),
)


def phase_plan(total: int) -> list[Phase]:
    """The load phases for `total` messages; the sizes always add up to `total`.

    A phase gets max(1, round(total * fraction)) ids, clipped to what is
    left. A phase that gets nothing (a tiny total) is dropped.
    """
    left = total
    plan = []
    drip = min(DRIP_MESSAGES, total)
    left -= drip
    plan.append(Phase('drip', 'slow drip', drip, DRIP_RATE, 0))
    for spec in PHASE_TABLE:
        size = left if spec.fraction is None else min(left, max(1, round(total * spec.fraction)))
        left -= size
        plan.append(Phase(spec.label, spec.title, size, spec.rate, spec.pause_seconds))
    return [phase for phase in plan if phase.size > 0]


# "Hot" queries that many messages reference, driving visible fan-IN in
# the worker: when two messages in the same arrange() window share a
# (pattern, file_path) pair, the handler dedupes them into one
# multi-offset task. The hot set is small so collisions are likely within
# a 10-message window.
HOT_QUERIES = [
    {'pattern': r'async def', 'file_path': '/project/drakkar/partition.py'},
    {'pattern': r'error', 'file_path': '/project/drakkar/app.py'},
    {'pattern': r'import', 'file_path': '/project/drakkar'},
]


def make_message(request_id: str) -> dict:
    """One SearchRequest with multiple patterns and files.

    Each request fans out to len(patterns) x len(file_paths) subprocess
    tasks in the worker. Some fraction of messages draw from HOT_QUERIES
    so that within an arrange() window there's a good chance of two
    messages requesting the same (pattern, file_path) — triggering
    framework-level fan-IN dedup in the worker.

    Shape matches the handler's 1-3 patterns, 1-2 files constraint.
    """
    # ~30% of messages draw one (pattern, file_path) pair from HOT_QUERIES
    # so fan-in is frequent enough to be visible in the debug UI.
    use_hot = random.random() < 0.30
    if use_hot:
        hot = random.choice(HOT_QUERIES)
        hot_patterns = [hot['pattern']]
        hot_paths = [hot['file_path']]
        # Optionally extend with random extras (still 1-3 patterns, 1-2 paths)
        extra_picks = random.sample(SEARCH_REQUESTS, k=2)
        if random.random() < 0.4:
            hot_patterns.append(extra_picks[0]['pattern'])
        if random.random() < 0.3:
            hot_patterns.append(extra_picks[1]['pattern'])
        if random.random() < 0.3:
            hot_paths.append(extra_picks[0]['file_path'])
        patterns = list(dict.fromkeys(hot_patterns))[:3]
        file_paths = list(dict.fromkeys(hot_paths))[:2]
    else:
        num_patterns = random.choices([1, 2, 3], weights=[2, 3, 2])[0]
        num_paths = random.choices([1, 2], weights=[3, 2])[0]
        picks = random.sample(SEARCH_REQUESTS, k=max(num_patterns, num_paths))
        patterns = list({p['pattern'] for p in picks[:num_patterns]})
        file_paths = list({p['file_path'] for p in picks[:num_paths]})

    return {
        'request_id': request_id,
        'patterns': patterns,
        'file_paths': file_paths,
        'repeat': make_repeat(REPEAT_SCALE),
    }


def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}', flush=True)


def maybe_mirror_over_http(message: dict) -> None:
    """With probability HTTP_MIRROR_RATIO, POST the same message to the HTTP-only worker.

    This is IN ADDITION to the Kafka send in send_batch/send_steady, never
    instead of it — verify_delivery.py only ever reconstructs delivery from
    the Kafka-produced id range, so this mirror has no effect on it.

    Best effort by design: the mirror is a demo of the HTTP source, so a
    refused connection or a 5xx is logged and never fails the run.
    """
    if HTTP_MIRROR_RATIO <= 0 or random.random() >= HTTP_MIRROR_RATIO:
        return
    body = json.dumps(message).encode()
    headers = {'Content-Type': 'application/json'}
    if HTTP_MIRROR_TOKEN:
        headers['Authorization'] = f'Bearer {HTTP_MIRROR_TOKEN}'
    request = urllib.request.Request(HTTP_MIRROR_URL, data=body, headers=headers, method='POST')
    try:
        with urllib.request.urlopen(request, timeout=HTTP_MIRROR_TIMEOUT_SECONDS) as response:
            print(f'  mirror: {message["request_id"]} -> {response.status}', flush=True)
    except urllib.error.HTTPError as exc:
        print(f'  mirror: {message["request_id"]} -> {exc.code}', flush=True)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f'  mirror: {message["request_id"]} -> error {exc}', flush=True)


def send_batch(producer, ids, label=''):
    """Send one message per id in `ids` as fast as possible (burst)."""
    for request_id in ids:
        message = make_message(request_id)
        producer.produce(
            topic=TOPIC,
            key=message['request_id'].encode(),
            value=json.dumps(message).encode(),
            callback=delivery_report,
        )
        maybe_mirror_over_http(message)
        producer.poll(0)
    producer.flush(timeout=10)
    print(f'  [{label}] burst: {len(ids)} messages sent', flush=True)


def send_steady(producer, ids, rate, label=''):
    """Send one message per id in `ids` at `rate` per second."""
    interval = 1.0 / rate if rate > 0 else 0
    for i, request_id in enumerate(ids):
        message = make_message(request_id)
        producer.produce(
            topic=TOPIC,
            key=message['request_id'].encode(),
            value=json.dumps(message).encode(),
            callback=delivery_report,
        )
        maybe_mirror_over_http(message)
        producer.poll(0)
        if (i + 1) % 100 == 0:
            print(f'  [{label}] steady: {i + 1}/{len(ids)} at {rate}/sec', flush=True)
        if interval > 0:
            time.sleep(interval)
    producer.flush(timeout=10)


def main():
    print(f'Producer starting: {TOTAL_MESSAGES} total messages to {TOPIC}', flush=True)

    producer = Producer({'bootstrap.servers': BROKERS})
    # One id per logical request, handed out in order across the phases. The
    # phases below slice this list rather than counting, so the ids a phase
    # sends are always exactly the ones no earlier phase used.
    ids = [REQUEST_ID_FORMAT.format(n) for n in range(1, TOTAL_MESSAGES + 1)]
    sent = 0

    def take(count):
        """The next `count` unsent ids, clipped to what is left."""
        nonlocal sent
        chunk = ids[sent : sent + count]
        sent += len(chunk)
        return chunk

    number = 0
    for phase in phase_plan(TOTAL_MESSAGES):
        chunk = take(phase.size)
        speed = 'at max speed' if phase.rate is None else f'at {phase.rate}/sec'
        print(f'Phase {number}: {phase.title} — {len(chunk)} msgs {speed}', flush=True)
        number += 1
        if phase.rate is None:
            send_batch(producer, chunk, label=phase.label)
        else:
            send_steady(producer, chunk, rate=phase.rate, label=phase.label)
        if phase.pause_seconds:
            print(f'Phase {number}: pause — {phase.pause_seconds} seconds silence', flush=True)
            number += 1
            time.sleep(phase.pause_seconds)

    # Flood: the SAME id range again, no pauses. Same volume and the
    # same consumer lag as before, but every request is now redelivered, so
    # the upsert convergence and the "a redelivery must not un-notify"
    # invariant are actually under load rather than assumed.
    print(f'\nPhase {number}: FLOOD — {TOTAL_MESSAGES} msgs at max speed (redelivering every id)', flush=True)
    send_batch(producer, ids, label='flood')

    _done(sent)


def _done(sent):
    # PRODUCER_DONE is the marker the CI workflow greps for; the count is
    # DISTINCT request ids, which is what verify_delivery.py reconstructs.
    print(f'Producer finished: {sent} messages sent', flush=True)
    print(f'PRODUCER_DONE distinct_request_ids={sent}', flush=True)
    # keep running so docker doesn't restart
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
