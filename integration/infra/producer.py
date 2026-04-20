"""Integration test producer with realistic load patterns.

Load phases:
  1. Warm-up:    500 msgs at steady 15/sec
  2. Pause:      15 second silence
  3. Burst:      200 msgs as fast as possible
  4. Steady:     2000 msgs at 20/sec
  5. Pause:      10 second silence
  6. Burst:      300 msgs as fast as possible
  7. Cool-down:  remaining msgs at 10/sec

Messages include a "repeat" field (how many times to run rg):
  - 97% of messages: repeat 1-15 (normal, seconds)
  - 3% of messages:  repeat 150-250 (slow outliers, minutes)
"""

import json
import os
import random
import time
import uuid

from confluent_kafka import Producer

BROKERS = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
TOPIC = os.environ.get('SOURCE_TOPIC', 'search-requests')
TOTAL_MESSAGES = int(os.environ.get('TOTAL_MESSAGES', '5000'))

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


def make_repeat() -> int:
    """97% normal (1-15), 3% slow outlier (150-250)."""
    if random.random() < 0.03:
        return random.randint(150, 250)
    return random.randint(1, 15)


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


def make_message() -> dict:
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
        'request_id': str(uuid.uuid4()),
        'patterns': patterns,
        'file_paths': file_paths,
        'repeat': make_repeat(),
    }


def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}', flush=True)


def send_batch(producer, count, label=''):
    """Send `count` messages as fast as possible (burst)."""
    for _i in range(count):
        message = make_message()
        producer.produce(
            topic=TOPIC,
            key=message['request_id'].encode(),
            value=json.dumps(message).encode(),
            callback=delivery_report,
        )
        producer.poll(0)
    producer.flush(timeout=10)
    print(f'  [{label}] burst: {count} messages sent', flush=True)


def send_steady(producer, count, rate, label=''):
    """Send `count` messages at `rate` per second."""
    interval = 1.0 / rate if rate > 0 else 0
    for i in range(count):
        message = make_message()
        producer.produce(
            topic=TOPIC,
            key=message['request_id'].encode(),
            value=json.dumps(message).encode(),
            callback=delivery_report,
        )
        producer.poll(0)
        if (i + 1) % 100 == 0:
            print(f'  [{label}] steady: {i + 1}/{count} at {rate}/sec', flush=True)
        if interval > 0:
            time.sleep(interval)
    producer.flush(timeout=10)


def main():
    print(f'Producer starting: {TOTAL_MESSAGES} total messages to {TOPIC}', flush=True)

    producer = Producer({'bootstrap.servers': BROKERS})
    sent = 0

    # Phase 0: slow drip — 10 messages, one every 5 seconds
    print('Phase 0: slow drip — 10 msgs, one every 5 sec', flush=True)
    send_steady(producer, 10, rate=0.2, label='drip')
    sent += 10

    # Phase 1: warm-up
    phase1 = min(500, TOTAL_MESSAGES - sent)
    print(f'Phase 1: warm-up — {phase1} msgs at 15/sec', flush=True)
    send_steady(producer, phase1, rate=15, label='warmup')
    sent += phase1

    if sent >= TOTAL_MESSAGES:
        return _done(sent)

    # Phase 2: pause
    print('Phase 2: pause — 15 seconds silence', flush=True)
    time.sleep(15)

    # Phase 3: burst
    phase3 = min(200, TOTAL_MESSAGES - sent)
    print(f'Phase 3: burst — {phase3} msgs at max speed', flush=True)
    send_batch(producer, phase3, label='burst1')
    sent += phase3

    if sent >= TOTAL_MESSAGES:
        return _done(sent)

    # Phase 4: steady
    phase4 = min(2000, TOTAL_MESSAGES - sent)
    print(f'Phase 4: steady — {phase4} msgs at 20/sec', flush=True)
    send_steady(producer, phase4, rate=20, label='steady')
    sent += phase4

    if sent >= TOTAL_MESSAGES:
        return _done(sent)

    # Phase 5: pause
    print('Phase 5: pause — 10 seconds silence', flush=True)
    time.sleep(10)

    # Phase 6: burst
    phase6 = min(300, TOTAL_MESSAGES - sent)
    print(f'Phase 6: burst — {phase6} msgs at max speed', flush=True)
    send_batch(producer, phase6, label='burst2')
    sent += phase6

    if sent >= TOTAL_MESSAGES:
        return _done(sent)

    # Phase 7: cool-down — remaining messages
    remaining = TOTAL_MESSAGES - sent
    print(f'Phase 7: cool-down — {remaining} msgs at 10/sec', flush=True)
    send_steady(producer, remaining, rate=10, label='cooldown')
    sent += remaining

    # Phase 8: flood — same volume again, no pauses, create massive consumer lag
    print(f'\nPhase 8: FLOOD — {TOTAL_MESSAGES} msgs at max speed (creating consumer lag)', flush=True)
    send_batch(producer, TOTAL_MESSAGES, label='flood')
    sent += TOTAL_MESSAGES

    _done(sent)


def _done(sent):
    print(f'Producer finished: {sent} messages sent', flush=True)
    # keep running so docker doesn't restart
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
