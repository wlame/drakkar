"""Helpers shared by the archive and election tests.

Sealed recorder files in a cluster's history folder, laid out as sealing
leaves them, and the election's provider call counters.
"""

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from drakkar.metrics import provider_calls
from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, format_dt
from drakkar.recorder.history import history_dir, meta_path, write_meta

HOUR = 3600.0
DAY = 24 * HOUR
CLUSTER = 'search-fleet'


def sealed_db_name(worker: str, start: float) -> str:
    """Sealed recorder file name for a worker whose file opened at ``start``."""
    return f'{worker}-{datetime.fromtimestamp(start, tz=UTC).strftime("%Y-%m-%d__%H_%M_%S")}.db'


def history_folder(tmp_path: Path, cluster: str = CLUSTER) -> Path:
    """The history folder of ``cluster`` under ``<tmp_path>/shared``, created."""
    folder = Path(history_dir(str(tmp_path / 'shared'), cluster))
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def make_sealed_db(folder: Path, worker: str, start: float, *, event_count: int = 2) -> str:
    """One sealed recorder file (rollback journal) with its meta file, as sealing leaves them."""
    path = folder / sealed_db_name(worker, start)
    db = sqlite3.connect(str(path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        """INSERT INTO worker_config
           (id, worker_name, cluster_name, ip_address, debug_port, created_at, created_at_dt)
           VALUES (1, ?, ?, ?, ?, ?, ?)""",
        [worker, CLUSTER, '10.0.0.1', 8080, start, format_dt(start)],
    )
    db.executescript(SCHEMA_EVENTS)
    for i in range(event_count):
        ts = start + i
        db.execute(
            'INSERT INTO events (ts, dt, event, partition, offset) VALUES (?, ?, ?, ?, ?)',
            [ts, format_dt(ts), 'consumed', 0, i],
        )
    db.commit()
    db.close()
    write_meta(meta_path(str(path)), {'schema': 1, 'worker': worker})
    return str(path)


def due_start(now: float, *, days_back: int = 3) -> float:
    """A file start time inside a window that is comfortably due at ``now``."""
    day_start = (now // DAY) * DAY
    return day_start - days_back * DAY + 10 * HOUR


def election_calls(provider: str, op: str, outcome: str) -> float:
    """``drakkar_provider_calls_total`` of the election for one provider, operation and outcome."""
    return provider_calls.labels(feature='election', provider=provider, op=op, outcome=outcome)._value.get()
