"""Generate cross-backend DB fixtures for the Go backend's interop tests.

Writes one recorder DB and one cache DB through the REAL engines (not raw
SQL), so the fixtures carry exactly what a production Python worker puts
on disk — schema, pragmas-affected layout, JSON encodings, canonical
datetime strings. The Go repo commits the output under
``internal/crossbackend/testdata/python-db/`` and reads it in its
round-trip tests; the mirror generator lives at
``drakkar-go/internal/crossbackend/gen`` and feeds ``tests/fixtures/go-db/``
here. Regenerate via ``just gen-db-fixtures`` after any schema or
encoding change, and commit the refreshed fixtures in the sibling repo.

Fixture content contract (consumed by both repos' tests — keep in sync):

- worker ``py-fixture`` (Go writes ``go-fixture``), cluster ``main``
- recorder.db: ``worker_config`` row (source_topic ``fixture-topic``,
  consumer_group ``fixture-group``, one kafka sink ``out``), events:
  ``committed`` @ partition 0 / offset 1, ``periodic_run``
  (``fixture-periodic``), ``webapp_request_received`` (client
  ``fixture-client``, request id ``fx-req-1``, canonical started_at
  ``2026-07-05T12:00:00.250000Z``)
- cache.db: ``fx:global`` (global, ``{"v": "global"}``), ``fx:cluster``
  (cluster, ``"cluster-value"``), ``fx:local`` (local, ``"local-value"``),
  ``fx:expired`` (global, already expired at read time)
"""

from __future__ import annotations

import argparse
import asyncio
import shutil
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from drakkar.cache import Cache, CacheEngine
from drakkar.cache.models import CacheScope
from drakkar.config import (
    CacheConfig,
    DrakkarConfig,
    KafkaConfig,
    KafkaSinkConfig,
    SinksConfig,
    UIConfig,
    UIRecorderConfig,
)
from drakkar.recorder import EventRecorder, list_db_files
from drakkar.recorder.helpers import encode_json_str
from drakkar.webapp import WebRequestContext

WORKER = 'py-fixture'
CLUSTER = 'main'
WEBAPP_STARTED_AT = datetime(2026, 7, 5, 12, 0, 0, 250000, tzinfo=UTC)


async def _generate(work_dir: Path) -> tuple[Path, Path]:
    """Write both fixture DBs into ``work_dir`` and return their paths."""
    ui_config = UIConfig(recorder=UIRecorderConfig(db_dir=str(work_dir)))

    recorder = EventRecorder(ui_config, worker_name=WORKER, cluster_name=CLUSTER)
    await recorder.start()
    await recorder.write_config(
        DrakkarConfig(
            kafka=KafkaConfig(source_topic='fixture-topic', consumer_group='fixture-group'),
            sinks=SinksConfig(kafka={'out': KafkaSinkConfig(topic='fixture-out')}),
        )
    )
    recorder.record_committed(0, 1)
    recorder.record_periodic_run('fixture-periodic', 0.25, 'success')
    # One handler annotation so the Go side can prove it reads a
    # Python-written annotation row (contract v1.3) — no new column, but the
    # envelope inside metadata is part of the cross-backend contract.
    recorder.record_annotation(
        kind='fixture_annotation',
        partition=0,
        metadata_json=encode_json_str(
            {
                'kind': 'fixture_annotation',
                'scope': 'message',
                'hook': 'arrange',
                'window_id': 1,
                # Empty for message scope: only window rows, which have no
                # anchor column, carry offsets for the trace query to match.
                'offsets': [],
                'data': {'source': 'python'},
            }
        ),
        offset=1,
        labels={'fixture': 'yes'},
    )
    recorder.record_webapp_request_received(
        WebRequestContext(
            request_id='fx-req-1',
            client_name='fixture-client',
            request=None,
            started_at=WEBAPP_STARTED_AT,
            headers={},
        )
    )
    await recorder.flush()
    await recorder.stop()
    recorder_db = Path(list_db_files(str(work_dir), WORKER)[-1])

    engine = CacheEngine(
        config=CacheConfig(enabled=True, db_dir=str(work_dir)),
        ui_config=ui_config,
        worker_id=WORKER,
        cluster_name=CLUSTER,
        recorder=None,
    )
    cache = Cache(origin_worker_id=WORKER)
    engine.attach_cache(cache)
    await engine.start()
    cache.set('fx:global', {'v': 'global'}, scope=CacheScope.GLOBAL)
    cache.set('fx:cluster', 'cluster-value', scope=CacheScope.CLUSTER)
    cache.set('fx:local', 'local-value', scope=CacheScope.LOCAL)
    cache.set('fx:expired', 'gone', ttl=0.001, scope=CacheScope.GLOBAL)
    await engine._flush_once()
    await engine.stop()
    cache_db = work_dir / f'{WORKER}-cache.db.actual'

    return recorder_db, cache_db


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        '--out',
        required=True,
        help='target fixture directory (e.g. ../drakkar-go/internal/crossbackend/testdata/python-db)',
    )
    out_dir = Path(parser.parse_args().out)
    out_dir.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory() as tmp:
        recorder_db, cache_db = asyncio.run(_generate(Path(tmp)))
        shutil.copy(recorder_db, out_dir / 'recorder.db')
        shutil.copy(cache_db, out_dir / 'cache.db')

    (out_dir / 'README.md').write_text(
        'Python-written Drakkar DB fixtures for cross-backend interop tests.\n'
        '\n'
        f'Generated by `just gen-db-fixtures` in the drakkar (Python) repo\n'
        f'(scripts/gen_db_fixtures.py) through the REAL recorder/cache engines.\n'
        f'Worker `{WORKER}`, cluster `{CLUSTER}`; see the generator docstring\n'
        'for the exact row contract the consuming tests rely on.\n'
        'Regenerate + recommit after any schema or encoding change.\n'
    )
    print(f'wrote {out_dir / "recorder.db"} and {out_dir / "cache.db"}')


if __name__ == '__main__':
    main()
