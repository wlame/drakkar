"""Regenerate the recorder event-type vocabulary fixture.

Event names are a wire contract: they are written into ``events.event``
and the UI matches on them, so a rename is a breaking change rather than
a refactor. :class:`drakkar.recorder.schema.EventType` is the source of
truth; this script vendors it as JSON so a test can pin the enum against
a reviewed list instead of against itself.

Usage::

    just gen-event-vocabulary            # rewrites tests/fixtures/
    uv run python scripts/gen_event_vocabulary.py --out=DIR
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from drakkar.recorder.schema import EventType

# Names that are broadcast on ``/ws`` but never written to ``events.event``.
# ``throughput`` is a periodic aggregate the UI renders live; buffering it
# would grow the events table without any query reading it back.
WS_ONLY: frozenset[str] = frozenset({'throughput'})

FIXTURE_NAME = 'event_vocabulary.json'
DEFAULT_OUT = Path(__file__).resolve().parent.parent / 'tests' / 'fixtures'

COMMENT = (
    'Recorder event-type vocabulary - a wire contract. Generated from '
    'drakkar.recorder.schema.EventType via `just gen-event-vocabulary`. '
    'Do not hand-edit.'
)


def build_document() -> dict[str, object]:
    """Return the fixture body: the sorted vocabulary and where each name lands."""
    events = sorted(member.value for member in EventType)
    return {
        '_comment': COMMENT,
        'events': events,
        # True when the name appears in the events.event column; False for
        # the WS-only broadcasts. The docs table is checked against this.
        'stored_in_events': {name: name not in WS_ONLY for name in events},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--out',
        type=Path,
        default=DEFAULT_OUT,
        help=f'directory to write {FIXTURE_NAME} into (default: tests/fixtures)',
    )
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    target = args.out / FIXTURE_NAME
    target.write_text(json.dumps(build_document(), indent=2, ensure_ascii=False) + '\n')
    print(f'wrote {target}')


if __name__ == '__main__':
    main()
