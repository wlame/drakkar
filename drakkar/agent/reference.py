"""The generated agent reference page: every registry entry as one Markdown page for the docs site.

The page is the output of ``drakkar-agent reference``. A test checks that the
committed ``docs/agent-reference.md`` equals it, so the page cannot drift from
the registry.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from drakkar.agent.registry import (
    EMPTY_PART_MARKDOWN,
    FIELD_STYLES,
    Registry,
    _full_entry,
    _md_field,
    reference_of,
)

CODE_MARK = '`'
# Escaped outside code spans, so a text such as <storage.dir> is shown and not read as HTML.
HTML_ESCAPES = (('&', '&amp;'), ('<', '&lt;'), ('>', '&gt;'))

# The registry parts on the page, in manual order, with their headings. Handler notes are not on it.
REFERENCE_PARTS: tuple[tuple[str, str], ...] = (
    ('concepts', 'Concepts'),
    ('config', 'Config advice'),
    ('metrics', 'Metrics'),
    ('events', 'Events'),
    ('recipes', 'Recipes'),
    ('routes', 'Routes'),
    ('rules', 'Rules'),
)

PAGE_TITLE = '# Agent Reference'
PAGE_INTRO = (
    'This page is generated from the agent knowledge registry by `drakkar-agent reference`. '
    'Do not edit it by hand: run `just agent-reference`.',
    'A worker with `agent.enabled: true` serves the same entries on `GET /api/v1/agent/manual`. '
    'See [Agent Surface](agent.md) for the routes, the config keys and the handler notes.',
    'PromQL queries keep the placeholders `{{worker}}` and `{{cluster}}`. '
    'A worker fills them from `ui.prometheus.worker_label` and `ui.prometheus.cluster_label`.',
)


def escape_outside_code(text: str) -> str:
    """Escape ``&``, ``<`` and ``>`` outside code spans.

    The text is split at each backtick; the pieces at even positions are outside
    a code span. With an odd number of backticks the last piece is at an odd
    position, so it is kept as it is.
    """
    pieces = text.split(CODE_MARK)
    for index in range(0, len(pieces), 2):
        for raw, escaped in HTML_ESCAPES:
            pieces[index] = pieces[index].replace(raw, escaped)
    return CODE_MARK.join(pieces)


def _entry_blocks(part: str, entry: Mapping[str, Any]) -> list[str]:
    """The blocks of one entry: its reference as a heading, its summary, then one block per field."""
    rendered = _full_entry(entry, promql_filters=None)
    blocks = [f'### `{reference_of(part, entry["key"])}`', escape_outside_code(entry['summary'])]
    blocks.extend(
        escape_outside_code(_md_field(style, label, rendered[name]))
        for name, label, style in FIELD_STYLES
        if name in rendered
    )
    return blocks


def reference_markdown(registry: Registry) -> str:
    """The reference page: blocks joined by one empty line, ending with one LF."""
    blocks = [PAGE_TITLE, *PAGE_INTRO, f'Registry version: `{registry.version}`.']
    for part, heading in REFERENCE_PARTS:
        blocks.append(f'## {heading}')
        entries = registry.parts[part]
        if not entries:
            blocks.append(EMPTY_PART_MARKDOWN)
        for entry in entries:
            blocks.extend(_entry_blocks(part, entry))
    return '\n\n'.join(blocks) + '\n'
