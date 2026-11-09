"""The answer of the agent metrics route: the current samples of each metric, with its meaning.

The input is what a Prometheus registry collects. A metric is listed under the
name it is exposed with (a counter with ``_total``, an info with ``_info``).
Samples are sorted by their label values, label names in byte order, and at most
:data:`MAX_SAMPLES` of them are sent; ``samples_total`` counts them all. A
histogram has one sample per label set, with its count, sum and cumulative
buckets.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from drakkar.agent.registry import Registry, fill_promql

MAX_SAMPLES = 100

_EXPOSED_SUFFIX = {'counter': '_total', 'info': '_info'}
_SERIES_TYPES = ('histogram', 'gaugehistogram', 'summary')
_COUNT_SUFFIXES = ('_count', '_gcount')
_SUM_SUFFIXES = ('_sum', '_gsum')
_CREATED_SUFFIX = '_created'
_BUCKET_SUFFIX = '_bucket'
_BOUND_LABEL = 'le'


@dataclass(frozen=True)
class MetricsQuery:
    """The filter of one metrics answer: ``names`` (``None`` when absent) and ``prefix``."""

    names: Sequence[str] | None = None
    prefix: str | None = None

    @property
    def is_filtered(self) -> bool:
        """True when the answer is not the one of every metric."""
        return self.names is not None or self.prefix is not None

    def matches(self, name: str) -> bool:
        """A metric is listed when no filter is given, or when it is one of ``names`` or starts with ``prefix``."""
        if not self.is_filtered:
            return True
        return (self.names is not None and name in self.names) or (
            self.prefix is not None and name.startswith(self.prefix)
        )


@dataclass(frozen=True)
class MetricNotes:
    """The meaning of the handler's own metrics, by exposed name; ``promql`` is sent only in ``full`` detail."""

    meaning: Mapping[str, str] = field(default_factory=dict)
    healthy: Mapping[str, str] = field(default_factory=dict)
    promql: Mapping[str, Sequence[Mapping[str, str]]] = field(default_factory=dict)


def exposed_name(family_name: str, family_type: str) -> str:
    """The name a family is exposed with: a counter adds ``_total``, an info adds ``_info``."""
    return family_name + _EXPOSED_SUFFIX.get(family_type, '')


def json_number(value: float) -> int | float | None:
    """A sample value for JSON: a whole number as an integer, ``None`` for NaN and infinities."""
    if not math.isfinite(value):
        return None
    if value == int(value):
        return int(value)
    return value


def bound_text(bound: float) -> str:
    """An upper bound in the Prometheus text form: ``0.005``, ``1``, ``+Inf``."""
    if math.isinf(bound):
        return '+Inf' if bound > 0 else '-Inf'
    if bound == int(bound) and abs(bound) < 1e15:
        return str(int(bound))
    return repr(bound)


def _sorted_labels(labels: Mapping[str, str]) -> dict[str, str]:
    return {name: labels[name] for name in sorted(labels)}


def _label_order(labels: Mapping[str, str]) -> tuple[bytes, ...]:
    return tuple(labels[name].encode('utf-8') for name in sorted(labels))


def _value_samples(samples: Iterable[Any]) -> list[dict[str, Any]]:
    return [
        {'labels': _sorted_labels(sample.labels), 'value': json_number(sample.value)}
        for sample in samples
        if not sample.name.endswith(_CREATED_SUFFIX)
    ]


def _series_samples(family_name: str, samples: Iterable[Any]) -> list[dict[str, Any]]:
    """One sample per label set: ``count``, ``sum`` and the cumulative ``buckets`` in bound order."""
    series: dict[tuple[tuple[str, str], ...], dict[str, Any]] = {}
    for sample in samples:
        suffix = sample.name[len(family_name) :]
        labels = {name: value for name, value in sample.labels.items() if name != _BOUND_LABEL}
        row = series.setdefault(
            tuple(sorted(labels.items())),
            {'labels': _sorted_labels(labels), 'count': 0, 'sum': 0, 'buckets': []},
        )
        if suffix in _COUNT_SUFFIXES:
            row['count'] = json_number(sample.value)
        elif suffix in _SUM_SUFFIXES:
            row['sum'] = json_number(sample.value)
        elif suffix == _BUCKET_SUFFIX:
            row['buckets'].append((float(sample.labels[_BOUND_LABEL]), int(sample.value)))
    for row in series.values():
        row['buckets'] = [{'le': bound_text(bound), 'count': count} for bound, count in sorted(row['buckets'])]
    return list(series.values())


def family_samples(family: Any) -> list[dict[str, Any]]:
    """Every sample of one collected family, sorted by label values."""
    if family.type in _SERIES_TYPES:
        samples = _series_samples(family.name, family.samples)
    else:
        samples = _value_samples(family.samples)
    samples.sort(key=lambda sample: _label_order(sample['labels']))
    return samples


def metrics_answer(
    *,
    families: Iterable[Any],
    registry: Registry,
    handler_families: frozenset[str],
    query: MetricsQuery,
    full: bool,
    promql_filters: Mapping[str, str],
    notes: MetricNotes,
) -> dict[str, Any]:
    """``{"metrics": [...], "unknown": [...]}`` for the framework metrics and the handler's metrics.

    ``handler_families`` holds the family names of the handler's metrics. Metrics
    are listed by exposed name in byte order; ``unknown`` holds the asked names
    that no listed metric has, in the asked order.
    """
    entries = {entry['key']: entry for entry in registry.parts['metrics']}
    listed: dict[str, dict[str, Any]] = {}
    for family in families:
        name = exposed_name(family.name, family.type)
        if name not in entries and family.name not in handler_families:
            continue
        if not query.matches(name):
            continue
        entry = entries.get(name)
        row: dict[str, Any] = {
            'name': name,
            'type': family.type,
            'help': family.documentation,
            'meaning': entry['meaning'] if entry else notes.meaning.get(name),
            'healthy': entry['healthy'] if entry else notes.healthy.get(name),
        }
        if full and entry is not None:
            if 'promql' in entry:
                row['promql'] = [
                    {'title': recipe['title'], 'query': fill_promql(recipe['query'], promql_filters)}
                    for recipe in entry['promql']
                ]
            if 'related' in entry:
                row['related'] = list(entry['related'])
        elif full and name in notes.promql:
            row['promql'] = [
                {'title': recipe['title'], 'query': fill_promql(recipe['query'], promql_filters)}
                for recipe in notes.promql[name]
            ]
        samples = family_samples(family)
        row['samples'] = samples[:MAX_SAMPLES]
        row['samples_total'] = len(samples)
        listed[name] = row
    unknown = [name for name in query.names or () if name not in listed]
    return {
        'metrics': [listed[name] for name in sorted(listed, key=lambda text: text.encode('utf-8'))],
        'unknown': list(dict.fromkeys(unknown)),
    }
