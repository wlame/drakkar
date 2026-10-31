"""The allowed range of each config value written with a unit.

A bound message prints the bounds and the value as canonical unit texts
(``recorder.memory.max_size must be from 16MiB to 512MiB, got 600MiB``), the
form an operator writes, never the seconds or bytes a model holds. The table
is data: one row per key, the bounds in the unit the model holds. A strict
lower bound is written as the smallest value that passes (``1ms`` for "more
than zero" on a duration held in seconds), so every message says "at least".
"""

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from drakkar.config.units import unit_kind_of

KIB = 1024
MIB = 1024 * KIB
GIB = 1024 * MIB

# The smallest duration a unit text can hold, in seconds: "more than zero" on a duration held in seconds.
ONE_MILLISECOND = 0.001

# The sink types whose instances share the policy keys of sinks.defaults.
_SINK_TYPES = ('kafka', 'postgres', 'mongo', 'redis', 'http', 'filesystem')

# A path segment that stands for every name of a map, visited in sorted order.
EVERY_NAME = '*'


@dataclass(frozen=True)
class UnitBound:
    """The range of one key; ``path`` is its dotted YAML path, ``*`` for each name of a map.

    ``minimum`` and ``maximum`` are in the unit the model holds (seconds,
    minutes, hours or bytes); ``None`` means no bound on that side.
    """

    path: str
    minimum: float | None
    maximum: float | None


UNIT_BOUNDS: tuple[UnitBound, ...] = (
    UnitBound('sources.kafka.startup_align.interval', 1, None),
    UnitBound('executor.task_timeout', 1, None),
    UnitBound('executor.drain_timeout', 1, None),
    UnitBound('sinks.defaults.delivery_timeout', ONE_MILLISECOND, None),
    *(UnitBound(f'sinks.{sink_type}.*.delivery_timeout', ONE_MILLISECOND, None) for sink_type in _SINK_TYPES),
    UnitBound('sinks.kafka.*.flush_timeout', ONE_MILLISECOND, None),
    UnitBound('sinks.http.*.request_timeout', 1, None),
    UnitBound('dlq.flush_timeout', ONE_MILLISECOND, None),
    UnitBound('recorder.flush_interval', 1, None),
    UnitBound('recorder.memory.window', 10, 86_400),
    UnitBound('recorder.memory.max_size', 16 * MIB, 512 * MIB),
    UnitBound('recorder.sqlite.rotation_interval', 1, None),
    UnitBound('recorder.sqlite.state_interval', 1, None),
    UnitBound('recorder.archive.window', 1, None),
    UnitBound('recorder.archive.due_after', 1, None),
    UnitBound('recorder.postgres.initial_retention', 1, 168),
    UnitBound('recorder.postgres.max_queue', 16 * MIB, GIB),
    UnitBound('recorder.postgres.resilience.timeout', ONE_MILLISECOND, None),
    UnitBound('recorder.search.timeout', ONE_MILLISECOND, None),
    UnitBound('cache.flush_interval', ONE_MILLISECOND, None),
    UnitBound('cache.cleanup_interval', ONE_MILLISECOND, None),
    UnitBound('cache.exchange.interval', ONE_MILLISECOND, None),
    UnitBound('cache.exchange.timeout', ONE_MILLISECOND, None),
    UnitBound('cache.exchange.cycle_deadline', 0.1, None),
    UnitBound('autodiscover.heartbeat', 1, None),
    UnitBound('autodiscover.refresh', 1, None),
    UnitBound('autodiscover.offline_after', 1, None),
    UnitBound('autodiscover.prune_after', 1, None),
    UnitBound('autodiscover.resilience.timeout', ONE_MILLISECOND, None),
    UnitBound('runtime_health.tick', 0.011, None),
    UnitBound('runtime_health.warn_lag', ONE_MILLISECOND, None),
    UnitBound('runtime_health.stall', ONE_MILLISECOND, None),
    UnitBound('runtime_health.sample_interval', ONE_MILLISECOND, None),
    UnitBound('runtime_health.history_window', 60, None),
    UnitBound('runtime_health.max_episode', 10, None),
    UnitBound('runtime_health.stack_probe_interval', ONE_MILLISECOND, None),
    UnitBound('ui.probe.max_total_size', 1, None),
    UnitBound('ui.timeline.max_age', 1, 1440),
)


def bound_message(path: str, bound: UnitBound, value: Any, to_text: Any) -> str | None:
    """The message for ``value`` outside ``bound``, or ``None`` when it is inside; texts come from ``to_text``."""
    below = bound.minimum is not None and value < bound.minimum
    above = bound.maximum is not None and value > bound.maximum
    if not below and not above:
        return None
    got = to_text(value)
    if bound.minimum is not None and bound.maximum is not None:
        return f'{path} must be from {to_text(bound.minimum)} to {to_text(bound.maximum)}, got {got}'
    if bound.minimum is not None:
        return f'{path} must be at least {to_text(bound.minimum)}, got {got}'
    return f'{path} must be at most {to_text(bound.maximum)}, got {got}'


def check_unit_bounds(config: BaseModel) -> None:
    """Raise for the first value outside its range, in table order.

    Raises:
        ValueError: ``<path> must be from <min> to <max>, got <value>`` (or ``at least``, ``at most``).
    """
    for bound in UNIT_BOUNDS:
        for path, value, to_text in _values_at(config, bound.path.split('.'), ''):
            message = bound_message(path, bound, value, to_text)
            if message is not None:
                raise ValueError(message)


def _values_at(node: Any, segments: list[str], prefix: str) -> Iterator[tuple[str, Any, Any]]:
    """Each ``(path, value, to_text)`` at ``segments`` below ``node``; a ``None`` value has no bound."""
    head, rest = segments[0], segments[1:]
    if head == EVERY_NAME:
        for name in sorted(node):
            yield from _values_at(node[name], rest, f'{prefix}.{name}')
        return
    path = f'{prefix}.{head}' if prefix else head
    child = getattr(node, head)
    if rest:
        yield from _values_at(child, rest, path)
        return
    if child is not None:
        yield path, child, _unit_text_of(type(node), head)


def _unit_text_of(model: type[BaseModel], field_name: str) -> Any:
    """The canonical-text writer of a unit field."""
    kind = unit_kind_of(model.model_fields[field_name])
    if kind is None:
        raise TypeError(f'{model.__name__}.{field_name} is not a field with a unit')
    return kind.to_text
