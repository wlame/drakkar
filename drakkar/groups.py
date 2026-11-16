"""Task groups of the streaming arrange hook: creation, label collection, note checks."""

from __future__ import annotations

from collections.abc import Sequence

from drakkar.models import ExecutorTask, SourceMessage, TaskGroup

# Exact types, not isinstance: a subclass of str or int can carry attributes,
# and so a reference to a task or a result, which a note must never hold.
_NOTE_VALUE_TYPES = frozenset({str, int, float, bool})


class ArrangeStreamError(Exception):
    """A value that ``arrange_stream`` produced breaks the rules of the hook."""


def check_stream_item(item: object, message: SourceMessage) -> list[ExecutorTask]:
    """Return ``item`` as the task list of one group, or raise ``ArrangeStreamError``.

    The item must be a list of tasks, each covering only ``message``, and no
    task may belong to a group already: the same task object in two groups
    (or twice in one list) would count its outcome twice.
    """
    if not isinstance(item, list):
        raise ArrangeStreamError(f'arrange_stream must yield a list of ExecutorTask, got {type(item).__name__}')
    expected_offsets = [message.offset]
    seen: set[int] = set()
    for task in item:
        if not isinstance(task, ExecutorTask):
            raise ArrangeStreamError(f'arrange_stream must yield a list of ExecutorTask, got {type(task).__name__}')
        if task.source_offsets != expected_offsets:
            raise ArrangeStreamError(
                f'arrange_stream yielded task {task.task_id} with source_offsets {task.source_offsets}; '
                f'a stream task covers only its message, offset {message.offset}'
            )
        if task.group is not None or id(task) in seen:
            raise ArrangeStreamError(f'arrange_stream yielded task {task.task_id} twice; a task belongs to one group')
        seen.add(id(task))
    return item


def collect_group_labels(tasks: list[ExecutorTask], names: Sequence[str]) -> dict[str, list[str]]:
    """Return, for each declared label name, the values of that label on the tasks.

    Values keep the order of the task list, and equal values stay: one entry
    for each task that has the label. A name that no task has gives ``[]``.
    """
    labels: dict[str, list[str]] = {name: [] for name in names}
    if not labels:
        return labels
    for task in tasks:
        for name, values in labels.items():
            value = task.labels.get(name)
            if value is not None:
                values.append(value)
    return labels


def new_task_group(
    *,
    index: int,
    source_message: SourceMessage,
    tasks: list[ExecutorTask],
    label_names: Sequence[str],
    started_at: float,
) -> TaskGroup:
    """Create the group of one yielded task list and set ``task.group`` on each task.

    The group keeps the count and the declared labels of the tasks, never the
    tasks themselves.
    """
    group = TaskGroup(
        index=index,
        source_message=source_message,
        task_count=len(tasks),
        labels=collect_group_labels(tasks, label_names),
        started_at=started_at,
    )
    for task in tasks:
        task.group = group
    return group


def drop_invalid_notes(group: TaskGroup) -> list[str]:
    """Remove the notes whose value is not a str, int, float or bool; return their keys, sorted."""
    invalid = sorted(key for key, value in group.notes.items() if type(value) not in _NOTE_VALUE_TYPES)
    for key in invalid:
        del group.notes[key]
    return invalid
