"""The ``periodic`` section: the election of cluster tasks and the overrides of declared tasks.

``periodic.election`` picks the provider that decides, for each slot of a
cluster periodic task, the one worker of the cluster that runs it (the
``history.archive`` system task included). ``periodic.tasks.<name>`` changes
the schedule, the lease or the enabled flag of a task the handler declares
with ``@periodic``; an override that names no declared task fails startup.
"""

import time
from collections.abc import Mapping

from pydantic import BaseModel, Field, field_validator, model_validator

from drakkar.config.resilience import ResilienceConfig, validate_resilience
from drakkar.cronexpr import validate_cron
from drakkar.periodic import (
    OVERRIDE_NULL,
    PREFIX_OVERRIDE,
    RULE_AT_MOST_ONE,
    RULE_DOT_NAME,
    RULE_LEASE_BOUNDS,
)
from drakkar.schedule import MAX_LEASE_SECONDS, MIN_LEASE_SECONDS


class ElectionConfig(BaseModel):
    """Which provider decides the one worker of a cluster that runs a cluster task slot."""

    provider: str = Field(
        default='filesystem',
        min_length=1,
        description=(
            'Election provider: filesystem (a lock file in shared.dir), postgres (a row in a table '
            'of the database named by periodic.election.connection), redis (a key on the Redis '
            'server named by periodic.election.connection), consul (a key of the Consul agent named '
            'by periodic.election.connection, held by a session). '
            'Every worker of a cluster must use the same provider.'
        ),
    )
    connection: str = Field(
        default='main',
        min_length=1,
        description=(
            'Name of the entry under connections.<provider> that the consul, postgres and redis '
            'providers use. The other providers ignore it.'
        ),
    )
    resilience: ResilienceConfig = Field(
        default_factory=ResilienceConfig,
        description='Time limit, retries and circuit breaker for the calls to the election provider.',
    )

    @model_validator(mode='after')
    def _validate_resilience(self) -> 'ElectionConfig':
        """Check the rules of the resilience block that span fields, naming it by its YAML path."""
        validate_resilience('periodic.election.resilience', self.resilience)
        return self


class PeriodicTaskOverride(BaseModel):
    """Changes to one declared periodic task; a key left out keeps the declared value."""

    enabled: bool = Field(
        default=True,
        description='false = the task does not run on this worker (and leaves the cluster definitions).',
    )
    interval: float | None = Field(
        default=None,
        description=(
            'New interval in seconds; replaces the declared interval or cron. Give at most one of '
            'interval and cron. A cluster task needs a whole number of at least 30.'
        ),
    )
    cron: str | None = Field(
        default=None,
        description=(
            'New five-field UTC cron expression; replaces the declared interval or cron. Give at '
            'most one of interval and cron.'
        ),
    )
    lease_seconds: int | None = Field(
        default=None,
        description=(
            f'New lease of a cluster task in seconds, a whole number from {MIN_LEASE_SECONDS} to '
            f'{MAX_LEASE_SECONDS}. Only cluster tasks take a lease.'
        ),
    )


def _override_rule(name: str, override: PeriodicTaskOverride, now_epoch: int) -> str | None:
    """The first load rule ``override`` breaks, without prefix, or ``None``.

    Order: a system task name, at most one schedule, the cron check, the lease bounds.
    The rules that need the declared task run at startup.
    """
    if '.' in name:
        return RULE_DOT_NAME
    if override.interval is not None and override.cron is not None:
        return RULE_AT_MOST_ONE
    if override.cron is not None:
        message = validate_cron(override.cron, now_epoch=now_epoch)
        if message is not None:
            return message
    if override.lease_seconds is not None and not MIN_LEASE_SECONDS <= override.lease_seconds <= MAX_LEASE_SECONDS:
        return RULE_LEASE_BOUNDS
    return None


class PeriodicConfig(BaseModel):
    """The election of cluster periodic tasks and the overrides of declared tasks."""

    election: ElectionConfig = Field(
        default_factory=ElectionConfig,
        description='How the one worker of a cluster that runs a cluster task slot is chosen.',
    )
    tasks: dict[str, PeriodicTaskOverride] = Field(
        default_factory=dict,
        description=(
            'Overrides of declared periodic tasks, keyed by the method name. A name the handler does '
            'not declare fails startup; a name with a dot (a system task) fails the load.'
        ),
    )

    @field_validator('tasks', mode='before')
    @classmethod
    def _reject_entries_without_value(cls, value: object) -> object:
        """Refuse an entry with no value (``name:`` and nothing after it), naming the first in sorted order.

        Without this check pydantic would report a type error that does not
        say an empty mapping keeps the declared settings.
        """
        if isinstance(value, Mapping):
            empty = sorted(str(name) for name, entry in value.items() if entry is None)
            if empty:
                raise ValueError(OVERRIDE_NULL.replace('{name}', empty[0]))
        return value

    @model_validator(mode='after')
    def _validate_overrides(self) -> 'PeriodicConfig':
        """Check every override in sorted name order, so the first message is the same for any key order."""
        now_epoch = int(time.time())
        for name in sorted(self.tasks):
            message = _override_rule(name, self.tasks[name], now_epoch)
            if message is not None:
                raise ValueError(PREFIX_OVERRIDE.format(name=name) + message)
        return self
