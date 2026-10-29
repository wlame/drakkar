"""The names of the worker and of its cluster.

Each name is a literal, or the value of an environment variable: the variable
named by ``name_env`` wins when it is set and not empty.
"""

import os

from pydantic import Field

from drakkar.config.base import ConfigModel


def _name_from(literal: str, name_env: str) -> str:
    """The value of the variable ``name_env`` when it is set and not empty, else ``literal``."""
    from_env = os.environ.get(name_env, '') if name_env else ''
    return from_env or literal


class WorkerConfig(ConfigModel):
    """The worker name used in logs, metrics, the registry card and the UI."""

    name: str = Field(
        default='',
        description='Literal worker name. Empty, and no value from name_env: the generated drakkar-<hex>.',
    )
    name_env: str = Field(
        default='WORKER_ID',
        description='Environment variable that holds the worker name; set and not empty, it wins over name.',
    )

    def resolved_name(self) -> str:
        """The configured name, or ``''`` when neither the variable nor ``name`` gives one."""
        return _name_from(self.name, self.name_env)


class ClusterConfig(ConfigModel):
    """The cluster this worker belongs to: the lock scope of cluster tasks and the history folder."""

    name: str = Field(
        default='',
        description=(
            'Literal cluster name. Empty with autodiscover on logs one startup warning naming the scope default.'
        ),
    )
    name_env: str = Field(
        default='',
        description='Environment variable that holds the cluster name; set and not empty, it wins over name.',
    )

    def resolved_name(self) -> str:
        """The cluster name: the variable's value when it is set and not empty, else ``name``."""
        return _name_from(self.name, self.name_env)
