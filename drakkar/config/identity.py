"""The names of the worker and of its cluster.

Each name is a literal, or the value of an environment variable: the variable
named by ``name_env`` wins when it is set and not empty.
"""

import os
import re
import secrets
import socket

from pydantic import Field

from drakkar.config.base import ConfigModel
from drakkar.fleet.card import sanitize_worker_name

# The worker name is a folder name, a file name prefix and the key of the
# registry card on every provider, so it must be one safe word. A name that
# passes is equal to its sanitized form everywhere.
WORKER_NAME_MAX_LENGTH = 128
_WORKER_NAME = re.compile(rf'[A-Za-z0-9_-]{{1,{WORKER_NAME_MAX_LENGTH}}}')
WORKER_NAME_INVALID = "worker.name: '{name}' must be 1 to 128 letters, digits, '_' or '-'; use '{suggestion}'"

# A host name that names no host in particular: two workers would share it.
_UNSPECIFIC_HOST_NAMES = frozenset({'localhost'})
GENERATED_NAME_PREFIX = 'drakkar-'


def is_valid_worker_name(name: str) -> bool:
    """True when ``name`` is 1 to 128 characters of ``[A-Za-z0-9_-]``."""
    return _WORKER_NAME.fullmatch(name) is not None


def safe_worker_name(text: str) -> str:
    """``text`` made safe as a worker name: sanitized and cut to 128 characters (``''`` stays ``''``)."""
    return sanitize_worker_name(text)[:WORKER_NAME_MAX_LENGTH]


def check_worker_name(name: str) -> None:
    """Refuse a configured worker name that is not equal to its safe form.

    Raises:
        ValueError: the name breaks the rule; the message suggests the safe form.
    """
    if not is_valid_worker_name(name):
        raise ValueError(WORKER_NAME_INVALID.format(name=name, suggestion=safe_worker_name(name)))


def generated_worker_name() -> str:
    """The worker name when none is configured: the host name made safe, else ``drakkar-<8 hex>``.

    In Kubernetes the host name is the pod name, which a container restart
    keeps, so a restarted worker finds its own folder again. A host name
    that is empty or ``localhost`` names no host in particular; a random
    name is used then.
    """
    host = safe_worker_name(socket.gethostname())
    if host and host.lower() not in _UNSPECIFIC_HOST_NAMES:
        return host
    return f'{GENERATED_NAME_PREFIX}{secrets.token_hex(4)}'


def _name_from(literal: str, name_env: str) -> str:
    """The value of the variable ``name_env`` when it is set and not empty, else ``literal``."""
    from_env = os.environ.get(name_env, '') if name_env else ''
    return from_env or literal


class WorkerConfig(ConfigModel):
    """The worker name used in logs, metrics, the registry card and the UI."""

    name: str = Field(
        default='',
        description=(
            "Literal worker name: 1 to 128 letters, digits, '_' or '-', unique in the fleet. "
            'Empty, and no value from name_env: the host name with every other character turned '
            'into _, or drakkar-<8 hex> when the host name is empty or localhost.'
        ),
    )
    name_env: str = Field(
        default='WORKER_ID',
        description=(
            'Environment variable that holds the worker name; set and not empty, it wins over name. '
            'Its value follows the same rule as name.'
        ),
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
