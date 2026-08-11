"""Best-effort discovery of how much CPU this worker can actually use.

The executor pool size is operator-configured, and nothing else in the
framework knows whether the host can back it: a 35-slot pool on an 8-core
machine (or in a container with a 4-core cgroup quota) time-shares every
subprocess, task wall times stretch and converge, and the timeline shows
uniformly slow tasks with no visible cause. This module answers "how many
CPUs are really available", so startup can warn when the pool exceeds it.

Everything here is best-effort and read once at startup: a platform without
CPU affinity (macOS) falls back to the plain CPU count, and missing cgroup
files simply mean "no quota".
"""

from __future__ import annotations

import os
from pathlib import Path

# cgroup v2 unified hierarchy: one file, "``<quota> <period>``" in
# microseconds, or "``max <period>``" when unlimited.
CGROUP_V2_CPU_MAX = Path('/sys/fs/cgroup/cpu.max')
# cgroup v1: quota and period in separate files; quota -1 means unlimited.
CGROUP_V1_CPU_QUOTA = Path('/sys/fs/cgroup/cpu/cpu.cfs_quota_us')
CGROUP_V1_CPU_PERIOD = Path('/sys/fs/cgroup/cpu/cpu.cfs_period_us')


def effective_cpu_count() -> int:
    """CPUs this process can actually use, as a whole number (min 1).

    The affinity mask (which cores the scheduler may place us on) capped by
    the cgroup CPU quota (how much cumulative CPU time those cores may
    spend). A container commonly sees the host's full core count in
    ``os.cpu_count()`` while its quota allows a fraction of it — the quota
    is the truth there.
    """
    cores = _affinity_count()
    quota = cgroup_cpu_quota()
    if quota is not None:
        # A 2.5-core quota cannot run 3 subprocesses at full speed — round
        # the cap DOWN, but never below one.
        cores = min(cores, max(1, int(quota)))
    return cores


def _affinity_count() -> int:
    # getattr rather than a direct call: sched_getaffinity is Linux-only, and
    # the attribute does not exist (even in type stubs) on macOS.
    getaffinity = getattr(os, 'sched_getaffinity', None)
    if getaffinity is not None:
        return len(getaffinity(0))
    return os.cpu_count() or 1


def cgroup_cpu_quota(
    v2_cpu_max: Path = CGROUP_V2_CPU_MAX,
    v1_quota: Path = CGROUP_V1_CPU_QUOTA,
    v1_period: Path = CGROUP_V1_CPU_PERIOD,
) -> float | None:
    """The cgroup CPU limit in cores, or None when unlimited/undetectable.

    The file paths are parameters only so tests can point at fixtures; real
    callers use the defaults.
    """
    try:
        if v2_cpu_max.exists():
            quota_part, _, period_part = v2_cpu_max.read_text().strip().partition(' ')
            if quota_part == 'max':
                return None
            return int(quota_part) / int(period_part)
        if v1_quota.exists() and v1_period.exists():
            quota_us = int(v1_quota.read_text().strip())
            if quota_us <= 0:
                return None
            return quota_us / int(v1_period.read_text().strip())
    except (OSError, ValueError):
        # Malformed or unreadable cgroup files — treat as "no quota" rather
        # than failing worker startup over a diagnostic.
        return None
    return None
