"""Best-effort discovery of host-level facts the worker cannot know otherwise.

The executor pool size is operator-configured, and nothing else in the
framework knows whether the host can back it: a 35-slot pool on an 8-core
machine (or in a container with a 4-core cgroup quota) time-shares every
subprocess, task wall times stretch and converge, and the timeline shows
uniformly slow tasks with no visible cause. This module answers "how many
CPUs are really available", so startup can warn when the pool exceeds it —
plus "how many bytes has the network moved", so the recorder can stream
RX/TX rates to the debug UI.

Everything here is best-effort: a platform without CPU affinity (macOS)
falls back to the plain CPU count, missing cgroup files simply mean "no
quota", and a platform without ``/proc/net/dev`` reports network I/O as
unavailable rather than failing.
"""

from __future__ import annotations

import os
from pathlib import Path

# Kernel-maintained per-interface I/O counters; Linux only. The recorder's
# net_io sampler reads it every state-sync tick.
PROC_NET_DEV = Path('/proc/net/dev')
# Per-process memory / thread facts; Linux only.
PROC_SELF_STATUS = Path('/proc/self/status')
# Open file descriptors of this process. ``/proc/self/fd`` on Linux;
# ``/dev/fd`` is the macOS equivalent, tried second.
FD_DIRS = (Path('/proc/self/fd'), Path('/dev/fd'))

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


def read_net_io_bytes(proc_net_dev: Path = PROC_NET_DEV) -> tuple[int, int] | None:
    """Total ``(rx_bytes, tx_bytes)`` across all non-loopback interfaces.

    Returns ``None`` when the counters are unavailable (no ``/proc/net/dev``
    — macOS, exotic containers) or unreadable; callers treat that as
    "feature off", never as an error.

    These are **host-wide** kernel counters (per network namespace): they
    cover this process and its subprocesses, but also anything else sharing
    the namespace. Per-process network accounting does not exist without
    root/eBPF, so this is the honest best available signal. In a container
    with its own network namespace it is effectively the app's own traffic.

    The path is a parameter only so tests can point at fixtures.
    """
    try:
        lines = proc_net_dev.read_text().splitlines()
    except OSError:
        return None

    rx_total = 0
    tx_total = 0
    seen_interface = False
    # Format (two header lines, then one line per interface):
    #   eth0: <rx_bytes> <rx_packets> ... 8 fields ... <tx_bytes> ...
    for line in lines:
        name, sep, rest = line.partition(':')
        if not sep:
            continue  # header line
        if name.strip() == 'lo':
            continue  # loopback traffic is not network usage
        fields = rest.split()
        if len(fields) < 9:
            continue  # malformed row — skip it, keep the rest
        try:
            rx_total += int(fields[0])
            tx_total += int(fields[8])
        except ValueError:
            continue
        seen_interface = True

    if not seen_interface:
        return None
    return rx_total, tx_total


def read_self_stats(status_path: Path = PROC_SELF_STATUS) -> tuple[int | None, int | None]:
    """Current ``(rss_bytes, thread_count)`` of this process, each None when unavailable.

    Parsed from ``/proc/self/status`` (``VmRSS`` in kB, ``Threads`` a plain
    count), so both are None on macOS. The path is a parameter only so tests
    can point at fixtures.
    """
    try:
        text = status_path.read_text()
    except OSError:
        return None, None

    rss_bytes: int | None = None
    threads: int | None = None
    for line in text.splitlines():
        key, sep, value = line.partition(':')
        if not sep:
            continue
        try:
            if key == 'VmRSS':
                # "VmRSS:      123456 kB"
                rss_bytes = int(value.split()[0]) * 1024
            elif key == 'Threads':
                threads = int(value.strip())
        except (ValueError, IndexError):
            continue
    return rss_bytes, threads


def read_open_fd_count(fd_dirs: tuple[Path, ...] = FD_DIRS) -> int | None:
    """Number of open file descriptors of this process, or None when unknowable.

    A steadily climbing count is the classic slow leak (unclosed pipes,
    sockets, DB handles) that ends in EMFILE hours later — worth a place in
    every resource snapshot. The directories are parameters only for tests.
    """
    for fd_dir in fd_dirs:
        try:
            # The listing itself opens one fd (the directory), included in
            # the count on Linux — a constant off-by-one nobody charts.
            return len(os.listdir(fd_dir))
        except OSError:
            continue
    return None
