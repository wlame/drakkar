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
PROC_MOUNTSTATS = Path('/proc/self/mountstats')
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

# Host load averages (1/5/15 min); Linux only.
PROC_LOADAVG = Path('/proc/loadavg')
# Kernel pressure-stall information (PSI): one file per resource, each with
# precomputed 10s/60s/300s averages — Linux ≥ 4.20, sometimes masked in
# containers.
PROC_PRESSURE_DIR = Path('/proc/pressure')
# cgroup CPU throttling counters: cumulative "how often / how long did the
# quota gate us". v2 keeps them in cpu.stat next to usage; v1 in its own
# cpu.stat with throttled_time in NANOseconds.
CGROUP_V2_CPU_STAT = Path('/sys/fs/cgroup/cpu.stat')
CGROUP_V1_CPU_STAT = Path('/sys/fs/cgroup/cpu/cpu.stat')
# Mount table used to answer "what filesystem type backs this path".
PROC_MOUNTS = Path('/proc/mounts')
# Per-thread scheduler stats; {tid} is a kernel thread id (native_id).
PROC_TASK_STAT_TEMPLATE = '/proc/self/task/{tid}/stat'

# Filesystem types that put a network round-trip under every write — a
# recorder database on one of these shares fate with the network path.
NETWORK_FS_TYPES = frozenset(
    {
        'nfs',
        'nfs2',
        'nfs3',
        'nfs4',
        'cifs',
        'smb3',
        'smbfs',
        'sshfs',
        'fuse.sshfs',
        '9p',
        'glusterfs',
        'ceph',
        'lustre',
        'afs',
    }
)

# PSI files worth sampling, as (file name, line kind, emitted key) rows —
# data over branching so adding a resource is a row, not a code path.
# cpu has no meaningful `full` line (newer kernels print one, always ~0).
_PSI_ROWS = (
    ('cpu', 'some', 'cpu_some_avg10'),
    ('io', 'some', 'io_some_avg10'),
    ('io', 'full', 'io_full_avg10'),
    ('memory', 'some', 'mem_some_avg10'),
    ('memory', 'full', 'mem_full_avg10'),
)


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


def read_nfs_io_bytes(mountstats: Path = PROC_MOUNTSTATS) -> tuple[int, int] | None:
    """Total ``(read_bytes, write_bytes)`` transferred to/from NFS servers.

    Summed across every NFS mount visible to this process, from
    ``/proc/self/mountstats``. Returns ``None`` when the file is
    unavailable (macOS) or no NFS mount is visible; callers treat that as
    "feature off", never as an error.

    Why this exists next to :func:`read_net_io_bytes`: in a container
    with its own network namespace, kernel-NFS traffic is INVISIBLE to
    the namespace's interface counters — the RPC traffic leaves through
    the HOST's interfaces, because the host kernel's NFS client does the
    transfer. ``mountstats``, by contrast, follows the *mount namespace*:
    a bind-mounted NFS volume shows up here with live byte counters, so a
    containerized worker reading task inputs over NFS finally sees that
    traffic. We report the ``server_read`` / ``server_write`` columns of
    the ``bytes:`` line — bytes actually transferred over the wire, page
    cache hits excluded.

    Honest caveat: the counters are per *mount* (per superblock), so any
    other process on the host using the same mount contributes to them.
    On a worker whose NFS volume exists for its own inputs, they are
    effectively the app's traffic.

    The path is a parameter only so tests can point at fixtures.
    """
    try:
        lines = mountstats.read_text().splitlines()
    except OSError:
        return None

    read_total = 0
    write_total = 0
    seen_nfs = False
    in_nfs_section = False
    # Format: one ``device <src> mounted on <mnt> with fstype <fs> ...``
    # line starts each mount's section; NFS sections then carry indented
    # stat lines, among them:
    #   bytes: <normal_read> <normal_write> <direct_read> <direct_write>
    #          <server_read> <server_write> <read_pages> <write_pages>
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('device '):
            # ``with fstype nfs`` / ``nfs4`` (word-split so 'nfsd' or a
            # hypothetical 'nfsX' prefix match cannot false-positive).
            parts = stripped.split()
            try:
                fstype = parts[parts.index('fstype') + 1]
            except (ValueError, IndexError):
                fstype = ''
            in_nfs_section = fstype in ('nfs', 'nfs2', 'nfs3', 'nfs4')
            continue
        if not in_nfs_section or not stripped.startswith('bytes:'):
            continue
        fields = stripped.removeprefix('bytes:').split()
        if len(fields) < 6:
            continue  # malformed row — skip it, keep other mounts
        try:
            read_total += int(fields[4])
            write_total += int(fields[5])
        except ValueError:
            continue
        seen_nfs = True

    if not seen_nfs:
        return None
    return read_total, write_total


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


def read_loadavg(path: Path = PROC_LOADAVG) -> tuple[float, float, float] | None:
    """Host load averages ``(load1, load5, load15)``, or None when unavailable.

    Load average counts runnable + uninterruptible-sleep tasks host-wide —
    the classic "is the whole box oversubscribed" number, complementing the
    per-cgroup views below. The path is a parameter only for tests.
    """
    try:
        fields = path.read_text().split()
        return float(fields[0]), float(fields[1]), float(fields[2])
    except (OSError, ValueError, IndexError):
        return None


def read_pressure(dir_path: Path = PROC_PRESSURE_DIR) -> dict[str, float] | None:
    """Kernel pressure-stall averages (PSI), or None when PSI is unavailable.

    Returns the 10-second averages as ``{cpu_some_avg10, io_some_avg10,
    io_full_avg10, mem_some_avg10, mem_full_avg10}`` — percent of wall time
    in which at least one task (``some``) or every non-idle task (``full``)
    was stalled on the resource. These are the kernel's own precomputed
    windows, so a 10-second sampling cadence reads them loss-free.

    A single unreadable/malformed resource file drops its keys; None only
    when nothing was readable (pre-4.20 kernel, PSI masked in the
    container). The directory is a parameter only for tests.
    """
    result: dict[str, float] = {}
    for file_name, line_kind, key in _PSI_ROWS:
        try:
            text = (dir_path / file_name).read_text()
        except OSError:
            continue
        # Each line: ``some avg10=1.23 avg60=0.80 avg300=0.40 total=123``.
        for line in text.splitlines():
            fields = line.split()
            if len(fields) < 2 or fields[0] != line_kind:
                continue
            try:
                key_name, _, value = fields[1].partition('=')
                if key_name == 'avg10':
                    result[key] = float(value)
            except ValueError:
                pass
            break
    return result or None


def read_cpu_throttle(
    v2_stat: Path = CGROUP_V2_CPU_STAT,
    v1_stat: Path = CGROUP_V1_CPU_STAT,
) -> tuple[int, int] | None:
    """Cumulative cgroup CPU throttling ``(nr_throttled, throttled_usec)``.

    ``nr_throttled`` counts enforcement periods in which the cgroup hit its
    quota and was descheduled; ``throttled_usec`` is the total time spent
    throttled. Both are cumulative since cgroup creation — callers diff
    them per interval. None when the process runs without a CPU controller
    (bare host, no quota) or the files are unreadable.

    cgroup v2 is tried first (``throttled_usec`` in microseconds), then v1
    (``throttled_time`` in nanoseconds, converted). The paths are
    parameters only for tests.
    """
    for path, time_key, divisor in ((v2_stat, 'throttled_usec', 1), (v1_stat, 'throttled_time', 1000)):
        try:
            text = path.read_text()
        except OSError:
            continue
        nr_throttled: int | None = None
        throttled_usec: int | None = None
        for line in text.splitlines():
            key, _, value = line.partition(' ')
            try:
                if key == 'nr_throttled':
                    nr_throttled = int(value)
                elif key == time_key:
                    throttled_usec = int(value) // divisor
            except ValueError:
                continue
        if nr_throttled is not None and throttled_usec is not None:
            return nr_throttled, throttled_usec
    return None


def read_nfs_mount_stats(mountstats: Path = PROC_MOUNTSTATS) -> dict[str, tuple[int, int, int]] | None:
    """Per-NFS-mount cumulative ``{mount: (ops, trans, rtt_ms_total)}``.

    Summed across every operation type in the mount's ``per-op statistics``
    section of ``/proc/self/mountstats``: ``ops`` completed operations,
    ``trans`` transmissions (``trans - ops`` = retransmissions — the "server
    not answering" signal), and ``rtt_ms_total`` cumulative server
    round-trip milliseconds (``rtt / ops`` = average RTT per op — the
    "server slow" signal, which moves under storage contention even while
    throughput looks normal). All cumulative; callers diff per interval.

    None when the file is unavailable or no NFS mount is visible. The path
    is a parameter only for tests.
    """
    try:
        lines = mountstats.read_text().splitlines()
    except OSError:
        return None

    per_mount: dict[str, tuple[int, int, int]] = {}
    current_mount: str | None = None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('device '):
            parts = stripped.split()
            try:
                fstype = parts[parts.index('fstype') + 1]
                mount = parts[parts.index('on') + 1]
            except (ValueError, IndexError):
                current_mount = None
                continue
            if fstype in ('nfs', 'nfs2', 'nfs3', 'nfs4'):
                current_mount = mount
                per_mount.setdefault(mount, (0, 0, 0))
            else:
                current_mount = None
            continue
        if current_mount is None:
            continue
        # Per-op lines: ``READ: <ops> <trans> <timeouts> <bytes_sent>
        # <bytes_recv> <queue_ms> <rtt_ms> <execute_ms> [errors]``. Any
        # ALL-CAPS ``NAME:`` line with enough numeric fields qualifies.
        op_name, sep, rest = stripped.partition(':')
        if not sep or not op_name.isupper():
            continue
        fields = rest.split()
        if len(fields) < 8:
            continue
        try:
            ops, trans, rtt_ms = int(fields[0]), int(fields[1]), int(fields[6])
        except ValueError:
            continue
        prev_ops, prev_trans, prev_rtt = per_mount[current_mount]
        per_mount[current_mount] = (prev_ops + ops, prev_trans + trans, prev_rtt + rtt_ms)

    return per_mount or None


def read_thread_cpu_ms(tid: int, template: str = PROC_TASK_STAT_TEMPLATE) -> float | None:
    """CPU milliseconds (user + system) consumed by one thread, or None.

    Reads ``/proc/self/task/<tid>/stat`` — readable from ANY thread, which
    is the point: a watchdog thread can measure the event-loop thread's CPU
    consumption while that thread is wedged. ``tid`` is the kernel thread
    id (``threading.get_native_id()``), not the Python ident.

    The template is a parameter only for tests.
    """
    try:
        text = Path(template.format(tid=tid)).read_text()
        # Field 2 (comm) may contain spaces and parentheses; everything
        # before the LAST ')' is comm, fields after it are space-split.
        # utime is stat field 14, stime 15 → indexes 11 and 12 after comm.
        fields = text.rpartition(')')[2].split()
        ticks = int(fields[11]) + int(fields[12])
        return ticks * 1000.0 / os.sysconf('SC_CLK_TCK')
    except (OSError, ValueError, IndexError):
        return None


def detect_network_fs(path: str, mounts_path: Path = PROC_MOUNTS) -> tuple[str, str] | None:
    """``(mount_point, fstype)`` when *path* lives on a network filesystem.

    Resolves the path, walks ``/proc/mounts``, keeps the longest mount-point
    prefix (real path-component prefix, so ``/mnt/data`` never claims
    ``/mnt/database``), and reports the mount only when its fstype is in
    :data:`NETWORK_FS_TYPES`. None otherwise — including when the mount
    table is unreadable; this feeds a warning, never a refusal.

    The mounts path is a parameter only for tests.
    """
    try:
        lines = mounts_path.read_text().splitlines()
    except OSError:
        return None

    resolved = os.path.realpath(path)
    best: tuple[str, str] | None = None
    for line in lines:
        fields = line.split()
        if len(fields) < 3:
            continue
        mount_point, fstype = fields[1], fields[2]
        if resolved != mount_point and not resolved.startswith(mount_point.rstrip('/') + '/'):
            continue
        if best is None or len(mount_point) > len(best[0]):
            best = (mount_point, fstype)

    if best is not None and best[1] in NETWORK_FS_TYPES:
        return best
    return None


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
