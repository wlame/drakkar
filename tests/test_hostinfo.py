"""Tests for best-effort host-fact discovery (drakkar.hostinfo)."""

from pathlib import Path

import pytest

from drakkar import hostinfo
from drakkar.hostinfo import (
    cgroup_cpu_quota,
    detect_network_fs,
    effective_cpu_count,
    read_cpu_throttle,
    read_loadavg,
    read_nfs_mount_stats,
    read_pressure,
    read_thread_cpu_ms,
)


def write(tmp_path: Path, name: str, content: str) -> Path:
    p = tmp_path / name
    p.write_text(content)
    return p


def missing(tmp_path: Path) -> Path:
    return tmp_path / 'does-not-exist'


class TestCgroupCpuQuota:
    def test_v2_quota_and_period(self, tmp_path):
        v2 = write(tmp_path, 'cpu.max', '200000 100000\n')
        assert cgroup_cpu_quota(v2_cpu_max=v2) == 2.0

    def test_v2_unlimited_max(self, tmp_path):
        v2 = write(tmp_path, 'cpu.max', 'max 100000\n')
        assert cgroup_cpu_quota(v2_cpu_max=v2) is None

    def test_v2_malformed_is_no_quota(self, tmp_path):
        v2 = write(tmp_path, 'cpu.max', 'garbage\n')
        assert cgroup_cpu_quota(v2_cpu_max=v2) is None

    def test_v1_quota_over_period(self, tmp_path):
        quota = write(tmp_path, 'cpu.cfs_quota_us', '150000\n')
        period = write(tmp_path, 'cpu.cfs_period_us', '100000\n')
        assert cgroup_cpu_quota(v2_cpu_max=missing(tmp_path), v1_quota=quota, v1_period=period) == 1.5

    def test_v1_unlimited_negative_quota(self, tmp_path):
        quota = write(tmp_path, 'cpu.cfs_quota_us', '-1\n')
        period = write(tmp_path, 'cpu.cfs_period_us', '100000\n')
        assert cgroup_cpu_quota(v2_cpu_max=missing(tmp_path), v1_quota=quota, v1_period=period) is None

    def test_no_cgroup_files_is_no_quota(self, tmp_path):
        assert (
            cgroup_cpu_quota(v2_cpu_max=missing(tmp_path), v1_quota=missing(tmp_path), v1_period=missing(tmp_path))
            is None
        )


class TestEffectiveCpuCount:
    @pytest.fixture
    def eight_core_affinity(self, monkeypatch):
        monkeypatch.setattr(hostinfo, '_affinity_count', lambda: 8)

    def test_no_quota_uses_affinity(self, eight_core_affinity, monkeypatch):
        monkeypatch.setattr(hostinfo, 'cgroup_cpu_quota', lambda: None)
        assert effective_cpu_count() == 8

    def test_quota_caps_affinity(self, eight_core_affinity, monkeypatch):
        monkeypatch.setattr(hostinfo, 'cgroup_cpu_quota', lambda: 2.5)
        assert effective_cpu_count() == 2

    def test_quota_larger_than_affinity_changes_nothing(self, eight_core_affinity, monkeypatch):
        monkeypatch.setattr(hostinfo, 'cgroup_cpu_quota', lambda: 64.0)
        assert effective_cpu_count() == 8

    def test_fractional_quota_never_reports_zero(self, eight_core_affinity, monkeypatch):
        monkeypatch.setattr(hostinfo, 'cgroup_cpu_quota', lambda: 0.5)
        assert effective_cpu_count() == 1

    def test_real_environment_reports_at_least_one(self):
        assert effective_cpu_count() >= 1


class TestReadLoadavg:
    def test_reads_three_load_figures(self, tmp_path):
        p = write(tmp_path, 'loadavg', '0.42 1.10 2.03 2/1234 56789\n')
        assert read_loadavg(path=p) == (0.42, 1.10, 2.03)

    def test_missing_file_is_unavailable(self, tmp_path):
        assert read_loadavg(path=missing(tmp_path)) is None

    def test_malformed_is_unavailable(self, tmp_path):
        p = write(tmp_path, 'loadavg', 'garbage\n')
        assert read_loadavg(path=p) is None


class TestReadPressure:
    @pytest.fixture
    def pressure_dir(self, tmp_path):
        d = tmp_path / 'pressure'
        d.mkdir()
        (d / 'cpu').write_text('some avg10=1.23 avg60=0.80 avg300=0.40 total=12345678\n')
        (d / 'io').write_text(
            'some avg10=5.50 avg60=3.00 avg300=1.00 total=222\nfull avg10=2.75 avg60=1.50 avg300=0.50 total=111\n'
        )
        (d / 'memory').write_text(
            'some avg10=0.10 avg60=0.05 avg300=0.01 total=33\nfull avg10=0.02 avg60=0.01 avg300=0.00 total=22\n'
        )
        return d

    def test_reads_all_five_keys(self, pressure_dir):
        assert read_pressure(dir_path=pressure_dir) == {
            'cpu_some_avg10': 1.23,
            'io_some_avg10': 5.50,
            'io_full_avg10': 2.75,
            'mem_some_avg10': 0.10,
            'mem_full_avg10': 0.02,
        }

    def test_missing_resource_file_drops_its_keys(self, pressure_dir):
        (pressure_dir / 'memory').unlink()
        result = read_pressure(dir_path=pressure_dir)
        assert result is not None
        assert 'mem_some_avg10' not in result
        assert result['cpu_some_avg10'] == 1.23

    def test_missing_directory_is_unavailable(self, tmp_path):
        assert read_pressure(dir_path=missing(tmp_path)) is None

    def test_cpu_full_line_is_ignored(self, pressure_dir):
        # Newer kernels add a mostly-meaningless `full` line to cpu too.
        (pressure_dir / 'cpu').write_text(
            'some avg10=1.23 avg60=0.80 avg300=0.40 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'
        )
        result = read_pressure(dir_path=pressure_dir)
        assert result is not None
        assert 'cpu_full_avg10' not in result
        assert result['cpu_some_avg10'] == 1.23

    def test_malformed_file_drops_its_keys(self, pressure_dir):
        (pressure_dir / 'io').write_text('nonsense\n')
        result = read_pressure(dir_path=pressure_dir)
        assert result is not None
        assert 'io_some_avg10' not in result
        assert result['cpu_some_avg10'] == 1.23


CGROUP_V2_CPU_STAT = """\
usage_usec 1234567
user_usec 1000000
system_usec 234567
nr_periods 5000
nr_throttled 42
throttled_usec 987654
"""

CGROUP_V1_CPU_STAT = """\
nr_periods 5000
nr_throttled 42
throttled_time 987654000
"""


class TestReadCpuThrottle:
    def test_v2_reports_periods_and_usec(self, tmp_path):
        v2 = write(tmp_path, 'cpu.stat', CGROUP_V2_CPU_STAT)
        assert read_cpu_throttle(v2_stat=v2, v1_stat=missing(tmp_path)) == (42, 987654)

    def test_v1_converts_ns_to_usec(self, tmp_path):
        v1 = write(tmp_path, 'cpu.stat.v1', CGROUP_V1_CPU_STAT)
        assert read_cpu_throttle(v2_stat=missing(tmp_path), v1_stat=v1) == (42, 987654)

    def test_v2_wins_over_v1(self, tmp_path):
        v2 = write(tmp_path, 'cpu.stat', CGROUP_V2_CPU_STAT)
        v1 = write(tmp_path, 'cpu.stat.v1', 'nr_throttled 1\nthrottled_time 1000\n')
        assert read_cpu_throttle(v2_stat=v2, v1_stat=v1) == (42, 987654)

    def test_v2_without_throttle_lines_falls_through_to_unavailable(self, tmp_path):
        # A cgroup v2 file exists on every unified-hierarchy host, but a
        # cgroup with no CPU controller carries no nr_throttled line.
        v2 = write(tmp_path, 'cpu.stat', 'usage_usec 1\n')
        assert read_cpu_throttle(v2_stat=v2, v1_stat=missing(tmp_path)) is None

    def test_no_files_is_unavailable(self, tmp_path):
        assert read_cpu_throttle(v2_stat=missing(tmp_path), v1_stat=missing(tmp_path)) is None


# A realistic /proc/self/mountstats with per-op sections. Per-op fields after
# the op name: ops trans timeouts bytes_sent bytes_recv queue_ms rtt_ms
# execute_ms [errors]. RTT totals are cumulative milliseconds.
MOUNTSTATS_PEROP_SAMPLE = """\
device /dev/vda1 mounted on /data with fstype ext4
\tbytes:\t9 9 9 9 999999 999999 9 9
device fs1.example.com:/export/data mounted on /mnt/data with fstype nfs4 statvers=1.1
\topts:\trw,vers=4.2
\tbytes:\t1048576 2048 0 0 5242880 4096 1280 1
\tper-op statistics
\t        NULL: 1 1 0 44 24 0 0 0 0
\t        READ: 100 102 0 76602 3242000 302 3670 4084 0
\t       WRITE: 50 50 0 512000 8000 100 1330 1500 0
device fs2.example.com:/export/logs mounted on /mnt/logs with fstype nfs statvers=1.1
\tbytes:\t0 0 0 0 1000000 500000 244 122
\tper-op statistics
\t     GETATTR: 200 200 0 20000 22400 5 400 450 0
"""


class TestReadNfsMountStats:
    def test_sums_ops_trans_rtt_per_mount(self, tmp_path):
        p = write(tmp_path, 'mountstats', MOUNTSTATS_PEROP_SAMPLE)
        assert read_nfs_mount_stats(mountstats=p) == {
            '/mnt/data': (151, 153, 5000),
            '/mnt/logs': (200, 200, 400),
        }

    def test_no_nfs_mounts_is_unavailable(self, tmp_path):
        p = write(tmp_path, 'mountstats', 'device proc mounted on /proc with fstype proc\n')
        assert read_nfs_mount_stats(mountstats=p) is None

    def test_missing_file_is_unavailable(self, tmp_path):
        assert read_nfs_mount_stats(mountstats=missing(tmp_path)) is None

    def test_malformed_op_line_is_skipped_not_fatal(self, tmp_path):
        broken = MOUNTSTATS_PEROP_SAMPLE + '\t        BADOP: not numbers\n'
        p = write(tmp_path, 'mountstats', broken)
        result = read_nfs_mount_stats(mountstats=p)
        assert result is not None
        assert result['/mnt/logs'] == (200, 200, 400)

    def test_nfs_mount_without_perop_section_reports_zeros(self, tmp_path):
        content = 'device fs:/x mounted on /mnt/x with fstype nfs4 statvers=1.1\n\tbytes:\t0 0 0 0 1 1 0 0\n'
        p = write(tmp_path, 'mountstats', content)
        assert read_nfs_mount_stats(mountstats=p) == {'/mnt/x': (0, 0, 0)}


class TestReadThreadCpuMs:
    def test_sums_utime_and_stime_ticks(self, tmp_path, monkeypatch):
        # comm contains spaces and parens — parsing must anchor on the LAST ')'.
        stat = '1234 (weird) name)) R 1 1234 1234 0 -1 4194304 100 0 0 0 250 150 0 0 20 0 4 0 12345'
        p = write(tmp_path, 'stat', stat + ' 0 0 0\n')
        monkeypatch.setattr(hostinfo.os, 'sysconf', lambda name: 100)
        # (250 + 150) ticks at 100 Hz = 4000 ms.
        assert read_thread_cpu_ms(1234, template=str(p)) == 4000.0

    def test_missing_task_is_unavailable(self, tmp_path):
        assert read_thread_cpu_ms(99999, template=str(tmp_path / 'task-{tid}-stat')) is None

    def test_malformed_is_unavailable(self, tmp_path):
        p = write(tmp_path, 'stat', 'no parens here\n')
        assert read_thread_cpu_ms(1, template=str(p)) is None


PROC_MOUNTS_SAMPLE = """\
sysfs /sys sysfs rw,nosuid 0 0
/dev/vda1 / ext4 rw,relatime 0 0
/dev/vdb1 /var/lib ext4 rw,relatime 0 0
fs1.example.com:/export /mnt/data nfs4 rw,relatime,vers=4.2 0 0
fs2.example.com:/export/deep /mnt/data/deep cifs rw 0 0
"""


class TestDetectNetworkFS:
    @pytest.fixture
    def mounts(self, tmp_path):
        return write(tmp_path, 'mounts', PROC_MOUNTS_SAMPLE)

    def test_path_on_nfs_mount_is_detected(self, mounts):
        assert detect_network_fs('/mnt/data/incoming/x.db', mounts_path=mounts) == ('/mnt/data', 'nfs4')

    def test_longest_prefix_wins(self, mounts):
        assert detect_network_fs('/mnt/data/deep/y', mounts_path=mounts) == ('/mnt/data/deep', 'cifs')

    def test_local_fs_is_not_flagged(self, mounts):
        assert detect_network_fs('/var/lib/drakkar', mounts_path=mounts) is None

    def test_mount_point_itself_matches(self, mounts):
        assert detect_network_fs('/mnt/data', mounts_path=mounts) == ('/mnt/data', 'nfs4')

    def test_sibling_prefix_does_not_false_positive(self, mounts):
        # /mnt/database shares the string prefix but not the path prefix.
        assert detect_network_fs('/mnt/database', mounts_path=mounts) is None

    def test_missing_mounts_file_is_unavailable(self, tmp_path):
        assert detect_network_fs('/mnt/data', mounts_path=missing(tmp_path)) is None
