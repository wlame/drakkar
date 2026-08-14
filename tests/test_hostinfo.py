"""Tests for best-effort host-fact discovery (drakkar.hostinfo)."""

from pathlib import Path

import pytest

from drakkar import hostinfo
from drakkar.hostinfo import cgroup_cpu_quota, effective_cpu_count, read_net_io_bytes, read_nfs_io_bytes


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


# A realistic /proc/net/dev: two header lines, loopback, two real interfaces.
# rx_bytes is the first field after the colon, tx_bytes the ninth.
PROC_NET_DEV_SAMPLE = """\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 9999999    9999    0    0    0     0          0         0  9999999    9999    0    0    0     0       0          0
  eth0: 1000000    5000    0    0    0     0          0         0   400000    3000    0    0    0     0       0          0
  eth1:  500000    2000    0    0    0     0          0         0   100000    1000    0    0    0     0       0          0
"""


class TestReadNetIOBytes:
    def test_sums_non_loopback_interfaces(self, tmp_path):
        p = write(tmp_path, 'net_dev', PROC_NET_DEV_SAMPLE)
        assert read_net_io_bytes(proc_net_dev=p) == (1_500_000, 500_000)

    def test_loopback_is_excluded(self, tmp_path):
        only_lo = '\n'.join(PROC_NET_DEV_SAMPLE.splitlines()[:3]) + '\n'
        p = write(tmp_path, 'net_dev', only_lo)
        assert read_net_io_bytes(proc_net_dev=p) is None

    def test_missing_file_is_unavailable(self, tmp_path):
        assert read_net_io_bytes(proc_net_dev=missing(tmp_path)) is None

    def test_malformed_row_is_skipped_not_fatal(self, tmp_path):
        broken = PROC_NET_DEV_SAMPLE + '  bad0: not numbers here\n'
        p = write(tmp_path, 'net_dev', broken)
        assert read_net_io_bytes(proc_net_dev=p) == (1_500_000, 500_000)

    def test_short_row_is_skipped_not_fatal(self, tmp_path):
        broken = PROC_NET_DEV_SAMPLE + '  tun0: 1 2 3\n'
        p = write(tmp_path, 'net_dev', broken)
        assert read_net_io_bytes(proc_net_dev=p) == (1_500_000, 500_000)


# A realistic /proc/self/mountstats: non-NFS mounts without stats, one nfs4
# and one nfs3 mount with `bytes:` lines, and an ext4 section carrying a
# decoy `bytes:` line that must NOT be counted. The bytes: fields are
# normal_read normal_write direct_read direct_write SERVER_READ SERVER_WRITE
# read_pages write_pages — we sum only the server_* pair (wire bytes).
MOUNTSTATS_SAMPLE = """\
device rootfs mounted on / with fstype rootfs
device proc mounted on /proc with fstype proc
device /dev/vda1 mounted on /data with fstype ext4
\tbytes:\t9 9 9 9 999999 999999 9 9
device fs1.example.com:/export/data mounted on /mnt/data with fstype nfs4 statvers=1.1
\topts:\trw,vers=4.2,rsize=1048576,wsize=1048576
\tage:\t86
\tbytes:\t1048576 2048 0 0 5242880 4096 1280 1
\tRPC iostats version: 1.1  p/v: 100003/4 (nfs)
\txprt:\ttcp 0 0 2 0 0 767 766 0 897 0 2 0 0
device fs2.example.com:/export/logs mounted on /mnt/logs with fstype nfs statvers=1.1
\tbytes:\t0 0 0 0 1000000 500000 244 122
"""


class TestReadNfsIOBytes:
    def test_sums_server_bytes_across_nfs_mounts_only(self, tmp_path):
        p = write(tmp_path, 'mountstats', MOUNTSTATS_SAMPLE)
        # 5242880 + 1000000 reads; 4096 + 500000 writes. The ext4 decoy
        # bytes: line contributes nothing.
        assert read_nfs_io_bytes(mountstats=p) == (6_242_880, 504_096)

    def test_no_nfs_mounts_is_unavailable(self, tmp_path):
        no_nfs = 'device rootfs mounted on / with fstype rootfs\ndevice proc mounted on /proc with fstype proc\n'
        p = write(tmp_path, 'mountstats', no_nfs)
        assert read_nfs_io_bytes(mountstats=p) is None

    def test_missing_file_is_unavailable(self, tmp_path):
        assert read_nfs_io_bytes(mountstats=missing(tmp_path)) is None

    def test_malformed_bytes_line_is_skipped_not_fatal(self, tmp_path):
        broken = MOUNTSTATS_SAMPLE + 'device fs3:/x mounted on /mnt/x with fstype nfs4\n\tbytes:\tnot numbers\n'
        p = write(tmp_path, 'mountstats', broken)
        assert read_nfs_io_bytes(mountstats=p) == (6_242_880, 504_096)

    def test_short_bytes_line_is_skipped_not_fatal(self, tmp_path):
        broken = MOUNTSTATS_SAMPLE + 'device fs3:/x mounted on /mnt/x with fstype nfs4\n\tbytes:\t1 2 3\n'
        p = write(tmp_path, 'mountstats', broken)
        assert read_nfs_io_bytes(mountstats=p) == (6_242_880, 504_096)

    def test_nfsd_or_other_fstypes_never_match(self, tmp_path):
        # Word-exact fstype matching: 'nfsd' (the server-side pseudo fs)
        # must not be mistaken for a client mount.
        content = 'device nfsd mounted on /proc/fs/nfsd with fstype nfsd\n\tbytes:\t1 1 1 1 100 100 1 1\n'
        p = write(tmp_path, 'mountstats', content)
        assert read_nfs_io_bytes(mountstats=p) is None
