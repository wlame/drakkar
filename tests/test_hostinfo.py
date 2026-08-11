"""Tests for best-effort host CPU-capacity discovery (drakkar.hostinfo)."""

from pathlib import Path

import pytest

from drakkar import hostinfo
from drakkar.hostinfo import cgroup_cpu_quota, effective_cpu_count


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
