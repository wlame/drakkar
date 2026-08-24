"""Unit tests for :class:`drakkar.hostinfo.HostSampler`.

The sampler owns the previous-tick state behind every rate field in a
``resource_sample`` event. It is tested here without a recorder or a
database; every ``/proc`` reader is patched, so the tests never read the
real host they run on.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from drakkar import hostinfo
from drakkar.hostinfo import HostSampler

MIB = 1024 * 1024


@pytest.fixture
def sampler(monkeypatch):
    """A sampler whose host sources all read as "unavailable" by default."""
    monkeypatch.setattr(hostinfo, 'read_self_stats', lambda: (None, None))
    monkeypatch.setattr(hostinfo, 'read_open_fd_count', lambda: None)
    monkeypatch.setattr(hostinfo, 'read_loadavg', lambda: None)
    monkeypatch.setattr(hostinfo, 'read_pressure', lambda: None)
    monkeypatch.setattr(hostinfo, 'read_cpu_throttle', lambda: None)
    monkeypatch.setattr(hostinfo, 'read_nfs_mount_stats', lambda: None)
    return HostSampler()


class TestPlatformFacts:
    def test_reports_rss_threads_and_fds(self, sampler, monkeypatch):
        monkeypatch.setattr(hostinfo, 'read_self_stats', lambda: (256 * MIB, 20))
        monkeypatch.setattr(hostinfo, 'read_open_fd_count', lambda: 42)

        meta = sampler.sample()

        assert meta['rss_bytes'] == 256 * MIB
        assert meta['threads'] == 20
        assert meta['open_fds'] == 42
        assert 'rx_bytes_total' not in meta  # network rates are not sampled

    def test_unavailable_sources_are_omitted_not_zeroed(self, sampler):
        meta = sampler.sample()

        for key in ('rss_bytes', 'threads', 'open_fds'):
            assert key not in meta


class TestCpuPercent:
    def test_first_sample_has_no_cpu_percent(self, sampler):
        meta = sampler.sample()

        assert 'cpu_self_pct' not in meta
        assert 'cpu_children_pct' not in meta

    def test_second_sample_reports_cpu_deltas_as_percent(self, sampler, monkeypatch):
        usages = iter(
            [
                SimpleNamespace(ru_utime=1.0, ru_stime=0.0),  # self, tick 1
                SimpleNamespace(ru_utime=2.0, ru_stime=0.0),  # children, tick 1
                SimpleNamespace(ru_utime=2.0, ru_stime=0.0),  # self, tick 2 (+1s cpu)
                SimpleNamespace(ru_utime=5.0, ru_stime=1.0),  # children, tick 2 (+4s cpu)
            ]
        )
        monkeypatch.setattr(hostinfo.resource, 'getrusage', lambda _which: next(usages))
        clock = iter([100.0, 110.0])  # 10 s apart
        monkeypatch.setattr(hostinfo.time, 'monotonic', lambda: next(clock))

        sampler.sample()
        meta = sampler.sample()

        assert meta['cpu_self_pct'] == 10.0  # 1 cpu-second over 10 s
        assert meta['cpu_children_pct'] == 40.0  # 4 cpu-seconds over 10 s
        assert meta['interval_s'] == 10.0


class TestHostPressure:
    """The host-pressure keys of resource_sample (contract v1.15)."""

    def test_load_and_psi_reported_as_read(self, sampler, monkeypatch):
        monkeypatch.setattr(hostinfo, 'read_loadavg', lambda: (4.21, 3.05, 2.0))
        monkeypatch.setattr(hostinfo, 'read_pressure', lambda: {'cpu_some_avg10': 12.5, 'io_full_avg10': 3.1})

        meta = sampler.sample()

        assert meta['load1'] == 4.21
        assert meta['load5'] == 3.05
        assert meta['psi_cpu_some_avg10'] == 12.5
        assert meta['psi_io_full_avg10'] == 3.1

    def test_throttle_first_tick_primes_second_reports_delta(self, sampler, monkeypatch):
        readings = iter([(100, 500_000), (103, 750_000)])
        monkeypatch.setattr(hostinfo, 'read_cpu_throttle', lambda: next(readings))

        first = sampler.sample()
        second = sampler.sample()

        assert 'cpu_throttled_periods' not in first
        assert second['cpu_throttled_periods'] == 3
        assert second['cpu_throttled_ms'] == 250.0

    def test_throttle_zero_delta_is_reported_as_zero(self, sampler, monkeypatch):
        # Zero throttling under a quota is a real answer ("we were NOT
        # gated"), distinct from "no cgroup limit" which omits the keys.
        monkeypatch.setattr(hostinfo, 'read_cpu_throttle', lambda: (42, 987_000))

        sampler.sample()
        meta = sampler.sample()

        assert meta['cpu_throttled_periods'] == 0
        assert meta['cpu_throttled_ms'] == 0.0

    def test_throttle_counter_reset_skips_interval_and_reprimes(self, sampler, monkeypatch):
        readings = iter([(100, 500_000), (2, 1_000), (4, 3_000)])
        monkeypatch.setattr(hostinfo, 'read_cpu_throttle', lambda: next(readings))

        sampler.sample()
        reset_meta = sampler.sample()  # counters went backwards — no keys
        final_meta = sampler.sample()  # measured from the re-primed values

        assert 'cpu_throttled_periods' not in reset_meta
        assert final_meta['cpu_throttled_periods'] == 2
        assert final_meta['cpu_throttled_ms'] == 2.0

    def test_all_pressure_sources_unavailable_add_no_keys(self, sampler):
        sampler.sample()
        meta = sampler.sample()

        for key in ('load1', 'load5', 'cpu_throttled_periods', 'cpu_throttled_ms', 'nfs_mounts'):
            assert key not in meta
        assert not any(key.startswith('psi_') for key in meta)


class TestNfsMountDeltas:
    def test_nfs_mounts_report_interval_ops_rtt_retrans(self, sampler, monkeypatch):
        readings = iter(
            [
                {'/mnt/data': (1000, 1010, 40_000), '/mnt/logs': (500, 500, 1_000)},
                {'/mnt/data': (1100, 1130, 90_000), '/mnt/logs': (500, 500, 1_000)},
            ]
        )
        monkeypatch.setattr(hostinfo, 'read_nfs_mount_stats', lambda: next(readings))

        first = sampler.sample()
        second = sampler.sample()

        assert 'nfs_mounts' not in first  # nothing to diff yet
        # /mnt/data: 100 ops, (1130-1010)-(1100-1000)=20 retrans,
        # 50000 ms rtt over 100 ops = 500 ms/op. /mnt/logs was quiet — no row.
        assert second['nfs_mounts'] == [{'mount': '/mnt/data', 'ops': 100, 'rtt_ms': 500.0, 'retrans': 20}]

    def test_nfs_counter_reset_drops_mount_for_the_interval(self, sampler, monkeypatch):
        readings = iter(
            [
                {'/mnt/data': (1000, 1000, 40_000)},
                {'/mnt/data': (10, 10, 100)},  # remount — counters reset
                {'/mnt/data': (60, 62, 600)},
            ]
        )
        monkeypatch.setattr(hostinfo, 'read_nfs_mount_stats', lambda: next(readings))

        sampler.sample()
        reset_meta = sampler.sample()
        final_meta = sampler.sample()

        assert 'nfs_mounts' not in reset_meta
        assert final_meta['nfs_mounts'] == [{'mount': '/mnt/data', 'ops': 50, 'rtt_ms': 10.0, 'retrans': 2}]

    def test_retrans_without_ops_still_reports_the_mount(self, sampler, monkeypatch):
        # A server that stopped answering: retransmissions climb while no
        # operation completes — the most important interval to report.
        readings = iter(
            [
                {'/mnt/data': (1000, 1000, 40_000)},
                {'/mnt/data': (1000, 1025, 40_000)},
            ]
        )
        monkeypatch.setattr(hostinfo, 'read_nfs_mount_stats', lambda: next(readings))

        sampler.sample()
        meta = sampler.sample()

        assert meta['nfs_mounts'] == [{'mount': '/mnt/data', 'ops': 0, 'rtt_ms': 0.0, 'retrans': 25}]

    def test_mount_appearing_mid_interval_is_skipped_once(self, sampler, monkeypatch):
        readings = iter(
            [
                {'/mnt/data': (1000, 1000, 40_000)},
                {'/mnt/data': (1050, 1050, 40_500), '/mnt/new': (10, 10, 100)},
            ]
        )
        monkeypatch.setattr(hostinfo, 'read_nfs_mount_stats', lambda: next(readings))

        sampler.sample()
        meta = sampler.sample()

        assert [row['mount'] for row in meta['nfs_mounts']] == ['/mnt/data']
