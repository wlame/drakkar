# Host Pressure

When every task slows down at once — regardless of task type, input size,
or code path — the cause is almost never the handler. It is a shared
resource: the host's CPUs, its cgroup quota, memory reclaim, or the
storage behind a network mount. Host pressure sampling answers the one
question a per-task view cannot: **which resource is this worker fighting
for?**

The samples ride the existing `resource_sample` flight-recorder event
(one per `ui.recorder.state_sync_interval_seconds` tick, default 10 s),
so they are persisted, rotated, archived, and replayable like every other
event: an archive alone can show what the host looked like during an
incident long after the process is gone. The Runtime tab renders the
current values live over the WebSocket stream, which keeps working even
while the recorder's database writes are degraded.

Everything on this page is best-effort and Linux-only: a key is omitted
when its source is unavailable (no PSI on the kernel, no cgroup limit, no
NFS mounts), and clients hide readouts for absent keys. Nothing here can
fail worker startup.

## What is sampled

| Keys | Source | What it tells you |
|------|--------|-------------------|
| `load1`, `load5` | `/proc/loadavg` | Host-wide runnable + uninterruptible tasks. The classic "is the whole box oversubscribed" number — it counts every process on the host, not just this worker. |
| `psi_cpu_some_avg10` | `/proc/pressure/cpu` | Percent of the last 10 s in which at least one task sat runnable but unscheduled. The kernel's own CPU-contention verdict. |
| `psi_io_some_avg10`, `psi_io_full_avg10` | `/proc/pressure/io` | Time stalled on I/O. `full` means *all* non-idle tasks were stalled at once — the whole workload waiting on storage. |
| `psi_mem_some_avg10`, `psi_mem_full_avg10` | `/proc/pressure/memory` | Time stalled on memory reclaim/refault — the signature of memory thrash without an OOM kill. |
| `cpu_throttled_periods`, `cpu_throttled_ms` | cgroup `cpu.stat` (v2, v1 fallback) | How often and how long the cgroup CPU quota descheduled this container **during the interval**. Zero under a quota is a real answer ("not gated"); no quota omits the keys. |
| `nfs_mounts[]` — `{mount, ops, rtt_ms, retrans}` | `/proc/self/mountstats` | Per-NFS-mount health for the interval: operations completed, **average server round-trip per operation**, and retransmissions. |

PSI values are the kernel's precomputed 10-second averages, so the
default 10-second sampling cadence reads them loss-free.

## Reading it during an incident

The pattern that motivated this page: tasks that normally track input
size suddenly all take 5–15 s, the event loop lags, the queue balloons —
then everything recovers, then it happens again. The samples separate the
usual suspects:

- **NFS server or path contention** — `nfs_mounts[].rtt_ms` multiplies
  (tens of ms → seconds per op) while `ops` stays flat or drops.
  Throughput (the header's `NFS R/W` rates) can look normal; RTT is the
  discriminating signal. Rising `retrans` means the server is not
  answering at all. Cross-check who else uses the same export.
- **cgroup CPU throttling** — `cpu_throttled_ms` climbs toward the
  interval length. The container is CPU-capped: every subprocess and the
  event loop itself get descheduled. Raise the quota or shrink the pool.
- **Host CPU oversubscription** — `psi_cpu_some_avg10` high and `load1`
  well above the core count, with throttling keys absent or zero:
  someone else on the host is eating the CPUs.
- **Storage/IO pressure** — `psi_io_full_avg10` high: the workload as a
  whole is waiting on storage. Combined with quiet `nfs_mounts`, suspect
  local disks.
- **Memory thrash** — `psi_mem_*` high while RSS looks stable: the host
  is reclaiming and refaulting; check neighbours.

The [Runtime Health](runtime-health.md#lag-episodes-and-verdicts) monitor
consumes the same signals to print a per-episode verdict (`blocked` /
`cpu_bound` / `starved`), and its `runtime_lag_episode` events embed the
throttle and PSI evidence they saw.

## Semantics and caveats

- **NFS counters are per mount (per superblock)**: any other process on
  the host using the same mount contributes to them. On a worker whose
  NFS volume exists for its own inputs, they are effectively the app's
  traffic. `rtt_ms` averages over all operation types, weighted by count.
- **Load average and PSI are host-wide** (or cgroup-wide where the kernel
  scopes them); they deliberately look beyond this process — that is the
  point.
- **Deltas re-prime on counter resets**: a remount or cgroup replacement
  drops one interval instead of reporting a nonsense negative rate.
- Containers sometimes mask `/proc/pressure`; the PSI keys then simply
  never appear.

## Recorder databases on network filesystems

At startup the recorder resolves `ui.recorder.db_dir` against
`/proc/mounts`; when it lands on a network filesystem (`nfs*`, `cifs`,
`sshfs`, `9p`, `glusterfs`, `ceph`, `lustre`, …) it logs
`recorder_db_dir_network_fs` and continues. SQLite on such mounts risks
lock corruption, and — worse for an incident — every recorder flush then
shares fate with the network path: the observability stack degrades
exactly when it is needed. Point `db_dir` at a local disk.

## Replay after the fact

`resource_sample` rows are ordinary events: filter them on the History
page, query `GET /api/v1/events?event_types=resource_sample`, or read
them straight out of a rotated/archived database next to the tasks that
were running at the time. See [Local Databases](local-databases.md).

## See also

- [Runtime Health](runtime-health.md) — the event-loop side of the same
  incidents: lag, stalls, episodes, verdicts.
- [Observability](observability.md) — the full event-type table.
