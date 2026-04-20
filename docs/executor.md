# Executor System

Drakkar runs external binaries as subprocesses -- not through a shell, but directly via `asyncio.create_subprocess_exec`. Arguments are passed as a list, making the execution safe from shell injection by design. Concurrency is controlled by an `asyncio.Semaphore` sized to `executor.max_executors`, and each task is bounded by a wall-clock timeout via `asyncio.wait_for`.

---

## ExecutorTask

Every subprocess execution starts with an `ExecutorTask` created in your [arrange()](handler.md#arrange-required) hook. The task carries everything the framework needs to launch the process and track its lifecycle.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `task_id` | `str` | (required) | Unique identifier. Use `make_task_id(prefix)` to generate a time-sortable hex ID (e.g., `t-0194a3b2c1d4e5f6-a7c2f1e3`). |
| `args` | `list[str]` | (required) | CLI arguments appended to the binary path when launching the process. |
| `source_offsets` | `list[int]` | (required) | Kafka offsets of the source messages that produced this task. Used for [offset watermark tracking](handler.md#offset-commit-logic) -- offsets are committed only after all sinks confirm delivery. |
| `metadata` | `dict` | `{}` | Arbitrary key-value data carried through the pipeline. Accessible in [on_task_complete()](handler.md#on_task_complete) via `result.task.metadata`. |
| `labels` | `dict[str, str]` | `{}` | User-defined key-value [labels](handler.md#task-labels) shown in the [debug UI](observability.md#debug-ui) (live timeline, task detail page, trace view). Useful for `request_id`, `user_id`, or other domain identifiers. |
| `env` | `dict[str, str]` | `{}` | Per-task environment variables. Merged on top of `executor.env` from config (task overrides config on key conflict). See [Environment Variables](#environment-variables). |
| `binary_path` | `str \| None` | `None` | Per-task binary override. Takes precedence over `executor.binary_path` from config. |
| `stdin` | `str \| None` | `None` | Optional string piped to the process stdin after launch. When `None`, stdin is not connected. |

### Generating Task IDs

The `make_task_id()` helper produces short, time-sortable, unique IDs:

```python
from drakkar import make_task_id

task_id = make_task_id('search')
# "search-0194a3b2c1d4e5f6-a7c2f1e3"
```

The format is `{prefix}-{timestamp_hex}-{random_hex}`. Lexicographic order matches creation order, and the nanosecond timestamp combined with a 32-bit random suffix ensures uniqueness.

---

## Binary Path Resolution

The binary to execute is resolved per-task using this priority:

1. **`task.binary_path`** -- set on the individual `ExecutorTask`
2. **`executor.binary_path`** -- from the YAML/env config

If neither is set, the task fails immediately with `exit_code=-1` and a descriptive error message. No process is launched.

This two-level resolution lets you run different binaries for different task types within the same handler:

```python
async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        binary = '/usr/bin/fast-search' if msg.payload.priority == 'high' else '/usr/bin/deep-search'
        tasks.append(ExecutorTask(
            task_id=make_task_id('search'),
            args=['--query', msg.payload.query],
            source_offsets=[msg.offset],
            binary_path=binary,
        ))
    return tasks
```

If all tasks use the same binary, set `executor.binary_path` in your config and omit `binary_path` from individual tasks.

---

## Execution Flow

Each task goes through these steps:

### 1. Enter Semaphore Queue

The task enters the waiting state (`waiting_count` incremented). It blocks on `async with semaphore` until a slot is available.

### 2. Acquire Slot

When the semaphore is acquired, `waiting_count` is decremented, `active_count` is incremented, and a slot ID (0 to `max_executors - 1`) is assigned from the available pool.

### 3. Launch Process

The framework calls `asyncio.create_subprocess_exec` with:

- First argument: the resolved binary path
- Remaining arguments: `task.args` (passed as individual arguments, not via shell)
- `stdin`: `PIPE` if `task.stdin` is set, otherwise not connected
- `stdout`: `PIPE` (always captured)
- `stderr`: `PIPE` (always captured)

### 4. Communicate

`proc.communicate(input=stdin_bytes)` writes stdin (if any) and waits for the process to exit. All stdout and stderr are captured in memory.

### 5. Enforce Timeout

The entire `communicate()` call is wrapped in `asyncio.wait_for(timeout=executor.task_timeout_seconds)` (default: 120 seconds).

### 6. Handle Outcome

Four outcomes are possible:

**Success (exit code 0)** -- Returns an `ExecutorResult` with captured output. The [on_task_complete()](handler.md#on_task_complete) hook is called next.

**Non-zero exit** -- Raises `ExecutorTaskError`. The [on_error()](handler.md#on_error) hook decides what to do.

**Timeout** -- The process is killed (`proc.kill()`), and `ExecutorTaskError` is raised with `stderr='task timed out'`.

**OSError (binary not found)** -- `ExecutorTaskError` is raised immediately. No process was ever started.

### 7. Release Slot

In the `finally` block (guaranteed to run), the slot ID is returned to the pool, `active_count` is decremented, and the semaphore is released for the next waiting task. If the process is still running (e.g., after a timeout), it is killed before release.

---

## ExecutorResult

Returned on successful execution (exit code 0). Also attached to `ExecutorTaskError` on failures for inspection.

| Field | Type | Description |
|-------|------|-------------|
| `exit_code` | `int` | Process exit code. `0` on success, non-zero on failure, `-1` for timeout/launch errors. |
| `stdout` | `str` | Captured process stdout, decoded as UTF-8 with `errors='replace'`. |
| `stderr` | `str` | Captured process stderr, decoded as UTF-8 with `errors='replace'`. |
| `duration_seconds` | `float` | Wall-clock time from process start to completion, rounded to 3 decimal places. |
| `task` | `ExecutorTask` | The original task that produced this result. Carries `metadata` and `labels` through the pipeline. |
| `pid` | `int \| None` | OS process ID. `None` if the process never started. |

---

## Error Handling (on_error Hook)

When a task fails, the framework calls your [on_error()](handler.md#on_error) hook. The `error` argument is an `ExecutorError`:

| Field | Type | Description |
|-------|------|-------------|
| `task` | `ExecutorTask` | The task that failed. |
| `exit_code` | `int \| None` | Process exit code. `None` if the process failed to start or timed out. |
| `stderr` | `str` | Process stderr or a short error description (e.g., `'task timed out'`). |
| `exception` | `str \| None` | Exception message for timeout/launch failures. `None` for normal non-zero exits. |
| `pid` | `int \| None` | Process ID. `None` if the process never started. |

### Return Values

The hook must return one of:

**`ErrorAction.RETRY`** -- Re-execute the same task. The framework retries up to `executor.max_retries` (default: 3) times. Retries are immediate (no backoff). If retries are exhausted, the task is marked as failed and the window continues.

**`ErrorAction.SKIP`** -- Mark the task as failed, append its result to the window, and continue. This is the default behavior.

**`list[ExecutorTask]`** -- Replace the failed task with new tasks. The new tasks are added to the current window and execute immediately. The window's `total_tasks` grows dynamically to include them. Use this to split a large failing task into smaller ones, or to substitute a fallback binary.

### Example

```python
from drakkar import BaseDrakkarHandler, ErrorAction, ExecutorError, ExecutorTask

class MyHandler(BaseDrakkarHandler):
    async def on_error(self, task, error):
        # Retry transient failures (simulated by exit code 75)
        if error.exit_code == 75:
            return ErrorAction.RETRY

        # Timeout: try a lighter binary as fallback
        if error.exception and 'Timeout' in error.exception:
            return [ExecutorTask(
                task_id=make_task_id('fallback'),
                args=task.args + ['--fast-mode'],
                source_offsets=task.source_offsets,
                metadata=task.metadata,
                binary_path='/usr/bin/lightweight-search',
            )]

        # Everything else: skip
        return ErrorAction.SKIP
```

---

## Concurrency and Backpressure

### Semaphore-based Concurrency

`executor.max_executors` (default: 4) controls how many subprocesses run in parallel across all partitions. The `ExecutorPool` maintains an `asyncio.Semaphore` of this size. Tasks from any partition compete for the same pool.

When all slots are occupied, additional tasks wait in the semaphore queue. The `waiting_count` property tracks how many tasks are queued, while `active_count` tracks how many are running.

### Kafka Consumer Backpressure

The framework pauses and resumes the Kafka consumer based on total queued work across all partition processors. See [Performance Tuning](performance.md#backpressure) for tuning recommendations. Two watermarks control the behavior:

| Watermark | Formula | Default (max_executors=4) |
|-----------|---------|-------------------------|
| **High** (pause) | `max_executors` x `backpressure_high_multiplier` | 4 x 32 = **128** |
| **Low** (resume) | max(1, `max_executors` x `backpressure_low_multiplier`) | max(1, 4 x 4) = **16** |

The total queued count is the sum of `queue_size + inflight_count` across all active partition processors.

- **When total queued >= high watermark**: the consumer is paused (all assigned partitions stop receiving messages).
- **When total queued <= low watermark**: the consumer is resumed.

The gap between high and low watermarks (hysteresis) prevents rapid pause/resume oscillation. Pause and resume always operate on all assigned partitions simultaneously.

### Configuration

```yaml
executor:
  max_executors: 8
  backpressure_high_multiplier: 32   # pause at 8 * 32 = 256 queued
  backpressure_low_multiplier: 4     # resume at 8 * 4 = 32 queued
```

---

## Windowing

Messages are collected into windows before being passed to [arrange()](handler.md#arrange-required). A window contains 1 to `executor.window_size` (default: 100) messages from a single partition.

### Window Lifecycle

1. The partition processor waits for the first message (blocks up to 1 second).
2. It drains any additional messages from the queue without blocking, up to `window_size`.
3. The window of messages is passed to [arrange()](handler.md#arrange-required), which returns `ExecutorTask` objects.
4. All tasks in the window are launched concurrently.
5. The window tracks `completed_count` vs `total_tasks` to know when it is done.
6. On completion, [on_window_complete()](handler.md#on_window_complete) is called, offsets are marked complete, and a [commit is attempted](handler.md#offset-commit-logic).

### Concurrent Windows

Multiple windows can be in-flight concurrently for the same partition. The processor does not wait for window N to complete before starting window N+1. This keeps throughput high when tasks have variable execution times.

Each window's tasks execute independently. Offset commits follow [watermark semantics](handler.md#offset-commit-logic) -- offsets are committed only when all preceding messages have been fully processed and delivered. A slow task in window 1 blocks offset advancement even if window 2 has already finished.

### Dynamic Window Growth

If [on_error()](handler.md#on_error) returns replacement tasks, the window's `total_tasks` count grows. The window only completes when all tasks -- including dynamically added ones -- have finished.

---

## Stdin Support

The `stdin` field on `ExecutorTask` lets you pipe data to a subprocess without passing it as CLI arguments. When set, the framework connects a pipe to the process stdin and writes the string immediately after launch.

This is useful for passing structured input (JSON, configuration) that would be awkward or unsafe as command-line arguments.

### Example

```python
import json
from drakkar import ExecutorTask, make_task_id

async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        payload = {
            'query': msg.payload.query,
            'filters': msg.payload.filters,
            'options': {'max_results': 100, 'timeout_ms': 5000},
        }
        tasks.append(ExecutorTask(
            task_id=make_task_id('search'),
            args=['--mode', 'stdin'],
            source_offsets=[msg.offset],
            metadata={'request_id': msg.payload.request_id},
            stdin=json.dumps(payload),
        ))
    return tasks
```

The subprocess reads the JSON from stdin:

```python
#!/usr/bin/env python3
import json
import sys

request = json.load(sys.stdin)
# process request...
print(json.dumps(result))
```

When `stdin` is `None` (the default), the process stdin is not connected at all.

---

## Environment Variables

Custom environment variables can be passed to executor subprocesses at
two levels: globally via config and per-task via `ExecutorTask.env`.

### Merge order

The subprocess environment is built by merging three layers, where
later layers override earlier ones on key conflict:

1. **Parent process env** -- the worker's own environment, filtered
   through `executor.env_inherit_deny` (see below)
2. **Config env** (`executor.env`) -- applied to all tasks
3. **Task env** (`ExecutorTask.env`) -- applied to one task, overrides config

When neither config nor task defines custom env vars and no deny patterns
are configured, the subprocess inherits the parent environment directly
(no copy overhead).

### Parent-env filtering (secrets guard)

By default, the parent environment is **not** passed to subprocesses
verbatim. A deny-list of `fnmatch` patterns (`executor.env_inherit_deny`)
is applied case-insensitively to env var names, and matching vars are
stripped before the subprocess starts. The default patterns are:

```
DRAKKAR_*        # framework internals (KAFKA__BROKERS, SINKS__*__DSN, ...)
*PASSWORD*
*SECRET*
*TOKEN*
*_KEY
*_DSN
*CREDENTIAL*
```

This keeps operator-configured secrets — most commonly the `DRAKKAR_*`
env overrides like `DRAKKAR_SINKS__POSTGRES__MAIN__DSN` — from reaching
the executor binary where they could be read, logged, or exfiltrated.

Two knobs control the behavior:

```yaml
executor:
  env_inherit_parent: true         # false = fully isolated from parent env
  env_inherit_deny:                # override the default patterns
    - DRAKKAR_*
    - '*PASSWORD*'
    - '*SECRET*'
    - '*TOKEN*'
    - '*_KEY'
    - '*_DSN'
    - '*CREDENTIAL*'
```

Set `env_inherit_deny: []` to trust the full parent environment (e.g.,
if you already gate the process environment externally). Set
`env_inherit_parent: false` to run subprocesses with **only**
`executor.env` + `ExecutorTask.env` — nothing from the parent at all.
The `PATH` and other standard vars are still subject to the deny list;
if you rely on them, make sure your patterns don't match them (they
don't with the defaults).

### Config-level env

Set `executor.env` in your YAML config to pass variables to every task:

```yaml
executor:
  binary_path: "/usr/local/bin/my-tool"
  env:
    DATABASE_URL: "postgresql://localhost/mydb"
    LOG_LEVEL: "warn"
    CORPUS_PATH: "/data/corpus"
```

These are useful for connection strings, feature flags, or paths that
are the same for every task but shouldn't be hardcoded in the binary.

Environment variable override: `DRAKKAR_EXECUTOR__ENV='{"KEY": "value"}'`

### Per-task env

Set `env` on `ExecutorTask` in `arrange()` for task-specific values:

```python
async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        tasks.append(ExecutorTask(
            task_id=make_task_id('proc'),
            args=['--process'],
            source_offsets=[msg.offset],
            env={
                'REQUEST_ID': msg.payload.request_id,
                'PRIORITY': str(msg.payload.priority),
            },
        ))
    return tasks
```

Per-task env is useful when the binary reads configuration from
environment variables and different messages need different settings.

### Override example

Config sets a default, task overrides it for specific messages:

```yaml
# drakkar.yaml
executor:
  env:
    MODE: "standard"
    TIMEOUT: "30"
```

```python
# handler.py — arrange()
ExecutorTask(
    task_id=make_task_id('proc'),
    args=['--run'],
    source_offsets=[msg.offset],
    env={'MODE': 'turbo'},  # overrides config's "standard"
    # TIMEOUT: "30" is still inherited from config
)
```

The subprocess sees `MODE=turbo` and `TIMEOUT=30`.

---

## Configuration Reference

All executor settings live under the `executor` key in your YAML config:

```yaml
executor:
  binary_path: /usr/local/bin/my-worker    # default binary (optional)
  max_executors: 4                           # concurrent subprocess limit
  task_timeout_seconds: 120                # per-task wall-clock timeout
  window_size: 100                         # max messages per arrange() call
  max_retries: 3                           # retry limit per failed task
  drain_timeout_seconds: 30                # max wait for in-flight tasks on shutdown
  backpressure_high_multiplier: 32         # pause consumer at max_executors * this
  backpressure_low_multiplier: 4           # resume consumer at max_executors * this
```

| Field | Type | Default | Min | Description |
|-------|------|---------|-----|-------------|
| `binary_path` | `str \| None` | `None` | 1 char | Default subprocess binary. `None` requires per-task override via `ExecutorTask.binary_path`. |
| `max_executors` | `int` | `4` | 1 | Concurrent subprocess limit (semaphore size). |
| `task_timeout_seconds` | `int` | `120` | 1 | Per-subprocess wall-clock timeout in seconds. |
| `window_size` | `int` | `100` | 1 | Max messages per `arrange()` window. |
| `max_retries` | `int` | `3` | 0 | Max retries per failed task. 0 disables retries. |
| `drain_timeout_seconds` | `int` | `30` | 1 | Max wait for in-flight tasks during graceful shutdown. On timeout, offsets of still-in-flight tasks are **not** committed (they replay on restart). |
| `backpressure_high_multiplier` | `int` | `32` | 1 | Pause threshold = `max_executors` x this. |
| `backpressure_low_multiplier` | `int` | `4` | 1 | Resume threshold = max(1, `max_executors` x this). |

Environment variable overrides use the `DRAKKAR_EXECUTOR__` prefix:

```bash
DRAKKAR_EXECUTOR__MAX_EXECUTORS=8
DRAKKAR_EXECUTOR__TASK_TIMEOUT_SECONDS=300
```
