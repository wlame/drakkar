# Review Findings Fix Plan

## Overview

Address security, architecture, and quality findings from the consolidated code review. Scoped to actionable items — excludes findings marked as accepted risk (SQLite env vars exposure, debug server coupling, cross-worker discovery trust).

## Context

- **Coverage**: 93.71% (756 tests passing)
- **Lint/format**: clean
- **Type checker**: 1 diagnostic in logging.py

Key findings to fix:
1. Debug server security (auth for sensitive endpoints, configurable host)
2. Filesystem sink path containment
3. Log file handle leak
4. DLQ failure counter
5. Credential redaction in sink logs
6. Executor slot management performance
7. Deprecated asyncio API
8. Type checker fix
9. Test gaps (poll loop, concurrent windows)
10. Test helper deduplication

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Run full test suite after each task
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Debug server — configurable host + token auth for sensitive endpoints

**Files:**
- Modify: `drakkar/config.py`
- Modify: `drakkar/debug_server.py`
- Modify: `tests/test_debug_server.py`

- [ ] Add `host: str = '127.0.0.1'` and `auth_token: str = ''` to `DebugConfig`
- [ ] Use `config.host` in `DebugServer.start()` uvicorn config instead of hardcoded `0.0.0.0`
- [ ] Add FastAPI middleware/dependency that checks `Authorization: Bearer {token}` on protected routes: `/debug/download/`, `/api/debug/merge`, `/api/debug/databases`
- [ ] When `auth_token` is empty, no auth required (backward compatible)
- [ ] Write tests: token required returns 401 without header, 200 with correct header
- [ ] Write tests: token empty = no auth on protected routes
- [ ] Write tests: non-protected routes (dashboard, live, partitions) always accessible
- [ ] Run tests — must pass before task 2

### Task 2: Filesystem sink — enforce base_path containment

**Files:**
- Modify: `drakkar/sinks/filesystem.py`
- Modify: `drakkar/config.py`
- Modify: `tests/test_sinks.py`

- [ ] Make `base_path` required (non-empty) in `FileSinkConfig` by adding `min_length=1`
- [ ] In `FileSink.deliver()`, resolve `payload.path` relative to `base_path`, then canonicalize with `os.path.realpath` and verify it starts with `realpath(base_path)`
- [ ] Raise `ValueError` on path traversal attempt
- [ ] Write tests: normal relative path works
- [ ] Write tests: `../` traversal raises ValueError
- [ ] Write tests: absolute path outside base_path raises ValueError
- [ ] Update existing filesystem sink tests for required base_path
- [ ] Run tests — must pass before task 3

### Task 3: Debug server file download — add realpath canonicalization

**Files:**
- Modify: `drakkar/debug_server.py`
- Modify: `tests/test_debug_server.py`

- [ ] In download endpoint, after constructing `full` path, verify `os.path.realpath(full)` starts with `os.path.realpath(config.db_dir) + os.sep`
- [ ] Same check in merge endpoint for input filenames
- [ ] Write tests: normal filename works
- [ ] Write tests: traversal attempt returns 400
- [ ] Run tests — must pass before task 4

### Task 4: Log file handle leak fix + type checker fix

**Files:**
- Modify: `drakkar/logging.py`
- Modify: `drakkar/app.py`
- Modify: `tests/test_logging.py`

- [ ] Change `_resolve_output` return type to `TextIO` (fixes ty diagnostic)
- [ ] Store opened file handle in a module-level variable so it can be closed
- [ ] Add `close_logging()` function that closes the file handle if one was opened
- [ ] Call `close_logging()` in `DrakkarApp._shutdown()`
- [ ] Write test: `close_logging()` closes file handle
- [ ] Write test: `close_logging()` is no-op for stderr/stdout
- [ ] Run tests — must pass before task 5

### Task 5: DLQ failure counter

**Files:**
- Modify: `drakkar/metrics.py`
- Modify: `drakkar/sinks/dlq.py`
- Modify: `tests/test_sinks.py`

- [ ] Add `drakkar_dlq_send_failures_total` Counter to metrics.py
- [ ] In `DLQSink.send()` except block, increment the counter
- [ ] Write test: counter increments when DLQ send fails
- [ ] Run tests — must pass before task 6

### Task 6: Credential redaction in sink logs

**Files:**
- Create: `drakkar/utils.py`
- Modify: `drakkar/sinks/mongo.py`
- Modify: `drakkar/sinks/redis.py`
- Modify: `drakkar/sinks/kafka.py`
- Modify: `drakkar/sinks/dlq.py`
- Create: `tests/test_utils.py`

- [ ] Create `drakkar/utils.py` with `redact_url(url: str) -> str` that replaces `user:pass@` with `***:***@` in URIs
- [ ] Apply `redact_url` to MongoDB URI, Redis URL, Kafka brokers, and DLQ brokers in `connect()` log calls
- [ ] Write tests for `redact_url`: with credentials, without credentials, empty string, non-URI string
- [ ] Run tests — must pass before task 7

### Task 7: Executor slot management — heapq

**Files:**
- Modify: `drakkar/executor.py`

- [ ] Replace `_available_slots` list + `sort()` with `heapq.heappush/heappop`
- [ ] Run existing executor tests — must pass (no new tests needed, behavior unchanged)

### Task 8: Fix deprecated asyncio.get_event_loop()

**Files:**
- Modify: `drakkar/recorder.py`

- [ ] Replace `asyncio.get_event_loop()` with `asyncio.get_running_loop()` in `record_task_started`
- [ ] Run existing recorder tests — must pass

### Task 9: Test — poll loop coverage

**Files:**
- Modify: `tests/test_app.py`

- [ ] Add test that runs `_poll_loop` for a few iterations with mock consumer returning messages, verify messages dispatched to processors
- [ ] Add test that runs `_poll_loop` with queue depth above high watermark, verify consumer.pause called
- [ ] Add test that runs `_poll_loop` with empty poll + empty queues, verify consumer_idle metric increments
- [ ] Add test that runs `_poll_loop` with messages in queue + idle executor slots, verify executor_idle_waste metric increments
- [ ] Run tests — must pass before task 10

### Task 10: Test — concurrent window processing

**Files:**
- Modify: `tests/test_partition.py`

- [ ] Add test with `window_size=1` and 3 messages where task 1 is slow (sleep), tasks 2,3 complete first
- [ ] Verify offsets committed only after all three complete (watermark correctness)
- [ ] Verify `PendingContext` in second arrange() call includes task from first window
- [ ] Run tests — must pass before task 11

### Task 11: Test helper consolidation

**Files:**
- Modify: `tests/conftest.py`
- Modify: `tests/test_metrics.py`
- Modify: `tests/test_partition.py`
- Modify: `tests/test_recorder.py`
- Modify: `tests/test_config_modes.py`
- Modify: `tests/test_app.py`

- [ ] Move `make_msg` to conftest.py (used in 4+ files)
- [ ] Move `_setup_app_sinks` to conftest.py (duplicated in test_app.py and test_config_modes.py)
- [ ] Update all importing files
- [ ] Run full test suite — must pass before task 12

### Task 12: Update docs

**Files:**
- Modify: `docs/configuration.md`
- Modify: `docs/observability.md`
- Modify: `docs/sinks.md`

- [ ] Document `debug.host` and `debug.auth_token` in configuration.md
- [ ] Document `base_path` is now required for filesystem sink in sinks.md
- [ ] Document `drakkar_dlq_send_failures_total` metric in observability.md
- [ ] Verify mkdocs build passes

### Task 13: Final verification

- [ ] Run full test suite: `uv run pytest tests/ --cov=drakkar`
- [ ] Run linter: `uvx ruff check drakkar/ tests/`
- [ ] Run type checker: `uv run ty check drakkar/`
- [ ] Verify coverage did not decrease from 93.71%
- [ ] Verify mkdocs build: `mkdocs build --strict`
- [ ] Move plan to `docs/plans/completed/`

## Post-Completion

**Manual verification:**
- Test debug UI auth with browser (protected routes require token in URL param or header)
- Test filesystem sink path traversal rejection in integration environment
- Verify credential redaction in production logs
