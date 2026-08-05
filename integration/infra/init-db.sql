CREATE TABLE IF NOT EXISTS search_results (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    pattern VARCHAR(256) NOT NULL,
    match_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_search_results_request_id ON search_results(request_id);

-- ---------------------------------------------------------------------
-- Tables for the WRITE-OPERATION demo in on_message_complete.
--
-- search_results above is written with a plain INSERT (one row per task).
-- These two show why the other operations exist.
-- ---------------------------------------------------------------------

-- One row per REQUEST, written with an UPSERT keyed on request_id.
--
-- Delivery is at-least-once, so a redelivered request would duplicate this
-- row under a plain INSERT. The upsert converges instead: the same request
-- always leaves exactly one row, however often it is delivered.
--
-- `notified` is deliberately NOT in the upsert's update_columns, so a
-- redelivery cannot un-send a webhook that already went out. The UPDATE that
-- sets it runs afterwards, in payload order.
CREATE TABLE IF NOT EXISTS request_summaries (
    request_id VARCHAR(64) PRIMARY KEY,
    total_matches INTEGER NOT NULL DEFAULT 0,
    succeeded_tasks INTEGER NOT NULL DEFAULT 0,
    failed_tasks INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0,
    notified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Running totals per search pattern, accumulated by the named statement
-- `bump_pattern_stats` (see sinks.postgres.archive_results_db.statements).
--
-- The declarative operations cannot express this: the new value depends on
-- the old one (`total_matches + :matches`), which is exactly what the
-- operator-authored escape hatch is for.
CREATE TABLE IF NOT EXISTS pattern_stats (
    pattern VARCHAR(256) PRIMARY KEY,
    total_matches BIGINT NOT NULL DEFAULT 0,
    requests BIGINT NOT NULL DEFAULT 0,
    last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
