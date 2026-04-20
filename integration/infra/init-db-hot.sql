-- Schema for the "hot" Postgres instance (docker service: postgres-hot).
--
-- Stores ONE AGGREGATE ROW per high-match search request. Populated by
-- the main worker's on_message_complete hook once every task derived
-- from a SearchRequest has reached a terminal state — so each row
-- represents the full fan-out outcome for that request.
--
-- The separate server (and separate pool in the worker config) keeps
-- heavy archive writes from starving the fast interactive reads.

CREATE TABLE IF NOT EXISTS hot_recent_matches (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    partition INTEGER NOT NULL,
    "offset" BIGINT NOT NULL,
    total_tasks INTEGER NOT NULL,
    succeeded_tasks INTEGER NOT NULL,
    failed_tasks INTEGER NOT NULL,
    replaced_tasks INTEGER NOT NULL,
    total_matches INTEGER NOT NULL,
    max_matches INTEGER NOT NULL,
    duration_seconds REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_hot_recent_matches_request_id
    ON hot_recent_matches(request_id);

CREATE INDEX IF NOT EXISTS idx_hot_recent_matches_created_at
    ON hot_recent_matches(created_at DESC);
