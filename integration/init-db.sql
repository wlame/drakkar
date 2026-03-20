CREATE TABLE IF NOT EXISTS search_results (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(64) NOT NULL,
    pattern VARCHAR(256) NOT NULL,
    match_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_search_results_request_id ON search_results(request_id);
