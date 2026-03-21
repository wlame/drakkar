"""Custom application-level Prometheus metrics.

These are user-defined metrics specific to this pipeline, not framework
metrics. Define them at module level so they're registered once on import.
Use them in handler hooks like collect() and on_error().
"""

from prometheus_client import Counter, Histogram

search_match_count = Histogram(
    'app_search_match_count',
    'Number of matches per search request',
    buckets=(0, 1, 5, 10, 50, 100, 500),
)

search_errors_total = Counter(
    'app_search_errors_total',
    'Total search executor failures',
    ['error_type'],
)

delivery_retries_total = Counter(
    'app_delivery_retries_total',
    'Total sink delivery retries',
    ['sink_type'],
)
