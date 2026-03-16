QUEUE_KEY = "forgequeue:queue"
JOB_KEY_PREFIX = "forgequeue:job:"
STATS_KEY = "forgequeue:stats"
JOBS_INDEX_KEY = "forgequeue:jobs:index"
DELAYED_KEY = "forgequeue:delayed"
DLQ_KEY = "forgequeue:dlq"
DLQ_ZSET_KEY = "forgequeue:dlq:z"
DLQ_RETRIED_PREFIX = "forgequeue:dlq:retried:"
DLQ_RETENTION_SECONDS = 60 * 60 * 24 * 7  # 7 days
DLQ_MAX_SIZE = 1000

# Content-based deduplication (type + normalized payload) for short window.
DEDUP_PREFIX = "forgequeue:dedup:"
DEDUP_TTL_SECONDS = 60

# Processing lock prevents concurrent processing of the same job_id.
PROCESSING_LOCK_PREFIX = "forgequeue:processing:"
JOB_DONE_PREFIX = "job:done:"

# Visibility timeout: jobs moved to in-flight ZSET with score = visible_at.
# If not acked before visible_at, recovery pass moves them back to the queue.
IN_FLIGHT_KEY = "forgequeue:in_flight"
VISIBILITY_TIMEOUT_SECONDS = 300  # 5 minutes

# Lease key used to ensure only the owning worker can extend visibility.
# Value is a random token; key expires unless periodically extended (heartbeat).
LEASE_KEY_PREFIX = "forgequeue:lease:"

# Health + observability keys
HEALTH_PING_KEY = "forgequeue:health:ping"
WORKER_HEARTBEAT_PREFIX = "forgequeue:worker:hb:"

# Metrics keys (Redis-backed so API can export a single /metrics view)
METRICS_PREFIX = "forgequeue:metrics:"
METRIC_JOBS_ENQUEUED_TOTAL = METRICS_PREFIX + "jobs_enqueued_total"
METRIC_JOBS_COMPLETED_TOTAL = METRICS_PREFIX + "jobs_completed_total"
METRIC_JOBS_FAILED_TOTAL = METRICS_PREFIX + "jobs_failed_total"
METRIC_JOBS_RETRIED_TOTAL = METRICS_PREFIX + "jobs_retried_total"
METRIC_DLQ_ADDED_TOTAL = METRICS_PREFIX + "dlq_added_total"

# Histogram keys for job processing duration (seconds).
# Stored as:
# - <...>:sum:<type> (float)
# - <...>:count:<type> (int)
# - <...>:bucket:<type>:<le> (int)   where le is "0.1", "0.5", ..., "+Inf"
METRIC_JOB_DURATION_SECONDS = METRICS_PREFIX + "job_processing_duration_seconds"

