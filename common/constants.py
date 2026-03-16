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

