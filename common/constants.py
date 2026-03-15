QUEUE_KEY = "forgequeue:queue"
JOB_KEY_PREFIX = "forgequeue:job:"
STATS_KEY = "forgequeue:stats"
JOBS_INDEX_KEY = "forgequeue:jobs:index"
DELAYED_KEY = "forgequeue:delayed"
DLQ_KEY = "forgequeue:dlq"
JOB_DONE_PREFIX = "job:done:"

# Visibility timeout: jobs moved to in-flight ZSET with score = visible_at.
# If not acked before visible_at, recovery pass moves them back to the queue.
IN_FLIGHT_KEY = "forgequeue:in_flight"
VISIBILITY_TIMEOUT_SECONDS = 300  # 5 minutes

