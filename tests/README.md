# Tests

```bash
docker compose up -d
docker compose run --rm tests pytest tests/ -v
```

## Reliability matrix

| Scenario | File | Checks |
|----------|------|--------|
| Worker dies after claim, before ack | `test_reliability_crash_recovery.py` | Job re-queued after visibility timeout; completes or DLQ |
| Worker dies during heartbeat window | `test_reliability_crash_recovery.py` | Same |
| Long job + heartbeat | `test_reliability_visibility.py` | Single execution |
| Expired in-flight reclaimed | `test_reliability_crash_recovery.py` | Recovery re-queues |
| Exhausted retries → DLQ | `test_reliability_dlq.py` | `attempts == max_attempts`, in DLQ |
| DLQ retry | `test_reliability_dlq.py` | Removed from DLQ; can run again |
| Dedup same payload | `test_reliability_dedup_and_contention.py` | One logical run |
| Two workers race reclaim | `test_reliability_dedup_and_contention.py` | At-most-one execution |

**Invariants:** no silent job loss; no concurrent execution of the same logical job; terminal failures in DLQ; metrics line up with queue/DLQ depth where applicable.
