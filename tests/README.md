# ForgeQueue tests

## Running tests

With Docker (recommended; requires API and worker running):

```bash
docker compose up -d
docker compose run --rm tests pytest tests/ -v
```

## Reliability test matrix

These tests prove the queue holds up under failure. Each scenario has explicit acceptance criteria.

| Scenario | Test file | Acceptance criteria |
|----------|------------|----------------------|
| **Worker dies after claiming but before ack** | `test_reliability_crash_recovery.py` | No lost jobs; job is re-queued after visibility timeout and eventually completes or goes to DLQ. |
| **Worker dies during heartbeat window** | `test_reliability_crash_recovery.py` | Same as above; job is reclaimed and completed. |
| **Long-running job extends visibility** | `test_reliability_visibility.py` | No double-run; job runs exactly once (heartbeat extends visibility). |
| **Expired in-flight job gets reclaimed** | `test_reliability_crash_recovery.py` | Recovery pass re-queues; job eventually completes or DLQ. |
| **Retry goes to DLQ after max attempts** | `test_reliability_dlq.py` | Exhausted jobs always land in DLQ; attempts == max_attempts. |
| **DLQ retry requeues correctly** | `test_reliability_dlq.py` | Retry removes from DLQ and job can complete (or fail again). |
| **Duplicate enqueue same logical payload** | `test_reliability_dedup_and_contention.py` | Returns same job or skips enqueue; only one logical execution. |
| **Two workers racing for same reclaimable job** | `test_reliability_dedup_and_contention.py` | Only one actually runs it (JOB_DONE / processing lock prevents double execution). |

### Global acceptance criteria (asserted across tests)

- **No lost jobs** – Every claimed job eventually completes or lands in DLQ (recovery re-queues unacked).
- **No concurrent execution of same logical job** – Dedup, processing lock, and JOB_DONE ensure at-most-once execution per logical job.
- **Exhausted jobs always land in DLQ** – After `max_attempts` failures, job is in DLQ list and ZSET.
- **Reclaimed jobs eventually complete or DLQ** – Recovery re-queues; worker picks up and runs to completion or exhausts retries.
- **Metrics reflect the real state** – queue_depth, in_flight_count, and counters (enqueued/completed/failed/dlq_added) are exposed and consistent.
