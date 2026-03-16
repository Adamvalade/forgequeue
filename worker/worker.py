import json
import os
import threading
import time
import traceback
import uuid

from common.redis_client import get_redis
from common.constants import (
    QUEUE_KEY,
    JOB_KEY_PREFIX,
    JOB_DONE_PREFIX,
    DELAYED_KEY,
    DLQ_KEY,
    DLQ_ZSET_KEY,
    DLQ_MAX_SIZE,
    DLQ_RETENTION_SECONDS,
    IN_FLIGHT_KEY,
    LEASE_KEY_PREFIX,
    PROCESSING_LOCK_PREFIX,
    VISIBILITY_TIMEOUT_SECONDS,
)

r = get_redis()

# Override via env for testing (e.g. VISIBILITY_TIMEOUT_SECONDS=10 so recovery runs after 10s).
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", str(VISIBILITY_TIMEOUT_SECONDS)))

HEARTBEAT_INTERVAL_SECONDS = float(
    os.getenv(
        "HEARTBEAT_INTERVAL_SECONDS",
        # Default: roughly 1/3 of visibility timeout, capped to 30s, with a 1s floor.
        str(max(1.0, min(30.0, VISIBILITY_TIMEOUT / 3))),
    )
)

WORKER_ID = os.getenv("WORKER_ID", str(uuid.uuid4()))


def perform_job(r, job_id: str, job_type: str, payload: dict):
    if job_type == "sleep":
        ms = int(payload.get("ms", 1000))
        import time
        time.sleep(ms / 1000.0)
        return

    if job_type == "always_fail":
        raise RuntimeError("always_fail: intentional failure for retry demo")

    if job_type == "fail_once":
        key = f"fail_once_seen:{job_id}"
        if r.get(key) is None:
            r.set(key, "1")
            raise RuntimeError("fail_once: intentional first-attempt failure")
        return

    raise RuntimeError(f"Unknown job type: {job_type}")


def update(job_id: str, **fields):
    fields["updated_at"] = time.time()
    # redis-py wants strings/ints/floats, not None
    clean = {k: v for k, v in fields.items() if v is not None}
    r.hset(JOB_KEY_PREFIX + job_id, mapping=clean)


def run_job(job_type: str, payload: dict, job_id: str):
    if job_type == "sleep":
        ms = int(payload.get("ms", 1000))
        time.sleep(ms / 1000.0)
        return
    if job_type == "always_fail":
        raise RuntimeError("always_fail: intentional failure")

    if job_type == "fail_once":
        key = f"fail_once_seen:{job_id}"
        if r.get(key) is None:
            r.set(key, "1")
            raise RuntimeError("fail_once: intentional first failure")
        return

    raise RuntimeError(f"Unknown job type: {job_type}")


_HEARTBEAT_LUA = """
local lease_key = KEYS[1]
local in_flight_key = KEYS[2]
local job_id = ARGV[1]
local token = ARGV[2]
local visible_at = ARGV[3]
local ttl_seconds = ARGV[4]

if redis.call('GET', lease_key) ~= token then
  return 0
end

redis.call('ZADD', in_flight_key, visible_at, job_id)
redis.call('EXPIRE', lease_key, ttl_seconds)
return 1
"""


def _start_heartbeat(job_id: str, token: str):
    """
    Heartbeat thread: extend the in-flight visible_at while the job is running.
    Only extends if our lease token still matches.
    """
    stop = threading.Event()

    def _beat():
        lease_key = LEASE_KEY_PREFIX + job_id
        while not stop.is_set():
            time.sleep(HEARTBEAT_INTERVAL_SECONDS)
            if stop.is_set():
                break
            visible_at = time.time() + VISIBILITY_TIMEOUT
            try:
                r.eval(
                    _HEARTBEAT_LUA,
                    2,
                    lease_key,
                    IN_FLIGHT_KEY,
                    job_id,
                    token,
                    str(visible_at),
                    str(VISIBILITY_TIMEOUT),
                )
            except Exception:
                # Heartbeat is best-effort; if Redis is down, the lease will expire and job may be retried.
                pass

    t = threading.Thread(target=_beat, name=f"heartbeat:{job_id}", daemon=True)
    t.start()
    return stop


def main():
    print("worker started", flush=True)
    while True:

        now = time.time()

        # Promote ready delayed jobs
        due = r.zrangebyscore(DELAYED_KEY, 0, now)
        if due:
            r.zrem(DELAYED_KEY, *due)
            for jid in due:
                update(jid, status="queued", next_run_at="")
                r.lpush(QUEUE_KEY, jid)

        # Recovery pass: move in-flight jobs whose visibility timeout has passed
        # back to the main queue so another worker can process them (worker crash recovery).
        due_in_flight = r.zrangebyscore(IN_FLIGHT_KEY, 0, now)
        for jid in due_in_flight:
            r.zrem(IN_FLIGHT_KEY, jid)
            r.delete(LEASE_KEY_PREFIX + jid)
            if not r.exists(JOB_DONE_PREFIX + jid):
                update(jid, status="queued", next_run_at="")
                r.lpush(QUEUE_KEY, jid)

        item = r.brpop(QUEUE_KEY, timeout=5)
        if item is None:
            continue
        _, job_id = item

        lease_key = LEASE_KEY_PREFIX + job_id
        lease_token = f"{WORKER_ID}:{uuid.uuid4()}"

        # Claim job with visibility timeout: use current time so the full timeout applies
        # (now from loop start is stale after recovery pass and BRPOP).
        visible_at = time.time() + VISIBILITY_TIMEOUT
        # Establish lease so only the owning worker can extend visibility.
        # If we can't set it, requeue to avoid losing the job.
        if not r.set(lease_key, lease_token, ex=VISIBILITY_TIMEOUT, nx=True):
            r.lpush(QUEUE_KEY, job_id)
            continue

        r.zadd(IN_FLIGHT_KEY, {job_id: visible_at})

        job = r.hgetall(JOB_KEY_PREFIX + job_id)
        if not job:
            r.zrem(IN_FLIGHT_KEY, job_id)
            r.delete(lease_key)
            continue

        # Idempotency check: already completed
        if r.exists(JOB_DONE_PREFIX + job_id):
            r.zrem(IN_FLIGHT_KEY, job_id)
            r.delete(lease_key)
            continue

        # Processing lock: prevents concurrent processing of the same job_id.
        proc_key = PROCESSING_LOCK_PREFIX + job_id
        proc_token = WORKER_ID
        if not r.set(proc_key, proc_token, nx=True, ex=VISIBILITY_TIMEOUT):
            # Someone else is processing it; release our claim and retry later.
            r.zrem(IN_FLIGHT_KEY, job_id)
            r.delete(lease_key)
            r.zadd(DELAYED_KEY, {job_id: time.time() + 1.0})
            continue

        attempts = int(job.get("attempts", "0")) + 1
        max_attempts = int(job.get("max_attempts", "3"))
        job_type = job.get("type", "")
        payload = json.loads(job.get("payload", "{}") or "{}")

        update(job_id, status="running", attempts=attempts, last_error="")

        try:
            hb_stop = _start_heartbeat(job_id, lease_token)
            try:
                run_job(job_type, payload, job_id)
            finally:
                hb_stop.set()
            r.zrem(IN_FLIGHT_KEY, job_id)
            r.delete(lease_key)
            r.delete(proc_key)
            r.set(JOB_DONE_PREFIX + job_id, "1")
            update(job_id, status="succeeded", last_error="")
        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"

            if attempts < max_attempts:
                backoff = min(2 ** attempts, 10)
                next_run_at = time.time() + backoff

                update(
                    job_id,
                    status="retrying",
                    last_error=err,
                    next_run_at=next_run_at,
                )

                r.zrem(IN_FLIGHT_KEY, job_id)
                r.delete(lease_key)
                r.delete(proc_key)
                r.zadd(DELAYED_KEY, {job_id: next_run_at})
            else:
                r.zrem(IN_FLIGHT_KEY, job_id)
                r.delete(lease_key)
                r.delete(proc_key)
                update(job_id, status="failed", last_error=err)
                r.lpush(DLQ_KEY, job_id)
                now2 = time.time()
                r.zadd(DLQ_ZSET_KEY, {job_id: now2})
                # Retention: TTL-by-time and max size.
                r.zremrangebyscore(DLQ_ZSET_KEY, 0, now2 - DLQ_RETENTION_SECONDS)
                # Keep only newest DLQ_MAX_SIZE items (remove oldest ranks).
                excess = r.zcard(DLQ_ZSET_KEY) - DLQ_MAX_SIZE
                if excess > 0:
                    r.zremrangebyrank(DLQ_ZSET_KEY, 0, excess - 1)


if __name__ == "__main__":
    main()

