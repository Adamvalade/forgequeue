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
    WORKER_HEARTBEAT_PREFIX,
    METRIC_DLQ_ADDED_TOTAL,
    METRIC_JOBS_COMPLETED_TOTAL,
    METRIC_JOBS_FAILED_TOTAL,
    METRIC_JOBS_RETRIED_TOTAL,
    METRIC_JOB_DURATION_SECONDS,
)
from common.metrics import DEFAULT_DURATION_BUCKETS
from common.observability import configure_logging, log_event

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
logger = configure_logging("forgequeue-worker")

WORKER_HEARTBEAT_TTL_SECONDS = int(os.getenv("WORKER_HEARTBEAT_TTL_SECONDS", "60"))
WORKER_HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "15"))


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


def run_job(
    job_type: str,
    payload: dict,
    job_id: str,
    attempt_no: int = 1,
    visibility_seconds: int = VISIBILITY_TIMEOUT,
):
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

    if job_type == "crash_sim":
        if attempt_no > 1:
            return
        extra = float(payload.get("buffer_sec", 2))
        time.sleep(float(visibility_seconds) + extra)
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


def _start_worker_registration():
    """
    Worker registration with TTL: periodically SET a key with EX.
    API readiness can consider "at least one worker is heartbeating".
    """
    stop = threading.Event()

    def _beat():
        key = WORKER_HEARTBEAT_PREFIX + WORKER_ID
        while not stop.is_set():
            try:
                r.set(key, str(time.time()), ex=WORKER_HEARTBEAT_TTL_SECONDS)
            except Exception:
                # Best-effort; if Redis is down, readiness should fail anyway.
                pass
            stop.wait(WORKER_HEARTBEAT_INTERVAL_SECONDS)

    t = threading.Thread(target=_beat, name=f"worker_heartbeat:{WORKER_ID}", daemon=True)
    t.start()
    return stop


def _observe_duration(job_type: str, duration_seconds: float):
    """
    Redis-backed histogram with cumulative Prometheus-style buckets.
    """
    buckets = DEFAULT_DURATION_BUCKETS
    le_values = [str(b) for b in buckets] + ["+Inf"]

    pipe = r.pipeline(transaction=False)
    pipe.incr(f"{METRIC_JOB_DURATION_SECONDS}:count:{job_type}", 1)
    pipe.incrbyfloat(f"{METRIC_JOB_DURATION_SECONDS}:sum:{job_type}", float(duration_seconds))

    # Cumulative buckets: increment all le >= value (including +Inf)
    for le_str in le_values:
        if le_str == "+Inf":
            pipe.incr(f"{METRIC_JOB_DURATION_SECONDS}:bucket:{job_type}:+Inf", 1)
            continue
        le = float(le_str)
        if duration_seconds <= le:
            pipe.incr(f"{METRIC_JOB_DURATION_SECONDS}:bucket:{job_type}:{le_str}", 1)

    pipe.execute()


def main():
    log_event(logger, event="worker_started", worker_id=WORKER_ID, visibility_timeout=VISIBILITY_TIMEOUT)
    worker_hb_stop = _start_worker_registration()
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
            r.delete(PROCESSING_LOCK_PREFIX + jid)
            if not r.exists(JOB_DONE_PREFIX + jid):
                update(jid, status="queued", next_run_at="")
                r.lpush(QUEUE_KEY, jid)

        item = r.brpop(QUEUE_KEY, timeout=5)
        if item is None:
            continue
        _, job_id = item

        job = r.hgetall(JOB_KEY_PREFIX + job_id)
        if not job:
            continue

        job_vis = VISIBILITY_TIMEOUT
        if job.get("type") == "crash_sim":
            try:
                pl = json.loads(job.get("payload", "{}") or "{}")
                if pl.get("simulated_visibility_sec") is not None:
                    job_vis = max(3, min(int(pl["simulated_visibility_sec"]), 3600))
            except (TypeError, ValueError, json.JSONDecodeError):
                pass

        lease_key = LEASE_KEY_PREFIX + job_id
        lease_token = f"{WORKER_ID}:{uuid.uuid4()}"

        visible_at = time.time() + job_vis
        if not r.set(lease_key, lease_token, ex=job_vis, nx=True):
            r.lpush(QUEUE_KEY, job_id)
            continue

        r.zadd(IN_FLIGHT_KEY, {job_id: visible_at})

        # Idempotency check: already completed
        if r.exists(JOB_DONE_PREFIX + job_id):
            r.zrem(IN_FLIGHT_KEY, job_id)
            r.delete(lease_key)
            continue

        # Processing lock: prevents concurrent processing of the same job_id.
        proc_key = PROCESSING_LOCK_PREFIX + job_id
        proc_token = WORKER_ID
        if not r.set(proc_key, proc_token, nx=True, ex=job_vis):
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
        log_event(
            logger,
            event="started",
            job_id=job_id,
            worker_id=WORKER_ID,
            type=job_type,
            attempts=attempts,
            max_attempts=max_attempts,
        )

        try:
            start = time.time()
            if job_type == "crash_sim":
                run_job(
                    job_type,
                    payload,
                    job_id,
                    attempt_no=attempts,
                    visibility_seconds=job_vis,
                )
            else:
                hb_stop = _start_heartbeat(job_id, lease_token)
                try:
                    run_job(job_type, payload, job_id, attempt_no=attempts)
                finally:
                    hb_stop.set()
            duration = time.time() - start
            if job_type == "crash_sim":
                r.zrem(IN_FLIGHT_KEY, job_id)
                r.delete(lease_key)
                r.delete(proc_key)
                if r.exists(JOB_DONE_PREFIX + job_id):
                    log_event(
                        logger,
                        event="crash_sim_peer_finished",
                        job_id=job_id,
                        worker_id=WORKER_ID,
                        type=job_type,
                        duration=duration,
                    )
                else:
                    log_event(
                        logger,
                        event="crash_sim_released",
                        job_id=job_id,
                        worker_id=WORKER_ID,
                        type=job_type,
                        duration=duration,
                    )
                continue
            try:
                _observe_duration(job_type, duration)
            except Exception:
                pass
            r.zrem(IN_FLIGHT_KEY, job_id)
            r.delete(lease_key)
            r.delete(proc_key)
            r.set(JOB_DONE_PREFIX + job_id, "1")
            update(job_id, status="succeeded", last_error="")
            r.incr(f"{METRIC_JOBS_COMPLETED_TOTAL}:{job_type}", 1)
            log_event(
                logger,
                event="completed",
                job_id=job_id,
                worker_id=WORKER_ID,
                type=job_type,
                duration=duration,
                attempts=attempts,
            )
        except Exception as e:
            duration = None
            try:
                # If we failed before setting start (shouldn't happen), skip.
                duration = time.time() - start  # type: ignore[name-defined]
                _observe_duration(job_type, duration)
            except Exception:
                pass
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
                r.incr(f"{METRIC_JOBS_RETRIED_TOTAL}:{job_type}", 1)
                log_event(
                    logger,
                    event="retried",
                    job_id=job_id,
                    worker_id=WORKER_ID,
                    type=job_type,
                    duration=duration,
                    attempts=attempts,
                    max_attempts=max_attempts,
                    next_run_at=next_run_at,
                    error=str(e),
                )
            else:
                r.zrem(IN_FLIGHT_KEY, job_id)
                r.delete(lease_key)
                r.delete(proc_key)
                update(job_id, status="failed", last_error=err)
                r.lpush(DLQ_KEY, job_id)
                now2 = time.time()
                r.zadd(DLQ_ZSET_KEY, {job_id: now2})
                r.incr(f"{METRIC_JOBS_FAILED_TOTAL}:{job_type}", 1)
                r.incr(f"{METRIC_DLQ_ADDED_TOTAL}:{job_type}", 1)
                # Retention: TTL-by-time and max size.
                r.zremrangebyscore(DLQ_ZSET_KEY, 0, now2 - DLQ_RETENTION_SECONDS)
                # Keep only newest DLQ_MAX_SIZE items (remove oldest ranks).
                excess = r.zcard(DLQ_ZSET_KEY) - DLQ_MAX_SIZE
                if excess > 0:
                    r.zremrangebyrank(DLQ_ZSET_KEY, 0, excess - 1)
                log_event(
                    logger,
                    event="dlq",
                    job_id=job_id,
                    worker_id=WORKER_ID,
                    type=job_type,
                    duration=duration,
                    attempts=attempts,
                    max_attempts=max_attempts,
                    error=str(e),
                )


if __name__ == "__main__":
    main()

