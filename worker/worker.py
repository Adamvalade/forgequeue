import json
import time
import traceback

from common.redis_client import get_redis
from common.constants import QUEUE_KEY, JOB_KEY_PREFIX

r = get_redis()

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
    r.hset(JOB_KEY_PREFIX + job_id, mapping=fields)
    
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

def main():
    print("worker started", flush=True)
    while True:
        item = r.brpop(QUEUE_KEY, timeout=5)
        if item is None:
            continue
        _, job_id = item

        job = r.hgetall(JOB_KEY_PREFIX + job_id)
        if not job:
            continue

        attempts = int(job.get("attempts", "0")) + 1
        max_attempts = int(job.get("max_attempts", "3"))
        job_type = job.get("type", "")
        payload = json.loads(job.get("payload", "{}") or "{}")

        update(job_id, status="running", attempts=attempts, last_error="")

        try:
            run_job(job_type, payload, job_id)
            update(job_id, status="succeeded", last_error="")
        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"

            if attempts < max_attempts:
                # retry later
                backoff = min(2 ** attempts, 10)
                update(job_id, status="queued", last_error=err)
                time.sleep(backoff)
                r.lpush(QUEUE_KEY, job_id)
            else:
                # permanent failure
                update(job_id, status="failed", last_error=err)

if __name__ == "__main__":
    main()

