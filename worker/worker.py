import json
import time
import traceback

from common.redis_client import get_redis
from common.constants import QUEUE_KEY, JOB_KEY_PREFIX

r = get_redis()

def update(job_id: str, **fields):
    fields["updated_at"] = time.time()
    r.hset(JOB_KEY_PREFIX + job_id, mapping=fields)

def run_job(job_type: str, payload: dict):
    if job_type == "sleep":
        ms = int(payload.get("ms", 1000))
        time.sleep(ms / 1000.0)
        return
    raise ValueError(f"unknown job type: {job_type}")

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
            run_job(job_type, payload)
            update(job_id, status="succeeded")
        except Exception as e:
            err = f"{e}\n{traceback.format_exc()}"
            update(job_id, status="failed", last_error=err)

            if attempts < max_attempts:
                time.sleep(min(2 ** attempts, 10))
                update(job_id, status="queued")
                r.lpush(QUEUE_KEY, job_id)

if __name__ == "__main__":
    main()

