from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict
import uuid
import json
import time

from common.redis_client import get_redis
from common.constants import QUEUE_KEY, JOB_KEY_PREFIX
from fastapi import FastAPI, HTTPException, Header

r = get_redis()
app = FastAPI(title="ForgeQueue")


ALLOWED_JOB_TYPES = {"sleep", "fail_once", "always_fail"}
IDEMP_PREFIX = "idemp:"
IDEMP_TTL_SECONDS = 60 * 60 * 24  # 24 hours


class CreateJobRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = {}
    max_attempts: int = 3


@app.post("/jobs")
def create_job(req: CreateJobRequest, idempotency_key: str | None = Header(default=None)):
    if idempotency_key:
        idem_key = IDEMP_PREFIX + idempotency_key
        existing = r.get(idem_key)
        if existing:
            return {"job_id": existing, "status": "queued", "idempotent_replay": True}

    if req.type not in ALLOWED_JOB_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown job type: {req.type}")
    job_id = str(uuid.uuid4())

    if idempotency_key:
        ok = r.set(idem_key, job_id, nx=True, ex=IDEMP_TTL_SECONDS)
        if not ok:
            existing = r.get(idem_key)
            return {"job_id": existing, "status": "queued", "idempotent_replay": True}
    now = time.time()

    job = {
        "job_id": job_id,
        "type": req.type,
        "status": "queued",
        "payload": json.dumps(req.payload),
        "attempts": 0,
        "max_attempts": req.max_attempts,
        "created_at": now,
        "updated_at": now,
        "last_error": "",
    }

    r.hset(JOB_KEY_PREFIX + job_id, mapping=job)
    r.lpush(QUEUE_KEY, job_id)

    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    data = r.hgetall(JOB_KEY_PREFIX + job_id)
    if not data:
        raise HTTPException(status_code=404, detail="job not found")
    return data


@app.get("/stats")
def get_stats():
    keys = r.keys(JOB_KEY_PREFIX + "*")

    stats = {
        "queued": 0,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
        "total_jobs": len(keys),
    }

    for key in keys:
        job = r.hgetall(key)
        status = job.get("status")
        if status in stats:
            stats[status] += 1

    return stats
