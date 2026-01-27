from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict
import uuid
import json
import time

from common.redis_client import get_redis
from common.constants import QUEUE_KEY, JOB_KEY_PREFIX

r = get_redis()
app = FastAPI(title="ForgeQueue")

class CreateJobRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = {}
    max_attempts: int = 3

@app.post("/jobs")
def create_job(req: CreateJobRequest):
    job_id = str(uuid.uuid4())
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

