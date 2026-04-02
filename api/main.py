import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, Response
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Any, Dict, Optional
import hashlib
import uuid
import json
import time

from common.redis_client import get_redis
from common.constants import (
    QUEUE_KEY,
    JOB_KEY_PREFIX,
    JOBS_INDEX_KEY,
    DLQ_KEY,
    DLQ_ZSET_KEY,
    DLQ_RETENTION_SECONDS,
    DLQ_MAX_SIZE,
    DLQ_RETRIED_PREFIX,
    DELAYED_KEY,
    IN_FLIGHT_KEY,
    JOB_DONE_PREFIX,
    LEASE_KEY_PREFIX,
    DEDUP_PREFIX,
    DEDUP_TTL_SECONDS,
    HEALTH_PING_KEY,
    WORKER_HEARTBEAT_PREFIX,
    METRIC_JOBS_ENQUEUED_TOTAL,
    METRIC_JOBS_COMPLETED_TOTAL,
    METRIC_JOBS_FAILED_TOTAL,
    METRIC_JOBS_RETRIED_TOTAL,
    METRIC_DLQ_ADDED_TOTAL,
)
from common.metrics import RedisMetricsCollector
from common.observability import configure_logging, log_event
from prometheus_client import CollectorRegistry, CONTENT_TYPE_LATEST, generate_latest

r = get_redis()
_API_BOOT_AT = time.time()
FORGEQUEUE_GITHUB_URL = os.getenv(
    "FORGEQUEUE_GITHUB_URL", "https://github.com/Adamvalade/forgequeue"
)

app = FastAPI(
    title="ForgeQueue",
    description="Fault-tolerant Redis-backed job queue with retries, crash recovery, and Prometheus metrics.",
    version="0.1.0",
)
logger = configure_logging("forgequeue-api")


@app.get("/")
def root():
    return {
        "service": "ForgeQueue",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "health": "/health",
        "ready": "/ready",
        "metrics": "/metrics",
        "demo": "/demo/",
        "system": "/system",
    }

# Prometheus registry backed by Redis (single /metrics view across processes).
_registry = CollectorRegistry(auto_describe=True)
_registry.register(RedisMetricsCollector(r))


ALLOWED_JOB_TYPES = {"sleep", "fail_once", "always_fail", "crash_sim"}
IDEMP_PREFIX = "idemp:"
IDEMP_TTL_SECONDS = 60 * 60 * 24  # 24 hours
TERMINAL_STATUSES = {"succeeded", "failed"}
ACTIVE_STATUSES = {"queued", "running", "retrying"}


def _dedup_hash(job_type: str, payload: dict) -> str:
    normalized = json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    raw = f"{job_type}|{normalized}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class CreateJobRequest(BaseModel):
    type: str
    payload: Dict[str, Any] = {}
    max_attempts: int = 3


class RetryDlqRequest(BaseModel):
    max_attempts: Optional[int] = None
    force: bool = False


@app.post("/jobs")
def create_job(req: CreateJobRequest, idempotency_key: str | None = Header(default=None)):
    if idempotency_key:
        idem_key = IDEMP_PREFIX + idempotency_key
        existing = r.get(idem_key)
        if existing:
            return {"job_id": existing, "status": "queued", "idempotent_replay": True}

    if req.type not in ALLOWED_JOB_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown job type: {req.type}")

    # Content-based deduplication (short TTL window).
    dedup_key = DEDUP_PREFIX + _dedup_hash(req.type, req.payload)
    existing_job_id = r.get(dedup_key)
    if existing_job_id:
        existing = r.hgetall(JOB_KEY_PREFIX + existing_job_id)
        if existing and existing.get("status") in ACTIVE_STATUSES:
            return {
                "job_id": existing_job_id,
                "status": existing.get("status", "queued"),
                "dedup_replay": True,
            }
        # If job is terminal or missing, allow a new enqueue by clearing dedup key.
        r.delete(dedup_key)

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
        "payload": json.dumps(req.payload, sort_keys=True, separators=(",", ":")),
        "attempts": 0,
        "max_attempts": req.max_attempts,
        "created_at": now,
        "updated_at": now,
        "last_error": "",
    }

    r.hset(JOB_KEY_PREFIX + job_id, mapping=job)
    r.sadd(JOBS_INDEX_KEY, job_id)
    # Best-effort: set dedup mapping only if absent; if a race occurs, keep the first.
    r.set(dedup_key, job_id, nx=True, ex=DEDUP_TTL_SECONDS)
    r.lpush(QUEUE_KEY, job_id)

    # Metrics: Redis-backed counter (monotonic).
    r.incr(f"{METRIC_JOBS_ENQUEUED_TOTAL}:{req.type}", 1)

    log_event(
        logger,
        event="enqueued",
        job_id=job_id,
        type=req.type,
        max_attempts=req.max_attempts,
    )

    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    data = r.hgetall(JOB_KEY_PREFIX + job_id)
    if not data:
        raise HTTPException(status_code=404, detail="job not found")
    return data


def _sum_redis_counter(base: str) -> int:
    total = 0
    try:
        for key in r.scan_iter(match=base + ":*"):
            total += int(float(r.get(key) or 0))
    except Exception:
        pass
    return total


def _worker_heartbeat_summary():
    now = time.time()
    count = 0
    newest_age = None
    try:
        for key in r.scan_iter(match=WORKER_HEARTBEAT_PREFIX + "*"):
            count += 1
            raw = r.get(key)
            if raw is not None:
                try:
                    ts = float(raw)
                    age = now - ts
                    if newest_age is None or age < newest_age:
                        newest_age = age
                except ValueError:
                    pass
    except Exception:
        pass
    return {"registered_workers": count, "newest_heartbeat_age_seconds": newest_age}


@app.get("/system")
def system_status():
    now = time.time()
    try:
        qd = r.llen(QUEUE_KEY)
        inf = r.zcard(IN_FLIGHT_KEY)
        dlq = r.zcard(DLQ_ZSET_KEY)
    except Exception:
        qd = inf = dlq = -1
    return {
        "api_uptime_seconds": round(now - _API_BOOT_AT, 3),
        "workers": _worker_heartbeat_summary(),
        "queue_depth": qd,
        "in_flight": inf,
        "dlq_depth": dlq,
        "totals": {
            "jobs_enqueued": _sum_redis_counter(METRIC_JOBS_ENQUEUED_TOTAL),
            "jobs_completed": _sum_redis_counter(METRIC_JOBS_COMPLETED_TOTAL),
            "jobs_failed": _sum_redis_counter(METRIC_JOBS_FAILED_TOTAL),
            "jobs_retried": _sum_redis_counter(METRIC_JOBS_RETRIED_TOTAL),
            "dlq_added": _sum_redis_counter(METRIC_DLQ_ADDED_TOTAL),
        },
    }


@app.get("/stats")
def get_stats():
    # source of truth: job hashes
    ids = list(r.smembers(JOBS_INDEX_KEY))

    counts = {"queued": 0, "running": 0, "succeeded": 0, "failed": 0}

    for job_id in ids:
        job = r.hgetall(JOB_KEY_PREFIX + job_id)
        if not job:
            continue
        status = job.get("status", "")
        if status in counts:
            counts[status] += 1

    counts["total_jobs"] = sum(counts.values())
    counts["queue_depth"] = r.llen(QUEUE_KEY)
    return counts


@app.get("/health")
def health():
    """
    Liveness: returns 200 if Redis is reachable; 503 otherwise.
    """
    try:
        r.ping()
        return {"status": "ok", "redis": "ok"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis down: {e}")


@app.get("/ready")
def ready():
    """
    Readiness: verifies Redis and (best-effort) ability to write/read a test key.
    Also reports whether at least one worker has heartbeated recently.
    """
    try:
        r.ping()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis down: {e}")

    # Optional read/write ping.
    try:
        token = str(time.time())
        r.set(HEALTH_PING_KEY, token, ex=30)
        got = r.get(HEALTH_PING_KEY)
        if got != token:
            raise RuntimeError("health ping mismatch")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"redis rw failed: {e}")

    # Worker liveness (at least one heartbeat key exists).
    worker_count = 0
    try:
        for _ in r.scan_iter(match=WORKER_HEARTBEAT_PREFIX + "*"):
            worker_count += 1
            if worker_count >= 1:
                break
    except Exception:
        worker_count = 0

    if worker_count < 1:
        raise HTTPException(status_code=503, detail="no workers heartbeating")

    return {"status": "ok", "redis": "ok", "workers": worker_count}


@app.get("/metrics")
def metrics():
    # Registry collector reads from Redis at scrape time.
    payload = generate_latest(_registry)
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


def _dlq_retention_cleanup(now: float):
    r.zremrangebyscore(DLQ_ZSET_KEY, 0, now - DLQ_RETENTION_SECONDS)
    # Enforce max size: keep newest DLQ_MAX_SIZE items (remove oldest).
    excess = r.zcard(DLQ_ZSET_KEY) - DLQ_MAX_SIZE
    if excess > 0:
        r.zremrangebyrank(DLQ_ZSET_KEY, 0, excess - 1)


@app.get("/dlq")
def list_dlq(limit: int = 50):
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")

    now = time.time()
    _dlq_retention_cleanup(now)

    job_ids = r.zrevrange(DLQ_ZSET_KEY, 0, limit - 1)
    items = []
    for job_id in job_ids:
        job = r.hgetall(JOB_KEY_PREFIX + job_id)
        if not job:
            continue
        items.append(
            {
                "job_id": job_id,
                "status": job.get("status", ""),
                "attempts": job.get("attempts", ""),
                "max_attempts": job.get("max_attempts", ""),
                "updated_at": job.get("updated_at", ""),
                "last_error": job.get("last_error", ""),
            }
        )

    return {"items": items}


@app.get("/dlq/{job_id}")
def get_dlq_job(job_id: str):
    # Must exist AND be present in DLQ index (ZSET). We also accept legacy list membership.
    job = r.hgetall(JOB_KEY_PREFIX + job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    in_dlq = r.zscore(DLQ_ZSET_KEY, job_id) is not None
    if not in_dlq:
        # Legacy support: if it exists in the DLQ list, treat it as DLQ.
        if job_id not in r.lrange(DLQ_KEY, 0, 5000):
            raise HTTPException(status_code=404, detail="job not in dlq")

    return job


@app.post("/dlq/{job_id}/retry")
def retry_dlq_job(job_id: str, req: RetryDlqRequest = RetryDlqRequest()):
    job = r.hgetall(JOB_KEY_PREFIX + job_id)
    if not job:
        raise HTTPException(status_code=404, detail="job not found")

    # Require DLQ membership (ZSET or legacy list).
    in_dlq = r.zscore(DLQ_ZSET_KEY, job_id) is not None or (job_id in r.lrange(DLQ_KEY, 0, 5000))
    if not in_dlq:
        raise HTTPException(status_code=404, detail="job not in dlq")

    retried_key = DLQ_RETRIED_PREFIX + job_id
    if not req.force:
        if r.exists(retried_key):
            raise HTTPException(status_code=409, detail="job already retried from dlq")
        r.set(retried_key, "1", ex=60 * 60 * 24)  # 24h guardrail

    # Remove from DLQ structures.
    r.zrem(DLQ_ZSET_KEY, job_id)
    r.lrem(DLQ_KEY, 0, job_id)

    # Clear any in-flight/delayed state and leases.
    r.zrem(IN_FLIGHT_KEY, job_id)
    r.delete(LEASE_KEY_PREFIX + job_id)
    r.zrem(DELAYED_KEY, job_id)
    r.delete(JOB_DONE_PREFIX + job_id)

    now = time.time()
    mapping = {
        "status": "queued",
        "attempts": 0,
        "updated_at": now,
        "last_error": "",
        "next_run_at": "",
    }
    if req.max_attempts is not None:
        mapping["max_attempts"] = req.max_attempts
    r.hset(JOB_KEY_PREFIX + job_id, mapping=mapping)
    r.lpush(QUEUE_KEY, job_id)

    return {"job_id": job_id, "status": "queued"}


@app.get("/demo", include_in_schema=False)
def demo_redirect():
    return RedirectResponse(url="/demo/")


@app.get("/demo/config.json", include_in_schema=False)
def demo_config():
    return {"github_url": FORGEQUEUE_GITHUB_URL}


_demo_dir = Path(__file__).resolve().parent / "demo"
if _demo_dir.is_dir():
    app.mount(
        "/demo",
        StaticFiles(directory=str(_demo_dir), html=True),
        name="demo",
    )
