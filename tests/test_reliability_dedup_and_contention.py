"""
Reliability: deduplication and single-run under contention.

Acceptance criteria:
- Duplicate enqueue with same logical payload returns same job or skips enqueue;
  only one logical execution.
- Two workers racing for the same reclaimable job: only one actually runs it
  (idempotency / processing lock / JOB_DONE check prevents double execution).
"""
import time
import uuid

import pytest
import requests

from helpers import get_job, wait_for_status

QUEUE_KEY = "forgequeue:queue"
JOB_KEY_PREFIX = "forgequeue:job:"
JOB_DONE_PREFIX = "job:done:"
JOBS_INDEX_KEY = "forgequeue:jobs:index"


def create_job(
    api_url: str,
    job_type: str,
    payload: dict | None = None,
    max_attempts: int = 1,
    idem: str | None = None,
):
    headers = {"Content-Type": "application/json"}
    if idem:
        headers["Idempotency-Key"] = idem
    resp = requests.post(
        f"{api_url}/jobs",
        headers=headers,
        json={"type": job_type, "payload": payload or {}, "max_attempts": max_attempts},
        timeout=5,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["job_id"], data.get("dedup_replay", False), data.get("idempotent_replay", False)


def test_duplicate_enqueue_same_logical_payload_returns_same_job(api, r):
    """
    Two enqueues with same type + payload (no idempotency key) return same job_id
    when the first is still active (queued/running/retrying). Second call is dedup.
    """
    payload = {"ms": 100, "key": str(uuid.uuid4())}
    job_id1, replay1, _ = create_job(api, "sleep", payload, max_attempts=1)
    job_id2, replay2, _ = create_job(api, "sleep", payload, max_attempts=1)

    assert job_id1 == job_id2, "Same logical payload must return same job_id"
    assert replay2 is True, "Second enqueue should be dedup replay"
    # Only one job record
    assert r.scard(JOBS_INDEX_KEY) >= 1
    final = wait_for_status(api, job_id1, {"succeeded", "failed"}, timeout_s=15)
    assert final["status"] == "succeeded"
    assert int(final.get("attempts", 0)) == 1, "Logical job runs once"


def test_duplicate_enqueue_with_idempotency_key_returns_same_job(api, r):
    """
    Two enqueues with same idempotency key return same job_id (idempotent_replay).
    """
    idem = f"idem-{uuid.uuid4()}"
    job_id1, _, idem1 = create_job(api, "sleep", {"ms": 50}, max_attempts=1, idem=idem)
    job_id2, _, idem2 = create_job(api, "sleep", {"ms": 50}, max_attempts=1, idem=idem)

    assert job_id1 == job_id2
    assert idem2 is True
    final = wait_for_status(api, job_id1, {"succeeded"}, timeout_s=15)
    assert final["status"] == "succeeded"
    assert int(final.get("attempts", 0)) == 1


def test_double_queued_same_job_id_only_runs_once(api, r):
    """
    Two workers racing for same reclaimable job: only one actually runs it.
    Simulate by pushing the same job_id onto the queue twice (as if recovery
    or a bug double lpush'd). Worker should run it once; second pop sees
    JOB_DONE and skips. No concurrent execution of same logical job.
    """
    job_id = str(uuid.uuid4())
    now = time.time()
    r.hset(
        JOB_KEY_PREFIX + job_id,
        mapping={
            "job_id": job_id,
            "type": "sleep",
            "status": "queued",
            "payload": "{\"ms\":80}",
            "attempts": 0,
            "max_attempts": 2,
            "created_at": now,
            "updated_at": now,
            "last_error": "",
        },
    )
    r.sadd(JOBS_INDEX_KEY, job_id)
    r.lpush(QUEUE_KEY, job_id, job_id)

    final = wait_for_status(api, job_id, {"succeeded", "failed"}, timeout_s=25)
    assert final["status"] == "succeeded"
    # Must have been executed exactly once (second pop sees JOB_DONE and skips)
    assert int(final.get("attempts", 0)) == 1, "Same job_id must run only once despite double push"
    assert r.exists(JOB_DONE_PREFIX + job_id)
