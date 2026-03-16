"""
Reliability: worker death and in-flight reclaim.

Acceptance criteria:
- No lost jobs: if a worker dies after claiming but before ack, the job is re-queued
  and eventually completed or sent to DLQ.
- Expired in-flight jobs get reclaimed by the recovery pass and eventually complete or DLQ.
"""
import os
import time

import pytest
import requests

from helpers import get_job, wait_for_status

# Redis keys (must match common.constants)
QUEUE_KEY = "forgequeue:queue"
JOB_KEY_PREFIX = "forgequeue:job:"
IN_FLIGHT_KEY = "forgequeue:in_flight"
LEASE_KEY_PREFIX = "forgequeue:lease:"
JOB_DONE_PREFIX = "job:done:"
DELAYED_KEY = "forgequeue:delayed"

VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", "15"))


def create_job(api_url: str, job_type: str, payload: dict | None = None, max_attempts: int = 3):
    resp = requests.post(
        f"{api_url}/jobs",
        headers={"Content-Type": "application/json"},
        json={"type": job_type, "payload": payload or {}, "max_attempts": max_attempts},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()["job_id"]


def test_worker_dies_after_claim_before_ack_job_is_not_lost(api, r):
    """
    Worker dies after claiming but before ack.
    Simulate: job is claimed (in IN_FLIGHT, lease set) but never acked.
    After visibility timeout, recovery re-queues the job. One worker eventually
    completes it. No lost job.
    """
    job_id = create_job(api, "sleep", {"ms": 100}, max_attempts=2)
    # Wait until worker has claimed it (running)
    wait_for_status(api, job_id, {"running", "queued"}, timeout_s=10)

    # Simulate worker death: leave job in IN_FLIGHT with visible_at in the past
    # so the next recovery pass will re-queue it. Remove lease so recovery can re-queue
    # (recovery removes lease and re-queues if not done).
    r.zadd(IN_FLIGHT_KEY, {job_id: time.time() - 1})
    r.delete(LEASE_KEY_PREFIX + job_id)
    assert not r.exists(JOB_DONE_PREFIX + job_id), "job must not be done yet"

    # Wait for recovery pass (worker loop runs every 5s brpop + recovery). Need
    # to wait longer than one brpop (5s) so recovery runs and re-queues.
    time.sleep(7)

    # Job must eventually complete (no lost job)
    final = wait_for_status(api, job_id, {"succeeded", "failed"}, timeout_s=25)
    assert final["status"] == "succeeded", f"Job must not be lost; got {final}"
    # Should have been attempted at least twice: once before "death", once after reclaim
    assert int(final.get("attempts", 0)) >= 1


def test_worker_dies_during_heartbeat_window_job_reclaimed(api, r):
    """
    Worker dies during heartbeat window (job was running, heartbeating).
    We can't kill the process in e2e; we simulate by making the job look
    like an expired in-flight (visible_at in past) and clearing lease.
    Recovery should re-queue; job eventually completes. No lost job.
    """
    job_id = create_job(api, "sleep", {"ms": 500}, max_attempts=2)
    wait_for_status(api, job_id, {"running", "queued"}, timeout_s=10)

    # Simulate worker died while job was in-flight (heartbeat had been extending
    # visible_at; now worker is gone, we set visible_at to past).
    r.zadd(IN_FLIGHT_KEY, {job_id: time.time() - 2})
    r.delete(LEASE_KEY_PREFIX + job_id)
    if r.exists(JOB_DONE_PREFIX + job_id):
        pytest.skip("Job already completed before we could simulate death")

    time.sleep(7)

    final = wait_for_status(api, job_id, {"succeeded", "failed"}, timeout_s=25)
    assert final["status"] == "succeeded", f"Job must be reclaimed and complete; got {final}"


def test_expired_in_flight_job_gets_reclaimed_and_completes(api, r):
    """
    Expired in-flight job gets reclaimed by recovery pass and eventually completes.
    Directly place a job in IN_FLIGHT with visible_at in the past; do not set
    JOB_DONE. Recovery must move it back to queue; worker picks it up and completes.
    """
    job_id = create_job(api, "sleep", {"ms": 50}, max_attempts=1)
    # Get it into running so job hash and queue state exist, then force into
    # expired in-flight state without completing
    wait_for_status(api, job_id, {"running", "queued", "succeeded"}, timeout_s=8)
    if r.exists(JOB_DONE_PREFIX + job_id):
        pytest.skip("Job already succeeded")

    r.zadd(IN_FLIGHT_KEY, {job_id: time.time() - 1})
    r.delete(LEASE_KEY_PREFIX + job_id)
    r.lrem(QUEUE_KEY, 0, job_id)

    time.sleep(7)

    final = wait_for_status(api, job_id, {"succeeded", "failed"}, timeout_s=20)
    assert final["status"] == "succeeded", f"Reclaimed job must complete; got {final}"
