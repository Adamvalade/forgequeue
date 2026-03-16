"""
Reliability: long-running job extends visibility and is not double-run.

Acceptance criteria:
- No concurrent execution of same logical job: a long-running job that extends
  visibility via heartbeat must complete exactly once (no double-run).
"""
import time

import pytest
import requests

from helpers import wait_for_status

IN_FLIGHT_KEY = "forgequeue:in_flight"


def create_job(api_url: str, job_type: str, payload: dict | None = None, max_attempts: int = 1):
    resp = requests.post(
        f"{api_url}/jobs",
        headers={"Content-Type": "application/json"},
        json={"type": job_type, "payload": payload or {}, "max_attempts": max_attempts},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()["job_id"]


def test_long_running_job_extends_visibility_and_does_not_double_run(api, r):
    """
    Long-running job extends visibility (heartbeat) and is executed exactly once.
    Run a sleep long enough that visibility would expire without heartbeat;
    heartbeat keeps it in-flight. Assert job completes once (attempts=1, succeeded).
    """
    # Sleep longer than default visibility (15s in tests) so without heartbeat
    # the job would become reclaimable. With heartbeat it stays with the same worker.
    sleep_ms = 22000  # 22s
    job_id = create_job(api, "sleep", {"ms": sleep_ms}, max_attempts=1)

    # While it's running, in-flight score should be extended (heartbeat)
    running = wait_for_status(api, job_id, {"running"}, timeout_s=10)
    assert running["status"] == "running"

    s1 = r.zscore(IN_FLIGHT_KEY, job_id)
    assert s1 is not None
    # Wait past one heartbeat interval (~5s with 15s visibility)
    time.sleep(6)
    s2 = r.zscore(IN_FLIGHT_KEY, job_id)
    if s2 is not None:
        assert s2 > s1, "Heartbeat should extend in-flight visible_at"
    # If job already completed (s2 is None), we still require exactly one run below

    # Job must complete exactly once (no reclaim, no double-run)
    final = wait_for_status(api, job_id, {"succeeded", "failed"}, timeout_s=40)
    assert final["status"] == "succeeded", f"Job must succeed; got {final}"
    assert int(final.get("attempts", 0)) == 1, "Must run exactly once (no reclaim)"
