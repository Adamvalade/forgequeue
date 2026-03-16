import os
import time
import uuid

import pytest
import redis
import requests


API_URL = os.getenv("API_URL", "http://localhost:8000")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
VISIBILITY_TIMEOUT_SECONDS = int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", "15"))


@pytest.fixture()
def r():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()
    client.flushdb()
    return client


@pytest.fixture()
def api():
    # Ensure API is reachable before running assertions.
    deadline = time.time() + 15
    last_err = None
    while time.time() < deadline:
        try:
            resp = requests.get(f"{API_URL}/stats", timeout=2)
            if resp.status_code == 200:
                return API_URL
            last_err = f"status={resp.status_code} body={resp.text}"
        except Exception as e:  # noqa: BLE001 - best-effort wait
            last_err = repr(e)
        time.sleep(0.5)
    raise RuntimeError(f"API not ready: {last_err}")


def create_job(api_url: str, job_type: str, payload: dict | None = None, max_attempts: int = 2, idem: str | None = None):
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
    return resp.json()["job_id"]


def get_job(api_url: str, job_id: str):
    resp = requests.get(f"{api_url}/jobs/{job_id}", timeout=5)
    resp.raise_for_status()
    return resp.json()


def wait_for_status(api_url: str, job_id: str, target: set[str], timeout_s: float = 30):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        last = get_job(api_url, job_id)
        if last.get("status") in target:
            return last
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {target}. Last={last}")


def test_idempotency_key_returns_same_job(api, r):
    idem = f"pytest-{uuid.uuid4()}"
    job1 = create_job(api, "sleep", {"ms": 5}, max_attempts=1, idem=idem)
    job2 = create_job(api, "sleep", {"ms": 5}, max_attempts=1, idem=idem)
    assert job1 == job2


def test_fail_once_retries_then_succeeds(api, r):
    job_id = create_job(api, "fail_once", {}, max_attempts=2)

    # Should eventually succeed after 2 attempts.
    final = wait_for_status(api, job_id, {"succeeded"}, timeout_s=30)
    assert final["attempts"] == "2"


def test_always_fail_ends_in_dlq(api, r):
    job_id = create_job(api, "always_fail", {}, max_attempts=2)

    final = wait_for_status(api, job_id, {"failed"}, timeout_s=40)
    assert final["attempts"] == "2"

    # Ensure it was pushed to DLQ list.
    dlq = r.lrange("forgequeue:dlq", 0, -1)
    assert job_id in dlq


def test_retry_is_scheduled_in_delayed_zset(api, r):
    job_id = create_job(api, "always_fail", {}, max_attempts=2)

    # Wait until we see retrying, which indicates it has been scheduled via ZSET.
    interim = wait_for_status(api, job_id, {"retrying", "failed"}, timeout_s=10)
    if interim["status"] == "failed":
        pytest.fail(f"Job failed too quickly to observe retry scheduling: {interim}")

    score = r.zscore("forgequeue:delayed", job_id)
    assert score is not None
    assert score > time.time() - 1


def test_stats_includes_queue_depth_and_totals(api, r):
    # Create a couple jobs and let them finish.
    j1 = create_job(api, "sleep", {"ms": 25}, max_attempts=1)
    j2 = create_job(api, "sleep", {"ms": 25}, max_attempts=1)
    wait_for_status(api, j1, {"succeeded"}, timeout_s=10)
    wait_for_status(api, j2, {"succeeded"}, timeout_s=10)

    resp = requests.get(f"{api}/stats", timeout=5)
    resp.raise_for_status()
    data = resp.json()
    assert "queue_depth" in data
    assert "total_jobs" in data
    assert data["succeeded"] >= 2


def test_visibility_timeout_requeues_unacked_job(api, r):
    # This asserts the core crash-recovery invariant without killing containers:
    # simulate a claim by placing in in-flight with visible_at in the past.
    job_id = create_job(api, "sleep", {"ms": 5}, max_attempts=1)
    wait_for_status(api, job_id, {"running", "queued"}, timeout_s=5)

    # Force into in-flight with a past score (recovery should re-queue it if not done).
    r.zadd("forgequeue:in_flight", {job_id: time.time() - 1})
    r.delete(f"job:done:{job_id}")

    # Wait > brpop timeout (5s) + a little; worker loop should run recovery pass.
    time.sleep(7)

    # Job should have been re-queued and completed (sleep=5ms).
    final = wait_for_status(api, job_id, {"succeeded"}, timeout_s=15)
    assert final["status"] == "succeeded"
