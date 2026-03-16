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


def test_content_dedup_returns_same_job(api, r):
    # Same type + same payload without idempotency key should dedup briefly.
    job1 = create_job(api, "sleep", {"ms": 50}, max_attempts=1)
    job2 = create_job(api, "sleep", {"ms": 50}, max_attempts=1)
    assert job1 == job2


def test_content_dedup_allows_new_job_after_terminal(api, r):
    job1 = create_job(api, "sleep", {"ms": 5}, max_attempts=1)
    wait_for_status(api, job1, {"succeeded"}, timeout_s=10)
    job2 = create_job(api, "sleep", {"ms": 5}, max_attempts=1)
    assert job2 != job1


def test_processing_lock_prevents_concurrent_pickup(api, r):
    # Create a job directly in Redis so we can set the lock BEFORE enqueuing (avoid race).
    job_id = str(uuid.uuid4())
    now = time.time()
    r.hset(
        f"forgequeue:job:{job_id}",
        mapping={
            "job_id": job_id,
            "type": "sleep",
            "status": "queued",
            "payload": "{\"ms\":2000}",
            "attempts": 0,
            "max_attempts": 1,
            "created_at": now,
            "updated_at": now,
            "last_error": "",
        },
    )
    r.sadd("forgequeue:jobs:index", job_id)

    r.set(f"forgequeue:processing:{job_id}", "blocked", ex=10, nx=True)
    r.lpush("forgequeue:queue", job_id)

    time.sleep(2)
    job = get_job(api, job_id)
    assert job["status"] == "queued"
    assert str(job["attempts"]) == "0"

    # Release lock and ensure the job completes.
    r.delete(f"forgequeue:processing:{job_id}")
    wait_for_status(api, job_id, {"succeeded"}, timeout_s=30)


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

    # And appears in the DLQ API list.
    resp = requests.get(f"{api}/dlq?limit=50", timeout=5)
    resp.raise_for_status()
    items = resp.json()["items"]
    assert any(it["job_id"] == job_id for it in items)


def test_dlq_retry_requeues_job(api, r):
    job_id = create_job(api, "always_fail", {}, max_attempts=1)
    wait_for_status(api, job_id, {"failed"}, timeout_s=30)

    # Retry from DLQ with higher max_attempts so it can succeed if type allows.
    # We'll switch type by directly editing the hash to fail_once to ensure retry path is tested
    # without changing the API schema.
    r.hset(f"forgequeue:job:{job_id}", mapping={"type": "fail_once", "payload": "{}"})

    resp = requests.post(f"{api}/dlq/{job_id}/retry", json={"max_attempts": 2}, timeout=5)
    resp.raise_for_status()

    final = wait_for_status(api, job_id, {"succeeded"}, timeout_s=40)
    assert final["attempts"] == "2"


def test_dlq_retry_guardrail_requires_force(api, r):
    job_id = create_job(api, "always_fail", {}, max_attempts=1)
    wait_for_status(api, job_id, {"failed"}, timeout_s=30)

    # First retry is allowed.
    resp1 = requests.post(f"{api}/dlq/{job_id}/retry", json={"max_attempts": 1}, timeout=5)
    assert resp1.status_code == 200

    # Put it back into DLQ so we can retry again (simulate it failing again).
    r.hset(f"forgequeue:job:{job_id}", mapping={"status": "failed", "attempts": "1", "max_attempts": "1"})
    r.lpush("forgequeue:dlq", job_id)
    r.zadd("forgequeue:dlq:z", {job_id: time.time()})

    # Second retry without force should be blocked.
    resp2 = requests.post(f"{api}/dlq/{job_id}/retry", json={"max_attempts": 1}, timeout=5)
    assert resp2.status_code == 409

    # With force it should work.
    resp3 = requests.post(f"{api}/dlq/{job_id}/retry", json={"max_attempts": 1, "force": True}, timeout=5)
    assert resp3.status_code == 200


def test_get_dlq_job_requires_membership(api, r):
    job_id = create_job(api, "sleep", {"ms": 5}, max_attempts=1)
    # Not in DLQ
    resp = requests.get(f"{api}/dlq/{job_id}", timeout=5)
    assert resp.status_code == 404


def test_get_dlq_job_supports_legacy_list_membership(api, r):
    job_id = create_job(api, "sleep", {"ms": 5}, max_attempts=1)
    # Add only to legacy list (not zset)
    r.lpush("forgequeue:dlq", job_id)
    resp = requests.get(f"{api}/dlq/{job_id}", timeout=5)
    resp.raise_for_status()
    data = resp.json()
    assert data["job_id"] == job_id


def test_dlq_list_enforces_limit_validation(api, r):
    resp = requests.get(f"{api}/dlq?limit=0", timeout=5)
    assert resp.status_code == 400
    resp = requests.get(f"{api}/dlq?limit=501", timeout=5)
    assert resp.status_code == 400


def test_dlq_retention_cleanup_drops_old_entries(api, r):
    # Create two jobs and mark them as failed.
    old_id = create_job(api, "sleep", {"ms": 5}, max_attempts=1)
    new_id = create_job(api, "sleep", {"ms": 6}, max_attempts=1)
    r.hset(f"forgequeue:job:{old_id}", mapping={"status": "failed", "attempts": "1", "max_attempts": "1"})
    r.hset(f"forgequeue:job:{new_id}", mapping={"status": "failed", "attempts": "1", "max_attempts": "1"})

    now = time.time()
    week = 60 * 60 * 24 * 7
    r.zadd("forgequeue:dlq:z", {old_id: now - (week + 10), new_id: now})

    # Listing triggers cleanup; old entry should be removed from zset.
    resp = requests.get(f"{api}/dlq?limit=50", timeout=5)
    resp.raise_for_status()
    assert r.zscore("forgequeue:dlq:z", old_id) is None
    assert r.zscore("forgequeue:dlq:z", new_id) is not None


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
    j2 = create_job(api, "sleep", {"ms": 26}, max_attempts=1)
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


def test_heartbeat_extends_in_flight_visibility(api, r):
    # Create a long-ish job so we can observe heartbeat while it's running.
    job_id = create_job(api, "sleep", {"ms": 12000}, max_attempts=1)
    wait_for_status(api, job_id, {"running"}, timeout_s=10)

    # The worker should extend the in-flight ZSET score periodically while the job runs.
    s1 = r.zscore("forgequeue:in_flight", job_id)
    assert s1 is not None

    # Wait longer than the default heartbeat interval (~visibility_timeout/3 = 5s with timeout=15s).
    time.sleep(6.5)
    s2 = r.zscore("forgequeue:in_flight", job_id)
    assert s2 is not None

    assert s2 > s1, f"expected in-flight visible_at to increase, got s1={s1} s2={s2}"
