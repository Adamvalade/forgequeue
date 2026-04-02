import time
import uuid

import pytest
import requests

from helpers import create_job, get_job, wait_for_status


def test_root_lists_service_links(api):
    resp = requests.get(f"{api}/", timeout=5)
    resp.raise_for_status()
    body = resp.json()
    assert body.get("service") == "ForgeQueue"
    assert body.get("docs") == "/docs"
    assert body.get("health") == "/health"
    assert body.get("demo") == "/demo/"
    assert body.get("system") == "/system"


def test_demo_page_served(api):
    resp = requests.get(f"{api}/demo/", timeout=5)
    resp.raise_for_status()
    assert "ForgeQueue" in resp.text
    assert "Fault-tolerant" in resp.text


def test_system_endpoint(api):
    resp = requests.get(f"{api}/system", timeout=5)
    resp.raise_for_status()
    body = resp.json()
    assert "api_uptime_seconds" in body
    assert body["workers"]["registered_workers"] >= 1
    t = body["totals"]
    assert "jobs_enqueued" in t
    assert "jobs_completed" in t
    assert "jobs_retried" in t


def test_demo_config_json(api):
    resp = requests.get(f"{api}/demo/config.json", timeout=5)
    resp.raise_for_status()
    data = resp.json()
    assert data.get("github_url", "").startswith("http")


def test_crash_sim_reclaimed_then_succeeds(api, r):
    job_id = create_job(
        api,
        "crash_sim",
        {"simulated_visibility_sec": 8, "buffer_sec": 1},
        max_attempts=2,
    )
    final = wait_for_status(api, job_id, {"succeeded"}, timeout_s=60)
    assert int(final["attempts"]) >= 2


def test_demo_redirects_to_trailing_slash(api):
    resp = requests.get(f"{api}/demo", allow_redirects=False, timeout=5)
    assert resp.status_code in (301, 307, 308)
    loc = resp.headers.get("location", "")
    assert loc.endswith("/demo/") or loc.rstrip("/").endswith("demo")


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


def test_health_and_ready_endpoints(api, r):
    # /health should be 200 as long as Redis is reachable.
    resp = requests.get(f"{api}/health", timeout=5)
    resp.raise_for_status()
    data = resp.json()
    assert data.get("status") == "ok"
    assert data.get("redis") == "ok"

    # /ready requires Redis + worker heartbeat. r fixture flushes Redis so worker
    # must re-register (heartbeat interval up to 15s); allow enough time.
    deadline = time.time() + 35
    last_status = None
    while time.time() < deadline:
        resp2 = requests.get(f"{api}/ready", timeout=5)
        last_status = resp2.status_code
        if resp2.status_code == 200:
            body = resp2.json()
            assert body.get("status") == "ok"
            assert body.get("redis") == "ok"
            assert isinstance(body.get("workers"), int)
            assert body["workers"] >= 1
            break
        time.sleep(0.5)
    else:
        pytest.fail(f"/ready did not become ready (last_status={last_status})")


def test_worker_heartbeat_key_exists(api, r):
    # Worker should register itself with a heartbeat key carrying a TTL.
    # Note: the `r` fixture flushes Redis per test, so we must wait for the worker
    # to re-register its heartbeat key.
    deadline = time.time() + 35
    key = None
    ttl = None
    while time.time() < deadline:
        keys = r.keys("forgequeue:worker:hb:*")
        if keys:
            key = keys[0]
            ttl = r.ttl(key)
            if ttl and ttl > 0:
                break
        time.sleep(0.5)
    assert key is not None, "expected at least one worker heartbeat key"
    assert ttl is not None and ttl > 0, f"expected heartbeat TTL > 0, got ttl={ttl}"


def test_metrics_expose_counters_and_gauges(api, r):
    # Create jobs to exercise success, retry, and DLQ paths.
    j_sleep = create_job(api, "sleep", {"ms": 10}, max_attempts=1)
    j_fail_once = create_job(api, "fail_once", {}, max_attempts=2)
    j_always_fail = create_job(api, "always_fail", {}, max_attempts=2)

    wait_for_status(api, j_sleep, {"succeeded"}, timeout_s=20)
    wait_for_status(api, j_fail_once, {"succeeded"}, timeout_s=40)
    wait_for_status(api, j_always_fail, {"failed"}, timeout_s=40)

    # Give worker a moment to flush metrics to Redis.
    time.sleep(1)

    resp = requests.get(f"{api}/metrics", timeout=5)
    resp.raise_for_status()
    text = resp.text

    # Gauges should be present.
    assert "queue_depth" in text
    assert "in_flight_count" in text
    assert "dlq_depth" in text

    # Per-type counters for enqueued/completed/failed/retried/dlq_added.
    assert 'jobs_enqueued_total{type="sleep"}' in text
    assert 'jobs_enqueued_total{type="fail_once"}' in text
    assert 'jobs_enqueued_total{type="always_fail"}' in text

    assert 'jobs_completed_total{type="sleep"}' in text
    assert 'jobs_completed_total{type="fail_once"}' in text

    assert 'jobs_failed_total{type="always_fail"}' in text

    # fail_once should have at least one retry.
    assert 'jobs_retried_total{type="fail_once"}' in text

    # always_fail should land in DLQ.
    assert 'dlq_added_total{type="always_fail"}' in text


def test_metrics_include_histogram_for_job_durations(api, r):
    # Run a few sleep jobs to populate the duration histogram for type="sleep".
    job_ids = [
        create_job(api, "sleep", {"ms": 10}, max_attempts=1),
        create_job(api, "sleep", {"ms": 100}, max_attempts=1),
        create_job(api, "sleep", {"ms": 1000}, max_attempts=1),
    ]
    for jid in job_ids:
        wait_for_status(api, jid, {"succeeded"}, timeout_s=30)

    # Histogram keys are written by the worker; poll /metrics until it shows up to avoid flakiness.
    deadline = time.time() + 35
    text = ""
    while time.time() < deadline:
        resp = requests.get(f"{api}/metrics", timeout=5)
        resp.raise_for_status()
        text = resp.text
        lines = text.splitlines()
        has_count = any('job_processing_duration_seconds_count' in ln and 'type="sleep"' in ln for ln in lines)
        has_sum = any('job_processing_duration_seconds_sum' in ln and 'type="sleep"' in ln for ln in lines)
        has_bucket = any(
            'job_processing_duration_seconds_bucket' in ln
            and 'type="sleep"' in ln
            and ('le="0.1"' in ln or 'le="0.5"' in ln or 'le="+Inf"' in ln)
            for ln in lines
        )

        if has_count and has_sum and has_bucket:
            break
        time.sleep(0.5)
    else:
        pytest.fail("expected sleep duration histogram to appear in /metrics")
