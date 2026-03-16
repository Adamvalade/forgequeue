"""Shared helpers for e2e and reliability tests."""
import os
import time

import requests

API_URL = os.getenv("API_URL", "http://localhost:8000")
VISIBILITY_TIMEOUT_SECONDS = int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", "15"))


def create_job(
    api_url: str,
    job_type: str,
    payload: dict | None = None,
    max_attempts: int = 2,
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
    return resp.json()["job_id"]


def get_job(api_url: str, job_id: str, allow_404: bool = False):
    resp = requests.get(f"{api_url}/jobs/{job_id}", timeout=5)
    if allow_404 and resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def wait_for_status(
    api_url: str, job_id: str, target: set[str], timeout_s: float = 30
):
    """Poll job status until it is in target. Retries on 404 a few times (eventual consistency after flush)."""
    deadline = time.time() + timeout_s
    last = None
    retries_404 = 3
    while time.time() < deadline:
        try:
            last = get_job(api_url, job_id)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404 and retries_404 > 0:
                retries_404 -= 1
                time.sleep(0.5)
                continue
            raise
        if last and last.get("status") in target:
            return last
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {target}. Last={last}")
