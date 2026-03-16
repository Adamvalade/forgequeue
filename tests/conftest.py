"""Shared pytest fixtures for e2e and reliability tests."""
import os
import time

import pytest
import redis
import requests

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
API_URL = os.getenv("API_URL", "http://localhost:8000")
VISIBILITY_TIMEOUT_SECONDS = int(os.getenv("VISIBILITY_TIMEOUT_SECONDS", "15"))


@pytest.fixture()
def r():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    client.ping()
    client.flushdb()
    return client


@pytest.fixture()
def api(r):
    """Depends on r so Redis is flushed before we wait for API (deterministic order)."""
    deadline = time.time() + 15
    last_err = None
    while time.time() < deadline:
        try:
            resp = requests.get(f"{API_URL}/stats", timeout=2)
            if resp.status_code == 200:
                return API_URL
            last_err = f"status={resp.status_code} body={resp.text}"
        except Exception as e:
            last_err = repr(e)
        time.sleep(0.5)
    raise RuntimeError(f"API not ready: {last_err}")
