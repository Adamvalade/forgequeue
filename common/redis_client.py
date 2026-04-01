import os
import ssl

import redis


def get_redis():
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    kwargs: dict = {"decode_responses": True}
    # Managed providers (e.g. Upstash) often use rediss://; set REDIS_SSL_CERT_REQS_NONE=1 if TLS verify fails.
    if url.startswith("rediss://") and os.getenv("REDIS_SSL_CERT_REQS_NONE", "").lower() in (
        "1",
        "true",
        "yes",
    ):
        kwargs["ssl_cert_reqs"] = ssl.CERT_NONE
    return redis.Redis.from_url(url, **kwargs)
