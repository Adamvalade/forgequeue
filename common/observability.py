import json
import logging
import os
import sys
import time
from typing import Any, Optional


def configure_logging(service: str) -> logging.Logger:
    """
    Minimal structured logging to stdout.
    - Always emits one JSON object per line
    - Uses `event` as the primary message field
    """
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(service)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(level)
        # We'll format JSON ourselves; keep formatter simple.
        h.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(h)

    return logger


def log_event(
    logger: logging.Logger,
    *,
    event: str,
    job_id: Optional[str] = None,
    worker_id: Optional[str] = None,
    duration: Optional[float] = None,
    error: Optional[str] = None,
    **fields: Any,
) -> None:
    payload: dict[str, Any] = {
        "ts": time.time(),
        "event": event,
    }
    if job_id is not None:
        payload["job_id"] = job_id
    if worker_id is not None:
        payload["worker_id"] = worker_id
    if duration is not None:
        payload["duration"] = duration
    if error is not None:
        payload["error"] = error
    payload.update(fields)

    # Avoid exceptions on non-serializable types; stringify as last resort.
    try:
        logger.info(json.dumps(payload, separators=(",", ":"), sort_keys=True))
    except TypeError:
        safe = {k: (v if isinstance(v, (str, int, float, bool)) or v is None else str(v)) for k, v in payload.items()}
        logger.info(json.dumps(safe, separators=(",", ":"), sort_keys=True))

