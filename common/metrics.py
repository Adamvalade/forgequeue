from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)

from common.constants import (
    DLQ_ZSET_KEY,
    IN_FLIGHT_KEY,
    QUEUE_KEY,
    METRIC_DLQ_ADDED_TOTAL,
    METRIC_JOBS_COMPLETED_TOTAL,
    METRIC_JOBS_ENQUEUED_TOTAL,
    METRIC_JOBS_FAILED_TOTAL,
    METRIC_JOBS_RETRIED_TOTAL,
    METRIC_JOB_DURATION_SECONDS,
)


DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)


def _bucket_le_strings(buckets: Iterable[float]) -> list[str]:
    # Prometheus expects `le` label as a string.
    return [str(b) for b in buckets] + ["+Inf"]


@dataclass(frozen=True)
class RedisMetricsCollector:
    """
    Prometheus collector that reads metrics from Redis so that API can serve /metrics
    as a single view across API + worker processes.
    """

    redis: any
    duration_buckets: tuple[float, ...] = DEFAULT_DURATION_BUCKETS

    def collect(self):
        # Gauges (computed at scrape time)
        g_queue = GaugeMetricFamily("queue_depth", "Current queue depth")
        g_in_flight = GaugeMetricFamily("in_flight_count", "Current in-flight job count")
        g_dlq = GaugeMetricFamily("dlq_depth", "Current DLQ depth (zset)")

        try:
            g_queue.add_metric([], float(self.redis.llen(QUEUE_KEY)))
            g_in_flight.add_metric([], float(self.redis.zcard(IN_FLIGHT_KEY)))
            g_dlq.add_metric([], float(self.redis.zcard(DLQ_ZSET_KEY)))
        except Exception:
            # If Redis is unavailable, expose no gauge values (scrape will still succeed,
            # but health/readiness should surface the issue).
            pass

        yield g_queue
        yield g_in_flight
        yield g_dlq

        # Counters (Redis holds monotonically increasing totals)
        c_enq = CounterMetricFamily(
            "jobs_enqueued_total", "Total jobs enqueued", labels=["type"]
        )
        c_done = CounterMetricFamily(
            "jobs_completed_total", "Total jobs completed", labels=["type"]
        )
        c_fail = CounterMetricFamily(
            "jobs_failed_total", "Total jobs failed", labels=["type"]
        )
        c_retry = CounterMetricFamily(
            "jobs_retried_total", "Total job retries scheduled", labels=["type"]
        )
        c_dlq = CounterMetricFamily(
            "dlq_added_total", "Total jobs added to DLQ", labels=["type"]
        )

        for base_key, fam in (
            (METRIC_JOBS_ENQUEUED_TOTAL, c_enq),
            (METRIC_JOBS_COMPLETED_TOTAL, c_done),
            (METRIC_JOBS_FAILED_TOTAL, c_fail),
            (METRIC_JOBS_RETRIED_TOTAL, c_retry),
            (METRIC_DLQ_ADDED_TOTAL, c_dlq),
        ):
            try:
                for key in self.redis.scan_iter(match=base_key + ":*"):
                    # key format: <base>:<type>
                    parts = key.split(":")
                    job_type = parts[-1] if parts else "unknown"
                    val = self.redis.get(key)
                    fam.add_metric([job_type], float(val or 0))
            except Exception:
                # If Redis is down, skip.
                pass

        yield c_enq
        yield c_done
        yield c_fail
        yield c_retry
        yield c_dlq

        # Histogram (Redis-backed)
        # Prometheus histogram is exposed as:
        # - _bucket{le="..."} cumulative counts
        # - _sum
        # - _count
        h = HistogramMetricFamily(
            "job_processing_duration_seconds",
            "Job processing duration in seconds",
            labels=["type"],
        )

        le_labels = _bucket_le_strings(self.duration_buckets)

        try:
            # Discover job types present in histogram keys.
            types: set[str] = set()
            for key in self.redis.scan_iter(match=METRIC_JOB_DURATION_SECONDS + ":count:*"):
                # key format: <base>:count:<type>
                types.add(key.split(":")[-1])

            for job_type in sorted(types):
                count = int(self.redis.get(f"{METRIC_JOB_DURATION_SECONDS}:count:{job_type}") or 0)
                sum_ = float(self.redis.get(f"{METRIC_JOB_DURATION_SECONDS}:sum:{job_type}") or 0.0)

                # Buckets stored as *cumulative* counts so we can map directly.
                buckets: list[tuple[str, float]] = []
                for le_str in le_labels:
                    bkey = f"{METRIC_JOB_DURATION_SECONDS}:bucket:{job_type}:{le_str}"
                    buckets.append((le_str, float(self.redis.get(bkey) or 0)))

                # prometheus_client's HistogramMetricFamily.add_metric expects positional args:
                # add_metric(labels, buckets, sum_value, count_value)
                h.add_metric([job_type], buckets, sum_, count)
        except Exception:
            pass

        yield h

