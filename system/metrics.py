"""Process-local retry counters and gauges for membership and the API queue."""

from prometheus_client import Counter, Gauge

RETRY_SUBMISSIONS = Counter(
    "inference_retry_submissions_total",
    "Number of retry submissions accepted by another worker",
)

RETRY_COMPLETIONS = Counter(
    "inference_retry_completions_total",
    "Number of retry submissions completed successfully by another worker",
)

WORKER_COUNT = Gauge(
    "inference_registered_workers",
    "Number of workers currently discovered by the API",
)

QUEUE_DEPTH = Gauge(
    "inference_api_queue_depth",
    "Number of tasks waiting in the API queue",
)
