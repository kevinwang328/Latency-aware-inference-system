"""Process-local counters and gauges for API and worker-proxy activity."""

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

WORKER_QUEUED_BATCHES = Gauge(
    "inference_worker_queued_batches",
    "Batches waiting in the API-side worker proxy, excluding the active batch",
    ["worker_id"],
)

WORKER_ACTIVE_RPCS = Gauge(
    "inference_worker_active_rpcs",
    "Inference RPC calls currently in flight from the API-side worker proxy",
    ["worker_id"],
)

WORKER_ROUTING_STATE = Gauge(
    "inference_worker_routing_state",
    "One when the API-side worker proxy has the indicated lifecycle state; zero otherwise",
    ["worker_id", "state"],
)
