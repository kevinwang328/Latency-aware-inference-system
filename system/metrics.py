"""Process-local counters for accepted retries and successful retry completion."""

from prometheus_client import Counter

RETRY_SUBMISSIONS = Counter(
    "inference_retry_submissions_total",
    "Number of retry submissions accepted by another worker",
)

RETRY_COMPLETIONS = Counter(
    "inference_retry_completions_total",
    "Number of retry submissions completed successfully by another worker",
)
