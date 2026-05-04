"""
metrics.py
==========
Metrics calculation module for latency-aware inference system experiments.

This module computes comprehensive performance metrics from raw request logs,
following engineering best practices for distributed systems measurement.

Key Metrics:
- Request counts (total, completed, failed)
- Latency percentiles (P50, P95, P99, avg)
- Throughput
- Failure rates
- Timestamps for reproducibility

Note: We use numpy for efficient percentile calculation when available,
falling back to pure Python for better portability.
"""

import statistics
from typing import List, Dict, Any
from datetime import datetime

# Try to import numpy for efficient percentile calculation
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


class RequestLog:
    """
    Represents a single request in the system.

    Attributes:
        request_id: Unique identifier for the request
        start_time: Unix timestamp when request was sent
        end_time: Unix timestamp when response was received
        latency: Response time in milliseconds
        success: Boolean indicating whether request succeeded
        error: Error message if request failed (None if successful)
    """
    def __init__(
        self,
        request_id: str,
        start_time: float,
        end_time: float,
        latency: float,
        success: bool,
        error: str = None
    ):
        self.request_id = request_id
        self.start_time = start_time
        self.end_time = end_time
        self.latency = latency
        self.success = success
        self.error = error


class ExperimentMetrics:
    """
    Computes comprehensive metrics from a collection of request logs.

    This class handles:
    - Percentile calculations (P50, P95, P99)
    - Throughput computation
    - Failure rate analysis
    - Timestamp recording for reproducibility

    Engineering Notes:
    - Tolerates empty or failed-only request logs gracefully
    - Separates calculation from I/O for better testability
    - Uses robust percentile calculation (nearest_rank method)
    """

    def __init__(self, logs: List[RequestLog], experiment_name: str = ""):
        """
        Initialize metrics calculator.

        Args:
            logs: List of RequestLog objects
            experiment_name: Name of the experiment (for reporting)
        """
        self.logs = logs
        self.experiment_name = experiment_name
        self.timestamp = datetime.utcnow().isoformat()

    def _calculate_percentile(self, values: List[float], percentile: float) -> float:
        """
        Calculate percentile using nearest_rank method.

        Args:
            values: Sorted list of values
            percentile: Percentile to calculate (0-100)

        Returns:
            Percentile value
        """
        if not values:
            return 0.0

        if HAS_NUMPY:
            return float(np.percentile(values, percentile))
        else:
            # Pure Python implementation using nearest_rank method
            sorted_values = sorted(values)
            rank = int((percentile / 100.0) * len(sorted_values))
            # Clamp rank to valid range
            rank = max(0, min(rank, len(sorted_values) - 1))
            return float(sorted_values[rank])

    def get_metrics(self) -> Dict[str, Any]:
        """
        Calculate all metrics from logs.

        Returns:
            Dictionary containing all computed metrics
        """
        if not self.logs:
            return self._empty_metrics()

        total_requests = len(self.logs)
        completed_requests = sum(1 for log in self.logs if log.success)
        failed_requests = total_requests - completed_requests

        # Calculate latencies for successful requests only
        successful_latencies = [log.latency for log in self.logs if log.success]

        if not successful_latencies:
            return self._failed_metrics(total_requests, failed_requests)

        # Calculate latency percentiles
        sorted_latencies = sorted(successful_latencies)
        avg_latency = statistics.mean(successful_latencies)
        p50_latency = self._calculate_percentile(sorted_latencies, 50)
        p95_latency = self._calculate_percentile(sorted_latencies, 95)
        p99_latency = self._calculate_percentile(sorted_latencies, 99)

        # Calculate throughput (requests per second)
        if self.logs:
            min_time = min(log.start_time for log in self.logs)
            max_time = max(log.end_time for log in self.logs)
            duration_seconds = max_time - min_time

            if duration_seconds > 0:
                throughput = completed_requests / duration_seconds
            else:
                # All requests completed instantly (test scenario)
                throughput = float(completed_requests) if duration_seconds == 0 else 0.0
        else:
            throughput = 0.0

        failure_rate = failed_requests / total_requests if total_requests > 0 else 0.0

        return {
            "total_requests": total_requests,
            "completed_requests": completed_requests,
            "failed_requests": failed_requests,
            "avg_latency": round(avg_latency, 2),
            "p50_latency": round(p50_latency, 2),
            "p95_latency": round(p95_latency, 2),
            "p99_latency": round(p99_latency, 2),
            "throughput": round(throughput, 2),
            "failure_rate": round(failure_rate, 4),
            "timestamp": self.timestamp,
        }

    def _empty_metrics(self) -> Dict[str, Any]:
        """Return zero metrics when no logs present."""
        return {
            "total_requests": 0,
            "completed_requests": 0,
            "failed_requests": 0,
            "avg_latency": 0.0,
            "p50_latency": 0.0,
            "p95_latency": 0.0,
            "p99_latency": 0.0,
            "throughput": 0.0,
            "failure_rate": 0.0,
            "timestamp": self.timestamp,
        }

    def _failed_metrics(self, total: int, failed: int) -> Dict[str, Any]:
        """Return metrics when all requests failed."""
        return {
            "total_requests": total,
            "completed_requests": 0,
            "failed_requests": failed,
            "avg_latency": 0.0,
            "p50_latency": 0.0,
            "p95_latency": 0.0,
            "p99_latency": 0.0,
            "throughput": 0.0,
            "failure_rate": 1.0,
            "timestamp": self.timestamp,
        }


def calculate_metrics_from_logs(logs: List[RequestLog], name: str = "") -> Dict[str, Any]:
    """
    Convenience function to calculate metrics from logs.

    Args:
        logs: List of RequestLog objects
        name: Experiment name

    Returns:
        Dictionary of metrics
    """
    calculator = ExperimentMetrics(logs, name)
    return calculator.get_metrics()
