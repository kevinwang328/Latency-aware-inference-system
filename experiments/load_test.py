"""
load_test.py
============
Load generator for benchmarking the inference API server.

This module implements concurrent HTTP load testing with precise latency measurement,
failure tolerance, and detailed logging for distributed systems experimentation.

Design:
- Uses thread pool for concurrent requests
- Measures latency from before send to after receive
- Graceful failure handling - continues on request errors
- No external dependencies beyond standard library + requests
- Thread-safe result collection

Usage:
    from load_test import LoadTester

    tester = LoadTester(
        url="http://localhost:8000/predict",
        request_rate=100,  # requests per second
        duration=10,  # seconds
        num_threads=4,  # concurrent client threads
        request_body={"x": 1},
    )
    logs = tester.run()
"""

import time
import threading
import uuid
import logging
from typing import List, Dict, Any, Optional
from collections import defaultdict
from queue import Queue, Empty
from datetime import datetime

import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from metrics import RequestLog

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class LoadTester:
    """
    Concurrent HTTP load generator with precise latency measurement.

    This class orchestrates the load testing process:
    1. Spawns worker threads to send requests
    2. Measures end-to-end latency for each request
    3. Collects results in thread-safe manner
    4. Handles failures gracefully

    Attributes:
        url: Target API endpoint (e.g., http://localhost:8000/predict)
        request_rate: Target requests per second
        duration: Test duration in seconds
        num_threads: Number of concurrent worker threads
        request_body: JSON payload to send in request body
    """

    def __init__(
        self,
        url: str,
        request_rate: float,
        duration: int,
        num_threads: int = 1,
        request_body: Dict[str, Any] = None,
        timeout: int = 30,
    ):
        """
        Initialize load tester.

        Args:
            url: API endpoint URL
            request_rate: Target requests per second (distributed across threads)
            duration: Test duration in seconds
            num_threads: Number of concurrent worker threads
            request_body: JSON payload for each request (default: {"x": 1})
            timeout: Request timeout in seconds

        Raises:
            ValueError: If invalid parameters provided
        """
        if request_rate <= 0:
            raise ValueError(f"request_rate must be > 0, got {request_rate}")
        if duration <= 0:
            raise ValueError(f"duration must be > 0, got {duration}")
        if num_threads <= 0:
            raise ValueError(f"num_threads must be > 0, got {num_threads}")

        self.url = url
        self.request_rate = request_rate
        self.duration = duration
        self.num_threads = num_threads
        self.timeout = timeout
        self.request_body = request_body or {"x": 1}

        # Thread-safe result collection
        self.logs: List[RequestLog] = []
        self.logs_lock = threading.Lock()

        # Shared state for workers
        self.stop_event = threading.Event()
        self.test_start_time = None
        self.test_end_time = None

        # Statistics for monitoring
        self.stats = {
            "requests_sent": 0,
            "requests_completed": 0,
            "requests_failed": 0,
        }
        self.stats_lock = threading.Lock()

    def _send_request_batch(self, rate_per_thread: float):
        """
        Worker thread function: Send requests at controlled rate.

        This function:
        1. Calculates inter-request delay based on rate and thread count
        2. Sends requests until stop_event is set
        3. Measures latency precisely (before send to after receive)
        4. Records all results in thread-safe manner
        5. Continues on errors without crashing

        Args:
            rate_per_thread: Requests per second for this thread
        """
        inter_request_delay = 1.0 / rate_per_thread if rate_per_thread > 0 else 0.0
        last_request_time = time.time()

        while not self.stop_event.is_set():
            # Rate limiting: wait until next request time
            now = time.time()
            time_since_last = now - last_request_time

            if time_since_last < inter_request_delay:
                sleep_time = inter_request_delay - time_since_last
                time.sleep(min(sleep_time, 0.01))  # Cap sleep to avoid busy-waiting
                continue

            # Send request with precise latency measurement
            request_id = str(uuid.uuid4())
            start_time = time.time()

            try:
                response = requests.post(
                    self.url,
                    json=self.request_body,
                    timeout=self.timeout,
                )
                end_time = time.time()

                # Check HTTP status code
                success = 200 <= response.status_code < 300
                latency_ms = (end_time - start_time) * 1000

                with self.logs_lock:
                    self.logs.append(
                        RequestLog(
                            request_id=request_id,
                            start_time=start_time,
                            end_time=end_time,
                            latency=latency_ms,
                            success=success,
                            error=None if success else f"HTTP {response.status_code}",
                        )
                    )

                    with self.stats_lock:
                        self.stats["requests_completed"] += 1
                        if not success:
                            self.stats["requests_failed"] += 1

            except Timeout:
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000

                with self.logs_lock:
                    self.logs.append(
                        RequestLog(
                            request_id=request_id,
                            start_time=start_time,
                            end_time=end_time,
                            latency=latency_ms,
                            success=False,
                            error="Timeout",
                        )
                    )

                    with self.stats_lock:
                        self.stats["requests_completed"] += 1
                        self.stats["requests_failed"] += 1

            except (ConnectionError, RequestException) as e:
                end_time = time.time()
                latency_ms = (end_time - start_time) * 1000

                with self.logs_lock:
                    self.logs.append(
                        RequestLog(
                            request_id=request_id,
                            start_time=start_time,
                            end_time=end_time,
                            latency=latency_ms,
                            success=False,
                            error=str(type(e).__name__),
                        )
                    )

                    with self.stats_lock:
                        self.stats["requests_completed"] += 1
                        self.stats["requests_failed"] += 1

            last_request_time = time.time()

            with self.stats_lock:
                self.stats["requests_sent"] += 1

    def run(self) -> List[RequestLog]:
        """
        Execute the load test.

        Returns:
            List of RequestLog objects containing all measurements

        Raises:
            ConnectionError: If unable to connect to API server
        """
        logger.info(f"Starting load test: {self.request_rate} req/s for {self.duration}s")
        logger.info(f"URL: {self.url}, Threads: {self.num_threads}")

        # Verify server is reachable
        try:
            response = requests.post(
                self.url,
                json=self.request_body,
                timeout=5,
            )
        except Exception as e:
            error_msg = (
                f"ERROR: Cannot reach API server at {self.url}\n"
                f"Details: {str(e)}\n"
                f"Please ensure the server is running on {self.url}"
            )
            logger.error(error_msg)
            raise ConnectionError(error_msg)

        self.test_start_time = time.time()
        self.stop_event.clear()

        # Calculate per-thread request rate
        rate_per_thread = self.request_rate / self.num_threads

        # Spawn worker threads
        threads = []
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self._send_request_batch,
                args=(rate_per_thread,),
                daemon=True,
                name=f"LoadTester-Worker-{i}",
            )
            threads.append(thread)
            thread.start()

        # Wait for duration
        time.sleep(self.duration)

        # Signal threads to stop
        self.stop_event.set()

        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=5)

        self.test_end_time = time.time()

        # Log summary
        logger.info(
            f"Load test completed. "
            f"Requests sent: {self.stats['requests_sent']}, "
            f"Completed: {self.stats['requests_completed']}, "
            f"Failed: {self.stats['requests_failed']}"
        )

        return self.logs

    def run_with_progress(self) -> List[RequestLog]:
        """
        Execute load test with progress reporting.

        Returns:
            List of RequestLog objects

        Note:
            Periodically logs progress for long-running tests
        """
        logger.info(f"Starting load test with progress tracking...")
        logger.info(f"Target: {self.request_rate} req/s for {self.duration}s")

        self.test_start_time = time.time()
        self.stop_event.clear()

        # Verify server is reachable
        try:
            requests.post(self.url, json=self.request_body, timeout=5)
        except Exception as e:
            error_msg = (
                f"ERROR: Cannot reach API server at {self.url}\n"
                f"Details: {str(e)}"
            )
            logger.error(error_msg)
            raise ConnectionError(error_msg)

        rate_per_thread = self.request_rate / self.num_threads

        # Spawn worker threads
        threads = []
        for i in range(self.num_threads):
            thread = threading.Thread(
                target=self._send_request_batch,
                args=(rate_per_thread,),
                daemon=True,
                name=f"LoadTester-Worker-{i}",
            )
            threads.append(thread)
            thread.start()

        # Monitor progress
        start_time = time.time()
        while time.time() - start_time < self.duration:
            remaining = self.duration - (time.time() - start_time)
            with self.stats_lock:
                stats_copy = self.stats.copy()

            logger.info(
                f"[{self.duration - remaining:.1f}s/{self.duration}s] "
                f"Sent: {stats_copy['requests_sent']}, "
                f"Completed: {stats_copy['requests_completed']}, "
                f"Failed: {stats_copy['requests_failed']}"
            )
            time.sleep(1)

        # Stop threads
        self.stop_event.set()

        # Wait for threads to complete
        for thread in threads:
            thread.join(timeout=5)

        self.test_end_time = time.time()

        # Final summary
        logger.info(
            f"Load test completed. "
            f"Total requests sent: {self.stats['requests_sent']}, "
            f"Completed: {self.stats['requests_completed']}, "
            f"Failed: {self.stats['requests_failed']}"
        )

        return self.logs


def run_simple_load_test(
    url: str,
    request_rate: float,
    duration: int,
    num_threads: int = 1,
) -> List[RequestLog]:
    """
    Convenience function for simple load testing.

    Args:
        url: API endpoint
        request_rate: Requests per second
        duration: Duration in seconds
        num_threads: Number of threads

    Returns:
        List of RequestLog objects
    """
    tester = LoadTester(
        url=url,
        request_rate=request_rate,
        duration=duration,
        num_threads=num_threads,
    )
    return tester.run()
