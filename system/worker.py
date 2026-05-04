"""
worker.py
=========
Worker pool for processing inference batches.

Each Worker runs in its own thread and simulates model inference with a
configurable per-item latency.  A failure_rate in (0, 1] causes the worker
to raise an exception on that fraction of batches, enabling the failure
injection experiments.

The WorkerPool manages a fixed number of workers and exposes a single
submit(batch) method that the scheduler calls.
"""

import logging
import random
import threading
import time
from typing import List

from .config import config
from .task import Task

logger = logging.getLogger(__name__)

Batch = List[Task]


class Worker:
    """
    Processes one batch at a time in a dedicated thread.

    Inference is simulated by sleeping for
    `config.inference_latency_ms * len(batch)` milliseconds.
    The result for each task is `{"prediction": input_data["x"] * 2}`.
    """

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self._batch_queue: List[Batch] = []
        self._lock = threading.Lock()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"Worker-{worker_id}"
        )
        self._processed = 0

    def start(self) -> None:
        self._stop.clear()
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._has_work.set()
        self._thread.join(timeout=timeout)

    def submit(self, batch: Batch) -> None:
        with self._lock:
            self._batch_queue.append(batch)
        self._has_work.set()

    def busy(self) -> bool:
        with self._lock:
            return bool(self._batch_queue)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._has_work.wait(timeout=0.1)
            self._has_work.clear()

            while True:
                with self._lock:
                    if not self._batch_queue:
                        break
                    batch = self._batch_queue.pop(0)
                self._process(batch)

    def _process(self, batch: Batch) -> None:
        for task in batch:
            task.mark_processing()

        # Simulate failure injection
        if config.failure_rate > 0 and random.random() < config.failure_rate:
            error_msg = f"Worker-{self.worker_id}: injected failure"
            logger.debug(error_msg)
            for task in batch:
                task.mark_failed(error_msg)
            return

        # Fixed overhead (GPU setup) + small per-item cost
        # This makes larger batches more efficient, like real ML inference
        per_item_ms = config.inference_latency_ms * 0.05
        total_ms = config.inference_latency_ms + len(batch) * per_item_ms
        time.sleep(total_ms / 1000.0)

        for task in batch:
            try:
                x = task.input_data.get("x", 0) if isinstance(task.input_data, dict) else 0
                task.mark_done({"prediction": x * x, "worker_id": self.worker_id})
            except Exception as exc:
                task.mark_failed(str(exc))

        self._processed += len(batch)


class WorkerPool:
    """
    Fixed-size pool of Worker instances.

    submit(batch) routes the batch to the least-busy worker using a simple
    round-robin; if all workers are busy the current worker still accepts
    it (workers queue internally).
    """

    def __init__(self):
        self._workers: List[Worker] = []
        self._rr_index = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        n = config.num_workers
        self._workers = [Worker(i) for i in range(n)]
        for w in self._workers:
            w.start()
        logger.info("WorkerPool started with %d workers", n)

    def stop(self, timeout: float = 5.0) -> None:
        for w in self._workers:
            w.stop(timeout=timeout)
        logger.info("WorkerPool stopped")

    def submit(self, batch: Batch) -> None:
        if not self._workers:
            for task in batch:
                task.mark_failed("no workers available")
            return

        with self._lock:
            # Round-robin across idle workers; fall back to all workers
            idle = [w for w in self._workers if not w.busy()]
            pool = idle if idle else self._workers
            worker = pool[self._rr_index % len(pool)]
            self._rr_index += 1

        worker.submit(batch)
