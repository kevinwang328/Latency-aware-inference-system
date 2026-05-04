"""
scheduler.py
============
Scheduler strategies for the latency-aware inference system.

Three strategies ship out of the box:

  fifo          – Process tasks strictly in arrival order.
  batching      – Collect up to batch_size tasks before dispatching.
  latency_aware – Prioritise old tasks to bound tail latency (P99).

The Scheduler class wraps all three behind a single interface so the
API server can switch strategies at runtime via /config/scheduler.
"""

import logging
import threading
import time
from typing import List

from .config import config
from .queue import TaskQueue
from .task import Task

logger = logging.getLogger(__name__)

Batch = List[Task]


# ---------------------------------------------------------------------------
# Strategy implementations
# ---------------------------------------------------------------------------

class FIFOScheduler:
    """
    Emit one batch per tick containing up to batch_size tasks in
    the order they arrived.
    """

    def next_batch(self, queue: TaskQueue) -> Batch:
        batch: Batch = []
        while len(batch) < config.batch_size:
            task = queue.get(timeout=0.05)
            if task is None:
                break
            batch.append(task)
        return batch


class BatchingScheduler:
    """
    Block until a full batch is available (or a short timeout expires),
    then return the batch.  Maximises GPU utilisation at the cost of
    added queueing latency.
    """
    _FILL_TIMEOUT = 0.1  # seconds to wait for a full batch

    def next_batch(self, queue: TaskQueue) -> Batch:
        deadline = time.time() + self._FILL_TIMEOUT
        batch: Batch = []

        while len(batch) < config.batch_size and time.time() < deadline:
            task = queue.get(timeout=0.01)
            if task is not None:
                batch.append(task)

        return batch


class LatencyAwareScheduler:
    """
    Prioritise tasks that have been waiting longer than
    config.latency_threshold_ms.  Uses a local buffer so tasks are never
    re-enqueued (which would push them behind newly arriving tasks and cause
    starvation).  The buffer is always sorted oldest-first; stale tasks at
    the front are dispatched before fresh ones.
    """

    def __init__(self):
        self._buffer: List[Task] = []

    def next_batch(self, queue: TaskQueue) -> Batch:
        # Keep buffer capped: only drain enough to top it up to batch_size*4
        cap = config.batch_size * 4
        if len(self._buffer) < cap:
            new_tasks = queue.drain(max_items=cap - len(self._buffer))
            self._buffer.extend(new_tasks)

        if not self._buffer:
            time.sleep(0.01)
            return []

        threshold = config.latency_threshold_ms
        old = [t for t in self._buffer if t.age_ms >= threshold]
        fresh = [t for t in self._buffer if t.age_ms < threshold]

        old.sort(key=lambda t: t.creation_time)
        fresh.sort(key=lambda t: t.creation_time)

        batch = (old + fresh)[: config.batch_size]

        batch_ids = {id(t) for t in batch}
        self._buffer = [t for t in self._buffer if id(t) not in batch_ids]

        return batch


# ---------------------------------------------------------------------------
# Unified Scheduler facade
# ---------------------------------------------------------------------------

class Scheduler:
    """
    Runs a background loop that continuously pulls tasks from the queue and
    forwards batches to a registered batch_handler callable.

    The active strategy is re-read from config on every tick, so live
    reconfiguration via /config/scheduler takes effect immediately.
    """

    def __init__(self, queue: TaskQueue, batch_handler):
        self._queue = queue
        self._handler = batch_handler
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        self._strategies = {
            "fifo": FIFOScheduler(),
            "batching": BatchingScheduler(),
            "latency_aware": LatencyAwareScheduler(),
        }

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name="Scheduler"
        )
        self._thread.start()
        logger.info("Scheduler started")

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        logger.info("Scheduler stopped")

    def _loop(self) -> None:
        while not self._stop.is_set():
            strategy = self._strategies.get(config.scheduler_strategy)
            if strategy is None:
                logger.warning("Unknown scheduler strategy: %s", config.scheduler_strategy)
                time.sleep(0.1)
                continue

            batch = strategy.next_batch(self._queue)
            if batch:
                try:
                    self._handler(batch)
                except Exception:
                    logger.exception("batch_handler raised an exception")
