"""
queue.py
========
Thread-safe task queue for the inference system.

The TaskQueue sits between the API server (producers) and the scheduler
(consumer). It blocks when full so back-pressure propagates naturally to
HTTP clients without any explicit throttle loop.
"""

import threading
import time
from queue import Queue, Full, Empty
from typing import List, Optional

from .task import Task


class TaskQueue:
    """
    Bounded FIFO queue with blocking put/get and bulk-drain support.

    The scheduler calls drain() to collect all currently available tasks
    in one shot, which avoids repeated lock acquisitions during batch
    assembly.
    """

    def __init__(self, maxsize: int = 10_000):
        self._q: Queue = Queue(maxsize=maxsize)
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._lock = threading.Lock()

    def put(self, task: Task, timeout: float = 5.0) -> None:
        """
        Add a task to the queue.

        Raises:
            Full: If the queue is at capacity after `timeout` seconds.
        """
        self._q.put(task, block=True, timeout=timeout)
        with self._lock:
            self._total_enqueued += 1

    def get(self, timeout: float = 1.0) -> Optional[Task]:
        """
        Remove and return the next task.

        Returns None on timeout instead of raising Empty, so callers can
        loop cleanly with a stop-event check.
        """
        try:
            task = self._q.get(block=True, timeout=timeout)
            with self._lock:
                self._total_dequeued += 1
            return task
        except Empty:
            return None

    def drain(self, max_items: int = 256) -> List[Task]:
        """
        Non-blocking bulk retrieval: return up to `max_items` tasks that
        are already in the queue right now.

        Used by the scheduler to assemble batches without blocking.
        """
        items: List[Task] = []
        for _ in range(max_items):
            try:
                items.append(self._q.get_nowait())
            except Empty:
                break
        with self._lock:
            self._total_dequeued += len(items)
        return items

    def qsize(self) -> int:
        return self._q.qsize()

    def empty(self) -> bool:
        return self._q.empty()

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "queue_size": self._q.qsize(),
                "total_enqueued": self._total_enqueued,
                "total_dequeued": self._total_dequeued,
            }
