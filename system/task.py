"""
task.py
=======
Core Task data structure for the inference system.

A Task is the unit of work: it carries the client's input, tracks its
lifecycle (pending → processing → done/failed), and stores the result
so the API server can return it to the caller.
"""

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"


@dataclass
class Task:
    """
    Represents a single inference request travelling through the system.

    Attributes:
        task_id:       Unique identifier (UUID4 string).
        input_data:    Raw payload from the client (arbitrary dict).
        creation_time: Unix timestamp of when the task entered the queue.
        status:        Current lifecycle stage.
        result:        Inference output; populated by the worker on success.
        error:         Error message; populated by the worker on failure.
        start_time:    Unix timestamp when a worker began processing.
        end_time:      Unix timestamp when processing completed (success or fail).
    """
    input_data: Any
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    creation_time: float = field(default_factory=time.time)
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Any] = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @property
    def age_ms(self) -> float:
        """Milliseconds since the task was created."""
        return (time.time() - self.creation_time) * 1000

    @property
    def processing_latency_ms(self) -> Optional[float]:
        """Worker processing time in ms, or None if not yet complete."""
        if self.start_time is None or self.end_time is None:
            return None
        return (self.end_time - self.start_time) * 1000

    def mark_processing(self) -> None:
        self.status = TaskStatus.PROCESSING
        self.start_time = time.time()

    def mark_done(self, result: Any) -> None:
        self.status = TaskStatus.DONE
        self.result = result
        self.end_time = time.time()

    def mark_failed(self, error: str) -> None:
        self.status = TaskStatus.FAILED
        self.error = error
        self.end_time = time.time()

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "age_ms": round(self.age_ms, 2),
            "processing_latency_ms": (
                round(self.processing_latency_ms, 2)
                if self.processing_latency_ms is not None
                else None
            ),
        }
