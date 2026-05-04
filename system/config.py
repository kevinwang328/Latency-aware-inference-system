"""
config.py
=========
System-wide configuration for the latency-aware inference system.
All tuneable parameters live here so experiments can override them
via the /config/* API endpoints without touching worker or scheduler code.
"""

import threading
from dataclasses import dataclass, field
from typing import Literal

SchedulerStrategy = Literal["fifo", "batching", "latency_aware"]


@dataclass
class SystemConfig:
    """
    Runtime-mutable configuration shared across server, scheduler, and workers.

    All fields are read/written under a single lock so that live reconfiguration
    from experiment HTTP calls is race-free.
    """
    # Inference settings
    batch_size: int = 4
    num_workers: int = 4
    inference_latency_ms: float = 60.0   # simulated per-item cost

    # Scheduling strategy: "fifo", "batching", or "latency_aware"
    scheduler_strategy: SchedulerStrategy = "fifo"

    # Failure injection (0.0 = never fail, 1.0 = always fail)
    failure_rate: float = 0.0

    # Latency-aware scheduler threshold: tasks older than this are expedited
    latency_threshold_ms: float = 50.0

    # Queue
    max_queue_size: int = 10_000

    # Server
    host: str = "0.0.0.0"
    port: int = 8000

    # Internal lock — not part of the dataclass value
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False, compare=False)

    def update(self, **kwargs) -> None:
        with self._lock:
            for key, value in kwargs.items():
                if not hasattr(self, key):
                    raise ValueError(f"Unknown config key: {key}")
                setattr(self, key, value)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "batch_size": self.batch_size,
                "num_workers": self.num_workers,
                "inference_latency_ms": self.inference_latency_ms,
                "scheduler_strategy": self.scheduler_strategy,
                "failure_rate": self.failure_rate,
                "latency_threshold_ms": self.latency_threshold_ms,
                "max_queue_size": self.max_queue_size,
                "host": self.host,
                "port": self.port,
            }


# Module-level singleton shared by all components
config = SystemConfig()
