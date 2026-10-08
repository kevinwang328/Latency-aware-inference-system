"""
api_server.py
=============
FastAPI server for the latency-aware inference system.

Endpoints:
  POST /predict                 – Submit an inference request (blocks until done)
  POST /config/batch_size       – Change batch size at runtime
  POST /config/scheduler        – Switch scheduler strategy at runtime
  POST /config/failure_rate     – Inject worker failures at runtime
  GET  /status                  – System health and queue stats

Usage:
    python -m system.api_server
    # or
    uvicorn system.api_server:app --host 0.0.0.0 --port 8000
"""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from typing import Any
from queue import Full

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from fastapi import Request, Response
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

import os

from .config import config
from .queue import TaskQueue
from .scheduler import Scheduler
from .task import Task
from .worker import WorkerPool
from system import task

USE_GRPC = os.environ.get("USE_GRPC", "0") == "1"
if USE_GRPC:
    from .grpc_worker_pool import GrpcWorkerPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Application state (initialised in lifespan)
# ---------------------------------------------------------------------------

_task_queue: TaskQueue
_worker_pool = None
_scheduler: Scheduler


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start the queue, workers, and scheduler; stop them when the API shuts down."""
    global _task_queue, _worker_pool, _scheduler

    _task_queue = TaskQueue(maxsize=config.max_queue_size)
    if USE_GRPC:
        _worker_pool = GrpcWorkerPool(zk_hosts=os.environ.get("ZK_HOSTS", "localhost:2181"))
    else:
        _worker_pool = WorkerPool()
    _worker_pool.start()

    _scheduler = Scheduler(_task_queue, _worker_pool.submit)
    _scheduler.start()

    logger.info("System started — host=%s port=%d", config.host, config.port)
    yield

    _scheduler.stop()
    _worker_pool.stop()
    logger.info("System shutdown complete")


app = FastAPI(
    title="Latency-Aware Inference System",
    version="1.0.0",
    lifespan=lifespan,
)

PREDICT_REQUESTS = Counter(
    "predict_requests_total",
    "Total prediction requests",
)

PREDICT_FAILURES = Counter(
    "predict_failures_total",
    "Prediction requests that failed",
)

PREDICT_RESPONSES = Counter(
    "predict_responses_total",
    "Completed prediction requests by HTTP status code",
    ["status_code"],
)
for status_code in ("200", "422", "500", "503", "504"):
    PREDICT_RESPONSES.labels(status_code=status_code)

PREDICT_DURATION = Histogram(
    "predict_duration_seconds",
    "Prediction request duration in seconds",
    ["status_code"],
    buckets=(0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60),
)
for status_code in ("200", "422", "500", "503", "504"):
    PREDICT_DURATION.labels(status_code=status_code)


@app.middleware("http")
async def record_metrics(request: Request, call_next):
    # Exclude monitoring traffic from prediction request metrics.
    """Count prediction responses and observe their full HTTP request duration."""
    if request.method != "POST" or request.url.path != "/predict":
        return await call_next(request)

    PREDICT_REQUESTS.inc()
    start = time.perf_counter()
    status_code = "500"

    try:
        response = await call_next(request)
    except Exception:
        PREDICT_FAILURES.inc()
        raise
    else:
        status_code = str(response.status_code)
        if response.status_code >= 400:
            PREDICT_FAILURES.inc()
        return response
    finally:
        PREDICT_RESPONSES.labels(status_code=status_code).inc()
        PREDICT_DURATION.labels(status_code=status_code).observe(
            time.perf_counter() - start
        )


@app.get("/metrics", include_in_schema=False)
async def metrics():
    """Expose the process-local Prometheus registry in text format."""
    return Response(
        content=generate_latest(),
        headers={"Content-Type": CONTENT_TYPE_LATEST},
    )

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class PredictRequest(BaseModel):
    x: Any = Field(..., description="Input value for inference")


class PredictResponse(BaseModel):
    task_id: str
    result: Any
    latency_ms: float


class BatchSizeConfig(BaseModel):
    batch_size: int = Field(..., ge=1, le=512)


class SchedulerConfig(BaseModel):
    scheduler: str = Field(..., pattern="^(fifo|batching|latency_aware)$")


class FailureRateConfig(BaseModel):
    failure_rate: float = Field(..., ge=0.0, le=1.0)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/predict", response_model=PredictResponse)
async def predict(request: PredictRequest):
    """
    Submit an inference request and wait synchronously for the result.

    The client blocks here until a worker completes the task (or fails it).
    This mirrors typical synchronous inference APIs used in benchmarks.
    """
    task = Task(
        deadline=time.monotonic() + 30.0,
        input_data={"x": request.x}
    )

    try:
        _task_queue.put(task, timeout=0.0)
    except Full:
        raise HTTPException(status_code=503, detail="task queue is full")

    # Poll for completion (workers signal via task.status)
    while not task.is_expired():
        if task.status.value in ("done", "failed"):
            break
        await asyncio.sleep(0.001)

    if task.is_expired():
        task.mark_failed(
            "request deadline exceeded",
            error_code="deadline_exceeded",
        )

    if task.status.value == "failed":
        if task.error_code == "deadline_exceeded":
            status_code = 504
        elif task.error_code == "overloaded":
            status_code = 503
        else:
            status_code = 500

        raise HTTPException(
            status_code=status_code,
            detail=task.error or "inference failed",
        )

    if task.status.value != "done":
        raise HTTPException(status_code=504, detail="inference timed out")

    return PredictResponse(
        task_id=task.task_id,
        result=task.result,
        latency_ms=round((task.end_time - task.creation_time) * 1000, 2),
    )


@app.post("/config/batch_size")
async def set_batch_size(body: BatchSizeConfig):
    """Set the maximum number of tasks assembled into a scheduler batch."""
    config.update(batch_size=body.batch_size)
    logger.info("batch_size updated to %d", body.batch_size)
    return {"status": "ok", "batch_size": config.batch_size}


@app.post("/config/scheduler")
async def set_scheduler(body: SchedulerConfig):
    """Select the strategy used on the next scheduler iteration."""
    config.update(scheduler_strategy=body.scheduler)
    logger.info("scheduler_strategy updated to %s", body.scheduler)
    return {"status": "ok", "scheduler": config.scheduler_strategy}


@app.post("/config/failure_rate")
async def set_failure_rate(body: FailureRateConfig):
    """Update failure injection locally and push it to connected gRPC workers."""
    config.update(failure_rate=body.failure_rate)
    _worker_pool.push_config(config.failure_rate, config.inference_latency_ms)
    logger.info("failure_rate updated to %.3f", body.failure_rate)
    return {"status": "ok", "failure_rate": config.failure_rate}


@app.get("/status")
async def status():
    """Return configuration and API queue statistics; this is not a readiness probe."""
    return {
        "status": "running",
        "config": config.snapshot(),
        "queue": _task_queue.stats,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "system.api_server:app",
        host=config.host,
        port=config.port,
        log_level="info",
    )
