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

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import os

from .config import config
from .queue import TaskQueue
from .scheduler import Scheduler
from .task import Task
from .worker import WorkerPool

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
    global _task_queue, _worker_pool, _scheduler

    _task_queue = TaskQueue(maxsize=config.max_queue_size)
    if USE_GRPC:
        _worker_pool = GrpcWorkerPool(zk_hosts="localhost:2181")
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
    task = Task(input_data={"x": request.x})

    try:
        _task_queue.put(task, timeout=5.0)
    except Exception:
        raise HTTPException(status_code=503, detail="Queue is full — try again later")

    # Poll for completion (workers signal via task.status)
    deadline = time.time() + 30.0
    while time.time() < deadline:
        if task.status.value in ("done", "failed"):
            break
        await asyncio.sleep(0.001)

    if task.status.value == "failed":
        raise HTTPException(status_code=500, detail=task.error or "inference failed")

    if task.status.value != "done":
        raise HTTPException(status_code=504, detail="inference timed out")

    return PredictResponse(
        task_id=task.task_id,
        result=task.result,
        latency_ms=round((task.end_time - task.creation_time) * 1000, 2),
    )


@app.post("/config/batch_size")
async def set_batch_size(body: BatchSizeConfig):
    config.update(batch_size=body.batch_size)
    logger.info("batch_size updated to %d", body.batch_size)
    return {"status": "ok", "batch_size": config.batch_size}


@app.post("/config/scheduler")
async def set_scheduler(body: SchedulerConfig):
    config.update(scheduler_strategy=body.scheduler)
    logger.info("scheduler_strategy updated to %s", body.scheduler)
    return {"status": "ok", "scheduler": config.scheduler_strategy}


@app.post("/config/failure_rate")
async def set_failure_rate(body: FailureRateConfig):
    config.update(failure_rate=body.failure_rate)
    _worker_pool.push_config(config.failure_rate, config.inference_latency_ms)
    logger.info("failure_rate updated to %.3f", body.failure_rate)
    return {"status": "ok", "failure_rate": config.failure_rate}


@app.get("/status")
async def status():
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
