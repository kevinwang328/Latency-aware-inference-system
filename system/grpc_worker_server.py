"""
grpc_worker_server.py
=====================
Standalone worker process that:
  1. Registers itself with ZooKeeper on startup.
  2. Serves inference requests via gRPC (WorkerService).
  3. Deregisters from ZooKeeper on shutdown.

Usage:
    python -m system.grpc_worker_server --worker-id 0 --port 50051
    python -m system.grpc_worker_server --worker-id 1 --port 50052
"""

import argparse
import json
import logging
import random
import time
from concurrent import futures

import grpc

from . import inference_pb2, inference_pb2_grpc
from .zookeeper_registry import WorkerRegistry

logger = logging.getLogger(__name__)

# Mutable worker-local config (updated via UpdateConfig RPC)
_failure_rate: float = 0.0
_inference_latency_ms: float = 60.0


class WorkerServicer(inference_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_id: int):
        self.worker_id = worker_id

    def ProcessBatch(self, request, context):
        global _failure_rate, _inference_latency_ms

        results = []

        # Failure injection
        if _failure_rate > 0 and random.random() < _failure_rate:
            err = f"Worker-{self.worker_id}: injected failure"
            logger.debug(err)
            for t in request.tasks:
                results.append(
                    inference_pb2.TaskResult(
                        task_id=t.task_id, success=False, error=err
                    )
                )
            return inference_pb2.BatchResponse(results=results)

        # Simulate inference latency
        n = len(request.tasks)
        total_ms = _inference_latency_ms + n * (_inference_latency_ms * 0.2)
        time.sleep(total_ms / 1000.0)

        for t in request.tasks:
            try:
                input_data = json.loads(t.input_json)
                x = input_data.get("x", 0) if isinstance(input_data, dict) else 0
                result = {"prediction": x * x, "worker_id": self.worker_id}
                results.append(
                    inference_pb2.TaskResult(
                        task_id=t.task_id,
                        success=True,
                        result_json=json.dumps(result),
                    )
                )
            except Exception as exc:
                results.append(
                    inference_pb2.TaskResult(
                        task_id=t.task_id, success=False, error=str(exc)
                    )
                )

        return inference_pb2.BatchResponse(results=results)

    def UpdateConfig(self, request, context):
        global _failure_rate, _inference_latency_ms
        _failure_rate = request.failure_rate
        _inference_latency_ms = request.inference_latency_ms
        logger.info(
            "Config updated: failure_rate=%.2f inference_latency_ms=%.1f",
            _failure_rate,
            _inference_latency_ms,
        )
        return inference_pb2.ConfigAck(ok=True)


def serve(worker_id: int, port: int, zk_hosts: str) -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    inference_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerServicer(worker_id), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info("Worker-%d gRPC server listening on port %d", worker_id, port)

    registry = WorkerRegistry(zk_hosts)
    registry.register(worker_id, f"localhost:{port}")

    try:
        server.wait_for_termination()
    finally:
        registry.deregister()
        logger.info("Worker-%d shut down", worker_id)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="gRPC inference worker")
    parser.add_argument("--worker-id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--zk-hosts", default="localhost:2181")
    args = parser.parse_args()
    serve(args.worker_id, args.port, args.zk_hosts)
