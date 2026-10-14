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
import os
import signal
import threading
from concurrent import futures

import grpc

from . import inference_pb2, inference_pb2_grpc
from .zookeeper_registry import WorkerRegistry

logger = logging.getLogger(__name__)

# Mutable worker-local config (updated via UpdateConfig RPC)
_failure_rate: float = 0.0
_inference_latency_ms: float = 60.0


class WorkerServicer(inference_pb2_grpc.WorkerServiceServicer):
    def __init__(self, worker_id: int, backend=None):
        """Associate inference results with this worker identity."""
        self.worker_id = worker_id
        self.backend = backend
        if self.backend is None and os.environ.get('INFERENCE_BACKEND') == 'v7_3_full':
            from .worker_v73 import V73Worker
            self.backend = V73Worker()

    def ProcessBatch(self, request, context):
        """Simulate batch inference while dropping expired tasks and observing RPC cancellation."""
        if self.backend is not None:
            return self._process_v73(request, context)
        global _failure_rate, _inference_latency_ms

        results = []
        recieved_time = time.monotonic()
        deadlines = {}
        for t in request.tasks:
            if t.HasField("remaining_seconds"):
                deadlines[t.task_id] = recieved_time + t.remaining_seconds
            else:
                deadlines[t.task_id] = None

        active_tasks = []

        for t in request.tasks:
            deadline = deadlines[t.task_id]

            if deadline is not None and time.monotonic() >= deadline:
                results.append(
                    inference_pb2.TaskResult(
                        task_id=t.task_id,
                        success=False,
                        error="request deadline exceeded",
                        error_code="deadline_exceeded",
                    )
                )
            else:
                active_tasks.append(t)

        if not active_tasks:
            return inference_pb2.BatchResponse(results=results)

        # Failure injection
        if _failure_rate > 0 and random.random() < _failure_rate:
            err = f"Worker-{self.worker_id}: injected failure"
            logger.debug(err)
            for t in active_tasks:
                results.append(
                    inference_pb2.TaskResult(
                        task_id=t.task_id, success=False, error=err
                    )
                )
            return inference_pb2.BatchResponse(results=results)

        # Simulate inference latency
        n = len(active_tasks)
        total_ms = _inference_latency_ms + n * (_inference_latency_ms * 0.2)
        finish_at = time.monotonic() + total_ms / 1000.0

        while True:
            # Stop simulated work when the caller cancels or the RPC deadline expires.
            if not context.is_active():
                return inference_pb2.BatchResponse(results=results)

            now = time.monotonic()
            remaining_tasks = []

            for t in active_tasks:
                deadline = deadlines[t.task_id]

                if deadline is not None and now >= deadline:
                    results.append(
                        inference_pb2.TaskResult(
                            task_id=t.task_id,
                            success=False,
                            error="request deadline exceeded",
                            error_code="deadline_exceeded",
                        )
                    )
                else:
                    remaining_tasks.append(t)

            active_tasks = remaining_tasks

            if not active_tasks:
                return inference_pb2.BatchResponse(results=results)

            remaining_wait = finish_at - time.monotonic()
            if remaining_wait <= 0:
                break

            time.sleep(min(0.01, remaining_wait))

        for t in active_tasks:
            deadline = deadlines[t.task_id]

            if deadline is not None and time.monotonic() >= deadline:
                results.append(
                    inference_pb2.TaskResult(
                        task_id=t.task_id,
                        success=False,
                        error="request deadline exceeded",
                        error_code="deadline_exceeded",
                    )
                )
                continue

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

    def _process_v73(self, request, context):
        from .worker_v73 import WorkerError
        received = time.monotonic()
        results = []
        for task in request.tasks:
            try:
                try:
                    payload = json.loads(task.input_json)
                except (ValueError, TypeError) as exc:
                    raise WorkerError('invalid_spec', 'input_json must be valid JSON') from exc
                deadline = received + task.remaining_seconds if task.HasField('remaining_seconds') else None
                result = self.backend.process(payload, context, deadline=deadline)
                results.append(inference_pb2.TaskResult(task_id=task.task_id, success=True,
                                                       result_json=json.dumps(result)))
            except WorkerError as exc:
                results.append(inference_pb2.TaskResult(task_id=task.task_id, success=False,
                                                       error=str(exc), error_code=exc.code))
            except Exception:
                logger.exception('Real model worker failed for task %s', task.task_id)
                results.append(inference_pb2.TaskResult(task_id=task.task_id, success=False,
                                                       error='unexpected worker failure', error_code='worker_error'))
        return inference_pb2.BatchResponse(results=results)

    def UpdateConfig(self, request, context):
        """Replace this worker process's failure and latency simulation settings."""
        global _failure_rate, _inference_latency_ms
        _failure_rate = request.failure_rate
        _inference_latency_ms = request.inference_latency_ms
        logger.info(
            "Config updated: failure_rate=%.2f inference_latency_ms=%.1f",
            _failure_rate,
            _inference_latency_ms,
        )
        return inference_pb2.ConfigAck(ok=True)


def serve(worker_id: int, port: int, zk_hosts: str, register: bool = True, bind: str = None) -> None:
    """Start the gRPC service, register its address, and release registration on shutdown."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[
        ('grpc.max_send_message_length', 16 * 1024 * 1024),
        ('grpc.max_receive_message_length', 16 * 1024 * 1024),
    ])
    inference_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerServicer(worker_id), server
    )
    host = bind or os.environ.get('WORKER_BIND_ADDR') or (
        '127.0.0.1' if os.environ.get('INFERENCE_BACKEND') == 'v7_3_full' else '[::]')
    if not server.add_insecure_port(f"{host}:{port}"):
        raise RuntimeError('worker failed to bind its gRPC listener')
    server.start()
    logger.info("Worker-%d gRPC server listening on port %d", worker_id, port)

    registry = WorkerRegistry(zk_hosts) if register else None
    worker_address = os.environ.get(
        "WORKER_ADVERTISE_ADDR",
        f"localhost:{port}",
    )
    if registry is not None:
        registry.register(worker_id, worker_address)
    shutdown_event = threading.Event()

    def request_shutdown(signum, frame):
        shutdown_event.set()

    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)

    def publish_draining():
        if registry is None:
            return
        try:
            registry.set_state("DRAINING")
        except Exception:
            logger.exception("Failed to publish draining state")

    try:
        shutdown_event.wait()  # Wait for shutdown signal
        shutdown_deadline = time.monotonic() + 30
        publisher = threading.Thread(
            target=publish_draining,
            daemon=True,
            name="PublishDraining",
        )

        publisher.start()
        publisher.join(timeout=2)

        if publisher.is_alive():
            logger.warning("Draining publication is still pending")

        remaining = max(0.0, shutdown_deadline - time.monotonic())
        stopped = server.stop(grace=remaining)
        stopped.wait()

    finally:
        if registry is not None:
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
    parser.add_argument("--no-zookeeper", action="store_true", help="Serve a statically routed worker")
    parser.add_argument("--bind", default=None, help="Listener host (real model defaults to loopback)")
    args = parser.parse_args()
    serve(args.worker_id, args.port, args.zk_hosts, register=not args.no_zookeeper, bind=args.bind)
