"""
grpc_worker_pool.py
===================
GrpcWorkerPool discovers live workers from ZooKeeper and dispatches
inference batches to them via gRPC.

Each remote worker is wrapped in a GrpcWorker object that owns a
dedicated thread + internal queue, mirroring the design of the original
thread-based Worker.  Batches submitted to the pool are round-robined
across idle workers exactly as before.
"""

import json
import logging
import threading
from typing import Dict, List, Optional

import grpc

from . import inference_pb2, inference_pb2_grpc
from .task import Task
from .zookeeper_registry import WorkerDiscovery

logger = logging.getLogger(__name__)

Batch = List[Task]


class GrpcWorker:
    """
    Client-side proxy for one remote gRPC worker process.
    Maintains its own queue + thread so submit() is non-blocking,
    matching the behaviour of the original Worker class.
    """

    def __init__(self, worker_id: str, address: str):
        self.worker_id = worker_id
        self.address = address
        self._channel = grpc.insecure_channel(address)
        self._stub = inference_pb2_grpc.WorkerServiceStub(self._channel)
        self._batch_queue: List[Batch] = []
        self._lock = threading.Lock()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"GrpcWorker-{worker_id}"
        )
        self._thread.start()

    def submit(self, batch: Batch) -> None:
        with self._lock:
            self._batch_queue.append(batch)
        self._has_work.set()

    def busy(self) -> bool:
        with self._lock:
            return bool(self._batch_queue)

    def stop(self) -> None:
        self._stop.set()
        self._has_work.set()
        self._thread.join(timeout=3)
        self._channel.close()

    def update_config(self, failure_rate: float, inference_latency_ms: float) -> None:
        try:
            self._stub.UpdateConfig(
                inference_pb2.ConfigUpdate(
                    failure_rate=failure_rate,
                    inference_latency_ms=inference_latency_ms,
                ),
                timeout=5,
            )
        except Exception as exc:
            logger.warning("UpdateConfig failed for %s: %s", self.worker_id, exc)

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

        task_map = {task.task_id: task for task in batch}
        requests = [
            inference_pb2.TaskRequest(
                task_id=task.task_id,
                input_json=json.dumps(task.input_data),
            )
            for task in batch
        ]

        try:
            response = self._stub.ProcessBatch(
                inference_pb2.BatchRequest(tasks=requests),
                timeout=35,
            )
            for result in response.results:
                task = task_map.get(result.task_id)
                if task is None:
                    continue
                if result.success:
                    task.mark_done(json.loads(result.result_json))
                else:
                    task.mark_failed(result.error)
        except Exception as exc:
            logger.error("gRPC call failed: %s", exc)
            for task in batch:
                task.mark_failed(str(exc))


class GrpcWorkerPool:
    """
    Worker pool that discovers live workers from ZooKeeper and routes
    batches to them via gRPC.  Worker join/leave is handled automatically
    through ZooKeeper watches — no manual reconfiguration needed.
    """

    def __init__(self, zk_hosts: str = "localhost:2181"):
        self._zk_hosts = zk_hosts
        self._workers: Dict[str, GrpcWorker] = {}
        self._lock = threading.Lock()
        self._rr_index = 0
        self._discovery: Optional[WorkerDiscovery] = None

    def start(self) -> None:
        self._discovery = WorkerDiscovery(
            zk_hosts=self._zk_hosts,
            on_change=self._update_workers,
        )
        initial = self._discovery.get_workers()
        self._update_workers(initial)
        logger.info("GrpcWorkerPool started with %d workers", len(self._workers))

    def stop(self) -> None:
        if self._discovery:
            self._discovery.stop()
        with self._lock:
            for w in self._workers.values():
                w.stop()
        logger.info("GrpcWorkerPool stopped")

    def push_config(self, failure_rate: float, inference_latency_ms: float) -> None:
        with self._lock:
            workers = list(self._workers.values())
        for w in workers:
            w.update_config(failure_rate, inference_latency_ms)

    def submit(self, batch: Batch) -> None:
        with self._lock:
            if not self._workers:
                for task in batch:
                    task.mark_failed("no workers available")
                return
            idle = [w for w in self._workers.values() if not w.busy()]
            pool = idle if idle else list(self._workers.values())
            worker = pool[self._rr_index % len(pool)]
            self._rr_index += 1

        worker.submit(batch)

    def _update_workers(self, workers: Dict[str, str]) -> None:
        with self._lock:
            # Connect to new workers
            for wid, addr in workers.items():
                if wid not in self._workers:
                    self._workers[wid] = GrpcWorker(wid, addr)
                    logger.info("Connected to %s at %s", wid, addr)
            # Disconnect from gone workers
            gone = [wid for wid in self._workers if wid not in workers]
            for wid in gone:
                self._workers[wid].stop()
                del self._workers[wid]
                logger.info("Worker %s removed", wid)
