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
from typing import Callable, Dict, List, Optional

import grpc

from . import inference_pb2, inference_pb2_grpc
from .task import Task, TaskStatus
from .zookeeper_registry import WorkerDiscovery
from .metrics import RETRY_COMPLETIONS, RETRY_SUBMISSIONS, WORKER_COUNT
logger = logging.getLogger(__name__)

Batch = List[Task]


class GrpcWorker:
    """
    Client-side proxy for one remote gRPC worker process.
    Maintains its own queue + thread so submit() is non-blocking,
    matching the behaviour of the original Worker class.
    """

    def __init__(self, worker_id: str, address: str,
                 retry_handler: Callable[[Task, str], bool]):
        """Create a channel and start the serial dispatch thread for one remote worker."""
        self.worker_id = worker_id
        self.address = address
        self._retry_handler = retry_handler
        self._channel = grpc.insecure_channel(address)
        self._stub = inference_pb2_grpc.WorkerServiceStub(self._channel)
        self._batch_queue: List[Batch] = []
        self._lock = threading.Lock()
        self._has_work = threading.Event()
        self._stop = threading.Event()
        self._retry_on_stop = False
        self._processing = False
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name=f"GrpcWorker-{worker_id}"
        )
        self._thread.start()

    def submit(self, batch: Batch) -> None:
        """Enqueue a batch or mark its tasks overloaded if the proxy rejects it."""
        if not self.try_submit(batch):
            for task in batch:
                task.mark_failed("worker unavailable or at capacity", "overloaded")

    def try_submit(self, batch: Batch) -> bool:
        """Atomically accept a batch only when running and below two outstanding batches."""
        with self._lock:
            if self._stop.is_set():
                return False
            outstanding = self._processing + len(self._batch_queue)
            if outstanding >= 2:
                return False

            self._batch_queue.append(batch)
        self._has_work.set()
        return True

    def busy(self) -> bool:
        """Report whether this proxy has an active RPC or a queued batch."""
        with self._lock:
            return self._processing or bool(self._batch_queue)

    def begin_stop(self, retry_pending: bool = False) -> None:
        """Reject new submissions before the pool releases its membership lock."""
        with self._lock:
            if not self._stop.is_set():
                self._retry_on_stop = retry_pending
                self._stop.set()
        self._has_work.set()

    def stop(self, retry_pending: bool = False) -> None:
        """Cancel the active RPC and hand off queued tasks only during worker removal."""
        self.begin_stop(retry_pending)
        with self._lock:
            pending = self._batch_queue
            self._batch_queue = []
        # Cancel the active RPC promptly; its exception handler transfers its
        # own tasks. Never wait for that handler while holding the pool lock.
        self._channel.close()
        for batch in pending:
            for task in batch:
                self._finish_or_retry(task, "worker removed", self._retry_on_stop)
        if threading.current_thread() is not self._thread:
            self._thread.join(timeout=3)

    def _finish_or_retry(self, task: Task, error: str, retry_allowed: bool) -> None:
        """Preserve terminal states, enforce deadlines, and attempt eligible handoff."""
        if task.status in (TaskStatus.DONE, TaskStatus.FAILED):
            return
        if task.is_expired():
            task.mark_failed("request deadline exceeded", "deadline_exceeded")
            return
        if retry_allowed:
            try:
                if self._retry_handler(task, self.worker_id):
                    return
            except Exception:
                logger.exception("Retry dispatch failed for task %s", task.task_id)
        task.mark_failed(error)

    def update_config(self, failure_rate: float, inference_latency_ms: float) -> None:
        """Send runtime simulation settings to the remote worker."""
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
        """Dispatch queued batches serially without holding the queue lock during RPCs."""
        while not self._stop.is_set():
            self._has_work.wait(timeout=0.1)
            self._has_work.clear()
            while True:
                with self._lock:
                    if self._stop.is_set() or not self._batch_queue:
                        break
                    batch = self._batch_queue.pop(0)
                    self._processing = True

                try:
                    self._process(batch)
                finally:
                    with self._lock:
                        self._processing = False

    def _process(self, batch: Batch) -> None:
        """Send live tasks with their budgets and reconcile results or eligible RPC failures."""
        active_batch = []
        for task in batch:
            if task.status in (TaskStatus.DONE, TaskStatus.FAILED):

                continue
            if task.is_expired():
                task.mark_failed("request deadline exceeded", error_code="deadline_exceeded")
            else:
                active_batch.append(task)

        if not active_batch:
            return

        batch = active_batch

        if self._stop.is_set():
            for task in batch:
                self._finish_or_retry(task, "worker removed", self._retry_on_stop)
            return

        for task in batch:
            task.mark_processing()

        task_map = {task.task_id: task for task in batch}
        requests = []
        remaining_times = []
        for task in batch:
            request = inference_pb2.TaskRequest(
                task_id=task.task_id,
                input_json=json.dumps(task.input_data),
            )

            remaining_time = task.remaining_seconds()

            if remaining_time is not None:
                request.remaining_seconds = remaining_time
                remaining_times.append(remaining_time)

            requests.append(request)
        rpc_timeout = max(remaining_times) if remaining_times else 35.0

        if rpc_timeout <= 0:
            for task in batch:
                task.mark_failed(
                    "request deadline exceeded",
                    error_code="deadline_exceeded",
                )
            return
        try:
            response = self._stub.ProcessBatch(
                inference_pb2.BatchRequest(tasks=requests),
                timeout=rpc_timeout,
            )
            for result in response.results:
                task = task_map.get(result.task_id)
                if task is None:
                    continue
                if result.success:
                    task.mark_done(json.loads(result.result_json))
                    if task.retry_count and task.status == TaskStatus.DONE:
                        RETRY_COMPLETIONS.inc()
                        logger.info("Completed retried task %s on %s",
                                    task.task_id, self.worker_id)
                else:
                    task.mark_failed(result.error, error_code=result.error_code or None)

        except grpc.RpcError as exc:
            logger.warning(
                "Worker %s RPC failed: %s",
                self.worker_id,
                exc.code(),
            )

            if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                for task in batch:
                    task.mark_failed(
                        "request deadline exceeded",
                        error_code="deadline_exceeded",
                    )
                return

            retiring = self._stop.is_set() and self._retry_on_stop
            retry_allowed = (
                exc.code() == grpc.StatusCode.UNAVAILABLE
                and (not self._stop.is_set() or retiring)
            ) or (exc.code() == grpc.StatusCode.CANCELLED and retiring)
            for task in batch:
                self._finish_or_retry(task, str(exc), retry_allowed)

        except Exception as exc:
            logger.error("gRPC call failed: %s", exc)
            for task in batch:
                # Removal may close the channel between the stop check and
                # invocation, producing a local error instead of RpcError.
                self._finish_or_retry(
                    task, str(exc),
                    self._stop.is_set() and self._retry_on_stop
                    and isinstance(exc, ValueError),
                )


class GrpcWorkerPool:
    """
    Worker pool that discovers live workers from ZooKeeper and routes
    batches to them via gRPC.  Worker join/leave is handled automatically
    through ZooKeeper watches — no manual reconfiguration needed.
    """

    def __init__(self, zk_hosts: str = "localhost:2181"):
        """Initialize membership and routing state; discovery starts in start()."""
        self._zk_hosts = zk_hosts
        self._workers: Dict[str, GrpcWorker] = {}
        self._lock = threading.Lock()
        self._rr_index = 0
        self._discovery: Optional[WorkerDiscovery] = None
        self._stopping = False

    def start(self) -> None:
        """Watch ZooKeeper membership and create proxies for registered workers."""
        self._discovery = WorkerDiscovery(
            zk_hosts=self._zk_hosts,
            on_change=self._update_workers,
        )
        initial = self._discovery.get_workers()
        self._update_workers(initial)
        logger.info("GrpcWorkerPool started with %d workers", len(self._workers))

    def stop(self) -> None:
        """Reject retries, stop discovery, and close proxies outside the membership lock."""
        with self._lock:
            self._stopping = True
            workers = list(self._workers.values())
            self._workers.clear()
            WORKER_COUNT.set(0)
            for worker in workers:
                worker.begin_stop()
        if self._discovery:
            self._discovery.stop()
        for worker in workers:
            worker.stop()
        logger.info("GrpcWorkerPool stopped")

    def push_config(self, failure_rate: float, inference_latency_ms: float) -> None:
        """Snapshot membership before sending configuration RPCs outside the pool lock."""
        with self._lock:
            workers = list(self._workers.values())
        for w in workers:
            w.update_config(failure_rate, inference_latency_ms)

    def submit(self, batch: Batch) -> None:
        """Prefer idle proxies, then try available capacity before rejecting a batch."""
        with self._lock:
            if not self._workers:
                for task in batch:
                    task.mark_failed("no workers available")
                return
            idle = [w for w in self._workers.values() if not w.busy()]
            pool = idle if idle else list(self._workers.values())
            worker = pool[self._rr_index % len(pool)]
            self._rr_index += 1

        if worker.try_submit(batch):
            return
        else:
            with self._lock:
                for w in self._workers.values():
                    if w is worker:
                        continue
                    if w.try_submit(batch):
                        return
            for task in batch:
                task.mark_failed(
                    "all workers at capacity",
                    error_code="overloaded",
                )



    def _update_workers(self, workers: Dict[str, str]) -> None:
        """Apply membership changes and retire removed proxies outside the pool lock."""
        removed = []
        with self._lock:
            if self._stopping:
                return
            # Add proxies before removing stale members so handoff sees current candidates.
            for wid, addr in workers.items():
                if wid not in self._workers:
                    self._workers[wid] = GrpcWorker(wid, addr, self.try_resubmit)
                    logger.info("Connected to %s at %s", wid, addr)
            # Mark removed proxies stopped before releasing the membership lock.
            gone = [wid for wid in self._workers if wid not in workers]
            for wid in gone:
                worker = self._workers.pop(wid)
                worker.begin_stop(retry_pending=True)
                removed.append(worker)
                logger.info("Worker %s removed", wid)
            WORKER_COUNT.set(len(self._workers))
        for worker in removed:
            worker.stop(retry_pending=True)


    def try_resubmit(self, task: Task, failed_worker_id: str) -> bool:
        """Try one handoff to a different worker, preserving the original task deadline."""
        with self._lock:
            if self._stopping:
                return False
            candidates = [
                w for w in self._workers.values()
                if w.worker_id != failed_worker_id
            ]
        if not candidates:
            return False

        if not task.try_reserve_retry():
            return False

        idle = []
        busy = []

        for w in candidates:
            if w.busy():
                busy.append(w)
            else:
                idle.append(w)

        for w in idle + busy:
            if w.try_submit([task]):
                RETRY_SUBMISSIONS.inc()
                logger.info("Retrying task %s from %s to %s (retry=%d)",
                            task.task_id, failed_worker_id, w.worker_id, task.retry_count)

                return True
        return False
