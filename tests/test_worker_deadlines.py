"""Verify simulated execution budgets and the client-side RPC timeout mapping."""

from concurrent import futures
import time
import unittest
from unittest.mock import patch

import grpc

from system import inference_pb2, inference_pb2_grpc
from system import grpc_worker_server
from system.grpc_worker_pool import GrpcWorker
from system.task import Task, TaskStatus


class Context:
    """Provide a deterministic cancellation deadline for direct service calls."""

    def __init__(self, cancel_after=None):
        self.started = time.monotonic()
        self.cancel_after = cancel_after

    def is_active(self):
        return (self.cancel_after is None
                or time.monotonic() - self.started < self.cancel_after)


class DeadlineTests(unittest.TestCase):
    def setUp(self):
        failure = patch.object(grpc_worker_server, "_failure_rate", 0)
        latency = patch.object(grpc_worker_server, "_inference_latency_ms", 100)
        failure.start()
        latency.start()
        self.addCleanup(failure.stop)
        self.addCleanup(latency.stop)
        self.worker = grpc_worker_server.WorkerServicer(7)

    def request(self, identity, budget=None):
        task = inference_pb2.TaskRequest(task_id=identity, input_json='{"x":3}')
        if budget is not None:
            task.remaining_seconds = budget
        return task

    def call(self, tasks, context=None):
        return self.worker.ProcessBatch(
            inference_pb2.BatchRequest(tasks=tasks), context or Context()
        )

    def test_already_expired_task_has_one_timeout_result(self):
        results = self.call([self.request("expired", 0)]).results
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0].success)
        self.assertEqual(results[0].error_code, "deadline_exceeded")

    def test_mixed_deadlines_preserve_the_unexpired_task(self):
        response = self.call([self.request("short", .02), self.request("long", 1)])
        results = {result.task_id: result for result in response.results}
        self.assertEqual(len(response.results), 2)
        self.assertEqual(results["short"].error_code, "deadline_exceeded")
        self.assertTrue(results["long"].success)

    def test_task_without_a_budget_completes(self):
        self.assertTrue(self.call([self.request("unlimited")]).results[0].success)

    def test_all_expired_tasks_end_the_simulated_wait_early(self):
        with patch.object(grpc_worker_server, "_inference_latency_ms", 1000):
            started = time.monotonic()
            result = self.call([self.request("short", .03)]).results[0]
            self.assertLess(time.monotonic() - started, .5)
            self.assertEqual(result.error_code, "deadline_exceeded")

    def test_rpc_cancellation_ends_the_simulated_wait_early(self):
        with patch.object(grpc_worker_server, "_inference_latency_ms", 1000):
            started = time.monotonic()
            self.call([self.request("long", 5)], Context(.03))
            self.assertLess(time.monotonic() - started, .5)

    def test_rpc_timeout_marks_failure_without_retry(self):
        class SlowWorker(grpc_worker_server.WorkerServicer):
            def ProcessBatch(self, request, context):
                time.sleep(.2)
                return inference_pb2.BatchResponse()

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(SlowWorker(1), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        retries = []
        proxy = GrpcWorker("test", f"127.0.0.1:{port}",
                           lambda *args: retries.append(args) or False)
        try:
            task = Task(input_data={"x": 3}, deadline=time.monotonic() + .05)
            proxy._process([task])
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(task.error_code, "deadline_exceeded")
            self.assertEqual(retries, [])
        finally:
            proxy.stop()
            server.stop(0).wait()


if __name__ == "__main__":
    unittest.main()
