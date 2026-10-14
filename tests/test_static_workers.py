"""Static routing must neither connect to ZooKeeper nor retry durable attempts."""
from concurrent import futures
import os
import unittest
from unittest.mock import patch

import grpc

from system import inference_pb2, inference_pb2_grpc
from system.grpc_worker_pool import GrpcWorker, GrpcWorkerPool
from system.task import Task, TaskStatus


class StaticWorkerTests(unittest.TestCase):
    def test_static_workers_env_bypasses_zookeeper_and_routes_real_rpc(self):
        class Echo(inference_pb2_grpc.WorkerServiceServicer):
            def ProcessBatch(self, request, context):
                return inference_pb2.BatchResponse(results=[inference_pb2.TaskResult(task_id=t.task_id, success=True, result_json='{"answer":9}') for t in request.tasks])
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(Echo(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        self.addCleanup(lambda: server.stop(0).wait())
        with patch.dict(os.environ, {"STATIC_WORKERS": '{"vast":"127.0.0.1:%d"}' % port}), patch("system.grpc_worker_pool.WorkerDiscovery", side_effect=AssertionError("ZooKeeper must not start")):
            pool = GrpcWorkerPool()
            pool.start()
            try:
                self.assertTrue(pool.has_workers())
                task = Task(input_data={"x": 3})
                pool._workers["vast"]._process([task])
                self.assertEqual(task.result, {"answer": 9})
            finally:
                pool.stop()

    def test_malformed_static_map_fails_before_starting_discovery(self):
        for value in ('[]', '{"bad":null}', '{"":"localhost:9000"}', 'not-json'):
            with self.subTest(value=value), patch.dict(os.environ, {"STATIC_WORKERS": value}):
                with self.assertRaises(ValueError):
                    GrpcWorkerPool()

    def test_durable_job_transport_failure_is_retryable_without_immediate_handoff(self):
        proxy = GrpcWorker("absent", "127.0.0.1:1", lambda *_: self.fail("durable attempts must not hand off"))
        try:
            task = Task(input_data={"kind": "synthx_v73", "spec": {}, "attempt_id": "test"})
            proxy._process([task])
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(task.error_code, "transport_error")
        finally:
            proxy.stop()

    def test_rpc_omitted_result_fails_durable_job_for_recovery(self):
        class Empty(inference_pb2_grpc.WorkerServiceServicer):
            def ProcessBatch(self, request, context):
                return inference_pb2.BatchResponse()
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(Empty(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        proxy = GrpcWorker("empty", f"127.0.0.1:{port}", lambda *_: False)
        try:
            task = Task(input_data={"kind": "synthx_v73"})
            proxy._process([task])
            self.assertEqual(task.status, TaskStatus.FAILED)
            self.assertEqual(task.error_code, "transport_error")
        finally:
            proxy.stop()
            server.stop(0).wait()


if __name__ == "__main__":
    unittest.main()
