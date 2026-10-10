"""Exercise SIGTERM using real worker subprocesses and gRPC calls."""

import json
import signal
import socket
import subprocess
import sys
import time
import unittest

import grpc
from system import inference_pb2 as pb, inference_pb2_grpc as rpc


CHILD = r'''
import signal, sys, threading
from system import grpc_worker_server as worker
mode, port = sys.argv[1], int(sys.argv[2])
released = threading.Event()
class Registry:
    def __init__(self, hosts): pass
    def register(self, wid, address): pass
    def set_state(self, state):
        assert state == "DRAINING", state
        print(state, flush=True)
        if mode == "blocked":
            released.wait()
        elif mode == "failed":
            raise RuntimeError("injected publication failure")
    def deregister(self):
        released.set()
        print("DEREGISTERED", flush=True)
worker.WorkerRegistry = Registry
worker._inference_latency_ms = 1000
original_signal = signal.signal
def install_signal(signum, handler):
    result = original_signal(signum, handler)
    if signum == signal.SIGINT:
        print("READY", flush=True)
    return result
worker.signal.signal = install_signal
worker.serve(99, port, "unused")
'''


class WorkerShutdownTests(unittest.TestCase):
    def start_worker(self, mode):
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1', 0))
            port = reserved.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, '-u', '-c', CHILD, mode, str(port)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
        self.addCleanup(cleanup)
        line = process.stdout.readline().strip()
        self.assertEqual(line, 'READY')
        channel = grpc.insecure_channel(f'127.0.0.1:{port}')
        self.addCleanup(channel.close)
        grpc.channel_ready_future(channel).result(timeout=5)
        return process, rpc.WorkerServiceStub(channel)

    def assert_clean_exit(self, process):
        output, errors = process.communicate(timeout=6)
        self.assertEqual(process.returncode, 0, errors)
        self.assertIn('DRAINING', output)
        self.assertIn('DEREGISTERED', output)
        return errors

    def test_sigterm_finishes_active_rpc_then_deregisters(self):
        process, stub = self.start_worker('normal')
        future = stub.ProcessBatch.future(pb.BatchRequest(tasks=[
            pb.TaskRequest(task_id='active', input_json='{"x":3}', remaining_seconds=5)
        ]), timeout=5)
        time.sleep(.2)
        process.send_signal(signal.SIGTERM)
        response = future.result(timeout=5)
        self.assertTrue(response.results[0].success)
        self.assertEqual(json.loads(response.results[0].result_json)['prediction'], 9)
        self.assert_clean_exit(process)

    def test_blocked_publication_does_not_block_server_shutdown(self):
        process, _ = self.start_worker('blocked')
        started = time.monotonic()
        process.send_signal(signal.SIGTERM)
        errors = self.assert_clean_exit(process)
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 1.8)
        self.assertLess(elapsed, 5)
        self.assertIn('Draining publication is still pending', errors)

    def test_publication_exception_still_allows_shutdown(self):
        process, _ = self.start_worker('failed')
        process.send_signal(signal.SIGTERM)
        errors = self.assert_clean_exit(process)
        self.assertIn('Failed to publish draining state', errors)
