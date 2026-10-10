"""Verify per-worker load and routing metrics across RPC and membership changes."""

import time
import unittest

import grpc
from prometheus_client import REGISTRY, generate_latest

from system.task import TaskStatus
from system.zookeeper_registry import WorkerInfo
import test_worker_handoff as fixtures


class WorkerMetricsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.HandoffTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.pool = self.fixture.pool
        self.worker = self.pool._workers["live"]

    def sample(self, metric, wid="live", **labels):
        return REGISTRY.get_sample_value(metric, {"worker_id": wid, **labels})

    def wait_sample(self, metric, expected):
        end = time.monotonic() + 2
        while time.monotonic() < end:
            if self.sample(metric) == expected:
                return
            time.sleep(.005)
        self.assertEqual(self.sample(metric), expected)

    def test_queue_excludes_active_rpc_and_returns_to_zero(self):
        self.fixture.live.blocked = True
        active, queued = self.fixture.task(), self.fixture.task()
        self.assertTrue(self.worker.try_submit([active]))
        self.assertTrue(self.fixture.live.started.wait(2))
        self.assertEqual(self.sample("inference_worker_active_rpcs"), 1)
        self.assertEqual(self.sample("inference_worker_queued_batches"), 0)
        self.assertTrue(self.worker.try_submit([queued]))
        self.assertEqual(self.sample("inference_worker_queued_batches"), 1)
        self.assertFalse(self.worker.try_submit([self.fixture.task()]))
        self.assertEqual(self.sample("inference_worker_queued_batches"), 1)
        self.fixture.live.blocked = False
        self.fixture.wait_done([active, queued])
        self.wait_sample("inference_worker_active_rpcs", 0)
        self.wait_sample("inference_worker_queued_batches", 0)
        self.assertEqual(queued.status, TaskStatus.DONE)

    def test_rpc_error_resets_active_gauge(self):
        self.fixture.live.abort_code = grpc.StatusCode.INTERNAL
        task = self.fixture.task()
        self.worker.submit([task])
        self.fixture.wait_done([task])
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.wait_sample("inference_worker_active_rpcs", 0)

    def test_draining_changes_state_without_clearing_active_load(self):
        self.fixture.live.blocked = True
        task = self.fixture.task()
        self.worker.submit([task])
        self.assertTrue(self.fixture.live.started.wait(2))
        self.worker.set_routing_state("DRAINING")
        self.assertEqual(self.sample("inference_worker_routing_state", state="READY"), 0)
        self.assertEqual(self.sample("inference_worker_routing_state", state="DRAINING"), 1)
        self.assertEqual(self.sample("inference_worker_active_rpcs"), 1)
        self.assertFalse(self.worker.try_submit([self.fixture.task()]))
        self.fixture.live.blocked = False
        self.fixture.wait_done([task])

    def test_removed_worker_series_disappear_and_rejoin_starts_empty(self):
        retired = self.worker
        self.pool._update_workers({})
        for metric in ("inference_worker_queued_batches", "inference_worker_active_rpcs"):
            self.assertIsNone(self.sample(metric))
        self.assertIsNone(self.sample("inference_worker_routing_state", state="READY"))
        self.assertIsNone(self.sample("inference_worker_routing_state", state="DRAINING"))
        self.pool._update_workers({"live": WorkerInfo(self.fixture.live_addr, "READY")})
        retired.stop()
        self.assertEqual(self.sample("inference_worker_active_rpcs"), 0)
        self.assertEqual(self.sample("inference_worker_queued_batches"), 0)
        self.assertEqual(self.sample("inference_worker_routing_state", state="READY"), 1)

    def test_metrics_are_exported_and_pool_shutdown_removes_labels(self):
        output = generate_latest().decode()
        self.assertIn('inference_worker_queued_batches{worker_id="live"} 0.0', output)
        self.assertIn('inference_worker_active_rpcs{worker_id="live"} 0.0', output)
        self.pool.stop()
        self.assertIsNone(self.sample("inference_worker_active_rpcs"))
        self.assertIsNone(self.sample("inference_worker_queued_batches"))
