"""Check discovered membership and the API queue gauge against live state."""

import asyncio
import unittest
from unittest.mock import patch

from prometheus_client import REGISTRY

from system import api_server
from system.metrics import QUEUE_DEPTH
from system.task import Task
import test_worker_handoff as fixtures


class WorkerCountTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.HandoffTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def count(self):
        return REGISTRY.get_sample_value("inference_registered_workers")

    def test_membership_gauge_tracks_additions_and_removals(self):
        self.assertEqual(self.count(), 2)
        self.fixture.pool._update_workers({"live": self.fixture.live_addr})
        self.assertEqual(self.count(), 1)
        self.fixture.pool._update_workers({})
        self.assertEqual(self.count(), 0)

    def test_shutdown_resets_membership_gauge(self):
        self.fixture.pool.stop()
        self.assertEqual(self.count(), 0)


class QueueDepthTests(unittest.TestCase):
    def test_lifespan_binds_queue_depth_to_current_queue_contents(self):
        async def check():
            with patch.object(api_server, "USE_GRPC", False), \
                    patch.object(api_server, "WorkerPool"), \
                    patch.object(api_server, "Scheduler"):
                async with api_server.lifespan(api_server.app):
                    queue = api_server._task_queue
                    read = lambda: REGISTRY.get_sample_value("inference_api_queue_depth")
                    self.assertEqual(read(), 0)
                    queue.put(Task(input_data={"x": 3}), timeout=0)
                    queue.put(Task(input_data={"x": 4}), timeout=0)
                    self.assertEqual(read(), 2)
                    queue.get(timeout=0)
                    self.assertEqual(read(), 1)
                    queue.drain()
                    self.assertEqual(read(), 0)
        try:
            asyncio.run(check())
        finally:
            QUEUE_DEPTH.set_function(lambda: 0)


if __name__ == "__main__":
    unittest.main()
