"""Route only to READY proxies without cancelling accepted work."""

import unittest
from unittest.mock import patch

from system.task import TaskStatus
import test_worker_handoff as fixtures


class ReadyRoutingTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.HandoffTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.pool = self.fixture.pool
        self.draining = self.pool._workers['dead']
        self.ready = self.pool._workers['live']
        self.draining.set_routing_state('DRAINING')

    def test_busy_ready_fallback_never_attempts_draining_proxy(self):
        self.fixture.live.blocked = True
        active, queued = self.fixture.task(), self.fixture.task()
        self.assertTrue(self.ready.try_submit([active]))
        self.assertTrue(self.fixture.live.started.wait(2))
        with patch.object(self.draining, 'try_submit', side_effect=AssertionError('Selected DRAINING worker')):
            self.pool.submit([queued])
        self.fixture.live.blocked = False
        self.fixture.wait_done([active, queued])
        self.assertEqual([active.status, queued.status], [TaskStatus.DONE] * 2)

    def test_all_draining_rejects_every_task_and_reports_unready(self):
        self.ready.set_routing_state('DRAINING')
        tasks = [self.fixture.task() for _ in range(3)]
        self.pool.submit(tasks)
        self.assertFalse(self.pool.has_workers())
        self.assertTrue(all(t.status == TaskStatus.FAILED and t.error_code == 'overloaded' for t in tasks))
        self.assertFalse(self.fixture.dead.received)
        self.assertFalse(self.fixture.live.received)

    def test_retry_does_not_reserve_when_only_other_worker_is_draining(self):
        task = self.fixture.task()
        self.assertFalse(self.pool.try_resubmit(task, 'live'))
        self.assertEqual(task.retry_count, 0)

    def test_ready_and_stopped_are_distinct_from_busy(self):
        self.assertTrue(self.ready.is_ready())
        self.ready.begin_stop()
        self.assertFalse(self.ready.is_ready())
        self.assertFalse(self.pool.has_workers())

    def test_empty_membership_rejects_as_unavailable(self):
        self.pool.stop()
        task = self.fixture.task()
        self.pool.submit([task])
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.error_code, 'overloaded')
