"""Check retry counters against accepted, completed, and rejected handoffs."""

import time
import unittest

from prometheus_client import REGISTRY

import test_worker_handoff as fixtures


class RetryMetricTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.HandoffTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.before = self.counts()

    def counts(self):
        return tuple(REGISTRY.get_sample_value(name) for name in (
            "inference_retry_submissions_total",
            "inference_retry_completions_total",
        ))

    def test_accepted_retry_is_not_complete_until_result_arrives(self):
        self.fixture.live.blocked = True
        task = self.fixture.task()
        self.assertTrue(self.fixture.pool.try_resubmit(task, "dead"))
        self.assertTrue(self.fixture.live.started.wait(2))
        self.assertEqual(self.counts(), (self.before[0] + 1, self.before[1]))
        self.fixture.live.blocked = False
        self.fixture.wait_done([task])
        end = time.monotonic() + 2
        expected = (self.before[0] + 1, self.before[1] + 1)
        while self.counts() != expected and time.monotonic() < end:
            time.sleep(.005)
        self.assertEqual(self.counts(), expected)

    def test_full_target_does_not_count_a_retry_submission(self):
        self.fixture.live.blocked = True
        worker = self.fixture.pool._workers["live"]
        self.assertTrue(worker.try_submit([self.fixture.task()]))
        self.assertTrue(self.fixture.live.started.wait(2))
        self.assertTrue(worker.try_submit([self.fixture.task()]))
        self.assertFalse(self.fixture.pool.try_resubmit(self.fixture.task(), "dead"))
        self.assertEqual(self.counts(), self.before)

    def test_normal_completion_does_not_count_as_a_retry(self):
        task = self.fixture.task()
        self.fixture.pool._workers["live"].try_submit([task])
        self.fixture.wait_done([task])
        self.assertEqual(self.counts(), self.before)


if __name__ == "__main__":
    unittest.main()
