import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from deploy.ci.deploy_platform import Runtime, DeployError
from system.deployment import DeploymentGate


class DrainRuntime(Runtime):
    """Real rollout gates/files; replace only remote probes and backend boundary."""
    def __init__(self, root):
        self.root = Path(root)
        (self.root / 'control').mkdir()
        self.platform_token = None
        self.marker_names = set()
        self.config = {'drain_timeout_seconds': 10}
        self.backend = SimpleNamespace(drain_token='backend-token')
        self.events = []
        self.gateway_gate = DeploymentGate(self.root / 'control/gateway-drain')
        self.worker_gate = DeploymentGate(self.root / 'control/worker-drain')
        self.gateway_gate.enter()  # already accepted work still needs the adapter
        self.worker_gate.enter()  # a timed-out caller still has a live handler
    def backend_call(self, name):
        self.events.append(name)
        if name == 'wait_idle':
            # An admitted backend operation must still be able to enter gateway.
            assert self.gateway_gate.enter()
            self.gateway_gate.leave()
        if name == 'deployment_status':
            return {'draining': True, 'drain_id': 'backend-token', 'active_requests': 0, 'active_operations': 0}
    def gateway_status(self): return self.gateway_gate.snapshot()
    def worker_status(self): return self.worker_gate.snapshot()
    def gateway_quiet(self):
        self.events.append('gateway-probe')
        if self.gateway_gate.snapshot()['active']:
            # A queued gateway task must still be able to enter worker.
            assert self.worker_gate.enter()
            self.worker_gate.leave()
            self.gateway_gate.leave()
            return False
        return True
    def busy(self): return False
    def assert_protected(self): pass


class DrainSequenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.runtime = DrainRuntime(self.tmp.name)
    def test_backend_then_gateway_then_actual_worker_handlers_drain(self):
        r = self.runtime
        r.begin_drain()
        self.assertFalse(r.gateway_gate.snapshot()['draining'])
        self.assertFalse(r.worker_gate.snapshot()['draining'])
        def finish_handler(_):
            if r.worker_gate.snapshot()['draining']:
                self.assertFalse(r.worker_gate.enter())
                r.worker_gate.leave()
        with patch('deploy.ci.deploy_platform.time.sleep', side_effect=finish_handler):
            r.wait_idle()
        self.assertLess(r.events.index('wait_idle'), r.events.index('gateway-probe'))
        self.assertTrue(r.platform_status())
        r.end_drain()
        self.assertFalse(r.gateway_gate.snapshot()['draining'])
        self.assertFalse(r.worker_gate.snapshot()['draining'])
        self.assertEqual(r.events[-1], 'end_drain')
    def test_failure_before_atomic_marker_publication_cleans_up_and_reopens(self):
        r = self.runtime
        r.begin_drain()
        with patch('deploy.ci.deploy_platform.os.fsync', side_effect=OSError('disk full')):
            with self.assertRaises(OSError): r.fence('gateway-drain')
        r.end_drain()
        self.assertFalse(r.gateway_gate.snapshot()['draining'])
        self.assertEqual(list((r.root / 'control').iterdir()), [])
        self.assertEqual(r.events[-1], 'end_drain')
    def test_interruption_after_publication_retains_ownership_for_cleanup(self):
        r = self.runtime
        r.begin_drain()
        import os
        real_link = os.link
        def publish_then_interrupt(source, target):
            real_link(source, target)
            raise OSError('interrupted after publish')
        with patch('deploy.ci.deploy_platform.os.link', side_effect=publish_then_interrupt):
            with self.assertRaises(OSError): r.fence('gateway-drain')
        self.assertTrue(r.gateway_gate.snapshot()['draining'])
        r.end_drain()
        self.assertFalse(r.gateway_gate.snapshot()['draining'])
        self.assertEqual(r.events[-1], 'end_drain')
    def test_foreign_marker_never_reopens_backend(self):
        r = self.runtime
        r.begin_drain(); r.fence('gateway-drain')
        r.gateway_gate.marker.write_text('someone-else')
        with self.assertRaises(DeployError): r.end_drain()
        self.assertNotIn('end_drain', r.events)
