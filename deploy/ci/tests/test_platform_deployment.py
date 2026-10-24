import copy
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from deploy.ci.deploy_platform import Deployment, DeployError, parse_command, validate_archive, metrics_idle, validate_layout

SHA = 'a' * 40
OLD = {'gateway': 'sha256:' + 'b' * 64, 'text-worker': 'sha256:' + 'c' * 64}
NEW = 'sha256:' + 'd' * 64


class HostBoundary:
    """Docker/host boundary only; rollout orchestration under test remains real."""
    def __init__(self):
        self.images = dict(OLD)
        self.state = {'sha': 'old', 'images': dict(OLD), 'compose': 'old.json'}
        self.backend_pins = dict(OLD)
        self.draining = False
        self.busy_now = False
        self.fail_health = False
        self.fail_rollback = False
        self.stops = 0
        self.protected = {'backend': 'business-image', 'tunnel': 'tunnel-image'}
        self.pending = False
    def prepare(self, sha): return {'sha': sha, 'compose': 'new.json'}
    def load(self, archive, sha): return NEW
    def previous(self): return copy.deepcopy(self.state)
    def busy(self): return self.busy_now
    def begin_drain(self): self.draining = True
    def wait_idle(self):
        if self.busy_now: raise DeployError('busy', 75)
    def begin_transaction(self, previous): self.pending = True
    def stop(self): self.stops += 1
    def start(self, release, images): self.images = dict(images)
    def healthy(self, images):
        return not self.fail_rollback and (not self.fail_health or images == OLD)
    def record(self, release, images, previous):
        self.backend_pins = dict(images)
        self.state = {**release, 'images': dict(images)}
        if getattr(self, 'interrupt_record', False): raise OSError('interrupted metadata publication')
    def restore(self, previous):
        self.state = copy.deepcopy(previous); self.backend_pins = dict(previous['images'])
    def end_drain(self): self.draining = False
    def finish(self): self.pending = False


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(); self.addCleanup(self.directory.cleanup)
        self.archive = Path(self.directory.name) / 'image.tar.gz'
        with tarfile.open(self.archive, 'w:gz') as archive:
            body = json.dumps([{'RepoTags': ['synthx-platform:' + SHA], 'Config': 'config.json', 'Layers': []}]).encode()
            info = tarfile.TarInfo('manifest.json'); info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.host = HostBoundary()
    def run_deployment(self): return Deployment(self.host).run(SHA, self.archive, self.digest)
    def test_success_switches_both_services_and_pins_without_touching_backend_or_tunnel(self):
        self.run_deployment()
        self.assertEqual(self.host.images, {'gateway': NEW, 'text-worker': NEW})
        self.assertEqual(self.host.backend_pins, self.host.images)
        self.assertEqual(self.host.protected, {'backend': 'business-image', 'tunnel': 'tunnel-image'})
        self.assertFalse(self.host.draining); self.assertFalse(self.host.pending)
    def test_busy_does_not_stop_any_service(self):
        self.host.busy_now = True
        with self.assertRaises(DeployError) as caught: self.run_deployment()
        self.assertEqual(caught.exception.exit_code, 75)
        self.assertEqual(self.host.stops, 0); self.assertEqual(self.host.images, OLD)
        self.assertFalse(self.host.draining)
    def test_health_failure_restores_both_images_and_backend_dependency_pins(self):
        self.host.fail_health = True
        with self.assertRaises(DeployError): self.run_deployment()
        self.assertEqual(self.host.images, OLD); self.assertEqual(self.host.backend_pins, OLD)
        self.assertFalse(self.host.draining); self.assertFalse(self.host.pending)
    def test_partial_metadata_failure_rolls_back_pins_and_release(self):
        self.host.interrupt_record = True
        with self.assertRaises(DeployError): self.run_deployment()
        self.assertEqual(self.host.images, OLD); self.assertEqual(self.host.backend_pins, OLD)
        self.assertEqual(self.host.state['compose'], 'old.json')
        self.assertFalse(self.host.draining)
    def test_failed_rollback_keeps_admission_closed_and_transaction_for_operator(self):
        self.host.fail_health = self.host.fail_rollback = True
        with self.assertRaisesRegex(DeployError, 'rollback failed'): self.run_deployment()
        self.assertTrue(self.host.draining); self.assertTrue(self.host.pending)
    def test_wrong_digest_never_reaches_docker(self):
        with self.assertRaises(DeployError): Deployment(self.host).run(SHA, self.archive, '0' * 64)
        self.assertEqual(self.host.stops, 0); self.assertFalse(self.host.pending)
    def test_ssh_command_cannot_run_a_shell_or_choose_arbitrary_revision(self):
        self.assertEqual(parse_command('deploy-platform ' + SHA + ' ' + 'e' * 64), ('dev', SHA, 'e' * 64))
        for command in ('id', 'deploy-platform main ' + 'e' * 64, 'deploy-platform ' + SHA + ' ' + 'e' * 64 + ';id'):
            with self.assertRaises(DeployError): parse_command(command)


class SafetyBoundaryTests(unittest.TestCase):
    def metrics(self):
        return '\n'.join(['inference_api_queue_depth 0', 'inference_registered_workers 2'] + [
            line for worker in ('73', '122') for line in (
                f'inference_worker_queued_batches{{worker_id="{worker}"}} 0',
                f'inference_worker_active_rpcs{{worker_id="{worker}"}} 0',
                f'inference_worker_routing_state{{worker_id="{worker}",state="READY"}} 1')])
    def test_missing_or_nonfinite_metrics_never_claim_idle(self):
        self.assertTrue(metrics_idle(self.metrics()))
        for text in ('', self.metrics().replace('inference_api_queue_depth 0', 'inference_api_queue_depth NaN'),
                     self.metrics().replace('worker_id="122"', 'worker_id="123"')):
            self.assertFalse(metrics_idle(text))
    def test_prometheus_label_order_does_not_change_idle_detection(self):
        text=self.metrics()
        for worker in ('73','122'):
            text=text.replace(f'worker_id="{worker}",state="READY"',f'state="READY",worker_id="{worker}"')
        self.assertTrue(metrics_idle(text))

    def test_active_rpc_queue_or_unready_adapter_blocks_deployment(self):
        for before, after in [('inference_api_queue_depth 0','inference_api_queue_depth 1'),
                              ('inference_worker_active_rpcs{worker_id="73"} 0','inference_worker_active_rpcs{worker_id="73"} 1'),
                              ('state="READY"} 1','state="READY"} 0')]:
            self.assertFalse(metrics_idle(self.metrics().replace(before, after)))
    def layout(self):
        return {'services': {'gateway': {'image': NEW, 'ports': [{'host_ip': '127.0.0.1', 'published': '8100', 'target': 8000}], 'networks': {'default': None}},
                             'text-worker': {'image': NEW, 'networks': {'default': None}}},
                'networks': {'default': {'external': True, 'name': 'deploy_default'}}}
    def test_public_port_extra_service_or_network_replacement_is_rejected(self):
        validate_layout(self.layout())
        for mutate in (lambda c:c['services']['gateway']['ports'][0].update(host_ip='0.0.0.0'),
                       lambda c:c['services'].update(backend={}),
                       lambda c:c['networks']['default'].update(external=False),
                       lambda c:c['services']['text-worker'].update(ports=[{'published':50062}])):
            value=self.layout(); mutate(value)
            with self.assertRaises(DeployError): validate_layout(value)

if __name__ == '__main__': unittest.main()


class StrictCutoverTests(unittest.TestCase):
    def test_old_product_is_rejected_before_fetch_load_or_stop(self):
        from unittest.mock import Mock
        from deploy.ci.deploy_platform import Runtime
        host=object.__new__(Runtime)
        from deploy.ci.deploy_platform import Environment
        host.target=Environment('dev')
        host.inspect=Mock(return_value=json.dumps(['SYNTHFLOW_TEXT_PIPELINE=text_v1.2.2']))
        host.command=Mock()
        with self.assertRaisesRegex(DeployError,'coordinated'):
            host.prepare(SHA)
        host.command.assert_not_called()

    def test_v123_product_passes_compatibility_gate(self):
        from unittest.mock import Mock
        from deploy.ci.deploy_platform import Runtime
        host=object.__new__(Runtime)
        from deploy.ci.deploy_platform import Environment
        host.target=Environment('dev')
        host.inspect=Mock(return_value=json.dumps(['SYNTHFLOW_TEXT_PIPELINE=text_v1.2.3']))
        host.assert_product_compatible()
