"""Real Unix socket/local files with an in-memory S3 boundary; no model calls."""
import copy
import hashlib
import io
import json
from pathlib import Path
import socket
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

from system import inference_pb2 as pb
from system import inference_pb2_grpc as rpc
from system import grpc_worker_server
import grpc


PIN = 'ba67d1234be517da77f9a4fdfe26b6c5831bda461ee44d2c4bf400d9367a0d86'


class MemoryS3:
    def __init__(self, data=b'RIFF-wave'):
        self.data = data
        self.objects = {}
        self.fail_upload = False
        self.reads = 0

    def get_object(self, *, Bucket, Key):
        assert Bucket == 'synthx-jobs' and Key.endswith('/input.wav')
        self.reads += 1
        return {'Body': io.BytesIO(self.data), 'ContentLength': len(self.data)}

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        if self.fail_upload:
            raise OSError('temporary S3 failure')
        self.objects[(Bucket, Key)] = Body.read() if hasattr(Body, 'read') else Body


class Context:
    def __init__(self):
        self.active = True

    def is_active(self):
        return self.active


class SocketDaemon:
    def __init__(self, root, *, delay=0, status='completed', symlink=False, missing=False,
                 checkpoint=PIN, bad_hash=False, external_worker_log=False,
                 wrong_report=False):
        self.path = root / 'daemon.sock'
        self.calls = []
        self.delay = delay
        self.status = status
        self.symlink = symlink
        self.missing = missing
        self.checkpoint = checkpoint
        self.bad_hash = bad_hash
        self.external_worker_log = external_worker_log
        self.wrong_report = wrong_report
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.bind(str(self.path))
        self.sock.listen()
        self.sock.settimeout(.1)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def run(self):
        while not self.stop.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with conn:
                request = json.loads(conn.makefile('rb').readline())
                self.calls.append(request)
                out = Path(request['out'])
                out.mkdir()  # Existing output must never be reused.
                time.sleep(self.delay)
                artifacts = {}
                if self.status == 'completed':
                    for name, data in {
                        'final.vital': b'{"settings": {}}',
                        'final.unified.json': b'{"schema_version": "1.0", "common": {}}',
                        'final.wav': b'RIFF-final',
                        'selected.wav': b'RIFF-selected',
                    }.items():
                        if self.missing and name == 'final.wav':
                            continue
                        (out / name).write_bytes(data)
                        artifacts[name] = {'path': str(out / name),
                                           'sha256': hashlib.sha256(data).hexdigest()}
                    if self.bad_hash:
                        artifacts['final.vital']['sha256'] = '0' * 64
                    (out / 'result.json').write_text(json.dumps({
                        'status': 'completed', 'mode': request['mode'],
                        'checkpoint_sha256': self.checkpoint, 'artifacts': artifacts}))
                    if self.symlink:
                        (out / 'final.vital').unlink()
                        (out / 'final.vital').symlink_to(Path(request['input']))
                response = {'ok': True, 'result': {
                    'status': self.status, 'output': str(out),
                    'report': str(out / ('other.json' if self.wrong_report else 'result.json')),
                    'artifacts': artifacts,
                    'worker_log': str(out.parent.parent / 'requests' / 'request-id' / 'worker.log')
                    if self.external_worker_log else str(out / 'worker.log')}}
                try:
                    conn.sendall(json.dumps(response).encode() + b'\n')
                except BrokenPipeError:
                    pass

    def close(self):
        self.stop.set()
        self.sock.close()
        self.thread.join(2)


class V73WorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='v73-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.s3 = MemoryS3()
        self.job = str(uuid.uuid4())
        self.project = str(uuid.uuid4())
        self.payload = {'kind': 'synthx_v73', 'attempt_id': str(uuid.uuid4()), 'spec': {
            'job_id': self.job, 'project_id': self.project, 'user_id': 'user',
            'model': 'v7_3_full', 'checkpoint_sha256': PIN, 'mode': 'poly',
            'input': {'bucket': 'synthx-jobs',
                      'key': f'projects/{self.project}/jobs/{self.job}/input.wav',
                      'sha256': hashlib.sha256(self.s3.data).hexdigest(),
                      'size_bytes': len(self.s3.data)},
            'output_prefix': f'projects/{self.project}/jobs/{self.job}/results/'}}

    def worker(self, **daemon_kwargs):
        from system.worker_v73 import V73Worker
        self.daemon = SocketDaemon(self.root, **daemon_kwargs)
        self.addCleanup(self.daemon.close)
        return V73Worker(deployment_dir=self.root, socket_path=self.daemon.path, s3=self.s3)

    def error(self, worker, code, payload=None, **kwargs):
        from system.worker_v73 import WorkerError
        with self.assertRaises(WorkerError) as caught:
            worker.process(payload or self.payload, Context(), **kwargs)
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_uploads_verified_required_artifacts_and_returns_unified_document(self):
        worker = self.worker()
        result = worker.process(self.payload, Context())
        self.assertEqual(result['job_id'], self.job)
        self.assertEqual(result['checkpoint_sha256'], PIN)
        self.assertEqual(result['parameter_json'], {'schema_version': '1.0', 'common': {}})
        self.assertEqual(set(result['artifacts']), {'final.vital', 'final.unified.json',
                                                   'final.wav', 'result.json', 'selected.wav'})
        for name, artifact in result['artifacts'].items():
            data = self.s3.objects[(artifact['bucket'], artifact['key'])]
            self.assertEqual(artifact['key'], self.payload['spec']['output_prefix'] + name)
            self.assertEqual(artifact['sha256'], hashlib.sha256(data).hexdigest())
            self.assertEqual(artifact['size_bytes'], len(data))
        self.assertEqual(self.daemon.calls[0]['mode'], 'poly')
        self.assertIsNone(self.daemon.calls[0]['midi'])

    def test_replay_new_attempt_uses_immutable_cached_result_without_execution(self):
        worker = self.worker()
        result = worker.process(self.payload, Context())
        replay = copy.deepcopy(self.payload)
        replay['attempt_id'] = str(uuid.uuid4())
        self.assertEqual(worker.process(replay, Context()), result)
        self.assertEqual(len(self.daemon.calls), 1)
        self.assertEqual(self.s3.reads, 1)

    def test_same_job_with_different_spec_is_conflict(self):
        worker = self.worker()
        worker.process(self.payload, Context())
        changed = copy.deepcopy(self.payload)
        changed['spec']['mode'] = 'mono'
        self.error(worker, 'spec_conflict', changed)

    def test_rejects_untrusted_spec_before_download(self):
        worker = self.worker()
        bad_fields = [('model', 'other'), ('mode', 'other'), ('job_id', '../escape'),
                      ('project_id', '../escape'), ('checkpoint_sha256', '0' * 64),
                      ('output_prefix', 'outside/'), ('extra', '/etc/passwd')]
        for key, value in bad_fields:
            with self.subTest(key=key):
                bad = copy.deepcopy(self.payload)
                bad['spec'][key] = value
                self.error(worker, 'invalid_spec', bad)
        for key, value in [('bucket', 'other'), ('key', '/etc/passwd'),
                           ('size_bytes', 50 * 1024 * 1024 + 1), ('size_bytes', True),
                           ('sha256', 'invalid')]:
            with self.subTest(input_key=key):
                bad = copy.deepcopy(self.payload)
                bad['spec']['input'][key] = value
                self.error(worker, 'invalid_spec', bad)
        self.assertEqual(self.s3.reads, 0)
        self.assertEqual(self.daemon.calls, [])

    def test_wrong_input_hash_and_size_never_reach_model(self):
        worker = self.worker()
        self.s3.data = b'wrong'
        self.error(worker, 'input_integrity_error')
        self.assertEqual(self.daemon.calls, [])

    def test_same_size_input_with_wrong_hash_never_reaches_model(self):
        worker = self.worker()
        self.s3.data = b'wrongwav!'
        self.error(worker, 'input_integrity_error')
        self.assertEqual(self.daemon.calls, [])

    def test_lying_content_length_cannot_bypass_streaming_size_limit(self):
        worker = self.worker()
        self.s3.get_object = lambda **kwargs: {'Body': io.BytesIO(b'excess-data'),
                                              'ContentLength': len(b'RIFF-wave')}
        self.error(worker, 'input_integrity_error')
        self.assertEqual(self.daemon.calls, [])

    def test_symlink_output_directory_is_rejected(self):
        worker = self.worker()
        outside = self.root / 'unowned'
        outside.mkdir()
        (self.root / 'results' / self.job).symlink_to(outside, target_is_directory=True)
        self.error(worker, 'invalid_artifacts')
        self.assertEqual(self.daemon.calls, [])

    def test_missing_artifact_prevents_success(self):
        self.error(self.worker(missing=True), 'invalid_artifacts')
        self.assertEqual(self.s3.objects, {})

    def test_symlink_artifact_prevents_any_upload(self):
        self.error(self.worker(symlink=True), 'invalid_artifacts')
        self.assertEqual(self.s3.objects, {})

    def test_no_usable_note_is_terminal_and_replayed(self):
        worker = self.worker(status='no_usable_note')
        self.assertFalse(self.error(worker, 'no_usable_note').retryable)
        self.error(worker, 'no_usable_note')
        self.assertEqual(len(self.daemon.calls), 1)

    def test_upload_failure_retries_upload_without_rerunning_model(self):
        worker = self.worker()
        self.s3.fail_upload = True
        self.assertTrue(self.error(worker, 'storage_error').retryable)
        self.s3.fail_upload = False
        result = worker.process(self.payload, Context())
        self.assertIn('final.vital', result['artifacts'])
        self.assertEqual(len(self.daemon.calls), 1)

    def test_timeout_preserves_running_execution_and_later_replays_result(self):
        worker = self.worker(delay=.3)
        self.error(worker, 'deadline_exceeded', deadline=time.monotonic() + .05)
        self.assertTrue(self.error(worker, 'already_running').retryable)
        time.sleep(.4)
        self.assertEqual(worker.process(self.payload, Context())['job_id'], self.job)
        self.assertEqual(len(self.daemon.calls), 1)

    def test_existing_unfinished_output_is_never_resubmitted(self):
        worker = self.worker()
        (self.root / 'results' / self.job).mkdir(parents=True)
        self.assertTrue(self.error(worker, 'already_running').retryable)
        self.assertEqual(self.daemon.calls, [])

    def test_expired_request_has_no_download_or_model_side_effect(self):
        worker = self.worker()
        self.error(worker, 'deadline_exceeded', deadline=time.monotonic() - 1)
        self.assertEqual(self.s3.reads, 0)
        self.assertEqual(self.daemon.calls, [])

    def test_rpc_backend_emits_typed_error(self):
        worker = self.worker(status='no_usable_note')
        server = grpc_worker_server.WorkerServicer(9, backend=worker)
        result = server.ProcessBatch(pb.BatchRequest(tasks=[pb.TaskRequest(
            task_id=self.job, input_json=json.dumps(self.payload), remaining_seconds=2)]),
            Context()).results[0]
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, 'no_usable_note')

    def test_cancellation_during_download_never_starts_model(self):
        worker = self.worker()
        context = Context()
        original = self.s3.get_object

        def cancel_download(**kwargs):
            context.active = False
            return original(**kwargs)

        self.s3.get_object = cancel_download
        from system.worker_v73 import WorkerError
        with self.assertRaises(WorkerError):
            worker.process(self.payload, context)
        time.sleep(.1)
        self.assertEqual(self.daemon.calls, [])

    def test_wrong_daemon_checkpoint_prevents_any_upload(self):
        self.error(self.worker(checkpoint='0' * 64), 'checkpoint_mismatch')
        self.assertEqual(self.s3.objects, {})

    def test_report_hash_mismatch_prevents_any_upload(self):
        self.error(self.worker(bad_hash=True), 'invalid_artifacts')
        self.assertEqual(self.s3.objects, {})

    def test_actual_daemon_log_outside_output_is_ignored(self):
        worker = self.worker(external_worker_log=True)
        result = worker.process(self.payload, Context())
        self.assertEqual(result['job_id'], self.job)
        self.assertNotIn('worker.log', result['artifacts'])

    def test_report_reference_must_be_fixed_result_json(self):
        self.error(self.worker(wrong_report=True), 'invalid_artifacts')
        self.assertEqual(self.s3.objects, {})

    def test_cross_instance_execution_is_serialized(self):
        from system.worker_v73 import V73Worker
        worker = self.worker(delay=.2)
        self.error(worker, 'deadline_exceeded', deadline=time.monotonic() + .04)
        other = V73Worker(deployment_dir=self.root, socket_path=self.daemon.path, s3=self.s3)
        different = copy.deepcopy(self.payload)
        new_job = str(uuid.uuid4())
        different['spec']['job_id'] = new_job
        different['spec']['input']['key'] = f'projects/{self.project}/jobs/{new_job}/input.wav'
        different['spec']['output_prefix'] = f'projects/{self.project}/jobs/{new_job}/results/'
        self.assertTrue(self.error(other, 'worker_busy', different).retryable)
        time.sleep(.3)
        self.assertEqual(other.process(different, Context())['job_id'], new_job)
        self.assertEqual(len(self.daemon.calls), 2)

    def test_restart_replays_uploaded_result(self):
        from system.worker_v73 import V73Worker
        worker = self.worker()
        expected = worker.process(self.payload, Context())
        other = V73Worker(deployment_dir=self.root, socket_path=self.daemon.path, s3=self.s3)
        self.assertEqual(other.process(self.payload, Context()), expected)
        self.assertEqual(len(self.daemon.calls), 1)

    def test_restart_recovers_completed_report_without_rerunning_model(self):
        from system.worker_v73 import V73Worker
        worker = self.worker()
        expected = worker.process(self.payload, Context())
        cache = self.root / '.synthx_v73' / self.job / 'state.json'
        state = json.loads(cache.read_text())
        del state['result']
        del state['response']
        state['running'] = True
        cache.write_text(json.dumps(state))
        self.s3.objects.clear()
        other = V73Worker(deployment_dir=self.root, socket_path=self.daemon.path, s3=self.s3)
        self.assertEqual(other.process(self.payload, Context()), expected)
        self.assertEqual(len(self.daemon.calls), 1)
        self.assertIn(('synthx-jobs', self.payload['spec']['output_prefix'] + 'final.vital'), self.s3.objects)

    def test_restart_with_unfinished_report_remains_retryable(self):
        worker = self.worker()
        worker.process(self.payload, Context())
        cache = self.root / '.synthx_v73' / self.job / 'state.json'
        state = json.loads(cache.read_text())
        del state['result']
        del state['response']
        state['running'] = True
        cache.write_text(json.dumps(state))
        (self.root / 'results' / self.job / 'result.json').unlink()
        self.assertTrue(self.error(worker, 'already_running').retryable)
        self.assertEqual(len(self.daemon.calls), 1)

    def test_simulator_default_and_real_backend_selection(self):
        worker = self.worker()
        with patch.dict('os.environ', {'INFERENCE_BACKEND': 'v7_3_full'}), \
                patch('system.worker_v73.V73Worker', return_value=worker):
            selected = grpc_worker_server.WorkerServicer(3)
            result = selected.ProcessBatch(pb.BatchRequest(tasks=[pb.TaskRequest(
                task_id=self.job, input_json=json.dumps(self.payload))]), Context()).results[0]
            self.assertTrue(result.success)
        with patch.dict('os.environ', {'INFERENCE_BACKEND': 'simulation'}):
            simulator = grpc_worker_server.WorkerServicer(3)
            result = simulator.ProcessBatch(pb.BatchRequest(tasks=[pb.TaskRequest(
                task_id='simulation', input_json='{"x":4}')]), Context()).results[0]
            self.assertEqual(json.loads(result.result_json)['prediction'], 16)

    def test_real_service_runs_without_zookeeper_and_maps_wire_errors(self):
        child = '''
import os, signal, sys
from system import grpc_worker_server as worker
os.environ['INFERENCE_BACKEND'] = 'v7_3_full'
os.environ['V73_DEPLOYMENT_DIR'] = sys.argv[2]
class ForbiddenRegistry:
    def __init__(self, hosts): raise AssertionError('ZooKeeper must not be used')
worker.WorkerRegistry = ForbiddenRegistry
original = signal.signal
def ready(signum, handler):
    result = original(signum, handler)
    if signum == signal.SIGINT: print('READY', flush=True)
    return result
worker.signal.signal = ready
worker.serve(99, int(sys.argv[1]), 'unused', register=False)
'''
        with socket.socket() as reserved:
            reserved.bind(('127.0.0.1', 0))
            port = reserved.getsockname()[1]
        process = subprocess.Popen([sys.executable, '-u', '-c', child, str(port), str(self.root)],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.communicate(timeout=5)
        self.addCleanup(cleanup)
        self.assertEqual(process.stdout.readline().strip(), 'READY')
        with grpc.insecure_channel(f'127.0.0.1:{port}') as channel:
            grpc.channel_ready_future(channel).result(timeout=5)
            result = rpc.WorkerServiceStub(channel).ProcessBatch(pb.BatchRequest(tasks=[
                pb.TaskRequest(task_id='invalid', input_json='{"x":4}')]), timeout=2).results[0]
            self.assertFalse(result.success)
            self.assertEqual(result.error_code, 'invalid_spec')
        process.send_signal(signal.SIGTERM)
        _, errors = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, errors)


if __name__ == '__main__':
    unittest.main()
