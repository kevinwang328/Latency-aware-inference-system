"""Vast V7.3 bridge. The calibrated model remains behind its existing socket.

RPC cancellation stops waiting, not execution: a background collector retains
the job flock and records the daemon response and uploaded result. A process
crash recovers only a completed report under the recorded job output path;
an unresolved run stays retryable until the gateway job deadline or operator
recovery. It never silently reuses a model output directory.
"""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import socket
import threading
import time
import uuid

logger = logging.getLogger(__name__)
CHECKPOINT_SHA256 = 'ba67d1234be517da77f9a4fdfe26b6c5831bda461ee44d2c4bf400d9367a0d86'
DEPLOYMENT = '/workspace/Transformer_dec/pad_v73_inference_20261012'
MAX_INPUT_BYTES = 50 * 1024 * 1024
REQUIRED = ('final.vital', 'final.unified.json', 'final.wav', 'result.json')
OPTIONAL = ('selected.wav', 'mono_preview.wav', 'predicted.mid', 'performance.json')


class WorkerError(Exception):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def _uuid(value):
    return isinstance(value, str) and str(uuid.UUID(value)) == value


def validate_payload(payload):
    """Reject arbitrary buckets, paths, model releases and unknown fields."""
    try:
        if not isinstance(payload, dict) or set(payload) != {'kind', 'spec', 'attempt_id'}:
            raise ValueError('invalid task envelope')
        if payload['kind'] != 'synthx_v73' or not _uuid(payload['attempt_id']):
            raise ValueError('invalid task kind or attempt UUID')
        spec = payload['spec']
        fields = {'job_id', 'project_id', 'user_id', 'model', 'checkpoint_sha256',
                  'mode', 'input', 'output_prefix'}
        if not isinstance(spec, dict) or set(spec) != fields:
            raise ValueError('unexpected spec fields')
        if not _uuid(spec['job_id']) or not _uuid(spec['project_id']):
            raise ValueError('job and project must be canonical UUIDs')
        if not isinstance(spec['user_id'], str) or not spec['user_id']:
            raise ValueError('user_id is required')
        if spec['model'] != 'v7_3_full' or spec['checkpoint_sha256'] != CHECKPOINT_SHA256:
            raise ValueError('model or checkpoint does not match pinned release')
        if spec['mode'] not in ('poly', 'mono'):
            raise ValueError('mode must be poly or mono')
        base = f"projects/{spec['project_id']}/jobs/{spec['job_id']}/"
        inp = spec['input']
        if not isinstance(inp, dict) or set(inp) != {'bucket', 'key', 'sha256', 'size_bytes'}:
            raise ValueError('invalid input fields')
        if inp['bucket'] != 'synthx-jobs' or inp['key'] != base + 'input.wav':
            raise ValueError('input storage location does not match job')
        if spec['output_prefix'] != base + 'results/':
            raise ValueError('output prefix does not match job')
        if type(inp['size_bytes']) is not int or not 0 < inp['size_bytes'] <= MAX_INPUT_BYTES:
            raise ValueError('input must be between 1 byte and 50 MiB')
        if not isinstance(inp['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', inp['sha256']):
            raise ValueError('invalid input SHA256')
        fingerprint = hashlib.sha256(_canonical(spec)).hexdigest()
        # Detach from the caller so no later mutation can change execution.
        return json.loads(_canonical(spec)), fingerprint
    except (KeyError, ValueError, TypeError, AttributeError) as exc:
        raise WorkerError('invalid_spec', str(exc)) from exc


class V73Worker:
    def __init__(self, deployment_dir=None, socket_path=None, s3=None, *, upload_workers=4):
        if type(upload_workers) is not int or not 1 <= upload_workers <= 4:
            raise ValueError("upload_workers must be an integer from 1 to 4")
        self.upload_workers = upload_workers
        self.root = Path(deployment_dir or os.environ.get('V73_DEPLOYMENT_DIR', DEPLOYMENT)).absolute()
        self.socket_path = str(socket_path or os.environ.get('V73_SOCKET_PATH', str(self.root / 'pad_v73.sock')))
        self._s3 = s3
        self.cache_root = self.root / '.synthx_v73'
        self.results_root = self.root / 'results'
        self._safe_dir(self.root)
        self._safe_dir(self.cache_root)
        self._safe_dir(self.results_root)

    def _safe_path(self, path, code='invalid_artifacts'):
        path = Path(path).absolute()
        if path.resolve() != path or not path.is_relative_to(self.root):
            raise WorkerError(code, 'local path is outside deployment or contains a symlink')
        return path

    def _safe_dir(self, path):
        self._safe_path(path)
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._safe_path(path)

    def _lock(self, path, code):
        self._safe_path(path)
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise WorkerError(code, 'job is already running' if code == 'already_running'
                              else 'model worker is busy', retryable=True) from exc
        return fd

    def _read_json(self, path):
        self._safe_path(path)
        return json.loads(path.read_text(encoding='utf-8'))

    def _write_json(self, path, value):
        self._safe_path(path)
        temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
        with temporary.open('xb') as stream:
            stream.write(_canonical(value))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)

    @staticmethod
    def _check_active(context, deadline):
        if (deadline is not None and time.monotonic() >= deadline) or not context.is_active():
            raise WorkerError('deadline_exceeded', 'request deadline exceeded')

    def process(self, payload, context, deadline=None):
        spec, fingerprint = validate_payload(payload)
        if deadline is not None and not math.isfinite(deadline):
            raise WorkerError('invalid_spec', 'deadline must be finite')
        self._check_active(context, deadline)
        job_dir = self.cache_root / spec['job_id']
        self._safe_dir(job_dir)
        job_lock = self._lock(job_dir / 'job.lock', 'already_running')
        try:
            state_path = job_dir / 'state.json'
            state = self._read_json(state_path) if state_path.exists() else {}
            if state and state.get('fingerprint') != fingerprint:
                raise WorkerError('spec_conflict', 'job spec differs from its immutable fingerprint')
            if 'result' in state:
                return state['result']
            if 'error' in state:
                error = state['error']
                raise WorkerError(error['code'], error['message'])
            out = self._safe_path(self.results_root / spec['job_id'])
            if 'response' not in state and (state.get('running') or out.exists()):
                recovered = self._recover_response(state, out)
                if recovered is None:
                    raise WorkerError('already_running', 'existing model execution is unresolved', True)
                state['response'] = recovered
                state['running'] = False
            model_lock = self._lock(self.cache_root / 'execution.lock', 'worker_busy')
        except BaseException:
            os.close(job_lock)
            raise
        done = threading.Event()
        outcome = {}

        def run():
            try:
                outcome['result'] = self._execute(spec, fingerprint, job_dir, state, out, context, deadline)
            except WorkerError as exc:
                outcome['error'] = exc
            except Exception:
                logger.exception('Unexpected V7.3 worker failure for job %s', spec['job_id'])
                outcome['error'] = WorkerError('worker_error', 'unexpected worker failure', True)
            finally:
                os.close(model_lock)
                os.close(job_lock)
                done.set()

        collector = threading.Thread(target=run, name=f"V73-{spec['job_id']}", daemon=True)
        collector.start()
        while not done.wait(.02):
            self._check_active(context, deadline)
        self._check_active(context, deadline)
        if 'error' in outcome:
            raise outcome['error']
        return outcome['result']

    def _client(self):
        if self._s3 is None:
            import boto3
            from botocore.config import Config
            endpoint = os.environ.get('SUPABASE_S3_ENDPOINT')
            if not endpoint:
                raise WorkerError('worker_configuration', 'SUPABASE_S3_ENDPOINT is required')
            self._s3 = boto3.Session(profile_name='supabase').client(
                's3', endpoint_url=endpoint, region_name=os.environ.get('AWS_REGION', 'us-east-1'),
                config=Config(s3={'addressing_style': 'path'},
                              request_checksum_calculation='when_required',
                              response_checksum_validation='when_required',
                              connect_timeout=10, read_timeout=60,
                              retries={'max_attempts': 3, 'mode': 'standard'}))
        return self._s3

    def _download(self, spec, input_path):
        inp = spec['input']
        temporary = input_path.with_suffix('.download')
        self._safe_path(input_path)
        self._safe_path(temporary)
        body = None
        try:
            obj = self._client().get_object(Bucket=inp['bucket'], Key=inp['key'])
            body = obj['Body']
            if obj.get('ContentLength', inp['size_bytes']) != inp['size_bytes']:
                raise WorkerError('input_integrity_error', 'download size differs from spec')
            digest = hashlib.sha256()
            total = 0
            with temporary.open('wb') as stream:
                while True:
                    data = body.read(min(1024 * 1024, MAX_INPUT_BYTES + 1 - total))
                    if not data:
                        break
                    total += len(data)
                    if total > MAX_INPUT_BYTES or total > inp['size_bytes']:
                        raise WorkerError('input_integrity_error', 'download exceeds declared input size')
                    digest.update(data)
                    stream.write(data)
            if total != inp['size_bytes'] or digest.hexdigest() != inp['sha256']:
                raise WorkerError('input_integrity_error', 'download SHA256 or size differs from spec')
            os.replace(temporary, input_path)
        except WorkerError:
            raise
        except Exception as exc:
            raise WorkerError('storage_error', 'input download failed', True) from exc
        finally:
            if body is not None:
                body.close()
            temporary.unlink(missing_ok=True)

    def _call_daemon(self, request, state_path, state):
        sent = False
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
                sock.settimeout(10)
                sock.connect(self.socket_path)
                # Persist uncertainty before any request bytes can reach the daemon.
                state['running'] = True
                self._write_json(state_path, state)
                sent = True
                sock.sendall(_canonical(request) + b'\n')
                sock.settimeout(None)
                with sock.makefile('rb') as stream:
                    line = stream.readline(16 * 1024 * 1024 + 1)
                if not line.endswith(b'\n') or len(line) > 16 * 1024 * 1024:
                    raise ValueError('invalid daemon response frame')
                response = json.loads(line)
                if not isinstance(response, dict) or type(response.get('ok')) is not bool:
                    raise ValueError('invalid daemon response')
                return response
        except Exception as exc:
            raise WorkerError('already_running' if sent else 'worker_unavailable',
                              'daemon result is unresolved' if sent else 'model socket is unavailable',
                              True) from exc

    def _execute(self, spec, fingerprint, job_dir, state, out, context, deadline):
        state_path = job_dir / 'state.json'
        state['fingerprint'] = fingerprint
        state['output'] = str(out)
        self._write_json(state_path, state)
        try:
            if 'response' not in state:
                input_path = job_dir / 'input.wav'
                self._download(spec, input_path)
                self._check_active(context, deadline)
                response = self._call_daemon({'input': str(input_path), 'out': str(out),
                                              'mode': spec['mode'], 'midi': None}, state_path, state)
                state['response'] = response
                state['running'] = False
                self._write_json(state_path, state)
            result = self._upload_result(spec, out, state['response'])
            state['result'] = result
            self._write_json(state_path, state)
            return result
        except WorkerError as exc:
            if not exc.retryable and exc.code != 'deadline_exceeded':
                state['error'] = {'code': exc.code, 'message': str(exc)}
                self._write_json(state_path, state)
            raise

    def _recover_response(self, state, out):
        # Unclaimed/preexisting directories have no immutable ownership marker.
        if not state.get('running') or state.get('output') != str(out):
            return None
        path = self._safe_path(out / 'result.json')
        if not path.is_file():
            return None
        try:
            report = self._read_json(path)
        except (ValueError, OSError):
            return None  # The daemon may still be writing its completion report.
        if not isinstance(report, dict) or report.get('status') not in ('completed', 'no_usable_note'):
            return None
        return {'ok': True, 'result': {'status': report['status'], 'output': str(out),
                                      'report': str(path), 'artifacts': report.get('artifacts', {})}}

    def _upload_result(self, spec, out, response):
        if not response['ok']:
            raise WorkerError('model_error', 'model daemon reported an execution failure')
        report = response.get('result')
        if not isinstance(report, dict):
            raise WorkerError('model_error', 'invalid model result')
        if report.get('status') == 'no_usable_note':
            raise WorkerError('no_usable_note', 'model found no usable note')
        if report.get('status') != 'completed':
            raise WorkerError('model_error', 'model did not complete inference')
        if report.get('output') != str(out):
            raise WorkerError('invalid_artifacts', 'model output directory does not match job')
        self._safe_path(out)
        files = {}
        for name in REQUIRED + OPTIONAL:
            path = self._safe_path(out / name)
            if not path.exists() and name in OPTIONAL:
                continue
            if not path.is_file():
                raise WorkerError('invalid_artifacts', f'required artifact is missing: {name}')
            files[name] = path
        try:
            parameter_json = self._read_json(files['final.unified.json'])
            if not isinstance(parameter_json, dict):
                raise ValueError('Unified document must be an object')
            detailed_report = self._read_json(files['result.json'])
            if not isinstance(detailed_report, dict) or detailed_report.get('status') != 'completed':
                raise ValueError('result report must show completed execution')
        except (ValueError, OSError) as exc:
            raise WorkerError('invalid_artifacts', 'invalid Unified parameter document') from exc
        if detailed_report.get('checkpoint_sha256') != CHECKPOINT_SHA256:
            raise WorkerError('checkpoint_mismatch', 'model report checkpoint differs from pinned release')
        if detailed_report.get('mode') != spec['mode']:
            raise WorkerError('invalid_artifacts', 'model report mode differs from job spec')
        metadata = {}
        for name, path in files.items():
            digest = hashlib.sha256()
            size = 0
            with path.open('rb') as stream:
                while chunk := stream.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
            metadata[name] = {'bucket': 'synthx-jobs', 'key': spec['output_prefix'] + name,
                              'sha256': digest.hexdigest(), 'size_bytes': size}
        self._verify_manifest(report.get('artifacts', {}), out, metadata)
        self._verify_manifest(detailed_report.get('artifacts', {}), out, metadata, required=True)
        if self._verify_path(report.get('report'), out) != out / 'result.json':
            raise WorkerError('invalid_artifacts', 'model report must be this job result.json')
        # worker_log belongs to the daemon's private requests directory. It is
        # diagnostic metadata only: never open, follow, validate or upload it.
        artifacts = {}
        try:
            # Build the client before starting threads; never share a Session.
            client = self._client()
            def upload(item):
                name, path = item
                started = time.perf_counter()
                with path.open('rb') as stream:
                    client.put_object(Bucket='synthx-jobs',
                                      Key=spec['output_prefix'] + name, Body=stream)
                logger.info('V73_ARTIFACT_UPLOAD job=%s name=%s bytes=%d seconds=%.3f',
                            spec['job_id'], name, metadata[name]['size_bytes'],
                            time.perf_counter() - started)
                return name
            # Wait for every upload before publishing the durable result. The
            # context also drains in-flight uploads when any individual one fails.
            with ThreadPoolExecutor(max_workers=self.upload_workers,
                                    thread_name_prefix='v73-upload') as pool:
                for name in pool.map(upload, files.items()):
                    artifacts[name] = metadata[name]
        except Exception as exc:
            raise WorkerError('storage_error', 'artifact upload failed', True) from exc
        return {'job_id': spec['job_id'], 'model': 'v7_3_full',
                'checkpoint_sha256': CHECKPOINT_SHA256, 'artifacts': artifacts,
                'parameter_json': parameter_json}

    def _verify_path(self, reference, out):
        if not isinstance(reference, str):
            raise WorkerError('invalid_artifacts', 'artifact path must be a string')
        candidate = Path(reference) if Path(reference).is_absolute() else out / reference
        candidate = self._safe_path(candidate)
        if not candidate.is_relative_to(out):
            raise WorkerError('invalid_artifacts', 'artifact escapes job output')
        return candidate

    def _verify_manifest(self, manifest, out, metadata, required=False):
        if not isinstance(manifest, dict):
            raise WorkerError('invalid_artifacts', 'invalid model artifact manifest')
        if required and not set(REQUIRED[:-1]).issubset(manifest):
            raise WorkerError('invalid_artifacts', 'required artifact is missing from model report')
        for name, reference in manifest.items():
            descriptor = reference if isinstance(reference, dict) else {}
            candidate = self._verify_path(descriptor.get('path') if descriptor else reference, out)
            if name in metadata:
                if candidate != out / name:
                    raise WorkerError('invalid_artifacts', 'artifact path does not match its fixed filename')
                if required and descriptor.get('sha256') != metadata[name]['sha256']:
                    raise WorkerError('invalid_artifacts', 'model artifact SHA256 does not match file')
                if 'sha256' in descriptor and descriptor['sha256'] != metadata[name]['sha256']:
                    raise WorkerError('invalid_artifacts', 'model artifact SHA256 does not match file')
