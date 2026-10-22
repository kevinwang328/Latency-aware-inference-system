"""Retired text versions must never enter the queue or reach a model service."""
import time
import unittest
from unittest.mock import Mock, patch
import httpx
from system import api_server
from system.worker_text import TextWorker
from system.worker_v73 import WorkerError


class RetiredGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_retired_versions_are_rejected_before_enqueue(self):
        for model in ('text_v1.2.2', 'text_v1.2', 'text_v1'):
            with self.subTest(model=model):
                queue = Mock()
                queue.put.side_effect = AssertionError('retired request reached scheduler')
                with patch.multiple(api_server, _task_queue=queue, _job_gateway=object(), _jobs_token='test-only', create=True):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url='http://test') as client:
                        result = await client.post('/text/tasks', headers={'Authorization':'Bearer test-only'}, json={
                            'model':model, 'operation':'retrieve', 'payload':{'pipeline_version':model, 'target_engine':'vital'}})
                self.assertEqual(result.status_code,422)
                queue.put.assert_not_called()


class RetiredAdapterTests(unittest.TestCase):
    def test_adapter_rejects_retired_versions_before_http(self):
        worker = TextWorker('http://unused.invalid')
        for model in ('text_v1.2.2', 'text_v1.2', 'text_v1'):
            with self.subTest(model=model), patch('urllib.request.urlopen') as http:
                with self.assertRaises(WorkerError):
                    worker.process({'kind':'synthx_text_v122','model':model,'operation':'retrieve',
                                    'payload':{'pipeline_version':model,'target_engine':'vital'}}, None, deadline=time.monotonic()+2)
                http.assert_not_called()

    def test_new_worker_uses_only_canonical_origin(self):
        with patch.dict('os.environ', {'TEXT_SERVICE_URL':'http://current:18083','TEXT_V123_SERVICE_URL':'http://retired-config:18080'}):
            worker = TextWorker()
        self.assertEqual(worker.base_url,'http://current:18083')
        self.assertFalse(hasattr(worker,'v123_url'))

    def test_retired_backend_configuration_cannot_start_as_simulation(self):
        from system.grpc_worker_server import WorkerServicer
        with patch.dict('os.environ', {'INFERENCE_BACKEND':'text_v1.2.2'}), self.assertRaises(ValueError):
            WorkerServicer(122)
