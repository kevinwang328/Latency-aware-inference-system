"""Retired text versions must never enter the queue or reach a model service."""
import json
import time
from unittest.mock import patch
import httpx
import pytest
from system import api_server
from system.worker_text import TextWorker
from system.worker_v73 import WorkerError


@pytest.mark.asyncio
@pytest.mark.parametrize('model', ['text_v1.2.2', 'text_v1.2', 'text_v1'])
async def test_retired_version_rejected_before_enqueue(model):
    from unittest.mock import Mock
    queue = Mock()
    queue.put.side_effect = AssertionError('retired request reached scheduler')
    with patch.multiple(api_server, _task_queue=queue, _job_gateway=object(), _jobs_token='test-only', create=True):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app), base_url='http://test') as client:
            result = await client.post('/text/tasks', headers={'Authorization':'Bearer test-only'}, json={
                'model':model, 'operation':'retrieve', 'payload':{'pipeline_version':model, 'target_engine':'vital'}})
    assert result.status_code == 422
    queue.put.assert_not_called()


@pytest.mark.parametrize('model', ['text_v1.2.2', 'text_v1.2', 'text_v1'])
def test_adapter_rejects_retired_version_before_http(model):
    worker = TextWorker('http://unused.invalid')
    with patch('urllib.request.urlopen') as http, pytest.raises(WorkerError):
        worker.process({'kind':'synthx_text_v122','model':model,'operation':'retrieve',
                        'payload':{'pipeline_version':model,'target_engine':'vital'}}, None, deadline=time.monotonic()+2)
    http.assert_not_called()


def test_new_worker_uses_only_canonical_origin():
    with patch.dict('os.environ', {'TEXT_SERVICE_URL':'http://current:18083','TEXT_V123_SERVICE_URL':'http://retired-config:18080'}):
        worker = TextWorker()
    assert worker.base_url == 'http://current:18083'
    assert not hasattr(worker, 'v123_url')


def test_retired_backend_configuration_cannot_start_as_simulation():
    from system.grpc_worker_server import WorkerServicer
    with patch.dict('os.environ', {'INFERENCE_BACKEND':'text_v1.2.2'}), pytest.raises(ValueError):
        WorkerServicer(122)
