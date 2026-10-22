"""Real HTTP → gRPC version routing, retaining the legacy refinement worker."""
import json
import threading
import unittest
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import grpc
import httpx
from system import api_server, inference_pb2_grpc
from system.grpc_worker_server import WorkerServicer
from system.grpc_worker_pool import GrpcWorkerPool
from system.queue import TaskQueue
from system.scheduler import Scheduler
from system.worker_text import TextWorker


class TextV123GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_v123_routes_to_its_origin_and_rejects_mismatched_versions(self):
        calls = []
        answer_model = ['text_v1.2.3']
        class HTTP(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                calls.append((self.path, body))
                self.send_response(200); self.end_headers()
                result = {'model':answer_model[0]}
                if self.path.endswith('retrieve'):
                    result.update(primary_candidate_id='vital:a',candidates=[{'candidate_id':'vital:a'}])
                else:
                    result.update(use_clap=True,ranked_candidates=[{'candidate_id':'vital:a','clap_similarity':.4}])
                self.wfile.write(json.dumps(result).encode())
        http = ThreadingHTTPServer(('127.0.0.1',0), HTTP)
        threading.Thread(target=http.serve_forever,daemon=True).start()
        self.addCleanup(http.server_close); self.addCleanup(http.shutdown)
        url = 'http://127.0.0.1:%d' % http.server_port
        worker = TextWorker('http://127.0.0.1:1',v123_url=url)
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        inference_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerServicer(122,worker),server)
        port=server.add_insecure_port('127.0.0.1:0'); server.start()
        self.addCleanup(lambda:server.stop(0).wait())
        with patch.dict('os.environ',{'WORKER_CAPABILITIES':'{"text":["synthx_text_v122","synthx_text_v123"]}'}):
            pool=GrpcWorkerPool(static_workers={'text':'127.0.0.1:%d'%port});pool.start();self.addCleanup(pool.stop)
            queue=TaskQueue(maxsize=4); scheduler=Scheduler(queue,pool.submit);scheduler.start();self.addCleanup(scheduler.stop)
            with patch.multiple(api_server,_task_queue=queue,_job_gateway=object(),_jobs_token='test-only',create=True):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=api_server.app),base_url='http://test') as client:
                    headers={'Authorization':'Bearer test-only'}
                    body={'model':'text_v1.2.3','operation':'retrieve','payload':{'pipeline_version':'text_v1.2.3','description':'warm pad','target_engine':'vital'}}
                    self.assertEqual((await client.post('/text/tasks',json=body)).status_code,401)
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,200,result.text)
                    self.assertEqual(result.json()['model'],'text_v1.2.3')
                    self.assertEqual(calls,[('/text-v1/retrieve',body['payload'])])
                    body['payload']['pipeline_version']='text_v1.2_refinement'
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,422)
                    self.assertEqual(len(calls),1)
                    body['payload']['pipeline_version']='text_v1.2.3'
                    answer_model[0]='text_v1.2.2'
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,502)
                    body.update(operation='rerank',payload={'candidate_ids':['vital:a'],'caption':'pad'})
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,502)
                    answer_model[0]='text_v1.2.3'
                    result=await client.post('/text/tasks',json=body,headers=headers)
                    self.assertEqual(result.status_code,200,result.text)

    async def test_unconfigured_v123_never_falls_back_to_legacy_http(self):
        from system.worker_v73 import WorkerError
        import time
        worker=TextWorker('http://127.0.0.1:1')
        with self.assertRaises(WorkerError) as failure:
            worker.process({'kind':'synthx_text_v123','model':'text_v1.2.3','operation':'retrieve','payload':{'pipeline_version':'text_v1.2.3','target_engine':'vital'}},None,deadline=time.monotonic()+2)
        self.assertEqual(failure.exception.code,'invalid_spec')
